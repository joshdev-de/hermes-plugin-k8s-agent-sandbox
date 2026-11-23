"""Sandboxes and Lease against an in-memory cluster and SDK that share one state (tests/fakes.py).

Assertions are on what the cluster and the sandbox's pod end up holding, not on call logs.
"""

import pytest

from hermes_plugins.k8s_agent_sandbox import files, sandboxes   # loaded by conftest.py
from hermes_plugins.k8s_agent_sandbox.settings import Settings
from fakes import FakeCluster, FakeSdk

NS = "hermes-sandboxes"
TASK = "session:cli:abc123"
CLAIM = sandboxes.claim_name(TASK)


@pytest.fixture
def host_files(tmp_path):
    """One Hermes file on the host, synced to /root/.hermes/skills/a.md."""
    path = tmp_path / "a.md"
    path.write_text("skill")
    return [(str(path), "/root/.hermes/skills/a.md")]


@pytest.fixture
def cluster():
    return FakeCluster()


@pytest.fixture
def sdk(cluster):
    return FakeSdk(cluster)


@pytest.fixture
def make_pool(cluster, sdk, host_files):
    """A Sandboxes over the fakes; sweeps after 1800 s, no upkeep loop unless ``interval`` is set."""
    def make(*, interval=0, **settings):
        settings = {"namespace": NS, "orphan_idle": 1800, **settings}
        return sandboxes.Sandboxes(cluster, sdk, Settings(**settings), interval=interval,
                                   files=lambda: files.HermesFiles(lambda: host_files))
    return make


@pytest.fixture
def pool(make_pool):
    return make_pool()


# --- acquire ---------------------------------------------------------------------------------------

def test_acquire_claims_a_running_sandbox_with_hermes_files_and_a_heartbeat(pool, cluster):
    lease = pool.acquire(TASK, persistent=True)
    assert lease.held and lease.claim == CLAIM
    assert cluster.mode_of(CLAIM) == "Running"
    assert cluster.claim(CLAIM)["metadata"]["labels"] == sandboxes.labels(TASK)
    assert sandboxes.HEARTBEAT in cluster.claim(CLAIM)["metadata"]["annotations"]
    assert cluster.pod_of(CLAIM)["/root/.hermes/skills/a.md"] == b"skill"


def test_acquire_resumes_a_suspended_claim_before_adopting_it(make_pool, cluster, sdk):
    pool = make_pool(resume_timeout=42)
    cluster.add_claim(CLAIM, labels=sandboxes.labels(TASK), mode="Suspended")
    lease = pool.acquire(TASK, persistent=True)   # the fake times out adopting a suspended claim
    assert cluster.mode_of(CLAIM) == "Running"
    assert sdk.created[0]["sandbox_ready_timeout"] == 42
    assert sdk.handle(CLAIM).resumed_with == 42   # waited for the Running generation's Ready
    assert cluster.pod_of(CLAIM)["/root/.hermes/skills/a.md"] == b"skill"


def test_acquire_of_a_running_claim_adopts_it_and_replaces_stale_files(pool, cluster, sdk):
    cluster.add_claim(CLAIM, labels=sandboxes.labels(TASK))
    cluster.pod_of(CLAIM).update({"/root/.hermes/old.md": b"removed on the host", "/workspace/w": b"kept"})
    pool.acquire(TASK, persistent=True)
    pod = cluster.pod_of(CLAIM)
    assert "/root/.hermes/old.md" not in pod and pod["/workspace/w"] == b"kept"
    assert sdk.created[0]["sandbox_ready_timeout"] == sandboxes.NEW_CLAIM_TIMEOUT


def deadline(cluster, claim):
    """Seconds from now until the controller deletes the claim, and the policy it applies."""
    lifecycle = cluster.claim(claim)["spec"]["lifecycle"]
    return sandboxes._parse_time(lifecycle["shutdownTime"]) - time.time(), lifecycle["shutdownPolicy"]


def test_adopting_a_claim_moves_its_deletion_deadline_to_max_idle_from_now(make_pool, cluster):
    # The SDK keeps an adopted claim's lifecycle: a claim created long ago would otherwise be
    # deleted on its creation-based deadline while in use.
    pool = make_pool(max_idle=7200)
    cluster.add_claim(CLAIM, labels=sandboxes.labels(TASK))
    cluster.claim(CLAIM)["spec"] = {"lifecycle": {"shutdownTime": "2023-11-14T22:13:20Z",
                                                  "shutdownPolicy": "Delete"}}
    pool.acquire(TASK, persistent=True)
    left, policy = deadline(cluster, CLAIM)
    assert 7100 < left <= 7200 and policy == "Delete"


def test_acquire_that_fails_holds_nothing(pool, sdk, cluster):
    cluster.add_claim(CLAIM, labels=sandboxes.labels(TASK))
    def fail(**kwargs):
        raise TimeoutError("no Ready sandbox")
    sdk.create_sandbox = fail
    with pytest.raises(TimeoutError):
        pool.acquire(TASK, persistent=True)
    assert pool.sweep(now=FUTURE) == 1      # not held here, so the sweep suspends it
    assert cluster.mode_of(CLAIM) == "Suspended"


# --- sweep -----------------------------------------------------------------------------------------

T0 = 1_700_000_000.0   # 2023-11-14T22:13:20Z
FUTURE = T0 * 2


def stamp(t):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def orphan(cluster, name, *, heartbeat=None, created=T0, mode="Running", labels=sandboxes.MANAGED_BY):
    annotations = {sandboxes.HEARTBEAT: stamp(heartbeat)} if heartbeat is not None else {}
    return cluster.add_claim(name, labels=labels, annotations=annotations, created=stamp(created), mode=mode)


def test_sweep_suspends_a_sandbox_without_recent_heartbeat(pool, cluster):
    orphan(cluster, "a", heartbeat=T0)
    assert pool.sweep(now=T0 + 1801) == 1
    assert cluster.mode_of("a") == "Suspended"


def test_sweep_keeps_a_recently_heartbeated_sandbox(pool, cluster):
    orphan(cluster, "a", heartbeat=T0 + 1000)
    assert pool.sweep(now=T0 + 1801) == 0
    assert cluster.mode_of("a") == "Running"


def test_sweep_falls_back_to_the_creation_time(pool, cluster):
    orphan(cluster, "a", created=T0)
    assert pool.sweep(now=T0 + 1801) == 1


def test_sweep_leaves_claims_held_here(pool, cluster):
    lease = pool.acquire(TASK, persistent=True)
    assert pool.sweep(now=FUTURE) == 0
    assert cluster.mode_of(CLAIM) == "Running" and lease.held


def test_sweep_skips_claims_being_deleted(pool, cluster):
    orphan(cluster, "a", heartbeat=T0)
    cluster.claim("a")["metadata"]["deletionTimestamp"] = stamp(T0)
    assert pool.sweep(now=FUTURE) == 0


def test_sweep_ignores_claims_of_other_owners(pool, cluster):
    orphan(cluster, "a", heartbeat=T0, labels={})
    assert pool.sweep(now=FUTURE) == 0


def test_sweep_does_not_count_an_already_suspended_sandbox(pool, cluster):
    orphan(cluster, "a", heartbeat=T0, mode="Suspended")
    assert pool.sweep(now=FUTURE) == 0


# --- delete_session --------------------------------------------------------------------------------

def test_delete_session_deletes_the_claims_it_created_and_their_pods(pool, cluster):
    pool.acquire(TASK, persistent=True)
    other = pool.acquire("session:cli:zzz999", persistent=True)
    assert pool.delete_session("abc123") == [CLAIM]
    assert cluster.claim(CLAIM) is None and cluster.claim(other.claim) is not None


def test_delete_session_without_claims_deletes_nothing(pool):
    pool.acquire("default", persistent=True)
    assert pool.delete_session("abc123") == []


def test_claim_name_is_stable_and_dns_safe():
    import re
    task = "session:telegram:Some User/Chat:20261122_ABC"
    name = sandboxes.claim_name(task)
    assert name == sandboxes.claim_name(task) != sandboxes.claim_name(task + "x")
    assert re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", name) and len(name) <= 63


# --- release ---------------------------------------------------------------------------------------

def test_persistent_release_suspends_and_keeps_the_claim(pool, cluster, sdk):
    lease = pool.acquire(TASK, persistent=True)
    sandbox = sdk.handle(CLAIM)
    lease.release()
    assert not lease.held
    assert cluster.mode_of(CLAIM) == "Suspended" and cluster.claim(CLAIM) is not None
    assert sandbox.closed


def test_release_is_idempotent(pool, cluster):
    lease = pool.acquire(TASK, persistent=True)
    lease.release()
    cluster.set_mode(CLAIM, "Running")            # someone else resumed it meanwhile
    lease.release()                               # Hermes may call cleanup() again (eviction, __del__)
    assert cluster.mode_of(CLAIM) == "Running"


def test_non_persistent_release_deletes_the_claim(pool, cluster):
    lease = pool.acquire(TASK, persistent=False)
    lease.release()
    assert cluster.claim(CLAIM) is None


def test_a_claim_held_twice_here_is_suspended_when_its_last_lease_goes(pool, cluster, sdk):
    first, second = pool.acquire(TASK, persistent=True), pool.acquire(TASK, persistent=True)
    first.release()
    assert cluster.mode_of(CLAIM) == "Running"
    second.ready()
    assert sdk.handle(CLAIM).commands.run("true").exit_code == 0   # its connection still works
    second.release()
    assert cluster.mode_of(CLAIM) == "Suspended"


def test_suspend_idle_off_keeps_the_pod_running(make_pool, cluster):
    lease = make_pool(suspend_idle=False).acquire(TASK, persistent=True)
    lease.release()
    assert cluster.mode_of(CLAIM) == "Running" and not lease.held


def test_failed_suspend_is_logged_not_raised(pool, cluster, monkeypatch):
    lease = pool.acquire(TASK, persistent=True)
    def api_down(*args, **kwargs):
        raise RuntimeError("API down")
    monkeypatch.setattr(cluster, "patch_namespaced_custom_object", api_down)
    lease.release()                               # the sweep retries later
    assert not lease.held


# --- ready -----------------------------------------------------------------------------------------

def test_ready_with_files_in_place_only_probes(pool, cluster, sdk):
    lease = pool.acquire(TASK, persistent=True)
    before = len(sdk.handle(CLAIM).ran)
    lease.ready()
    assert len(sdk.handle(CLAIM).ran) == before + 1   # the marker probe, nothing copied


def test_ready_copies_everything_into_a_restarted_pod(pool, cluster):
    lease = pool.acquire(TASK, persistent=True)
    cluster.pod_of(CLAIM).clear()                 # pod evicted and recreated: /root/.hermes is empty
    lease.ready()
    assert cluster.pod_of(CLAIM)["/root/.hermes/skills/a.md"] == b"skill"


def test_ready_resumes_a_sandbox_suspended_under_it_and_copies_the_files_again(make_pool, cluster, sdk):
    lease = make_pool(resume_timeout=42).acquire(TASK, persistent=True)
    cluster.set_mode(CLAIM, "Suspended")          # another process's sweep, or its cleanup
    lease.ready()
    sandbox = sdk.handle(CLAIM)
    assert cluster.mode_of(CLAIM) == "Running" and sandbox.resumed_with == 42
    assert cluster.pod_of(CLAIM)["/root/.hermes/skills/a.md"] == b"skill"
    assert sandbox.commands.run("true").exit_code == 0


def test_ready_after_release_raises(pool):
    lease = pool.acquire(TASK, persistent=True)
    lease.release()
    with pytest.raises(sandboxes.Released):
        lease.ready()


# --- commands and streams --------------------------------------------------------------------------

def test_start_after_release_raises(pool):
    lease = pool.acquire(TASK, persistent=True)
    lease.release()
    with pytest.raises(sandboxes.Released):
        lease.start("true", timeout=60)


def test_stream_target_resumes_a_sandbox_suspended_under_it(pool, cluster):
    lease = pool.acquire(TASK, persistent=True)
    cluster.set_mode(CLAIM, "Suspended")
    assert lease.stream_target() == "10.0.0.7:8080"
    assert cluster.mode_of(CLAIM) == "Running"
    assert cluster.pod_of(CLAIM)["/root/.hermes/skills/a.md"] == b"skill"


def test_stream_target_after_release_raises(pool):
    lease = pool.acquire(TASK, persistent=True)
    lease.release()
    with pytest.raises(sandboxes.Released):
        lease.stream_target()


# --- upkeep loop: heartbeat and orphan sweep ------------------------------------------------------

import gc
import threading
import time
import weakref

OLD = "2000-01-01T00:00:00Z"


def wait_for(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not reached in time")
        time.sleep(0.005)


def upkeep_threads():
    return [t for t in threading.enumerate() if t.name == "agent-sandbox-upkeep"]


def last_heartbeat(cluster, claim):
    return cluster.claim(claim)["metadata"]["annotations"][sandboxes.HEARTBEAT]


@pytest.fixture
def fast_pool(make_pool):
    pool = make_pool(interval=0.01)
    yield pool
    pool.stop_sweeping()
    wait_for(lambda: not upkeep_threads())        # no test leaves one behind


def test_held_claims_are_heartbeated_until_released(fast_pool, cluster):
    lease = fast_pool.acquire(TASK, persistent=True)
    cluster.claim(CLAIM)["metadata"]["annotations"][sandboxes.HEARTBEAT] = OLD
    wait_for(lambda: last_heartbeat(cluster, CLAIM) != OLD)
    lease.release()
    wait_for(lambda: not upkeep_threads())        # nothing held, not sweeping: the loop ends
    cluster.claim(CLAIM)["metadata"]["annotations"][sandboxes.HEARTBEAT] = OLD
    time.sleep(0.05)
    assert last_heartbeat(cluster, CLAIM) == OLD


def test_each_heartbeat_moves_the_deletion_deadline(make_pool, cluster):
    pool = make_pool(interval=0.01, max_idle=7200)
    lease = pool.acquire(TASK, persistent=True)
    cluster.claim(CLAIM)["spec"]["lifecycle"]["shutdownTime"] = "2023-11-14T22:13:20Z"
    wait_for(lambda: deadline(cluster, CLAIM)[0] > 7100)
    lease.release()
    wait_for(lambda: not upkeep_threads())


def test_a_dropped_lease_is_collected_and_no_longer_heartbeated(fast_pool, cluster):
    # Hermes dropping an environment without cleanup(): its lease must go with it, and the claim
    # must stop being heartbeated, so the sweep can suspend it.
    lease = fast_pool.acquire(TASK, persistent=True)
    ref = weakref.ref(lease)
    del lease
    gc.collect()
    assert ref() is None
    wait_for(lambda: not upkeep_threads())
    assert fast_pool.sweep(now=FUTURE) == 1


def test_the_loop_starts_again_for_the_next_lease(fast_pool, cluster):
    fast_pool.acquire(TASK, persistent=True).release()
    wait_for(lambda: not upkeep_threads())
    lease = fast_pool.acquire(TASK, persistent=True)
    cluster.claim(CLAIM)["metadata"]["annotations"][sandboxes.HEARTBEAT] = OLD
    wait_for(lambda: last_heartbeat(cluster, CLAIM) != OLD)
    lease.release()


def test_sweeping_suspends_orphans_until_stopped(fast_pool, cluster):
    orphan(cluster, "a", heartbeat=T0)
    fast_pool.start_sweeping()
    wait_for(lambda: cluster.mode_of("a") == "Suspended")
    fast_pool.stop_sweeping()
    wait_for(lambda: not upkeep_threads())        # nothing held, not sweeping: the loop ends
    orphan(cluster, "b", heartbeat=T0)
    time.sleep(0.05)
    assert cluster.mode_of("b") == "Running"


def test_a_copy_that_stopped_sweeping_still_heartbeats_its_leases(fast_pool, cluster):
    # An unloaded copy: its environments, still in Hermes's cache, keep their claims alive.
    fast_pool.start_sweeping()
    lease = fast_pool.acquire(TASK, persistent=True)
    fast_pool.stop_sweeping()
    orphan(cluster, "a", heartbeat=T0)
    cluster.claim(CLAIM)["metadata"]["annotations"][sandboxes.HEARTBEAT] = OLD
    wait_for(lambda: last_heartbeat(cluster, CLAIM) != OLD)
    assert cluster.mode_of("a") == "Running"
    assert upkeep_threads()
    lease.release()


def test_an_unloaded_copys_sandboxes_is_collected_with_its_last_lease(make_pool):
    # After _unload only the old copy's leases hold its Sandboxes; the upkeep loop doesn't.
    pool = make_pool(interval=0.01)
    pool.start_sweeping()
    lease = pool.acquire(TASK, persistent=True)
    pool.stop_sweeping()
    ref = weakref.ref(pool)
    del pool
    gc.collect()
    assert ref() is not None                      # the lease keeps it
    lease.release()
    del lease
    wait_for(lambda: gc.collect() is not None and ref() is None)
    wait_for(lambda: not upkeep_threads())


def test_no_upkeep_loop_while_the_sweep_and_the_deadline_are_off(make_pool):
    pool = make_pool(orphan_idle=0, max_idle=0, interval=None)
    pool.start_sweeping()
    lease = pool.acquire(TASK, persistent=True)
    assert not upkeep_threads()
    lease.release()


def test_with_the_sweep_off_held_claims_keep_their_deadline_moving_and_nothing_is_swept(make_pool, cluster):
    pool = make_pool(orphan_idle=0, max_idle=7200, interval=0.01)
    orphan(cluster, "a", heartbeat=T0)
    pool.start_sweeping()                          # off: no sweep, even on the running loop
    lease = pool.acquire(TASK, persistent=True)
    cluster.claim(CLAIM)["spec"]["lifecycle"]["shutdownTime"] = "2023-11-14T22:13:20Z"
    wait_for(lambda: deadline(cluster, CLAIM)[0] > 7100)
    assert pool.sweep(now=FUTURE) == 0 and cluster.mode_of("a") == "Running"
    lease.release()
    wait_for(lambda: not upkeep_threads())


def test_with_the_deadline_off_a_claim_gets_none_and_loses_an_old_one(make_pool, cluster, sdk):
    pool = make_pool(max_idle=0)
    cluster.add_claim(CLAIM, labels=sandboxes.labels(TASK))
    cluster.claim(CLAIM)["spec"] = {"lifecycle": {"shutdownTime": "2023-11-14T22:13:20Z"}}
    pool.acquire(TASK, persistent=True)
    assert "lifecycle" not in cluster.claim(CLAIM)["spec"]
    assert sdk.created[0]["shutdown_after_seconds"] is None


def test_the_interval_is_a_third_of_the_shorter_period_that_is_on():
    assert sandboxes.upkeep_interval(1800, 2592000) == 600
    assert sandboxes.upkeep_interval(0, 2592000) == 864000
    assert sandboxes.upkeep_interval(1800, 0) == 600
    assert sandboxes.upkeep_interval(120, 0) == 60
    assert sandboxes.upkeep_interval(0, 0) == 0
