"""AgentSandboxEnvironment and the provider, as Hermes calls them, on a real Sandboxes over the
in-memory cluster and SDK (tests/fakes.py).

The cleanup() cases follow docs/hermes-contract.md: persistent → suspend (claim and volume kept),
non-persistent → claim deleted, idempotent, never raising once the sandbox is released.
"""

import gc
import weakref

import pytest

import hermes_plugins.k8s_agent_sandbox as plugin          # loaded by conftest.py, as Hermes loads it
from hermes_plugins.k8s_agent_sandbox import environment, files, sandboxes
from hermes_plugins.k8s_agent_sandbox.settings import Settings
from fakes import FakeCluster, FakeSdk

TASK = "session:cli:abc123"
CLAIM = sandboxes.claim_name(TASK)


@pytest.fixture
def cluster():
    return FakeCluster()


@pytest.fixture
def pool(cluster, monkeypatch):
    pool = sandboxes.Sandboxes(cluster, FakeSdk(cluster), Settings(orphan_idle=0, max_idle=0),
                               files=lambda: files.HermesFiles(lambda: []))
    monkeypatch.setattr(plugin, "_sandboxes", lambda: pool)
    monkeypatch.setattr(environment, "_bridge_paths", [])   # no grpc to locate in tests
    return pool


def create(task_id=TASK, config=None):
    return plugin.AgentSandboxProvider().create_environment(
        cwd="", timeout=60, task_id=task_id, container_config=config)


# --- Provider --------------------------------------------------------------------------------------

@pytest.mark.parametrize("task_id, config, persistent", [
    (TASK, {"container_persistent": True}, True),
    (TASK, {"container_persistent": False}, False),
    (TASK, None, True),
    ("prompt-backend-probe", {"container_persistent": True}, False),
])
def test_provider_creates_an_environment_on_the_tasks_sandbox(pool, cluster, task_id, config, persistent):
    env = create(task_id, config)
    assert env._persistent is persistent      # Hermes's is_persistent_env reads this attribute
    assert env.cwd == "/workspace"
    assert cluster.mode_of(sandboxes.claim_name(task_id)) == "Running"


# --- cleanup() -------------------------------------------------------------------------------------

def test_persistent_cleanup_suspends_and_keeps_the_claim(pool, cluster):
    create().cleanup()
    assert cluster.mode_of(CLAIM) == "Suspended"


def test_non_persistent_cleanup_deletes_the_claim(pool, cluster):
    create(config={"container_persistent": False}).cleanup()
    assert cluster.claim(CLAIM) is None


def test_cleanup_stops_long_lived_processes_and_is_idempotent(pool, cluster, monkeypatch):
    env = create()
    stopped = []
    monkeypatch.setattr(env, "stop_all_processes", lambda: stopped.append(True))
    env.cleanup()
    cluster.set_mode(CLAIM, "Running")        # another process resumed it meanwhile
    env.cleanup()                             # Hermes may call it again (eviction, __del__)
    assert stopped == [True]
    assert cluster.mode_of(CLAIM) == "Running"


def test_a_failing_process_stop_still_releases_the_sandbox(pool, cluster, monkeypatch):
    env = create()
    def fail():
        raise RuntimeError("bridge gone")
    monkeypatch.setattr(env, "stop_all_processes", fail)
    env.cleanup()
    assert cluster.mode_of(CLAIM) == "Suspended"


def test_a_dropped_environment_is_collected(pool):
    # Hermes owns the environment: nothing in the plugin may keep it alive.
    ref = weakref.ref(create())
    gc.collect()
    assert ref() is None


# --- Commands and streams --------------------------------------------------------------------------

def test_a_command_after_another_process_suspended_the_sandbox_resumes_it(pool, cluster):
    env = create()
    cluster.set_mode(CLAIM, "Suspended")
    env._before_execute()
    assert cluster.mode_of(CLAIM) == "Running"
    assert files.MARKER in cluster.pod_of(CLAIM)


def test_a_stream_into_a_suspended_sandbox_resumes_it(pool, cluster):
    env = create()
    cluster.set_mode(CLAIM, "Suspended")
    argv = env.exec_prefix(user="pn")
    assert cluster.mode_of(CLAIM) == "Running"
    assert argv[argv.index("--target") + 1] == "10.0.0.7:8080"
    assert argv[-4:] == ["runuser", "-u", "pn", "--"]


def test_a_command_runs_in_the_tasks_sandbox(pool, cluster):
    env = create()
    env._before_execute()
    proc = env._run_bash("touch /workspace/x")
    assert proc.wait(timeout=2) == 0
    assert "/workspace/x" in cluster.pod_of(CLAIM)


def test_exec_prefix_after_cleanup_is_none(pool):
    env = create()
    env.cleanup()
    assert env.exec_prefix() is None          # Hermes then treats the backend as unable to stream


def test_exec_prefix_refuses_a_user_that_is_no_account_name(pool):
    assert create().exec_prefix(user="pn; rm -rf /") is None
