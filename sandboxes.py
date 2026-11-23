"""This plugin's sandboxes: everything it does with SandboxClaims, Sandboxes and their pods.

    Sandboxes(cluster, sdk, settings, files=...)    one per loaded copy of the plugin
      .acquire(task_id, persistent=...) -> Lease    the task's sandbox, running, with Hermes's files
      .start_sweeping() / .stop_sweeping()          the orphan sweep of this copy, on the upkeep loop
      .sweep()                                      suspend sandboxes no Hermes process holds
      .delete_session(session_id)                   delete a session's claims, volumes included

    Lease                                           one environment's hold on its sandbox
      .ready()                                      before every command: running, files present
      .start(command, timeout=...)                  start a command on sandboxd: its process handle
      .stream_target()                              ready, then sandboxd's gRPC address for streams
      .release()                                    suspend, keep or delete; idempotent

The SDK's Sandbox handle stays in here: callers see commands and addresses, not the SDK.

Talks to a Kubernetes ``CustomObjectsApi`` (``cluster``) and the agent-sandbox SDK's
``SandboxClient`` (``sdk``), both handed in, with the copy's ``settings.Settings``. Imports nothing
from Hermes; ``files`` makes the object that puts Hermes's files into a sandbox (files.HermesFiles).

The upkeep loop, one daemon thread per copy, is the orphan protocol's clock: every tick it writes
the heartbeat of each claim a Lease here holds, then sweeps if this copy sweeps. A heartbeat is what
tells every sweep, in any process or copy, that a claim is in use; it also moves the claim's
deletion deadline (``spec.lifecycle.shutdownTime``) to ``max_idle`` from now. The loop runs while
this copy sweeps or holds a lease: an unloaded copy stops sweeping but keeps heartbeating the claims
its environments, still in Hermes's cache, hold, until Hermes cleans them up.
"""

from __future__ import annotations

import hashlib
import logging
import re
import threading
import time
import weakref
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Optional, Protocol

if TYPE_CHECKING:
    from .settings import Settings

logger = logging.getLogger(__name__)

CLAIMS = ("extensions.agents.x-k8s.io", "v1beta1", "sandboxclaims")
SANDBOXES = ("agents.x-k8s.io", "v1beta1", "sandboxes")
MANAGED_BY = {"app.kubernetes.io/managed-by": "hermes-k8s-agent-sandbox"}
SESSION_LABEL = "hermes-k8s-agent-sandbox-session"   # hash of the Hermes session id (label values max 63 chars)
HEARTBEAT = "hermes-k8s-agent-sandbox-heartbeat"     # claim annotation: last time a Hermes process held it
NEW_CLAIM_TIMEOUT = 180                          # seconds for a warm-pool claim to become Ready


def upkeep_interval(orphan_idle: int, max_idle: int) -> int:
    """Seconds between upkeep ticks: three heartbeats per period of the shorter of the two settings
    that are on, so a held claim never looks orphaned to any sweep nor reaches its deletion deadline
    (min. 60). 0 when both are off: nothing reads heartbeats then."""
    periods = [p for p in (orphan_idle, max_idle) if p]
    return max(60, min(periods) // 3) if periods else 0


class Released(RuntimeError):
    """The lease was released; its environment was cleaned up."""


class Files(Protocol):
    def ensure(self, sandbox: Any, *, fresh: bool = False) -> None: ...


# --- Names and labels ------------------------------------------------------------------------------

def claim_name(task_id: str) -> str:
    """A stable, DNS-1123-safe SandboxClaim name for a Hermes task id."""
    slug = re.sub(r"[^a-z0-9-]+", "-", task_id.lower()).strip("-")[:40] or "task"
    digest = hashlib.sha256(task_id.encode()).hexdigest()[:8]
    return f"hermes-{slug}-{digest}"


def labels(task_id: str) -> dict[str, str]:
    """Labels for a task's claim: what sweep() and delete_session() find it by."""
    result = dict(MANAGED_BY)
    session_id = _session_id(task_id)
    if session_id:
        result[SESSION_LABEL] = _session_label(session_id)
    return result


def _session_id(task_id: str) -> Optional[str]:
    """The Hermes session id a task belongs to: task ids look like 'session:<origin...>:<session_id>'."""
    if task_id.startswith("session:") and ":" in task_id:
        return task_id.rsplit(":", 1)[1] or None
    return None


def _session_label(session_id: str) -> str:
    return hashlib.sha256(session_id.encode()).hexdigest()[:32]


def _is_not_found(e: Exception) -> bool:
    return getattr(e, "status", None) == 404


def _stamp(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_time(value: Any) -> Optional[float]:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _claim_sandbox_name(claim: dict) -> Optional[str]:
    """The Sandbox a claim is bound to (warm-pool sandboxes keep their own names)."""
    name = ((claim.get("status") or {}).get("sandbox") or {}).get("name")
    return name or ((claim.get("metadata") or {}).get("annotations") or {}).get("agents.x-k8s.io/sandbox-name")


class Sandboxes:
    """This plugin's sandboxes in one namespace, for one loaded copy of the plugin."""

    def __init__(self, cluster: Any, sdk: Any, settings: "Settings", *, files: Callable[[], Files],
                 interval: Optional[float] = None):
        """``interval``: seconds between upkeep ticks, by default
        ``upkeep_interval(orphan_idle, max_idle)``; tests shorten it, or pass 0 for no upkeep loop.
        Always 0 when both are 0."""
        self._cluster = cluster
        self._sdk = sdk
        self.settings = settings
        self.namespace = settings.namespace
        self._files = files
        default = upkeep_interval(settings.orphan_idle, settings.max_idle)
        self._interval = 0 if not default else default if interval is None else interval
        self._lock = threading.RLock()   # reentrant: release() may run from __del__ in any thread
        self._leases: "weakref.WeakSet[Lease]" = weakref.WeakSet()
        self._sweeping = False           # this copy sweeps (start_sweeping() until stop_sweeping())
        self._upkeep = False             # the upkeep loop runs

    def acquire(self, task_id: str, *, persistent: bool) -> "Lease":
        """The task's sandbox, running and Ready, heartbeating, with Hermes's files copied in.

        Creates the claim from the warm pool, or adopts it (with its volume) if it exists, setting
        it running first if it was suspended. Raises what the SDK raises (timeout, API error);
        nothing is held then.
        """
        claim = claim_name(task_id)
        # A suspended claim isn't Ready, so adopting it would wait out the timeout: set it running
        # first, then let create_sandbox wait for the new pod.
        resumed = self._resume(claim)
        sandbox = self._sdk.create_sandbox(
            warmpool=self.settings.warmpool,
            namespace=self.namespace,
            claim_name=claim,
            labels=labels(task_id),
            adopt_existing=True,
            shutdown_after_seconds=self.settings.max_idle or None,
            sandbox_ready_timeout=self.settings.resume_timeout if resumed else NEW_CLAIM_TIMEOUT,
        )
        if resumed:
            # The claim's Ready may still be the one from before the suspend; wait for the Sandbox
            # itself to report Ready for the Running generation.
            sandbox.resume(timeout=self.settings.resume_timeout)
        lease = Lease(self, claim, persistent, sandbox, self._files())
        with self._lock:
            self._leases.add(lease)
            self._start_upkeep()
        try:
            self._heartbeat(claim)
        except Exception as e:
            logger.warning("k8s_agent_sandbox: heartbeat for %s failed: %s", claim, e)
        logger.info("k8s_agent_sandbox: task %s uses claim %s/%s%s", task_id, self.namespace, claim,
                    " (resumed)" if resumed else "")
        lease.ready()
        return lease

    def start_sweeping(self) -> None:
        """Sweep on every upkeep tick from now on (the first one interval from now), until
        ``stop_sweeping()``. Nothing when the sweep is off (``orphan_idle`` 0)."""
        if not self.settings.orphan_idle:
            return
        with self._lock:
            self._sweeping = True
            self._start_upkeep()

    def stop_sweeping(self) -> None:
        """No more sweeps from this copy (its unload). Held claims keep their heartbeat; the upkeep
        loop ends once none is held."""
        with self._lock:
            self._sweeping = False

    def sweep(self, *, now: Optional[float] = None) -> int:
        """Suspend running sandboxes of this plugin's claims that no Hermes process has held for
        ``orphan_idle`` seconds: left behind when Hermes was killed or restarted without cleaning up, or
        created outside any session (the gateway's ``default`` task). Claims held here are never
        suspended. Returns how many it suspended."""
        idle = self.settings.orphan_idle
        if not idle:
            return 0
        with self._lock:
            held = {lease.claim for lease in list(self._leases) if lease.held}
        now = time.time() if now is None else now
        suspended = 0
        for claim in self._list(CLAIMS, MANAGED_BY):
            meta = claim.get("metadata") or {}
            if meta.get("name") in held or meta.get("deletionTimestamp"):
                continue
            last = (_parse_time((meta.get("annotations") or {}).get(HEARTBEAT))
                    or _parse_time(meta.get("creationTimestamp")))
            sandbox_name = _claim_sandbox_name(claim)
            if last is None or now - last < idle or not sandbox_name:
                continue
            try:
                if self._set_operating_mode(sandbox_name, "Suspended"):
                    suspended += 1
                    logger.info("k8s_agent_sandbox: suspended %s (claim %s): unused for %d s",
                                sandbox_name, meta.get("name"), int(now - last))
            except Exception as e:
                logger.warning("k8s_agent_sandbox: suspending %s failed: %s", sandbox_name, e)
        return suspended

    def delete_session(self, session_id: str) -> list[str]:
        """Delete the claims of a Hermes session; their Sandbox, pod and volume go with
        them (suspended or not), and so does a connection this copy holds to one. Returns the
        deleted claim names; raises if listing them fails."""
        deleted = []
        for item in self._list(CLAIMS, {SESSION_LABEL: _session_label(session_id)}):
            name = item["metadata"]["name"]
            self._sdk.delete_sandbox(name, namespace=self.namespace)   # logs its own errors
            deleted.append(name)
        return deleted

    # --- Upkeep loop ------------------------------------------------------------------------------

    def _start_upkeep(self) -> None:
        """Under ``self._lock``: start the upkeep loop unless it runs, or it has nothing to do (both
        the sweep and the deletion deadline off)."""
        if self._upkeep or not self._interval:
            return
        self._upkeep = True
        threading.Thread(target=Sandboxes._upkeep_loop, args=(weakref.ref(self), self._interval),
                         daemon=True, name="agent-sandbox-upkeep").start()

    @staticmethod
    def _upkeep_loop(pool_ref: "weakref.ref[Sandboxes]", interval: float) -> None:
        """Every ``interval`` seconds: refresh the heartbeat of every claim held here, whether or not
        commands run (background jobs, long commands, streams), then sweep if this copy sweeps.
        Ends when it neither sweeps nor holds a claim; the next acquire() or start_sweeping()
        starts it again. Holds the Sandboxes and its leases only during a tick, so a lease Hermes
        drops is collected and its claim ages out, and so is an unloaded copy's Sandboxes."""
        while True:
            time.sleep(interval)
            pool = pool_ref()
            if pool is None:
                return
            with pool._lock:
                claims = {lease.claim for lease in list(pool._leases) if lease.held}
                sweeping = pool._sweeping
                if not claims and not sweeping:
                    pool._upkeep = False
                    return
            for claim in claims:
                try:
                    pool._heartbeat(claim)
                except Exception as e:   # the next tick retries; three per idle period leave room
                    logger.warning("k8s_agent_sandbox: heartbeat for %s failed: %s", claim, e)
            if sweeping:
                try:
                    pool.sweep()
                except Exception as e:
                    logger.warning("k8s_agent_sandbox: orphan sweep failed: %s", e)
            del pool

    def _release(self, lease: "Lease", sandbox: Any) -> None:
        """The last lease on a claim here suspends it (persistent) or deletes it; never raises."""
        with self._lock:
            self._leases.discard(lease)
            still_held = any(other.claim == lease.claim and other.held for other in list(self._leases))
        if still_held:
            # Same claim, same SDK handle (SandboxClient shares it): leave both to the other lease.
            logger.info("k8s_agent_sandbox: keeping claim %s running: still in use here", lease.claim)
            return
        if not lease.persistent:
            # Through the SDK, which also closes its connection; it logs its own errors, and the
            # claim's deadline (last heartbeat + max idle) still applies.
            self._sdk.delete_sandbox(lease.claim, namespace=self.namespace)
            logger.info("k8s_agent_sandbox: deleted claim %s", lease.claim)
            return
        try:
            sandbox.close_connection()
        except Exception as e:
            logger.warning("k8s_agent_sandbox: closing the connection to %s failed: %s", lease.claim, e)
        # Keep the claim: the next lease for this task adopts it. Hermes cleans up when the
        # environment went idle, at the end of a session and at process exit: the pod goes, the
        # volume stays.
        if not self.settings.suspend_idle:
            logger.info("k8s_agent_sandbox: keeping claim %s running (persistent)", lease.claim)
            return
        try:
            if self._set_operating_mode(sandbox.sandbox_id, "Suspended"):
                logger.info("k8s_agent_sandbox: suspended %s (claim %s kept)", sandbox.sandbox_id, lease.claim)
        except Exception as e:   # the sweep retries later: this claim's heartbeat stopped
            logger.warning("k8s_agent_sandbox: suspending %s failed: %s", lease.claim, e)

    # --- Custom objects ----------------------------------------------------------------------------

    def _get(self, kind: tuple[str, str, str], name: str) -> dict:
        return self._cluster.get_namespaced_custom_object(*kind[:2], self.namespace, kind[2], name)

    def _list(self, kind: tuple[str, str, str], selector: dict[str, str]) -> list[dict]:
        result = self._cluster.list_namespaced_custom_object(
            *kind[:2], self.namespace, kind[2], label_selector=",".join(f"{k}={v}" for k, v in selector.items()))
        return result.get("items", [])

    def _patch(self, kind: tuple[str, str, str], name: str, body: dict) -> None:
        self._cluster.patch_namespaced_custom_object(*kind[:2], self.namespace, kind[2], name, body)

    def _heartbeat(self, claim: str) -> None:
        """Mark the claim as held now, and move its deletion deadline to ``max_idle`` from now.

        The deadline (``spec.lifecycle.shutdownTime``, policy Delete) is enforced by the controller,
        so a claim no Hermes process holds any more is deleted even if no Hermes process ever runs
        again; one in use never reaches it. The SDK sets it only on create, never on adoption. With
        ``max_idle`` 0 the claim gets no deadline, and one it has is removed.
        """
        now = datetime.now(timezone.utc)
        lifecycle = None   # merge patch: null removes it
        if self.settings.max_idle:
            deadline = now + timedelta(seconds=self.settings.max_idle)
            lifecycle = {"shutdownTime": _stamp(deadline), "shutdownPolicy": "Delete"}
        self._patch(CLAIMS, claim, {"metadata": {"annotations": {HEARTBEAT: _stamp(now)}},
                                    "spec": {"lifecycle": lifecycle}})

    def _set_operating_mode(self, sandbox_name: str, mode: str) -> bool:
        """Set the Sandbox's spec.operatingMode without waiting for the controller.

        True if it was changed, False if it already had that mode or is gone. The patch carries the
        UID it read, so a Sandbox replaced in between is refused rather than changed.
        """
        try:
            sandbox = self._get(SANDBOXES, sandbox_name)
        except Exception as e:
            if _is_not_found(e):
                return False
            raise
        if (sandbox.get("spec") or {}).get("operatingMode", "Running") == mode:
            return False
        self._patch(SANDBOXES, sandbox_name,
                    {"metadata": {"uid": sandbox["metadata"]["uid"]}, "spec": {"operatingMode": mode}})
        return True

    def _resume(self, claim: str) -> bool:
        """If the claim exists and its Sandbox is suspended, set it running (no wait). True if it did.

        Errors are logged, not raised: create_sandbox then reports what's wrong.
        """
        try:
            obj = self._get(CLAIMS, claim)
        except Exception as e:
            if not _is_not_found(e):   # a new claim needs no resume
                logger.warning("k8s_agent_sandbox: reading claim %s failed: %s", claim, e)
            return False
        sandbox_name = _claim_sandbox_name(obj)
        if not sandbox_name:
            return False
        try:
            # Heartbeat first, so no other process's sweep suspends it again while it starts.
            self._heartbeat(claim)
            if self._set_operating_mode(sandbox_name, "Running"):
                logger.info("k8s_agent_sandbox: resuming suspended sandbox %s (claim %s)", sandbox_name, claim)
                return True
        except Exception as e:
            logger.warning("k8s_agent_sandbox: resuming %s failed: %s", sandbox_name, e)
        return False


class Lease:
    """One environment's hold on its task's sandbox, from ``Sandboxes.acquire()`` to ``release()``.

    Holds no reference to the environment, and nothing in ``Sandboxes`` holds the Lease strongly:
    if Hermes drops the environment, the Lease goes with it and its claim stops being heartbeated.
    """

    def __init__(self, pool: Sandboxes, claim: str, persistent: bool, sandbox: Any, files: Files):
        self._pool = pool
        self.claim = claim
        self.persistent = persistent
        self._sandbox = sandbox
        self._files = files
        self._lock = threading.RLock()   # reentrant: release() may run from __del__ in any thread
        self._ready_lock = threading.Lock()   # one ready() at a time; release() doesn't wait for it

    @property
    def held(self) -> bool:
        return self._sandbox is not None

    def _held(self) -> Any:
        sandbox = self._sandbox
        if sandbox is None:
            raise Released(self.claim)
        return sandbox

    def ready(self) -> None:
        """Make the sandbox ready for a command: Hermes's files present (one sandboxd exec when
        they are), and running again if it was suspended under this lease (another process's
        sweep or cleanup), with the files copied into the new pod.

        Failures are logged, not raised: the command then fails and reports why. Raises
        ``Released`` after ``release()``.
        """
        with self._ready_lock:
            sandbox = self._held()
            try:
                self._files.ensure(sandbox)
                return
            except Exception as e:
                error = e
            try:
                if not sandbox.is_suspended():
                    logger.warning("k8s_agent_sandbox: file sync for %s failed: %s", self.claim, error)
                    return
                logger.info("k8s_agent_sandbox: %s was suspended while in use, resuming", self.claim)
                self._pool._heartbeat(self.claim)   # before resuming: no sweep suspends it again
                sandbox.resume(timeout=self._pool.settings.resume_timeout)
                self._files.ensure(sandbox, fresh=True)
            except Exception as e:
                logger.warning("k8s_agent_sandbox: resuming %s failed: %s", self.claim, e)

    def start(self, command: str, *, timeout: float) -> Any:
        """Start ``command`` (one string, run by sandboxd's ``/bin/sh -c``) and return its process
        handle once sandboxd reports the start, waiting up to ``timeout`` for that: ``events()``
        yields its output and exit events, ``close()`` cancels the stream, which kills the
        command's process tree.

        Doesn't call ``ready()``: Hermes's ``_before_execute()`` does, before its command timeout
        starts, so a resume doesn't count against it. Raises ``Released`` after ``release()``, and
        what sandboxd raises.
        """
        return self._held().commands.start(command, timeout=timeout)

    def stream_target(self) -> str:
        """``ready()``, then sandboxd's gRPC address (``host:port``) for a stream client such as
        stdio_bridge.py, resolved now (in pod-IP mode a resumed pod has a new one). Raises
        ``Released`` after ``release()``, and when the address can't be resolved."""
        self.ready()
        connector = self._held().connector
        connector.connect()
        target = connector.strategy.grpc_target
        if not target:
            raise RuntimeError(f"no gRPC address for {self.claim}")
        return target

    def release(self) -> None:
        """Let go of the sandbox: suspend it (persistent; unless K8S_AGENT_SANDBOX_SUSPEND_IDLE=false),
        delete the claim (not persistent), or leave it to another lease here on the same claim.
        Idempotent, any thread; never raises (failures are logged, the sweep or the claim's
        deadline catch up)."""
        with self._lock:
            sandbox, self._sandbox = self._sandbox, None
        if sandbox is not None:
            self._pool._release(self, sandbox)
