"""Hermes terminal backend: Kubernetes Agent Sandbox (kubernetes-sigs/agent-sandbox).

Every Hermes task gets its own sandbox pod, claimed from a SandboxWarmPool (gVisor, no Kubernetes
API access, its own /workspace volume). Hermes's terminal and file tools run there through
sandboxd; Hermes itself, its memory and its keys stay in the Hermes pod.

Modules:
  __init__.py     what the plugin owns, once per loaded copy of this module: register(), the
                  provider, the session-delete hook and the copy's Sandboxes, which sweeps from
                  register() until the copy is unloaded. A process may load several copies (one
                  per profile, a new one per plugin reload), each with its own globals:
                  docs/hermes-contract.md
  environment.py  what Hermes owns: one AgentSandboxEnvironment per active period of a task,
                  and the process handle of each command (docs/hermes-contract.md)
  sandboxes.py    everything done with SandboxClaims, Sandboxes and their pods: acquire a
                  task's sandbox (a Lease), recover, release, delete, and the upkeep loop
                  (heartbeat and orphan sweep)
  files.py        Hermes's files in the sandbox (/root/.hermes)
  settings.py     configuration (environment variables of the Hermes process, read once per copy)
  stdio_bridge.py the argv exec_prefix() hands Hermes for streams

Lifecycle of a session's sandbox:
  - created on the session's first tool call, kept when the session ends (resumable, with its files);
  - suspended (pod deleted, Sandbox and volume kept) when Hermes cleans up the environment: its idle
    reaper (terminal.lifetime_seconds without a command or running background process), the end
    of a session, or process exit. Resumed (fresh pod, same volume) when the task is used again;
  - suspended by the orphan sweep when no Hermes process has used it for K8S_AGENT_SANDBOX_ORPHAN_IDLE
    (Hermes was killed or restarted before it could clean up);
  - deleted, with its volume, when Hermes deletes or prunes the session (on_session_delete
    hook; archiving a session keeps it, since archived sessions stay resumable);
  - deleted by the controller K8S_AGENT_SANDBOX_MAX_IDLE after a Hermes process last held it (a safety
    net against claims nothing else deletes).

Requires Hermes to run inside the cluster, with a ServiceAccount allowed to create/get/list/patch/
delete SandboxClaims and get/list/patch Sandboxes in that namespace
(see README.md, "Cluster requirements").

Suspending loses what lives only in the pod: processes (desktop, Chromium, background jobs),
installed system packages and /root/.hermes (copied in again on resume). /root and /workspace are
on the volume and survive. Several Hermes processes may share a sandbox (gateway, `hermes chat`);
every claim held here gets a heartbeat annotation refreshed for as long as it is held, which is
what the orphan sweep reads, and a process whose sandbox another one suspended resumes it on
its next command.

Streams (``supports_streams``): Hermes's long-lived processes, Bot Desktop, computer_use and the
browser need a local argv that runs a process in the sandbox with stdin/stdout open, the shape of
``docker exec -i``. Here that is stdio_bridge.py, which runs the command through sandboxd's gRPC
process API (the same connection as all other commands), plus ``runuser -u <user> --`` when
Hermes asks for a user (the desktop runs as ``pn``, like ``docker exec -u pn``).
"""

from __future__ import annotations

import importlib.util
import logging
import os
import threading
from typing import Any, Optional

from agent.terminal_env_provider import TerminalEnvironmentProvider

from . import settings
from .environment import REMOTE_HERMES, WORKDIR, AgentSandboxEnvironment
from .sandboxes import Sandboxes

logger = logging.getLogger(__name__)

_sandboxes_lock = threading.Lock()
_sandboxes_instance: Optional[Sandboxes] = None


def _sandboxes() -> Sandboxes:
    """This copy's Sandboxes, on the in-cluster ServiceAccount, with the settings read now
    (created once per loaded copy; ``_unload`` lets go of it).

    Every Lease holds the Sandboxes it came from, so after a reload the old copy's environments
    keep theirs (and their heartbeats) until Hermes cleans them up.
    """
    global _sandboxes_instance
    with _sandboxes_lock:
        if _sandboxes_instance is None:
            from k8s_agent_sandbox import SandboxClient, SandboxdInClusterConnectionConfig
            from kubernetes import client, config

            from .files import HermesFiles

            config.load_incluster_config()
            current = settings.Settings.from_env()
            _sandboxes_instance = Sandboxes(
                client.CustomObjectsApi(),
                SandboxClient(connection_config=SandboxdInClusterConnectionConfig(mode=current.connection)),
                current,
                files=HermesFiles,
            )
        return _sandboxes_instance


def _unload() -> None:
    """``ctx.on_unload``: this copy stops sweeping and lets go of its Sandboxes. Its environments,
    still in Hermes's cache after a reload, hold their Leases and through them the Sandboxes, which
    keeps heartbeating their claims until Hermes cleans them up and is then collected; the next
    copy sweeps."""
    global _sandboxes_instance
    with _sandboxes_lock:
        pool, _sandboxes_instance = _sandboxes_instance, None
    if pool is not None:
        pool.stop_sweeping()


class AgentSandboxProvider(TerminalEnvironmentProvider):
    name = "k8s_agent_sandbox"
    display_name = "Agent Sandbox"
    is_remote = True
    is_container = True
    # Sandboxes are resumed by claim name; give ephemeral sessions their own.
    session_isolated_when_nonpersistent = True
    # exec_prefix(): long-lived processes, Bot Desktop, computer_use and the browser run in the sandbox.
    supports_streams = True

    @property
    def description(self) -> str:
        return "Run commands in a Kubernetes Agent Sandbox pod (one gVisor sandbox per session)."

    @property
    def default_cwd(self) -> Optional[str]:
        return WORKDIR

    @property
    def cache_path_base(self) -> Optional[str]:
        # Where synced cache files land in the sandbox: /root/.hermes, as with the Docker backend.
        return REMOTE_HERMES

    @property
    def env_description(self) -> str:
        return "a Kubernetes Agent Sandbox pod (Debian Linux, gVisor)"

    def is_available(self) -> bool:
        # Cheap and offline: the SDK is installed and we run inside a cluster.
        return (importlib.util.find_spec("k8s_agent_sandbox") is not None
                and bool(os.environ.get("KUBERNETES_SERVICE_HOST")))

    def setup_instructions(self) -> list[str]:
        return ["Runs only inside the cluster, with RBAC for SandboxClaims and Sandboxes (see the plugin's README).",
                "Set K8S_AGENT_SANDBOX_WARMPOOL / K8S_AGENT_SANDBOX_NAMESPACE if not 'hermes-backend' / 'hermes-sandboxes'."]

    def create_environment(self, *, cwd: str, timeout: int, task_id: str = "default",
                           image: Optional[str] = None, container_config: Optional[dict[str, Any]] = None,
                           **kwargs: Any):
        persistent = bool((container_config or {}).get("container_persistent", True))
        # Hermes's short-lived probes (e.g. "prompt-backend-probe", which inspects the backend for
        # the system prompt) must not keep a sandbox: delete their claim on cleanup.
        if "probe" in task_id:
            persistent = False
        return AgentSandboxEnvironment(cwd=cwd or WORKDIR, timeout=timeout,
                                       lease=_sandboxes().acquire(task_id, persistent=persistent))


def _on_session_delete(session_id: Optional[str] = None, reason: Optional[str] = None,
                       deleted_ids: Optional[list[str]] = None, **kwargs: Any) -> None:
    """Hermes deleted or pruned sessions: delete their sandboxes and volumes.

    Fired after a committed delete/prune (manual or sessions.auto_prune) and after an archive
    (reason="archive"). Archived sessions stay resumable, so archives keep their sandbox.
    Fail-open: errors are logged, never raised into Hermes.
    """
    if reason == "archive":
        return
    ids = [i for i in (deleted_ids or [session_id]) if i]
    if not ids:
        return
    try:
        pool = _sandboxes()
        for sid in ids:
            for name in pool.delete_session(sid):
                logger.info("k8s_agent_sandbox: session %s %s: deleted claim %s", sid, reason or "deleted", name)
    except Exception as e:
        logger.warning("k8s_agent_sandbox: cleaning up sandboxes of deleted sessions %s failed: %s", ids, e)


def register(ctx):
    ctx.register_terminal_environment_provider(AgentSandboxProvider())
    ctx.register_hook("on_session_delete", _on_session_delete)
    # Sweep from the start, not only once this process creates an environment: a restarted gateway
    # must also put to sleep the sandboxes its predecessor left running. Only in the cluster.
    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        try:
            _sandboxes().start_sweeping()
        except Exception as e:   # fail-open: commands report the same problem when they need it
            logger.warning("k8s_agent_sandbox: no orphan sweep: %s", e)
    # A reload or disable unloads this copy (ctx.on_unload: Hermes >= 0.22).
    ctx.on_unload(_unload)
