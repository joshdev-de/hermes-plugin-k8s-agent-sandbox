"""The objects Hermes owns: one ``AgentSandboxEnvironment`` per active period of a task, and a
``_SandboxdProcess`` handle per command.

Hermes keeps the environment (its env cache, file-ops cache and background processes hold it),
calls ``cleanup()`` when it is done for now, and creates a new one on the task's next tool call;
see docs/hermes-contract.md. The sandbox behind it outlives every such object: each environment
holds a ``sandboxes.Lease`` on its task's sandbox, which does everything about the sandbox itself
(running, heartbeat, Hermes's files, suspend or delete). What is left here is Hermes's side:
commands, streams and cleanup.

Imports nothing from the plugin's ``__init__``: the provider hands each environment its Lease.
"""

from __future__ import annotations

import logging
import os
import re
import shlex
import sys
import threading
from typing import Optional

from tools.environments.base import BaseEnvironment

from .files import REMOTE_HERMES, WORKDIR
from .sandboxes import Lease

logger = logging.getLogger(__name__)

# Sandbox layout the SandboxTemplate must provide (README.md), mirroring Hermes's Docker backend:
#   /workspace     persistent (volume), sandboxd --root-dir, used to stage uploads
#   /root          persistent (volume), $HOME
#   /root/.hermes  temporary folder (not on the volume): Hermes's skills, cache and credential
#                  files (files.py), where the Docker backend mounts them read-only
__all__ = ["AgentSandboxEnvironment", "REMOTE_HERMES", "WORKDIR"]

_USER_RE = re.compile(r"[a-z_][a-z0-9_-]*[$]?")   # a plain account name (as `docker exec -u` takes)
BRIDGE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stdio_bridge.py")
_bridge_paths: Optional[list[str]] = None


def _bridge_path_args() -> list[str]:
    """``--path`` arguments that let the bridge import grpc, protobuf and the SDK's generated
    ProcessService stubs (from Hermes's plugin dependency environment) without importing the SDK."""
    global _bridge_paths
    if _bridge_paths is None:
        import grpc
        import google.protobuf
        import k8s_agent_sandbox

        dirs = [os.path.dirname(os.path.dirname(grpc.__file__)),
                os.path.dirname(os.path.dirname(os.path.dirname(google.protobuf.__file__))),
                os.path.join(os.path.dirname(k8s_agent_sandbox.__file__), "_proto")]
        _bridge_paths = [arg for d in dict.fromkeys(dirs) for arg in ("--path", d)]
    return _bridge_paths


START_TIMEOUT = 60     # seconds for sandboxd to report a command started (pod still starting, ...)
SAFETY_MARGIN = 30     # Hermes kills at its own timeout; this kills a command it somehow never did


class _SandboxdProcess:
    """ProcessHandle (poll/kill/wait/stdout/returncode) for one command run with sandboxd's
    streaming start (``Lease.start()``), like the ``docker exec`` process of Hermes's Docker backend.

    A worker thread starts the command and copies its output events into a pipe as they arrive;
    Hermes's drain thread reads the other end. ``kill()`` cancels the stream: sandboxd then kills
    the command's whole process tree (SIGKILL to the process group plus a /proc walk for
    descendants, which gVisor needs), the same as when a client disconnects. ``SendSignal``
    would only reach the process group.
    """

    def __init__(self, lease: Lease, command: str, *, safety_timeout: float):
        self._lock = threading.Lock()
        self._handle = None          # SDK ProcessHandle once sandboxd reports the start
        self._killed = False
        self._returncode: Optional[int] = None
        self._done = threading.Event()
        read_fd, self._write_fd = os.pipe()
        self._stdout = os.fdopen(read_fd, "r", encoding="utf-8", errors="replace")
        self._safety_timeout = safety_timeout
        threading.Thread(target=self._worker, args=(lease, command), daemon=True,
                         name="agent-sandbox-exec").start()

    def _write(self, data: bytes) -> None:
        view = memoryview(data)
        while view:
            view = view[os.write(self._write_fd, view):]

    def _worker(self, lease: Lease, command: str) -> None:
        returncode = 1
        timer = None
        try:
            handle = lease.start(command, timeout=START_TIMEOUT)
            with self._lock:
                self._handle = handle
                killed = self._killed
            if killed:               # kill() arrived while the command was starting
                handle.close()
            else:
                timer = threading.Timer(self._safety_timeout, self._safety_kill)
                timer.daemon = True
                timer.start()
            for event in handle.events():
                if event.type == "exit":
                    returncode = event.exit_code
                elif event.data:
                    self._write(event.data)
        except Exception as e:
            if self._killed:         # our own cancel ends the stream with an error: expected
                returncode = 137     # SIGKILL, as `docker exec` reports it; Hermes sets its own code
            else:
                logger.warning("k8s_agent_sandbox: command failed in sandboxd: %s", e)
                try:
                    self._write(f"\n[k8s_agent_sandbox: {e}]\n".encode())
                except OSError:
                    pass
        finally:
            if timer is not None:
                timer.cancel()
            # Output first, then the exit status: Hermes stops draining shortly after poll()
            # reports an exit, so everything must already be in the pipe.
            try:
                os.close(self._write_fd)
            except OSError:
                pass
            self._returncode = returncode
            self._done.set()

    def _safety_kill(self) -> None:
        logger.warning("k8s_agent_sandbox: command still running %ss after start, killing it",
                       self._safety_timeout)
        self.kill()

    @property
    def stdout(self):
        return self._stdout

    @property
    def returncode(self) -> Optional[int]:
        return self._returncode

    def poll(self) -> Optional[int]:
        return self._returncode if self._done.is_set() else None

    def wait(self, timeout: Optional[float] = None) -> Optional[int]:
        self._done.wait(timeout=timeout)
        return self._returncode

    def kill(self) -> None:
        """Non-blocking: cancelling a gRPC stream doesn't wait for the server."""
        with self._lock:
            if self._done.is_set() or self._killed:
                return
            self._killed = True
            handle = self._handle
        if handle is not None:
            handle.close()


class AgentSandboxEnvironment(BaseEnvironment):
    """One Hermes task's environment, on a Lease of the task's sandbox. Commands run as
    ``bash -c`` through sandboxd.

    Hermes owns this object (its env cache, file-ops cache and background processes hold it);
    the plugin keeps no reference to it. It ends at every ``cleanup()``, and the task's next tool
    call creates a new one, on a new Lease of the same claim. When Hermes calls ``cleanup()``, and
    what it expects then, is in docs/hermes-contract.md.
    """

    # Stdin is embedded in the command as a heredoc: no file staging through the sandbox API.
    _stdin_mode = "heredoc"
    # Commands run as root and the sandbox has no privilege-elevation tool: skip Hermes's probe for one.
    _sudo_askpass_runtime_checked = True

    def __init__(self, *, cwd: str, timeout: int, lease: Lease):
        super().__init__(cwd=cwd, timeout=timeout)
        self._lease = lease
        self._persistent = lease.persistent   # Hermes's is_persistent_env reads this attribute
        self._lock = threading.Lock()

    def _before_execute(self) -> None:
        super()._before_execute()
        # Running (resumed if another process suspended it) and Hermes's files in place.
        self._lease.ready()

    def _run_bash(self, cmd_string: str, *, login: bool = False, timeout: int = 120,
                  stdin_data: str | None = None):
        if stdin_data is not None:   # execute() already embeds it (heredoc mode); direct callers too
            cmd_string = self._embed_stdin_heredoc(cmd_string, stdin_data)
        # exec: bash replaces sandboxd's `/bin/sh -c` and leads the process group sandboxd kills.
        # stdout and stderr interleaved in order (one pipe), as Hermes's other backends return
        # them. Stdin from /dev/null: sandboxd's Start keeps a stdin pipe open that nothing
        # writes, so a command reading stdin would hang instead of seeing EOF.
        shell_cmd = f"exec bash {'-l ' if login else ''}-c {shlex.quote(cmd_string)} </dev/null 2>&1"
        return _SandboxdProcess(self._lease, shell_cmd, safety_timeout=timeout + SAFETY_MARGIN)

    def exec_prefix(self, *, user: Optional[str] = None, interactive: bool = True) -> Optional[list[str]]:
        """Local argv running its remainder (``bash -c <script>``) in the sandbox, with stdout open
        and, if ``interactive``, stdin too: the plugin's stdio_bridge.py on sandboxd's gRPC
        process API, the same connection every other command uses (no Kubernetes exec access).

        The bridge passes the remaining words through unchanged and exits with the remote exit
        status; its stdin EOF closes the remote stdin. Unlike ``docker exec``, killing the bridge
        also kills the remote process tree (sandboxd does that when a stream ends); Hermes's own
        stop of a long-lived process (recorded pid) then finds it gone.

        In the sandbox, the command starts in ``/`` (``env -C /``), like ``docker exec -w /``
        (sandboxd's own cwd is its root, /workspace). ``user`` (Bot Desktop asks for ``pn``):
        ``runuser -u <user> --``, which needs the container's SETUID/SETGID and sets HOME, USER and
        LOGNAME for that user, keeping the rest of the container's environment, as ``docker exec -u``.

        None (Hermes then treats the backend as unable to stream) when there is no sandbox
        (cleaned up) or its gRPC address can't be resolved right now.
        """
        if not self._lease.held:
            return None
        try:
            target = self._lease.stream_target()   # resumes a sandbox another process suspended
            paths = _bridge_path_args()
        except Exception as e:
            logger.warning("k8s_agent_sandbox: no stream into %s now: %s", self._lease.claim, e)
            return None
        argv = [sys.executable, "-I", BRIDGE, *paths, "--target", target]
        if not interactive:
            argv.append("--no-stdin")
        argv += ["--", "env", "-C", "/"]
        if user and user not in ("root", "0"):
            if not _USER_RE.fullmatch(user):
                logger.warning("k8s_agent_sandbox: refusing stream for user %r (not an account name)", user)
                return None
            argv += ["runuser", "-u", user, "--"]
        return argv

    def cleanup(self):
        with self._lock:
            if not self._lease.held:
                return
            # Long-lived processes this environment started (Docker's cleanup does the same): stop
            # them in the sandbox (recorded pid) and their local bridge clients.
            try:
                self.stop_all_processes()
            except Exception as e:
                logger.warning("k8s_agent_sandbox: stopping long-lived processes of %s failed: %s",
                               self._lease.claim, e)
            self._lease.release()   # suspend, delete or keep; never raises
