"""Hermes's files in the sandbox: ``HermesFiles.ensure()``, one idempotent "make sure they're there".

The same files Hermes's Docker/Podman backend mounts read-only (credential files, skills, cache:
tools.credential_files), copied one-way into /root/.hermes: no bulk_download_fn, so nothing the
sandbox changes flows back to Hermes. /root/.hermes is a temporary folder (not on the volume), so
every new pod starts without them.

``ensure()`` before every command costs one sandboxd exec: a probe for the marker that, if the
marker is missing (new or restarted pod), also empties the folder. Then either Hermes's
FileSyncManager pushes what changed (rate-limited, usually nothing) or a new one pushes
everything and the marker is written. The marker is a presence flag, not a per-writer token, so
several Hermes processes sharing a sandbox don't take turns copying everything.

A full copy also happens on the first ``ensure()`` of each HermesFiles and with ``fresh=True``:
FileSyncManager only knows what its own instance pushed, so a sandbox this instance didn't fill
may hold files Hermes has since removed.

The sync machinery is Hermes's own (tools.environments.file_sync, the same its remote backends
use), which is not part of the public plugin API. If a Hermes release moves it, the plugin still
loads and runs commands, only without Hermes's files in the sandbox (logged once per process).
"""

from __future__ import annotations

import io
import logging
import os
import shlex
import tarfile
import threading
from typing import Any, Callable, Optional

try:   # private Hermes API (see above): fail soft, so the backend keeps working without the files
    from tools.environments.file_sync import FileSyncManager, iter_sync_files, quoted_rm_command
except ImportError as e:
    FileSyncManager = iter_sync_files = quoted_rm_command = None
    FILE_SYNC_MISSING: Optional[str] = str(e)
else:
    FILE_SYNC_MISSING = None

logger = logging.getLogger(__name__)

WORKDIR = "/workspace"                                   # sandboxd's root: uploads are staged here
REMOTE_HERMES = "/root/.hermes"
MARKER = f"{REMOTE_HERMES}/.k8s-agent-sandbox-synced"    # present after a complete copy

_R, _M = shlex.quote(REMOTE_HERMES), shlex.quote(MARKER)
_EMPTY = f"mkdir -p {_R} && find {_R} -mindepth 1 -delete"
# Exit 0: the marker is there. Exit 3: it wasn't, and the folder is now empty. Else: failed.
_PROBE = f"[ -f {_M} ] && exit 0; {_EMPTY} && exit 3; exit 1"

_missing_warned = False


class HermesFiles:
    """Hermes's files in one sandbox, for one Lease. ``ensure()`` raises when the sandbox can't be
    reached (the caller decides whether that means it was suspended)."""

    def __init__(self, get_files: Optional[Callable[[], list[tuple[str, str]]]] = None):
        self._get_files = get_files or (lambda: iter_sync_files(REMOTE_HERMES))
        self._sync: Optional[FileSyncManager] = None
        self._lock = threading.Lock()

    def ensure(self, sandbox: Any, *, fresh: bool = False) -> None:
        if FILE_SYNC_MISSING is not None:
            _warn_missing()
            return
        with self._lock:
            if self._sync is not None and not fresh:
                result = sandbox.commands.run(_PROBE, timeout=60, command_timeout=30)
                if result.exit_code == 0:
                    self._sync.sync()   # rate-limited: changed or new files reach the sandbox
                    return
                if result.exit_code != 3:
                    raise RuntimeError(f"checking {REMOTE_HERMES} failed ({result.exit_code}): "
                                       f"{result.stderr or result.stdout}")
                logger.info("k8s_agent_sandbox: %s has no Hermes files (new or restarted pod), copying",
                            sandbox.sandbox_id)
            else:
                _check(sandbox.commands.run(_EMPTY, timeout=60, command_timeout=30), "emptying")
            self._sync = None
            sync = FileSyncManager(
                get_files_fn=self._get_files,
                upload_fn=lambda host, remote: _push(sandbox, [(host, remote)]),
                delete_fn=lambda paths: _delete(sandbox, paths),
                bulk_upload_fn=lambda pairs: _push(sandbox, pairs),
            )
            # FileSyncManager logs and rolls back a failed push; it retries on the next ensure().
            sync.sync(force=True)
            _check(sandbox.commands.run(f"touch {_M}", timeout=30, command_timeout=10), "marking")
            self._sync = sync
            logger.info("k8s_agent_sandbox: copied Hermes files into %s of %s", REMOTE_HERMES, sandbox.sandbox_id)


def _warn_missing() -> None:
    global _missing_warned
    if not _missing_warned:
        _missing_warned = True
        logger.warning("k8s_agent_sandbox: this Hermes has no compatible tools.environments.file_sync "
                       "(%s): sandboxes run without Hermes's skills, cache and credential files",
                       FILE_SYNC_MISSING)


def _check(result: Any, what: str) -> None:
    if result.exit_code != 0:
        raise RuntimeError(f"{what} {REMOTE_HERMES} failed ({result.exit_code}): {result.stderr or result.stdout}")


def _push(sandbox: Any, pairs: list[tuple[str, str]]) -> None:
    """Upload (host_path, remote_path) pairs as one gzipped tar, unpacked at / in the sandbox."""
    if not pairs:
        return
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for host_path, remote_path in pairs:
            tar.add(host_path, arcname=remote_path.lstrip("/"), recursive=False)
    # Staged under /workspace (sandboxd's file API is confined to its root), removed right after.
    staged = f".hermes-sync-{os.getpid()}-{threading.get_ident()}.tar.gz"
    path = shlex.quote(f"{WORKDIR}/{staged}")
    sandbox.files.write(staged, buf.getvalue())
    result = sandbox.commands.run(f"tar xzf {path} -C / --no-same-owner; rc=$?; rm -f {path}; exit $rc",
                                  timeout=120, command_timeout=90)
    if result.exit_code != 0:   # raising rolls the sync back, so the next cycle retries
        raise RuntimeError(f"unpacking synced files failed ({result.exit_code}): {result.stderr or result.stdout}")


def _delete(sandbox: Any, remote_paths: list[str]) -> None:
    if remote_paths:
        # Also drop folders left empty, so ~/.hermes mirrors Hermes's tree as a mount would.
        sandbox.commands.run(
            f"{quoted_rm_command(remote_paths)}; "
            f"find {_R} -mindepth 1 -type d -empty -delete 2>/dev/null; true",
            timeout=60, command_timeout=30)
