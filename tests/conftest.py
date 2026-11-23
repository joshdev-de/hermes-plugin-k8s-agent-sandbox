"""Stand-ins for the Hermes modules the plugin imports, so tests run without Hermes installed.

The repository root is the plugin package (it has an ``__init__.py``), so pytest imports it before
running any test here; with these stubs that import succeeds offline.
"""

import shlex
import sys
import types


def _module(name: str, **attrs) -> None:
    module = types.ModuleType(name)
    module.__dict__.update(attrs)
    sys.modules.setdefault(name, module)


class TerminalEnvironmentProvider:
    pass


class BaseEnvironment:
    def __init__(self, *, cwd: str, timeout: int):
        self.cwd, self.timeout = cwd, timeout

    def stop_all_processes(self):
        pass

    def _before_execute(self):
        pass


class FileSyncManager:
    """Hermes's FileSyncManager as far as the plugin relies on it: uploads what it hasn't pushed
    yet (all of it on a new instance), deletes what disappeared, swallows transport errors and
    retries them next time."""

    def __init__(self, *, get_files_fn, upload_fn, delete_fn, bulk_upload_fn=None, **kwargs):
        self._get, self._bulk, self._delete = get_files_fn, bulk_upload_fn, delete_fn
        self._synced = set()

    def sync(self, *, force: bool = False):
        current = list(self._get())
        to_upload = [(h, r) for h, r in current if r not in self._synced]
        to_delete = sorted(self._synced - {r for _, r in current})
        try:
            if to_upload:
                self._bulk(to_upload)
            if to_delete:
                self._delete(to_delete)
        except Exception:
            return
        self._synced = {r for _, r in current}


_module("agent")
_module("agent.terminal_env_provider", TerminalEnvironmentProvider=TerminalEnvironmentProvider)
_module("tools")
_module("tools.environments")
_module("tools.environments.base", BaseEnvironment=BaseEnvironment)
_module("tools.environments.file_sync", FileSyncManager=FileSyncManager,
        iter_sync_files=lambda *a, **k: [],
        quoted_rm_command=lambda paths: "rm -f " + " ".join(shlex.quote(p) for p in paths))


# Load the plugin as Hermes does: its directory as a package, so `from .sandboxes import ...` resolves.
import importlib.util  # noqa: E402
from pathlib import Path  # noqa: E402

ROOT = Path(__file__).parents[1]
# Under hermes_plugins, as Hermes names it: a top-level k8s_agent_sandbox would hide the SDK of the
# same name, which the plugin imports.
_module("hermes_plugins", __path__=[])
_spec = importlib.util.spec_from_file_location("hermes_plugins.k8s_agent_sandbox", ROOT / "__init__.py",
                                               submodule_search_locations=[str(ROOT)])
plugin = importlib.util.module_from_spec(_spec)
sys.modules["hermes_plugins.k8s_agent_sandbox"] = plugin
sys.modules["hermes_plugins"].k8s_agent_sandbox = plugin
_spec.loader.exec_module(plugin)
# pytest also imports the repository root, a package whose folder name is no module name, as
# "__init__" without a parent package; hand it the same, correctly loaded module.
sys.modules.setdefault("__init__", plugin)
