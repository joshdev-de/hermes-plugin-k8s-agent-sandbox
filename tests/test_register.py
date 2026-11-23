"""register() against a fake Hermes ctx: what the plugin hands Hermes, and its copy's orphan sweep."""

import threading
import time
from pathlib import Path

import pytest

import hermes_plugins.k8s_agent_sandbox as plugin   # loaded by conftest.py, as Hermes loads it
from hermes_plugins.k8s_agent_sandbox import files, sandboxes
from hermes_plugins.k8s_agent_sandbox.settings import Settings
from fakes import FakeCluster, FakeSdk

ROOT = Path(__file__).parents[1]


class FakeCtx:
    def __init__(self):
        self.providers, self.hooks, self.unload_callbacks = [], {}, []

    def register_terminal_environment_provider(self, provider):
        self.providers.append(provider)

    def register_hook(self, name, fn):
        self.hooks[name] = fn

    def on_unload(self, callback):
        self.unload_callbacks.append(callback)


def test_register_hands_hermes_the_provider_the_session_delete_hook_and_the_unload_callback(monkeypatch):
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)   # no sweep outside a cluster
    ctx = FakeCtx()
    plugin.register(ctx)
    assert [p.name for p in ctx.providers] == ["k8s_agent_sandbox"]
    assert set(ctx.hooks) == {"on_session_delete"}
    assert ctx.unload_callbacks == [plugin._unload]
    assert not ctx.providers[0].is_available()


def test_manifest_declares_what_register_registers(monkeypatch):
    yaml = pytest.importorskip("yaml")

    class StrictLoader(yaml.SafeLoader):   # PyYAML keeps the last of duplicate keys silently
        def construct_mapping(self, node, deep=False):
            keys = [self.construct_object(k, deep=deep) for k, _ in node.value]
            assert len(keys) == len(set(keys)), f"duplicate keys in plugin.yaml: {keys}"
            return super().construct_mapping(node, deep=deep)

    manifest = yaml.load((ROOT / "plugin.yaml").read_text(), Loader=StrictLoader)
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    ctx = FakeCtx()
    plugin.register(ctx)
    assert manifest["name"] == ctx.providers[0].name
    assert isinstance(manifest["version"], str)
    assert manifest["provides_tools"] == []                       # FakeCtx has no register_tool
    assert sorted(manifest["provides_hooks"]) == sorted(ctx.hooks)


def test_archived_sessions_keep_their_sandboxes(monkeypatch):
    def no_cluster():
        raise AssertionError("must not touch the cluster")
    monkeypatch.setattr(plugin, "_sandboxes", no_cluster)
    plugin._on_session_delete(session_id="abc", reason="archive")


def upkeep_threads():
    return [t for t in threading.enumerate() if t.name == "agent-sandbox-upkeep"]


def wait_for(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not reached in time")
        time.sleep(0.005)


@pytest.fixture
def in_cluster(monkeypatch):
    """This copy's Sandboxes over the fakes, with a fast upkeep loop, as _sandboxes() would make it."""
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    cluster = FakeCluster()
    pool = sandboxes.Sandboxes(cluster, FakeSdk(cluster), Settings(orphan_idle=1800), interval=0.01,
                               files=lambda: files.HermesFiles(lambda: []))
    monkeypatch.setattr(plugin, "_sandboxes_instance", pool)
    yield cluster
    pool.stop_sweeping()
    wait_for(lambda: not upkeep_threads())


def test_register_sweeps_until_the_copy_is_unloaded(in_cluster):
    cluster = in_cluster
    cluster.add_claim("a", labels=sandboxes.MANAGED_BY, created="2000-01-01T00:00:00Z")
    ctx = FakeCtx()
    plugin.register(ctx)
    wait_for(lambda: cluster.mode_of("a") == "Suspended")

    for callback in ctx.unload_callbacks:          # a reload or disable
        callback()
    assert plugin._sandboxes_instance is None       # only its leases keep the old Sandboxes now
    wait_for(lambda: not upkeep_threads())
    cluster.add_claim("b", labels=sandboxes.MANAGED_BY, created="2000-01-01T00:00:00Z")
    time.sleep(0.05)
    assert cluster.mode_of("b") == "Running"        # the next copy sweeps, not this one


def test_register_without_a_usable_cluster_config_still_registers(monkeypatch):
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    def broken():
        raise RuntimeError("no service account token")
    monkeypatch.setattr(plugin, "_sandboxes", broken)
    ctx = FakeCtx()
    plugin.register(ctx)
    assert ctx.providers and ctx.unload_callbacks == [plugin._unload]


def test_unload_without_a_sandboxes_is_a_no_op(monkeypatch):
    monkeypatch.setattr(plugin, "_sandboxes_instance", None)
    plugin._unload()
