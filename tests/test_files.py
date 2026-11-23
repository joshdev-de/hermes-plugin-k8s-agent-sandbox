"""HermesFiles.ensure() against a fake sandbox pod (tests/fakes.py): what ends up in /root/.hermes."""

import pytest

from hermes_plugins.k8s_agent_sandbox import files   # loaded by conftest.py
from fakes import FakeCluster, FakeSdk, Unreachable

A, B = "/root/.hermes/skills/a.md", "/root/.hermes/b.json"


@pytest.fixture
def cluster():
    return FakeCluster()


@pytest.fixture
def sandbox(cluster):
    return FakeSdk(cluster).create_sandbox(
        warmpool="hermes-backend", namespace="ns", claim_name="c", labels={}, adopt_existing=True,
        shutdown_after_seconds=60, sandbox_ready_timeout=60)


@pytest.fixture
def host(tmp_path):
    """The host's Hermes files: {remote_path: host_path}, editable by the test."""
    mapping = {}
    for remote, text in ((A, "a"), (B, "b")):
        path = tmp_path / remote.rsplit("/", 1)[1]
        path.write_text(text)
        mapping[remote] = str(path)
    return mapping


@pytest.fixture
def hermes_files(host):
    return files.HermesFiles(lambda: [(h, r) for r, h in host.items()])


def pod(cluster):
    return {k: v for k, v in cluster.pod_of("c").items() if k.startswith("/root/.hermes/")}


def test_first_ensure_replaces_whatever_the_folder_held(cluster, sandbox, hermes_files):
    cluster.pod_of("c")["/root/.hermes/stale"] = b"x"
    hermes_files.ensure(sandbox)
    assert pod(cluster) == {A: b"a", B: b"b", files.MARKER: b""}


def test_files_in_place_cost_one_probe(cluster, sandbox, hermes_files):
    hermes_files.ensure(sandbox)
    before = len(sandbox.ran)
    hermes_files.ensure(sandbox)
    assert len(sandbox.ran) == before + 1


def test_files_removed_on_the_host_go_from_the_sandbox(cluster, sandbox, hermes_files, host):
    hermes_files.ensure(sandbox)
    del host[B]
    hermes_files.ensure(sandbox)
    assert B not in pod(cluster) and A in pod(cluster)


def test_a_pod_without_the_marker_gets_everything_again(cluster, sandbox, hermes_files):
    hermes_files.ensure(sandbox)
    cluster.pod_of("c").clear()                   # pod restarted
    hermes_files.ensure(sandbox)
    assert pod(cluster) == {A: b"a", B: b"b", files.MARKER: b""}


def test_fresh_copies_everything_even_with_the_marker_present(cluster, sandbox, hermes_files):
    hermes_files.ensure(sandbox)
    cluster.pod_of("c")["/root/.hermes/stale"] = b"x"
    hermes_files.ensure(sandbox, fresh=True)
    assert "/root/.hermes/stale" not in pod(cluster)


def test_an_unreachable_sandbox_raises(cluster, sandbox, hermes_files):
    cluster.set_mode("c", "Suspended")
    with pytest.raises(Unreachable):
        hermes_files.ensure(sandbox)


def test_a_failed_copy_is_retried_on_the_next_ensure(cluster, sandbox, hermes_files, monkeypatch):
    real_write = sandbox.files.write
    def upload_down(path, data):
        raise OSError("upload failed")
    monkeypatch.setattr(sandbox.files, "write", upload_down)
    hermes_files.ensure(sandbox)                  # logged by FileSyncManager, not raised
    assert A not in pod(cluster)
    monkeypatch.setattr(sandbox.files, "write", real_write)
    hermes_files.ensure(sandbox)
    assert pod(cluster)[A] == b"a"


def test_without_hermess_file_sync_commands_run_without_the_files(cluster, sandbox, monkeypatch, caplog):
    """A Hermes release that moves the private file_sync module: no copy, one warning, no error."""
    monkeypatch.setattr(files, "FILE_SYNC_MISSING", "No module named 'tools.environments.file_sync'")
    monkeypatch.setattr(files, "_missing_warned", False)
    def no_commands(*a, **k):
        raise AssertionError("must not touch the sandbox")
    monkeypatch.setattr(sandbox.commands, "run", no_commands)
    with caplog.at_level("WARNING"):
        files.HermesFiles(lambda: []).ensure(sandbox)
        files.HermesFiles(lambda: []).ensure(sandbox, fresh=True)
    assert len([r for r in caplog.records if "file_sync" in r.getMessage()]) == 1   # once per process
    assert A not in pod(cluster)
