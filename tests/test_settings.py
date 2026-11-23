"""Settings.from_env(): the one place the plugin reads its environment variables."""

from hermes_plugins.k8s_agent_sandbox.settings import Settings   # loaded by conftest.py


def test_defaults_without_variables(monkeypatch):
    for name in ("WARMPOOL", "NAMESPACE", "CONNECTION", "MAX_IDLE", "SUSPEND_IDLE",
                 "ORPHAN_IDLE", "RESUME_TIMEOUT"):
        monkeypatch.delenv(f"K8S_AGENT_SANDBOX_{name}", raising=False)
    assert Settings.from_env() == Settings()


def test_variables_are_read(monkeypatch):
    monkeypatch.setenv("K8S_AGENT_SANDBOX_WARMPOOL", "prod")
    monkeypatch.setenv("K8S_AGENT_SANDBOX_NAMESPACE", "sb")
    monkeypatch.setenv("K8S_AGENT_SANDBOX_CONNECTION", "in-cluster-pod-ip")
    monkeypatch.setenv("K8S_AGENT_SANDBOX_MAX_IDLE", "7200")
    monkeypatch.setenv("K8S_AGENT_SANDBOX_SUSPEND_IDLE", "off")
    monkeypatch.setenv("K8S_AGENT_SANDBOX_ORPHAN_IDLE", "900")
    monkeypatch.setenv("K8S_AGENT_SANDBOX_RESUME_TIMEOUT", "42")
    assert Settings.from_env() == Settings(warmpool="prod", namespace="sb", connection="in-cluster-pod-ip",
                                           max_idle=7200, suspend_idle=False, orphan_idle=900,
                                           resume_timeout=42)


def test_orphan_idle_has_a_minimum_and_zero_turns_it_off(monkeypatch):
    monkeypatch.setenv("K8S_AGENT_SANDBOX_ORPHAN_IDLE", "10")
    assert Settings.from_env().orphan_idle == 300
    monkeypatch.setenv("K8S_AGENT_SANDBOX_ORPHAN_IDLE", "0")
    assert Settings.from_env().orphan_idle == 0


def test_max_idle_has_a_minimum_and_zero_turns_it_off(monkeypatch):
    monkeypatch.setenv("K8S_AGENT_SANDBOX_MAX_IDLE", "60")
    assert Settings.from_env().max_idle == 3600
    monkeypatch.setenv("K8S_AGENT_SANDBOX_MAX_IDLE", "0")
    assert Settings.from_env().max_idle == 0


def test_blank_or_unparsable_values_fall_back_to_the_default(monkeypatch):
    monkeypatch.setenv("K8S_AGENT_SANDBOX_RESUME_TIMEOUT", "soon")
    monkeypatch.setenv("K8S_AGENT_SANDBOX_WARMPOOL", "  ")
    assert Settings.from_env().resume_timeout == 180
    assert Settings.from_env().warmpool == "hermes-backend"
