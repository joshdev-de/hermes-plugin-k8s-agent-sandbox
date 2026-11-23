"""Configuration: environment variables of the Hermes process, read once per loaded copy of the
plugin (``Settings.from_env()`` in ``_sandboxes()``); a plugin reload reads them again.

  K8S_AGENT_SANDBOX_WARMPOOL        warm pool to claim from              (default: hermes-backend)
  K8S_AGENT_SANDBOX_NAMESPACE       namespace of the pool and claims     (default: hermes-sandboxes)
  K8S_AGENT_SANDBOX_CONNECTION      in-cluster-service | in-cluster-pod-ip
                                    (default: in-cluster-service)
  K8S_AGENT_SANDBOX_MAX_IDLE        seconds after a claim was last held by any Hermes process until
                                    the controller deletes it, volume included; 0 turns the
                                    deadline off (default: 2592000 = 30 days, min. 3600)
  K8S_AGENT_SANDBOX_SUSPEND_IDLE    suspend a persistent sandbox on cleanup instead of leaving its
                                    pod running (default: true)
  K8S_AGENT_SANDBOX_ORPHAN_IDLE     seconds without a heartbeat from any Hermes process after which
                                    a running sandbox is suspended; 0 turns the sweep off
                                    (default: 1800, min. 300)
  K8S_AGENT_SANDBOX_RESUME_TIMEOUT  seconds to wait for a resumed sandbox's pod (default: 180)

Imports nothing from the plugin or from Hermes.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    warmpool: str = "hermes-backend"
    namespace: str = "hermes-sandboxes"
    connection: str = "in-cluster-service"
    max_idle: int = 2592000          # claim's shutdownTime: last heartbeat + this; 0: none
    suspend_idle: bool = True
    orphan_idle: int = 1800          # 0: no sweep
    resume_timeout: int = 180

    @classmethod
    def from_env(cls) -> "Settings":
        d = cls()
        idle = _int("K8S_AGENT_SANDBOX_ORPHAN_IDLE", d.orphan_idle)
        max_idle = _int("K8S_AGENT_SANDBOX_MAX_IDLE", d.max_idle)
        return cls(
            warmpool=_str("K8S_AGENT_SANDBOX_WARMPOOL", d.warmpool),
            namespace=_str("K8S_AGENT_SANDBOX_NAMESPACE", d.namespace),
            connection=_str("K8S_AGENT_SANDBOX_CONNECTION", d.connection),
            max_idle=max(max_idle, 3600) if max_idle > 0 else 0,
            suspend_idle=_flag("K8S_AGENT_SANDBOX_SUSPEND_IDLE", d.suspend_idle),
            orphan_idle=max(idle, 300) if idle > 0 else 0,
            resume_timeout=_int("K8S_AGENT_SANDBOX_RESUME_TIMEOUT", d.resume_timeout),
        )


def _str(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def _flag(name: str, default: bool) -> bool:
    return _str(name, "true" if default else "false").lower() not in ("0", "false", "no", "off")


def _int(name: str, default: int) -> int:
    try:
        return int(_str(name, str(default)))
    except ValueError:
        return default
