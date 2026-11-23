# Glossary

Terms this plugin's code and docs use with a fixed meaning. Hermes's own terms (task, session, environment, provider) follow Hermes; see [`docs/hermes-contract.md`](docs/hermes-contract.md).

**Claim** — a SandboxClaim, one per Hermes task (`sandboxes.claim_name(task_id)`). It binds a Sandbox from the warm pool, and its volume lives as long as the claim does.

**Lease** — one environment's hold on its task's sandbox, from `Sandboxes.acquire()` to `release()`. The environment holds it; `Sandboxes` only knows it weakly, so a dropped environment takes its Lease with it.

**Loaded copy** — one execution of the plugin module in a Hermes process, with its own globals and its own `Sandboxes`. A process runs one per profile, and a plugin reload makes a new one. What the docs call "per process" is really per loaded copy.

**Heartbeat** — the `hermes-k8s-agent-sandbox-heartbeat` annotation on a claim: the last time a loaded copy held it. Written for every held claim, whether or not commands run, together with the claim's deletion deadline (`spec.lifecycle.shutdownTime`): `K8S_AGENT_SANDBOX_MAX_IDLE` from then.

**Orphan sweep** — suspending running sandboxes of the plugin's claims whose heartbeat is older than `K8S_AGENT_SANDBOX_ORPHAN_IDLE`: sandboxes Hermes left behind when it died without cleaning up. Every loaded copy sweeps from `register()` until it is unloaded.

**Upkeep loop** — the one thread per loaded copy that writes the heartbeats and runs the orphan sweep, every third of the shorter of `K8S_AGENT_SANDBOX_ORPHAN_IDLE` and `K8S_AGENT_SANDBOX_MAX_IDLE`, counting only those that are on (at least 60 s; no loop when both are 0). It runs while the copy sweeps or holds a lease; an unloaded copy keeps heartbeating its leases' claims until Hermes cleans them up.

**Suspend / resume** — setting a Sandbox's `operatingMode` to `Suspended` (pod deleted, volume kept) or back to `Running` (a fresh pod on the same volume).
