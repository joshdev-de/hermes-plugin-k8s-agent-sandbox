# The Hermes side: who owns what, and when `cleanup()` runs

This plugin implements Hermes's terminal-backend interface. This page records how Hermes uses it, so changes to the plugin keep matching what Hermes expects. References are to [Hermes Agent](https://github.com/NousResearch/hermes-agent) **v0.23.3**; check them again when raising `requires_hermes`.

## Objects and ownership

Hermes knows nothing about Kubernetes. It sees two objects:

- **`AgentSandboxProvider`**, registered once in `register()`. Hermes asks it for an environment per `task_id` (`create_environment`), and reads static properties from it (`default_cwd`, `cache_path_base`, `env_description`, `supports_streams`, …).
- **`AgentSandboxEnvironment`**, one per *active period* of a task. Hermes runs commands through it (`execute()` → `_before_execute()` + `_run_bash()`, and `exec_prefix()` for streams), then calls `cleanup()`.

**Hermes holds the environment object, not the plugin.** `create_environment()` builds it and returns it; Hermes caches it and decides when it ends:

| Holder (Hermes) | Where | Released |
| --- | --- | --- |
| `_active_environments[task_id]` | `tools/terminal_tool.py` | removed by `_unregister_env` / the idle reaper / eviction, **before** `cleanup()` is called |
| `_file_ops_cache[task_id]` (`ShellFileOperations(env)`) | `tools/file_tools.py` | cleared together with the above |
| `ProcessSession.env_ref` (background processes) | `tools/process_registry.py` | the session is settled as `lost` after `cleanup()`, but the record may keep the object alive |
| local variables (a tool call in flight, the prompt probe) | e.g. `agent/prompt_builder.py` | when the call returns |

The code is split along that line:

- **`environment.py`** holds what Hermes owns: `AgentSandboxEnvironment` and `_SandboxdProcess`, the handle for each command. It imports nothing from the plugin's `__init__.py`. An environment holds one **`Lease`** and does only Hermes's side: commands, streams (`exec_prefix`) and `cleanup()`.
- **`sandboxes.py`** does everything with the sandbox itself. `Sandboxes.acquire(task_id, persistent=...)` returns a `Lease` on the task's sandbox: running and Ready (a suspended claim is set running before it is adopted), heartbeating, with Hermes's files copied in. `lease.ready()`, called before every command (`_before_execute()`) and inside `lease.stream_target()`, makes sure that is still so: it resumes a sandbox another process suspended under it and copies the files into the new pod. Commands start through `lease.start()`, streams get sandboxd's address from `lease.stream_target()`; the SDK's Sandbox handle never leaves `sandboxes.py`. `lease.release()` suspends the sandbox (persistent), deletes the claim (not persistent), or leaves it to another lease of the same copy on the same claim. `sweep()` and `delete_session()` work on all of the plugin's claims, and the **upkeep loop** writes the heartbeats and runs the sweep. It imports nothing from Hermes.
- **`files.py`** holds `HermesFiles`, which the Lease uses to put Hermes's files into `/root/.hermes`: one idempotent `ensure()`, one sandboxd exec when the files are already there.
- **`__init__.py`** holds what the plugin owns, once per **loaded copy** of the plugin (see [below](#how-many-copies-of-the-plugin-a-process-runs)): `register()`, the provider, the `on_session_delete` hook, and `_sandboxes()`, the copy's one `Sandboxes` (Kubernetes API client, SDK client, `Settings`). `register()` starts its sweep (`start_sweeping()`); `ctx.on_unload` (`_unload`) stops it and lets go of the `Sandboxes`. The provider calls `acquire()` and hands the Lease to the environment it creates.
- **`settings.py`** holds the configuration: a frozen `Settings`, read from the environment once per loaded copy and handed to `Sandboxes`.

**References run one way: environment → Lease → Sandboxes.** Nothing holds an environment: `Sandboxes` keeps its leases in a **`weakref.WeakSet`**, which feeds the sweep's `held` set (a safety net; their heartbeats are fresh anyway) and the "still in use here" check in `release()`. So if Hermes drops an environment without `cleanup()`, it is still garbage-collected (its `__del__` calls `cleanup()`), and so is its Lease.

**Every held claim is heartbeated; the sweep runs on the same loop.** Each copy's `Sandboxes` runs one daemon thread, the **upkeep loop**, every third of the shorter of `K8S_AGENT_SANDBOX_ORPHAN_IDLE` and `K8S_AGENT_SANDBOX_MAX_IDLE`, counting only those that are on (at least 60 s). Each tick refreshes the heartbeat annotation and the deletion deadline of every held claim, whether or not commands run (background jobs, long commands, streams), and then sweeps if the copy sweeps. The loop holds the `Sandboxes` and its leases only during a tick (a weakref otherwise), runs while the copy sweeps or holds a lease, and ends otherwise; the next `acquire()` or `start_sweeping()` starts it again. A Lease keeps its `Sandboxes` alive, so the heartbeat doesn't depend on the plugin copy still being loaded. A heartbeat is what tells every orphan sweep, in any process or copy, that a claim is in use. The two settings are independent: `K8S_AGENT_SANDBOX_ORPHAN_IDLE=0` turns off the sweep, `K8S_AGENT_SANDBOX_MAX_IDLE=0` the deadline; only with both off is there no loop (and no heartbeat beyond the one at acquire).

**The Python object and the sandbox are separate things.** The object is disposable: Hermes drops it after every `cleanup()` and creates a new one on the task's next tool call. The sandbox (SandboxClaim, Sandbox, volume) lives in the cluster and is keyed by `task_id` (`sandboxes.claim_name`). So the plugin keeps no per-task state in memory. A new object, another Hermes process, or a restarted Hermes finds the same sandbox again from the `task_id` alone, and reads the rest (operating mode, heartbeat) from the cluster.

**One SDK client per copy, so one connection per claim.** `SandboxClient.create_sandbox` hands out the same `Sandbox` handle for a claim while it is open, so two leases of one copy on the same claim share it. That is why only the last lease on a claim closes the connection and suspends or deletes; deleting goes through the SDK (`delete_sandbox`), which also closes the connection it tracks.

Because Hermes may still hold the object after `cleanup()`, methods must cope with a released Lease (`lease.held` is false; `ready()`, `start()` and `stream_target()` raise `Released`). `cleanup()` and `exec_prefix()` do. `_before_execute()` and `_run_bash()` don't, because Hermes takes the object out of its cache before calling `cleanup()` and doesn't run commands on it afterwards.

## How many copies of the plugin a process runs

A Hermes process can load the plugin module **more than once**. Every load executes the module again, with its own module globals, and calls `register()`, which creates a new provider. See `hermes_cli/plugins_loader.py` (`_directory_module_name`, `_load_directory_module`, `_evict_modules`) and `PluginContext._register_scoped_provider`:

- **One copy per profile.** Provider registrations are scoped per profile (`scope_key`). A process serving several profiles, such as a multi-profile gateway, loads the plugin once per profile, as `hermes_plugins.agent_sandbox` for the first and `hermes_plugins.agent_sandbox__home_<digest>` for each further one.
- **A new copy on every reload.** A forced rediscovery (the gateway's `reload-plugins`, a plugin install, enable or update, the auto-update tick) evicts the module and its submodules from `sys.modules` and executes it again. Hermes doesn't drop existing environments on a reload: they stay in `_active_environments`, keep working, and stay tied to the copy that created them.

So everything this documentation calls per process is really **per loaded copy**: `_sandboxes_instance`, with its settings, its leases and its upkeep loop (heartbeat and sweep). What follows from that:

- **One sweep per loaded copy:** one per profile in a multi-profile process. Each one sweeps all of the plugin's claims in the namespace. That's the same situation as several Hermes processes sharing a namespace, and the heartbeats cover it: every copy heartbeats the claims it holds, so no sweep suspends a claim another copy, process or replica still holds.
- **A reload hands the sweep over.** Hermes unloads the old copy before it loads the new one (`PluginManager.discover_and_load(force=True)`), and the old copy's `on_unload` callback (`_unload`) stops its sweep and clears `_sandboxes_instance`; the new copy starts its own in `register()`. The old copy's environments stay in Hermes's cache; their leases are then the only references to the old `Sandboxes`, whose upkeep loop keeps heartbeating their claims, so the new sweep leaves them alone. When Hermes has cleaned up the last of them, the loop ends and the old `Sandboxes` is garbage-collected.
- **"Still in use here" sees only one copy.** `release()` doesn't suspend a claim that another lease of the *same* copy still holds. If two profiles share a claim (in practice only task `default`, whose claim name doesn't depend on the profile), one profile's cleanup suspends the sandbox while the other still uses it. The other profile resumes it on its next command, as it would across processes.
- **Settings come from `os.environ`, read once per loaded copy** (`Settings.from_env()` in `_sandboxes()`); a reload reads them again. The copies share whatever the process environment holds, so with the variables set on the Deployment, all copies use the same namespace, warm pool and timeouts.

## When Hermes calls `cleanup()`

Nearly all callers go through `_cleanup_env()` in `tools/terminal_tool_lifecycle.py`. Hermes passes `force_remove` only to backends whose `cleanup()` accepts it (Docker), so this plugin's `cleanup()` is always called without arguments.

| Trigger | Hermes code | Notes |
| --- | --- | --- |
| **Idle reaper**, checked every 60 s | `_cleanup_thread_worker` → `_cleanup_inactive_envs(terminal.lifetime_seconds)` | Only if there's no activity for `lifetime_seconds`, no foreground call in flight (`_calls_in_flight`) and no running background process on the environment. Calls `stop_idle()` instead only for `sandbox:` (hosted) and Docker `:ws:` keys; session keys (`session:…`) get `cleanup()`. |
| **End of every turn**, non-persistent only | `cleanup_task_resources` (`agent/chat_completion_helpers.py`) → `cleanup_vm` | Skipped when `is_persistent_env(task_id)`, which reads the environment's **`_persistent`** attribute. So with `container_persistent: false`, the plugin deletes the claim after each turn. |
| **Session close**: `AIAgent.close()` | `_close_task_resources` (`agent/client_lifecycle.py`) → `cleanup_vm(session_id)` | TUI session teardown, gateway session eviction/reset, end of a cron job, a delegated sub-agent finishing, gateway shutdown. This is **not** a session delete. |
| **Process exit** | `atexit` `_atexit_cleanup` (`tools/terminal_tool.py`) and gateway shutdown (`gateway/run_shutdown.py`) → `cleanup_all_environments` | Doesn't happen on SIGKILL, OOM or node loss. The orphan sweep covers those. |
| **Backend degraded** (`EnvironmentConnectionError`) | `_degraded_result` → `_evict_environment_for_task` | The next call creates a new environment. For a persistent task this means suspend, then resume. |
| **Prompt backend probe** | `agent/prompt_builder.py`: `task_id="prompt-backend-probe"`, runs `uname`, then `cleanup()` right away | This is why `create_environment` treats any `task_id` containing `probe` as non-persistent. |
| **Session cwd / policy changes** | `tui_gateway/session_workdir.py`, `gateway/session_policy_refresh.py` → `cleanup_vm` | Suspend, then resume on the next command. |
| **Sandbox image switch** | `hermes_cli/sandbox_image_switch.py` → `_evict_environment_for_task` | Docker-oriented, but reaches any backend. |
| **Garbage collection** | `BaseEnvironment.__del__` → `cleanup()` | Why `cleanup()` must be idempotent. |

So `cleanup()` runs many times over a session's life, possibly from different threads, and Hermes never "un-cleans" an environment. The next use is always a new `create_environment()` call. The session's end for good is a separate signal: the `on_session_delete` hook. With `reason="archive"`, the plugin keeps the sandbox.

## What `cleanup()` must do

Hermes's written contract is short:

- `BaseEnvironment.cleanup()`: *"Release backend resources (container, instance, connection). Raise only while the sandbox may still be live."*
- `website/docs/developer-guide/terminal-environment-plugin.md`: *"called on session teardown / idle reaping"*, with persistence *"mirroring the built-in cloud backends"*.
- `cleanup_stops_processes = True` (the default, which this plugin keeps): once `cleanup()` returns, the background processes started through that object are gone, and Hermes marks them `lost`.

What survives is decided by `container_persistent`, following the built-in backends:

| Backend | `persistent=True`: compute | Files | Processes | `persistent=False` |
| --- | --- | --- | --- | --- |
| Daytona | stopped | kept | gone | deleted |
| Modal | terminated | snapshot, restored on next create (`ttl=None`) | gone | deleted |
| Vercel | stopped | snapshot | gone | deleted |
| Docker (persist mode) | **left running** | kept | keep running | stopped and removed |
| **k8s_agent_sandbox** | **suspended** (pod deleted) | **volume kept** | gone | **claim deleted** |

With `persistent=True`, the expected behaviour is that **files survive, compute stops and processes die**. This plugin matches that, most closely Daytona's stop-and-resume-by-name. `K8S_AGENT_SANDBOX_SUSPEND_IDLE=false` gives Docker's behaviour (the pod keeps running).

## Where the plugin deliberately differs

- **No `sync_back`.** Modal, Daytona and Vercel pull changes under `~/.hermes` back to the host on cleanup (`FileSyncManager.sync_back`). This plugin passes no `bulk_download_fn`: the copy is one-way, like Docker's read-only mounts, so a sandbox can't write into Hermes's home. Changes the sandbox makes to those files are lost.
- **The data's lifetime is capped.** Modal snapshots with `ttl=None` because "Hermes owns its lifetime". This plugin has the controller delete a claim (and its volume) `K8S_AGENT_SANDBOX_MAX_IDLE` after a Hermes process last held it (30 days by default), even if Hermes still considers the session resumable, for example an archived one. That's a safety net against leaked claims: a Hermes killed while holding a non-persistent claim, a missed `on_session_delete`, the gateway's `default` task. The heartbeat moves the claim's `shutdownTime` forward, so a session in use is never deleted.
- **A failed suspend doesn't raise.** If suspending fails, `cleanup()` logs a warning and returns, so Hermes marks the processes `lost` while the pod may still run them. They're unreachable anyway (`_sandbox` is already `None`), and the orphan sweep suspends the pod later.
- **Work Hermes doesn't see:** the heartbeat of every held claim and the orphan sweep (for when Hermes dies without calling `cleanup()`), resuming a sandbox another process suspended while it was in use (`lease.ready()`, before every command and stream), and the deletion after `K8S_AGENT_SANDBOX_MAX_IDLE` without use.
