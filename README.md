# hermes-plugin-k8s-agent-sandbox

A terminal backend for [Hermes Agent](https://github.com/NousResearch/hermes-agent) that runs Hermes's terminal, file and code-execution tools in [Kubernetes Agent Sandbox](https://github.com/kubernetes-sigs/agent-sandbox) pods: **one gVisor sandbox per Hermes session**, claimed from a warm pool.

Hermes keeps its memory, sessions, model keys and ServiceAccount. Model-written commands only ever run in a sandbox, which has no Kubernetes API access and none of Hermes's keys.

```
Hermes pod (namespace hermes)                      sandbox namespace (hermes-sandboxes)
  memory, sessions, config, keys                     SandboxWarmPool: ready gVisor pods
  model calls stay here           ── SDK/gRPC ──►    SandboxClaim per session ──► pod (image + sandboxd)
```

The plugin registers as `k8s_agent_sandbox`, so Hermes uses it with `terminal.backend: k8s_agent_sandbox`.

**Features**

- A sandbox per session, with its own persistent `/workspace` and `/root` volume. A resumed session gets its sandbox and files back.
- Output streams into Hermes as it is produced; interrupts and timeouts kill the command's whole process tree.
- Streams (`exec_prefix`) for long-lived processes, Bot Desktop, computer_use and the browser, over sandboxd's gRPC API. No `kubectl` and no `pods/exec` permission needed.
- Hermes's skills, cache and credential files are copied into the sandbox (one way, never persisted), as the Docker backend mounts them.
- Idle sandboxes are suspended (pod deleted, volume kept) and resumed on next use. An orphan sweep catches sandboxes left behind by a Hermes process that died.
- Deleting a session in Hermes deletes its sandbox and volume.

## Prerequisites

| | Version | Notes |
| --- | --- | --- |
| Kubernetes | any current release | Hermes must run **inside** the cluster (the plugin uses the in-cluster ServiceAccount). |
| gVisor | | A `RuntimeClass` for gVisor (`runsc`) on the nodes that run sandboxes. The only sandbox runtime tested and supported so far (see below). |
| Agent Sandbox | v1.0.12 | Controller **and** extensions (`sandbox-with-extensions.yaml`). |
| Hermes Agent | ≥ 0.23.0 | Tested with v0.23.3. |
| Storage | | A StorageClass for the per-sandbox volume (e.g. `local-path`). |
| NetworkPolicy | optional | A CNI that enforces NetworkPolicy, if you want to isolate the sandboxes (recommended). |

The Python dependency `k8s-agent-sandbox[grpc]==1.0.12` is installed with the plugin. Keep it at the same version as the Agent Sandbox controller.

**Sandbox runtimes:** only gVisor is tested and supported for now. Agent Sandbox can run sandboxes with other runtimes too, such as Kata Containers, and the plugin may well work with them, but the example template and the security model here (commands run as root, isolated by gVisor's own kernel) assume gVisor. Support for other runtimes under Agent Sandbox can be added: requests and contributions are welcome as [issues](https://github.com/joshdev-de/hermes-plugin-k8s-agent-sandbox/issues) and pull requests.

## Setup

Example manifests for every step are in [`examples/kubernetes/`](examples/kubernetes/). They use the namespaces `hermes` (Hermes) and `hermes-sandboxes` (sandboxes), a Hermes ServiceAccount `hermes`, and a SandboxTemplate and warm pool `hermes-backend`, which match the plugin's defaults (see [Configuration](#configuration) to change them). Review them before applying, in particular the StorageClass, the image tags and the egress CIDRs.

| File | Contents |
| --- | --- |
| [`00-namespaces.yaml`](examples/kubernetes/00-namespaces.yaml) | Namespaces `hermes` and `hermes-sandboxes` |
| [`01-runtimeclass.yaml`](examples/kubernetes/01-runtimeclass.yaml) | gVisor RuntimeClass (`handler: runsc`) |
| [`02-sandbox-template.yaml`](examples/kubernetes/02-sandbox-template.yaml) | SandboxTemplate for the per-session sandboxes, with its NetworkPolicy rules (Hermes → sandboxd, egress to DNS and the internet) |
| [`03-sandbox-warmpool.yaml`](examples/kubernetes/03-sandbox-warmpool.yaml) | SandboxWarmPool with two ready sandboxes |
| [`04-hermes-rbac.yaml`](examples/kubernetes/04-hermes-rbac.yaml) | ServiceAccount, Role and RoleBinding for Hermes |
| [`05-hermes-deployment.yaml`](examples/kubernetes/05-hermes-deployment.yaml) | Hermes Deployment that installs the plugin in an init container |
| [`kustomization.yaml`](examples/kubernetes/kustomization.yaml) | All of the above |

### 1. Install the Agent Sandbox controller

```bash
kubectl apply -f https://github.com/kubernetes-sigs/agent-sandbox/releases/download/v1.0.12/sandbox-with-extensions.yaml
```

Wait until its CRDs exist before the next step.

### 2. Apply the manifests

```bash
kubectl apply -k examples/kubernetes
```

What the pieces must provide, if you write your own:

- **RuntimeClass:** the handler your gVisor installation registers with containerd (usually `runsc`).
- **SandboxTemplate:**
  - `service: true`, so every sandbox gets a stable Service name the plugin connects to.
  - **sandboxd** as the container's entrypoint: REST on 8080 (with `/v1/health`), gRPC on 9090, `--root-dir=/workspace`. The example injects the sandboxd binary with an init container, so any image works unchanged.
  - The same layout as Hermes's Docker backend: `/workspace` and `/root` (`HOME`) on a persistent volume, `/root/.hermes` an `emptyDir` the plugin fills.
  - Tools in the image: `bash`, `tar`, `find`, `env -C`, and `runuser` (for the desktop user). Hermes's own sandbox image, `nousresearch/hermes-sandbox:<hermes version>-desktop`, has all of them plus the desktop stack (TigerVNC, Xfce, Chromium, cua-driver).
  - `automountServiceAccountToken: false`, so sandboxes get no Kubernetes API access.
- **RBAC:** in the sandbox namespace, Hermes's ServiceAccount needs `create, get, list, watch, patch, delete` on `sandboxclaims` (`extensions.agents.x-k8s.io`) and `get, list, watch, patch` on `sandboxes` (`agents.x-k8s.io`). Nothing else.
- **Network:** the Hermes pod must reach each sandbox on 8080 and 9090. The example lets the Agent Sandbox controller manage the NetworkPolicy (`networkPolicyManagement: Managed` with custom `networkPolicy` rules, which replace the controller's default; that default only admits `sandbox-router`, so the plugin couldn't connect). With `Unmanaged`, write the equivalent policies yourself. Streams, the browser's DevTools connection and the desktop are all relayed over gRPC, so no other port is needed. Sandboxes never call the model, so their egress can be limited to whatever the agent's commands need (or nothing).

### 3. Install the plugin in Hermes

[`05-hermes-deployment.yaml`](examples/kubernetes/05-hermes-deployment.yaml) does this in an init container. To install it by hand instead:

```bash
repo=https://github.com/joshdev-de/hermes-plugin-k8s-agent-sandbox.git
sha=$(git ls-remote "$repo" 'refs/tags/v0.1.0^{}' | cut -f1)   # the release's commit
hermes plugins install "$repo" --ref "$sha" --yes-deps --enable
```

`--ref` takes only a full 40-character commit SHA, not a tag, so the first line looks up the commit of the release tag. Without `--ref`, Hermes installs the latest commit on `main`.

Restart Hermes afterwards.

### 4. Configure Hermes

In Hermes's `config.yaml`:

```yaml
terminal:
  backend: k8s_agent_sandbox
  cwd: /workspace
  container_persistent: true   # a session keeps its sandbox and files
  lifetime_seconds: 900        # idle this long (no command, no background process): sandbox suspended
```

### Check it

Start a session and let the model run a command, e.g.:

```bash
hermes chat -q "Use the terminal tool to run: uname -r && id && pwd"
```

It should report a gVisor kernel (`4.19.0-gvisor`), `uid=0(root)` and `/workspace`. In the cluster:

```bash
kubectl -n hermes-sandboxes get sandboxclaims
kubectl -n hermes-sandboxes get sandboxes -o custom-columns=NAME:.metadata.name,MODE:.spec.operatingMode,READY:.status.conditions[0].status
```

## Configuration

Environment variables of the Hermes process, read once per loaded copy of the plugin (a plugin reload reads them again):

| Variable | Default | Meaning |
| --- | --- | --- |
| `K8S_AGENT_SANDBOX_WARMPOOL` | `hermes-backend` | SandboxWarmPool to claim from |
| `K8S_AGENT_SANDBOX_NAMESPACE` | `hermes-sandboxes` | namespace of the warm pool and claims |
| `K8S_AGENT_SANDBOX_CONNECTION` | `in-cluster-service` | `in-cluster-service` (sandbox Service DNS) or `in-cluster-pod-ip` |
| `K8S_AGENT_SANDBOX_MAX_IDLE` | `2592000` (30 days) | seconds after a Hermes process last held a claim until the controller deletes it, volume included; `0` = off (an existing deadline is removed when the claim is next held), minimum 3600 |
| `K8S_AGENT_SANDBOX_SUSPEND_IDLE` | `true` | suspend a persistent sandbox when Hermes cleans up its environment |
| `K8S_AGENT_SANDBOX_ORPHAN_IDLE` | `1800` | seconds without a heartbeat before the orphan sweep suspends a sandbox; `0` = off, minimum 300 |
| `K8S_AGENT_SANDBOX_RESUME_TIMEOUT` | `180` | seconds to wait for a resumed sandbox's pod |

Claims carry the labels `app.kubernetes.io/managed-by: hermes-k8s-agent-sandbox` and `hermes-k8s-agent-sandbox-session: <hash of the session id>`, and the heartbeat annotation `hermes-k8s-agent-sandbox-heartbeat`.

## How it works

### Commands

Hermes's terminal-backend interface (`TerminalEnvironmentProvider`) asks for an environment per task. The plugin:

1. claims a sandbox named `hermes-<task>-<hash>` from the warm pool, labelled with a hash of the session id. It uses `adopt_existing`, so a resumed session reattaches to its own sandbox and files;
2. runs each command as `exec bash -c '…' </dev/null 2>&1` through sandboxd's streaming process API, and gives Hermes a process handle like the Docker backend's `docker exec` process:
   - **Output streams** into Hermes as sandboxd sends it. stdout and stderr share one pipe, so they stay in order.
   - **Interrupts and timeouts kill the command:** cancelling the stream makes sandboxd kill the whole process tree (SIGKILL to the process group, plus a `/proc` walk for descendants, which gVisor needs). Hermes returns its usual result: the output so far plus `[Command interrupted]` (exit 130) or `[Command timed out after Ns]` (exit 124).
   - **Safety net:** a command still running 30 s past its timeout is killed by the plugin itself.
   - **Stdin:** Hermes embeds stdin as a heredoc; otherwise stdin is `/dev/null`, so a command reading it gets EOF instead of hanging.
   - Background jobs (`nohup … &`) outlive a command that exits normally, as with `docker exec`;
3. copies Hermes's files into the sandbox (see [Hermes's files](#hermess-files-in-the-sandbox));
4. on cleanup, keeps the claim for sessions (`container_persistent: true`) and suspends its sandbox, or deletes the claim for Hermes's short internal probes.

It connects to each sandbox's Service directly, so no sandbox-router is needed.

### Streams: long-lived processes, Bot Desktop, computer_use, browser

Some Hermes features need a process in the sandbox with stdin and stdout kept open: long-lived processes (`start_process`/`attach_process`/`stop_process`), the Bot Desktop screen, computer_use (cua-driver's MCP server over stdio) and the browser (Chromium on the sandbox screen). With Docker, Hermes uses `docker exec -i`. The plugin sets `supports_streams = True` and implements `exec_prefix()`, so `bot_desktop.placement: auto` puts the desktop into the session's sandbox. (This needs `plugins.isolation: in_process`, the default.)

- **Transport:** the prefix runs the plugin's `stdio_bridge.py` with Hermes's own Python, followed by Hermes's `bash -c <script>`. The bridge uses sandboxd's gRPC process API, the same connection every other command takes: local stdin goes to `WriteStdin`, local EOF closes the remote stdin, stdout and stderr stream back, and the bridge exits with the remote exit code (128+n for a signal). It imports only grpc, protobuf and the SDK's generated stubs, so a round trip takes about 0.2 s.
- **Working directory:** `env -C /` starts commands in `/`, as `docker exec -w /` does.
- **Environment:** some stream clients start with an almost empty environment, so the argv carries everything the bridge needs (the gRPC address and the plugin's dependency paths).
- **Ports:** Hermes forwards ports such as Chromium's DevTools over the stream, so nothing else in the sandbox has to be reachable.
- **Desktop user:** like `docker exec -u pn`, Bot Desktop asks for its desktop user, and the prefix then ends in `runuser -u pn --`. This works with the template's SETUID/SETGID capabilities under gVisor. Other commands run as root.
- **Stopping:** when the bridge exits or is killed, sandboxd kills the remote process tree. That's stricter than `docker exec`. Processes detached with `setsid` (the desktop launcher, Chromium) survive a normal exit, as with Docker. Environment cleanup stops all long-lived processes.

The bridge depends on a generated part of the SDK (`k8s_agent_sandbox/_proto`) that is not public API; check it when upgrading the SDK.

### Hermes's files in the sandbox

The Docker backend mounts credential files (registered by skills or config), the skills directories and cache directories into the container. Remote backends copy the same files with Hermes's `FileSyncManager`. The plugin does the same, into `/root/.hermes`:

- **Full copy** when an environment starts, and whenever the sandbox pod has restarted (the `emptyDir` then comes back empty; the plugin leaves a marker file after each full copy and checks for it before every command). The set goes over as one gzipped tar, staged under `/workspace` and deleted after unpacking.
- **Incremental** before each command: changed and new files are sent, deleted ones removed.
- **One way:** nothing flows back to Hermes. The sandbox can modify its copies, but the next full copy resets them.
- **Never persisted:** the copies, including credential files, never land on the volume.

### Idle sandboxes sleep

Agent Sandbox can suspend a sandbox: `spec.operatingMode: Suspended` deletes the pod and keeps the Sandbox, its Service name and its volume; `Running` starts a fresh pod on the same volume.

- **Suspend on cleanup:** Hermes cleans up an environment from its idle reaper (no command and no background process for `terminal.lifetime_seconds`), at the end of a session, and at process exit. For a persistent task the plugin then suspends the sandbox, freeing the pod and anything running in it (desktop, Chromium). Set `K8S_AGENT_SANDBOX_SUSPEND_IDLE=false` to keep pods running instead.
- **Orphan sweep:** a Hermes process that dies without cleaning up leaves its sandboxes running. Every claim a Hermes process holds gets its heartbeat annotation refreshed, from the environment's start until Hermes cleans it up, whether or not commands run. A sweep in each Hermes process (one per loaded copy of the plugin, e.g. per profile, on the same thread as the heartbeat; stopped when Hermes unloads that copy; see [`docs/hermes-contract.md`](docs/hermes-contract.md#how-many-copies-of-the-plugin-a-process-runs)) suspends running sandboxes of the plugin's claims whose heartbeat is older than `K8S_AGENT_SANDBOX_ORPHAN_IDLE`. Warm-pool sandboxes have no claim and are never touched.
- **Resume:** when an environment starts on a suspended claim, the plugin refreshes the heartbeat, sets the Sandbox to `Running` and waits for it to be ready (`K8S_AGENT_SANDBOX_RESUME_TIMEOUT`). Hermes's files are then copied in as after any pod restart. In practice a resume adds a few seconds to the first command.
- **Suspended while in use:** several Hermes processes can share a claim (the gateway and a `hermes chat` on the same task), and one's cleanup suspends it for the other. The other's next command notices, resumes the sandbox and runs.
- **What survives:** `/root` and `/workspace`. Lost, as after any pod restart: processes and installed system packages.

Hibernation with memory kept is not supported: the SDK only offers it as a GKE extension.

### Session lifecycle

- **Created** on the session's first tool call.
- **Suspended** when Hermes cleans up the environment, or by the orphan sweep.
- **Resumed** when the session is used again, with its files.
- **Kept** when a session is archived (archived sessions stay resumable).
- **Deleted**, with its volume, when Hermes deletes or prunes the session, and at the latest `K8S_AGENT_SANDBOX_MAX_IDLE` after a Hermes process last held it. Every heartbeat moves the claim's `spec.lifecycle.shutdownTime` forward, so a session in use never reaches it. This is independent of the orphan sweep: either can be turned off without the other.

### The Hermes side

[`docs/hermes-contract.md`](docs/hermes-contract.md) describes what Hermes expects from the backend: Hermes, not the plugin, holds each environment object; every point at which Hermes calls `cleanup()`; what `cleanup()` must keep or release, compared with Hermes's built-in backends; and where this plugin deliberately differs.

## Running the tests

The tests run offline: no cluster, no Hermes install. `tests/conftest.py` stubs the few Hermes modules the plugin imports and loads the plugin the way Hermes does, and `tests/fakes.py` stands in for the cluster and the agent-sandbox SDK. With [uv](https://docs.astral.sh/uv/), from the repository root:

```sh
uv run --with pytest pytest -q
```

That installs the plugin's dependencies into `.venv` and adds pytest just for the run. Run one file or test the usual pytest way, e.g. `uv run --with pytest pytest tests/test_sandboxes.py -k release`.

## Limitations

- **Copying Hermes's files uses internal Hermes code.** Which files go into `/root/.hermes`, and keeping them in step, comes from Hermes's `tools.environments.file_sync`, the code its own remote backends use. It is not part of Hermes's public plugin API, so `hermes plugins validate` warns about it, and a Hermes release may change it. If it disappears, the plugin still loads and runs commands, but without Hermes's skills, cache and credential files in the sandbox, and logs a warning once per process. Check that the files still arrive after each Hermes upgrade (`ls /root/.hermes` in a sandbox).

- **Bot Desktop state is per profile in Hermes, but sandboxes are per session.** Inside a session, the browser and computer_use use that session's sandbox and desktop. But when the gateway looks at "the" desktop outside any session (Desktop pane, status, idle auto-stop), it uses task `default` and creates an extra sandbox, `hermes-default-<hash>`, with no screen and no session label. So the Desktop pane doesn't show a session's screen. That sandbox is suspended like any other and deleted `K8S_AGENT_SANDBOX_MAX_IDLE` after its last use.
- Each stream or probe starts a Python process (about 0.2 s per round trip), and Bot Desktop runs several probes when it starts.
- A stream started right after another process suspended the sandbox, before any command repaired it, fails once; the next command resumes the sandbox.

