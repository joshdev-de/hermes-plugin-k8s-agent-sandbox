"""In-memory stand-ins for the cluster and the agent-sandbox SDK, sharing one state.

FakeCluster    the four CustomObjectsApi calls, over a dict, plus what the controller does that
               the plugin can observe: a Sandbox set Running gets a fresh pod (empty filesystem),
               a suspended one has no pod.
FakeSdk        the SandboxClient calls the plugin makes. create_sandbox creates or adopts a claim
               in the FakeCluster; adopting a suspended claim times out, as the real one waits
               for Ready. Handles are shared per claim while active, as SandboxClient does.
FakeSandbox    a Sandbox handle: commands and files act on its pod's filesystem and fail while
               the Sandbox is suspended (no pod, sandboxd unreachable).

Commands understand only the shell the plugin's file sync sends (test -f, mkdir/find wipe,
touch, tar unpack, rm) and the environment's ``exec bash -c <script>`` wrapper; anything else
succeeds without effect.
"""

from __future__ import annotations

import copy
import io
import itertools
import shlex
import tarfile
import types

CLAIM_GROUP, CLAIMS = "extensions.agents.x-k8s.io", "sandboxclaims"
SANDBOX_GROUP, SANDBOXES = "agents.x-k8s.io", "sandboxes"
WORKSPACE = "/workspace"


class ApiError(Exception):
    def __init__(self, status):
        super().__init__(f"HTTP {status}")
        self.status = status


def _merge(dst, src):
    for k, v in src.items():
        if v is None:                 # JSON merge patch: null removes the key
            dst.pop(k, None)
        elif isinstance(v, dict) and isinstance(dst.get(k), dict):
            _merge(dst[k], v)
        else:
            dst[k] = copy.deepcopy(v)


class FakeCluster:
    def __init__(self):
        self.objects = {}   # (group, plural, name) -> object
        self.pods = {}      # Sandbox name -> {path: bytes}: the running pod's filesystem
        self._uids = itertools.count(1)

    # --- Test helpers ------------------------------------------------------------------------------

    def claim(self, name):
        return self.objects.get((CLAIM_GROUP, CLAIMS, name))

    def sandbox(self, name):
        return self.objects.get((SANDBOX_GROUP, SANDBOXES, name))

    def sandbox_of(self, claim):
        obj = self.claim(claim)
        return obj and self.sandbox(obj["status"]["sandbox"]["name"])

    def mode_of(self, claim):
        return self.sandbox_of(claim)["spec"]["operatingMode"]

    def pod_of(self, claim):
        return self.pods.get(self.sandbox_of(claim)["metadata"]["name"])

    def add_claim(self, name, *, labels=None, mode="Running", annotations=None, created=None):
        """A claim bound to its own Sandbox, as create_sandbox leaves it."""
        sandbox = f"sb-{name}"
        meta = {"name": name, "labels": dict(labels or {}), "annotations": dict(annotations or {}),
                "uid": f"claim-{next(self._uids)}"}
        if created:
            meta["creationTimestamp"] = created
        self.objects[(CLAIM_GROUP, CLAIMS, name)] = {
            "metadata": meta, "status": {"sandbox": {"name": sandbox}}}
        self.objects[(SANDBOX_GROUP, SANDBOXES, sandbox)] = {
            "metadata": {"name": sandbox, "uid": f"sb-{next(self._uids)}"},
            "spec": {"operatingMode": mode}}
        if mode == "Running":
            self.pods[sandbox] = {}
        return name

    def set_mode(self, claim, mode):
        """Another process (or its sweep) changing the Sandbox under us."""
        name = self.sandbox_of(claim)["metadata"]["name"]
        self.patch_namespaced_custom_object(SANDBOX_GROUP, "v1beta1", "ns", SANDBOXES, name,
                                            {"spec": {"operatingMode": mode}})

    # --- CustomObjectsApi ----------------------------------------------------------------------------

    def get_namespaced_custom_object(self, group, version, namespace, plural, name):
        try:
            return copy.deepcopy(self.objects[(group, plural, name)])
        except KeyError:
            raise ApiError(404) from None

    def list_namespaced_custom_object(self, group, version, namespace, plural, label_selector=""):
        wanted = dict(pair.split("=", 1) for pair in label_selector.split(",") if pair)
        items = [copy.deepcopy(o) for (g, p, _), o in self.objects.items()
                 if g == group and p == plural
                 and all((o["metadata"].get("labels") or {}).get(k) == v for k, v in wanted.items())]
        return {"items": items}

    def patch_namespaced_custom_object(self, group, version, namespace, plural, name, body):
        obj = self.objects.get((group, plural, name))
        if obj is None:
            raise ApiError(404)
        uid = (body.get("metadata") or {}).get("uid")
        if uid is not None and uid != obj["metadata"].get("uid"):
            raise ApiError(409)
        before = (obj.get("spec") or {}).get("operatingMode", "Running")
        _merge(obj, body)
        if plural == SANDBOXES:   # the controller
            after = obj["spec"].get("operatingMode", "Running")
            if before == "Suspended" and after == "Running":
                self.pods[name] = {}
            elif after == "Suspended":
                self.pods.pop(name, None)

    def delete_namespaced_custom_object(self, group, version, namespace, plural, name):
        obj = self.objects.pop((group, plural, name), None)
        if obj is None:
            raise ApiError(404)
        if plural == CLAIMS:      # the Sandbox, its pod and volume go with the claim
            sandbox = obj["status"]["sandbox"]["name"]
            self.objects.pop((SANDBOX_GROUP, SANDBOXES, sandbox), None)
            self.pods.pop(sandbox, None)


class Result:
    def __init__(self, exit_code=0, stdout="", stderr=""):
        self.exit_code, self.stdout, self.stderr = exit_code, stdout, stderr


class Unreachable(ConnectionError):
    """What a command against a suspended or deleted sandbox gets: sandboxd isn't there."""


class FakeSandbox:
    def __init__(self, sdk, claim, sandbox_id):
        self._sdk, self._cluster = sdk, sdk.cluster
        self.claim_name, self.sandbox_id = claim, sandbox_id
        self.closed = False
        self.resumed_with = None
        self.commands = types.SimpleNamespace(run=self._run, start=self._start)
        self.files = types.SimpleNamespace(write=self._write)
        # exec_prefix() resolves sandboxd's gRPC address through the connector.
        self.connector = types.SimpleNamespace(connect=self._pod,
                                               strategy=types.SimpleNamespace(grpc_target="10.0.0.7:8080"))
        self.ran = []   # every command, in order

    @property
    def is_active(self):
        return not self.closed

    # --- Lifecycle (SDK Sandbox) ---------------------------------------------------------------------

    def is_suspended(self):
        sandbox = self._cluster.sandbox(self.sandbox_id)
        return sandbox is not None and sandbox["spec"]["operatingMode"] == "Suspended"

    def resume(self, *, timeout=180):
        if self.closed:
            raise RuntimeError("Sandbox connection is closed")
        self.resumed_with = timeout
        if self.is_suspended():
            self._cluster.patch_namespaced_custom_object(
                SANDBOX_GROUP, "v1beta1", "ns", SANDBOXES, self.sandbox_id, {"spec": {"operatingMode": "Running"}})

    def close_connection(self):
        self.closed = True

    # --- Pod -----------------------------------------------------------------------------------------

    def _pod(self):
        if self.closed:
            raise RuntimeError("Sandbox connection is closed")
        pod = self._cluster.pods.get(self.sandbox_id)
        if pod is None:
            raise Unreachable(f"sandbox {self.sandbox_id} has no pod")
        return pod

    def _write(self, path, data):
        self._pod()[f"{WORKSPACE}/{path}"] = bytes(data)

    def _run(self, command, timeout=None, command_timeout=None):
        pod = self._pod()
        self.ran.append(command)
        return Result(_shell(pod, command))

    def _start(self, command, timeout=None):
        """Streaming start: a handle whose events() are the command's exit, nothing else."""
        pod = self._pod()
        self.ran.append(command)
        code = _shell(pod, command)
        exit_event = types.SimpleNamespace(type="exit", exit_code=code, data=b"")
        return types.SimpleNamespace(events=lambda: iter([exit_event]), close=lambda: None)


def _under(path, root):
    return path.startswith(root.rstrip("/") + "/")


class _Exit(Exception):
    def __init__(self, code):
        self.code = code


def _shell(pod, command):
    """The few shell statements the file sync sends, run against a pod's {path: bytes}:
    ``a && b; c`` lists, ``exit N`` (``exit $rc`` exits with the last status)."""
    rc = 0
    try:
        for statement in command.split(";"):
            for i, part in enumerate(statement.split("&&")):
                if i and rc != 0:
                    break
                rc = _command(pod, shlex.split(part.replace("2>/dev/null", "")), rc)
    except _Exit as e:
        return e.code
    return rc


def _command(pod, words, rc):
    if not words or "=" in words[0]:          # empty, or `rc=$?`
        return rc
    if words[:2] == ["exec", "bash"] and "-c" in words:   # how the environment wraps each command
        return _shell(pod, words[words.index("-c") + 1])
    if words[0] == "exit":
        raise _Exit(int(words[1]) if words[1].isdigit() else rc)
    if words[0] in ("[", "test") and words[1] == "-f":
        return 0 if words[2] in pod else 1
    if words[0] == "find" and "-delete" in words:
        if "-type" not in words:                  # empty folders aren't modelled
            for path in [p for p in pod if _under(p, words[1])]:
                del pod[path]
        return 0
    if words[0] == "touch":
        pod[words[1]] = b""
        return 0
    if words[0] == "tar":
        archive = pod.get(words[2])
        if archive is None:
            return 2
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
            for member in tar.getmembers():
                pod["/" + member.name] = tar.extractfile(member).read()
        return 0
    if words[0] == "rm":
        for path in words[2:]:
            pod.pop(path, None)
        return 0
    return 0                                      # mkdir, true, ...


class FakeSdk:
    """The SandboxClient calls the plugin makes, against a FakeCluster."""

    def __init__(self, cluster):
        self.cluster = cluster
        self.created = []       # create_sandbox kwargs, in order
        self.deleted = []       # claim names passed to delete_sandbox
        self._handles = {}      # claim -> FakeSandbox (shared while active)

    def create_sandbox(self, *, warmpool, namespace, claim_name, labels, adopt_existing,
                       shutdown_after_seconds, sandbox_ready_timeout):
        self.created.append(dict(warmpool=warmpool, namespace=namespace, claim_name=claim_name,
                                 labels=labels, adopt_existing=adopt_existing,
                                 shutdown_after_seconds=shutdown_after_seconds,
                                 sandbox_ready_timeout=sandbox_ready_timeout))
        claim = self.cluster.claim(claim_name)
        if claim is None:
            self.cluster.add_claim(claim_name, labels=labels)
        elif not adopt_existing:
            raise ApiError(409)
        elif self.cluster.mode_of(claim_name) == "Suspended":
            raise TimeoutError(f"claim {claim_name} not Ready after {sandbox_ready_timeout}s")
        sandbox_id = self.cluster.claim(claim_name)["status"]["sandbox"]["name"]
        handle = self._handles.get(claim_name)
        if handle is None or not handle.is_active:
            handle = self._handles[claim_name] = FakeSandbox(self, claim_name, sandbox_id)
        return handle

    def handle(self, claim_name):
        """The Sandbox handle last handed out for a claim (tests look at what happened to it)."""
        return self._handles.get(claim_name)

    def delete_sandbox(self, claim_name, namespace="default"):
        self.deleted.append(claim_name)
        handle = self._handles.pop(claim_name, None)
        if handle is not None:
            handle.close_connection()
        try:
            self.cluster.delete_namespaced_custom_object(CLAIM_GROUP, "v1beta1", namespace, CLAIMS, claim_name)
        except ApiError:
            pass   # the SDK logs, never raises
