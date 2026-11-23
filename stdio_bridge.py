"""Stdio bridge: run a command in a sandbox through sandboxd's gRPC ProcessService, with this
process's stdin/stdout/stderr as the command's. The k8s_agent_sandbox plugin's ``exec_prefix()``
returns ``[python, -I, stdio_bridge.py, --path …, --target <host:port>, [--no-stdin], --]``, and
Hermes appends the argv to run (``bash -c <script>``), as it would after ``docker exec -i <c>``.

    stdin   read here and sent with WriteStdin; EOF here sends WriteStdin(eof)
            (--no-stdin: EOF right away, like ``docker exec`` without ``-i``)
    stdout, stderr   the Start stream's output events, written as they arrive
    exit status      the remote command's exit code (128+n when sandboxd reports a signal)

When this process dies, its gRPC stream ends and sandboxd kills the command's process tree.

Run with Hermes's own Python (-I: no environment-driven imports), so it works with the nearly
empty environment some stream clients get. Imports only grpc, protobuf and the SDK's generated
ProcessService stubs, not the SDK itself, to start quickly. Standalone: not imported by the plugin.
"""

import argparse
import os
import sys
import threading


def main() -> None:
    parser = argparse.ArgumentParser(prog="stdio_bridge")
    parser.add_argument("--path", action="append", default=[], help="prepend to sys.path")
    parser.add_argument("--target", required=True, help="sandboxd gRPC host:port")
    parser.add_argument("--no-stdin", action="store_true")
    parser.add_argument("argv", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    argv = args.argv[1:] if args.argv[:1] == ["--"] else args.argv
    if not argv:
        parser.error("no command")
    sys.path[:0] = args.path

    import grpc
    from google.protobuf import empty_pb2
    from process.v1 import process_pb2 as pb, process_pb2_grpc as pb_grpc

    channel = grpc.insecure_channel(args.target)
    stub = pb_grpc.ProcessServiceStub(channel)
    call = stub.Start(pb.StartRequest(config=pb.ProcessConfig(command=argv)))
    events = iter(call)
    try:
        first = next(events)
        if first.WhichOneof("event") != "init" or not first.init.process_id:
            raise RuntimeError("sandboxd sent no Init event")
        pid = first.init.process_id
    except (grpc.RpcError, StopIteration, RuntimeError) as e:
        detail = e.details() if isinstance(e, grpc.RpcError) else str(e)
        os.write(2, f"stdio_bridge: starting the command in the sandbox failed: {detail}\n".encode())
        os._exit(255)

    def pump_stdin() -> None:
        try:
            if not args.no_stdin:
                while True:
                    data = os.read(0, 65536)
                    if not data:
                        break
                    stub.WriteStdin(pb.WriteStdinRequest(process_id=pid, input=data))
            stub.WriteStdin(pb.WriteStdinRequest(process_id=pid, eof=empty_pb2.Empty()))
        except (grpc.RpcError, OSError):
            pass   # the command already exited, or stdin is unreadable

    threading.Thread(target=pump_stdin, daemon=True).start()

    code = 255
    try:
        for event in events:
            which = event.WhichOneof("event")
            if which == "stdout":
                os.write(1, event.stdout)
            elif which == "stderr":
                os.write(2, event.stderr)
            elif which == "exit":
                code = event.exit.exit_code
                if code < 0:          # a signal, if sandboxd reports it as -n
                    code = 128 - code
                break
    except grpc.RpcError as e:
        os.write(2, f"stdio_bridge: sandbox stream failed: {e.details()}\n".encode())
    except BrokenPipeError:
        pass   # our reader went away: exiting ends the stream, sandboxd stops the command
    # Leaving with os._exit: the stdin thread may still block on a read nobody will satisfy.
    os._exit(code & 0xFF)


if __name__ == "__main__":
    main()
