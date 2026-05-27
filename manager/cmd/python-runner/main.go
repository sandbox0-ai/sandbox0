package main

import (
	"fmt"
	"os"
	osexec "os/exec"
	"syscall"
)

const pythonBootstrap = `
import asyncio
import base64
import importlib.util
import inspect
import json
import os
import sys
import threading
import traceback


def _load_handler(module_path, handler_name):
    module_path = os.path.abspath(module_path)
    module_dir = os.path.dirname(module_path)
    if module_dir not in sys.path:
        sys.path.insert(0, module_dir)

    spec = importlib.util.spec_from_file_location("sandbox0_function", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load function module: {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules["sandbox0_function"] = module
    spec.loader.exec_module(module)

    target = module
    for part in handler_name.split("."):
        target = getattr(target, part)
    if not callable(target):
        raise TypeError(f"handler is not callable: {handler_name}")
    return target


def _normalize_headers(headers):
    if headers is None:
        return {}
    if not isinstance(headers, dict):
        raise TypeError("response headers must be an object")

    out = {}
    for key, value in headers.items():
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            out[str(key)] = [str(item) for item in value]
        else:
            out[str(key)] = [str(value)]
    return out


def _encode_body(value, headers):
    if value is None:
        return ""
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, bytearray):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, str):
        return base64.b64encode(value.encode("utf-8")).decode("ascii")

    headers.setdefault("content-type", ["application/json"])
    encoded = json.dumps(value, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(encoded).decode("ascii")


def _normalize_response(value):
    if value is None:
        return {"status": 204, "headers": {}, "body_base64": ""}

    if isinstance(value, dict):
        headers = _normalize_headers(value.get("headers"))
        status = int(value.get("status", 200))
        if "body_base64" in value:
            body_base64 = str(value.get("body_base64") or "")
        else:
            body_base64 = _encode_body(value.get("body"), headers)
        return {"status": status, "headers": headers, "body_base64": body_base64}

    headers = {}
    body_base64 = _encode_body(value, headers)
    return {"status": 200, "headers": headers, "body_base64": body_base64}


def _write_json_line(stream, value):
    json.dump(value, stream, separators=(",", ":"))
    stream.write("\n")
    stream.flush()


def _split_response(value):
    if value is None:
        return 204, {}, None, ""
    if isinstance(value, dict):
        headers = _normalize_headers(value.get("headers"))
        status = int(value.get("status", 200))
        if "body_base64" in value:
            return status, headers, None, str(value.get("body_base64") or "")
        return status, headers, value.get("body"), ""
    return 200, {}, value, ""


def _is_stream_iterable(value):
    if value is None:
        return False
    if isinstance(value, (str, bytes, bytearray, dict, list, tuple)):
        return False
    return hasattr(value, "__aiter__") or hasattr(value, "__iter__")


def _encode_stream_chunk(value, headers):
    if value is None:
        return ""
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, bytearray):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, str):
        return base64.b64encode(value.encode("utf-8")).decode("ascii")
    headers.setdefault("content-type", ["application/json"])
    encoded = json.dumps(value, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(encoded).decode("ascii")


async def _iter_stream_body(body):
    if _is_stream_iterable(body):
        if hasattr(body, "__aiter__"):
            async for chunk in body:
                yield chunk
            return
        for chunk in body:
            yield chunk
        return
    yield body


async def _run_stream(handler, request, output):
    result = handler(request)
    if inspect.isawaitable(result):
        result = await result

    status, headers, body, body_base64 = _split_response(result)
    _write_json_line(output, {"type": "start", "status": status, "headers": headers})

    if body_base64:
        _write_json_line(output, {"type": "chunk", "body_base64": body_base64})
        return
    async for chunk in _iter_stream_body(body):
        encoded = _encode_stream_chunk(chunk, headers)
        if encoded:
            _write_json_line(output, {"type": "chunk", "body_base64": encoded})


class _FunctionWebSocket:
    def __init__(self, input_stream, output_stream):
        self._input = input_stream
        self._output = output_stream
        self._write_lock = threading.Lock()

    async def receive(self):
        return await asyncio.to_thread(self._receive_sync)

    def _receive_sync(self):
        line = self._input.readline()
        if not line:
            return None
        frame = json.loads(line.decode("utf-8"))
        if frame.get("type") == "close":
            return None
        message_type = frame.get("message_type", "text")
        if message_type == "binary":
            return base64.b64decode(frame.get("data_base64") or "")
        return str(frame.get("data") or "")

    async def send(self, value):
        await asyncio.to_thread(self.send_sync, value)

    def send_sync(self, value):
        if isinstance(value, (bytes, bytearray)):
            frame = {
                "type": "message",
                "message_type": "binary",
                "data_base64": base64.b64encode(bytes(value)).decode("ascii"),
            }
        else:
            frame = {
                "type": "message",
                "message_type": "text",
                "data": "" if value is None else str(value),
            }
        with self._write_lock:
            _write_json_line(self._output, frame)

    async def close(self, reason=""):
        with self._write_lock:
            _write_json_line(self._output, {"type": "close", "reason": reason})

    def __aiter__(self):
        return self

    async def __anext__(self):
        message = await self.receive()
        if message is None:
            raise StopAsyncIteration
        return message


def _handler_accepts_websocket(handler):
    try:
        signature = inspect.signature(handler)
    except (TypeError, ValueError):
        return True
    positional = [
        param for param in signature.parameters.values()
        if param.kind in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD)
    ]
    return len(positional) >= 2 or any(
        param.kind == param.VAR_POSITIONAL for param in signature.parameters.values()
    )


async def _run_websocket(handler, request, input_stream, output_stream):
    ws = _FunctionWebSocket(input_stream, output_stream)
    if _handler_accepts_websocket(handler):
        result = handler(request, ws)
    else:
        result = handler(request)
    if inspect.isawaitable(result):
        await result


def _parse_args():
    args = sys.argv[1:]
    mode = "http"
    if args and args[0] in ("--http", "--stream", "--websocket"):
        mode = args[0][2:]
        args = args[1:]
    if len(args) != 2:
        print("usage: python-runner [--http|--stream|--websocket] <module-path> <handler>", file=sys.stderr)
        return None, None, None
    return mode, args[0], args[1]


def main():
    mode, module_path, handler_name = _parse_args()
    if not mode:
        return 64

    raw = sys.stdin.buffer.readline() if mode == "websocket" else sys.stdin.buffer.read()
    request = json.loads(raw.decode("utf-8")) if raw.strip() else {}
    handler = _load_handler(module_path, handler_name)

    original_stdout = sys.stdout
    sys.stdout = sys.stderr
    try:
        if mode == "stream":
            asyncio.run(_run_stream(handler, request, original_stdout))
            return 0
        if mode == "websocket":
            asyncio.run(_run_websocket(handler, request, sys.stdin.buffer, original_stdout))
            return 0
        result = handler(request)
        if inspect.isawaitable(result):
            result = asyncio.run(result)
    finally:
        sys.stdout = original_stdout

    response = _normalize_response(result)
    json.dump(response, original_stdout, separators=(",", ":"))
    original_stdout.write("\n")
    original_stdout.flush()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        traceback.print_exc(file=sys.stderr)
        raise SystemExit(1)
`

func main() {
	argsIn := os.Args[1:]
	if len(argsIn) == 0 {
		fmt.Fprintln(os.Stderr, "usage: python-runner [--http|--stream|--websocket] <module-path> <handler>")
		os.Exit(64)
	}
	if argsIn[0] == "--http" || argsIn[0] == "--stream" || argsIn[0] == "--websocket" {
		if len(argsIn) != 3 {
			fmt.Fprintln(os.Stderr, "usage: python-runner [--http|--stream|--websocket] <module-path> <handler>")
			os.Exit(64)
		}
	} else if len(argsIn) != 2 {
		fmt.Fprintln(os.Stderr, "usage: python-runner [--http|--stream|--websocket] <module-path> <handler>")
		os.Exit(64)
	}

	pythonPath, err := osexec.LookPath("python3")
	if err != nil {
		fmt.Fprintln(os.Stderr, "python3 not found in PATH")
		os.Exit(127)
	}

	args := append([]string{pythonPath, "-c", pythonBootstrap}, argsIn...)
	env := append(os.Environ(), "PYTHONUNBUFFERED=1")
	if err := syscall.Exec(pythonPath, args, env); err != nil {
		fmt.Fprintf(os.Stderr, "exec python3: %v\n", err)
		os.Exit(127)
	}
}
