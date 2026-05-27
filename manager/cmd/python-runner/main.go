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


def main():
    if len(sys.argv) != 3:
        print("usage: python-runner <module-path> <handler>", file=sys.stderr)
        return 64

    raw = sys.stdin.buffer.read()
    request = json.loads(raw.decode("utf-8")) if raw.strip() else {}
    handler = _load_handler(sys.argv[1], sys.argv[2])

    original_stdout = sys.stdout
    sys.stdout = sys.stderr
    try:
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
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: python-runner <module-path> <handler>")
		os.Exit(64)
	}

	pythonPath, err := osexec.LookPath("python3")
	if err != nil {
		fmt.Fprintln(os.Stderr, "python3 not found in PATH")
		os.Exit(127)
	}

	args := []string{pythonPath, "-c", pythonBootstrap, os.Args[1], os.Args[2]}
	env := append(os.Environ(), "PYTHONUNBUFFERED=1")
	if err := syscall.Exec(pythonPath, args, env); err != nil {
		fmt.Fprintf(os.Stderr, "exec python3: %v\n", err)
		os.Exit(127)
	}
}
