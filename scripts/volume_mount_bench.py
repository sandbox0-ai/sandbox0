#!/usr/bin/env python3
"""Benchmark pod-local storage and mounted Sandbox0 volumes in one sandbox pod.

The script creates a temporary SandboxTemplate with a declared volume mount,
creates a RWO SandboxVolume, claims a sandbox with that volume mounted, and
runs the benchmark inside the claimed sandbox pod. Both targets are measured in
the same pod and Python process to keep the runtime environment identical.
"""

from __future__ import annotations

import argparse
import base64
import copy
import dataclasses
import http.client
import json
import os
import statistics
import subprocess
import sys
import tempfile
import textwrap
import time
import urllib.parse
from typing import Any


REMOTE_BENCHMARK = r"""
import concurrent.futures
import dataclasses
import json
import os
import shutil
import statistics
import sys
import time


@dataclasses.dataclass
class PhaseResult:
    operations: int
    total_bytes: int
    total_seconds: float
    latencies_ms: list[float]

    def summary(self):
        if self.operations == 0 or self.total_seconds == 0:
            return {
                "operations": self.operations,
                "total_bytes": self.total_bytes,
                "total_seconds": self.total_seconds,
                "ops_per_second": 0.0,
                "mb_per_second": 0.0,
                "avg_ms": 0.0,
                "p50_ms": 0.0,
                "p95_ms": 0.0,
                "p99_ms": 0.0,
            }
        values = sorted(self.latencies_ms)
        return {
            "operations": self.operations,
            "total_bytes": self.total_bytes,
            "total_seconds": self.total_seconds,
            "ops_per_second": self.operations / self.total_seconds,
            "mb_per_second": (self.total_bytes / 1024 / 1024) / self.total_seconds,
            "avg_ms": statistics.fmean(self.latencies_ms),
            "p50_ms": percentile(values, 50),
            "p95_ms": percentile(values, 95),
            "p99_ms": percentile(values, 99),
        }


def percentile(values, pct):
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    rank = (len(values) - 1) * (pct / 100.0)
    low = int(rank)
    high = min(low + 1, len(values) - 1)
    weight = rank - low
    return values[low] * (1.0 - weight) + values[high] * weight


def phase_summary_median(round_summaries):
    keys = [
        "ops_per_second",
        "mb_per_second",
        "avg_ms",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "total_seconds",
    ]
    out = {
        "operations": round_summaries[0]["operations"] if round_summaries else 0,
        "total_bytes": round_summaries[0]["total_bytes"] if round_summaries else 0,
    }
    for key in keys:
        out[key] = statistics.median(item[key] for item in round_summaries) if round_summaries else 0.0
    return out


def build_paths(file_count, files_per_dir):
    return [
        "bench/d%04d/f%06d.bin" % (index // files_per_dir, index)
        for index in range(file_count)
    ]


def unique_dirs(root, paths):
    dirs = {os.path.dirname(os.path.join(root, path)) for path in paths}
    return sorted(path for path in dirs if path)


def prepare_dirs(root, paths):
    for path in unique_dirs(root, paths):
        os.makedirs(path, exist_ok=True)


def cleanup(path):
    shutil.rmtree(path, ignore_errors=True)


def run_parallel(paths, parallelism, worker):
    latencies = []
    total_bytes = 0
    start = time.perf_counter()

    def run_one(path):
        phase_start = time.perf_counter()
        written = worker(path)
        return written, (time.perf_counter() - phase_start) * 1000.0

    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as pool:
        for written, latency_ms in pool.map(run_one, paths):
            total_bytes += written
            latencies.append(latency_ms)

    return PhaseResult(
        operations=len(paths),
        total_bytes=total_bytes,
        total_seconds=time.perf_counter() - start,
        latencies_ms=latencies,
    )


def run_target_round(root, paths, payload, parallelism, precreate_dirs):
    cleanup(root)
    os.makedirs(root, exist_ok=True)
    if precreate_dirs:
        prepare_dirs(root, paths)

    def write_worker(relative_path):
        absolute_path = os.path.join(root, relative_path)
        if not precreate_dirs:
            os.makedirs(os.path.dirname(absolute_path), exist_ok=True)
        with open(absolute_path, "wb") as handle:
            handle.write(payload)
        return len(payload)

    write = run_parallel(paths, parallelism, write_worker)

    def read_worker(relative_path):
        absolute_path = os.path.join(root, relative_path)
        with open(absolute_path, "rb") as handle:
            data = handle.read()
        if data != payload:
            raise RuntimeError("read verification failed for %s" % relative_path)
        return len(data)

    read = run_parallel(paths, parallelism, read_worker)

    discovered = []
    start = time.perf_counter()
    latencies = []
    for directory, _, names in os.walk(root):
        for name in names:
            absolute_path = os.path.join(directory, name)
            stat_start = time.perf_counter()
            os.stat(absolute_path)
            latencies.append((time.perf_counter() - stat_start) * 1000.0)
            discovered.append(absolute_path)
    if len(discovered) != len(paths):
        raise RuntimeError("list+stat discovered %d files, expected %d" % (len(discovered), len(paths)))
    list_stat = PhaseResult(
        operations=len(discovered),
        total_bytes=0,
        total_seconds=time.perf_counter() - start,
        latencies_ms=latencies,
    )
    return {
        "write": write.summary(),
        "read": read.summary(),
        "list_stat": list_stat.summary(),
    }


def warmup(root, payload):
    cleanup(root)
    os.makedirs(root, exist_ok=True)
    path = os.path.join(root, "warmup.bin")
    with open(path, "wb") as handle:
        handle.write(payload)
    with open(path, "rb") as handle:
        if handle.read() != payload:
            raise RuntimeError("warmup verification failed")
    os.unlink(path)


def main():
    cfg = json.loads(sys.argv[1])
    paths = build_paths(cfg["file_count"], cfg["files_per_dir"])
    payload = bytes(index % 251 for index in range(cfg["file_size"]))
    run_id = str(int(time.time() * 1000))
    targets = [
        {
            "name": "pod-local-tmp",
            "path": os.path.join(cfg["local_root"], "s0-volume-bench-" + run_id),
            "backend": "localfs",
        },
        {
            "name": "mounted-s0fs-volume",
            "path": os.path.join(cfg["mount_path"], "s0-volume-bench-" + run_id),
            "backend": "s0fs",
        },
    ]

    for target in targets:
        warmup(os.path.join(target["path"], "_warmup"), payload)

    results = []
    for round_index in range(cfg["rounds"]):
        order = targets if round_index % 2 == 0 else list(reversed(targets))
        for target in order:
            root = os.path.join(target["path"], "round-%02d" % round_index)
            summary = run_target_round(
                root,
                paths,
                payload,
                cfg["parallelism"],
                cfg["precreate_dirs"],
            )
            results.append(
                {
                    "round": round_index + 1,
                    "target": target["name"],
                    "backend": target["backend"],
                    "path": target["path"],
                    "summary": summary,
                }
            )
            cleanup(root)

    grouped = {}
    for item in results:
        grouped.setdefault(item["target"], []).append(item)

    final = []
    for target in targets:
        items = grouped[target["name"]]
        final.append(
            {
                "target": target["name"],
                "backend": target["backend"],
                "path": target["path"],
                "rounds": len(items),
                "write": phase_summary_median([item["summary"]["write"] for item in items]),
                "read": phase_summary_median([item["summary"]["read"] for item in items]),
                "list_stat": phase_summary_median([item["summary"]["list_stat"] for item in items]),
            }
        )

    for target in targets:
        cleanup(target["path"])

    print(json.dumps(
        {
            "environment": {
                "cwd": os.getcwd(),
                "python": sys.version.split()[0],
                "uname": os.uname().sysname + " " + os.uname().release + " " + os.uname().machine,
            },
            "workload": cfg,
            "round_results": results,
            "results": final,
        },
        indent=2,
        sort_keys=True,
    ))


if __name__ == "__main__":
    main()
"""


@dataclasses.dataclass
class APIClient:
    base_url: str
    token: str = ""

    def request(self, method: str, path: str, body: Any | None = None) -> tuple[int, bytes]:
        parsed = urllib.parse.urlsplit(self.base_url)
        if parsed.scheme != "http":
            raise ValueError(f"only http base URLs are supported, got {parsed.scheme!r}")
        payload = b""
        headers = {"Connection": "close"}
        if body is not None:
            payload = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        conn = http.client.HTTPConnection(parsed.hostname or "127.0.0.1", parsed.port or 80, timeout=120)
        try:
            conn.request(method, parsed.path.rstrip("/") + path, body=payload, headers=headers)
            resp = conn.getresponse()
            data = resp.read()
            return resp.status, data
        finally:
            conn.close()

    def json_request(self, method: str, path: str, body: Any | None = None, expected: int | tuple[int, ...] = 200) -> Any:
        status, data = self.request(method, path, body)
        expected_values = (expected,) if isinstance(expected, int) else expected
        if status not in expected_values:
            raise RuntimeError(f"{method} {path} failed with {status}: {data.decode('utf-8', 'replace')}")
        if not data:
            return None
        return json.loads(data.decode("utf-8"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark mounted Sandbox0 volume performance in a sandbox pod.")
    parser.add_argument("--base-url", default="", help="Cluster gateway base URL. Auto-discovered when omitted.")
    parser.add_argument("--email", default="admin@example.com", help="Login email.")
    parser.add_argument("--password", default="", help="Login password. Auto-discovered from admin-password when omitted.")
    parser.add_argument("--kube-context", default="kind-sandbox0-e2e", help="Kubernetes context for kubectl.")
    parser.add_argument("--kube-namespace", default="sandbox0-system", help="Namespace for cluster services.")
    parser.add_argument("--cluster-gateway-service", default="fullmode-cluster-gateway", help="Cluster gateway service name.")
    parser.add_argument("--kind-cluster", default="sandbox0-e2e", help="Kind cluster name used for base URL discovery.")
    parser.add_argument("--template-base", default="default", help="Base SandboxTemplate to clone.")
    parser.add_argument("--template-id", default="", help="Temporary template ID. Defaults to a timestamped name.")
    parser.add_argument("--mount-path", default="/workspace/bench-volume", help="Mounted volume path inside the sandbox.")
    parser.add_argument("--local-root", default="/tmp", help="Local filesystem root inside the sandbox.")
    parser.add_argument("--container", default="", help="Optional container name for kubectl exec.")
    parser.add_argument("--file-count", type=int, default=1000, help="Number of files per round.")
    parser.add_argument("--file-size", type=int, default=4096, help="Bytes per file.")
    parser.add_argument("--parallelism", type=int, default=32, help="Worker threads inside the sandbox.")
    parser.add_argument("--files-per-dir", type=int, default=128, help="Files per benchmark directory.")
    parser.add_argument("--rounds", type=int, default=5, help="Benchmark rounds per target.")
    parser.add_argument("--precreate-dirs", action="store_true", help="Pre-create directories before timing writes.")
    parser.add_argument("--keep-resources", action="store_true", help="Do not delete created sandbox, template, or volume.")
    parser.add_argument("--json-output", default="", help="Optional path for JSON results.")
    return parser.parse_args()


def run_command(args: list[str], *, check: bool = True) -> str:
    completed = subprocess.run(args, check=check, text=True, capture_output=True)
    return completed.stdout.strip()


def kubectl(args: list[str], kube_context: str) -> str:
    cmd = ["kubectl"]
    if kube_context:
        cmd += ["--context", kube_context]
    cmd += args
    return run_command(cmd)


def discover_base_url(args: argparse.Namespace) -> str:
    if args.base_url:
        return args.base_url.rstrip("/")
    if args.kind_cluster:
        completed = subprocess.run(
            ["docker", "port", f"{args.kind_cluster}-control-plane", "30080/tcp"],
            text=True,
            capture_output=True,
            check=False,
        )
        mapping = completed.stdout.strip()
        if mapping:
            return "http://127.0.0.1:" + mapping.rsplit(":", 1)[-1].strip()
    node_port = kubectl(
        [
            "-n",
            args.kube_namespace,
            "get",
            "svc",
            args.cluster_gateway_service,
            "-o",
            "jsonpath={.spec.ports[0].nodePort}",
        ],
        args.kube_context,
    )
    return f"http://127.0.0.1:{node_port}"


def discover_password(args: argparse.Namespace) -> str:
    if args.password:
        return args.password
    encoded = kubectl(
        [
            "-n",
            args.kube_namespace,
            "get",
            "secret",
            "admin-password",
            "-o",
            "jsonpath={.data.password}",
        ],
        args.kube_context,
    )
    return base64.b64decode(encoded).decode("utf-8")


def data(resp: Any) -> Any:
    if not isinstance(resp, dict) or not resp.get("success"):
        raise RuntimeError(f"unexpected API response: {resp!r}")
    return resp.get("data")


def normalize_team_template_resources(spec: dict[str, Any]) -> None:
    main = spec.get("mainContainer")
    if not isinstance(main, dict):
        return
    resources = main.get("resources")
    if not isinstance(resources, dict):
        return
    cpu = parse_cpu_cores(resources.get("cpu"))
    if cpu is None:
        return
    memory_bytes = int(cpu * 4 * 1024 * 1024 * 1024)
    resources["memory"] = format_binary_quantity(memory_bytes)


def parse_cpu_cores(value: Any) -> float | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value:
        return None
    try:
        if value.endswith("m"):
            return float(value[:-1]) / 1000.0
        return float(value)
    except ValueError:
        return None


def format_binary_quantity(value: int) -> str:
    units = [
        ("Ti", 1024**4),
        ("Gi", 1024**3),
        ("Mi", 1024**2),
        ("Ki", 1024),
    ]
    for suffix, factor in units:
        if value >= factor and value % factor == 0:
            return f"{value // factor}{suffix}"
    return str(value)


def wait_for_template_ready(client: APIClient, template_id: str, timeout_seconds: int = 240) -> None:
    deadline = time.time() + timeout_seconds
    while True:
        tpl = data(client.json_request("GET", f"/api/v1/templates/{template_id}"))
        idle_count = ((tpl.get("status") or {}).get("idleCount"))
        if idle_count is not None and int(idle_count) >= 1:
            return
        if time.time() > deadline:
            raise TimeoutError(f"template {template_id} did not reach idleCount >= 1")
        time.sleep(5)


def find_pod_namespace(kube_context: str, pod_name: str) -> str:
    pods = json.loads(kubectl(["get", "pods", "-A", "-o", "json"], kube_context))
    matches = [
        item["metadata"]["namespace"]
        for item in pods.get("items", [])
        if item.get("metadata", {}).get("name") == pod_name
    ]
    if not matches:
        raise RuntimeError(f"pod {pod_name} not found")
    if len(matches) > 1:
        raise RuntimeError(f"pod {pod_name} matched multiple namespaces: {matches}")
    return matches[0]


def run_remote_benchmark(args: argparse.Namespace, pod_name: str, namespace: str) -> Any:
    cfg = {
        "file_count": args.file_count,
        "file_size": args.file_size,
        "parallelism": args.parallelism,
        "files_per_dir": args.files_per_dir,
        "rounds": args.rounds,
        "precreate_dirs": bool(args.precreate_dirs),
        "mount_path": args.mount_path,
        "local_root": args.local_root,
    }
    cmd = ["exec", "-n", namespace, pod_name]
    if args.container:
        cmd += ["-c", args.container]
    cmd += ["--", "python3", "-c", REMOTE_BENCHMARK, json.dumps(cfg)]
    raw = kubectl(cmd, args.kube_context)
    return json.loads(raw)


def print_markdown(result: Any) -> None:
    print()
    print("| Target | Backend | Write ops/s | Write p95 ms | Read ops/s | Read p95 ms | list+stat ops/s | list+stat p95 ms |")
    print("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for item in result["results"]:
        print(
            "| {target} | {backend} | {write_ops:.1f} | {write_p95:.2f} | {read_ops:.1f} | {read_p95:.2f} | {stat_ops:.1f} | {stat_p95:.2f} |".format(
                target=item["target"],
                backend=item["backend"],
                write_ops=item["write"]["ops_per_second"],
                write_p95=item["write"]["p95_ms"],
                read_ops=item["read"]["ops_per_second"],
                read_p95=item["read"]["p95_ms"],
                stat_ops=item["list_stat"]["ops_per_second"],
                stat_p95=item["list_stat"]["p95_ms"],
            )
        )


def main() -> int:
    args = parse_args()
    if args.file_count <= 0 or args.file_size <= 0 or args.parallelism <= 0 or args.rounds <= 0:
        raise SystemExit("file-count, file-size, parallelism, and rounds must be positive")

    base_url = discover_base_url(args)
    password = discover_password(args)
    client = APIClient(base_url=base_url)
    login = data(client.json_request("POST", "/auth/login", {"email": args.email, "password": password}))
    client.token = login["access_token"]

    template_id = args.template_id or f"volume-bench-{int(time.time())}"
    sandbox_id = ""
    volume_id = ""
    created_template = False
    try:
        base = data(client.json_request("GET", f"/api/v1/templates/{args.template_base}"))
        spec = copy.deepcopy(base["spec"])
        normalize_team_template_resources(spec)
        spec["volumeMounts"] = [
            {
                "name": "bench-volume",
                "mountPath": args.mount_path,
                "readOnly": False,
            }
        ]
        data(client.json_request("POST", "/api/v1/templates", {"template_id": template_id, "spec": spec}, expected=201))
        created_template = True
        wait_for_template_ready(client, template_id)

        volume = data(client.json_request("POST", "/api/v1/sandboxvolumes", {"access_mode": "RWO"}, expected=201))
        volume_id = volume["id"]
        claim = data(
            client.json_request(
                "POST",
                "/api/v1/sandboxes",
                {
                    "template": template_id,
                    "mounts": [
                        {
                            "sandboxvolume_id": volume_id,
                            "mount_point": args.mount_path,
                        }
                    ],
                },
                expected=201,
            )
        )
        sandbox_id = claim["sandbox_id"]
        pod_name = claim["pod_name"]
        namespace = find_pod_namespace(args.kube_context, pod_name)
        result = run_remote_benchmark(args, pod_name, namespace)
        result["sandbox0"] = {
            "base_url": base_url,
            "template_id": template_id,
            "sandbox_id": sandbox_id,
            "pod_name": pod_name,
            "pod_namespace": namespace,
            "volume_id": volume_id,
        }
        print(json.dumps(result, indent=2, sort_keys=True))
        print_markdown(result)
        if args.json_output:
            with open(args.json_output, "w", encoding="utf-8") as handle:
                json.dump(result, handle, indent=2, sort_keys=True)
        return 0
    finally:
        if not args.keep_resources:
            if sandbox_id:
                try:
                    client.json_request("DELETE", f"/api/v1/sandboxes/{sandbox_id}", expected=(200, 404))
                except Exception as exc:  # noqa: BLE001
                    print(f"warning: delete sandbox {sandbox_id} failed: {exc}", file=sys.stderr)
            if volume_id:
                deadline = time.time() + 60
                while True:
                    try:
                        client.json_request("DELETE", f"/api/v1/sandboxvolumes/{volume_id}", expected=(200, 404))
                        break
                    except Exception as exc:  # noqa: BLE001
                        if time.time() > deadline:
                            print(f"warning: delete volume {volume_id} failed: {exc}", file=sys.stderr)
                            break
                        time.sleep(2)
            if created_template:
                try:
                    client.json_request("DELETE", f"/api/v1/templates/{template_id}", expected=(200, 404))
                except Exception as exc:  # noqa: BLE001
                    print(f"warning: delete template {template_id} failed: {exc}", file=sys.stderr)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:  # noqa: BLE001
        print(textwrap.fill(f"error: {exc}", width=120), file=sys.stderr)
        raise SystemExit(1)
