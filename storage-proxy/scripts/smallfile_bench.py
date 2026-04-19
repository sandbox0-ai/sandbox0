#!/usr/bin/env python3

import argparse
import dataclasses
import http.client
import json
import os
import queue
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
from typing import Callable, Optional


@dataclasses.dataclass
class PhaseResult:
    operations: int
    total_bytes: int
    total_seconds: float
    latencies_ms: list[float]

    def summary(self) -> dict[str, float]:
        if self.operations == 0:
            return {
                "operations": 0,
                "total_bytes": 0,
                "total_seconds": 0.0,
                "ops_per_second": 0.0,
                "mb_per_second": 0.0,
                "avg_ms": 0.0,
                "p50_ms": 0.0,
                "p95_ms": 0.0,
                "p99_ms": 0.0,
            }

        sorted_latencies = sorted(self.latencies_ms)
        return {
            "operations": self.operations,
            "total_bytes": self.total_bytes,
            "total_seconds": self.total_seconds,
            "ops_per_second": self.operations / self.total_seconds if self.total_seconds else 0.0,
            "mb_per_second": (self.total_bytes / 1024 / 1024) / self.total_seconds if self.total_seconds else 0.0,
            "avg_ms": statistics.fmean(self.latencies_ms),
            "p50_ms": percentile(sorted_latencies, 50),
            "p95_ms": percentile(sorted_latencies, 95),
            "p99_ms": percentile(sorted_latencies, 99),
        }


@dataclasses.dataclass
class BenchmarkResult:
    name: str
    write: PhaseResult
    read: PhaseResult
    volume_id: Optional[str] = None
    backend_type: Optional[str] = None

    def summary(self) -> dict[str, object]:
        return {
            "name": self.name,
            "volume_id": self.volume_id,
            "backend_type": self.backend_type,
            "write": self.write.summary(),
            "read": self.read.summary(),
        }


def percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (len(sorted_values) - 1) * (pct / 100.0)
    low = int(rank)
    high = min(low + 1, len(sorted_values) - 1)
    weight = rank - low
    return sorted_values[low] * (1.0 - weight) + sorted_values[high] * weight


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark small-file workloads across local filesystem and s0fs volumes."
    )
    parser.add_argument("--base-url", default="", help="Cluster gateway base URL. Auto-discovered when omitted.")
    parser.add_argument("--email", default="admin@example.com", help="Login email for cluster-gateway.")
    parser.add_argument("--password", default="", help="Login password. Auto-discovered from admin-password secret when omitted.")
    parser.add_argument("--kind-cluster", default="", help="Kind cluster name to export kubeconfig from before kubectl calls.")
    parser.add_argument("--kube-namespace", default="sandbox0-system", help="Namespace for cluster services.")
    parser.add_argument("--cluster-gateway-service", default="fullmode-cluster-gateway", help="Cluster gateway service name.")
    parser.add_argument("--file-count", type=int, default=5000, help="Number of files per benchmark.")
    parser.add_argument("--file-size", type=int, default=4096, help="Bytes per file.")
    parser.add_argument("--parallelism", type=int, default=32, help="Concurrent workers for read/write phases.")
    parser.add_argument("--files-per-dir", type=int, default=128, help="Files per logical benchmark directory.")
    parser.add_argument("--cache-size", default="1G", help="Volume cache_size for created benchmark volumes.")
    parser.add_argument("--buffer-size", default="32M", help="Volume buffer_size for created benchmark volumes.")
    parser.add_argument("--prefetch", type=int, default=0, help="Volume prefetch value for created benchmark volumes.")
    parser.add_argument(
        "--writeback",
        action="store_true",
        help="Enable writeback for created benchmark volumes. Disabled by default to measure request-complete latency.",
    )
    parser.add_argument("--keep-volumes", action="store_true", help="Keep benchmark volumes instead of deleting them.")
    parser.add_argument("--json-output", default="", help="Optional file path for JSON results.")
    parser.add_argument(
        "--local-root-parent",
        default="",
        help="Parent directory for the local filesystem benchmark. Defaults to the system temp directory.",
    )
    return parser.parse_args()


def run_command(args: list[str], *, capture_output: bool = True) -> str:
    completed = subprocess.run(args, check=True, text=True, capture_output=capture_output)
    return completed.stdout.strip() if capture_output else ""


def ensure_kube_context(kind_cluster: str) -> None:
    if not kind_cluster:
        return
    run_command(["kind", "export", "kubeconfig", "--name", kind_cluster], capture_output=False)


def discover_base_url(args: argparse.Namespace) -> str:
    if args.base_url:
        return args.base_url.rstrip("/")
    if args.kind_cluster:
        mapping = run_command(
            [
                "docker",
                "port",
                f"{args.kind_cluster}-control-plane",
                "30080/tcp",
            ]
        )
        if mapping:
            host_port = mapping.rsplit(":", 1)[-1].strip()
            if host_port:
                return f"http://127.0.0.1:{host_port}"
    node_port = run_command(
        [
            "kubectl",
            "-n",
            args.kube_namespace,
            "get",
            "svc",
            args.cluster_gateway_service,
            "-o",
            "jsonpath={.spec.ports[0].nodePort}",
        ]
    )
    if not node_port:
        raise RuntimeError("failed to discover cluster-gateway NodePort")
    return f"http://127.0.0.1:{node_port}"


def discover_password(args: argparse.Namespace) -> str:
    if args.password:
        return args.password
    encoded = run_command(
        [
            "kubectl",
            "-n",
            args.kube_namespace,
            "get",
            "secret",
            "admin-password",
            "-o",
            "jsonpath={.data.password}",
        ]
    )
    import base64

    return base64.b64decode(encoded).decode("utf-8")


def api_request_bytes(
    base_url: str,
    method: str,
    endpoint: str,
    *,
    body: bytes = b"",
    headers: Optional[dict[str, str]] = None,
) -> bytes:
    parsed = urllib.parse.urlsplit(base_url)
    if parsed.scheme != "http":
        raise ValueError(f"only http is supported, got {parsed.scheme!r}")
    connection = http.client.HTTPConnection(parsed.hostname or "127.0.0.1", parsed.port or 80, timeout=60)
    try:
        request_headers = {"Connection": "close"}
        if headers:
            request_headers.update(headers)
        connection.request(method, parsed.path.rstrip("/") + endpoint, body=body, headers=request_headers)
        response = connection.getresponse()
        payload = response.read()
        if response.status < 200 or response.status >= 300:
            raise RuntimeError(f"{method} {endpoint} failed with {response.status}: {payload.decode('utf-8', 'replace')}")
        return payload
    finally:
        connection.close()


def login(base_url: str, email: str, password: str) -> str:
    last_error: Optional[BaseException] = None
    for attempt in range(3):
        try:
            payload = json.loads(
                api_request_bytes(
                    base_url,
                    "POST",
                    "/auth/login",
                    body=json.dumps({"email": email, "password": password}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                )
            )
            return payload["data"]["access_token"]
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == 2:
                break
            time.sleep(1.0)
    raise RuntimeError(f"login failed after retries: {last_error}") from last_error


def create_volume(args: argparse.Namespace, base_url: str, token: str) -> dict[str, object]:
    payload = json.loads(
        api_request_bytes(
            base_url,
            "POST",
            "/api/v1/sandboxvolumes",
            body=json.dumps(
                {
                    "cache_size": args.cache_size,
                    "prefetch": args.prefetch,
                    "buffer_size": args.buffer_size,
                    "writeback": args.writeback,
                    "access_mode": "RWO",
                }
            ).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
        )
    )
    return payload["data"]


def delete_volume(base_url: str, token: str, volume_id: str) -> None:
    api_request_bytes(
        base_url,
        "DELETE",
        f"/api/v1/sandboxvolumes/{volume_id}",
        headers={"Authorization": f"Bearer {token}"},
    )


def build_relative_paths(file_count: int, files_per_dir: int) -> list[str]:
    paths: list[str] = []
    for index in range(file_count):
        dir_index = index // files_per_dir
        paths.append(f"bench/d{dir_index:04d}/f{index:06d}.bin")
    return paths


def unique_dirs(paths: list[str]) -> list[str]:
    dirs = {os.path.dirname(path) for path in paths}
    return sorted(d for d in dirs if d)


def prepare_local_dirs(root: str, paths: list[str]) -> None:
    for directory in unique_dirs(paths):
        os.makedirs(os.path.join(root, directory), exist_ok=True)


class VolumeHTTPClient:
    def __init__(self, base_url: str, token: str, volume_id: str) -> None:
        parsed = urllib.parse.urlsplit(base_url)
        if parsed.scheme != "http":
            raise ValueError(f"only http is supported, got {parsed.scheme!r}")
        self._host = parsed.hostname or "127.0.0.1"
        self._port = parsed.port or 80
        self._base_path = parsed.path.rstrip("/")
        self._token = token
        self._volume_id = volume_id
        self._conn = self._new_connection()

    def _new_connection(self) -> http.client.HTTPConnection:
        return http.client.HTTPConnection(self._host, self._port, timeout=60)

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            return

    def _request(self, method: str, path: str, *, body: bytes = b"", headers: Optional[dict[str, str]] = None) -> bytes:
        req_headers = {"Authorization": f"Bearer {self._token}"}
        if headers:
            req_headers.update(headers)
        for attempt in range(2):
            try:
                self._conn.request(method, path, body=body, headers=req_headers)
                response = self._conn.getresponse()
                payload = response.read()
                if response.status < 200 or response.status >= 300:
                    raise RuntimeError(f"{method} {path} failed with {response.status}: {payload.decode('utf-8', 'replace')}")
                return payload
            except Exception:
                self.close()
                self._conn = self._new_connection()
                if attempt == 1:
                    raise
        raise RuntimeError("unreachable")

    def mkdir(self, logical_path: str) -> None:
        query = urllib.parse.urlencode({"path": "/" + logical_path, "mkdir": "true", "recursive": "true"})
        endpoint = f"{self._base_path}/api/v1/sandboxvolumes/{self._volume_id}/files?{query}"
        self._request("POST", endpoint)

    def write(self, logical_path: str, payload: bytes) -> None:
        query = urllib.parse.urlencode({"path": "/" + logical_path})
        endpoint = f"{self._base_path}/api/v1/sandboxvolumes/{self._volume_id}/files?{query}"
        self._request("POST", endpoint, body=payload, headers={"Content-Type": "application/octet-stream"})

    def read(self, logical_path: str) -> bytes:
        query = urllib.parse.urlencode({"path": "/" + logical_path})
        endpoint = f"{self._base_path}/api/v1/sandboxvolumes/{self._volume_id}/files?{query}"
        return self._request("GET", endpoint)

    def delete(self, logical_path: str) -> None:
        query = urllib.parse.urlencode({"path": "/" + logical_path})
        endpoint = f"{self._base_path}/api/v1/sandboxvolumes/{self._volume_id}/files?{query}"
        self._request("DELETE", endpoint)


def prepare_volume_dirs(base_url: str, token: str, volume_id: str, paths: list[str]) -> None:
    client = VolumeHTTPClient(base_url, token, volume_id)
    try:
        for directory in unique_dirs(paths):
            client.mkdir(directory)
    finally:
        client.close()


def run_parallel(
    items: list[str],
    parallelism: int,
    worker_factory: Callable[[], tuple[Callable[[str], int], Optional[Callable[[], None]]]],
) -> PhaseResult:
    item_queue: queue.Queue[str] = queue.Queue()
    for item in items:
        item_queue.put(item)

    latencies_lock = threading.Lock()
    latencies: list[float] = []
    total_bytes = 0
    total_bytes_lock = threading.Lock()
    errors: list[BaseException] = []

    start = time.perf_counter()

    def thread_main() -> None:
        nonlocal total_bytes
        worker, cleanup = worker_factory()
        local_latencies: list[float] = []
        local_bytes = 0
        try:
            while True:
                try:
                    item = item_queue.get_nowait()
                except queue.Empty:
                    break
                phase_start = time.perf_counter()
                local_bytes += worker(item)
                local_latencies.append((time.perf_counter() - phase_start) * 1000.0)
                item_queue.task_done()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            if cleanup is not None:
                cleanup()
            with latencies_lock:
                latencies.extend(local_latencies)
            with total_bytes_lock:
                total_bytes += local_bytes

    threads = [threading.Thread(target=thread_main, daemon=True) for _ in range(parallelism)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    if errors:
        raise RuntimeError(f"benchmark worker failed: {errors[0]}") from errors[0]

    total_seconds = time.perf_counter() - start
    return PhaseResult(
        operations=len(items),
        total_bytes=total_bytes,
        total_seconds=total_seconds,
        latencies_ms=latencies,
    )


def benchmark_local(root: str, paths: list[str], payload: bytes, parallelism: int) -> BenchmarkResult:
    prepare_local_dirs(root, paths)

    def write_worker_factory() -> Callable[[str], int]:
        def worker(relative_path: str) -> int:
            absolute_path = os.path.join(root, relative_path)
            with open(absolute_path, "wb") as file:
                file.write(payload)
            return len(payload)

        return worker, None

    def read_worker_factory() -> Callable[[str], int]:
        def worker(relative_path: str) -> int:
            absolute_path = os.path.join(root, relative_path)
            with open(absolute_path, "rb") as file:
                data = file.read()
            if data != payload:
                raise RuntimeError(f"local read verification failed for {relative_path}")
            return len(data)

        return worker, None

    write = run_parallel(paths, parallelism, write_worker_factory)
    read = run_parallel(paths, parallelism, read_worker_factory)
    return BenchmarkResult(name="local", write=write, read=read, backend_type="localfs")


def warm_volume(base_url: str, token: str, volume_id: str, payload: bytes) -> None:
    client = VolumeHTTPClient(base_url, token, volume_id)
    try:
        client.write("bench/_warmup.bin", payload)
        data = client.read("bench/_warmup.bin")
        if data != payload:
            raise RuntimeError("volume warmup read verification failed")
        client.delete("bench/_warmup.bin")
    finally:
        client.close()


def benchmark_volume(
    name: str,
    backend_type: str,
    base_url: str,
    token: str,
    volume_id: str,
    paths: list[str],
    payload: bytes,
    parallelism: int,
) -> BenchmarkResult:
    prepare_volume_dirs(base_url, token, volume_id, paths)
    warm_volume(base_url, token, volume_id, payload)

    def write_worker_factory() -> tuple[Callable[[str], int], Optional[Callable[[], None]]]:
        client = VolumeHTTPClient(base_url, token, volume_id)

        def worker(relative_path: str) -> int:
            client.write(relative_path, payload)
            return len(payload)

        return worker, client.close

    def read_worker_factory() -> tuple[Callable[[str], int], Optional[Callable[[], None]]]:
        client = VolumeHTTPClient(base_url, token, volume_id)

        def worker(relative_path: str) -> int:
            data = client.read(relative_path)
            if data != payload:
                raise RuntimeError(f"{name} read verification failed for {relative_path}")
            return len(data)

        return worker, client.close

    write = run_parallel(paths, parallelism, write_worker_factory)
    read = run_parallel(paths, parallelism, read_worker_factory)
    return BenchmarkResult(
        name=name,
        volume_id=volume_id,
        backend_type=backend_type,
        write=write,
        read=read,
    )


def print_markdown(results: list[BenchmarkResult]) -> None:
    print()
    print("| Target | Backend | Write ops/s | Write p95 (ms) | Read ops/s | Read p95 (ms) |")
    print("| --- | --- | ---: | ---: | ---: | ---: |")
    for result in results:
        write = result.write.summary()
        read = result.read.summary()
        backend = result.backend_type or "-"
        print(
            f"| {result.name} | {backend} | "
            f"{write['ops_per_second']:.1f} | {write['p95_ms']:.2f} | "
            f"{read['ops_per_second']:.1f} | {read['p95_ms']:.2f} |"
        )


def main() -> int:
    args = parse_args()
    if args.file_count <= 0:
        raise SystemExit("--file-count must be positive")
    if args.file_size <= 0:
        raise SystemExit("--file-size must be positive")
    if args.parallelism <= 0:
        raise SystemExit("--parallelism must be positive")
    if args.files_per_dir <= 0:
        raise SystemExit("--files-per-dir must be positive")

    ensure_kube_context(args.kind_cluster)
    base_url = discover_base_url(args)
    password = discover_password(args)
    token = login(base_url, args.email, password)

    paths = build_relative_paths(args.file_count, args.files_per_dir)
    payload = bytes((index % 251 for index in range(args.file_size)))
    local_root = tempfile.mkdtemp(prefix="storage-proxy-smallfiles-", dir=args.local_root_parent or None)

    created_volume_ids: list[str] = []
    results: list[BenchmarkResult] = []
    try:
        results.append(benchmark_local(local_root, paths, payload, args.parallelism))

        s0fs_volume = create_volume(args, base_url, token)
        s0fs_volume_id = str(s0fs_volume["id"])
        created_volume_ids.append(s0fs_volume_id)
        results.append(
            benchmark_volume(
                name="s0fs-volume",
                backend_type="s0fs",
                base_url=base_url,
                token=token,
                volume_id=s0fs_volume_id,
                paths=paths,
                payload=payload,
                parallelism=args.parallelism,
            )
        )
    finally:
        shutil.rmtree(local_root, ignore_errors=True)
        if not args.keep_volumes:
            for volume_id in created_volume_ids:
                try:
                    delete_volume(base_url, token, volume_id)
                except Exception:
                    pass

    payload = {
        "workload": {
            "base_url": base_url,
            "email": args.email,
            "file_count": args.file_count,
            "file_size": args.file_size,
            "parallelism": args.parallelism,
            "files_per_dir": args.files_per_dir,
            "cache_size": args.cache_size,
            "buffer_size": args.buffer_size,
            "prefetch": args.prefetch,
            "writeback": args.writeback,
        },
        "results": [result.summary() for result in results],
    }

    print(json.dumps(payload, indent=2))
    print_markdown(results)

    if args.json_output:
        with open(args.json_output, "w", encoding="utf-8") as file:
            json.dump(payload, file, indent=2)

    return 0


if __name__ == "__main__":
    sys.exit(main())
