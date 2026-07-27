#!/usr/bin/env python3
"""Verify an online Sandbox0 volume across sandbox destruction and reattachment."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from typing import Any

try:
    from .online_volume_small_file_bench import (
        S0CLI,
        cleanup_resources,
        ensure_template_mount,
        first_value,
        resolve_team,
        run_remote_json_job,
        timed_json,
        utc_now,
    )
except ImportError:
    from online_volume_small_file_bench import (
        S0CLI,
        cleanup_resources,
        ensure_template_mount,
        first_value,
        resolve_team,
        run_remote_json_job,
        timed_json,
        utc_now,
    )


REMOTE_REATTACH_WORKLOAD = r'''
import concurrent.futures
import hashlib
import json
import os
import platform
import shutil
import statistics
import sys
import time


def percentile(values, fraction):
    ordered = sorted(values)
    if not ordered:
        return 0.0
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def payload_for(index, size):
    seed = ("sandbox-volume-reattach-v1:%d" % index).encode("ascii")
    return hashlib.shake_256(seed).digest(size)


def relative_path(index, files_per_dir):
    bucket = index // files_per_dir
    return os.path.join(
        "bucket-%06d" % bucket,
        "file-%08d.bin" % index,
    )


def dataset_digest(records):
    digest = hashlib.sha256()
    for index, relative, size, content_digest in sorted(records):
        digest.update(("%d\0%s\0%d\0" % (index, relative, size)).encode("utf-8"))
        digest.update(bytes.fromhex(content_digest))
    return digest.hexdigest()


def mount_for_path(path):
    resolved = os.path.realpath(path)
    best = None
    try:
        with open("/proc/self/mountinfo", "r", encoding="utf-8") as handle:
            lines = handle.read().splitlines()
    except OSError:
        lines = []
    for line in lines:
        fields = line.split()
        if len(fields) < 10 or "-" not in fields:
            continue
        separator = fields.index("-")
        mount_point = fields[4].replace("\\040", " ")
        if resolved != mount_point and not resolved.startswith(mount_point.rstrip("/") + "/"):
            continue
        if best is None or len(mount_point) > len(best["mount_point"]):
            best = {
                "mount_point": mount_point,
                "mount_options": fields[5],
                "filesystem_type": fields[separator + 1],
                "source": fields[separator + 2],
                "super_options": fields[separator + 3] if len(fields) > separator + 3 else "",
            }
    return best or {}


def prepare_directories(root, file_count, files_per_dir):
    started = time.perf_counter()
    directory_count = (file_count + files_per_dir - 1) // files_per_dir
    for bucket in range(directory_count):
        os.makedirs(os.path.join(root, "bucket-%06d" % bucket), exist_ok=True)
    return directory_count, time.perf_counter() - started


def write_dataset(config, root):
    if os.path.exists(root):
        shutil.rmtree(root)
    os.makedirs(root)
    directory_count, directory_seconds = prepare_directories(
        root,
        config["file_count"],
        config["files_per_dir"],
    )

    def write_one(index):
        relative = relative_path(index, config["files_per_dir"])
        path = os.path.join(root, relative)
        payload = payload_for(index, config["file_size"])
        started = time.perf_counter()
        with open(path, "wb") as handle:
            written = handle.write(payload)
        elapsed = time.perf_counter() - started
        if written != len(payload):
            return {
                "index": index,
                "relative": relative,
                "error": "short write: %d of %d" % (written, len(payload)),
                "elapsed_seconds": elapsed,
            }
        return {
            "index": index,
            "relative": relative,
            "size": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "elapsed_seconds": elapsed,
        }

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=config["parallelism"]
    ) as executor:
        results = list(executor.map(write_one, range(config["file_count"])))
    write_seconds = time.perf_counter() - started
    errors = [item for item in results if item.get("error")]
    if errors:
        raise RuntimeError("write failures: %s" % json.dumps(errors[:20], sort_keys=True))

    sync_started = time.perf_counter()
    os.sync()
    sync_seconds = time.perf_counter() - sync_started
    records = [
        (
            item["index"],
            item["relative"],
            item["size"],
            item["sha256"],
        )
        for item in results
    ]
    latencies_ms = [item["elapsed_seconds"] * 1000.0 for item in results]
    return {
        "mode": "write",
        "passed": True,
        "files_written": len(results),
        "bytes_written": len(results) * config["file_size"],
        "directories_created": directory_count,
        "directory_create_seconds": directory_seconds,
        "write_seconds": write_seconds,
        "sync_seconds": sync_seconds,
        "writes_per_second": len(results) / write_seconds,
        "write_latency_ms": {
            "p50": statistics.median(latencies_ms),
            "p95": percentile(latencies_ms, 0.95),
        },
        "dataset_sha256": dataset_digest(records),
    }


def verify_dataset(config, root):
    def verify_one(index):
        relative = relative_path(index, config["files_per_dir"])
        path = os.path.join(root, relative)
        expected = payload_for(index, config["file_size"])
        started = time.perf_counter()
        try:
            with open(path, "rb") as handle:
                actual = handle.read()
        except OSError as exc:
            return {
                "index": index,
                "relative": relative,
                "error": "%s: %s" % (type(exc).__name__, exc),
                "elapsed_seconds": time.perf_counter() - started,
            }
        elapsed = time.perf_counter() - started
        if actual != expected:
            return {
                "index": index,
                "relative": relative,
                "error": "content mismatch",
                "actual_size": len(actual),
                "actual_sha256": hashlib.sha256(actual).hexdigest(),
                "expected_sha256": hashlib.sha256(expected).hexdigest(),
                "elapsed_seconds": elapsed,
            }
        return {
            "index": index,
            "relative": relative,
            "size": len(actual),
            "sha256": hashlib.sha256(actual).hexdigest(),
            "elapsed_seconds": elapsed,
        }

    first_read = verify_one(0)
    if first_read.get("error"):
        raise RuntimeError(
            "first read after reattach failed: %s"
            % json.dumps(first_read, sort_keys=True)
        )

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=config["parallelism"]
    ) as executor:
        results = list(executor.map(verify_one, range(config["file_count"])))
    verify_seconds = time.perf_counter() - started
    errors = [item for item in results if item.get("error")]
    records = [
        (
            item["index"],
            item["relative"],
            item["size"],
            item["sha256"],
        )
        for item in results
        if not item.get("error")
    ]
    latencies_ms = [item["elapsed_seconds"] * 1000.0 for item in results]
    observed_files = 0
    for _, _, filenames in os.walk(root):
        observed_files += sum(name.endswith(".bin") for name in filenames)
    passed = not errors and len(records) == config["file_count"]
    return {
        "mode": "verify",
        "passed": passed,
        "files_expected": config["file_count"],
        "files_observed": observed_files,
        "files_verified": len(records),
        "bytes_verified": sum(item[2] for item in records),
        "first_read_after_reattach": {
            "index": first_read["index"],
            "relative": first_read["relative"],
            "bytes": first_read["size"],
            "latency_ms": first_read["elapsed_seconds"] * 1000.0,
            "sha256": first_read["sha256"],
            "passed": True,
        },
        "verify_seconds": verify_seconds,
        "verified_reads_per_second": len(records) / verify_seconds,
        "read_latency_ms": {
            "p50": statistics.median(latencies_ms),
            "p95": percentile(latencies_ms, 0.95),
        },
        "dataset_sha256": dataset_digest(records),
        "errors": errors[:20],
    }


def main():
    config = json.loads(sys.argv[1])
    root = os.path.join(config["mount_path"], config["dataset_name"])
    if config["mode"] == "write":
        result = write_dataset(config, root)
    elif config["mode"] == "verify":
        result = verify_dataset(config, root)
    else:
        raise RuntimeError("unknown mode: %s" % config["mode"])
    result["configuration"] = {
        "dataset_name": config["dataset_name"],
        "file_count": config["file_count"],
        "file_size": config["file_size"],
        "files_per_dir": config["files_per_dir"],
        "parallelism": config["parallelism"],
    }
    result["environment"] = {
        "kernel": platform.release(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "cpu_count_visible": os.cpu_count(),
        "mount": mount_for_path(config["mount_path"]),
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    if not result["passed"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
'''


def log(message: str) -> None:
    print(f"[volume-reattach] {message}", file=sys.stderr, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify an online S0FS volume after attaching it to a fresh sandbox."
    )
    parser.add_argument("--s0-bin", default="s0", help="Path to the s0 CLI binary.")
    parser.add_argument("--profile", default="default", help="s0 CLI profile.")
    parser.add_argument("--config", default="", help="Optional s0 CLI config path.")
    parser.add_argument("--team-id", default="", help="Existing benchmark team ID.")
    parser.add_argument("--team-name", default="volume-benchmark", help="Benchmark team name.")
    parser.add_argument("--team-slug", default="volume-benchmark", help="Benchmark team slug.")
    parser.add_argument(
        "--create-team",
        action="store_true",
        help="Create the benchmark team when no exact name or slug match exists.",
    )
    parser.add_argument("--home-region", default="ali-ue1", help="Home region for a new team.")
    parser.add_argument("--template", default="default", help="Sandbox template ID.")
    parser.add_argument("--mount-path", default="/workspace", help="S0FS mount path.")
    parser.add_argument(
        "--dataset-name",
        default="sandbox-volume-reattach-v1",
        help="Directory name created below the mount path.",
    )
    parser.add_argument("--file-count", type=int, default=30000, help="Files in the dataset.")
    parser.add_argument("--file-size", type=int, default=4096, help="Bytes per file.")
    parser.add_argument("--parallelism", type=int, default=8, help="Concurrent I/O workers.")
    parser.add_argument("--files-per-dir", type=int, default=128, help="Files per directory.")
    parser.add_argument("--ttl", type=int, default=7200, help="Sandbox soft TTL in seconds.")
    parser.add_argument("--hard-ttl", type=int, default=10800, help="Sandbox hard TTL in seconds.")
    parser.add_argument(
        "--command-timeout-seconds",
        type=int,
        default=7200,
        help="Maximum time for each remote workload.",
    )
    parser.add_argument(
        "--reattach-timeout-seconds",
        type=int,
        default=300,
        help="Maximum time to wait for the RWO volume to attach to the consumer.",
    )
    parser.add_argument(
        "--cleanup-timeout-seconds",
        type=int,
        default=180,
        help="Maximum time to wait for volume deletion.",
    )
    parser.add_argument("--json-output", default="", help="Optional aggregate result path.")
    parser.add_argument(
        "--keep-resources",
        action="store_true",
        help="Keep the current sandbox and volume for debugging.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    for field in (
        "file_count",
        "file_size",
        "parallelism",
        "files_per_dir",
        "ttl",
        "hard_ttl",
        "command_timeout_seconds",
        "reattach_timeout_seconds",
        "cleanup_timeout_seconds",
    ):
        if int(getattr(args, field)) <= 0:
            raise SystemExit(f"--{field.replace('_', '-')} must be positive")
    if args.ttl > args.hard_ttl:
        raise SystemExit("--ttl cannot be greater than --hard-ttl")
    if not os.path.isabs(args.mount_path):
        raise SystemExit("--mount-path must be absolute")
    if (
        not args.dataset_name
        or args.dataset_name in {".", ".."}
        or os.path.basename(args.dataset_name) != args.dataset_name
    ):
        raise SystemExit("--dataset-name must be one non-special path component")
    if not args.team_id and not args.team_name and not args.team_slug:
        raise SystemExit("set --team-id, --team-name, or --team-slug")


def sandbox_create_args(args: argparse.Namespace, volume_id: str) -> list[str]:
    return [
        "sandbox",
        "create",
        "--template",
        args.template,
        "--mount",
        f"{volume_id}:{args.mount_path}",
        "--ttl",
        str(args.ttl),
        "--hard-ttl",
        str(args.hard_ttl),
    ]


def create_consumer_eventually(
    cli: S0CLI,
    args: argparse.Namespace,
    volume_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    started = time.perf_counter()
    deadline = time.monotonic() + args.reattach_timeout_seconds
    attempts = 0
    last_error = ""
    while time.monotonic() < deadline:
        attempts += 1
        completed = cli.run(
            sandbox_create_args(args, volume_id) + ["--output", "json"],
            timeout=600,
            check=False,
        )
        if completed.returncode == 0:
            try:
                sandbox = json.loads(completed.stdout)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"consumer create returned invalid JSON: {completed.stdout}"
                ) from exc
            return sandbox, {
                "attempts": attempts,
                "elapsed_seconds": time.perf_counter() - started,
            }
        last_error = completed.stderr.strip() or completed.stdout.strip()
        time.sleep(2)
    raise TimeoutError(
        "volume did not attach to a consumer within "
        f"{args.reattach_timeout_seconds}s after {attempts} attempts: {last_error}"
    )


def workload_config(args: argparse.Namespace, mode: str) -> dict[str, Any]:
    return {
        "mode": mode,
        "mount_path": args.mount_path,
        "dataset_name": args.dataset_name,
        "file_count": args.file_count,
        "file_size": args.file_size,
        "parallelism": args.parallelism,
        "files_per_dir": args.files_per_dir,
    }


def run_workload(
    cli: S0CLI,
    args: argparse.Namespace,
    sandbox_id: str,
    mode: str,
) -> tuple[dict[str, Any], float]:
    return run_remote_json_job(
        cli,
        sandbox_id,
        remote_path="/tmp/sandbox0-volume-reattach-bench.py",
        script=REMOTE_REATTACH_WORKLOAD,
        config=workload_config(args, mode),
        job_name=f"sandbox0-volume-reattach-{mode}",
        command_timeout_seconds=args.command_timeout_seconds,
        upload_prefix="s0-volume-reattach-",
    )


def delete_sandbox(cli: S0CLI, sandbox_id: str) -> dict[str, Any]:
    started = time.perf_counter()
    completed = cli.run(["sandbox", "delete", sandbox_id], timeout=120, check=False)
    result = {
        "deleted": completed.returncode == 0,
        "elapsed_seconds": time.perf_counter() - started,
    }
    if completed.returncode != 0:
        result["error"] = completed.stderr.strip() or completed.stdout.strip()
    return result


def main() -> int:
    args = parse_args()
    validate_args(args)
    binary = shutil.which(args.s0_bin) if os.path.sep not in args.s0_bin else args.s0_bin
    if not binary:
        raise SystemExit(f"s0 CLI not found: {args.s0_bin}")
    cli = S0CLI(binary=binary, profile=args.profile, config=args.config)

    team, team_created, original_team_id = resolve_team(cli, args)
    team_id = str(team["id"])
    switched_team = original_team_id != team_id
    if switched_team:
        log(f"selecting team {team_id}")
        cli.run(["team", "use", team_id])

    started_at = utc_now()
    sandbox_id = ""
    producer_id = ""
    consumer_id = ""
    volume_id = ""
    result: dict[str, Any] | None = None
    run_error: BaseException | None = None
    cleanup: dict[str, Any] = {}
    try:
        log(f"checking template {args.template!r}")
        template = cli.run_json(["template", "get", args.template])
        ensure_template_mount(template, args.mount_path)

        log("creating RWO S0FS volume")
        volume, volume_create_seconds = timed_json(
            cli,
            ["volume", "create", "--access-mode", "RWO", "--backend", "s0fs"],
        )
        volume_id = str(volume["id"])

        log(f"creating producer sandbox with volume {volume_id}")
        producer, producer_create_seconds = timed_json(
            cli,
            sandbox_create_args(args, volume_id),
            timeout=600,
        )
        producer_id = str(first_value(producer, "id", "ID"))
        sandbox_id = producer_id

        log(
            f"writing {args.file_count} files x {args.file_size} bytes "
            f"with {args.parallelism} workers"
        )
        writer, writer_command_seconds = run_workload(
            cli,
            args,
            producer_id,
            "write",
        )

        log(f"destroying producer sandbox {producer_id}")
        producer_delete = delete_sandbox(cli, producer_id)
        if not producer_delete["deleted"]:
            raise RuntimeError(f"producer sandbox deletion failed: {producer_delete}")
        sandbox_id = ""

        log("creating fresh consumer sandbox with the same volume")
        consumer, consumer_attach = create_consumer_eventually(cli, args, volume_id)
        consumer_id = str(first_value(consumer, "id", "ID"))
        sandbox_id = consumer_id

        log("verifying every file in the reattached volume")
        verifier, verifier_command_seconds = run_workload(
            cli,
            args,
            consumer_id,
            "verify",
        )
        digest_matches = writer["dataset_sha256"] == verifier["dataset_sha256"]
        passed = bool(writer["passed"] and verifier["passed"] and digest_matches)
        result = {
            "schema_version": 1,
            "passed": passed,
            "configuration": workload_config(args, "write") | {
                "profile": "sandbox-volume-reattach-v1",
                "persistence_boundary": "close every file, call os.sync(), destroy producer",
            },
            "producer": {
                "sandbox_id": producer_id,
                "create_seconds": producer_create_seconds,
                "delete": producer_delete,
                "workload_command_seconds": writer_command_seconds,
                "workload": writer,
            },
            "consumer": {
                "sandbox_id": consumer_id,
                "attach": consumer_attach,
                "workload_command_seconds": verifier_command_seconds,
                "workload": verifier,
            },
            "validation": {
                "dataset_digest_matches": digest_matches,
                "producer_dataset_sha256": writer["dataset_sha256"],
                "consumer_dataset_sha256": verifier["dataset_sha256"],
                "files_expected": args.file_count,
                "files_verified": verifier["files_verified"],
                "errors": verifier["errors"],
            },
            "online": {
                "started_at": started_at,
                "finished_at": utc_now(),
                "s0_binary": binary,
                "profile": args.profile,
                "team": team,
                "team_created": team_created,
                "template_id": args.template,
                "template_scope": template.get("scope"),
                "template_image": ((template.get("spec") or {}).get("mainContainer") or {}).get("image"),
                "template_resources": ((template.get("spec") or {}).get("mainContainer") or {}).get("resources"),
                "volume_id": volume_id,
                "volume_create_seconds": volume_create_seconds,
                "producer_cluster_id": producer.get("cluster_id", producer.get("ClusterID")),
                "consumer_cluster_id": consumer.get("cluster_id", consumer.get("ClusterID")),
            },
        }
    except BaseException as exc:  # noqa: BLE001
        run_error = exc
    finally:
        if args.keep_resources:
            log("keeping current resources as requested")
            cleanup = {
                "kept": True,
                "sandbox_id": sandbox_id,
                "producer_id": producer_id,
                "consumer_id": consumer_id,
                "volume_id": volume_id,
            }
        else:
            log("cleaning consumer sandbox and volume")
            cleanup = cleanup_resources(
                cli,
                sandbox_id,
                volume_id,
                args.cleanup_timeout_seconds,
            )
        if switched_team and original_team_id:
            log(f"restoring original team {original_team_id}")
            restored = cli.run(["team", "use", original_team_id], timeout=120, check=False)
            cleanup["original_team_restored"] = restored.returncode == 0
            if restored.returncode != 0:
                cleanup["team_restore_error"] = (
                    restored.stderr.strip() or restored.stdout.strip()
                )
        else:
            cleanup["original_team_restored"] = True

    if result is not None:
        result["cleanup"] = cleanup
        if args.json_output:
            output_dir = os.path.dirname(os.path.abspath(args.json_output))
            os.makedirs(output_dir, exist_ok=True)
            with open(args.json_output, "w", encoding="utf-8") as handle:
                json.dump(result, handle, indent=2, sort_keys=True)
                handle.write("\n")
            log(f"wrote result to {args.json_output}")
        print()
        print("| Gate | Result |")
        print("| --- | --- |")
        print(f"| Producer files written | {result['producer']['workload']['files_written']} |")
        print(f"| Consumer files verified | {result['consumer']['workload']['files_verified']} |")
        print(f"| Dataset SHA-256 match | {result['validation']['dataset_digest_matches']} |")
        print(f"| Overall | {'PASS' if result['passed'] else 'FAIL'} |")

    if run_error is not None:
        raise run_error
    if result is None:
        raise RuntimeError("reattach benchmark did not produce a result")
    if not result["passed"]:
        return 1
    if not cleanup.get("kept") and (
        not cleanup.get("sandbox", {}).get("deleted")
        or not cleanup.get("volume", {}).get("deleted")
    ):
        return 2
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
