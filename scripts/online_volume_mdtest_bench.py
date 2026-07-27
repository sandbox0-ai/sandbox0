#!/usr/bin/env python3
"""Run pinned mdtest profiles on an online Sandbox0 volume.

The legacy profile reproduces the arguments published by JuiceFS for its
JuiceFS/EFS metadata comparison. The current small-file profile pins mdtest
4.0.0 and exercises 30,000 files with 4 KiB writes and reads plus phase-level
sync. Mounted S0FS and pod-local storage are exercised in the same sandbox.
Dependency installation and tool compilation are outside timed runs.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
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


DEFAULT_IOR_REF = "d339caa501a146449a45ab876079dc37f513fc43"
DEFAULT_MODERN_IOR_REF = "967a9f65109760db8a3ac14a7fdd007f337d2960"
WORKLOAD_PROFILES = {
    "juicefs-public-2021": {
        "default_ior_ref": DEFAULT_IOR_REF,
        "description": "Historical JuiceFS/EFS zero-byte mdtest 3.4 profile.",
    },
    "sandbox-volume-small-files-4k-v1": {
        "default_ior_ref": DEFAULT_MODERN_IOR_REF,
        "description": "Current single-client 30,000 x 4 KiB mdtest profile.",
    },
}


REMOTE_MDTEST_BENCHMARK = r'''
import json
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import time


OPERATIONS = (
    "Directory creation",
    "Directory stat",
    "Directory removal",
    "File creation",
    "File stat",
    "File read",
    "File removal",
    "Tree creation",
    "Tree removal",
)

PROFILES = {
    "juicefs-public-2021": {
        "arguments": ["-b", "6", "-I", "8", "-z", "4"],
        "expected_version": "mdtest-3.4.0+dev",
        "expected_geometry": "1 tasks, 12440 files/directories",
        "expected_files_and_directories": 12440,
        "operations": OPERATIONS,
        "file_payload_bytes": 0,
        "read_semantics": "open-close only because read bytes is zero",
        "sync_semantics": "no explicit sync or fsync",
    },
    "sandbox-volume-small-files-4k-v1": {
        "arguments": [
            "-F",
            "-n",
            "30000",
            "-w",
            "4096",
            "-e",
            "4096",
            "-u",
            "-Y",
        ],
        "expected_version": "mdtest-4.0.0",
        "expected_geometry": "1 tasks, 30000 files",
        "expected_files_and_directories": 30000,
        "operations": (
            "File creation",
            "File stat",
            "File read",
            "File removal",
        ),
        "file_payload_bytes": 4096,
        "read_semantics": "read 4096 bytes per file; mdtest does not verify content",
        "sync_semantics": "mdtest -Y calls sync after each phase and includes it in timing",
    },
}


def trim_text(value, max_lines=320, max_chars=60000):
    if not value:
        return ""
    lines = value.splitlines()
    if len(lines) > max_lines:
        lines = lines[-max_lines:]
    output = "\n".join(lines)
    if len(output) > max_chars:
        output = output[-max_chars:]
    return output


def run(command, cwd=None, timeout=1800, env=None):
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )
    return {
        "command": command,
        "cwd": cwd,
        "exit_code": completed.returncode,
        "elapsed_seconds": time.perf_counter() - started,
        "stdout": trim_text(completed.stdout),
        "stderr": trim_text(completed.stderr),
    }


def run_checked(command, cwd=None, timeout=1800):
    result = run(command, cwd=cwd, timeout=timeout)
    if result["exit_code"] != 0:
        raise RuntimeError(json.dumps(result, sort_keys=True))
    return result


def read_text(path):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return handle.read().strip()
    except OSError:
        return ""


def mount_for_path(path):
    resolved = os.path.realpath(path)
    best = None
    for line in read_text("/proc/self/mountinfo").splitlines():
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


def statvfs(path):
    value = os.statvfs(path)
    return {
        "block_size": value.f_bsize,
        "blocks": value.f_blocks,
        "blocks_available": value.f_bavail,
        "inodes": value.f_files,
        "inodes_free": value.f_ffree,
    }


def parse_summary(output):
    rates = {}
    for line in output.splitlines():
        for operation in OPERATIONS:
            match = re.match(
                r"^\s*" + re.escape(operation) + r"\s*:?\s+"
                r"([-+0-9.eE]+)\s+([-+0-9.eE]+)\s+"
                r"([-+0-9.eE]+)\s+([-+0-9.eE]+)\s*$",
                line,
            )
            if match:
                rates[operation] = {
                    "max": float(match.group(1)),
                    "min": float(match.group(2)),
                    "mean": float(match.group(3)),
                    "stddev": float(match.group(4)),
                }
                break
    return rates


def version_line(output):
    for line in output.splitlines():
        if line.startswith("mdtest-") and "was launched" in line:
            return line.split(" was launched", 1)[0]
    return ""


def install_dependencies():
    packages = [
        "autoconf",
        "automake",
        "gcc",
        "git",
        "libopenmpi-dev",
        "libtool",
        "make",
        "openmpi-bin",
        "pkg-config",
    ]
    return [
        run_checked(["apt-get", "update"], timeout=900),
        run_checked(
            ["apt-get", "install", "-y", "--no-install-recommends", *packages],
            timeout=1800,
        ),
    ]


def build_mdtest(config):
    source = config["source_root"]
    shutil.rmtree(source, ignore_errors=True)
    commands = install_dependencies()
    commands.append(
        run_checked(
            [
                "git",
                "clone",
                "--filter=blob:none",
                "https://github.com/hpc/ior.git",
                source,
            ],
            timeout=900,
        )
    )
    commands.append(
        run_checked(
            ["git", "checkout", "--detach", config["ior_ref"]],
            cwd=source,
            timeout=300,
        )
    )
    commands.extend(
        [
            run_checked(["./bootstrap"], cwd=source, timeout=900),
            run_checked(["./configure", "--without-mpiio"], cwd=source, timeout=900),
            run_checked(["make", "-j", "2"], cwd=source, timeout=1800),
        ]
    )
    head = run_checked(["git", "rev-parse", "HEAD"], cwd=source, timeout=60)["stdout"].strip()
    if head != config["ior_ref"]:
        raise RuntimeError("checked out IOR ref %s, expected %s" % (head, config["ior_ref"]))
    binary = os.path.join(source, "src", "mdtest")
    if not os.access(binary, os.X_OK):
        raise RuntimeError("mdtest binary was not built: %s" % binary)
    return binary, commands, head


def benchmark_target(binary, target, config, round_index):
    profile = PROFILES[config["workload_profile"]]
    root = os.path.join(target["path"], config["workload_profile"])
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root, exist_ok=True)
    command = [binary, "-d", root, *profile["arguments"]]
    benchmark_env = os.environ.copy()
    benchmark_env["PMIX_MCA_gds"] = "hash"
    result = run(
        command,
        timeout=int(config["benchmark_timeout_seconds"]),
        env=benchmark_env,
    )
    result["environment_overrides"] = {"PMIX_MCA_gds": "hash"}
    rates = parse_summary(result["stdout"])
    version = version_line(result["stdout"])
    expected_geometry = profile["expected_geometry"] in result["stdout"]
    expected_operations = set(profile["operations"])
    passed = (
        result["exit_code"] == 0
        and expected_operations.issubset(rates)
        and version == profile["expected_version"]
        and expected_geometry
    )
    return {
        "round": round_index,
        "passed": passed,
        "version": version,
        "expected_geometry_found": expected_geometry,
        "rates": rates,
        "process_elapsed_seconds": result["elapsed_seconds"],
        "command": result,
    }


def summarize_runs(runs, operations):
    summary = {}
    for operation in operations:
        values = [item["rates"][operation]["mean"] for item in runs]
        summary[operation] = {
            "median_ops_per_second": statistics.median(values),
            "min_ops_per_second": min(values),
            "max_ops_per_second": max(values),
            "runs": values,
        }
    return summary


def environment_snapshot(targets):
    return {
        "os_release": read_text("/etc/os-release"),
        "kernel": platform.release(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "cpu_count_visible": os.cpu_count(),
        "cgroup_cpu_max": read_text("/sys/fs/cgroup/cpu.max"),
        "cgroup_memory_max": read_text("/sys/fs/cgroup/memory.max"),
        "memory_total_kib": next(
            (
                line.split()[1]
                for line in read_text("/proc/meminfo").splitlines()
                if line.startswith("MemTotal:")
            ),
            "",
        ),
        "targets": {
            target["backend"]: {
                "path": target["path"],
                "mount": mount_for_path(target["path"]),
                "statvfs": statvfs(target["path"]),
            }
            for target in targets
        },
    }


def main():
    config = json.loads(sys.argv[1])
    profile = PROFILES[config["workload_profile"]]
    targets = [
        {"backend": "localfs", "path": config["local_root"]},
        {"backend": "s0fs", "path": config["mount_path"]},
    ]
    for target in targets:
        if not os.path.isdir(target["path"]):
            raise RuntimeError("benchmark target is not a directory: %s" % target["path"])

    binary, build_commands, head = build_mdtest(config)
    runs_by_backend = {target["backend"]: [] for target in targets}
    for round_index in range(1, int(config["rounds"]) + 1):
        order = targets if round_index % 2 else list(reversed(targets))
        for target in order:
            result = benchmark_target(binary, target, config, round_index)
            runs_by_backend[target["backend"]].append(result)
            if not result["passed"]:
                raise RuntimeError(
                    "mdtest validation failed for %s round %d: %s"
                    % (target["backend"], round_index, json.dumps(result, sort_keys=True))
                )

    results = []
    for target in targets:
        runs = sorted(runs_by_backend[target["backend"]], key=lambda item: item["round"])
        results.append(
            {
                "backend": target["backend"],
                "path": target["path"],
                "passed": all(item["passed"] for item in runs),
                "summary": summarize_runs(runs, profile["operations"]),
                "runs": runs,
            }
        )

    output = {
        "schema_version": 1,
        "profile": {
            "name": config["workload_profile"],
            "arguments": profile["arguments"],
            "rounds_per_target": int(config["rounds"]),
            "tasks": 1,
            "expected_files_and_directories": profile["expected_files_and_directories"],
            "file_payload_bytes": profile["file_payload_bytes"],
            "read_semantics": profile["read_semantics"],
            "sync_semantics": profile["sync_semantics"],
            "cache_policy": "uncontrolled; no cache drop",
            "mpi_environment": {
                "PMIX_MCA_gds": "hash",
                "reason": "required for OpenMPI singleton startup under gVisor",
            },
        },
        "tool": {
            "repository": "https://github.com/hpc/ior.git",
            "requested_ref": config["ior_ref"],
            "resolved_commit": head,
            "reported_version": profile["expected_version"],
            "binary": binary,
            "build_commands": build_commands,
        },
        "environment": environment_snapshot(targets),
        "results": results,
        "passed": all(item["passed"] for item in results),
    }
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
'''


def log(message: str) -> None:
    print(f"[volume-mdtest] {message}", file=sys.stderr, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a pinned mdtest profile on online S0FS."
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
    parser.add_argument("--local-root", default="/tmp", help="Pod-local comparison path.")
    parser.add_argument("--rounds", type=int, default=3, help="Independent runs per target.")
    parser.add_argument(
        "--workload-profile",
        choices=sorted(WORKLOAD_PROFILES),
        default="juicefs-public-2021",
        help="Pinned mdtest workload profile.",
    )
    parser.add_argument(
        "--ior-ref",
        default="",
        help="Override the IOR git commit pinned by the workload profile.",
    )
    parser.add_argument(
        "--benchmark-timeout-seconds",
        type=int,
        default=1800,
        help="Timeout for each mdtest invocation.",
    )
    parser.add_argument(
        "--command-timeout-seconds",
        type=int,
        default=10800,
        help="Maximum time for install, build, and all benchmark runs.",
    )
    parser.add_argument("--ttl", type=int, default=10800, help="Sandbox soft TTL in seconds.")
    parser.add_argument("--hard-ttl", type=int, default=14400, help="Sandbox hard TTL in seconds.")
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
        help="Keep the benchmark sandbox and volume for debugging.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    for field in (
        "rounds",
        "benchmark_timeout_seconds",
        "command_timeout_seconds",
        "ttl",
        "hard_ttl",
        "cleanup_timeout_seconds",
    ):
        if int(getattr(args, field)) <= 0:
            raise SystemExit(f"--{field.replace('_', '-')} must be positive")
    if args.ttl > args.hard_ttl:
        raise SystemExit("--ttl cannot be greater than --hard-ttl")
    if not os.path.isabs(args.mount_path) or not os.path.isabs(args.local_root):
        raise SystemExit("--mount-path and --local-root must be absolute")
    if not args.team_id and not args.team_name and not args.team_slug:
        raise SystemExit("set --team-id, --team-name, or --team-slug")


def operation_medians(benchmark: dict[str, Any]) -> dict[str, dict[str, float]]:
    medians: dict[str, dict[str, float]] = {}
    for target in benchmark.get("results", []):
        medians[str(target["backend"])] = {
            operation: float(values["median_ops_per_second"])
            for operation, values in target["summary"].items()
        }
    return medians


def comparison(benchmark: dict[str, Any]) -> dict[str, float]:
    medians = operation_medians(benchmark)
    local = medians.get("localfs", {})
    s0fs = medians.get("s0fs", {})
    result = {}
    for operation in sorted(set(local) & set(s0fs)):
        result[operation] = s0fs[operation] / local[operation] if local[operation] else 0.0
    return result


def print_summary(result: dict[str, Any]) -> None:
    medians = operation_medians(result["benchmark"])
    print()
    print("| Operation | pod-local ops/s | S0FS ops/s | S0FS / local |")
    print("| --- | ---: | ---: | ---: |")
    operations = next(
        (
            list(target["summary"])
            for target in result["benchmark"].get("results", [])
            if target.get("backend") == "s0fs"
        ),
        [],
    )
    for operation in operations:
        local_value = medians["localfs"][operation]
        s0fs_value = medians["s0fs"][operation]
        ratio = s0fs_value / local_value if local_value else 0.0
        print(f"| {operation} | {local_value:.3f} | {s0fs_value:.3f} | {ratio:.3f} |")


def main() -> int:
    args = parse_args()
    validate_args(args)
    workload = WORKLOAD_PROFILES[args.workload_profile]
    ior_ref = args.ior_ref or str(workload["default_ior_ref"])
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

        log(f"claiming sandbox with volume {volume_id} mounted at {args.mount_path}")
        sandbox, sandbox_create_seconds = timed_json(
            cli,
            [
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
            ],
            timeout=600,
        )
        sandbox_id = str(first_value(sandbox, "id", "ID"))

        log(
            f"building pinned mdtest profile {args.workload_profile!r} and "
            f"running {args.rounds} round(s) per target"
        )
        benchmark, benchmark_command_seconds = run_remote_json_job(
            cli,
            sandbox_id,
            remote_path="/tmp/sandbox0-volume-mdtest-bench.py",
            script=REMOTE_MDTEST_BENCHMARK,
            config={
                "ior_ref": ior_ref,
                "source_root": f"/tmp/ior-{args.workload_profile}",
                "workload_profile": args.workload_profile,
                "mount_path": args.mount_path,
                "local_root": args.local_root,
                "rounds": args.rounds,
                "benchmark_timeout_seconds": args.benchmark_timeout_seconds,
            },
            job_name="sandbox0-volume-mdtest",
            command_timeout_seconds=args.command_timeout_seconds,
            upload_prefix="s0-volume-mdtest-",
        )
        result = {
            "schema_version": 1,
            "benchmark": benchmark,
            "comparison_s0fs_over_local": comparison(benchmark),
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
                "sandbox_id": sandbox_id,
                "sandbox_cluster_id": sandbox.get("cluster_id", sandbox.get("ClusterID")),
                "sandbox_pod_name": sandbox.get("pod_name", sandbox.get("PodName")),
                "sandbox_status": sandbox.get("status", sandbox.get("Status")),
                "orchestration_seconds": {
                    "volume_create": volume_create_seconds,
                    "sandbox_create_and_ready": sandbox_create_seconds,
                    "install_build_and_benchmark": benchmark_command_seconds,
                },
            },
        }
    except BaseException as exc:  # noqa: BLE001
        run_error = exc
    finally:
        if args.keep_resources:
            log("keeping benchmark resources as requested")
            cleanup = {
                "kept": True,
                "sandbox_id": sandbox_id,
                "volume_id": volume_id,
            }
        else:
            log("cleaning benchmark sandbox and volume")
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
                cleanup["team_restore_error"] = restored.stderr.strip() or restored.stdout.strip()
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
        print_summary(result)

    if run_error is not None:
        raise run_error
    if result is None:
        raise RuntimeError("benchmark did not produce a result")
    if not result["benchmark"].get("passed"):
        return 1
    if not cleanup.get("kept") and (
        not cleanup.get("sandbox", {}).get("deleted")
        or not cleanup.get("volume", {}).get("deleted")
    ):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
