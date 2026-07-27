#!/usr/bin/env python3
"""Benchmark mounted S0FS small-file I/O in an online Sandbox0 team.

The orchestrator uses only the s0 CLI for team selection and remote resource
operations. The benchmark runs mounted S0FS and pod-local storage in the same
sandbox process so both targets share the same CPU, memory, image, and node.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any

try:
    from .volume_mount_bench import REMOTE_BENCHMARK
except ImportError:
    from volume_mount_bench import REMOTE_BENCHMARK


@dataclasses.dataclass
class S0CLI:
    binary: str
    profile: str
    config: str

    def command(self, args: list[str]) -> list[str]:
        command = [self.binary]
        if self.profile:
            command += ["--profile", self.profile]
        if self.config:
            command += ["--config", self.config]
        command += args
        return command

    def run(
        self,
        args: list[str],
        *,
        timeout: int = 300,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        completed = subprocess.run(
            self.command(args),
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
        if check and completed.returncode != 0:
            raise RuntimeError(
                "command failed with exit code {code}: {command}\n"
                "stdout:\n{stdout}\n"
                "stderr:\n{stderr}".format(
                    code=completed.returncode,
                    command=" ".join(self.command(args)),
                    stdout=completed.stdout,
                    stderr=completed.stderr,
                )
            )
        return completed

    def run_json(self, args: list[str], *, timeout: int = 300) -> Any:
        completed = self.run(args + ["--output", "json"], timeout=timeout)
        try:
            return json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"command did not return JSON: {' '.join(self.command(args))}\n"
                f"stdout:\n{completed.stdout}\n"
                f"stderr:\n{completed.stderr}"
            ) from exc


def utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def log(message: str) -> None:
    print(f"[volume-benchmark] {message}", file=sys.stderr, flush=True)


def first_value(value: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in value:
            return value[key]
    raise KeyError("/".join(keys))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark many-small-file I/O on an online Sandbox0 S0FS volume."
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
    parser.add_argument("--file-count", type=int, default=30000, help="Files per target and round.")
    parser.add_argument("--file-size", type=int, default=4096, help="Bytes per file.")
    parser.add_argument("--parallelism", type=int, default=8, help="Concurrent I/O workers.")
    parser.add_argument("--files-per-dir", type=int, default=128, help="Files per directory.")
    parser.add_argument("--rounds", type=int, default=3, help="Rounds per target.")
    parser.add_argument(
        "--precreate-dirs",
        action="store_true",
        help="Create directories before timing writes.",
    )
    parser.add_argument("--ttl", type=int, default=7200, help="Sandbox soft TTL in seconds.")
    parser.add_argument("--hard-ttl", type=int, default=10800, help="Sandbox hard TTL in seconds.")
    parser.add_argument(
        "--command-timeout-seconds",
        type=int,
        default=7200,
        help="Maximum time for the remote benchmark command.",
    )
    parser.add_argument(
        "--cleanup-timeout-seconds",
        type=int,
        default=180,
        help="Maximum time to wait for volume detachment and deletion.",
    )
    parser.add_argument("--json-output", default="", help="Optional aggregate result path.")
    parser.add_argument(
        "--keep-resources",
        action="store_true",
        help="Keep the benchmark sandbox and volume for debugging.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    positive_fields = (
        "file_count",
        "file_size",
        "parallelism",
        "files_per_dir",
        "rounds",
        "ttl",
        "hard_ttl",
        "command_timeout_seconds",
        "cleanup_timeout_seconds",
    )
    for field in positive_fields:
        if int(getattr(args, field)) <= 0:
            raise SystemExit(f"--{field.replace('_', '-')} must be positive")
    if args.ttl > args.hard_ttl:
        raise SystemExit("--ttl cannot be greater than --hard-ttl")
    if not os.path.isabs(args.mount_path):
        raise SystemExit("--mount-path must be absolute")
    if not args.team_id and not args.team_name and not args.team_slug:
        raise SystemExit("set --team-id, --team-name, or --team-slug")


def select_team(teams: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, Any] | None:
    if args.team_id:
        matches = [team for team in teams if str(team.get("id", "")) == args.team_id]
    else:
        matches = [
            team
            for team in teams
            if (args.team_slug and str(team.get("slug", "")) == args.team_slug)
            or (args.team_name and str(team.get("name", "")) == args.team_name)
        ]
    if len(matches) > 1:
        raise RuntimeError("multiple teams matched the requested benchmark team")
    return matches[0] if matches else None


def resolve_team(cli: S0CLI, args: argparse.Namespace) -> tuple[dict[str, Any], bool, str]:
    log("resolving benchmark team")
    teams = cli.run_json(["team", "list"])
    if not isinstance(teams, list):
        raise RuntimeError(f"unexpected team list response: {type(teams).__name__}")
    current_team_id = next(
        (str(team["id"]) for team in teams if team.get("current")),
        "",
    )
    team = select_team(teams, args)
    created = False
    if team is None:
        if not args.create_team:
            raise RuntimeError(
                "benchmark team does not exist; rerun with --create-team or pass --team-id"
            )
        if not args.team_name or not args.home_region:
            raise RuntimeError("--team-name and --home-region are required to create a team")
        command = [
            "team",
            "create",
            "--name",
            args.team_name,
            "--home-region",
            args.home_region,
        ]
        if args.team_slug:
            command += ["--slug", args.team_slug]
        log(f"creating team {args.team_name!r} in region {args.home_region!r}")
        team = cli.run_json(command)
        created = True
    return team, created, current_team_id


def ensure_template_mount(template: dict[str, Any], mount_path: str) -> None:
    spec = template.get("spec") or {}
    volume_mounts = spec.get("volumeMounts") or []
    declared = {
        str(item.get("mountPath", ""))
        for item in volume_mounts
        if isinstance(item, dict)
    }
    if mount_path not in declared:
        raise RuntimeError(
            f"template does not declare mount path {mount_path!r}; declared paths: {sorted(declared)}"
        )


def timed_json(cli: S0CLI, args: list[str], *, timeout: int = 300) -> tuple[Any, float]:
    started = time.perf_counter()
    value = cli.run_json(args, timeout=timeout)
    return value, time.perf_counter() - started


def upload_text(
    cli: S0CLI,
    sandbox_id: str,
    remote_path: str,
    content: str,
    *,
    prefix: str,
) -> None:
    local_path = ""
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            prefix=prefix,
            suffix=".py",
            delete=False,
        ) as handle:
            handle.write(content)
            local_path = handle.name
        cli.run(
            [
                "sandbox",
                "files",
                "upload",
                local_path,
                remote_path,
                "--sandbox-id",
                sandbox_id,
            ],
            timeout=300,
        )
    finally:
        if local_path:
            try:
                os.unlink(local_path)
            except FileNotFoundError:
                pass


def run_remote_json_job(
    cli: S0CLI,
    sandbox_id: str,
    *,
    remote_path: str,
    script: str,
    config: dict[str, Any],
    job_name: str,
    command_timeout_seconds: int,
    upload_prefix: str,
) -> tuple[dict[str, Any], float]:
    upload_text(
        cli,
        sandbox_id,
        remote_path,
        script,
        prefix=upload_prefix,
    )
    run_id = str(int(time.time() * 1000))
    safe_job_name = job_name.replace("/", "-")
    result_path = f"/tmp/{safe_job_name}-result-{run_id}.json"
    error_path = f"/tmp/{safe_job_name}-error-{run_id}.log"
    status_path = f"/tmp/{safe_job_name}-status-{run_id}"
    launcher = (
        'python3 "$1" "$2" >"$3" 2>"$4"; '
        'status=$?; printf "%s\\n" "$status" >"$5"'
    )
    started = time.perf_counter()
    cli.run(
        [
            "sandbox",
            "exec",
            sandbox_id,
            "--ttl",
            str(command_timeout_seconds),
            "--no-wait",
            "--",
            "sh",
            "-c",
            launcher,
            safe_job_name,
            remote_path,
            json.dumps(config, separators=(",", ":")),
            result_path,
            error_path,
            status_path,
        ],
        timeout=120,
    )

    deadline = time.monotonic() + command_timeout_seconds
    next_progress = time.monotonic() + 60
    exit_code: int | None = None
    last_poll_error = ""
    while time.monotonic() < deadline:
        poll = cli.run(
            [
                "sandbox",
                "exec",
                sandbox_id,
                "--",
                "sh",
                "-c",
                'if [ -f "$1" ]; then cat "$1"; else printf pending; fi',
                f"{safe_job_name}-poll",
                status_path,
            ],
            timeout=120,
            check=False,
        )
        if poll.returncode == 0:
            status = poll.stdout.strip()
            if status != "pending":
                try:
                    exit_code = int(status)
                except ValueError as exc:
                    raise RuntimeError(f"invalid remote benchmark exit status: {status!r}") from exc
                break
        else:
            last_poll_error = poll.stderr.strip() or poll.stdout.strip()
        now = time.monotonic()
        if now >= next_progress:
            log(f"{safe_job_name} still running after {time.perf_counter() - started:.0f}s")
            next_progress = now + 60
        time.sleep(5)
    if exit_code is None:
        detail = f"; last poll error: {last_poll_error}" if last_poll_error else ""
        raise TimeoutError(
            f"remote job did not finish within {command_timeout_seconds}s{detail}"
        )

    elapsed = time.perf_counter() - started
    output = cli.run(
        ["sandbox", "exec", sandbox_id, "--", "cat", result_path],
        timeout=120,
        check=False,
    )
    error = cli.run(
        ["sandbox", "exec", sandbox_id, "--", "cat", error_path],
        timeout=120,
        check=False,
    )
    if exit_code != 0:
        raise RuntimeError(
            f"remote benchmark exited with code {exit_code}\n"
            f"stdout:\n{output.stdout}\n"
            f"stderr:\n{error.stdout or error.stderr}"
        )
    if output.returncode != 0:
        raise RuntimeError(
            "remote job result file could not be read\n"
            f"stdout:\n{output.stdout}\n"
            f"stderr:\n{output.stderr}"
        )
    try:
        return json.loads(output.stdout), elapsed
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "remote job did not return JSON\n"
            f"stdout:\n{output.stdout}\n"
            f"stderr:\n{error.stdout or error.stderr}"
        ) from exc


def run_remote_benchmark(
    cli: S0CLI,
    args: argparse.Namespace,
    sandbox_id: str,
) -> tuple[dict[str, Any], float]:
    config = {
        "file_count": args.file_count,
        "file_size": args.file_size,
        "parallelism": args.parallelism,
        "files_per_dir": args.files_per_dir,
        "rounds": args.rounds,
        "precreate_dirs": bool(args.precreate_dirs),
        "mount_path": args.mount_path,
        "local_root": "/tmp",
    }
    return run_remote_json_job(
        cli,
        sandbox_id,
        remote_path="/tmp/sandbox0-volume-small-file-bench.py",
        script=REMOTE_BENCHMARK,
        config=config,
        job_name="sandbox0-volume-small-file",
        command_timeout_seconds=args.command_timeout_seconds,
        upload_prefix="s0-volume-small-file-",
    )


def ratios(benchmark: dict[str, Any]) -> dict[str, float]:
    by_backend = {
        str(item["backend"]): item
        for item in benchmark.get("results", [])
        if isinstance(item, dict)
    }
    local = by_backend.get("localfs")
    s0fs = by_backend.get("s0fs")
    if not local or not s0fs:
        return {}

    result = {}
    for phase in ("write", "read", "list_stat"):
        local_ops = float(local[phase]["ops_per_second"])
        s0fs_ops = float(s0fs[phase]["ops_per_second"])
        result[f"{phase}_ops_s0fs_over_local"] = s0fs_ops / local_ops if local_ops else 0.0
    return result


def delete_volume_eventually(
    cli: S0CLI,
    volume_id: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    started = time.perf_counter()
    deadline = time.monotonic() + timeout_seconds
    attempts = 0
    last_error = ""
    while True:
        attempts += 1
        completed = cli.run(["volume", "delete", volume_id], timeout=120, check=False)
        if completed.returncode == 0:
            return {
                "deleted": True,
                "attempts": attempts,
                "elapsed_seconds": time.perf_counter() - started,
            }
        last_error = completed.stderr.strip() or completed.stdout.strip()
        if time.monotonic() >= deadline:
            return {
                "deleted": False,
                "attempts": attempts,
                "elapsed_seconds": time.perf_counter() - started,
                "error": last_error,
            }
        time.sleep(2)


def cleanup_resources(
    cli: S0CLI,
    sandbox_id: str,
    volume_id: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    cleanup: dict[str, Any] = {
        "sandbox": {"deleted": not bool(sandbox_id)},
        "volume": {"deleted": not bool(volume_id)},
    }
    if sandbox_id:
        started = time.perf_counter()
        completed = cli.run(["sandbox", "delete", sandbox_id], timeout=120, check=False)
        cleanup["sandbox"] = {
            "deleted": completed.returncode == 0,
            "elapsed_seconds": time.perf_counter() - started,
        }
        if completed.returncode != 0:
            cleanup["sandbox"]["error"] = completed.stderr.strip() or completed.stdout.strip()
    if volume_id:
        cleanup["volume"] = delete_volume_eventually(cli, volume_id, timeout_seconds)
    return cleanup


def print_summary(result: dict[str, Any]) -> None:
    print()
    print("| Target | Write ops/s | Write p95 ms | Read ops/s | Read p95 ms | list+stat ops/s | list+stat p95 ms |")
    print("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for item in result["benchmark"]["results"]:
        print(
            "| {target} | {write_ops:.1f} | {write_p95:.2f} | {read_ops:.1f} | "
            "{read_p95:.2f} | {stat_ops:.1f} | {stat_p95:.2f} |".format(
                target=item["target"],
                write_ops=item["write"]["ops_per_second"],
                write_p95=item["write"]["p95_ms"],
                read_ops=item["read"]["ops_per_second"],
                read_p95=item["read"]["p95_ms"],
                stat_ops=item["list_stat"]["ops_per_second"],
                stat_p95=item["list_stat"]["p95_ms"],
            )
        )
    print()
    for name, value in sorted(result["comparison"].items()):
        print(f"{name}: {value:.4f}")


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
            [
                "volume",
                "create",
                "--access-mode",
                "RWO",
                "--backend",
                "s0fs",
            ],
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
            "running {count} files x {size} bytes, parallelism {parallelism}, "
            "{rounds} round(s)".format(
                count=args.file_count,
                size=args.file_size,
                parallelism=args.parallelism,
                rounds=args.rounds,
            )
        )
        benchmark, benchmark_command_seconds = run_remote_benchmark(
            cli,
            args,
            sandbox_id,
        )
        result = {
            "schema_version": 1,
            "benchmark": benchmark,
            "comparison": ratios(benchmark),
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
                "bootstrap_mounts": sandbox.get(
                    "bootstrap_mounts",
                    sandbox.get("BootstrapMounts"),
                ),
                "orchestration_seconds": {
                    "volume_create": volume_create_seconds,
                    "sandbox_create_and_ready": sandbox_create_seconds,
                    "benchmark_command": benchmark_command_seconds,
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
                cleanup["team_restore_error"] = (
                    restored.stderr.strip() or restored.stdout.strip()
                )
        else:
            cleanup["original_team_restored"] = True

    if result is not None:
        result["cleanup"] = cleanup
        if args.json_output:
            with open(args.json_output, "w", encoding="utf-8") as handle:
                json.dump(result, handle, indent=2, sort_keys=True)
                handle.write("\n")
        print(json.dumps(result, indent=2, sort_keys=True))
        print_summary(result)

    cleanup_failed = (
        not args.keep_resources
        and (
            not cleanup.get("sandbox", {}).get("deleted", False)
            or not cleanup.get("volume", {}).get("deleted", False)
        )
    ) or not cleanup.get("original_team_restored", False)
    if run_error is not None:
        raise run_error
    if cleanup_failed:
        raise RuntimeError(f"benchmark completed but cleanup failed: {cleanup}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
