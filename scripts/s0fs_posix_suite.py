#!/usr/bin/env python3
"""Run S0FS POSIX/FUSE compatibility suites against a real sandbox volume.

This runner is intentionally a test harness for issue 611. It selects and runs
trusted suites/workloads, emits structured results, and avoids fixing or hiding
S0FS behavior.
"""

import argparse
import base64
import copy
import http.client
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple, Union


REMOTE_RUNNER = r'''
import hashlib
import concurrent.futures
import json
import os
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path


def run(cmd, cwd=None, timeout=900, check=False):
    started = time.time()
    completed = subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    result = {
        "cmd": cmd,
        "cwd": cwd,
        "exit_code": completed.returncode,
        "elapsed_seconds": time.time() - started,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }
    if check and completed.returncode != 0:
        raise RuntimeError(json.dumps(trim_command_result(result), sort_keys=True))
    return result


def trim_text(value, max_lines=240, max_chars=40000):
    if not value:
        return ""
    lines = value.splitlines()
    if len(lines) > max_lines:
        lines = lines[-max_lines:]
    out = "\n".join(lines)
    if len(out) > max_chars:
        out = out[-max_chars:]
    return out


def trim_command_result(result):
    return {
        "cmd": result["cmd"],
        "cwd": result["cwd"],
        "exit_code": result["exit_code"],
        "elapsed_seconds": result["elapsed_seconds"],
        "stdout_tail": trim_text(result["stdout"]),
        "stderr_tail": trim_text(result["stderr"]),
    }


def command_exists(name):
    return shutil.which(name) is not None


def apt_install(packages):
    if not packages:
        return []
    if os.geteuid() != 0:
        return [{"passed": False, "reason": "not running as root", "packages": packages}]
    if not command_exists("apt-get"):
        return [{"passed": False, "reason": "apt-get not found", "packages": packages}]
    update = run(["apt-get", "update"], timeout=600)
    install = run(["apt-get", "install", "-y", "--no-install-recommends", *packages], timeout=1200)
    return [trim_command_result(update), trim_command_result(install)]


def ensure_empty_dir(path):
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def file_digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_size(value):
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip().lower()
    multipliers = {
        "k": 1000,
        "kb": 1000,
        "kib": 1024,
        "m": 1000 ** 2,
        "mb": 1000 ** 2,
        "mib": 1024 ** 2,
        "g": 1000 ** 3,
        "gb": 1000 ** 3,
        "gib": 1024 ** 3,
    }
    for suffix, multiplier in sorted(multipliers.items(), key=lambda item: len(item[0]), reverse=True):
        if text.endswith(suffix):
            return int(float(text[:-len(suffix)]) * multiplier)
    return int(text)


def deterministic_chunks(index, size, salt, chunk_size=1024 * 1024):
    remaining = size
    counter = 0
    while remaining > 0:
        seed = ("%s:%d:%d" % (salt, index, counter)).encode("utf-8")
        block = hashlib.sha256(seed).digest()
        repeats = min(remaining, chunk_size)
        payload = (block * ((repeats // len(block)) + 1))[:repeats]
        yield payload
        remaining -= repeats
        counter += 1


def deterministic_file_digest(index, size, salt):
    h = hashlib.sha256()
    for chunk in deterministic_chunks(index, size, salt):
        h.update(chunk)
    return h.hexdigest()


def bulk_relpath(index):
    return "d%03d/d%03d/file%05d.bin" % ((index // 1000) % 1000, (index // 100) % 10, index)


def bulk_size(index, count, total_bytes):
    base = total_bytes // count
    extra = total_bytes % count
    return base + (1 if index < extra else 0)


def aggregate_manifest(entries):
    h = hashlib.sha256()
    for rel, size, digest in entries:
        h.update(("%s\t%d\t%s\n" % (rel, size, digest)).encode("utf-8"))
    return h.hexdigest()


def write_bulk_file(root, index, count, total_bytes, salt, use_temp_rename, fsync_every, post_write_stat):
    rel = bulk_relpath(index)
    size = bulk_size(index, count, total_bytes)
    path = os.path.join(root, rel)
    h = hashlib.sha256()
    tmp_path = path + ".tmp-%d" % os.getpid() if use_temp_rename else path
    phase = "mkdir"
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        phase = "open"
        with open(tmp_path, "wb") as handle:
            phase = "write"
            for chunk in deterministic_chunks(index, size, salt):
                handle.write(chunk)
                h.update(chunk)
            if fsync_every and index % fsync_every == 0:
                phase = "fsync"
                handle.flush()
                os.fsync(handle.fileno())
        if use_temp_rename:
            phase = "replace"
            os.replace(tmp_path, path)
        if post_write_stat:
            phase = "post_stat"
            st = os.stat(path)
            if st.st_size != size:
                raise RuntimeError("post-write size=%d want=%d" % (st.st_size, size))
            if use_temp_rename and os.path.exists(tmp_path):
                raise RuntimeError("temporary path still exists after replace: %s" % tmp_path)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("%s: %r" % (phase, exc)) from exc
    return rel, size, h.hexdigest()


def verify_bulk_file(root, index, count, total_bytes, salt):
    rel = bulk_relpath(index)
    size = bulk_size(index, count, total_bytes)
    path = os.path.join(root, rel)
    st = os.stat(path)
    actual_digest = file_digest(path)
    expected_digest = deterministic_file_digest(index, size, salt)
    passed = st.st_size == size and actual_digest == expected_digest
    return {
        "rel": rel,
        "size": size,
        "actual_size": st.st_size,
        "expected_sha256": expected_digest,
        "actual_sha256": actual_digest,
        "passed": passed,
    }


def tree_digest(root):
    root_path = Path(root)
    entries = []
    for path in sorted(root_path.rglob("*")):
        rel = path.relative_to(root_path).as_posix()
        if rel == ".git" or rel.startswith(".git/"):
            continue
        st = path.lstat()
        mode = stat.S_IFMT(st.st_mode)
        if stat.S_ISDIR(st.st_mode):
            entries.append(("dir", rel, oct(st.st_mode & 0o7777), ""))
        elif stat.S_ISLNK(st.st_mode):
            entries.append(("symlink", rel, oct(st.st_mode & 0o7777), os.readlink(path)))
        elif stat.S_ISREG(st.st_mode):
            entries.append(("file", rel, oct(st.st_mode & 0o7777), str(st.st_size), file_digest(path)))
        else:
            entries.append(("other", rel, oct(mode), oct(st.st_mode & 0o7777)))
    payload = "\n".join("\t".join(item) for item in entries).encode("utf-8")
    return {
        "sha256": sha256_bytes(payload),
        "entries": len(entries),
    }


def statfs_snapshot(path):
    vfs = os.statvfs(path)
    return {
        "f_bsize": vfs.f_bsize,
        "f_blocks": vfs.f_blocks,
        "f_bfree": vfs.f_bfree,
        "f_bavail": vfs.f_bavail,
        "f_files": vfs.f_files,
        "f_ffree": vfs.f_ffree,
    }


def target_roots(cfg):
    return [
        {
            "name": "pod-local-overlay",
            "backend": "localfs",
            "path": os.path.join(cfg["local_root"], "s0fs-posix-local"),
        },
        {
            "name": "mounted-s0fs-volume",
            "backend": "s0fs",
            "path": os.path.join(cfg["mount_path"], "s0fs-posix-suite"),
        },
    ]


def run_one_smoke_target(target):
    root = target["path"]
    ensure_empty_dir(root)
    checks = []

    def record(name, fn):
        started = time.time()
        try:
            detail = fn()
            checks.append({"name": name, "passed": True, "elapsed_seconds": time.time() - started, "detail": detail})
        except Exception as exc:  # noqa: BLE001
            checks.append({"name": name, "passed": False, "elapsed_seconds": time.time() - started, "error": repr(exc)})

    def basic_rw():
        path = os.path.join(root, "basic.txt")
        with open(path, "wb") as handle:
            handle.write(b"hello")
            handle.flush()
            os.fsync(handle.fileno())
        with open(path, "rb") as handle:
            data = handle.read()
        if data != b"hello":
            raise AssertionError(f"unexpected data {data!r}")
        return {"sha256": sha256_bytes(data)}

    def append_and_truncate():
        path = os.path.join(root, "append.txt")
        with open(path, "wb") as handle:
            handle.write(b"a")
        fd = os.open(path, os.O_WRONLY | os.O_APPEND)
        try:
            os.write(fd, b"bc")
            os.fsync(fd)
        finally:
            os.close(fd)
        with open(path, "r+b") as handle:
            handle.truncate(2)
        data = Path(path).read_bytes()
        if data != b"ab":
            raise AssertionError(f"append/truncate mismatch {data!r}")
        return {"size": len(data), "sha256": sha256_bytes(data)}

    def rename_link_symlink():
        src = os.path.join(root, "source.txt")
        renamed = os.path.join(root, "renamed.txt")
        hard = os.path.join(root, "hardlink.txt")
        sym = os.path.join(root, "symlink.txt")
        Path(src).write_bytes(b"link-data")
        os.rename(src, renamed)
        os.link(renamed, hard)
        os.symlink("renamed.txt", sym)
        if Path(hard).read_bytes() != b"link-data":
            raise AssertionError("hardlink readback mismatch")
        if Path(sym).read_bytes() != b"link-data":
            raise AssertionError("symlink readback mismatch")
        return {"renamed_inode": os.stat(renamed).st_ino, "hardlink_nlink": os.stat(renamed).st_nlink}

    def negative_lookup_then_create():
        path = os.path.join(root, "negative-then-create.txt")
        try:
            os.stat(path)
            raise AssertionError("path unexpectedly exists")
        except FileNotFoundError:
            pass
        Path(path).write_bytes(b"created-after-enoent")
        data = Path(path).read_bytes()
        if data != b"created-after-enoent":
            raise AssertionError("negative lookup create readback mismatch")
        return {"sha256": sha256_bytes(data)}

    def directory_digest():
        os.makedirs(os.path.join(root, "dir", "sub"), exist_ok=True)
        Path(os.path.join(root, "dir", "sub", "file.bin")).write_bytes(bytes(range(64)))
        return tree_digest(root)

    def statfs_check():
        vfs = os.statvfs(root)
        return {
            "f_bsize": vfs.f_bsize,
            "f_blocks": vfs.f_blocks,
            "f_bfree": vfs.f_bfree,
            "f_bavail": vfs.f_bavail,
            "f_files": vfs.f_files,
            "f_ffree": vfs.f_ffree,
        }

    record("basic_read_after_write_fsync", basic_rw)
    record("o_append_and_truncate", append_and_truncate)
    record("rename_hardlink_symlink", rename_link_symlink)
    record("negative_lookup_then_create", negative_lookup_then_create)
    record("directory_tree_digest", directory_digest)
    record("statfs", statfs_check)
    return {
        "target": target,
        "passed": all(item["passed"] for item in checks),
        "checks": checks,
    }


def run_smoke(cfg):
    results = [run_one_smoke_target(target) for target in target_roots(cfg)]
    return {
        "suite": "smoke",
        "passed": all(item["passed"] for item in results),
        "targets": results,
    }


def prepare_git_source(work_root, file_count, file_size):
    source = os.path.join(work_root, "git-source")
    bare = os.path.join(work_root, "git-source.git")
    ensure_empty_dir(work_root)
    os.makedirs(source, exist_ok=True)
    for index in range(file_count):
        directory = os.path.join(source, "dir%03d" % (index // 100))
        os.makedirs(directory, exist_ok=True)
        payload = hashlib.sha256(("s0fs-git-%d" % index).encode("utf-8")).digest()
        repeats = (file_size // len(payload)) + 1
        data = (payload * repeats)[:file_size]
        Path(os.path.join(directory, "file%05d.bin" % index)).write_bytes(data)
    commands = [
        run(["git", "init", "-q", source], timeout=120),
        run(["git", "-C", source, "config", "user.email", "s0fs-suite@example.com"], timeout=120),
        run(["git", "-C", source, "config", "user.name", "S0FS Suite"], timeout=120),
        run(["git", "-C", source, "add", "."], timeout=300),
        run(["git", "-C", source, "commit", "-qm", "initial"], timeout=300),
        run(["git", "clone", "--bare", source, bare], timeout=300),
        run(["git", "-C", bare, "repack", "-adb"], timeout=300),
    ]
    for command in commands:
        if command["exit_code"] != 0:
            raise RuntimeError(json.dumps(trim_command_result(command), sort_keys=True))
    return source, bare


def pack_info(repo):
    pack_dir = Path(repo) / ".git" / "objects" / "pack"
    items = []
    if not pack_dir.exists():
        return items
    for path in sorted(pack_dir.iterdir()):
        if path.is_file():
            items.append({"name": path.name, "size": path.stat().st_size, "sha256": file_digest(path)})
    return items


def run_git_target(target, bare_repo, repeat):
    dest = os.path.join(target["path"], "git-integrity-%02d" % repeat)
    shutil.rmtree(dest, ignore_errors=True)
    clone = run(
        [
            "git",
            "-c",
            "core.logAllRefUpdates=false",
            "clone",
            "--depth=1",
            "file://" + bare_repo,
            dest,
        ],
        timeout=900,
    )
    commands = [trim_command_result(clone)]
    if clone["exit_code"] != 0:
        return {
            "target": target,
            "repeat": repeat,
            "passed": False,
            "commands": commands,
        }
    fsck = run(["git", "-C", dest, "fsck", "--full"], timeout=300)
    commands.append(trim_command_result(fsck))
    verify_results = []
    for idx in sorted((Path(dest) / ".git" / "objects" / "pack").glob("*.idx")):
        verify = run(["git", "-C", dest, "verify-pack", "-v", str(idx)], timeout=300)
        verify_results.append(trim_command_result(verify))
    passed = fsck["exit_code"] == 0 and all(item["exit_code"] == 0 for item in verify_results)
    digest = tree_digest(dest)
    return {
        "target": target,
        "repeat": repeat,
        "passed": passed,
        "commands": commands,
        "verify_pack": verify_results,
        "tree_digest": digest,
        "pack_files": pack_info(dest),
    }


def run_git_integrity(cfg):
    work_root = os.path.join(cfg["work_root"], "git-integrity")
    _, bare = prepare_git_source(work_root, int(cfg["git_file_count"]), int(cfg["git_file_size"]))
    results = []
    for target in target_roots(cfg):
        ensure_empty_dir(target["path"])
        for repeat in range(int(cfg["git_repeats"])):
            results.append(run_git_target(target, bare, repeat))
    digests = {}
    for item in results:
        if item.get("tree_digest"):
            digests.setdefault(item["repeat"], {})[item["target"]["name"]] = item["tree_digest"]["sha256"]
    parity = []
    for repeat, values in sorted(digests.items()):
        unique = sorted(set(values.values()))
        parity.append({"repeat": repeat, "passed": len(unique) == 1, "digests": values})
    return {
        "suite": "git-integrity",
        "passed": all(item["passed"] for item in results) and all(item["passed"] for item in parity),
        "source": {
            "file_count": int(cfg["git_file_count"]),
            "file_size": int(cfg["git_file_size"]),
        },
        "results": results,
        "tree_digest_parity": parity,
    }


def run_bulk_target(target, cfg):
    root = os.path.join(target["path"], "bulk-smallfile")
    ensure_empty_dir(root)
    count = int(cfg["bulk_file_count"])
    total_bytes = parse_size(cfg["bulk_total_bytes"])
    concurrency = max(1, int(cfg["bulk_concurrency"]))
    salt = cfg.get("bulk_salt") or "s0fs-bulk-smallfile"
    fsync_every = int(cfg.get("bulk_fsync_every") or 0)
    started = time.time()
    statfs_before = statfs_snapshot(target["path"])
    write_entries = []
    write_errors = []
    write_error_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(
                write_bulk_file,
                root,
                index,
                count,
                total_bytes,
                salt,
                bool(cfg.get("bulk_temp_rename")),
                fsync_every,
                bool(cfg.get("bulk_post_write_stat")),
            ): index
            for index in range(count)
        }
        for future in concurrent.futures.as_completed(futures):
            index = futures[future]
            try:
                write_entries.append(future.result())
            except Exception as exc:  # noqa: BLE001
                write_error_count += 1
                if len(write_errors) < 20:
                    write_errors.append(
                        {
                            "phase": "write",
                            "index": index,
                            "rel": bulk_relpath(index),
                            "error": repr(exc),
                        }
                    )
    write_elapsed = time.time() - started
    write_entries.sort()
    manifest_sha = aggregate_manifest(write_entries)
    statfs_after_write = statfs_snapshot(target["path"])

    verify_started = time.time()
    failures = []
    failure_count = 0
    verified_entries = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(verify_bulk_file, root, index, count, total_bytes, salt): index
            for index in range(count)
        }
        for future in concurrent.futures.as_completed(futures):
            index = futures[future]
            try:
                item = future.result()
                if not item["passed"]:
                    failure_count += 1
                    if len(failures) < 20:
                        failures.append(item)
                verified_entries.append((item["rel"], item["actual_size"], item["actual_sha256"]))
            except Exception as exc:  # noqa: BLE001
                failure_count += 1
                item = {
                    "phase": "verify",
                    "index": index,
                    "rel": bulk_relpath(index),
                    "error": repr(exc),
                }
                if len(failures) < 20:
                    failures.append(item)
                verified_entries.append((item["rel"], -1, item["error"]))
    verify_elapsed = time.time() - verify_started
    verified_entries.sort()
    verified_manifest_sha = aggregate_manifest(verified_entries)
    tree = tree_digest(root)
    statfs_after_verify = statfs_snapshot(target["path"])

    delete_result = None
    if cfg.get("bulk_delete_after"):
        delete_started = time.time()
        shutil.rmtree(root)
        delete_result = {
            "elapsed_seconds": time.time() - delete_started,
            "statfs_after_delete": statfs_snapshot(target["path"]),
        }

    return {
        "target": target,
        "passed": write_error_count == 0
        and failure_count == 0
        and len(write_entries) == count
        and len(verified_entries) == count
        and manifest_sha == verified_manifest_sha,
        "file_count": count,
        "total_bytes": total_bytes,
        "concurrency": concurrency,
        "written_files": len(write_entries),
        "verified_files": len(verified_entries),
        "write_elapsed_seconds": write_elapsed,
        "verify_elapsed_seconds": verify_elapsed,
        "manifest_sha256": manifest_sha,
        "verified_manifest_sha256": verified_manifest_sha,
        "tree_digest": tree,
        "statfs_before": statfs_before,
        "statfs_after_write": statfs_after_write,
        "statfs_after_verify": statfs_after_verify,
        "delete": delete_result,
        "write_error_count": write_error_count,
        "write_errors": write_errors,
        "failure_count": failure_count,
        "failure_count_sampled": len(failures),
        "failures": failures,
    }


def run_bulk_smallfile_integrity(cfg):
    results = [run_bulk_target(target, cfg) for target in target_roots(cfg)]
    parity = sorted(set(item["verified_manifest_sha256"] for item in results if item.get("verified_manifest_sha256")))
    return {
        "suite": "bulk-smallfile-integrity",
        "passed": all(item["passed"] for item in results) and len(parity) == 1,
        "parity_passed": len(parity) == 1,
        "results": results,
    }


def run_archive_target(target, cfg):
    root = os.path.join(target["path"], "archive-copy-rsync")
    ensure_empty_dir(root)
    source = os.path.join(root, "source")
    extracted = os.path.join(root, "tar-extracted")
    copied = os.path.join(root, "cp-a")
    synced = os.path.join(root, "rsync-a")
    os.makedirs(source, exist_ok=True)
    local_cfg = dict(cfg)
    local_cfg["bulk_file_count"] = int(cfg["archive_file_count"])
    local_cfg["bulk_total_bytes"] = cfg["archive_total_bytes"]
    local_cfg["bulk_concurrency"] = int(cfg["archive_concurrency"])
    local_cfg["bulk_salt"] = "s0fs-archive-copy-rsync"
    run_bulk_target({"name": target["name"], "backend": target["backend"], "path": source}, local_cfg)
    source_digest = tree_digest(source)
    tar_path = os.path.join(root, "tree.tar")
    commands = []
    commands.append(trim_command_result(run(["tar", "-C", source, "-cf", tar_path, "."], timeout=900)))
    os.makedirs(extracted, exist_ok=True)
    commands.append(trim_command_result(run(["tar", "-C", extracted, "-xf", tar_path], timeout=900)))
    commands.append(trim_command_result(run(["cp", "-a", source + "/.", copied], timeout=900)))
    commands.append(trim_command_result(run(["rsync", "-a", source + "/", synced + "/"], timeout=900)))
    digests = {
        "source": source_digest,
        "tar-extracted": tree_digest(extracted),
        "cp-a": tree_digest(copied),
        "rsync-a": tree_digest(synced),
    }
    unique = sorted(set(item["sha256"] for item in digests.values()))
    return {
        "target": target,
        "passed": all(item["exit_code"] == 0 for item in commands) and len(unique) == 1,
        "commands": commands,
        "digests": digests,
    }


def run_archive_copy_rsync(cfg):
    results = [run_archive_target(target, cfg) for target in target_roots(cfg)]
    return {
        "suite": "archive-copy-rsync",
        "passed": all(item["passed"] for item in results),
        "results": results,
    }


def clone_or_update(repo_url, ref, dest):
    if os.path.exists(dest):
        shutil.rmtree(dest)
    clone = run(["git", "clone", "--depth=1", repo_url, dest], timeout=900)
    if clone["exit_code"] != 0:
        return [trim_command_result(clone)], False
    checkout = run(["git", "-C", dest, "fetch", "--depth=1", "origin", ref], timeout=300)
    commands = [trim_command_result(clone), trim_command_result(checkout)]
    if checkout["exit_code"] == 0:
        co = run(["git", "-C", dest, "checkout", "--detach", "FETCH_HEAD"], timeout=300)
    else:
        co = run(["git", "-C", dest, "checkout", "--detach", ref], timeout=300)
    commands.append(trim_command_result(co))
    return commands, co["exit_code"] == 0


def run_pjdfstest(cfg):
    dep_commands = []
    if cfg.get("install_deps"):
        dep_commands = apt_install(["autoconf", "automake", "gcc", "make", "perl", "openssl"])
    source = os.path.join(cfg["work_root"], "pjdfstest-src")
    clone_commands, ok = clone_or_update(
        "https://github.com/pjd/pjdfstest.git",
        cfg["pjdfstest_ref"],
        source,
    )
    if not ok:
        return {
            "suite": "pjdfstest",
            "passed": False,
            "dependency_commands": dep_commands,
            "commands": clone_commands,
        }
    build_commands = [
        trim_command_result(run(["autoreconf", "-ifs"], cwd=source, timeout=300)),
        trim_command_result(run(["./configure"], cwd=source, timeout=300)),
        trim_command_result(run(["make", "pjdfstest"], cwd=source, timeout=300)),
    ]
    if any(item["exit_code"] != 0 for item in build_commands):
        return {
            "suite": "pjdfstest",
            "passed": False,
            "dependency_commands": dep_commands,
            "commands": clone_commands + build_commands,
        }
    results = []
    for target in target_roots(cfg):
        test_root = os.path.join(target["path"], "pjdfstest")
        ensure_empty_dir(test_root)
        cmd = run(["prove", "-rv", os.path.join(source, "tests")], cwd=test_root, timeout=int(cfg["pjdfstest_timeout_seconds"]))
        results.append(
            {
                "target": target,
                "passed": cmd["exit_code"] == 0,
                "command": trim_command_result(cmd),
            }
        )
    return {
        "suite": "pjdfstest",
        "passed": all(item["passed"] for item in results),
        "dependency_commands": dep_commands,
        "commands": clone_commands + build_commands,
        "results": results,
    }


_XFSTESTS_BUILD = None
_IOR_BUILD = None
_FILEBENCH_BUILD = None


def find_first_command(names):
    for name in names:
        path = shutil.which(name)
        if path:
            return path
    return ""


def ensure_xfstests_tools(cfg):
    global _XFSTESTS_BUILD
    if _XFSTESTS_BUILD is not None:
        return _XFSTESTS_BUILD

    source = os.path.join(cfg["work_root"], "xfstests-src")
    tool_paths = {
        "fsx": os.path.join(source, "ltp", "fsx"),
        "fsstress": os.path.join(source, "ltp", "fsstress"),
    }
    if all(os.access(path, os.X_OK) for path in tool_paths.values()):
        _XFSTESTS_BUILD = {
            "available": True,
            "tool_paths": tool_paths,
            "dependency_commands": [],
            "commands": [],
            "reused": True,
        }
        return _XFSTESTS_BUILD

    dep_commands = apt_install(
        [
            "acl",
            "attr",
            "autoconf",
            "automake",
            "bc",
            "e2fsprogs",
            "gawk",
            "gcc",
            "git",
            "indent",
            "libacl1-dev",
            "libaio-dev",
            "libcap-dev",
            "libgdbm-dev",
            "libtool",
            "libtool-bin",
            "liburing-dev",
            "libuuid1",
            "make",
            "pkg-config",
            "psmisc",
            "python3",
            "quota",
            "sed",
            "sqlite3",
            "uuid-dev",
            "uuid-runtime",
            "xfsprogs",
            "xfslibs-dev",
        ]
    )
    clone_commands, ok = clone_or_update(
        "https://github.com/kdave/xfstests.git",
        cfg["xfstests_ref"],
        source,
    )
    if not ok:
        _XFSTESTS_BUILD = {
            "available": False,
            "tool_paths": tool_paths,
            "dependency_commands": dep_commands,
            "commands": clone_commands,
            "reason": "failed to clone or checkout xfstests",
        }
        return _XFSTESTS_BUILD

    build_commands = [
        trim_command_result(run(["make", "configure"], cwd=source, timeout=600)),
        trim_command_result(run(["./configure", "--libexecdir=/usr/lib", "--exec_prefix=/var/lib"], cwd=source, timeout=600)),
        trim_command_result(run(["make", "-C", "lib"], cwd=source, timeout=600)),
        trim_command_result(run(["make", "-C", "ltp", "fsx", "fsstress"], cwd=source, timeout=900)),
    ]
    _XFSTESTS_BUILD = {
        "available": all(os.access(path, os.X_OK) for path in tool_paths.values())
        and all(item["exit_code"] == 0 for item in build_commands),
        "tool_paths": tool_paths,
        "dependency_commands": dep_commands,
        "commands": clone_commands + build_commands,
        "reason": "" if all(item["exit_code"] == 0 for item in build_commands) else "failed to build xfstests ltp tools",
    }
    return _XFSTESTS_BUILD


def resolve_xfstests_tool(cfg, suite, names):
    binary = find_first_command(names)
    if binary:
        return binary, None
    if not cfg.get("install_deps"):
        return "", {
            "available": False,
            "dependency_commands": [],
            "commands": [],
            "reason": "%s binary not found; rerun with --install-deps to build xfstests ltp tools" % suite,
        }
    build = ensure_xfstests_tools(cfg)
    for name in names:
        path = build.get("tool_paths", {}).get(name)
        if path and os.access(path, os.X_OK):
            return path, build
    return "", build


def run_fsx(cfg):
    binary, build = resolve_xfstests_tool(cfg, "fsx", ["fsx", "fsx-linux"])
    if not binary:
        return {
            "suite": "fsx",
            "passed": False,
            "skipped": False,
            "reason": (build or {}).get("reason", "fsx/fsx-linux binary not found"),
            "dependency_commands": (build or {}).get("dependency_commands", []),
            "commands": (build or {}).get("commands", []),
        }
    results = []
    for target in target_roots(cfg):
        ensure_empty_dir(target["path"])
        test_file = os.path.join(target["path"], "fsx-test.bin")
        command = [binary]
        if cfg.get("fsx_avoid_unsupported"):
            command += ["-F", "-J", "-B", "-0"]
        command += ["-N", str(cfg["fsx_operations"]), "-l", str(cfg["fsx_file_size"]), test_file]
        cmd = run(command, timeout=900)
        results.append({"target": target, "passed": cmd["exit_code"] == 0, "command": trim_command_result(cmd)})
    return {
        "suite": "fsx",
        "passed": all(item["passed"] for item in results),
        "binary": binary,
        "dependency_commands": (build or {}).get("dependency_commands", []),
        "commands": (build or {}).get("commands", []),
        "results": results,
    }


def run_fsstress(cfg):
    binary, build = resolve_xfstests_tool(cfg, "fsstress", ["fsstress"])
    if not binary:
        return {
            "suite": "fsstress",
            "passed": False,
            "skipped": False,
            "reason": (build or {}).get("reason", "fsstress binary not found"),
            "dependency_commands": (build or {}).get("dependency_commands", []),
            "commands": (build or {}).get("commands", []),
        }
    results = []
    for target in target_roots(cfg):
        ensure_empty_dir(target["path"])
        cmd = run([binary, "-d", target["path"], "-n", str(cfg["fsstress_operations"]), "-p", str(cfg["fsstress_processes"])], timeout=900)
        results.append({"target": target, "passed": cmd["exit_code"] == 0, "command": trim_command_result(cmd)})
    return {
        "suite": "fsstress",
        "passed": all(item["passed"] for item in results),
        "binary": binary,
        "dependency_commands": (build or {}).get("dependency_commands", []),
        "commands": (build or {}).get("commands", []),
        "results": results,
    }


def resolve_apt_tool(binary_name, package_names, cfg):
    binary = find_first_command([binary_name])
    dep_commands = []
    if binary:
        return binary, dep_commands, ""
    if not cfg.get("install_deps"):
        return "", dep_commands, "%s binary not found; rerun with --install-deps" % binary_name
    dep_commands = apt_install(package_names)
    binary = find_first_command([binary_name])
    if binary:
        return binary, dep_commands, ""
    return "", dep_commands, "%s binary still not found after installing %s" % (binary_name, ",".join(package_names))


def ensure_ior_mdtest(cfg):
    global _IOR_BUILD
    if _IOR_BUILD is not None:
        return _IOR_BUILD

    source = os.path.join(cfg["work_root"], "ior-src")
    mdtest_path = os.path.join(source, "src", "mdtest")
    if os.access(mdtest_path, os.X_OK):
        _IOR_BUILD = {
            "available": True,
            "binary": mdtest_path,
            "dependency_commands": [],
            "commands": [],
            "reused": True,
        }
        return _IOR_BUILD

    dep_commands = apt_install(
        [
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
    )
    clone_commands, ok = clone_or_update(
        "https://github.com/hpc/ior.git",
        cfg["ior_ref"],
        source,
    )
    if not ok:
        _IOR_BUILD = {
            "available": False,
            "binary": mdtest_path,
            "dependency_commands": dep_commands,
            "commands": clone_commands,
            "reason": "failed to clone or checkout IOR",
        }
        return _IOR_BUILD

    build_commands = [
        trim_command_result(run(["./bootstrap"], cwd=source, timeout=600)),
        trim_command_result(run(["./configure", "--without-mpiio"], cwd=source, timeout=600)),
        trim_command_result(run(["make", "-j", "2"], cwd=source, timeout=1200)),
    ]
    build_ok = os.access(mdtest_path, os.X_OK) and all(item["exit_code"] == 0 for item in build_commands)
    _IOR_BUILD = {
        "available": build_ok,
        "binary": mdtest_path,
        "dependency_commands": dep_commands,
        "commands": clone_commands + build_commands,
        "reason": "" if build_ok else "failed to build IOR mdtest",
    }
    return _IOR_BUILD


def resolve_mdtest_tool(cfg):
    binary = find_first_command(["mdtest"])
    if binary:
        return binary, None
    if not cfg.get("install_deps"):
        return "", {
            "dependency_commands": [],
            "commands": [],
            "reason": "mdtest binary not found; rerun with --install-deps",
        }

    package_commands = apt_install(["ior"])
    binary = find_first_command(["mdtest"])
    if binary:
        return binary, {
            "available": True,
            "dependency_commands": package_commands,
            "commands": [],
            "source": "apt",
        }

    build = ensure_ior_mdtest(cfg)
    build["dependency_commands"] = package_commands + build.get("dependency_commands", [])
    if build.get("available") and os.access(build.get("binary", ""), os.X_OK):
        return build["binary"], build
    return "", build


def run_fio_target(target, binary, cfg):
    root = os.path.join(target["path"], "fio")
    ensure_empty_dir(root)
    workloads = [item.strip() for item in str(cfg["fio_workloads"]).split(",") if item.strip()]
    commands = []
    results = []
    prepared = False
    for workload in workloads:
        if workload in {"read", "randread"} and not prepared:
            prep = [
                binary,
                "--name=s0fs-fio-prepare",
                "--directory=" + root,
                "--rw=write",
                "--bs=" + str(cfg["fio_block_size"]),
                "--size=" + str(cfg["fio_size"]),
                "--numjobs=" + str(cfg["fio_numjobs"]),
                "--ioengine=" + str(cfg["fio_ioengine"]),
                "--group_reporting",
                "--end_fsync=1",
                "--output-format=json",
            ]
            commands.append(trim_command_result(run(prep, timeout=int(cfg["fio_timeout_seconds"]))))
            prepared = commands[-1]["exit_code"] == 0
        command = [
            binary,
            "--name=s0fs-fio-" + workload,
            "--directory=" + root,
            "--rw=" + workload,
            "--bs=" + str(cfg["fio_block_size"]),
            "--size=" + str(cfg["fio_size"]),
            "--numjobs=" + str(cfg["fio_numjobs"]),
            "--ioengine=" + str(cfg["fio_ioengine"]),
            "--group_reporting",
            "--output-format=json",
        ]
        if workload in {"write", "randwrite"}:
            command += ["--verify=crc32c", "--do_verify=1", "--end_fsync=1"]
        cmd = trim_command_result(run(command, timeout=int(cfg["fio_timeout_seconds"])))
        results.append({"workload": workload, "passed": cmd["exit_code"] == 0, "command": cmd})
    return {
        "target": target,
        "passed": all(item["passed"] for item in results) and all(item["exit_code"] == 0 for item in commands),
        "prepare_commands": commands,
        "results": results,
    }


def run_fio(cfg):
    binary, dep_commands, reason = resolve_apt_tool("fio", ["fio"], cfg)
    if not binary:
        return {
            "suite": "fio",
            "passed": False,
            "skipped": False,
            "reason": reason,
            "dependency_commands": dep_commands,
        }
    results = [run_fio_target(target, binary, cfg) for target in target_roots(cfg)]
    return {
        "suite": "fio",
        "passed": all(item["passed"] for item in results),
        "binary": binary,
        "dependency_commands": dep_commands,
        "results": results,
    }


def run_mdtest_target(target, binary, cfg):
    root = os.path.join(target["path"], "mdtest")
    ensure_empty_dir(root)
    command = [
        binary,
        "-d",
        root,
        "-n",
        str(cfg["mdtest_files"]),
        "-F",
        "-u",
    ]
    if int(cfg.get("mdtest_iterations") or 1) > 1:
        command += ["-i", str(cfg["mdtest_iterations"])]
    cmd = trim_command_result(run(command, timeout=int(cfg["mdtest_timeout_seconds"])))
    return {
        "target": target,
        "passed": cmd["exit_code"] == 0,
        "command": cmd,
    }


def run_mdtest(cfg):
    binary, build = resolve_mdtest_tool(cfg)
    if not binary:
        return {
            "suite": "mdtest",
            "passed": False,
            "skipped": False,
            "reason": (build or {}).get("reason", "mdtest binary not found"),
            "dependency_commands": (build or {}).get("dependency_commands", []),
            "commands": (build or {}).get("commands", []),
        }
    results = [run_mdtest_target(target, binary, cfg) for target in target_roots(cfg)]
    return {
        "suite": "mdtest",
        "passed": all(item["passed"] for item in results),
        "binary": binary,
        "dependency_commands": (build or {}).get("dependency_commands", []),
        "commands": (build or {}).get("commands", []),
        "results": results,
    }


def ensure_filebench(cfg):
    global _FILEBENCH_BUILD
    if _FILEBENCH_BUILD is not None:
        return _FILEBENCH_BUILD

    source = os.path.join(cfg["work_root"], "filebench-src")
    install_prefix = os.path.join(cfg["work_root"], "filebench-install")
    binary = os.path.join(install_prefix, "bin", "filebench")
    if os.access(binary, os.X_OK):
        _FILEBENCH_BUILD = {
            "available": True,
            "binary": binary,
            "dependency_commands": [],
            "commands": [],
            "reused": True,
        }
        return _FILEBENCH_BUILD

    dep_commands = apt_install(["autoconf", "automake", "bison", "flex", "gcc", "git", "libtool", "make"])
    clone_commands, ok = clone_or_update("https://github.com/filebench/filebench.git", cfg["filebench_ref"], source)
    if not ok:
        _FILEBENCH_BUILD = {
            "available": False,
            "binary": binary,
            "dependency_commands": dep_commands,
            "commands": clone_commands,
            "reason": "failed to clone or checkout Filebench",
        }
        return _FILEBENCH_BUILD

    build_commands = [
        trim_command_result(run(["libtoolize"], cwd=source, timeout=300)),
        trim_command_result(run(["aclocal"], cwd=source, timeout=300)),
        trim_command_result(run(["autoheader"], cwd=source, timeout=300)),
        trim_command_result(run(["automake", "--add-missing"], cwd=source, timeout=300)),
        trim_command_result(run(["autoconf"], cwd=source, timeout=300)),
        trim_command_result(run(["./configure", "--prefix=" + install_prefix], cwd=source, timeout=600)),
        trim_command_result(run(["make", "-j", "2"], cwd=source, timeout=1200)),
        trim_command_result(run(["make", "install"], cwd=source, timeout=1200)),
    ]
    build_ok = os.access(binary, os.X_OK) and all(item["exit_code"] == 0 for item in build_commands)
    _FILEBENCH_BUILD = {
        "available": build_ok,
        "binary": binary,
        "dependency_commands": dep_commands,
        "commands": clone_commands + build_commands,
        "reason": "" if build_ok else "failed to build Filebench",
    }
    return _FILEBENCH_BUILD


def resolve_filebench_tool(cfg):
    binary = find_first_command(["filebench"])
    if binary:
        return binary, None
    if not cfg.get("install_deps"):
        return "", {
            "dependency_commands": [],
            "commands": [],
            "reason": "filebench binary not found; rerun with --install-deps",
        }

    package_commands = apt_install(["filebench"])
    binary = find_first_command(["filebench"])
    if binary:
        return binary, {
            "available": True,
            "dependency_commands": package_commands,
            "commands": [],
            "source": "apt",
        }

    build = ensure_filebench(cfg)
    build["dependency_commands"] = package_commands + build.get("dependency_commands", [])
    if build.get("available") and os.access(build.get("binary", ""), os.X_OK):
        return build["binary"], build
    return "", build


def filebench_workload_text(name, directory, cfg):
    files = int(cfg["filebench_files"])
    file_size = str(cfg["filebench_file_size"])
    runtime = int(cfg["filebench_runtime_seconds"])
    dirwidth = int(cfg["filebench_dirwidth"])
    if name == "createfiles":
        return """
set $dir={directory}
set $nfiles={files}
set $filesize={file_size}
set $iosize={file_size}
set $dirwidth={dirwidth}

define fileset name=bigfileset,path=$dir,size=$filesize,entries=$nfiles,dirwidth=$dirwidth,prealloc=0

define process name=filecreate,instances=1
{{
  thread name=filecreatethread,memsize=10m,instances=1
  {{
    flowop createfile name=createfile1,filesetname=bigfileset,fd=1
    flowop writewholefile name=writefile1,filesetname=bigfileset,fd=1,iosize=$iosize
    flowop closefile name=closefile1,fd=1
    flowop finishoncount name=finish,value=$nfiles
  }}
}}

run {runtime}
""".format(directory=directory, files=files, file_size=file_size, dirwidth=dirwidth, runtime=runtime)
    if name == "statfiles":
        return """
set $dir={directory}
set $nfiles={files}
set $filesize={file_size}
set $dirwidth={dirwidth}

define fileset name=bigfileset,path=$dir,size=$filesize,entries=$nfiles,dirwidth=$dirwidth,prealloc=100

define process name=examinefiles,instances=1
{{
  thread name=examinefilethread,memsize=10m,instances=1
  {{
    flowop statfile name=statfile1,filesetname=bigfileset
    flowop finishoncount name=finish,value=$nfiles
  }}
}}

run {runtime}
""".format(directory=directory, files=files, file_size=file_size, dirwidth=dirwidth, runtime=runtime)
    raise ValueError("unsupported filebench workload %r" % name)


def run_filebench_target(target, binary, cfg):
    root = os.path.join(target["path"], "filebench")
    ensure_empty_dir(root)
    workload_names = [item.strip() for item in str(cfg["filebench_workloads"]).split(",") if item.strip()]
    results = []
    for name in workload_names:
        workload_root = os.path.join(root, name)
        ensure_empty_dir(workload_root)
        workload_file = os.path.join(root, name + ".f")
        Path(workload_file).write_text(filebench_workload_text(name, workload_root, cfg), encoding="utf-8")
        cmd = trim_command_result(run([binary, "-f", workload_file], timeout=int(cfg["filebench_timeout_seconds"])))
        results.append({"workload": name, "passed": cmd["exit_code"] == 0, "command": cmd})
    return {
        "target": target,
        "passed": all(item["passed"] for item in results),
        "results": results,
    }


def run_filebench(cfg):
    binary, build = resolve_filebench_tool(cfg)
    if not binary:
        return {
            "suite": "filebench",
            "passed": False,
            "skipped": False,
            "reason": (build or {}).get("reason", "filebench binary not found"),
            "dependency_commands": (build or {}).get("dependency_commands", []),
            "commands": (build or {}).get("commands", []),
        }
    targets = target_roots(cfg)
    baseline = run_filebench_target(targets[0], binary, cfg)
    if not baseline["passed"]:
        return {
            "suite": "filebench",
            "passed": False,
            "skipped": False,
            "reason": "filebench failed on the pod-local baseline; treating as a tool/environment failure",
            "binary": binary,
            "dependency_commands": (build or {}).get("dependency_commands", []),
            "commands": (build or {}).get("commands", []),
            "baseline_result": baseline,
        }
    results = [baseline] + [run_filebench_target(target, binary, cfg) for target in targets[1:]]
    return {
        "suite": "filebench",
        "passed": all(item["passed"] for item in results),
        "binary": binary,
        "dependency_commands": (build or {}).get("dependency_commands", []),
        "commands": (build or {}).get("commands", []),
        "results": results,
    }


def main():
    cfg = json.loads(sys.argv[1])
    os.makedirs(cfg["work_root"], exist_ok=True)
    suite_map = {
        "smoke": run_smoke,
        "git-integrity": run_git_integrity,
        "bulk-smallfile-integrity": run_bulk_smallfile_integrity,
        "archive-copy-rsync": run_archive_copy_rsync,
        "pjdfstest": run_pjdfstest,
        "fsx": run_fsx,
        "fsstress": run_fsstress,
        "fio": run_fio,
        "mdtest": run_mdtest,
        "filebench": run_filebench,
    }
    results = []
    for suite in cfg["suites"]:
        started = time.time()
        try:
            if suite not in suite_map:
                item = {"suite": suite, "passed": False, "skipped": False, "reason": "suite is selected in catalog but not implemented by this runner yet"}
            else:
                item = suite_map[suite](cfg)
        except Exception as exc:  # noqa: BLE001
            item = {"suite": suite, "passed": False, "error": repr(exc)}
        item["elapsed_seconds"] = time.time() - started
        results.append(item)
    payload = {
        "environment": {
            "cwd": os.getcwd(),
            "uid": os.geteuid(),
            "python": sys.version.split()[0],
            "uname": " ".join(os.uname()),
            "mount_path": cfg["mount_path"],
            "local_root": cfg["local_root"],
        },
        "config": {key: value for key, value in cfg.items() if key not in {"password"}},
        "results": results,
        "passed": bool(results) and all(item.get("passed") for item in results),
    }
    result_path = str(cfg.get("result_path") or "").strip()
    if result_path:
        Path(result_path).parent.mkdir(parents=True, exist_ok=True)
        Path(result_path).write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps({
            "passed": payload["passed"],
            "result_path": result_path,
            "results": [
                {
                    "suite": item.get("suite"),
                    "passed": bool(item.get("passed")),
                    "skipped": bool(item.get("skipped")),
                    "elapsed_seconds": item.get("elapsed_seconds", 0.0),
                }
                for item in results
            ],
        }, sort_keys=True))
    else:
        print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
'''


class APIClient:
    def __init__(self, base_url: str, token: str = "", team_id: str = "") -> None:
        self.base_url = base_url
        self.token = token
        self.team_id = team_id

    def request(self, method: str, path: str, body: Optional[Any] = None) -> Tuple[int, bytes]:
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
        if self.team_id:
            headers["X-Team-ID"] = self.team_id
        conn = http.client.HTTPConnection(parsed.hostname or "127.0.0.1", parsed.port or 80, timeout=180)
        try:
            conn.request(method, parsed.path.rstrip("/") + path, body=payload, headers=headers)
            resp = conn.getresponse()
            data = resp.read()
            return resp.status, data
        finally:
            conn.close()

    def json_request(
        self,
        method: str,
        path: str,
        body: Optional[Any] = None,
        expected: Union[int, Tuple[int, ...]] = 200,
    ) -> Any:
        status, data = self.request(method, path, body)
        expected_values = (expected,) if isinstance(expected, int) else expected
        if status not in expected_values:
            raise RuntimeError(f"{method} {path} failed with {status}: {data.decode('utf-8', 'replace')}")
        if not data:
            return None
        return json.loads(data.decode("utf-8"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run trusted POSIX/FUSE suites against a mounted S0FS sandbox volume.")
    parser.add_argument("--base-url", default="", help="Cluster gateway base URL. Auto-discovered when omitted.")
    parser.add_argument("--email", default="admin@example.com", help="Login email.")
    parser.add_argument("--password", default="", help="Login password. Auto-discovered from admin-password when omitted.")
    parser.add_argument("--kube-context", default="kind-sandbox0-e2e", help="Kubernetes context for kubectl.")
    parser.add_argument("--kube-namespace", default="sandbox0-system", help="Namespace for cluster services.")
    parser.add_argument("--cluster-gateway-service", default="fullmode-cluster-gateway", help="Cluster gateway service name.")
    parser.add_argument("--kind-cluster", default="sandbox0-e2e", help="Kind cluster name used for base URL discovery.")
    parser.add_argument("--template-base", default="default", help="Base SandboxTemplate to clone.")
    parser.add_argument("--template-id", default="", help="Temporary template ID. Defaults to a timestamped name.")
    parser.add_argument("--mount-path", default="/workspace/s0fs-posix", help="Mounted S0FS path inside the sandbox.")
    parser.add_argument("--local-root", default="/tmp", help="Pod-local baseline root inside the sandbox.")
    parser.add_argument("--container", default="", help="Optional container name for kubectl exec.")
    parser.add_argument("--volume-access-mode", default="RWO", choices=("RWO", "ROX"), help="SandboxVolume access mode for the mounted S0FS volume.")
    parser.add_argument("--suite", action="append", dest="suites", help="Suite to run. Defaults to smoke and git-integrity.")
    parser.add_argument("--install-deps", action="store_true", help="Install suite dependencies inside the sandbox with apt-get.")
    parser.add_argument("--keep-resources", action="store_true", help="Do not delete created sandbox, template, or volume.")
    parser.add_argument("--json-output", default="", help="Optional path for JSON results.")
    parser.add_argument("--git-file-count", type=int, default=300, help="Files in the generated git integrity repository.")
    parser.add_argument("--git-file-size", type=int, default=4096, help="Bytes per file in the generated git integrity repository.")
    parser.add_argument("--git-repeats", type=int, default=1, help="Clone repetitions per target for git-integrity.")
    parser.add_argument("--bulk-file-count", type=int, default=30000, help="Files in bulk-smallfile-integrity.")
    parser.add_argument("--bulk-total-bytes", default=str(500 * 1024 * 1024), help="Total bytes in bulk-smallfile-integrity.")
    parser.add_argument("--bulk-concurrency", type=int, default=8, help="Concurrent writers/readers for bulk-smallfile-integrity.")
    parser.add_argument("--bulk-fsync-every", type=int, default=0, help="fsync every Nth bulk file; 0 disables per-file fsync.")
    parser.add_argument("--bulk-no-temp-rename", action="store_true", help="Write bulk files directly instead of temp-file then rename.")
    parser.add_argument("--bulk-post-write-stat", action="store_true", help="Stat each bulk file immediately after writing it.")
    parser.add_argument("--bulk-delete-after", action="store_true", help="Delete bulk-smallfile-integrity files after verification and record statfs.")
    parser.add_argument("--archive-file-count", type=int, default=3000, help="Files generated for archive-copy-rsync.")
    parser.add_argument("--archive-total-bytes", default=str(50 * 1024 * 1024), help="Total bytes generated for archive-copy-rsync.")
    parser.add_argument("--archive-concurrency", type=int, default=4, help="Concurrent writers for archive-copy-rsync source tree.")
    parser.add_argument("--pjdfstest-ref", default="ededbeb2b44929972898afb87474b0937f78a877", help="pjdfstest git ref to run.")
    parser.add_argument("--pjdfstest-timeout-seconds", type=int, default=1800, help="Timeout per pjdfstest target.")
    parser.add_argument("--xfstests-ref", default="cac9fe2b8dc3e6dbc0c2383b9e3b4d3c1b9e7dd0", help="xfstests git ref used to build fsx/fsstress.")
    parser.add_argument("--ior-ref", default="967a9f65109760db8a3ac14a7fdd007f337d2960", help="IOR git ref used to build mdtest.")
    parser.add_argument("--filebench-ref", default="22620e602cbbebad90c0bd041896ebccf70dbf5f", help="Filebench git ref used when building from source.")
    parser.add_argument("--fsx-include-extended-ops", action="store_true", help="Allow fsx to use fallocate, clone, dedupe, and exchange-range operations.")
    parser.add_argument("--fsx-operations", type=int, default=1000, help="fsx operation count when the fsx suite is selected.")
    parser.add_argument("--fsx-file-size", type=int, default=1048576, help="fsx max file size when the fsx suite is selected.")
    parser.add_argument("--fsstress-operations", type=int, default=1000, help="fsstress operation count when selected.")
    parser.add_argument("--fsstress-processes", type=int, default=4, help="fsstress process count when selected.")
    parser.add_argument("--fio-workloads", default="write,read,randwrite,randread", help="Comma-separated fio workloads.")
    parser.add_argument("--fio-size", default="128m", help="fio size per workload.")
    parser.add_argument("--fio-block-size", default="1m", help="fio block size.")
    parser.add_argument("--fio-numjobs", type=int, default=1, help="fio numjobs.")
    parser.add_argument("--fio-ioengine", default="sync", help="fio ioengine.")
    parser.add_argument("--fio-timeout-seconds", type=int, default=1800, help="Timeout per fio command.")
    parser.add_argument("--mdtest-files", type=int, default=10000, help="Files per mdtest target.")
    parser.add_argument("--mdtest-iterations", type=int, default=1, help="mdtest iterations.")
    parser.add_argument("--mdtest-timeout-seconds", type=int, default=1800, help="Timeout per mdtest target.")
    parser.add_argument("--filebench-workloads", default="createfiles,statfiles", help="Comma-separated Filebench workloads.")
    parser.add_argument("--filebench-files", type=int, default=10000, help="Files per Filebench target workload.")
    parser.add_argument("--filebench-file-size", default="4k", help="File size for generated Filebench workloads.")
    parser.add_argument("--filebench-dirwidth", type=int, default=100, help="Directory width for generated Filebench filesets.")
    parser.add_argument("--filebench-runtime-seconds", type=int, default=60, help="Maximum Filebench run duration.")
    parser.add_argument("--filebench-timeout-seconds", type=int, default=1800, help="Timeout per Filebench workload.")
    return parser.parse_args()


def run_command(args: List[str], *, check: bool = True) -> str:
    completed = subprocess.run(
        args,
        check=check,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    return completed.stdout.strip()


def kubectl_command(args: List[str], kube_context: str) -> List[str]:
    cmd = ["kubectl"]
    if kube_context:
        cmd += ["--context", kube_context]
    cmd += args
    return cmd


def kubectl(args: List[str], kube_context: str) -> str:
    return run_command(kubectl_command(args, kube_context))


def kubectl_to_file(args: List[str], kube_context: str, output_path: str) -> None:
    with open(output_path, "w", encoding="utf-8") as handle:
        completed = subprocess.run(
            kubectl_command(args, kube_context),
            check=False,
            stdout=handle,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip())


def discover_base_url(args: argparse.Namespace) -> str:
    if args.base_url:
        return args.base_url.rstrip("/")
    if args.kind_cluster:
        completed = subprocess.run(
            ["docker", "port", f"{args.kind_cluster}-control-plane", "30080/tcp"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
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


def first_team_id(resp: Any) -> str:
    if isinstance(resp, dict):
        if isinstance(resp.get("data"), dict) and isinstance(resp["data"].get("teams"), list):
            return str(resp["data"]["teams"][0]["id"])
        if isinstance(resp.get("teams"), list):
            return str(resp["teams"][0]["id"])
    if isinstance(resp, list):
        return str(resp[0]["id"])
    raise RuntimeError(f"could not find team id in response: {resp!r}")


def normalize_team_template_resources(spec: Dict[str, Any]) -> None:
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


def parse_cpu_cores(value: Any) -> Optional[float]:
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


def wait_for_template_ready(client: APIClient, template_id: str, timeout_seconds: int = 300) -> None:
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


def wait_for_mount(args: argparse.Namespace, namespace: str, pod_name: str, mount_path: str) -> None:
    cmd = ["exec", "-n", namespace, pod_name]
    if args.container:
        cmd += ["-c", args.container]
    cmd += ["--", "sh", "-lc", f'for i in $(seq 1 60); do findmnt "{mount_path}" >/dev/null 2>&1 && exit 0; sleep 1; done; exit 1']
    kubectl(cmd, args.kube_context)


def run_remote_suites(args: argparse.Namespace, pod_name: str, namespace: str, suites: List[str]) -> Any:
    cfg = {
        "suites": suites,
        "install_deps": bool(args.install_deps),
        "mount_path": args.mount_path,
        "local_root": args.local_root,
        "work_root": "/tmp/s0fs-posix-runner",
        "result_path": f"/tmp/s0fs-posix-runner/result-{os.getpid()}-{int(time.time() * 1000)}.json",
        "git_file_count": args.git_file_count,
        "git_file_size": args.git_file_size,
        "git_repeats": args.git_repeats,
        "bulk_file_count": args.bulk_file_count,
        "bulk_total_bytes": args.bulk_total_bytes,
        "bulk_concurrency": args.bulk_concurrency,
        "bulk_fsync_every": args.bulk_fsync_every,
        "bulk_temp_rename": not args.bulk_no_temp_rename,
        "bulk_post_write_stat": args.bulk_post_write_stat,
        "bulk_delete_after": args.bulk_delete_after,
        "archive_file_count": args.archive_file_count,
        "archive_total_bytes": args.archive_total_bytes,
        "archive_concurrency": args.archive_concurrency,
        "pjdfstest_ref": args.pjdfstest_ref,
        "pjdfstest_timeout_seconds": args.pjdfstest_timeout_seconds,
        "xfstests_ref": args.xfstests_ref,
        "ior_ref": args.ior_ref,
        "filebench_ref": args.filebench_ref,
        "fsx_avoid_unsupported": not args.fsx_include_extended_ops,
        "fsx_operations": args.fsx_operations,
        "fsx_file_size": args.fsx_file_size,
        "fsstress_operations": args.fsstress_operations,
        "fsstress_processes": args.fsstress_processes,
        "fio_workloads": args.fio_workloads,
        "fio_size": args.fio_size,
        "fio_block_size": args.fio_block_size,
        "fio_numjobs": args.fio_numjobs,
        "fio_ioengine": args.fio_ioengine,
        "fio_timeout_seconds": args.fio_timeout_seconds,
        "mdtest_files": args.mdtest_files,
        "mdtest_iterations": args.mdtest_iterations,
        "mdtest_timeout_seconds": args.mdtest_timeout_seconds,
        "filebench_workloads": args.filebench_workloads,
        "filebench_files": args.filebench_files,
        "filebench_file_size": args.filebench_file_size,
        "filebench_dirwidth": args.filebench_dirwidth,
        "filebench_runtime_seconds": args.filebench_runtime_seconds,
        "filebench_timeout_seconds": args.filebench_timeout_seconds,
    }
    cmd = ["exec", "-n", namespace, pod_name]
    if args.container:
        cmd += ["-c", args.container]
    cmd += ["--", "python3", "-c", REMOTE_RUNNER, json.dumps(cfg)]
    raw = kubectl(cmd, args.kube_context)
    ack = json.loads(raw)
    if not ack.get("result_path"):
        raise RuntimeError(f"remote runner did not return a result path: {ack!r}")

    cat_cmd = ["exec", "-n", namespace, pod_name]
    if args.container:
        cat_cmd += ["-c", args.container]
    cat_cmd += ["--", "cat", str(ack["result_path"])]

    tmp_path = ""
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
            tmp_path = handle.name
        kubectl_to_file(cat_cmd, args.kube_context, tmp_path)
        with open(tmp_path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass


def print_markdown(result: Any) -> None:
    print()
    print("| Suite | Passed | Skipped | Elapsed seconds |")
    print("| --- | ---: | ---: | ---: |")
    for item in result["results"]:
        print(
            "| {suite} | {passed} | {skipped} | {elapsed:.2f} |".format(
                suite=item.get("suite", "-"),
                passed=str(bool(item.get("passed"))).lower(),
                skipped=str(bool(item.get("skipped"))).lower(),
                elapsed=float(item.get("elapsed_seconds", 0.0)),
            )
        )


def compact_result(result: Any, json_output: str = "") -> Dict[str, Any]:
    return {
        "passed": bool(result.get("passed")),
        "json_output": json_output,
        "sandbox0": result.get("sandbox0", {}),
        "results": [
            {
                "suite": item.get("suite"),
                "passed": bool(item.get("passed")),
                "skipped": bool(item.get("skipped")),
                "elapsed_seconds": item.get("elapsed_seconds", 0.0),
                "reason": item.get("reason", ""),
                "error": item.get("error", ""),
            }
            for item in result.get("results", [])
        ],
    }


def main() -> int:
    args = parse_args()
    suites = args.suites or ["smoke", "git-integrity"]
    if args.git_file_count <= 0 or args.git_file_size <= 0 or args.git_repeats <= 0:
        raise SystemExit("git-file-count, git-file-size, and git-repeats must be positive")

    base_url = discover_base_url(args)
    password = discover_password(args)
    client = APIClient(base_url=base_url)
    login = data(client.json_request("POST", "/auth/login", {"email": args.email, "password": password}))
    client.token = login["access_token"]
    client.team_id = first_team_id(client.json_request("GET", "/teams"))

    template_id = args.template_id or f"s0fs-posix-{int(time.time())}"
    sandbox_id = ""
    volume_id = ""
    created_template = False
    try:
        base = data(client.json_request("GET", f"/api/v1/templates/{args.template_base}"))
        spec = copy.deepcopy(base["spec"])
        normalize_team_template_resources(spec)
        spec["volumeMounts"] = [
            {
                "name": "s0fs-posix-volume",
                "mountPath": args.mount_path,
                "readOnly": False,
            }
        ]
        data(client.json_request("POST", "/api/v1/templates", {"template_id": template_id, "spec": spec}, expected=201))
        created_template = True
        wait_for_template_ready(client, template_id)

        volume = data(
            client.json_request(
                "POST",
                "/api/v1/sandboxvolumes",
                {
                    "access_mode": args.volume_access_mode,
                    "default_posix_uid": 0,
                    "default_posix_gid": 0,
                },
                expected=201,
            )
        )
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
        wait_for_mount(args, namespace, pod_name, args.mount_path)

        result = run_remote_suites(args, pod_name, namespace, suites)
        result["sandbox0"] = {
            "base_url": base_url,
            "template_id": template_id,
            "sandbox_id": sandbox_id,
            "pod_name": pod_name,
            "pod_namespace": namespace,
            "volume_id": volume_id,
            "mount_path": args.mount_path,
            "team_id": client.team_id,
        }
        print(json.dumps(compact_result(result, args.json_output), indent=2, sort_keys=True))
        print_markdown(result)
        if args.json_output:
            with open(args.json_output, "w", encoding="utf-8") as handle:
                json.dump(result, handle, indent=2, sort_keys=True)
        return 0 if result.get("passed") else 2
    finally:
        if not args.keep_resources:
            if sandbox_id:
                try:
                    client.json_request("DELETE", f"/api/v1/sandboxes/{sandbox_id}", expected=(200, 404))
                except Exception as exc:  # noqa: BLE001
                    print(f"warning: delete sandbox {sandbox_id} failed: {exc}", file=sys.stderr)
            if volume_id:
                deadline = time.time() + 90
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
