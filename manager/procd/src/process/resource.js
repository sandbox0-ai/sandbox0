import fs from "node:fs";
import path from "node:path";

function readNumber(file) {
  try {
    const value = fs.readFileSync(file, "utf8").trim();
    if (value === "max") return 0;
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) ? parsed : 0;
  } catch {
    return 0;
  }
}

export function processResourceUsage(pid) {
  const usage = {
    cpu_percent: -1,
    memory_rss: 0,
    memory_vms: 0,
    open_files: 0,
    thread_count: 0,
    container_memory_usage: readNumber("/sys/fs/cgroup/memory.current"),
    container_memory_limit: readNumber("/sys/fs/cgroup/memory.max"),
    container_memory_working_set: readNumber("/sys/fs/cgroup/memory.current"),
    io_read_bytes: 0,
    io_write_bytes: 0,
    memory_bytes: 0
  };
  if (!pid || pid <= 0) return usage;
  try {
    const statm = fs.readFileSync(`/proc/${pid}/statm`, "utf8").trim().split(/\s+/).map(Number);
    const pageSize = 4096;
    usage.memory_vms = (statm[0] || 0) * pageSize;
    usage.memory_rss = (statm[1] || 0) * pageSize;
    usage.memory_bytes = usage.memory_rss;
  } catch {
    // Best effort.
  }
  try {
    usage.open_files = fs.readdirSync(`/proc/${pid}/fd`).length;
  } catch {
    // Best effort.
  }
  try {
    const status = fs.readFileSync(`/proc/${pid}/status`, "utf8");
    const threads = status.match(/^Threads:\s+(\d+)/m);
    if (threads) usage.thread_count = Number(threads[1]);
  } catch {
    // Best effort.
  }
  try {
    const io = fs.readFileSync(`/proc/${pid}/io`, "utf8");
    const read = io.match(/^read_bytes:\s+(\d+)/m);
    const write = io.match(/^write_bytes:\s+(\d+)/m);
    if (read) usage.io_read_bytes = Number(read[1]);
    if (write) usage.io_write_bytes = Number(write[1]);
  } catch {
    // Best effort.
  }
  return usage;
}

export function allProcessResourceUsage(contexts) {
  const base = processResourceUsage(process.pid);
  const contextUsages = [];
  for (const ctx of contexts) {
    const usage = ctx.resourceUsage();
    contextUsages.push({
      context_id: ctx.id,
      type: ctx.type,
      alias: ctx.alias || "",
      running: ctx.isRunning(),
      paused: ctx.isPaused(),
      usage
    });
  }
  return {
    ...base,
    context_count: contextUsages.length,
    contexts: contextUsages,
    root_path: process.cwd(),
    cwd: path.resolve(".")
  };
}
