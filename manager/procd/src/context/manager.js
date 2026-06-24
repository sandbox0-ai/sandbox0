import { EventEmitter } from "node:events";
import { Context } from "./context.js";
import { ManagedProcess } from "../process/process.js";
import { errors, ProcdError } from "../process/errors.js";
import { allProcessResourceUsage } from "../process/resource.js";
import { normalizeStringMap } from "../util/env.js";

export class ContextManager extends EventEmitter {
  constructor(logger) {
    super();
    this.logger = logger;
    this.contexts = new Map();
    this.sandboxEnvVars = {};
    this.defaultCleanupPolicy = {};
    this.cleanupTimer = null;
  }

  setSandboxEnvVars(envVars) {
    this.sandboxEnvVars = normalizeStringMap(envVars);
  }

  getSandboxEnvVars() {
    return { ...this.sandboxEnvVars };
  }

  setDefaultCleanupPolicy(policy) {
    this.defaultCleanupPolicy = { ...(policy || {}) };
  }

  createContext(config, cleanupPolicy = {}) {
    const proc = new ManagedProcess(config, this.sandboxEnvVars);
    const ctx = new Context(proc, { ...this.defaultCleanupPolicy, ...cleanupPolicy });
    proc.on("start", (event) => this.emit("processStart", event));
    proc.on("exit", (event) => this.emit("processExit", event));
    proc.on("error", (err) => this.logger?.warn?.("process error", { context_id: proc.id, error: err.message || String(err) }));
    proc.start();
    this.contexts.set(ctx.id, ctx);
    return ctx;
  }

  listContexts() {
    return [...this.contexts.values()];
  }

  getContext(id) {
    const ctx = this.contexts.get(id);
    if (!ctx) throw new ProcdError("context_not_found", "context not found");
    return ctx;
  }

  deleteContext(id) {
    const ctx = this.getContext(id);
    ctx.stop();
    this.contexts.delete(id);
  }

  restartContext(id) {
    const ctx = this.getContext(id);
    ctx.restart();
    return ctx;
  }

  writeInput(id, data) {
    const ctx = this.getContext(id);
    ctx.touch();
    ctx.mainProcess.writeInput(data);
  }

  readOutput(id, onOutput, onClose) {
    const ctx = this.getContext(id);
    const output = (payload) => onOutput(payload);
    const close = () => onClose?.();
    ctx.mainProcess.on("output", output);
    ctx.mainProcess.once("outputClose", close);
    return () => {
      ctx.mainProcess.off("output", output);
      ctx.mainProcess.off("outputClose", close);
    };
  }

  pauseAll() {
    for (const ctx of this.contexts.values()) {
      if (ctx.isRunning() && !ctx.isPaused()) ctx.pause();
    }
  }

  resumeAll() {
    for (const ctx of this.contexts.values()) {
      if (ctx.isPaused()) ctx.resume();
    }
  }

  resizePTY(id, size) {
    if (!size || size.rows <= 0 || size.cols <= 0) throw errors.invalidPTYSize();
    this.getContext(id).mainProcess.resizePTY(size);
  }

  sendSignal(id, signal) {
    this.getContext(id).mainProcess.sendSignal(signal);
  }

  getResourceUsage(id) {
    return this.getContext(id).resourceUsage();
  }

  getAllResourceUsage() {
    return allProcessResourceUsage(this.listContexts());
  }

  startCleanup(intervalMs) {
    if (!intervalMs || intervalMs <= 0) return;
    this.cleanupTimer = setInterval(() => this.cleanupExpired(), intervalMs);
    this.cleanupTimer.unref();
  }

  cleanupExpired() {
    const now = Date.now();
    for (const [id, ctx] of this.contexts.entries()) {
      if (!ctx.shouldCleanup(now)) continue;
      try {
        ctx.stop();
      } finally {
        this.contexts.delete(id);
      }
    }
  }

  cleanup() {
    if (this.cleanupTimer) clearInterval(this.cleanupTimer);
    for (const ctx of this.contexts.values()) ctx.stop();
    this.contexts.clear();
  }
}

export function contextResponse(ctx, outputRaw = "") {
  const proc = ctx.mainProcess;
  const response = {
    id: ctx.id,
    type: ctx.type,
    alias: ctx.alias,
    command: ctx.command,
    cwd: ctx.cwd,
    env_vars: ctx.envVars || {},
    running: ctx.isRunning(),
    paused: ctx.isPaused(),
    created_at: ctx.createdAt.toISOString(),
    output_raw: outputRaw
  };
  if (ctx.type === "cmd") {
    response.stdout = proc.stdout || "";
    response.stderr = proc.stderr || "";
  }
  if (proc.isFinished()) {
    response.exit_code = proc.exitCode ?? 0;
    response.state = proc.state;
  }
  return response;
}
