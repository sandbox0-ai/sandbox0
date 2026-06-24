import { spawn } from "node:child_process";
import crypto from "node:crypto";
import { EventEmitter } from "node:events";
import { accessSync, constants, existsSync } from "node:fs";
import path from "node:path";
import readline from "node:readline";
import { mergeEnvironment } from "../util/env.js";
import { errors } from "./errors.js";
import { normalizeReplConfig, defaultReadyToken } from "./repl-config.js";
import { processResourceUsage } from "./resource.js";

const maxBufferedOutput = 1 << 20;
const defaultPath = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

function contextID() {
  return "ctx-" + cryptoRandom(8);
}

function cryptoRandom(n) {
  return crypto.randomBytes(Math.ceil(n / 2)).toString("hex").slice(0, n);
}

function appendLimited(current, data) {
  const next = current + data;
  if (Buffer.byteLength(next) <= maxBufferedOutput) return next;
  return next.slice(next.length - maxBufferedOutput);
}

function signalProcessGroup(pid, signal) {
  try {
    process.kill(-pid, signal);
  } catch (err) {
    if (err.code === "ESRCH") return;
    throw err;
  }
}

function executableExists(command, env) {
  if (!command) return false;
  const candidates = command.includes("/")
    ? [command]
    : String(env.PATH || defaultPath).split(":").filter(Boolean).map((dir) => path.join(dir, command));
  for (const candidate of candidates) {
    try {
      accessSync(candidate, constants.X_OK);
      return true;
    } catch {
      // Try the next PATH entry.
    }
  }
  return false;
}

function selectCandidate(candidates, env) {
  for (const candidate of candidates || []) {
    if (executableExists(candidate.name, env)) return candidate;
  }
  return candidates?.[0] || null;
}

function ptyHelperCommand() {
  const helperPath = process.env.SANDBOX0_PTY_HELPER_PATH || "/procd/bin/pty-helper";
  if (process.env.SANDBOX0_PTY_HELPER_USE_LOADER !== "true") return { command: helperPath, args: [] };
  const loaderPath = process.env.SANDBOX0_PROCD_LOADER_PATH || "/procd/runtime/ld-linux";
  const libraryPath = process.env.SANDBOX0_PROCD_LIBRARY_PATH || "/procd/runtime/lib";
  if (existsSync(loaderPath)) {
    return { command: loaderPath, args: ["--library-path", libraryPath, helperPath] };
  }
  return { command: helperPath, args: [] };
}

function encodePTYConfig(config) {
  return Buffer.from(JSON.stringify(config)).toString("base64");
}

export class ManagedProcess extends EventEmitter {
  constructor(config, sandboxEnv = {}) {
    super();
    this.id = config.id || contextID();
    this.type = config.type;
    this.alias = config.alias || "";
    this.command = [...(config.command || [])];
    this.cwd = config.cwd || "";
    this.envVars = { ...(config.envVars || {}) };
    this.ptySize = config.ptySize || null;
    this.term = config.term || "";
    this.replConfig = config.replConfig || null;
    this.sandboxEnv = sandboxEnv || {};
    this.state = "created";
    this.pid = 0;
    this.helperPID = 0;
    this.exitCode = null;
    this.createdAt = new Date();
    this.startedAt = null;
    this.finishedAt = null;
    this.paused = false;
    this.child = null;
    this.usesPTY = false;
    this.readyToken = defaultReadyToken;
    this.readyMode = "";
    this.readyBuffer = "";
    this.finished = false;
    this.stdout = "";
    this.stderr = "";
  }

  start() {
    if (this.isRunning()) throw errors.processAlreadyRunning();
    const spawnConfig = this.spawnConfig();
    this.state = "starting";
    this.finished = false;
    if (spawnConfig.pty) {
      this.startPTY(spawnConfig);
      return;
    }
    this.startDirect(spawnConfig);
  }

  startDirect(spawnConfig) {
    const child = spawn(spawnConfig.command[0], spawnConfig.command.slice(1), {
      cwd: spawnConfig.cwd || undefined,
      env: spawnConfig.env,
      detached: true,
      stdio: ["pipe", "pipe", "pipe"]
    });
    this.child = child;
    this.pid = child.pid || 0;
    this.startedAt = new Date();

    child.stdout.on("data", (chunk) => this.handleOutput("stdout", chunk));
    child.stderr.on("data", (chunk) => this.handleOutput("stderr", chunk));
    child.on("spawn", () => {
      this.state = "running";
      this.emit("start", this.startEvent());
      if (this.type === "repl") this.scheduleReplReady(spawnConfig.replConfig);
    });
    child.on("error", (err) => {
      this.state = "crashed";
      this.emit("error", err);
      this.finishProcess(1, "");
    });
    child.on("exit", (code, signal) => {
      this.finishProcess(code === null ? 137 : code, signal || "");
    });
  }

  startPTY(spawnConfig) {
    const helper = ptyHelperCommand();
    const helperEnv = mergeEnvironment({
      SANDBOX0_PTY_CONFIG: encodePTYConfig({
        command: spawnConfig.command,
        cwd: spawnConfig.cwd,
        env: spawnConfig.env,
        rows: spawnConfig.ptySize?.rows || 100,
        cols: spawnConfig.ptySize?.cols || 500
      })
    });
    const child = spawn(helper.command, helper.args, {
      env: helperEnv,
      stdio: ["pipe", "pipe", "pipe"]
    });
    this.child = child;
    this.usesPTY = true;
    this.helperPID = child.pid || 0;
    this.startedAt = new Date();

    const lines = readline.createInterface({ input: child.stdout });
    lines.on("line", (line) => this.handlePTYFrame(line, spawnConfig));
    child.stderr.on("data", (chunk) => this.handleOutput("stderr", chunk));
    child.on("error", (err) => {
      this.state = "crashed";
      this.emit("error", err);
      this.finishProcess(1, "");
    });
    child.on("exit", (code, signal) => {
      if (!this.finished) this.finishProcess(code === null ? 137 : code, signal || "");
    });
  }

  spawnConfig() {
    if (this.type === "cmd") {
      if (this.command.length === 0) throw errors.invalidCommand("invalid command: command is required");
      return {
        command: this.command,
        cwd: this.cwd,
        env: this.cmdEnvironment(),
        pty: Boolean(this.ptySize),
        ptySize: this.ptySize
      };
    }
    if (this.type === "repl") {
      const replConfig = normalizeReplConfig(this.alias || "python", this.replConfig);
      if (!replConfig) throw errors.unsupportedLanguage();
      const envLayer = this.replEnvironment(replConfig);
      const env = mergeEnvironment(this.sandboxEnv, this.envVars, envLayer);
      const candidate = selectCandidate(replConfig.candidates, env);
      if (!candidate) throw errors.unsupportedLanguage();
      this.alias = replConfig.name;
      this.readyMode = replConfig.ready?.mode || "";
      this.readyToken = replConfig.ready?.token || defaultReadyToken;
      this.readyBuffer = "";
      const ptySize = this.ptySize || { rows: 100, cols: 500 };
      return {
        command: [candidate.name, ...(candidate.args || [])],
        cwd: this.cwd,
        env,
        replConfig,
        pty: true,
        ptySize
      };
    }
    throw errors.unsupportedProcessType();
  }

  cmdEnvironment() {
    const env = mergeEnvironment(this.sandboxEnv, this.envVars);
    if (this.ptySize && !env.TERM) env.TERM = this.term || "xterm-256color";
    return env;
  }

  replEnvironment(replConfig) {
    const envLayer = {};
    for (const item of replConfig.env || []) {
      if (item.valueFrom === "term") envLayer[item.name] = this.term || replConfig.defaultTerm || "xterm-256color";
      else if (item.valueFrom === "prompt") envLayer[item.name] = replConfig.prompt?.customPrompt || defaultReadyToken;
      else envLayer[item.name] = item.value || "";
    }
    return envLayer;
  }

  scheduleReplReady(replConfig) {
    if (!replConfig) return;
    if (replConfig.ready?.mode === "startup_delay") {
      setTimeout(() => this.emitOutput("prompt", replConfig.ready?.token || defaultReadyToken), replConfig.ready?.startupDelayMs || 200);
    }
  }

  handleOutput(source, chunk) {
    const data = chunk.toString();
    if (source === "stdout") this.stdout = appendLimited(this.stdout, data);
    if (source === "stderr") this.stderr = appendLimited(this.stderr, data);
    this.emitOutput(source, data);
    if (this.type === "repl" && this.readyMode === "prompt_token") {
      if (this.detectReadyToken(data)) this.emitOutput("prompt", this.readyToken);
    }
  }

  handlePTYFrame(line, spawnConfig) {
    let frame;
    try {
      frame = JSON.parse(line);
    } catch {
      this.handleOutput("stderr", Buffer.from(line + "\n"));
      return;
    }
    if (frame.type === "start") {
      this.pid = frame.pid || 0;
      this.state = "running";
      this.emit("start", this.startEvent());
      if (this.type === "repl") this.scheduleReplReady(spawnConfig.replConfig);
      return;
    }
    if (frame.type === "output") {
      this.handleOutput("pty", Buffer.from(frame.data || "", "base64"));
      return;
    }
    if (frame.type === "error") {
      const message = frame.message || "pty helper failed";
      this.handleOutput("stderr", Buffer.from(message + "\n"));
      this.emit("error", new Error(message));
      return;
    }
    if (frame.type === "exit") {
      this.finishProcess(frame.exit_code ?? 0, frame.signal || "");
    }
  }

  detectReadyToken(data) {
    const token = this.readyToken || defaultReadyToken;
    if (!token) return false;
    const combined = this.readyBuffer + data;
    if (combined.includes(token)) {
      this.readyBuffer = "";
      return true;
    }
    this.readyBuffer = token.length > 1 ? combined.slice(-(token.length - 1)) : "";
    return false;
  }

  finishProcess(code, signal) {
    if (this.finished) return;
    this.finished = true;
    this.exitCode = code === null || code === undefined ? 137 : code;
    this.finishedAt = new Date();
    if (signal === "SIGKILL" || signal === "SIGTERM" || this.exitCode === 137 || this.exitCode === 143) this.state = "killed";
    else if (this.exitCode === 0) this.state = "stopped";
    else this.state = "crashed";
    this.emit("exit", this.exitEvent());
    this.emit("outputClose");
  }

  emitOutput(source, data) {
    const payload = { source, data };
    this.emit("output", payload);
    if (process.env.SANDBOX0_PROCESS_LOGS !== "false") {
      process.stdout.write(JSON.stringify({
        message: "sandbox process output",
        process_id: this.id,
        process_type: this.type,
        source,
        data
      }) + "\n");
    }
  }

  writeInput(data) {
    if (!this.child || !this.child.stdin || this.isFinished()) throw errors.processFinished();
    if (!this.isRunning()) throw errors.processNotRunning();
    if (this.usesPTY) {
      this.writePTYControl({ type: "input", data: Buffer.from(data).toString("base64") });
      return;
    }
    if (!this.child.stdin.write(data)) {
      // Backpressure is handled by Node; the API remains accepted.
    }
  }

  stop() {
    if (!this.isRunning()) return;
    if (this.usesPTY) this.writePTYControl({ type: "signal", signal: "SIGTERM" });
    else signalProcessGroup(this.pid, "SIGTERM");
    setTimeout(() => {
      if (!this.isRunning()) return;
      if (this.usesPTY) {
        this.writePTYControl({ type: "signal", signal: "SIGKILL" });
        if (this.child && !this.child.killed) this.child.kill("SIGKILL");
      } else {
        signalProcessGroup(this.pid, "SIGKILL");
      }
    }, 5000).unref();
  }

  restart() {
    this.stop();
    this.state = "created";
    this.paused = false;
    this.exitCode = null;
    this.child = null;
    this.start();
  }

  pause() {
    if (!this.isRunning()) throw errors.processNotRunning();
    if (this.usesPTY) this.writePTYControl({ type: "signal", signal: "SIGSTOP" });
    else signalProcessGroup(this.pid, "SIGSTOP");
    this.paused = true;
    this.state = "paused";
  }

  resume() {
    if (!this.child || this.isFinished()) throw errors.processNotRunning();
    if (this.usesPTY) this.writePTYControl({ type: "signal", signal: "SIGCONT" });
    else signalProcessGroup(this.pid, "SIGCONT");
    this.paused = false;
    this.state = "running";
  }

  sendSignal(signal) {
    if (!this.child || this.isFinished()) throw errors.processNotRunning();
    try {
      if (this.usesPTY) this.writePTYControl({ type: "signal", signal });
      else signalProcessGroup(this.pid, signal);
    } catch (err) {
      throw errors.signalFailed(err.message);
    }
  }

  resizePTY(size) {
    if (!this.usesPTY) throw errors.ptyUnavailable();
    this.writePTYControl({ type: "resize", rows: size.rows, cols: size.cols });
  }

  writePTYControl(frame) {
    if (!this.child || !this.child.stdin || this.isFinished()) throw errors.processFinished();
    if (!this.child.stdin.write(JSON.stringify(frame) + "\n")) {
      // The helper stdin pipe handles backpressure asynchronously.
    }
  }

  isRunning() {
    return this.state === "running" || this.state === "paused";
  }

  isPaused() {
    return this.paused || this.state === "paused";
  }

  isFinished() {
    return ["stopped", "killed", "crashed"].includes(this.state);
  }

  resourceUsage() {
    return processResourceUsage(this.pid);
  }

  startEvent() {
    return {
      process_id: this.id,
      process_type: this.type,
      pid: this.pid,
      command: this.command,
      env_vars: this.envVars,
      cwd: this.cwd,
      alias: this.alias
    };
  }

  exitEvent() {
    const duration = this.startedAt ? Date.now() - this.startedAt.getTime() : 0;
    return {
      process_id: this.id,
      process_type: this.type,
      pid: this.pid,
      exit_code: this.exitCode ?? 0,
      duration_ms: duration,
      stdout_preview: this.stdout.slice(0, 2048),
      stderr_preview: this.stderr.slice(0, 2048),
      state: this.state
    };
  }
}
