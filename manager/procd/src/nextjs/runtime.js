import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import path from "node:path";
import readline from "node:readline";
import { fileURLToPath } from "node:url";
import { mergeEnvironment } from "../util/env.js";

const runnerPath = fileURLToPath(new URL("./runner.js", import.meta.url));

function injectedNodeCommand() {
  const nodePath = process.env.SANDBOX0_NODE_PATH || "/procd/runtime/node";
  const loaderPath = process.env.SANDBOX0_PROCD_LOADER_PATH || "/procd/runtime/ld-linux";
  const libraryPath = process.env.SANDBOX0_PROCD_LIBRARY_PATH || "/procd/runtime/lib";
  if (existsSync(loaderPath) && existsSync(nodePath)) {
    return { command: loaderPath, args: ["--library-path", libraryPath, nodePath] };
  }
  return { command: process.execPath, args: [] };
}

function signalProcessGroup(pid, signal) {
  if (!pid) return;
  try {
    process.kill(-pid, signal);
  } catch (err) {
    if (err.code !== "ESRCH") {
      try {
        process.kill(pid, signal);
      } catch {
        // Already exited or not signalable.
      }
    }
  }
}

export class NextJSRuntimeManager {
  constructor(logger) {
    this.logger = logger;
    this.runtimes = new Map();
  }

  key(serviceID, cwd, port) {
    return `${serviceID}\0${path.resolve(cwd || ".")}\0${port}`;
  }

  async ensure(req) {
    const serviceID = String(req.service_id || "").trim();
    const port = Number(req.port || 0);
    const cwd = path.resolve(String(req.cwd || "/workspace"));
    if (!serviceID) throw new Error("service_id is required");
    if (!Number.isInteger(port) || port <= 0 || port > 65535) throw new Error("port must be between 1 and 65535");
    const key = this.key(serviceID, cwd, port);
    const existing = this.runtimes.get(key);
    if (existing) {
      await existing.ready;
      return existing.view();
    }

    const runtime = new NextJSRuntime(serviceID, cwd, port, req.env_vars || {}, this.logger);
    const ready = runtime.start();
    this.runtimes.set(key, runtime);
    try {
      await ready;
      return runtime.view();
    } catch (err) {
      this.runtimes.delete(key);
      await runtime.close();
      throw err;
    }
  }

  async closeAll() {
    const runtimes = [...this.runtimes.values()];
    this.runtimes.clear();
    await Promise.all(runtimes.map((runtime) => runtime.close()));
  }
}

class NextJSRuntime {
  constructor(serviceID, cwd, port, envVars, logger) {
    this.serviceID = serviceID;
    this.cwd = cwd;
    this.port = port;
    this.envVars = envVars;
    this.logger = logger;
    this.startedAt = new Date();
    this.child = null;
    this.ready = null;
    this.running = false;
  }

  start() {
    if (this.ready) return this.ready;
    this.ready = new Promise((resolve, reject) => {
      const node = injectedNodeCommand();
      const child = spawn(node.command, [...node.args, runnerPath], {
        cwd: this.cwd,
        env: mergeEnvironment({
          SANDBOX0_SERVICE_ID: this.serviceID,
          SANDBOX0_SERVICE_PORT: String(this.port),
          SANDBOX0_SERVICE_RUNTIME: "nextjs",
          SANDBOX0_NEXTJS_CWD: this.cwd,
          SANDBOX0_NEXTJS_PORT: String(this.port),
          PORT: String(this.port),
          HOSTNAME: "0.0.0.0"
        }, this.envVars),
        detached: true,
        stdio: ["ignore", "pipe", "pipe", "pipe"]
      });
      this.child = child;

      const control = readline.createInterface({ input: child.stdio[3] });
      let settled = false;
      const fail = (err) => {
        if (settled) return;
        settled = true;
        reject(err);
      };
      const ready = () => {
        if (settled) return;
        settled = true;
        this.running = true;
        resolve();
      };
      control.on("line", (line) => {
        let frame;
        try {
          frame = JSON.parse(line);
        } catch {
          return;
        }
        if (frame.type === "ready") ready();
        if (frame.type === "error") fail(new Error(frame.message || "nextjs runtime failed"));
      });
      child.stdout.on("data", (chunk) => this.logger?.info?.("nextjs stdout", { service_id: this.serviceID, data: chunk.toString() }));
      child.stderr.on("data", (chunk) => this.logger?.warn?.("nextjs stderr", { service_id: this.serviceID, data: chunk.toString() }));
      child.on("error", fail);
      child.on("exit", (code, signal) => {
        this.running = false;
        if (!settled) fail(new Error(`nextjs runtime exited before ready: code=${code} signal=${signal || ""}`));
      });
    });
    return this.ready;
  }

  view() {
    return {
      service_id: this.serviceID,
      runtime: "nextjs",
      cwd: this.cwd,
      port: this.port,
      running: this.running,
      pid: this.child?.pid || 0,
      started_at: this.startedAt.toISOString()
    };
  }

  close() {
    return new Promise((resolve) => {
      const child = this.child;
      if (!child || child.exitCode !== null || child.signalCode !== null) {
        this.running = false;
        resolve();
        return;
      }
      const done = () => {
        this.running = false;
        resolve();
      };
      const killTimer = setTimeout(() => {
        signalProcessGroup(child.pid, "SIGKILL");
      }, 5000);
      killTimer.unref();
      child.once("exit", () => {
        clearTimeout(killTimer);
        done();
      });
      signalProcessGroup(child.pid, "SIGTERM");
    });
  }
}
