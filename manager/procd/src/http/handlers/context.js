import { codes, readJSON, writeError, writeJSON } from "../response.js";
import { contextResponse } from "../../context/manager.js";
import { ProcdError } from "../../process/errors.js";
import { defaultReadyToken } from "../../process/repl-config.js";

const execTimeoutMs = 30_000;
const signalMap = {
  INT: "SIGINT",
  TERM: "SIGTERM",
  KILL: "SIGKILL",
  HUP: "SIGHUP",
  QUIT: "SIGQUIT",
  USR1: "SIGUSR1",
  USR2: "SIGUSR2",
  WINCH: "SIGWINCH",
  STOP: "SIGSTOP",
  CONT: "SIGCONT"
};

function errorStatus(err) {
  if (err instanceof ProcdError && err.code === "context_not_found") return [404, "context_not_found", err.message];
  if (err.code === "process_finished") return [410, "process_finished", err.message];
  if (err.code === "process_not_running") return [409, "process_not_running", err.message];
  if (err.code === "input_buffer_full") return [409, "input_buffer_full", err.message];
  if (err.code === "pty_unavailable") return [409, "pty_unavailable", err.message];
  if (err.code === "invalid_pty_size") return [400, codes.badRequest, err.message];
  if (err.code === "invalid_command" || err.code === "unsupported_language" || err.code === "unsupported_process_type") return [400, "invalid_request", err.message];
  return [500, codes.internal, err.message || String(err)];
}

function parseSignal(value) {
  const trimmed = String(value || "").trim();
  if (!trimmed) throw new Error("signal is required");
  const upper = trimmed.toUpperCase().replace(/^SIG/, "");
  if (signalMap[upper]) return signalMap[upper];
  const n = Number.parseInt(upper, 10);
  if (Number.isInteger(n) && n > 0 && n <= 64) return n;
  throw new Error("unsupported signal");
}

export class ContextHandler {
  constructor(manager) {
    this.manager = manager;
  }

  list(_req, res) {
    writeJSON(res, 200, { contexts: this.manager.listContexts().map((ctx) => contextResponse(ctx)) });
  }

  async create(req, res) {
    let body;
    try {
      body = await readJSON(req);
    } catch (err) {
      writeError(res, 400, "invalid_request", err.message);
      return;
    }
    if ((body.idle_timeout_sec || 0) < 0 || (body.ttl_sec || 0) < 0) {
      writeError(res, 400, "invalid_request", "idle_timeout_sec and ttl_sec must be >= 0");
      return;
    }
    if (body.repl && body.type !== "repl") {
      writeError(res, 400, "invalid_request", "repl is only valid for repl contexts");
      return;
    }
    if (body.cmd && body.type !== "cmd") {
      writeError(res, 400, "invalid_request", "cmd is only valid for cmd contexts");
      return;
    }
    let replConfig = body.repl?.repl_config || null;
    let alias = body.repl?.alias || replConfig?.name || "";
    if (replConfig) {
      replConfig = { ...replConfig };
      if (!replConfig.name) replConfig.name = alias;
      if (!replConfig.name) {
        writeError(res, 400, "invalid_request", "repl.repl_config.name is required");
        return;
      }
      if (alias && alias !== replConfig.name) {
        writeError(res, 400, "invalid_request", "repl.alias must match repl.repl_config.name");
        return;
      }
      alias = alias || replConfig.name;
      if (!Array.isArray(replConfig.candidates) || replConfig.candidates.length === 0) {
        writeError(res, 400, "invalid_request", "REPL config must have at least one candidate");
        return;
      }
      for (let i = 0; i < replConfig.candidates.length; i++) {
        if (!replConfig.candidates[i]?.name) {
          writeError(res, 400, "invalid_request", `candidate ${i}: name is required`);
          return;
        }
      }
    }
    const cleanupPolicy = {};
    if (body.idle_timeout_sec > 0) cleanupPolicy.idleTimeoutMs = body.idle_timeout_sec * 1000;
    if (body.ttl_sec > 0) cleanupPolicy.maxLifetimeMs = body.ttl_sec * 1000;
    try {
      const ctx = this.manager.createContext({
        type: body.type,
        alias,
        command: body.cmd?.command || [],
        cwd: body.cwd || "",
        envVars: body.env_vars || {},
        ptySize: body.pty_size || null,
        replConfig
      }, cleanupPolicy);
      if (body.wait_until_done) {
        const output = await this.execInputSync(ctx, body.repl?.input || "");
        writeJSON(res, 201, contextResponse(ctx, output));
      } else {
        writeJSON(res, 201, contextResponse(ctx));
      }
    } catch (err) {
      const [status, code, message] = errorStatus(err);
      writeError(res, status, code, message);
    }
  }

  get(_req, res, params) {
    try {
      writeJSON(res, 200, contextResponse(this.manager.getContext(params.id)));
    } catch (err) {
      const [status, code, message] = errorStatus(err);
      writeError(res, status, code, message);
    }
  }

  delete(_req, res, params) {
    try {
      this.manager.deleteContext(params.id);
      writeJSON(res, 200, { deleted: true });
    } catch (err) {
      const [status, code, message] = errorStatus(err);
      writeError(res, status, code, message);
    }
  }

  restart(_req, res, params) {
    try {
      writeJSON(res, 200, contextResponse(this.manager.restartContext(params.id)));
    } catch (err) {
      const [status, code, message] = errorStatus(err);
      writeError(res, status, code, message);
    }
  }

  async writeInput(req, res, params) {
    try {
      const body = await readJSON(req);
      if (body.data === undefined || body.data === null) {
        writeError(res, 400, "invalid_request", "data is required");
        return;
      }
      this.manager.writeInput(params.id, Buffer.from(String(body.data)));
      writeJSON(res, 200, { written: true });
    } catch (err) {
      const [status, code, message] = errorStatus(err);
      writeError(res, status, code, message);
    }
  }

  async exec(req, res, params) {
    try {
      const body = await readJSON(req);
      if (!body.data) {
        writeError(res, 400, "invalid_request", "data is required");
        return;
      }
      const ctx = this.manager.getContext(params.id);
      const output = await this.execInputSync(ctx, String(body.data));
      const response = { output_raw: output };
      if (ctx.type === "cmd") {
        response.stdout = ctx.mainProcess.stdout || "";
        response.stderr = ctx.mainProcess.stderr || "";
      }
      if (ctx.mainProcess.isFinished()) {
        response.exit_code = ctx.mainProcess.exitCode ?? 0;
        response.state = ctx.mainProcess.state;
      }
      writeJSON(res, 200, response);
    } catch (err) {
      if (err.code === "exec_timeout") writeError(res, 408, "exec_timeout", "execution timed out");
      else {
        const [status, code, message] = errorStatus(err);
        writeError(res, status, code, message);
      }
    }
  }

  stats(_req, res, params) {
    try {
      const ctx = this.manager.getContext(params.id);
      writeJSON(res, 200, {
        context_id: ctx.id,
        type: ctx.type,
        alias: ctx.alias,
        running: ctx.isRunning(),
        paused: ctx.isPaused(),
        usage: ctx.resourceUsage()
      });
    } catch (err) {
      const [status, code, message] = errorStatus(err);
      writeError(res, status, code, message);
    }
  }

  async resize(req, res, params) {
    try {
      const body = await readJSON(req);
      if (!body.rows || !body.cols) {
        writeError(res, 400, "invalid_request", "rows and cols must be > 0");
        return;
      }
      this.manager.resizePTY(params.id, { rows: body.rows, cols: body.cols });
      writeJSON(res, 200, { resized: true });
    } catch (err) {
      const [status, code, message] = errorStatus(err);
      writeError(res, status, code, message);
    }
  }

  async signal(req, res, params) {
    try {
      const body = await readJSON(req);
      this.manager.sendSignal(params.id, parseSignal(body.signal));
      writeJSON(res, 200, { signaled: true });
    } catch (err) {
      if (!(err instanceof ProcdError) && err.message === "unsupported signal") writeError(res, 400, "invalid_request", err.message);
      else {
        const [status, code, message] = errorStatus(err);
        writeError(res, status, code, message);
      }
    }
  }

  websocket(conn, params) {
    let ctx;
    try {
      ctx = this.manager.getContext(params.id);
    } catch {
      conn.close("context not found");
      return;
    }
    const proc = ctx.mainProcess;
    if (proc.isFinished()) {
      if (proc.stdout) conn.sendJSON({ type: "output", source: "stdout", data: proc.stdout });
      if (proc.stderr) conn.sendJSON({ type: "output", source: "stderr", data: proc.stderr });
      conn.sendJSON({ type: "done", exit_code: proc.exitCode ?? 0, state: proc.state });
      conn.close("context finished");
      return;
    }
    let pendingRequestID = "";
    const unsubscribe = this.manager.readOutput(params.id, (output) => {
      if (output.source === "prompt") {
        if (pendingRequestID) conn.sendJSON({ type: "done", request_id: pendingRequestID });
        pendingRequestID = "";
        return;
      }
      conn.sendJSON({ type: "output", source: output.source, data: output.data });
    }, () => {
      conn.sendJSON({ type: "done", exit_code: proc.exitCode ?? 0, state: proc.state });
      conn.close("context output closed");
    });
    conn.on("message", (_type, data) => {
      try {
        const msg = JSON.parse(String(data));
        if (msg.type === "input") {
          pendingRequestID = msg.request_id || "";
          this.manager.writeInput(params.id, Buffer.from(String(msg.data || "")));
        } else if (msg.type === "resize") {
          this.manager.resizePTY(params.id, { rows: msg.rows, cols: msg.cols });
        } else if (msg.type === "signal") {
          this.manager.sendSignal(params.id, parseSignal(msg.signal));
        }
      } catch (err) {
        conn.sendJSON({ type: "error", error: err.message });
      }
    });
    conn.on("close", unsubscribe);
  }

  execInputSync(ctx, input) {
    if (!ctx.mainProcess || ctx.mainProcess.isFinished()) {
      const err = new Error("process finished");
      err.code = "process_finished";
      throw err;
    }
    return new Promise((resolve, reject) => {
      let output = "";
      let promptTimer = null;
      const cleanup = this.manager.readOutput(ctx.id, (payload) => {
        if (payload.source === "prompt") {
          clearTimeout(promptTimer);
          promptTimer = setTimeout(() => {
            cleanup();
            resolve(ctx.type === "repl" ? normalizeReplOutput(output) : output);
          }, 50);
          return;
        }
        output += payload.data || "";
      }, () => {
        cleanup();
        resolve(ctx.type === "repl" ? normalizeReplOutput(output) : output);
      });
      const timer = setTimeout(() => {
        cleanup();
        const err = new Error("execution timed out");
        err.code = "exec_timeout";
        reject(err);
      }, execTimeoutMs);
      const done = (value) => {
        clearTimeout(timer);
        resolve(value);
      };
      const fail = (err) => {
        clearTimeout(timer);
        reject(err);
      };
      try {
        let normalized = input;
        if (ctx.type === "repl" && !normalized.endsWith("\n") && !normalized.endsWith("\r")) normalized += "\n";
        ctx.mainProcess.writeInput(Buffer.from(normalized));
        if (ctx.type === "cmd") {
          ctx.mainProcess.once("outputClose", () => done(output));
        }
      } catch (err) {
        cleanup();
        fail(err);
      }
    });
  }
}

function normalizeReplOutput(raw) {
  let normalized = raw.endsWith(defaultReadyToken) ? raw.slice(0, -defaultReadyToken.length) : raw;
  if (!normalized.startsWith(defaultReadyToken)) normalized = defaultReadyToken + normalized;
  return normalized;
}
