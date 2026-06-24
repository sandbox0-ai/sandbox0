import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { codes, readJSON, writeError, writeJSON } from "../http/response.js";
import { mergeEnvironment } from "../util/env.js";

const runtimePython = "python";
const sourceTypeInline = "inline";
const defaultFilename = "main.py";
const defaultTimeoutMs = 30_000;
const maxTimeoutMs = 120_000;
const maxHTTPRequestBytes = 8 << 20;
const maxInlineSourceBytes = 256 << 10;
const maxFunctionExecuteBytes = maxHTTPRequestBytes + maxInlineSourceBytes + (1 << 20);
const maxFunctionStdoutBytes = 4 << 20;
const maxFunctionStderrBytes = 64 << 10;
const maxFunctionStreamFrame = 4 << 20;
const handlerPattern = /^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$/;
const digestPattern = /^sha256:[a-f0-9]{64}$/;

function inlineDigest(code) {
  return "sha256:" + crypto.createHash("sha256").update(code || "").digest("hex");
}

function legacyInlineDigest(filename, code) {
  return "sha256:" + crypto.createHash("sha256").update(`${filename}\0${code || ""}`).digest("hex");
}

function validateRequest(req) {
  if (req.runtime !== runtimePython) throw new Error(`runtime must be "${runtimePython}"`);
  if (!handlerPattern.test(req.handler || "")) throw new Error(`handler must match ${handlerPattern}`);
  if (req.source?.type !== sourceTypeInline) throw new Error(`source.type must be "${sourceTypeInline}"`);
  if (!String(req.source?.code || "").trim()) throw new Error("source.code is required");
  if (Buffer.byteLength(req.source.code) > maxInlineSourceBytes) throw new Error(`source.code exceeds limit ${maxInlineSourceBytes} bytes`);
  if (!req.request?.path) throw new Error("request.path is required");
  if (!String(req.request.path).startsWith("/")) throw new Error("request.path must start with /");
  if (req.request.body_base64) {
    try {
      Buffer.from(req.request.body_base64, "base64");
    } catch {
      throw new Error("request.body_base64 must be valid base64");
    }
  }
  if ((req.timeout_ms || 0) < 0) throw new Error("timeout_ms must be >= 0");
}

function decodeResponse(data) {
  if (String(data).trim() === "") throw new Error("function returned an empty response");
  const response = JSON.parse(data);
  if (!response.status) response.status = 200;
  if (response.status < 100 || response.status > 599) throw new Error("function response status must be between 100 and 599");
  if (response.body_base64) Buffer.from(response.body_base64, "base64");
  if (!response.headers) response.headers = {};
  return response;
}

function limitedAppend(buffer, chunk, limit) {
  if (buffer.length >= limit) return buffer;
  const remaining = limit - buffer.length;
  return Buffer.concat([buffer, chunk.subarray(0, remaining)]);
}

function killProcessGroup(child) {
  if (!child?.pid) return;
  try {
    process.kill(-child.pid, "SIGKILL");
  } catch {
    try {
      child.kill("SIGKILL");
    } catch {
      // Already exited.
    }
  }
}

function isRunnerStartError(err) {
  return ["ENOENT", "EACCES", "ENOTDIR", "EISDIR", "ENOEXEC"].includes(err?.code);
}

export class FunctionHandler {
  constructor(config, contextManager, logger) {
    this.runnerPath = config.functionRunnerPath;
    this.cacheRoot = config.functionCacheRoot;
    this.contextManager = contextManager;
    this.logger = logger;
  }

  sandboxEnvVars() {
    return this.contextManager?.getSandboxEnvVars?.() || {};
  }

  async execute(req, res) {
    let body;
    try {
      body = await readJSON(req, maxFunctionExecuteBytes);
      validateRequest(body);
    } catch (err) {
      if (err.code === "request_too_large") writeError(res, 413, "request_too_large", "function execution request is too large");
      else writeError(res, 400, codes.badRequest, err.message);
      return;
    }
    const timeoutMs = this.requestTimeout(body.timeout_ms);
    if (timeoutMs instanceof Error) {
      writeError(res, 400, codes.badRequest, timeoutMs.message);
      return;
    }
    let modulePath;
    try {
      modulePath = this.materializeSource(body.source);
    } catch (err) {
      writeError(res, 400, codes.badRequest, err.message);
      return;
    }
    const payload = this.runnerPayload(body);
    try {
      const result = await this.run(modulePath, body.handler, body.env_vars, payload, timeoutMs);
      if (result.truncatedStdout) {
        writeError(res, 502, codes.unavailable, "function response is too large");
        return;
      }
      const response = decodeResponse(result.stdout.toString());
      writeJSON(res, 200, response);
    } catch (err) {
      if (err.timeout) writeError(res, 504, codes.unavailable, "function execution timed out");
      else if (isRunnerStartError(err)) writeError(res, err.code === "ENOENT" || err.code === "ENOTDIR" ? 503 : 500, err.code === "ENOENT" ? codes.unavailable : codes.internal, "function runtime unavailable");
      else if (err instanceof SyntaxError) writeError(res, 502, codes.unavailable, `function returned invalid JSON: ${err.message}`);
      else writeError(res, 500, "function_failed", "function execution failed");
    }
  }

  async stream(req, res) {
    let body;
    try {
      body = await readJSON(req, maxFunctionExecuteBytes);
      validateRequest(body);
    } catch (err) {
      if (err.code === "request_too_large") writeError(res, 413, "request_too_large", "function execution request is too large");
      else writeError(res, 400, codes.badRequest, err.message);
      return;
    }
    let modulePath;
    try {
      modulePath = this.materializeSource(body.source);
    } catch (err) {
      writeError(res, 400, codes.badRequest, err.message);
      return;
    }
    try {
      await this.runStream(res, modulePath, body.handler, body.env_vars, this.runnerPayload(body), body.timeout_ms || 0);
    } catch (err) {
      if (!res.headersSent) writeError(res, isRunnerStartError(err) ? 503 : 500, isRunnerStartError(err) ? codes.unavailable : "function_failed", isRunnerStartError(err) ? "function runtime unavailable" : "function stream failed");
      this.logger?.warn("Function stream failed", { error: err.message });
    }
  }

  async websocket(conn, initReq) {
    validateRequest(initReq);
    const modulePath = this.materializeSource(initReq.source);
    await this.runWebSocket(conn, modulePath, initReq.handler, initReq.env_vars, this.runnerPayload(initReq));
  }

  requestTimeout(timeoutMs) {
    if (!timeoutMs) return defaultTimeoutMs;
    if (timeoutMs < 0) return new Error("timeout_ms must be >= 0");
    if (timeoutMs > maxTimeoutMs) return new Error(`timeout_ms must be <= ${maxTimeoutMs}`);
    return timeoutMs;
  }

  materializeSource(source) {
    let digest = source.digest || inlineDigest(source.code);
    if (!digestPattern.test(digest)) throw new Error("source.digest must be a sha256 digest");
    const expected = inlineDigest(source.code);
    if (source.code && digest !== expected && digest !== legacyInlineDigest(source.filename || defaultFilename, source.code)) {
      throw new Error("source.digest does not match source code");
    }
    const dir = path.join(this.cacheRoot, digest.replace("sha256:", ""));
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    const modulePath = path.join(dir, defaultFilename);
    if (source.code) fs.writeFileSync(modulePath, source.code, { mode: 0o600 });
    return modulePath;
  }

  runnerPayload(req) {
    return Buffer.from(JSON.stringify({
      service_id: req.service_id || "",
      route_id: req.route_id || "",
      method: req.request?.method || "",
      path: req.request?.path || "",
      raw_query: req.request?.raw_query || "",
      headers: req.request?.headers || {},
      body_base64: req.request?.body_base64 || ""
    }));
  }

  run(modulePath, handler, envVars, payload, timeoutMs) {
    return new Promise((resolve, reject) => {
      const child = spawn(this.runnerPath, [modulePath, handler], {
        cwd: path.dirname(modulePath),
        env: mergeEnvironment(this.sandboxEnvVars(), envVars || {}),
        detached: true,
        stdio: ["pipe", "pipe", "pipe"]
      });
      let stdout = Buffer.alloc(0);
      let stderr = Buffer.alloc(0);
      let truncatedStdout = false;
      let truncatedStderr = false;
      const timer = setTimeout(() => {
        const err = new Error("function execution timed out");
        err.timeout = true;
        killProcessGroup(child);
        reject(err);
      }, timeoutMs);
      child.stdout.on("data", (chunk) => {
        if (stdout.length + chunk.length > maxFunctionStdoutBytes) truncatedStdout = true;
        stdout = limitedAppend(stdout, chunk, maxFunctionStdoutBytes);
      });
      child.stderr.on("data", (chunk) => {
        if (stderr.length + chunk.length > maxFunctionStderrBytes) truncatedStderr = true;
        stderr = limitedAppend(stderr, chunk, maxFunctionStderrBytes);
      });
      child.on("error", (err) => {
        clearTimeout(timer);
        reject(err);
      });
      child.on("exit", (code) => {
        clearTimeout(timer);
        if (code !== 0) {
          const err = new Error(`function process failed: ${code}: ${stderr.toString()}`);
          err.stderr = stderr.toString();
          err.truncatedStderr = truncatedStderr;
          reject(err);
          return;
        }
        resolve({ stdout, stderr, truncatedStdout, truncatedStderr });
      });
      child.stdin.end(payload);
    });
  }

  runStream(res, modulePath, handler, envVars, payload, timeoutMs) {
    return new Promise((resolve, reject) => {
      const child = spawn(this.runnerPath, ["--stream", modulePath, handler], {
        cwd: path.dirname(modulePath),
        env: mergeEnvironment(this.sandboxEnvVars(), envVars || {}),
        detached: true,
        stdio: ["pipe", "pipe", "pipe"]
      });
      let stderr = Buffer.alloc(0);
      let lineBuffer = "";
      let started = false;
      let timer = null;
      if (timeoutMs > 0) {
        timer = setTimeout(() => {
          killProcessGroup(child);
          reject(new Error("function execution timed out"));
        }, timeoutMs);
      }
      child.stderr.on("data", (chunk) => { stderr = limitedAppend(stderr, chunk, maxFunctionStderrBytes); });
      child.stdout.on("data", (chunk) => {
        lineBuffer += chunk.toString();
        while (true) {
          const idx = lineBuffer.indexOf("\n");
          if (idx < 0) break;
          const line = lineBuffer.slice(0, idx);
          lineBuffer = lineBuffer.slice(idx + 1);
          if (Buffer.byteLength(line) > maxFunctionStreamFrame) {
            killProcessGroup(child);
            reject(new Error("function stream frame too large"));
            return;
          }
          let frame;
          try {
            frame = JSON.parse(line);
          } catch (err) {
            killProcessGroup(child);
            reject(new Error(`function stream returned invalid JSON: ${err.message}`));
            return;
          }
          if (frame.type === "start") {
            if (started) continue;
            for (const [key, values] of Object.entries(frame.headers || {})) {
              for (const value of values) res.setHeader(key, [...(res.getHeader(key) ? [].concat(res.getHeader(key)) : []), value]);
            }
            res.writeHead(frame.status || 200);
            started = true;
          } else if (frame.type === "chunk") {
            if (!started) {
              res.writeHead(200);
              started = true;
            }
            const data = Buffer.from(frame.body_base64 || "", "base64");
            if (data.length > 0) res.write(data);
          } else if (frame.type === "error") {
            killProcessGroup(child);
            reject(new Error(frame.error || "function stream failed"));
            return;
          }
        }
      });
      child.on("error", (err) => {
        if (timer) clearTimeout(timer);
        reject(err);
      });
      child.on("exit", (code) => {
        if (timer) clearTimeout(timer);
        if (code !== 0) reject(new Error(`function stream process failed: ${code}: ${stderr.toString()}`));
        else {
          if (!res.writableEnded) res.end();
          resolve();
        }
      });
      child.stdin.write(payload);
      child.stdin.write("\n");
      child.stdin.end();
    });
  }

  runWebSocket(conn, modulePath, handler, envVars, payload) {
    return new Promise((resolve, reject) => {
      const child = spawn(this.runnerPath, ["--websocket", modulePath, handler], {
        cwd: path.dirname(modulePath),
        env: mergeEnvironment(this.sandboxEnvVars(), envVars || {}),
        detached: true,
        stdio: ["pipe", "pipe", "pipe"]
      });
      let lineBuffer = "";
      let closed = false;
      const close = () => {
        if (closed) return;
        closed = true;
        killProcessGroup(child);
        conn.close();
      };
      child.stdout.on("data", (chunk) => {
        lineBuffer += chunk.toString();
        while (true) {
          const idx = lineBuffer.indexOf("\n");
          if (idx < 0) break;
          const line = lineBuffer.slice(0, idx);
          lineBuffer = lineBuffer.slice(idx + 1);
          let frame;
          try {
            frame = JSON.parse(line);
          } catch (err) {
            close();
            reject(new Error(`function websocket returned invalid JSON: ${err.message}`));
            return;
          }
          if (frame.type === "message") {
            if (frame.message_type === "binary") conn.sendBinary(Buffer.from(frame.data_base64 || "", "base64"));
            else conn.sendText(frame.data || "");
          } else if (frame.type === "close") {
            close();
            resolve();
          }
        }
      });
      child.on("error", reject);
      child.on("exit", () => {
        conn.close();
        resolve();
      });
      conn.on("message", (type, data) => {
        if (closed) return;
        if (type === "binary") child.stdin.write(JSON.stringify({ type: "message", message_type: "binary", data_base64: data.toString("base64") }) + "\n");
        else child.stdin.write(JSON.stringify({ type: "message", message_type: "text", data }) + "\n");
      });
      conn.on("close", () => {
        child.stdin.write(JSON.stringify({ type: "close" }) + "\n");
        close();
      });
      child.stdin.write(payload);
      child.stdin.write("\n");
    });
  }
}
