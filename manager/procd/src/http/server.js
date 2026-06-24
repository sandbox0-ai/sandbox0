import http from "node:http";
import { codes, writeError, writeJSON } from "./response.js";
import { requireInternalAuth } from "./auth.js";
import { acceptWebSocket, isWebSocketUpgrade } from "./websocket.js";

function compileRoute(method, pattern, handler, auth = true) {
  const names = [];
  const source = pattern.split("/").map((part) => {
    if (part.startsWith(":")) {
      names.push(part.slice(1));
      return "([^/]+)";
    }
    return part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }).join("/");
  return { method, pattern, regex: new RegExp(`^${source}$`), names, handler, auth };
}

function matchRoute(routes, method, pathname) {
  for (const route of routes) {
    if (route.method !== method) continue;
    const match = pathname.match(route.regex);
    if (!match) continue;
    const params = {};
    route.names.forEach((name, i) => { params[name] = decodeURIComponent(match[i + 1]); });
    return { route, params };
  }
  return null;
}

function isLocalhost(req) {
  const addr = req.socket.remoteAddress;
  return addr === "127.0.0.1" || addr === "::1" || addr === "::ffff:127.0.0.1";
}

export class ProcdHTTPServer {
  constructor(deps) {
    this.config = deps.config;
    this.logger = deps.logger;
    this.validator = deps.validator;
    this.contextHandler = deps.contextHandler;
    this.fileHandler = deps.fileHandler;
    this.sandboxHandler = deps.sandboxHandler;
    this.initializeHandler = deps.initializeHandler;
    this.webhookHandler = deps.webhookHandler;
    this.functionHandler = deps.functionHandler;
    this.nextjsHandler = deps.nextjsHandler;
    this.probeRunner = deps.probeRunner;
    this.routes = this.buildRoutes();
    this.server = http.createServer((req, res) => this.handle(req, res));
    this.server.on("upgrade", (req, socket, head) => this.handleUpgrade(req, socket, head));
  }

  buildRoutes() {
    const auth = true;
    return [
      compileRoute("GET", "/healthz", (req, res) => this.health(req, res), false),
      compileRoute("GET", "/readyz", (req, res) => this.ready(req, res), false),
      compileRoute("GET", "/sandbox-probes/:kind", (req, res, params) => this.sandboxProbe(req, res, params), false),
      compileRoute("POST", "/sandbox-probes/:kind", (req, res, params) => this.sandboxProbe(req, res, params), false),
      compileRoute("POST", "/api/v1/webhook/publish", (req, res) => {
        if (!isLocalhost(req)) {
          writeError(res, 403, codes.forbidden, "local access only");
          return;
        }
        return this.webhookHandler.publish(req, res);
      }, false),
      compileRoute("POST", "/api/v1/sandbox/pause", (req, res) => this.sandboxHandler.pause(req, res), auth),
      compileRoute("POST", "/api/v1/sandbox/resume", (req, res) => this.sandboxHandler.resume(req, res), auth),
      compileRoute("GET", "/api/v1/sandbox/stats", (req, res) => this.sandboxHandler.stats(req, res), auth),
      compileRoute("GET", "/api/v1/contexts", (req, res) => this.contextHandler.list(req, res), auth),
      compileRoute("POST", "/api/v1/contexts", (req, res) => this.contextHandler.create(req, res), auth),
      compileRoute("GET", "/api/v1/contexts/:id", (req, res, params) => this.contextHandler.get(req, res, params), auth),
      compileRoute("DELETE", "/api/v1/contexts/:id", (req, res, params) => this.contextHandler.delete(req, res, params), auth),
      compileRoute("POST", "/api/v1/contexts/:id/restart", (req, res, params) => this.contextHandler.restart(req, res, params), auth),
      compileRoute("POST", "/api/v1/contexts/:id/input", (req, res, params) => this.contextHandler.writeInput(req, res, params), auth),
      compileRoute("POST", "/api/v1/contexts/:id/exec", (req, res, params) => this.contextHandler.exec(req, res, params), auth),
      compileRoute("POST", "/api/v1/contexts/:id/resize", (req, res, params) => this.contextHandler.resize(req, res, params), auth),
      compileRoute("POST", "/api/v1/contexts/:id/signal", (req, res, params) => this.contextHandler.signal(req, res, params), auth),
      compileRoute("GET", "/api/v1/contexts/:id/stats", (req, res, params) => this.contextHandler.stats(req, res, params), auth),
      compileRoute("POST", "/api/v1/functions/execute", (req, res) => this.functionHandler.execute(req, res), auth),
      compileRoute("POST", "/api/v1/functions/stream", (req, res) => this.functionHandler.stream(req, res), auth),
      compileRoute("POST", "/api/v1/initialize", (req, res) => this.initializeHandler.initialize(req, res), auth),
      compileRoute("PUT", "/api/v1/sandbox/env_vars", (req, res) => this.initializeHandler.updateSandboxEnvVars(req, res), auth),
      compileRoute("POST", "/api/v1/services/nextjs/ensure", (req, res) => this.nextjsHandler.ensure(req, res), auth),
      compileRoute("GET", "/api/v1/files", (req, res) => this.fileHandler.handle(req, res), auth),
      compileRoute("POST", "/api/v1/files", (req, res) => this.fileHandler.handle(req, res), auth),
      compileRoute("DELETE", "/api/v1/files", (req, res) => this.fileHandler.handle(req, res), auth),
      compileRoute("POST", "/api/v1/files/move", (req, res) => this.fileHandler.move(req, res), auth),
      compileRoute("GET", "/api/v1/files/stat", (req, res) => this.fileHandler.stat(req, res), auth),
      compileRoute("GET", "/api/v1/files/list", (req, res) => this.fileHandler.list(req, res), auth)
    ];
  }

  async handle(req, res) {
    const started = Date.now();
    const pathname = new URL(req.url, "http://localhost").pathname;
    const matched = matchRoute(this.routes, req.method, pathname);
    if (!matched) {
      writeError(res, 404, codes.notFound, "not found");
      return;
    }
    const handler = matched.route.auth ? requireInternalAuth(this.validator, matched.route.handler) : matched.route.handler;
    try {
      await handler(req, res, matched.params);
    } catch (err) {
      this.logger?.error("HTTP handler failed", { path: pathname, error: err.stack || err.message });
      if (!res.headersSent) writeError(res, 500, codes.internal, "internal server error");
      else res.end();
    } finally {
      this.logger?.info("HTTP request", { method: req.method, path: pathname, status: res.statusCode, duration_ms: Date.now() - started });
    }
  }

  handleUpgrade(req, socket, head) {
    const pathname = new URL(req.url, "http://localhost").pathname;
    if (!isWebSocketUpgrade(req)) {
      socket.destroy();
      return;
    }
    let claims;
    try {
      claims = this.validator.validate(req.headers["x-internal-token"]);
      req.internalClaims = claims;
    } catch {
      socket.write("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
      socket.destroy();
      return;
    }
    const contextMatch = pathname.match(/^\/api\/v1\/contexts\/([^/]+)\/ws$/);
    const conn = acceptWebSocket(req, socket, head);
    if (!conn) return;
    if (contextMatch) {
      this.contextHandler.websocket(conn, { id: decodeURIComponent(contextMatch[1]) });
      return;
    }
    if (pathname === "/api/v1/files/watch") {
      this.fileHandler.websocket(conn);
      return;
    }
    if (pathname === "/api/v1/functions/ws") {
      conn.once("message", async (_type, data) => {
        try {
          const initReq = JSON.parse(String(data));
          await this.functionHandler.websocket(conn, initReq);
        } catch (err) {
          conn.close(err.message);
        }
      });
      return;
    }
    conn.close("not found");
  }

  health(_req, res) {
    const result = this.runProbe("liveness");
    if (result.status === "failed") {
      writeError(res, 503, codes.unavailable, result.message || "sandbox probe failed");
      return;
    }
    writeJSON(res, 200, { status: "healthy" });
  }

  ready(_req, res) {
    const result = this.runProbe("readiness");
    if (result.status && result.status !== "passed") {
      writeError(res, 503, codes.unavailable, result.message || "sandbox probe failed");
      return;
    }
    writeJSON(res, 200, { status: "ready" });
  }

  sandboxProbe(_req, res, params) {
    if (!["liveness", "readiness", "startup"].includes(params.kind)) {
      writeError(res, 400, codes.badRequest, "invalid sandbox probe kind");
      return;
    }
    const result = this.runProbe(params.kind);
    res.writeHead(result.status === "failed" ? 503 : 200, { "content-type": "application/json" });
    res.end(JSON.stringify(result) + "\n");
  }

  runProbe(kind) {
    if (this.probeRunner) return this.probeRunner(kind);
    return { kind, status: "passed", reason: "SandboxProbePassed", message: "sandbox probe passed" };
  }

  start() {
    return new Promise((resolve) => {
      this.server.listen(this.config.httpPort, "0.0.0.0", () => resolve());
    });
  }

  shutdown() {
    return new Promise((resolve, reject) => {
      this.server.close((err) => err ? reject(err) : resolve());
    });
  }
}
