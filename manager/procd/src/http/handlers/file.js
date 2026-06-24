import { readBody, readJSON, writeError, writeJSON } from "../response.js";
import { FileError } from "../../file/errors.js";

function acceptsJSON(req) {
  return String(req.headers.accept || "").includes("application/json") || String(req.headers["content-type"] || "").includes("application/json");
}

function handleFileError(res, err) {
  if (err instanceof FileError) {
    const status = {
      file_not_found: 404,
      directory_not_found: 404,
      file_too_large: 413,
      permission_denied: 403,
      path_exists: 409,
      path_not_directory: 409,
      path_not_file: 409
    }[err.code] || 500;
    writeError(res, status, err.code, err.message);
    return;
  }
  writeError(res, 500, "operation_failed", err.message || String(err));
}

export class FileHandler {
  constructor(manager) {
    this.manager = manager;
  }

  async handle(req, res) {
    const url = new URL(req.url, "http://localhost");
    const filePath = url.searchParams.get("path");
    if (!filePath) {
      writeError(res, 400, "invalid_request", "path is required");
      return;
    }
    try {
      if (req.method === "GET") {
        if (url.searchParams.has("stat") || url.searchParams.has("list")) {
          writeError(res, 400, "invalid_request", "stat/list queries are not supported");
          return;
        }
        const data = this.manager.readFile(filePath);
        if (acceptsJSON(req)) {
          writeJSON(res, 200, { content: data.toString("base64"), encoding: "base64" });
        } else {
          res.writeHead(200, { "content-type": "application/octet-stream" });
          res.end(data);
        }
      } else if (req.method === "POST") {
        if (url.searchParams.get("mkdir") === "true") {
          this.manager.makeDir(filePath, url.searchParams.get("recursive") === "true");
          writeJSON(res, 201, { created: true });
          return;
        }
        this.manager.writeFile(filePath, await readBody(req));
        writeJSON(res, 200, { written: true });
      } else if (req.method === "DELETE") {
        this.manager.remove(filePath);
        writeJSON(res, 200, { deleted: true });
      } else {
        writeError(res, 405, "method_not_allowed", "method not allowed");
      }
    } catch (err) {
      handleFileError(res, err);
    }
  }

  stat(req, res) {
    const filePath = new URL(req.url, "http://localhost").searchParams.get("path");
    if (!filePath) {
      writeError(res, 400, "invalid_request", "path is required");
      return;
    }
    try {
      writeJSON(res, 200, this.manager.stat(filePath));
    } catch (err) {
      handleFileError(res, err);
    }
  }

  list(req, res) {
    const filePath = new URL(req.url, "http://localhost").searchParams.get("path");
    if (!filePath) {
      writeError(res, 400, "invalid_request", "path is required");
      return;
    }
    try {
      writeJSON(res, 200, { entries: this.manager.listDir(filePath) });
    } catch (err) {
      handleFileError(res, err);
    }
  }

  async move(req, res) {
    try {
      const body = await readJSON(req);
      if (!body.source || !body.destination) {
        writeError(res, 400, "invalid_paths", "source and destination are required");
        return;
      }
      this.manager.move(body.source, body.destination);
      writeJSON(res, 200, { moved: true });
    } catch (err) {
      handleFileError(res, err);
    }
  }

  websocket(conn) {
    const subscriptions = new Map();
    conn.on("message", (_type, data) => {
      let req;
      try {
        req = JSON.parse(String(data));
      } catch {
        return;
      }
      if (req.action === "subscribe") {
        try {
          const { watcher, unsubscribe } = this.manager.subscribeWatch(req.path, Boolean(req.recursive), (event) => {
            conn.sendJSON({ type: "event", watch_id: event.watch_id, event: event.type, path: event.path });
          });
          subscriptions.set(watcher.id, unsubscribe);
          conn.sendJSON({ type: "subscribed", watch_id: watcher.id, path: req.path });
        } catch (err) {
          conn.sendJSON({ type: "error", error: err.message });
        }
      } else if (req.action === "unsubscribe") {
        const unsubscribe = subscriptions.get(req.watch_id);
        if (unsubscribe) {
          unsubscribe();
          subscriptions.delete(req.watch_id);
          conn.sendJSON({ type: "unsubscribed", watch_id: req.watch_id });
        }
      }
    });
    conn.on("close", () => {
      for (const unsubscribe of subscriptions.values()) unsubscribe();
      subscriptions.clear();
    });
  }
}
