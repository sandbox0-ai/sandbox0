import fs from "node:fs";
import path from "node:path";
import { readJSON, writeError, writeJSON } from "../response.js";
import { eventTypes } from "../../webhook/dispatcher.js";

function ensureMountDirs(dirs) {
  for (let i = 0; i < (dirs || []).length; i++) {
    const dir = path.resolve(String(dirs[i] || "").trim());
    if (!path.isAbsolute(dir) || dir === path.parse(dir).root) {
      throw new Error(`mount_dirs[${i}] must be an absolute non-root path`);
    }
    fs.mkdirSync(dir, { recursive: true, mode: 0o755 });
  }
}

export class InitializeHandler {
  constructor(webhookDispatcher, fileManager, contextManager, httpPort, logger) {
    this.webhookDispatcher = webhookDispatcher;
    this.fileManager = fileManager;
    this.contextManager = contextManager;
    this.httpPort = httpPort;
    this.logger = logger;
    this.readySentKey = "";
    this.unsubscribe = null;
    this.watchPath = "";
  }

  async initialize(req, res) {
    let body;
    try {
      body = await readJSON(req);
      if (!body.sandbox_id) {
        writeError(res, 400, "invalid_request", "sandbox_id is required");
        return;
      }
      ensureMountDirs(body.mount_dirs || []);
    } catch (err) {
      writeError(res, err.message?.startsWith("mount_dirs") ? 500 : 400, err.message?.startsWith("mount_dirs") ? "mount_dir_init_failed" : "invalid_request", err.message);
      return;
    }
    const claims = req.internalClaims || {};
    let teamID = body.team_id || "";
    if (claims.team_id) {
      if (!teamID) teamID = claims.team_id;
      else if (teamID !== claims.team_id) {
        writeError(res, 403, "forbidden", "team_id does not match token");
        return;
      }
    }
    this.contextManager.setSandboxEnvVars(body.env_vars || {});
    const webhook = body.webhook || {};
    this.webhookDispatcher.setConfig(webhook.url || "", webhook.secret || "");
    this.webhookDispatcher.setIdentity(body.sandbox_id, teamID);
    this.configureWebhookWatch((webhook.url || "").trim(), (webhook.watch_dir || "").trim());
    if (webhook.url) {
      const readyKey = `${body.sandbox_id}\0${webhook.url}`;
      if (readyKey !== this.readySentKey) {
        try {
          this.webhookDispatcher.enqueue({
            event_type: eventTypes.sandboxReady,
            payload: { http_port: this.httpPort, sandbox_id: body.sandbox_id }
          });
          this.readySentKey = readyKey;
        } catch (err) {
          writeError(res, 503, "webhook_enqueue_failed", err.message);
          return;
        }
      }
    }
    writeJSON(res, 200, { sandbox_id: body.sandbox_id, team_id: teamID });
  }

  async updateSandboxEnvVars(req, res) {
    try {
      const body = await readJSON(req);
      this.contextManager.setSandboxEnvVars(body.env_vars || {});
      writeJSON(res, 200, { env_vars: this.contextManager.getSandboxEnvVars() });
    } catch (err) {
      writeError(res, 400, "invalid_request", err.message);
    }
  }

  configureWebhookWatch(webhookURL, watchDir) {
    if (!webhookURL || !watchDir) {
      if (this.unsubscribe) this.unsubscribe();
      this.unsubscribe = null;
      this.watchPath = "";
      return;
    }
    if (watchDir === this.watchPath) return;
    if (this.unsubscribe) this.unsubscribe();
    this.unsubscribe = null;
    this.watchPath = "";
    try {
      const { unsubscribe } = this.fileManager.subscribeWatch(watchDir, true, (event) => {
        if (event.type === "invalidate") return;
        const payload = { event_type: event.type, path: event.path };
        if (event.old_path) payload.old_path = event.old_path;
        this.webhookDispatcher.enqueue({ event_type: eventTypes.fileModified, payload });
      });
      this.unsubscribe = unsubscribe;
      this.watchPath = watchDir;
    } catch (err) {
      this.logger?.warn("Failed to watch webhook directory", { watch_dir: watchDir, error: err.message });
    }
  }
}
