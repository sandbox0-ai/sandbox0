import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import https from "node:https";
import path from "node:path";

export const eventTypes = {
  sandboxReady: "sandbox.ready",
  sandboxKilled: "sandbox.killed",
  sandboxPaused: "sandbox.paused",
  sandboxResumed: "sandbox.resumed",
  processStarted: "process.started",
  processExited: "process.exited",
  processCrashed: "process.crashed",
  fileModified: "file.modified",
  agentEvent: "agent.event"
};

function eventID() {
  return "evt_" + crypto.randomUUID();
}

function signPayload(secret, body) {
  return crypto.createHmac("sha256", secret).update(body).digest("hex");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function shouldRetry(status, err) {
  if (err) return true;
  return status === 429 || status >= 500;
}

async function postJSON(targetURL, body, signature, timeoutMs) {
  const parsed = new URL(targetURL);
  const client = parsed.protocol === "https:" ? https : http;
  return new Promise((resolve, reject) => {
    const req = client.request(parsed, {
      method: "POST",
      timeout: timeoutMs,
      headers: {
        "content-type": "application/json",
        "content-length": Buffer.byteLength(body),
        ...(signature ? { "x-sandbox0-signature": signature } : {})
      }
    }, (res) => {
      res.resume();
      res.on("end", () => resolve(res.statusCode || 0));
    });
    req.on("timeout", () => {
      req.destroy(new Error("webhook request timed out"));
    });
    req.on("error", reject);
    req.end(body);
  });
}

export class WebhookDispatcher {
  constructor(options, logger) {
    this.logger = logger;
    this.config = { url: "", secret: "" };
    this.identity = { sandboxID: "", teamID: "" };
    this.options = {
      queueSize: options.webhookQueueSize || 256,
      maxRetries: options.webhookMaxRetries ?? 3,
      baseBackoffMs: options.webhookBaseBackoffMs || 500,
      requestTimeoutMs: options.webhookRequestTimeoutMs || 5000,
      outboxDir: options.webhookOutboxDir || ""
    };
    this.queue = [];
    this.closed = false;
    this.draining = false;
    this.outboxTimer = null;
    if (this.options.outboxDir) {
      this.outboxTimer = setInterval(() => this.drainOutbox().catch((err) => this.logger?.warn("Webhook outbox drain failed", { error: err.message })), 1000);
      this.outboxTimer.unref();
    }
  }

  setConfig(url, secret = "") {
    this.config = { url: (url || "").trim(), secret: secret || "" };
  }

  setIdentity(sandboxID, teamID) {
    this.identity = { sandboxID: sandboxID || "", teamID: teamID || "" };
  }

  enqueue(event) {
    const fullEvent = {
      event_id: event.event_id || event.eventID || eventID(),
      event_type: event.event_type || event.eventType,
      timestamp: event.timestamp || new Date().toISOString(),
      sandbox_id: event.sandbox_id || event.sandboxID || this.identity.sandboxID,
      team_id: event.team_id || event.teamID || this.identity.teamID,
      payload: event.payload || {}
    };
    if (!this.config.url) return fullEvent.event_id;
    if (this.closed) throw new Error("webhook dispatcher closed");
    if (this.options.outboxDir) {
      this.writeRecord(fullEvent);
      this.drainOutbox().catch((err) => this.logger?.warn("Webhook outbox drain failed", { error: err.message }));
      return fullEvent.event_id;
    }
    if (this.queue.length >= this.options.queueSize) throw new Error("webhook queue full");
    this.queue.push(fullEvent);
    this.drainQueue().catch((err) => this.logger?.warn("Webhook delivery failed", { error: err.message }));
    return fullEvent.event_id;
  }

  async drainQueue() {
    if (this.draining) return;
    this.draining = true;
    try {
      while (this.queue.length > 0) {
        const event = this.queue.shift();
        await this.sendWithRetry(event);
      }
    } finally {
      this.draining = false;
    }
  }

  async sendWithRetry(event) {
    let lastErr = null;
    for (let attempt = 0; attempt <= this.options.maxRetries; attempt++) {
      let status = 0;
      try {
        status = await this.sendOnce(event);
        if (status >= 200 && status < 300) return;
      } catch (err) {
        lastErr = err;
      }
      if (!shouldRetry(status, lastErr) || attempt === this.options.maxRetries) break;
      await sleep(this.backoff(attempt + 1));
    }
    if (lastErr) this.logger?.warn("Webhook delivery failed", { event_id: event.event_id, error: lastErr.message });
  }

  async sendOnce(event) {
    const body = JSON.stringify(event);
    const signature = this.config.secret ? signPayload(this.config.secret, body) : "";
    return postJSON(this.config.url, body, signature, this.options.requestTimeoutMs);
  }

  writeRecord(event) {
    fs.mkdirSync(this.options.outboxDir, { recursive: true, mode: 0o700 });
    const body = JSON.stringify(event);
    const record = {
      event,
      target_url: this.config.url,
      body: JSON.parse(body),
      signature: this.config.secret ? signPayload(this.config.secret, body) : "",
      attempts: 0,
      next_attempt_at: "",
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };
    const target = path.join(this.options.outboxDir, `${event.event_id}.json`);
    if (fs.existsSync(target)) return;
    const tmp = path.join(this.options.outboxDir, `.${event.event_id}.${process.pid}.tmp`);
    fs.writeFileSync(tmp, JSON.stringify(record, null, 2), { mode: 0o600 });
    fs.renameSync(tmp, target);
  }

  async drainOutbox() {
    if (!this.options.outboxDir || !fs.existsSync(this.options.outboxDir)) return;
    const files = fs.readdirSync(this.options.outboxDir).filter((name) => name.endsWith(".json")).sort();
    const now = Date.now();
    for (const name of files) {
      const file = path.join(this.options.outboxDir, name);
      let record;
      try {
        record = JSON.parse(fs.readFileSync(file, "utf8"));
      } catch {
        fs.mkdirSync(path.join(this.options.outboxDir, "bad"), { recursive: true, mode: 0o700 });
        fs.renameSync(file, path.join(this.options.outboxDir, "bad", name));
        continue;
      }
      if (record.next_attempt_at && new Date(record.next_attempt_at).getTime() > now) continue;
      let status = 0;
      let err = null;
      try {
        const body = JSON.stringify(record.body);
        status = await postJSON(record.target_url, body, record.signature, this.options.requestTimeoutMs);
      } catch (cause) {
        err = cause;
      }
      if (!err && status >= 200 && status < 300) {
        fs.rmSync(file, { force: true });
        continue;
      }
      if (!shouldRetry(status, err)) {
        fs.mkdirSync(path.join(this.options.outboxDir, "failed"), { recursive: true, mode: 0o700 });
        fs.renameSync(file, path.join(this.options.outboxDir, "failed", name));
        continue;
      }
      record.attempts = (record.attempts || 0) + 1;
      record.updated_at = new Date().toISOString();
      record.next_attempt_at = new Date(Date.now() + this.backoff(record.attempts)).toISOString();
      record.last_error = err ? err.message : `http status ${status}`;
      fs.writeFileSync(file, JSON.stringify(record, null, 2), { mode: 0o600 });
    }
  }

  backoff(attempt) {
    const jitter = Math.floor(Math.random() * (this.options.baseBackoffMs / 2 + 1));
    return this.options.baseBackoffMs * Math.pow(2, Math.max(0, attempt - 1)) + jitter;
  }

  async shutdown() {
    this.closed = true;
    if (this.outboxTimer) clearInterval(this.outboxTimer);
    await this.drainQueue();
    await this.drainOutbox();
  }
}
