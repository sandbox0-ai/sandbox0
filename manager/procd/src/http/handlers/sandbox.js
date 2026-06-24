import { writeError, writeJSON } from "../response.js";
import { eventTypes } from "../../webhook/dispatcher.js";

export class SandboxHandler {
  constructor(contextManager, webhookDispatcher, logger) {
    this.contextManager = contextManager;
    this.webhookDispatcher = webhookDispatcher;
    this.logger = logger;
  }

  pause(_req, res) {
    const usage = this.contextManager.getAllResourceUsage();
    try {
      this.contextManager.pauseAll();
      this.webhookDispatcher?.enqueue({ event_type: eventTypes.sandboxPaused, payload: { resource_usage: usage } });
      writeJSON(res, 200, { paused: true, resource_usage: usage });
    } catch (err) {
      writeJSON(res, 500, { paused: false, error: err.message, resource_usage: usage });
    }
  }

  resume(_req, res) {
    try {
      this.contextManager.resumeAll();
      this.webhookDispatcher?.enqueue({ event_type: eventTypes.sandboxResumed, payload: { resumed: true } });
      writeJSON(res, 200, { resumed: true });
    } catch (err) {
      writeJSON(res, 500, { resumed: false, error: err.message });
    }
  }

  stats(_req, res) {
    try {
      writeJSON(res, 200, this.contextManager.getAllResourceUsage());
    } catch (err) {
      writeError(res, 500, "stats_failed", err.message);
    }
  }
}
