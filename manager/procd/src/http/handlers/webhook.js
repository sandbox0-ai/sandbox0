import { readJSON, writeError, writeJSON } from "../response.js";
import { eventTypes } from "../../webhook/dispatcher.js";

export class WebhookHandler {
  constructor(dispatcher) {
    this.dispatcher = dispatcher;
  }

  async publish(req, res) {
    try {
      const body = await readJSON(req);
      const eventID = this.dispatcher.enqueue({
        event_id: body.event_id || "",
        event_type: eventTypes.agentEvent,
        payload: body.payload || {}
      });
      writeJSON(res, 202, { event_id: eventID });
    } catch (err) {
      writeError(res, 503, "webhook_enqueue_failed", err.message);
    }
  }
}
