import { readJSON, writeError, writeJSON } from "../response.js";

export class NextJSHandler {
  constructor(runtimeManager) {
    this.runtimeManager = runtimeManager;
  }

  async ensure(req, res) {
    try {
      const body = await readJSON(req);
      const view = await this.runtimeManager.ensure(body);
      writeJSON(res, 200, view);
    } catch (err) {
      writeError(res, 503, "nextjs_runtime_unavailable", err.message);
    }
  }
}
