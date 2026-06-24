export const codes = {
  badRequest: "bad_request",
  unauthorized: "unauthorized",
  forbidden: "forbidden",
  notFound: "not_found",
  conflict: "conflict",
  unavailable: "unavailable",
  internal: "internal_error"
};

export function writeJSON(res, status, data) {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify({ success: true, data }) + "\n");
}

export function writeError(res, status, code, message, details) {
  const error = { code, message };
  if (details !== undefined) error.details = details;
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify({ success: false, error }) + "\n");
}

export async function readBody(req, limit = 16 << 20) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > limit) {
      const err = new Error("request body too large");
      err.code = "request_too_large";
      throw err;
    }
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

export async function readJSON(req, limit) {
  const body = await readBody(req, limit);
  if (body.length === 0) return {};
  return JSON.parse(body.toString("utf8"));
}
