import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { createRequire } from "node:module";

const control = fs.createWriteStream(null, { fd: 3 });

function sendControl(frame) {
  control.write(JSON.stringify(frame) + "\n");
}

async function main() {
  const cwd = path.resolve(process.env.SANDBOX0_NEXTJS_CWD || process.cwd());
  const port = Number.parseInt(process.env.SANDBOX0_NEXTJS_PORT || process.env.PORT || "0", 10);
  if (!Number.isInteger(port) || port <= 0 || port > 65535) throw new Error("SANDBOX0_NEXTJS_PORT must be between 1 and 65535");

  const requireFromApp = createRequire(path.join(cwd, "package.json"));
  const nextModule = requireFromApp("next");
  const next = nextModule.default || nextModule;
  const app = next({ dev: process.env.NODE_ENV !== "production", dir: cwd, hostname: "0.0.0.0", port });
  const handle = app.getRequestHandler();
  await app.prepare();

  const server = http.createServer((req, res) => {
    Promise.resolve(handle(req, res)).catch((err) => {
      sendControl({ type: "request_error", message: err.message || String(err) });
      if (!res.headersSent) res.writeHead(500, { "content-type": "text/plain" });
      res.end("internal server error");
    });
  });
  await new Promise((resolve, reject) => {
    server.on("error", reject);
    server.listen(port, "0.0.0.0", resolve);
  });
  sendControl({ type: "ready", port, pid: process.pid });

  const shutdown = () => {
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 5000).unref();
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  sendControl({ type: "error", message: err.stack || err.message || String(err) });
  process.exit(1);
});
