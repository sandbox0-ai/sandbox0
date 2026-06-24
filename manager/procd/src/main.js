#!/usr/bin/env node
import { loadConfig } from "./config.js";
import { Logger } from "./util/logger.js";
import { configureNetdMITMCATrust } from "./trust/netd-mitm.js";
import { InternalAuthValidator } from "./http/auth.js";
import { ContextManager } from "./context/manager.js";
import { FileManager } from "./file/manager.js";
import { WebhookDispatcher, eventTypes } from "./webhook/dispatcher.js";
import { FunctionHandler } from "./function/handler.js";
import { NextJSRuntimeManager } from "./nextjs/runtime.js";
import { ContextHandler } from "./http/handlers/context.js";
import { FileHandler } from "./http/handlers/file.js";
import { SandboxHandler } from "./http/handlers/sandbox.js";
import { InitializeHandler } from "./http/handlers/initialize.js";
import { WebhookHandler } from "./http/handlers/webhook.js";
import { NextJSHandler } from "./http/handlers/nextjs.js";
import { ProcdHTTPServer } from "./http/server.js";

async function main() {
  const cfg = loadConfig();
  const logger = new Logger("procd", cfg.logLevel);
  logger.info("Starting Procd", { version: "js-0.1.0" });
  const bundlePath = configureNetdMITMCATrust();
  if (bundlePath) logger.info("Configured netd MITM CA trust", { bundle_path: bundlePath });

  const validator = new InternalAuthValidator(cfg.internalJWTPublicKeyPath, logger);
  const contextManager = new ContextManager(logger);
  contextManager.setDefaultCleanupPolicy({
    idleTimeoutMs: cfg.contextIdleTimeoutMs,
    maxLifetimeMs: cfg.contextMaxLifetimeMs,
    finishedTtlMs: cfg.contextFinishedTtlMs
  });
  contextManager.startCleanup(cfg.contextCleanupIntervalMs);

  const webhookDispatcher = new WebhookDispatcher(cfg, logger);
  contextManager.on("processStart", (event) => {
    try {
      webhookDispatcher.enqueue({ event_type: eventTypes.processStarted, payload: event });
    } catch (err) {
      logger.warn("Failed to enqueue process start webhook", { error: err.message });
    }
  });
  contextManager.on("processExit", (event) => {
    try {
      const eventType = event.state === "crashed" || event.state === "killed" ? eventTypes.processCrashed : eventTypes.processExited;
      webhookDispatcher.enqueue({ event_type: eventType, payload: event });
    } catch (err) {
      logger.warn("Failed to enqueue process exit webhook", { error: err.message });
    }
  });

  const fileManager = new FileManager(cfg.rootPath);
  const nextjsRuntimeManager = new NextJSRuntimeManager(logger);
  const functionHandler = new FunctionHandler(cfg, contextManager, logger);
  const server = new ProcdHTTPServer({
    config: cfg,
    logger,
    validator,
    contextHandler: new ContextHandler(contextManager),
    fileHandler: new FileHandler(fileManager),
    sandboxHandler: new SandboxHandler(contextManager, webhookDispatcher, logger),
    initializeHandler: new InitializeHandler(webhookDispatcher, fileManager, contextManager, cfg.httpPort, logger),
    webhookHandler: new WebhookHandler(webhookDispatcher),
    functionHandler,
    nextjsHandler: new NextJSHandler(nextjsRuntimeManager)
  });

  const shutdown = async (signal) => {
    logger.info("Received shutdown signal", { signal });
    try {
      webhookDispatcher.enqueue({ event_type: eventTypes.sandboxKilled, payload: { signal, reason: "shutdown" } });
    } catch {
      // Ignore during shutdown.
    }
    await server.shutdown().catch((err) => logger.error("HTTP server shutdown error", { error: err.message }));
    contextManager.cleanup();
    fileManager.close();
    await nextjsRuntimeManager.closeAll().catch((err) => logger.warn("Next.js runtime shutdown error", { error: err.message }));
    await webhookDispatcher.shutdown().catch((err) => logger.warn("Webhook dispatcher shutdown error", { error: err.message }));
    logger.info("Procd shutdown complete");
    process.exit(0);
  };
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  await server.start();
  logger.info("Procd is ready", { http_port: cfg.httpPort, root_path: cfg.rootPath });
}

main().catch((err) => {
  process.stderr.write(`Failed to start procd: ${err.stack || err.message}\n`);
  process.exit(1);
});
