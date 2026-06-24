function parseDuration(value, fallbackMs) {
  if (value === undefined || value === null || value === "") return fallbackMs;
  if (typeof value === "number") return value;
  const text = String(value).trim();
  if (text === "") return fallbackMs;
  const match = text.match(/^(-?\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)?$/);
  if (!match) throw new Error(`invalid duration ${text}`);
  const n = Number(match[1]);
  const unit = match[2] || "ns";
  const factor = {
    ns: 1 / 1e6,
    us: 1 / 1e3,
    "µs": 1 / 1e3,
    ms: 1,
    s: 1000,
    m: 60 * 1000,
    h: 60 * 60 * 1000
  }[unit];
  return Math.trunc(n * factor);
}

function parseIntEnv(name, fallback) {
  const value = process.env[name];
  if (value === undefined || value === "") return fallback;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) throw new Error(`invalid integer ${name}=${value}`);
  return parsed;
}

export function loadConfig() {
  return {
    httpPort: parseIntEnv("http_port", 49983),
    logLevel: process.env.log_level || "info",
    rootPath: process.env.root_path || "/workspace",
    contextCleanupIntervalMs: parseDuration(process.env.context_cleanup_interval, 30_000),
    contextIdleTimeoutMs: parseDuration(process.env.context_idle_timeout, 0),
    contextMaxLifetimeMs: parseDuration(process.env.context_max_lifetime, 0),
    contextFinishedTtlMs: parseDuration(process.env.context_finished_ttl, 0),
    webhookQueueSize: parseIntEnv("webhook_queue_size", 256),
    webhookRequestTimeoutMs: parseDuration(process.env.webhook_request_timeout, 5_000),
    webhookMaxRetries: parseIntEnv("webhook_max_retries", 3),
    webhookBaseBackoffMs: parseDuration(process.env.webhook_base_backoff, 500),
    webhookOutboxDir: process.env.webhook_outbox_dir || "/procd/state/webhook-outbox",
    internalJWTPublicKeyPath: process.env.INTERNAL_JWT_PUBLIC_KEY_PATH || "/config/internal_jwt_public.key",
    functionRunnerPath: process.env.SANDBOX0_FUNCTION_RUNNER || "/procd/bin/python-runner",
    functionCacheRoot: process.env.SANDBOX0_FUNCTION_CACHE_ROOT || "/procd/state/functions"
  };
}
