export class Context {
  constructor(proc, cleanupPolicy = {}) {
    this.id = proc.id;
    this.type = proc.type;
    this.alias = proc.alias || "";
    this.command = [...(proc.command || [])];
    this.cwd = proc.cwd || "";
    this.envVars = { ...(proc.envVars || {}) };
    this.mainProcess = proc;
    this.createdAt = new Date();
    this.updatedAt = new Date();
    this.lastActivityAt = new Date();
    this.finishedAt = null;
    this.cleanupPolicy = cleanupPolicy || {};

    proc.on("output", () => this.touch());
    proc.on("exit", () => {
      this.finishedAt = new Date();
      this.touch();
    });
  }

  touch() {
    this.lastActivityAt = new Date();
    this.updatedAt = new Date();
  }

  isRunning() {
    return this.mainProcess?.isRunning() || false;
  }

  isPaused() {
    return this.mainProcess?.isPaused() || false;
  }

  pause() {
    this.touch();
    return this.mainProcess?.pause();
  }

  resume() {
    this.touch();
    return this.mainProcess?.resume();
  }

  restart() {
    this.touch();
    return this.mainProcess?.restart();
  }

  stop() {
    return this.mainProcess?.stop();
  }

  resourceUsage() {
    return this.mainProcess?.resourceUsage() || {};
  }

  shouldCleanup(now = Date.now()) {
    const policy = this.cleanupPolicy || {};
    if (policy.maxLifetimeMs > 0 && now - this.createdAt.getTime() > policy.maxLifetimeMs) return true;
    if (policy.finishedTtlMs > 0 && this.finishedAt && now - this.finishedAt.getTime() > policy.finishedTtlMs) return true;
    if (policy.idleTimeoutMs > 0 && now - this.lastActivityAt.getTime() > policy.idleTimeoutMs) return true;
    return false;
  }
}
