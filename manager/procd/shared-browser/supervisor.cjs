'use strict';

const http = require('node:http');
const { spawn, execFile } = require('node:child_process');
const { promisify } = require('node:util');
const { setTimeout: delay } = require('node:timers/promises');
const { CONTROL_PORT, DesktopMaintenance } = require('./desktop.cjs');
const execute = promisify(execFile);

class BrowserSupervisor {
  constructor(executable, profile, desktop, dependencies = {}) {
    this.executable = executable;
    this.arguments = [`--user-data-dir=${profile}`, '--remote-debugging-address=127.0.0.1',
      '--remote-debugging-port=9222', '--no-first-run', '--no-default-browser-check',
      '--password-store=basic', '--disable-dev-shm-usage'];
    this.spawn = dependencies.spawn || spawn;
    this.execute = dependencies.execute || execute;
    this.probe = dependencies.probe || (async () => {
      const response = await fetch('http://127.0.0.1:9222/json/version', { signal: AbortSignal.timeout(1000) });
      return response.ok && typeof (await response.json()).webSocketDebuggerUrl === 'string';
    });
    this.controller = new AbortController();
    this.maintenance = new DesktopMaintenance(desktop, () => this.child?.pid, this.execute);
    this.onFailure = dependencies.onFailure || (error => { console.error(error); process.exitCode = 1; void this.close(); });
    this.child = null;
    this.starting = null;
    this.opening = null;
    this.restart = null;
    this.server = null;
  }
  async ensure(visible = false) {
    if (this.starting) return this.starting;
    this.controller.signal.throwIfAborted();
    if (this.child && this.child.exitCode === null && this.child.signalCode === null) return;
    clearTimeout(this.restart);
    this.starting = this.launch(visible);
    try { await this.starting; } finally { this.starting = null; }
  }
  async launch(visible) {
    const child = this.spawn(this.executable, [...this.arguments, ...(visible
      ? ['--start-maximized', '--restore-last-session'] : ['--no-startup-window'])], { stdio: 'inherit' });
    this.child = child;
    let spawnError;
    child.once('error', error => { spawnError = error; });
    child.once('exit', () => {
      if (this.child === child) this.child = null;
      if (!this.controller.signal.aborted) {
        this.restart = setTimeout(() => this.ensure().catch(this.onFailure), 500);
      }
    });
    const deadline = Date.now() + 30000;
    try {
      while (Date.now() < deadline) {
        this.controller.signal.throwIfAborted();
        if (spawnError) throw spawnError;
        if (child.exitCode !== null || child.signalCode !== null) throw new Error('Chromium exited during startup');
        if (await this.probe().catch(() => false)) return;
        await delay(100, undefined, { signal: this.controller.signal });
      }
      throw new Error('Chromium CDP did not become ready');
    } catch (error) { child.kill('SIGTERM'); throw error; }
  }
  open() {
    if (!this.opening) this.opening = this.restoreOrCreate().finally(() => { this.opening = null; });
    return this.opening;
  }
  async restoreOrCreate() {
    await this.ensure();
    const pid = this.child?.pid;
    const result = await this.execute('wmctrl', ['-lp'], { timeout: 5000, signal: this.controller.signal });
    const existing = result.stdout.split('\n').map(line => line.trim().split(/\s+/))
      .find(columns => Number(columns[2]) === pid && Number(columns[1]) >= -1);
    if (this.child?.pid !== pid) throw new Error('Browser changed while opening a window; retry');
    if (existing) {
      await this.execute('wmctrl', ['-ia', existing[0]], { timeout: 5000, signal: this.controller.signal });
    } else {
      await this.execute(this.executable, [...this.arguments, '--new-window', '--start-maximized', 'about:blank'],
        { timeout: 15000, signal: this.controller.signal });
    }
  }
  async listen(port = CONTROL_PORT) {
    await this.ensure(true);
    this.server = http.createServer(async (request, response) => {
      if (request.headers.origin || request.headers.host !== `127.0.0.1:${port}`) {
        response.writeHead(403).end(); return;
      }
      if (request.method !== 'POST' || request.url !== '/open') { response.writeHead(404).end(); return; }
      try { await this.open(); response.writeHead(204).end(); }
      catch { response.writeHead(503).end('Browser unavailable; try again.'); }
    });
    await new Promise((resolve, reject) => {
      this.server.once('error', reject);
      this.server.listen(port, '127.0.0.1', resolve);
    });
    this.server.on('error', this.onFailure);
    void this.monitor();
  }
  async monitor() {
    while (!this.controller.signal.aborted) {
      // Native windows can disappear between queries; do not tear down the desktop.
      await this.maintenance.refresh(this.controller.signal).catch(() => {});
      await delay(500, undefined, { signal: this.controller.signal }).catch(() => {});
    }
  }
  async close() {
    if (this.controller.signal.aborted) return;
    this.controller.abort();
    clearTimeout(this.restart);
    const child = this.child;
    if (child) {
      child.kill('SIGTERM');
      const deadline = Date.now() + 3000;
      while (child.exitCode === null && child.signalCode === null && Date.now() < deadline) await delay(50);
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    }
    if (this.server) {
      this.server.closeAllConnections();
      if (this.server.listening) await new Promise(resolve => this.server.close(resolve));
    }
  }
}

module.exports = { BrowserSupervisor };
if (require.main === module) {
  const [executable, profile, desktop] = process.argv.slice(2);
  const supervisor = new BrowserSupervisor(executable, profile, desktop);
  for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => { void supervisor.close(); });
  supervisor.listen().catch(supervisor.onFailure);
}
