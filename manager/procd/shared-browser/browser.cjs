#!/usr/bin/env node
'use strict';

const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const http = require('node:http');
const net = require('node:net');
const { spawn, execFileSync } = require('node:child_process');
const { setTimeout: delay } = require('node:timers/promises');

const HELP = `Usage: sandbox0-browser [options]

Start one shared Chromium desktop, a web viewer, and sandbox-local CDP.
The command stays in the foreground; run it in a supervised session or service.

  --host ADDRESS     Viewer listen address (default: 127.0.0.1)
  --port PORT        Viewer HTTP/WebSocket port (default: 6080 or SANDBOX0_SERVICE_PORT)
  --profile PATH     Persistent profile under /workspace (default: /workspace/.browser/profile)
  --downloads PATH   Download directory under /workspace (default: /workspace/browser-downloads)
  --size WIDTHxHEIGHT  Initial desktop size (default: 1440x900)
  --help             Show this help
  --version          Show runtime version

VNC stays on 127.0.0.1:5900; CDP stays on 127.0.0.1:9222.
The viewer provides GET /, GET /healthz, and WebSocket /vnc.
Use Private Preview, or an authenticated Sandbox Service, to access the viewer.
`;

function workspacePath(value) {
  if (!path.isAbsolute(value) || path.normalize(value) !== value ||
      !value.startsWith('/workspace/')) {
    throw new Error('Browser profile and downloads must be canonical paths under /workspace');
  }
  return value;
}

function options(args, env = process.env) {
  const config = {
    host: env.SANDBOX0_BROWSER_HOST || '127.0.0.1',
    port: env.SANDBOX0_SERVICE_PORT || '6080',
    profile: env.SANDBOX0_BROWSER_PROFILE || '/workspace/.browser/profile',
    downloads: env.SANDBOX0_BROWSER_DOWNLOADS || '/workspace/browser-downloads',
    size: '1440x900',
    browserUser: env.SANDBOX0_BROWSER_USER || 'sandbox-browser',
    assets: path.join(__dirname, 'novnc'),
  };
  for (let index = 0; index < args.length; index++) {
    const key = { '--host': 'host', '--port': 'port', '--profile': 'profile',
      '--downloads': 'downloads', '--size': 'size' }[args[index]];
    if (!key || !args[index + 1]) throw new Error(`Unknown or incomplete option: ${args[index]}`);
    config[key] = args[++index];
  }
  if (!/^\d+$/.test(String(config.port)) || Number(config.port) < 1 || Number(config.port) > 65535 ||
      [5900, 9222].includes(Number(config.port))) throw new Error('Invalid or reserved viewer port');
  config.port = Number(config.port);
  if (!net.isIP(config.host)) throw new Error('--host must be an IP address');
  const size = /^(\d{3,4})x(\d{3,4})$/.exec(config.size);
  if (!size || Number(size[1]) < 320 || Number(size[2]) < 240 ||
      Number(size[1]) > 4096 || Number(size[2]) > 4096) throw new Error('Invalid desktop size');
  workspacePath(config.profile);
  workspacePath(config.downloads);
  if (config.profile === config.downloads || config.downloads.startsWith(config.profile + '/') ||
      config.profile.startsWith(config.downloads + '/')) throw new Error('Profile and downloads must be separate directories');
  return config;
}

function ensureDirectory(directory) {
  let current = '/';
  for (const segment of directory.split('/').filter(Boolean)) {
    current = path.join(current, segment);
    try { fs.mkdirSync(current, { mode: 0o700 }); } catch (error) { if (error.code !== 'EEXIST') throw error; }
    if (!fs.lstatSync(current).isDirectory() || fs.realpathSync(current) !== current) {
      throw new Error('Browser directories must not contain symbolic links');
    }
  }
}

function profileIsRunning(profile, procRoot = '/proc') {
  for (const name of fs.readdirSync(procRoot)) {
    if (!/^\d+$/.test(name)) continue;
    try {
      const args = fs.readFileSync(path.join(procRoot, name, 'cmdline'), 'utf8').split('\0');
      if (args.includes('--user-data-dir=' + profile) ||
          args.some((arg, index) => arg === '--user-data-dir' && args[index + 1] === profile)) return true;
    } catch (error) { if (!['ENOENT', 'ESRCH'].includes(error.code)) throw error; }
  }
  return false;
}

function recoverProfile(profile, isRunning = profileIsRunning) {
  if (!fs.lstatSync(profile).isDirectory() || fs.realpathSync(profile) !== profile) {
    throw new Error('Browser profile is not a canonical directory');
  }
  // Scan actual command lines rather than trusting a retained hostname/PID:
  // RootFS recovery can change the hostname, and a PID can belong to another process.
  if (isRunning(profile)) throw new Error('Browser profile is already in use');
  const locks = [];
  for (const name of ['SingletonLock', 'SingletonSocket', 'SingletonCookie']) {
    const file = path.join(profile, name);
    try {
      const stat = fs.lstatSync(file);
      if (!stat.isSymbolicLink()) throw new Error(`Refusing to remove non-symlink browser lock: ${name}`);
      locks.push({ file, inode: stat.ino, target: fs.readlinkSync(file) });
    } catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
  if (isRunning(profile)) throw new Error('Browser profile became active during recovery');
  for (const lock of locks) {
    if (fs.lstatSync(lock.file).ino !== lock.inode || fs.readlinkSync(lock.file) !== lock.target) {
      throw new Error('Browser profile locks changed during recovery');
    }
  }
  for (const lock of locks) fs.unlinkSync(lock.file);
}

function prepareProfile(config) {
  ensureDirectory(config.profile);
  ensureDirectory(config.downloads);
  recoverProfile(config.profile);
  const preferences = path.join(config.profile, 'Default', 'Preferences');
  ensureDirectory(path.dirname(preferences));
  let contents = {};
  try {
    if (!fs.lstatSync(preferences).isFile()) throw new Error('Browser Preferences must be a regular file');
    contents = JSON.parse(fs.readFileSync(preferences, 'utf8'));
  } catch (error) { if (error.code !== 'ENOENT') throw error; }
  contents.download = { ...contents.download, default_directory: config.downloads, prompt_for_download: false };
  fs.writeFileSync(preferences, JSON.stringify(contents), { mode: 0o600 });
  execFileSync('chown', ['-R', `${config.browserUser}:${config.browserUser}`, config.profile, config.downloads]);
  // Imported guest roots may start at 0700. Grant directory traversal while
  // preserving existing read/write bits, including the browser user's parents.
  for (const directory of [config.profile, config.downloads]) {
    for (let parent = path.dirname(directory); ; parent = path.dirname(parent)) {
      fs.chmodSync(parent, (fs.statSync(parent).mode & 0o7777) | 0o111);
      if (parent === '/') break;
    }
  }
}

function wsLibrary() {
  try { return require('ws'); } catch (error) {
    if (error.code !== 'MODULE_NOT_FOUND') throw error;
    return require('/opt/coding-agents/node_modules/ws');
  }
}

function originAllowed(request) {
  if (!request.headers.origin) return true; // Authenticated backend clients have no browser Origin.
  try {
    const origin = new URL(request.headers.origin);
    return ['http:', 'https:'].includes(origin.protocol) &&
      [request.headers.host, request.headers['x-forwarded-host']].includes(origin.host);
  } catch { return false; }
}

function viewerServer(config, library = wsLibrary()) {
  const server = http.createServer(async (request, response) => {
    response.setHeader('Cache-Control', 'no-store');
    response.setHeader('X-Content-Type-Options', 'nosniff');
    response.setHeader('Referrer-Policy', 'no-referrer');
    response.setHeader('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'");
    if (!['GET', 'HEAD'].includes(request.method)) { response.writeHead(405, { Allow: 'GET, HEAD' }).end(); return; }
    try {
      const pathname = decodeURIComponent(request.url.split('?')[0]);
      if (pathname === '/healthz') {
        response.writeHead(200, { 'Content-Type': 'application/json' });
        response.end(request.method === 'HEAD' ? undefined : '{"ready":true}');
        return;
      }
      if (pathname.includes('\\') || pathname.includes('\0') || pathname.split('/').some(part => ['..', '.'].includes(part))) {
        response.writeHead(400).end(); return;
      }
      let root, relative;
      if (['/', '/viewer.js', '/viewer.css'].includes(pathname)) {
        root = __dirname;
        relative = pathname === '/' ? 'viewer.html' : pathname.slice(1);
      } else if (/^\/novnc\/(?:core|vendor)\/.+\.js$/.test(pathname)) {
        root = config.assets;
        relative = pathname.slice('/novnc/'.length);
      } else { response.writeHead(404).end(); return; }
      const file = await fs.promises.realpath(path.join(root, relative));
      if (!file.startsWith(path.resolve(root) + path.sep) || !(await fs.promises.stat(file)).isFile()) {
        response.writeHead(404).end(); return;
      }
      const mime = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8' }[path.extname(file)];
      const body = request.method === 'HEAD' ? undefined : await fs.promises.readFile(file);
      response.writeHead(200, { 'Content-Type': mime });
      response.end(body);
    } catch (error) {
      response.writeHead(error instanceof URIError ? 400 : 404).end();
    }
  });
  const websocket = new library.WebSocketServer({ noServer: true, perMessageDeflate: false, maxPayload: 64 * 1024 });
  server.on('upgrade', (request, socket, head) => {
    if (request.url !== '/vnc' || !originAllowed(request)) {
      socket.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n'); return;
    }
    websocket.handleUpgrade(request, socket, head, viewer => websocket.emit('connection', viewer));
  });
  websocket.on('connection', viewer => {
    const upstream = net.connect({ host: '127.0.0.1', port: config.vncPort || 5900 });
    upstream.setNoDelay(true);
    const close = () => { upstream.destroy(); if (viewer.readyState === library.WebSocket.OPEN) viewer.close(1011, 'Reconnect the browser viewer'); };
    upstream.on('data', data => {
      if (viewer.readyState !== library.WebSocket.OPEN) return;
      if (viewer.bufferedAmount + data.length > 1024 * 1024) { close(); return; }
      viewer.send(data, { binary: true }, error => { if (error) close(); });
    });
    viewer.on('message', (data, binary) => {
      if (!binary || upstream.destroyed || upstream.writableLength + data.length > 1024 * 1024) { close(); return; }
      upstream.write(data);
    });
    upstream.on('error', close);
    upstream.on('close', close);
    viewer.on('error', close);
    viewer.on('close', () => upstream.destroy());
  });
  return {
    server,
    async close() {
      for (const viewer of websocket.clients) viewer.terminate();
      await new Promise(resolve => websocket.close(resolve));
      server.closeAllConnections();
      if (server.listening) await new Promise(resolve => server.close(resolve));
    },
  };
}

class Processes {
  constructor() { this.children = []; this.controller = new AbortController(); this.failure = null; }
  start(command, args, env, log) {
    const fd = fs.openSync(log, 'a', 0o600);
    let child;
    try { child = spawn(command, args, { env, detached: true, stdio: ['ignore', fd, fd] }); }
    finally { fs.closeSync(fd); }
    this.children.push(child);
    const failed = error => {
      if (!this.controller.signal.aborted) { this.failure = error; this.controller.abort(); }
    };
    child.once('error', failed);
    child.once('exit', (code, signal) => failed(new Error(`${path.basename(command)} exited (${signal || code}); see ${log}`)));
    return child;
  }
  async close() {
    this.controller.abort();
    const signal = (child, value) => {
      if (!child.pid) return false;
      try { process.kill(-child.pid, value); return true; } catch (error) { if (error.code !== 'ESRCH') throw error; return false; }
    };
    for (const child of this.children.toReversed()) signal(child, 'SIGTERM');
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline && this.children.some(child => signal(child, 0))) await delay(50);
    for (const child of this.children.toReversed()) signal(child, 'SIGKILL');
  }
}

async function waitReady(probe, signal, label, timeout = 30000) {
  const deadline = Date.now() + timeout;
  while (!signal.aborted && Date.now() < deadline) {
    try { if (await probe()) return; } catch { /* The child is still starting. */ }
    await delay(50, undefined, { signal });
  }
  throw new Error(`Timed out waiting for ${label}`);
}

function tcpReady(port) {
  return new Promise(resolve => {
    const socket = net.connect({ host: '127.0.0.1', port });
    socket.setTimeout(500);
    socket.once('connect', () => { socket.destroy(); resolve(true); });
    socket.once('error', () => { socket.destroy(); resolve(false); });
    socket.once('timeout', () => { socket.destroy(); resolve(false); });
  });
}

function browserExecutable() {
  for (const name of ['google-chrome-stable', 'google-chrome']) {
    for (const directory of (process.env.PATH || '').split(':')) {
      const file = path.join(directory, name);
      try { fs.accessSync(file, fs.constants.X_OK); return file; } catch { /* Try bundled Chromium. */ }
    }
  }
  return require('/opt/coding-agents/node_modules/playwright-core').chromium.executablePath();
}

async function run(config) {
  if (process.platform !== 'linux' || process.getuid() !== 0) throw new Error('Run sandbox0-browser as the sandbox root user on Linux');
  if (!fs.existsSync(path.join(config.assets, 'core', 'rfb.js'))) throw new Error('The coding-agent image is missing the bundled viewer');
  for (const port of [5900, 9222]) if (await tcpReady(port)) throw new Error(`Browser port ${port} is already in use`);
  prepareProfile(config);
  const logs = '/tmp/sandbox0-browser';
  ensureDirectory(logs);
  const processes = new Processes();
  let viewer;
  const stop = () => processes.controller.abort();
  process.on('SIGTERM', stop);
  process.on('SIGINT', stop);
  try {
    const env = { ...process.env, HOME: logs, DISPLAY: ':99' };
    processes.start('Xtigervnc', [':99', '-geometry', config.size, '-depth', '24',
      '-rfbport', '5900', '-interface', '127.0.0.1', '-localhost', '-SecurityTypes', 'None',
      '-UseBlacklist', '0', '-FrameRate', '30', '-AlwaysShared', '-AcceptSetDesktopSize', '-ac', '-nolisten', 'tcp'], env, path.join(logs, 'desktop.log'));
    const signal = processes.controller.signal;
    await waitReady(() => fs.existsSync('/tmp/.X11-unix/X99') && tcpReady(5900), signal, 'desktop');
    processes.start('openbox', [], env, path.join(logs, 'window-manager.log'));
    const home = execFileSync('getent', ['passwd', config.browserUser], { encoding: 'utf8' }).trim().split(':')[5];
    if (!home) throw new Error('The coding-agent image is missing the browser user');
    processes.start('setpriv', ['--reuid=' + config.browserUser, '--regid=' + config.browserUser, '--init-groups',
      browserExecutable(), '--user-data-dir=' + config.profile, '--remote-debugging-address=127.0.0.1',
      '--remote-debugging-port=9222', '--no-first-run', '--no-default-browser-check', '--password-store=basic',
      '--disable-dev-shm-usage', '--start-maximized', '--restore-last-session'],
    { ...env, HOME: home }, path.join(logs, 'chromium.log'));
    await waitReady(async () => {
      const response = await fetch('http://127.0.0.1:9222/json/version', { signal: AbortSignal.any([signal, AbortSignal.timeout(1000)]) });
      return response.ok && typeof (await response.json()).webSocketDebuggerUrl === 'string';
    }, signal, 'Chromium CDP');
    signal.throwIfAborted();
    viewer = viewerServer(config);
    await new Promise((resolve, reject) => {
      viewer.server.once('error', reject);
      viewer.server.listen(config.port, config.host, resolve);
    });
    viewer.server.on('error', error => { processes.failure = error; stop(); });
    console.log(`Browser ready: viewer port ${config.port}, CDP http://127.0.0.1:9222, profile ${config.profile}`);
    if (!signal.aborted) await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }));
  } catch (error) {
    if (!processes.controller.signal.aborted || processes.failure) throw processes.failure || error;
  } finally {
    process.off('SIGTERM', stop);
    process.off('SIGINT', stop);
    if (viewer) await viewer.close();
    await processes.close();
    execFileSync('sync', ['-f', config.profile]);
  }
  if (processes.failure) throw processes.failure;
}

module.exports = { options, recoverProfile, profileIsRunning, originAllowed, viewerServer, Processes, waitReady, run };
if (require.main === module) {
  const args = process.argv.slice(2);
  if (args.length === 1 && args[0] === '--help') { process.stdout.write(HELP); }
  else if (args.length === 1 && args[0] === '--version') { console.log('sandbox0-browser 1'); }
  else {
    Promise.resolve().then(() => run(options(args))).catch(error => {
      console.error(`sandbox0-browser: ${error.message}`);
      process.exitCode = 1;
    });
  }
}
