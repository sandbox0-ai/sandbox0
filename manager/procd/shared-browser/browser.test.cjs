'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const net = require('node:net');
const { once } = require('node:events');
const ws = require('ws');
const { options, recoverProfile, profileIsRunning, originAllowed, viewerServer, Processes, waitReady } = require('./browser.cjs');

function temporary(context) {
  const directory = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'sandbox0-browser-test-')));
  context.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  return directory;
}

test('defaults keep viewer private and reject unsafe paths/ports', () => {
  const defaults = options([], {});
  assert.equal(defaults.host, '127.0.0.1');
  assert.equal(defaults.port, 6080);
  assert.equal(defaults.profile, '/workspace/.browser/profile');
  assert.equal(options([], { SANDBOX0_SERVICE_PORT: '6081' }).port, 6081);
  assert.equal(options(['--profile', '/workspace/.sandpi/browser/profile'], {}).profile, '/workspace/.sandpi/browser/profile');
  for (const args of [['--port', '9222'], ['--port', '5900'], ['--port', '0'], ['--port', '6080;id'],
    ['--profile', '/etc'], ['--profile', '/workspace/../etc/profile'], ['--size', '9999x9999']]) {
    assert.throws(() => options(args, {}));
  }
});

test('profile recovery protects active profiles and validates every lock before removing any', context => {
  const profile = temporary(context);
  const names = ['SingletonLock', 'SingletonCookie', 'SingletonSocket'];
  for (const name of names) fs.symlinkSync('previous-runtime-' + name, path.join(profile, name));
  assert.throws(() => recoverProfile(profile, () => true), /already in use/);
  assert.equal(fs.readlinkSync(path.join(profile, 'SingletonLock')), 'previous-runtime-SingletonLock');
  fs.unlinkSync(path.join(profile, 'SingletonCookie'));
  fs.writeFileSync(path.join(profile, 'SingletonCookie'), 'preserve this');
  assert.throws(() => recoverProfile(profile, () => false), /non-symlink/);
  assert.equal(fs.readlinkSync(path.join(profile, 'SingletonLock')), 'previous-runtime-SingletonLock');
  fs.unlinkSync(path.join(profile, 'SingletonCookie'));
  recoverProfile(profile, () => false);
  for (const name of names) assert.throws(() => fs.lstatSync(path.join(profile, name)), { code: 'ENOENT' });
  const link = path.join(profile, 'alias');
  fs.symlinkSync(profile, link);
  assert.throws(() => recoverProfile(link, () => false), /canonical/);
});

test('profile process detection tolerates PID reuse and supports both argument forms', context => {
  const proc = temporary(context);
  const directory = path.join(proc, '12');
  fs.mkdirSync(directory);
  const command = path.join(directory, 'cmdline');
  fs.writeFileSync(command, 'node\0unrelated\0');
  assert.equal(profileIsRunning('/workspace/.browser/profile', proc), false);
  fs.writeFileSync(command, 'chrome\0--user-data-dir=/workspace/.browser/profile\0');
  assert.equal(profileIsRunning('/workspace/.browser/profile', proc), true);
  fs.writeFileSync(command, 'chrome\0--user-data-dir\0/workspace/.browser/profile\0');
  assert.equal(profileIsRunning('/workspace/.browser/profile', proc), true);
});

test('viewer serves its assets and relays binary RFB through preview-rewritten hosts', async context => {
  const assets = temporary(context);
  fs.mkdirSync(path.join(assets, 'core'));
  fs.writeFileSync(path.join(assets, 'core', 'rfb.js'), 'export default class RFB {}');
  fs.symlinkSync(__filename, path.join(assets, 'core', 'escape.js'));
  const upstream = net.createServer(socket => {
    socket.write('RFB 003.008\n');
    socket.on('data', data => socket.write(data));
  });
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const viewer = viewerServer({ assets, vncPort: upstream.address().port }, ws);
  viewer.server.listen(0, '127.0.0.1');
  await once(viewer.server, 'listening');
  context.after(async () => { await viewer.close(); await new Promise(resolve => upstream.close(resolve)); });
  const base = `http://127.0.0.1:${viewer.server.address().port}`;
  const index = await fetch(base);
  assert.equal(index.status, 200);
  assert.match(await index.text(), /Shared Browser/);
  assert.match(index.headers.get('content-security-policy'), /default-src 'self'/);
  assert.equal((await fetch(base + '/viewer.js')).status, 200);
  assert.equal((await fetch(base + '/novnc/core/rfb.js')).status, 200);
  assert.equal((await fetch(base + '/novnc/core/escape.js')).status, 404);
  assert.equal((await fetch(base + '/browser.cjs')).status, 404);
  assert.equal((await fetch(base + '/novnc/core/%2e%2e%2frfb.js')).status, 400);
  assert.equal((await fetch(base, { method: 'POST' })).status, 405);
  assert.deepEqual(await (await fetch(base + '/healthz')).json(), { ready: true });

  const rejected = new ws.WebSocket(base.replace('http:', 'ws:') + '/vnc', { origin: 'https://foreign.example' });
  rejected.on('error', () => {});
  const [request, response] = await once(rejected, 'unexpected-response');
  assert.equal(response.statusCode, 403);
  response.destroy();
  rejected.terminate();

  const connection = new ws.WebSocket(base.replace('http:', 'ws:') + '/vnc', {
    origin: 'https://preview.example', headers: { 'X-Forwarded-Host': 'preview.example' },
  });
  context.after(() => connection.terminate());
  const [greeting, binary] = await once(connection, 'message');
  assert.equal(greeting.toString(), 'RFB 003.008\n');
  assert.equal(binary, true);
  const echoed = once(connection, 'message');
  connection.send(Buffer.from([0, 5, 255, 2]));
  assert.deepEqual((await echoed)[0], Buffer.from([0, 5, 255, 2]));
  const closed = once(connection, 'close');
  connection.send('not binary');
  await closed;
});

test('WebSocket Origin permits backend clients and rejects malformed browser origins', () => {
  assert(originAllowed({ headers: { host: 'localhost:6080' } }));
  assert(originAllowed({ headers: { host: 'localhost:6080', origin: 'https://preview.example', 'x-forwarded-host': 'preview.example' } }));
  assert(!originAllowed({ headers: { host: 'localhost:6080', origin: 'null' } }));
});

test('an essential child failure aborts startup and cleanup terminates the remaining process group', async context => {
  const directory = temporary(context);
  const processes = new Processes();
  context.after(() => processes.close());
  const active = processes.start(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], process.env, path.join(directory, 'active.log'));
  await once(active, 'spawn');
  const exited = once(active, 'exit');
  processes.start(process.execPath, ['-e', 'process.exit(7)'], process.env, path.join(directory, 'failed.log'));
  await once(processes.controller.signal, 'abort');
  assert.match(processes.failure.message, /exited \(7\)/);
  await processes.close();
  await exited;
  assert.throws(() => process.kill(active.pid, 0), { code: 'ESRCH' });
});

test('readiness waits for the actual probe and can be interrupted', async () => {
  const controller = new AbortController();
  let attempts = 0;
  await waitReady(async () => ++attempts === 2, controller.signal, 'CDP', 1000);
  assert.equal(attempts, 2);
  const waiting = waitReady(() => false, controller.signal, 'CDP', 1000);
  controller.abort();
  await assert.rejects(waiting);
});
