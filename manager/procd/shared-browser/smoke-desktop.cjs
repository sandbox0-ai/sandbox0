#!/usr/bin/env node
'use strict';
// Run only in a disposable Linux coding-agent sandbox. Owns display :99.
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const { spawn, execFile } = require('node:child_process');
const { promisify } = require('node:util');
const { setTimeout: delay } = require('node:timers/promises');
const { chromium } = require('/opt/coding-agents/node_modules/playwright-core');
const execute = promisify(execFile);
const directory = process.env.DESKTOP_SMOKE_OUTPUT || '/workspace/browser-desktop-smoke';
const profile = '/workspace/browser-desktop-smoke-profile';
const native = (command, args) => execute(command, args, { env: { ...process.env, DISPLAY: ':99' }, timeout: 10000 });
async function eventually(probe, description, timeout = 30000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    try { if (await probe()) return; } catch {}
    await delay(100);
  }
  throw new Error(`Timed out: ${description}`);
}
async function windows() {
  return (await native('wmctrl', ['-lx'])).stdout.trim().split('\n')
    .filter(line => /google-chrome|chromium/i.test(line)).map(line => line.split(/\s+/)[0]);
}
async function state(id, value) { return (await native('xprop', ['-id', id, '_NET_WM_STATE'])).stdout.includes(value); }
async function click(x, y) { await native('xdotool', ['mousemove', String(x), String(y), 'click', '1']); }
async function area(width, height) {
  await eventually(async () => (await native('xprop', ['-root', '_NET_WORKAREA'])).stdout.includes(`${width}, ${height - 52}`), 'panel work area');
}
async function fits(id, width, height) {
  const text = (await native('xdotool', ['getwindowgeometry', '--shell', id])).stdout;
  const values = Object.fromEntries(text.trim().split('\n').filter(line => line.includes('=')).map(line => {
    const [key, value] = line.split('='); return [key, Number(value)];
  }));
  return values.X >= 0 && values.Y >= 0 && values.X + values.WIDTH <= width && values.Y + values.HEIGHT <= height - 52;
}

async function main() {
  await fs.mkdir(directory, { recursive: true });
  const runtime = spawn('/usr/local/bin/sandbox0-browser', ['--port', '6080', '--profile', profile, '--require-version', '2'], { stdio: 'inherit' });
  let automation, viewer;
  try {
    await eventually(async () => (await fetch('http://127.0.0.1:6080/healthz')).ok, 'runtime readiness');
    await area(1440, 900);
    automation = await chromium.connectOverCDP('http://127.0.0.1:9222');
    await automation.contexts()[0].addCookies([{ name: 'runtime-persistence', value: 'preserved', url: 'https://example.test', expires: Date.now() / 1000 + 3600 }]);
    await eventually(async () => (await windows()).length === 1, 'initial window');
    let [id] = await windows();
    await native('scrot', ['--overwrite', `${directory}/desktop.png`]);
    await native('xdotool', ['windowminimize', id]);
    await eventually(() => state(id, '_NET_WM_STATE_HIDDEN'), 'minimize');
    await click(130, 874);
    await eventually(async () => !await state(id, '_NET_WM_STATE_HIDDEN'), 'task button restore');
    assert.deepEqual(await windows(), [id]);
    await native('xdotool', ['windowminimize', id]);
    await click(30, 874);
    await eventually(async () => !await state(id, '_NET_WM_STATE_HIDDEN'), 'launcher restore');
    assert.deepEqual(await windows(), [id]);
    await native('wmctrl', ['-ic', id]);
    await eventually(async () => (await windows()).length === 0, 'last window close');
    await eventually(async () => (await fetch('http://127.0.0.1:9222/json/version')).ok, 'CDP recovery');
    assert.equal(runtime.exitCode, null);
    assert.equal((await fetch('http://127.0.0.1:6080/healthz')).status, 200);
    assert.deepEqual(await windows(), []);
    await native('scrot', ['--overwrite', `${directory}/empty-desktop.png`]);
    await automation.close();
    await click(30, 874);
    await eventually(async () => (await windows()).length === 1, 'launcher opens after last close');
    automation = await chromium.connectOverCDP('http://127.0.0.1:9222');
    assert.equal((await automation.contexts()[0].cookies('https://example.test')).find(cookie => cookie.name === 'runtime-persistence')?.value, 'preserved');
    [id] = await windows();
    const cdp = await automation.newBrowserCDPSession();
    await cdp.send('Target.createTarget', { url: 'about:blank', newWindow: true });
    await eventually(async () => (await windows()).length === 2, 'multiple windows');
    await native('wmctrl', ['-ic', (await windows()).find(window => window !== id)]);
    await eventually(async () => (await windows()).length === 1, 'close second window');
    await native('wmctrl', ['-ia', id]);
    await native('xdotool', ['key', 'F11']);
    await eventually(() => state(id, '_NET_WM_STATE_FULLSCREEN'), 'F11 fullscreen');
    await native('xdotool', ['key', 'F11']);
    await eventually(async () => !await state(id, '_NET_WM_STATE_FULLSCREEN'), 'exit fullscreen');
    await native('wmctrl', ['-ir', id, '-b', 'remove,maximized_horz,maximized_vert']);
    await native('wmctrl', ['-ir', id, '-e', '0,100,60,900,600']);
    await delay(500);
    await native('wmctrl', ['-ir', id, '-b', 'add,maximized_horz,maximized_vert']);
    await native('xrandr', ['--output', 'VNC-0', '--mode', '640x480']);
    await area(640, 480);
    await eventually(() => fits(id, 640, 480), 'maximize within reduced work area');
    await native('wmctrl', ['-ir', id, '-b', 'remove,maximized_horz,maximized_vert']);
    await eventually(() => fits(id, 640, 480), 'restore within reduced work area');
    await native('scrot', ['--overwrite', `${directory}/restored.png`]);

    // The bundled noVNC client must resize, scale, and keep a live connection
    // through browser window closure. This catches transport and resize regressions.
    viewer = await chromium.launch({ executablePath: '/usr/bin/google-chrome-stable', headless: true, args: ['--no-sandbox'] });
    const page = await viewer.newPage({ viewport: { width: 320, height: 280 } });
    await page.goto('http://127.0.0.1:6080');
    await page.waitForFunction(() => document.getElementById('status').textContent.includes('share this browser'));
    await eventually(async () => {
      const dimensions = (await native('wmctrl', ['-d'])).stdout.match(/DG:\s+(\d+)x(\d+)/);
      return dimensions && Number(dimensions[1]) >= 640 && Number(dimensions[2]) >= 480;
    }, 'viewer minimum desktop');
    await eventually(() => page.locator('canvas').evaluate(canvas => {
      const rect = canvas.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0 && rect.left >= 0 && rect.top >= 0 && rect.right <= innerWidth + 1 && rect.bottom <= innerHeight + 1;
    }), 'scaled canvas fits viewer');
    await page.screenshot({ path: `${directory}/small-viewer.png` });
    await native('wmctrl', ['-ic', id]);
    await eventually(async () => (await windows()).length === 0, 'close with live viewer');
    await delay(800);
    assert.match(await page.locator('#status').innerText(), /share this browser/);
    console.log('PASS: taskbar/launcher restore, last-close recovery, persistent cookies, multiple windows, F11, maximize/restore after shrink, live noVNC scaling');
  } finally {
    await viewer?.close();
    await automation?.close().catch(() => {});
    runtime.kill('SIGTERM');
    await eventually(() => runtime.exitCode !== null || runtime.signalCode !== null, 'service shutdown');
    await eventually(async () => {
      try { await fetch('http://127.0.0.1:9222/json/version', { signal: AbortSignal.timeout(500) }); return false; }
      catch { return true; }
    }, 'CDP shutdown');
    await fs.rm(profile, { recursive: true, force: true });
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
