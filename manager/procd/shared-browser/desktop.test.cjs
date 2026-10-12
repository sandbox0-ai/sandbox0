'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { workArea, windowRows, fittedGeometry, DesktopMaintenance } = require('./desktop.cjs');

test('normal windows fit the current work area including their native frame', () => {
  const area = workArea('0 * DG: 640x480 VP: 0,0 WA: 0,0 640x428 N/A');
  assert.deepEqual(area, [0, 0, 640, 428]);
  const window = { x: 81, y: 80, width: 900, height: 600 };
  const frame = '_NET_FRAME_EXTENTS(CARDINAL) = 1, 1, 20, 1';
  assert.equal(fittedGeometry(window, area, frame), '0,64,41,574,366');
  assert.equal(fittedGeometry({ x: 1, y: 20, width: 500, height: 300 }, area, frame), null);
  assert.equal(fittedGeometry({ x: -100, y: -100, width: 500, height: 300 }, area, frame), '0,0,0,500,300');
  for (const state of ['FULLSCREEN', 'HIDDEN', 'MAXIMIZED_HORZ', 'MAXIMIZED_VERT']) {
    assert.equal(fittedGeometry(window, area, `${frame}\n_NET_WM_STATE = _NET_WM_STATE_${state}`), null);
  }
  assert.equal(workArea('0 * DG: 640x480 VP: 0,0 WA: 0,0 0x0'), null);
});

test('window ownership ignores other applications and hidden desktop rows', () => {
  const text = '0x01 0 11 10 20 800 600 host Browser\n0x02 0 12 0 0 640 480 host Other\n0x03 -1 11 0 0 640 480 host Hidden';
  assert.deepEqual(windowRows(text, 11), [{ id: '0x01', x: 10, y: 20, width: 800, height: 600 }]);
});

test('resize maintenance refreshes wallpaper once per size and fences changed browser ownership', async () => {
  let pid = 11, size = '640x480';
  const calls = [];
  const controller = new AbortController();
  const maintenance = new DesktopMaintenance('/tmp/desktop', () => pid, async (command, args) => {
    calls.push([command, args]);
    if (command === 'wmctrl' && args[0] === '-d') return { stdout: `0 * DG: ${size} VP: 0,0 WA: 0,0 640x428` };
    if (command === 'wmctrl' && args[0] === '-lpG') return { stdout: '0x01 0 11 81 80 900 600 host Browser' };
    if (command === 'xprop') { pid = 12; return { stdout: '_NET_FRAME_EXTENTS = 1, 1, 20, 1' }; }
    return { stdout: '' };
  });
  await maintenance.refresh(controller.signal);
  await maintenance.refresh(controller.signal);
  assert.equal(calls.filter(([cmd]) => cmd === 'feh').length, 1);
  assert.equal(calls.filter(([cmd, args]) => cmd === 'wmctrl' && args[0] === '-ir').length, 0);
  size = '800x600';
  await maintenance.refresh(controller.signal);
  assert.equal(calls.filter(([cmd]) => cmd === 'feh').length, 2);
});
