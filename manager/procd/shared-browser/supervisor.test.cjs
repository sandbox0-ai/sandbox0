'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { setTimeout: delay } = require('node:timers/promises');
const { BrowserSupervisor } = require('./supervisor.cjs');

function fixture(context) {
  const launches = [], commands = [], children = [];
  const supervisor = new BrowserSupervisor('chrome', '/workspace/profile with spaces', '/tmp/desktop', {
    spawn(_command, args) {
      launches.push(args);
      const child = Object.assign(new EventEmitter(), { pid: 100 + launches.length, exitCode: null, signalCode: null,
        kill(signal) { this.signalCode = signal; this.emit('exit', null, signal); } });
      children.push(child);
      return child;
    },
    execute: async (command, args) => { commands.push([command, args]); return { stdout: command === 'wmctrl' && args[0] === '-lp' ? `0x01 0 ${supervisor.child.pid} host Browser` : '' }; },
    probe: async () => true,
    onFailure(error) { throw error; },
  });
  context.after(() => supervisor.close());
  return { supervisor, launches, commands, children };
}

test('closing the last browser keeps the supervisor and restarts CDP without a window', async context => {
  const { supervisor, launches, children } = fixture(context);
  await supervisor.ensure(true);
  assert(launches[0].includes('--restore-last-session'));
  children[0].exitCode = 0;
  children[0].emit('exit', 0, null);
  await delay(600);
  assert.equal(launches.length, 2);
  assert(launches[1].includes('--no-startup-window'));
  assert(!supervisor.controller.signal.aborted);
  await supervisor.close();
  await delay(600);
  assert.equal(launches.length, 2);
});

test('launcher restores the owned minimized window and serializes concurrent opens', async context => {
  const { supervisor, launches, commands } = fixture(context);
  await supervisor.ensure(true);
  await Promise.all([supervisor.open(), supervisor.open(), supervisor.open()]);
  assert.equal(launches.length, 1);
  assert.deepEqual(commands, [['wmctrl', ['-lp']], ['wmctrl', ['-ia', '0x01']]]);
  supervisor.execute = async (command, args) => { commands.push([command, args]); return { stdout: '' }; };
  await supervisor.open();
  assert.equal(launches.length, 1);
  assert(commands.at(-1)[1].includes('--new-window'));
  assert(commands.at(-1)[1].includes('--user-data-dir=/workspace/profile with spaces'));
});

test('launcher control rejects browser origins, foreign hosts and wrong methods', async context => {
  const { supervisor, commands } = fixture(context);
  // Choose a free fixed port to exercise the production Host check unchanged.
  const net = require('node:net');
  const temporary = net.createServer();
  await new Promise(resolve => temporary.listen(0, '127.0.0.1', resolve));
  const port = temporary.address().port;
  await new Promise(resolve => temporary.close(resolve));
  await supervisor.listen(port);
  const base = `http://127.0.0.1:${port}`;
  assert.equal((await fetch(base + '/open', { method: 'POST', headers: { Origin: 'https://foreign.example' } })).status, 403);
  const foreignHostStatus = await new Promise((resolve, reject) => {
    const request = require('node:http').request(base + '/open', { method: 'POST', headers: { Host: 'foreign.example' } }, response => {
      response.resume(); resolve(response.statusCode);
    });
    request.once('error', reject); request.end();
  });
  assert.equal(foreignHostStatus, 403);
  assert.equal((await fetch(base + '/open')).status, 404);
  assert.equal((await fetch(base + '/open', { method: 'POST' })).status, 204);
  assert.equal(commands.filter(([cmd, args]) => cmd === 'wmctrl' && args[0] === '-ia').length, 1);
});
