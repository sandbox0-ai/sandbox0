import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, copyFileSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

function fixture(t, version = '9.8.7') {
  const directory = mkdtempSync(join(tmpdir(), 'coding-agent-update-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  for (const name of ['update.mjs', 'package.json', 'THIRD_PARTY.md']) {
    copyFileSync(new URL(name, import.meta.url), join(directory, name));
  }
  writeFileSync(join(directory, 'npm'), `#!${process.execPath}
import fs from 'node:fs';
if (process.argv[2] === 'view') {
  console.log(JSON.stringify(${JSON.stringify(version)}));
} else {
  fs.writeFileSync('install-args.json', JSON.stringify(process.argv.slice(2)));
}
`, { mode: 0o755 });
  // The fake npm is an ES module without changing the package manifest under test.
  let fake = readFileSync(join(directory, 'npm'), 'utf8');
  fake = fake.replace("import fs from 'node:fs';", "const fs = require('node:fs');");
  writeFileSync(join(directory, 'npm'), fake);
  return { directory, run: () => spawnSync(process.execPath, [join(directory, 'update.mjs')], {
    env: { ...process.env, PATH: `${directory}:${process.env.PATH}` }, encoding: 'utf8',
  }) };
}

test('updates every exact pin and license inventory and refreshes lock without scripts', t => {
  const { directory, run } = fixture(t);
  const result = run();
  assert.equal(result.status, 0, result.stderr);
  const manifest = JSON.parse(readFileSync(join(directory, 'package.json'), 'utf8'));
  const inventory = readFileSync(join(directory, 'THIRD_PARTY.md'), 'utf8');
  for (const [name, version] of Object.entries(manifest.dependencies)) {
    assert.equal(version, '9.8.7');
    assert.ok(inventory.includes(`| \`${name}\` | \`9.8.7\` |`));
  }
  assert.ok(inventory.includes('| `ttyd` | `1.7.7` | MIT |'));
  const args = JSON.parse(readFileSync(join(directory, 'install-args.json'), 'utf8'));
  assert.ok(args.includes('--package-lock-only'));
  assert.ok(args.includes('--ignore-scripts'));
});

test('rejects prerelease latest versions without modifying package pins', t => {
  const { directory, run } = fixture(t, '9.0.0-beta.1');
  const before = readFileSync(join(directory, 'package.json'), 'utf8');
  assert.notEqual(run().status, 0);
  assert.equal(readFileSync(join(directory, 'package.json'), 'utf8'), before);
});
