import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const directory = fileURLToPath(new URL('.', import.meta.url));
const manifest = JSON.parse(readFileSync(`${directory}/package.json`, 'utf8'));
// Resolve every direct dependency before changing files. Only stable npm latest
// versions are accepted; npm ci in both image builds verifies the resulting lock.
for (const name of Object.keys(manifest.dependencies)) {
  const version = JSON.parse(execFileSync('npm', ['view', `${name}@latest`, 'version', '--json'], {
    cwd: directory, encoding: 'utf8', timeout: 120_000,
  }));
  if (typeof version !== 'string' || !/^\d+\.\d+\.\d+$/.test(version)) {
    throw new Error(`Expected a stable exact version for ${name}: ${version}`);
  }
  manifest.dependencies[name] = version;
}
writeFileSync(`${directory}/package.json`, `${JSON.stringify(manifest, null, 2)}\n`);
execFileSync('npm', ['install', '--package-lock-only', '--ignore-scripts', '--no-audit', '--no-fund'], {
  cwd: directory, stdio: 'inherit', timeout: 300_000,
});
let inventory = readFileSync(`${directory}/THIRD_PARTY.md`, 'utf8');
for (const [name, version] of Object.entries(manifest.dependencies)) {
  const prefix = `| \`${name}\` | `;
  if (!inventory.split('\n').some(line => line.startsWith(prefix))) {
    throw new Error(`Missing third-party inventory entry for ${name}`);
  }
  inventory = inventory.split('\n').map(line => line.startsWith(prefix)
    ? `${prefix}\`${version}\` |${line.split('|').slice(3).join('|')}` : line).join('\n');
}
writeFileSync(`${directory}/THIRD_PARTY.md`, inventory);
