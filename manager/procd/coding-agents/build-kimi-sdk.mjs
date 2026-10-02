// Build the upstream in-process SDK alongside its CLI, from the same source.
// The native loop exports let hosts fence steering to an existing prompt.
import { build } from 'tsdown';
import { writeFile, mkdir, readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const source = process.cwd();
const { rawTextPlugin } = await import(pathToFileURL(resolve(source, 'build/raw-text-plugin.mjs')));
const cli = JSON.parse(await readFile('apps/kimi-code/package.json', 'utf8'));
await mkdir('sdk-host', { recursive: true });
await writeFile('sdk-host/index.ts', [
  "export { KimiHarness, createKimiHarness, SDKRpcClientV2 } from '../packages/node-sdk/src/index';",
  "export { IAgentLoopService } from '../packages/agent-core-v2/src/agent/loop/loop';",
  "export { IEventBus } from '../packages/agent-core-v2/src/app/event/eventBus';",
  `export const cliVersion = ${JSON.stringify(cli.version)};`,
].join('\n'));
await build({
  config: false, entry: [resolve(source, 'sdk-host/index.ts')], format: ['esm'],
  outDir: resolve(source, 'sdk-host/dist'), clean: true, dts: false, hash: false,
  plugins: [rawTextPlugin()], deps: { onlyBundle: false },
  banner: { js: [
    "import { fileURLToPath as __sdkFileURLToPath } from 'node:url';",
    "import { dirname as __sdkDirname } from 'node:path';",
    'const __filename = __sdkFileURLToPath(import.meta.url);',
    'const __dirname = __sdkDirname(__filename);',
  ].join('\n') },
  outputOptions: { codeSplitting: false, entryFileNames: 'index.mjs' },
});
