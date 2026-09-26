import assert from 'node:assert/strict';
import { copyFileSync, mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { spawnSync } from 'node:child_process';
import test from 'node:test';
import { claudeModels } from './inventory.mjs';

test('extracts the exact image CLI versions and paginated native models without inherited auth', t => {
  const directory = mkdtempSync(join(tmpdir(), 'coding-agent-inventory-test-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  copyFileSync(new URL('inventory.mjs', import.meta.url), join(directory, 'inventory.mjs'));
  const versionScript = `#!${process.execPath}\nconsole.log('1.2.3');\n`;
  writeFileSync(join(directory, 'kimi'), `#!${process.execPath}
const fs = require('node:fs');
if (process.env.KIMI_MODEL_NAME || process.env.KIMI_MODEL_API_KEY || !process.env.KIMI_CODE_HOME.startsWith(process.env.HOME)) process.exit(1);
if (process.argv.includes('--version')) { console.log('1.2.3'); process.exit(); }
const file = require('node:path').join(process.env.HOME, 'native-config.json');
if (process.argv.includes('add')) {
  (async () => {
    try { await fetch('https://models.dev/api.json'); process.exit(2); }
    catch (e) { if (!e.message.includes('Offline model inventory')) process.exit(3); }
    fs.writeFileSync(file, JSON.stringify({providers:{moonshotai:{type:'openai',baseUrl:'https://api.moonshot.ai/v1'}},
      models: process.env.FAKE_KIMI_EMPTY ? {} : {native:{provider:'moonshotai',model:'kimi-native',capabilities:['tool_use']},
        chat:{provider:'moonshotai',model:'chat-only',capabilities:[]}}}));
    console.log('Imported native catalog.');
  })();
} else if (process.argv.includes('list')) console.log(fs.readFileSync(file,'utf8'));
else process.exit(4);
`, { mode: 0o755 });
  const zcodePackage = join(directory, 'node_modules', '@test', 'zcode');
  const zcodeNative = join(zcodePackage, 'node_modules', '@zcode');
  mkdirSync(join(zcodePackage, 'dist/provider'), { recursive: true });
  mkdirSync(join(zcodeNative, 'provider-node/dist'), { recursive: true });
  mkdirSync(join(zcodeNative, 'provider/src'), { recursive: true });
  writeFileSync(join(zcodePackage, 'package.json'), '{}');
  writeFileSync(join(zcodePackage, 'dist/zcode.cjs'), versionScript, { mode: 0o755 });
  symlinkSync(join(zcodePackage, 'dist/zcode.cjs'), join(directory, 'zcode'));
  writeFileSync(join(zcodePackage, 'dist/provider/zcode-builtin.json'), JSON.stringify({schemaVersion:1,revision:1}));
  writeFileSync(join(zcodeNative, 'provider-node/package.json'), JSON.stringify({type:'module'}));
  writeFileSync(join(zcodeNative, 'provider/package.json'), JSON.stringify({type:'module',exports:'./src/index.ts'}));
  writeFileSync(join(zcodeNative, 'provider-node/dist/zcode-builtin-release.js'),
    `export { decodeZCodeBuiltinRelease } from '@zcode/provider';`);
  // Exercise source-only pnpm exports, Node type transformation and .js -> .ts
  // sibling resolution, as well as the installed rule engine's exclusions.
  writeFileSync(join(zcodeNative, 'provider/src/index.ts'), `export { decodeZCodeBuiltinRelease } from './release.js';`);
  writeFileSync(join(zcodeNative, 'provider/src/release.ts'), `
export function decodeZCodeBuiltinRelease(raw: {schemaVersion:number}) {
  if (raw.schemaVersion !== 1) throw new Error('Invalid native release');
  return {config:{providerTemplates:{toJSON:()=>[{templateId:'zai-standard-api'}],get:()=>({config:{
    api:{type:'openai-chat-completions',baseUrl:'https://api.z.ai/api/paas/v4'},
    builtinModelIds:['GLM-Native','disabled','chat-only','invalid']}})},
    modelConfigRules:{resolve:({modelId}:{modelId:string})=>({enabled:modelId!=='disabled',
      properties:{supportsToolCall:modelId!=='chat-only'},validateComplete:()=>modelId==='invalid'?['invalid']:[]})}}};
}`);
  writeFileSync(join(directory, 'claude'), `#!${process.execPath}
if (process.env.ANTHROPIC_MODEL || process.env.ANTHROPIC_DEFAULT_OPUS_MODEL || process.env.ANTHROPIC_API_KEY ||
    process.env.CLAUDE_CODE_USE_BEDROCK || !process.env.CLAUDE_CONFIG_DIR?.startsWith(process.env.HOME)) process.exit(1);
if (process.argv.includes('--version')) { console.log('1.2.3'); process.exit(); }
require('node:readline').createInterface({ input:process.stdin }).on('line', line => {
  const m = JSON.parse(line);
  if (m.type !== 'control_request' || m.request.subtype !== 'initialize') process.exit(2);
  // A discovery collector must never send a user prompt or perform inference.
  console.log(JSON.stringify({type:'control_response',response:{subtype:'success',request_id:m.request_id,
    response:{account:{apiProvider:'firstParty'},models:[
      {value:'default',resolvedModel:'claude-opus-example[1m]'},
      {value:'opus[1m]',resolvedModel:'claude-opus-example[1m]'},
      {value:'sonnet',resolvedModel:'claude-sonnet-example'},
      {value:'haiku',resolvedModel:'claude-haiku-example-20251001'}
    ]}}}));
});

`, { mode: 0o755 });
  const piPackage = join(directory, 'node_modules', '@test', 'pi-coding-agent');
  const registry = join(piPackage, 'node_modules', '@test', 'pi-ai');
  mkdirSync(join(registry, 'dist/providers'), { recursive: true });
  writeFileSync(join(piPackage, 'package.json'), JSON.stringify({ dependencies: { '@test/pi-ai': '1.2.3' } }));
  writeFileSync(join(piPackage, 'cli.cjs'), versionScript, { mode: 0o755 });
  symlinkSync(join(piPackage, 'cli.cjs'), join(directory, 'pi'));
  writeFileSync(join(registry, 'package.json'), JSON.stringify({ type: 'module', exports: { '.': { import: './dist/index.js' } } }));
  writeFileSync(join(registry, 'dist/index.js'), 'export const newApi = true;');
  writeFileSync(join(registry, 'dist/providers/all.js'), `export const getBuiltinProviders = () => ['openai'];
export const getBuiltinModels = () => [{ id:'pi-only-model', provider:'openai', api:'openai-responses' }];`);
  writeFileSync(join(directory, 'codex'), `#!${process.execPath}
const fs = require('node:fs');
if (process.env.OPENAI_API_KEY || !fs.existsSync(process.env.CODEX_HOME)) process.exit(1);
if (process.argv.includes('--version')) { console.log('codex-cli 1.2.3'); process.exit(); }
require('node:readline').createInterface({ input:process.stdin }).on('line', line => {
  const m = JSON.parse(line);
  if (m.method === 'initialize') console.log(JSON.stringify({id:m.id,result:{}}));
  if (m.method === 'model/list') {
    if (!m.params.includeHidden) process.exit(2);
    console.log(JSON.stringify({id:m.id,result: m.params.cursor ?
      {data:[{model:'hidden-model',isHidden:true}],nextCursor:null} :
      {data:[{model:'visible-model',hidden:false}],nextCursor:'page-2'}}));
  }
});
`, { mode: 0o755 });
  const result = spawnSync(process.execPath, [join(directory, 'inventory.mjs')], {
    env: { ...process.env, PATH: `${directory}:${process.env.PATH}`, OPENAI_API_KEY: 'dummy-must-not-be-inherited',
      ANTHROPIC_API_KEY: 'dummy-must-not-be-inherited', ANTHROPIC_MODEL: 'custom-override',
      ANTHROPIC_DEFAULT_OPUS_MODEL: 'wrong-model', CLAUDE_CODE_USE_BEDROCK: '1',
      KIMI_MODEL_NAME:'wrong-model', KIMI_MODEL_API_KEY:'dummy-must-not-be-inherited', KIMI_CODE_HOME:'/wrong/home' },
    encoding: 'utf8', timeout: 10_000,
  });
  assert.equal(result.status, 0, result.stderr);
  const inventory = JSON.parse(result.stdout);
  assert.equal(inventory.schemaVersion, 3);
  assert.equal(inventory.harnesses.codex.version, 'codex-cli 1.2.3');
  assert.deepEqual(inventory.harnesses.codex.nativeModels, [{ id:'visible-model',hidden:false }, { id:'hidden-model',hidden:true }]);
  assert.equal(Object.keys(inventory.harnesses).length, 5);
  assert.equal(inventory.harnesses.pi.discovery, 'installed-pi-ai/provider-model-registry');
  assert.deepEqual(inventory.harnesses.pi.nativeModels, [{ id:'pi-only-model', provider:'openai', api:'openai-responses', hidden:false }]);
  assert.equal(inventory.harnesses['claude-code'].discovery, 'sdk/initialize/models/resolvedModel');
  assert.deepEqual(inventory.harnesses['claude-code'].nativeModels, [
    { id:'claude-haiku-example-20251001', hidden:false, resolvedModel:'claude-haiku-example-20251001', aliases:['haiku'] },
    { id:'claude-opus-example', hidden:false, resolvedModel:'claude-opus-example[1m]', aliases:['default','opus[1m]'] },
    { id:'claude-sonnet-example', hidden:false, resolvedModel:'claude-sonnet-example', aliases:['sonnet'] },
  ]);
  assert.deepEqual(inventory.harnesses['kimi-code'].nativeModels, [{id:'kimi-native',provider:'moonshotai',
    api:'openai',baseUrl:'https://api.moonshot.ai/v1',hidden:false}]);
  assert.deepEqual(inventory.harnesses.zcode.nativeModels, [{id:'GLM-Native',provider:'zai-standard-api',
    api:'openai-chat-completions',baseUrl:'https://api.z.ai/api/paas/v4',hidden:false}]);
  const failed = spawnSync(process.execPath, [join(directory, 'inventory.mjs')], {
    env:{...process.env,PATH:`${directory}:${process.env.PATH}`,FAKE_KIMI_EMPTY:'1'},encoding:'utf8',timeout:10_000,
  });
  assert.notEqual(failed.status, 0);
  assert.match(failed.stderr, /Empty native Kimi tool model catalog/);
  assert.equal(failed.stdout, '', 'an incomplete receipt must never be published');
});

test('rejects unresolvable Claude aliases and non-first-party or empty catalogs', async t => {
  const directory = mkdtempSync(join(tmpdir(), 'claude-inventory-invalid-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  writeFileSync(join(directory, 'claude'), `#!${process.execPath}
require('node:readline').createInterface({ input:process.stdin }).on('line', line => {
  const request = JSON.parse(line);
  console.log(JSON.stringify({type:'control_response',response:{subtype:'success',request_id:request.request_id,
    response:JSON.parse(process.env.FAKE_CATALOG)}}));
});
`, { mode: 0o755 });
  for (const catalog of [
    { account:{apiProvider:'firstParty'}, models:[{value:'sonnet'}] },
    { account:{apiProvider:'bedrock'}, models:[{value:'sonnet',resolvedModel:'claude-sonnet-example'}] },
    { account:{apiProvider:'firstParty'}, models:[] },
  ]) {
    await assert.rejects(claudeModels({ ...process.env, PATH:`${directory}:${process.env.PATH}`,
      FAKE_CATALOG:JSON.stringify(catalog) }), /Invalid first-party|unresolved model alias/);
  }
});
