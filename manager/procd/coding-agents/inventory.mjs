import { execFileSync, spawn } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { createRequire, registerHooks, stripTypeScriptTypes } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

export const harnessBinaries = { codex: 'codex', 'claude-code': 'claude', pi: 'pi', 'kimi-code': 'kimi', zcode: 'zcode' };

function installedPackage(binary, env) {
  const executable = env.PATH.split(':').map(directory => join(directory, binary)).find(existsSync);
  if (!executable) throw new Error(`${binary} binary is missing`);
  const entry = realpathSync(executable);
  let root = dirname(entry);
  while (!existsSync(join(root, 'package.json'))) {
    const parent = dirname(root);
    if (parent === root) throw new Error(`${binary} package is missing`);
    root = parent;
  }
  return { entry, root };
}

/** Import the CLI's release snapshot through its own protocol/model validation. */
export async function kimiModels(env) {
  const home = mkdtempSync(join(tmpdir(), 'kimi-model-inventory-'));
  try {
    const offline = join(home, 'offline.mjs');
    // Trigger the CLI's built-in fallback even during network-enabled builds.
    // This makes the receipt reproducible and ties it to the installed release.
    writeFileSync(offline, 'globalThis.fetch = async () => { throw new Error("Offline model inventory"); };');
    const isolated = { ...env, HOME: home, KIMI_CODE_HOME: join(home, 'kimi'),
      NODE_OPTIONS: `--import=${pathToFileURL(offline).href}` };
    const run = args => execFileSync('kimi', args, { env: isolated, encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'], timeout: 30_000, maxBuffer: 4_000_000 });
    // Importing configuration performs no inference. The inert placeholder is
    // needed by the native importer and is deleted with the temporary home.
    const imported = run(['provider', 'catalog', 'add', 'moonshotai', '--api-key', 'inventory-not-a-credential']);
    if (/guessed/i.test(imported)) throw new Error('Kimi catalog has no declared protocol');
    const config = JSON.parse(run(['provider', 'list', '--json']));
    const provider = config.providers?.moonshotai;
    if (provider?.type !== 'openai' || provider.baseUrl !== 'https://api.moonshot.ai/v1' ||
        !config.models || typeof config.models !== 'object') throw new Error('Invalid native Kimi provider catalog');
    const models = Object.values(config.models).filter(model => model.provider === 'moonshotai' &&
      model.capabilities?.includes('tool_use')).map(model => {
      if (typeof model.model !== 'string' || !model.model) throw new Error('Invalid Kimi model ID');
      return { id: model.model, provider: 'moonshotai', api: provider.type, baseUrl: provider.baseUrl, hidden: false };
    });
    if (!models.length) throw new Error('Empty native Kimi tool model catalog');
    return models.sort((a, b) => a.id.localeCompare(b.id));
  } finally { rmSync(home, { recursive: true, force: true }); }
}

/** Read the installed ZCode release with its own decoder and model rule resolver. */
export async function zcodeModels(env) {
  const { entry, root } = installedPackage('zcode', env);
  const dependency = '@zcode/provider-node';
  const dependencyRoot = createRequire(entry).resolve.paths(dependency).map(path => join(path, dependency))
    .find(path => existsSync(join(path, 'package.json')));
  if (!dependencyRoot) throw new Error('Installed ZCode provider registry is missing');
  // pnpm deployment retains some workspace exports pointing at TypeScript.
  // Prefer compiled modules; otherwise transform the installed source using
  // the image's Node 24 runtime, preserving normal dependency resolution.
  const hook = registerHooks({
    resolve(specifier, context, nextResolve) {
      if (specifier.startsWith('.') && specifier.endsWith('.js') &&
          context.parentURL?.includes('/node_modules/@zcode/') && context.parentURL.includes('/src/')) {
        const url = new URL(specifier.replace(/\.js$/, '.ts'), context.parentURL).href;
        if (existsSync(fileURLToPath(url))) return { url, format: 'module', shortCircuit: true };
      }
      const result = nextResolve(specifier, context);
      if (result.url.startsWith('file:') && result.url.includes('/node_modules/@zcode/') &&
          result.url.includes('/src/') && result.url.endsWith('.ts')) {
        const url = result.url.replace('/src/', '/dist/').replace(/\.ts$/, '.js');
        if (existsSync(fileURLToPath(url))) return { ...result, url, format: 'module' };
      }
      return result;
    },
    load(url, context, nextLoad) {
      if (url.startsWith('file:') && url.includes('/node_modules/@zcode/') && url.endsWith('.ts')) {
        return { format: 'module', shortCircuit: true, source: stripTypeScriptTypes(
          readFileSync(fileURLToPath(url), 'utf8'), { mode: 'transform', sourceUrl: url }) };
      }
      return nextLoad(url, context);
    },
  });
  try {
    const api = await import(pathToFileURL(join(dependencyRoot, 'dist/zcode-builtin-release.js')).href);
    const release = api.decodeZCodeBuiltinRelease(JSON.parse(readFileSync(join(root, 'dist/provider/zcode-builtin.json'), 'utf8')));
    const models = [];
    for (const { templateId } of release.config.providerTemplates.toJSON()) {
      const provider = release.config.providerTemplates.get(templateId).config;
      for (const id of provider.builtinModelIds ?? []) {
        const model = release.config.modelConfigRules.resolve({ providerId: `inventory:${templateId}`,
          templateId, modelId: id, apiType: provider.api?.type, baseUrl: provider.api?.baseUrl });
        if (!model.enabled || !model.properties?.supportsToolCall || model.validateComplete().length) continue;
        if (typeof id !== 'string' || !id || !provider.api?.type || !provider.api?.baseUrl) {
          throw new Error('Invalid native ZCode model catalog');
        }
        models.push({ id, provider: templateId, api: provider.api.type, baseUrl: provider.api.baseUrl,
          hidden: provider.visibility === 'hidden' });
      }
    }
    if (!models.length) throw new Error('Empty native ZCode tool model catalog');
    return models.sort((a, b) => `${a.provider}/${a.id}`.localeCompare(`${b.provider}/${b.id}`));
  } finally { hook.deregister(); }
}

/** SDK initialization lists picker models and resolves aliases without a user turn. */
export async function claudeModels(env) {
  return new Promise((resolve, reject) => {
    const child = spawn('claude', ['--print', '--input-format', 'stream-json', '--output-format', 'stream-json',
      '--verbose', '--setting-sources', ''], { env, stdio: ['pipe', 'pipe', 'pipe'] });
    let buffered = '';
    let done = false;
    const timer = setTimeout(() => finish(new Error('Claude SDK initialize timed out')), 30_000);
    function finish(error, models) {
      if (done) return;
      done = true;
      clearTimeout(timer);
      child.kill();
      if (error) reject(error); else resolve(models);
    }
    child.on('error', finish);
    child.stdin.on('error', error => { if (!done) finish(error); });
    // Only structured initialization data is retained; no prompts or API calls.
    child.stderr.on('data', () => {});
    child.on('exit', () => { if (!done) finish(new Error('Claude exited before SDK initialization')); });
    child.stdout.on('data', chunk => {
      buffered += chunk;
      if (buffered.length > 4_000_000) return finish(new Error('Claude catalog exceeds limit'));
      let end;
      while ((end = buffered.indexOf('\n')) >= 0) {
        const line = buffered.slice(0, end); buffered = buffered.slice(end + 1);
        let message; try { message = JSON.parse(line); } catch { continue; }
        if (message.type !== 'control_response' || message.response?.request_id !== 'model-inventory') continue;
        const response = message.response.response;
        if (message.response.subtype !== 'success' || response?.account?.apiProvider !== 'firstParty' ||
            !Array.isArray(response.models) || !response.models.length) {
          return finish(new Error('Invalid first-party Claude SDK model catalog'));
        }
        const models = new Map();
        for (const model of response.models) {
          const resolved = model.resolvedModel ?? model.value;
          if (typeof resolved !== 'string' || !/^claude-[a-z0-9-]+(?:\[1m\])?$/.test(resolved) ||
              typeof model.value !== 'string') return finish(new Error('Claude returned an unresolved model alias'));
          // [1m] is a selector modifier, not a different provider model ID.
          // Preserve the resolved selector as evidence; join prices by the API ID.
          const id = resolved.replace(/\[1m\]$/, '');
          const entry = models.get(id) ?? { id, hidden: false, resolvedModel: resolved, aliases: [] };
          if (!entry.aliases.includes(model.value)) entry.aliases.push(model.value);
          models.set(id, entry);
        }
        return finish(undefined, [...models.values()].map(model => ({ ...model, aliases: model.aliases.sort() }))
          .sort((a, b) => a.id.localeCompare(b.id)));
      }
    });
    child.stdin.write(JSON.stringify({ type: 'control_request', request_id: 'model-inventory',
      request: { subtype: 'initialize' } }) + '\n');
  });
}

/** Read the installed Pi catalog, including provider/protocol identity, offline. */
export async function piModels(env) {
  const binary = env.PATH.split(':').map(directory => join(directory, 'pi')).find(existsSync);
  if (!binary) throw new Error('Pi binary is missing');
  const entry = realpathSync(binary);
  let directory = dirname(entry);
  while (!existsSync(join(directory, 'package.json'))) {
    const parent = dirname(directory);
    if (parent === directory) throw new Error('Pi package is missing');
    directory = parent;
  }
  const pkg = JSON.parse(readFileSync(join(directory, 'package.json'), 'utf8'));
  const dependency = Object.keys(pkg.dependencies ?? {}).find(name => name.endsWith('/pi-ai'));
  if (!dependency) throw new Error('Pi model registry dependency is missing');
  const root = createRequire(entry).resolve.paths(dependency).map(directory => join(directory, dependency))
    .find(directory => existsSync(join(directory, 'package.json')));
  if (!root) throw new Error('Installed Pi model registry is missing');
  const registryPackage = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
  const main = registryPackage.exports?.['.'];
  const target = typeof main === 'string' ? main : main?.import?.default ?? main?.import ?? main?.default ?? registryPackage.main;
  const api = await import(pathToFileURL(join(root, target)).href);
  // Pi 0.87 migrated the static registry to its exported providers/all API.
  const registry = typeof api.getProviders === 'function' ? api :
    await import(pathToFileURL(join(root, 'dist/providers/all.js')).href);
  const providers = registry.getProviders ?? registry.getBuiltinProviders;
  const models = registry.getModels ?? registry.getBuiltinModels;
  if (typeof providers !== 'function' || typeof models !== 'function') throw new Error('Unsupported Pi registry API');
  return providers().flatMap(provider => models(provider).map(model => ({
    id: model.id, provider: model.provider, api: model.api, hidden: false,
  }))).sort((a, b) => `${a.provider}/${a.id}`.localeCompare(`${b.provider}/${b.id}`));
}

/** Native picker entries are discovery evidence, not a guarantee for custom APIs. */
export async function codexModels(env) {
  return new Promise((resolve, reject) => {
    const child = spawn('codex', ['app-server'], { env, stdio: ['pipe', 'pipe', 'pipe'] });
    let buffered = '';
    const models = [];
    const timer = setTimeout(() => finish(new Error('Codex model/list timed out')), 60_000);
    let done = false;
    const send = (message) => child.stdin.write(JSON.stringify(message) + '\n');
    function finish(error) {
      if (done) return;
      done = true;
      clearTimeout(timer);
      child.kill();
      if (error) reject(error); else resolve(models);
    }
    child.on('error', finish);
    let stderr = '';
    child.stderr.on('data', chunk => { stderr = (stderr + chunk).slice(-4000); });
    child.on('exit', () => { if (!done) finish(new Error(`Codex app-server exited during discovery: ${stderr}`)); });
    child.stdout.on('data', (chunk) => {
      buffered += chunk;
      if (buffered.length > 4_000_000) return finish(new Error('Codex catalog exceeds limit'));
      let end;
      while ((end = buffered.indexOf('\n')) >= 0) {
        const line = buffered.slice(0, end); buffered = buffered.slice(end + 1);
        let message; try { message = JSON.parse(line); } catch { continue; }
        if (message.id === 1) {
          if (message.error) return finish(new Error('Codex initialize failed'));
          send({ method: 'initialized' });
          send({ id: 2, method: 'model/list', params: { limit: 100, includeHidden: true } });
        } else if (message.id === 2) {
          if (message.error || !Array.isArray(message.result?.data)) return finish(new Error('Invalid Codex model/list response'));
          for (const model of message.result.data) {
            if (typeof model.model !== 'string') return finish(new Error('Codex returned an invalid model ID'));
            models.push({ id: model.model, hidden: Boolean(model.hidden ?? model.isHidden) });
          }
          if (message.result.nextCursor) send({ id: 2, method: 'model/list', params: { limit: 100, includeHidden: true, cursor: message.result.nextCursor } });
          else finish();
        }
      }
    });
    send({ id: 1, method: 'initialize', params: { clientInfo: { name: 'sandbox0-model-inventory', version: '1.0.0' } } });
  });
}

export async function inventory() {
  const home = mkdtempSync(join(tmpdir(), 'coding-agent-inventory-'));
  mkdirSync(join(home, 'codex'));
  const env = { ...process.env, HOME: home, CODEX_HOME: join(home, 'codex'), NO_COLOR: '1' };
  // A published template must never inherit developer/account model discovery.
  for (const key of Object.keys(env)) if (/(?:API_KEY|AUTH_TOKEN|ACCESS_TOKEN)$/.test(key) ||
    /^(?:ANTHROPIC_|CLAUDE_CODE_|CLAUDE_CONFIG_DIR$|KIMI_|ZCODE_|NODE_OPTIONS$)/.test(key)) delete env[key];
  env.CLAUDE_CONFIG_DIR = join(home, 'claude');
  env.KIMI_CODE_HOME = join(home, 'kimi');
  env.ZCODE_DATA_BASE_DIR = join(home, 'zcode');
  for (const key of ['XDG_CONFIG_HOME', 'XDG_CACHE_HOME', 'XDG_DATA_HOME']) env[key] = join(home, key);
  try {
    const extractors = {
      codex: [codexModels, 'app-server/model/list'],
      'claude-code': [claudeModels, 'sdk/initialize/models/resolvedModel'],
      pi: [piModels, 'installed-pi-ai/provider-model-registry'],
      'kimi-code': [kimiModels, 'offline-provider/catalog/add+provider/list/moonshotai'],
      zcode: [zcodeModels, 'installed-zcode-builtin/provider-templates+model-rules'],
    };
    const harnesses = {};
    for (const [id, binary] of Object.entries(harnessBinaries)) {
      if (!extractors[id]) throw new Error(`Missing native model extractor for ${id}`);
      const version = execFileSync(binary, ['--version'], { env, encoding: 'utf8', timeout: 30_000 }).trim();
      if (!version) throw new Error(`Missing ${id} version`);
      const [extract, discovery] = extractors[id];
      const nativeModels = await extract(env);
      if (!nativeModels.length) throw new Error(`Empty native model catalog for ${id}`);
      harnesses[id] = { version, nativeModels, discovery };
    }
    return { schemaVersion: 3, harnesses };
  } finally { rmSync(home, { recursive: true, force: true }); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href) {
  process.stdout.write(JSON.stringify(await inventory(), null, 2) + '\n');
}
