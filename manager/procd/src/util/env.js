export function mergeEnvironment(...layers) {
  const env = { ...process.env };
  for (const layer of layers) {
    if (!layer) continue;
    for (const [key, value] of Object.entries(layer)) {
      if (value === undefined || value === null) continue;
      env[key] = String(value);
    }
  }
  return env;
}

export function normalizeStringMap(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  const out = {};
  for (const [key, val] of Object.entries(value)) {
    if (val === undefined || val === null) continue;
    out[String(key)] = String(val);
  }
  return out;
}
