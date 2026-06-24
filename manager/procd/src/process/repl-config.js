export const defaultReadyToken = "_S0_> ";
export const defaultContinuationToken = "__S0_CONT__ ";

const aliasAliases = {
  python3: "python",
  javascript: "node",
  nodejs: "node",
  rb: "ruby",
  R: "r",
  pl: "perl"
};

export const builtinConfigs = {
  python: {
    name: "python",
    candidates: [
      { name: "python3", args: ["-q", "-i", "-u", "-c", `import sys; sys.ps1='${defaultReadyToken}'; sys.ps2='${defaultContinuationToken}'`] },
      { name: "python", args: ["-q", "-i", "-u", "-c", `import sys; sys.ps1='${defaultReadyToken}'; sys.ps2='${defaultContinuationToken}'`] }
    ],
    env: [
      { name: "PYTHONUNBUFFERED", value: "1" },
      { name: "PYTHONDONTWRITEBYTECODE", value: "1" }
    ],
    ready: { mode: "prompt_token", token: defaultReadyToken }
  },
  node: {
    name: "node",
    candidates: [
      { name: "node", args: ["-e", `require('repl').start({prompt: '${defaultReadyToken}'})`] },
      { name: "nodejs", args: ["-e", `require('repl').start({prompt: '${defaultReadyToken}'})`] }
    ],
    ready: { mode: "prompt_token", token: defaultReadyToken }
  },
  bash: {
    name: "bash",
    candidates: [
      { name: "bash", args: ["--norc", "--noprofile", "-i"] },
      { name: "sh", args: ["-i"] }
    ],
    env: [
      { name: "TERM", valueFrom: "term" },
      { name: "PS1", valueFrom: "prompt" }
    ],
    defaultTerm: "xterm-256color",
    prompt: { customPrompt: defaultReadyToken },
    ready: { mode: "prompt_token", token: defaultReadyToken }
  },
  zsh: {
    name: "zsh",
    candidates: [
      { name: "zsh", args: ["--no-rcs", "-i"] },
      { name: "bash", args: ["--norc", "--noprofile", "-i"] },
      { name: "sh", args: ["-i"] }
    ],
    env: [
      { name: "TERM", valueFrom: "term" },
      { name: "PS1", valueFrom: "prompt" }
    ],
    defaultTerm: "xterm-256color",
    prompt: { customPrompt: defaultReadyToken },
    ready: { mode: "prompt_token", token: defaultReadyToken }
  },
  ruby: {
    name: "ruby",
    candidates: [
      { name: "ruby", args: ["-e", `require 'irb'; IRB.conf[:PROMPT][:S0]={PROMPT_I:'${defaultReadyToken}', PROMPT_S:'${defaultContinuationToken}', PROMPT_C:'${defaultContinuationToken}', RETURN:"%s\\n"}; IRB.conf[:PROMPT_MODE]=:S0; IRB.start`] }
    ],
    ready: { mode: "prompt_token", token: defaultReadyToken }
  },
  lua: {
    name: "lua",
    candidates: [
      { name: "lua", args: ["-i", "-e", `_PROMPT='${defaultReadyToken}'; _PROMPT2='${defaultContinuationToken}'`] },
      { name: "lua5.4", args: ["-i", "-e", `_PROMPT='${defaultReadyToken}'; _PROMPT2='${defaultContinuationToken}'`] },
      { name: "luajit", args: ["-i", "-e", `_PROMPT='${defaultReadyToken}'; _PROMPT2='${defaultContinuationToken}'`] }
    ],
    ready: { mode: "prompt_token", token: defaultReadyToken }
  },
  php: {
    name: "php",
    candidates: [{ name: "psysh", args: [] }, { name: "php", args: ["-a"] }],
    ready: { mode: "startup_delay", startupDelayMs: 200 }
  },
  r: {
    name: "r",
    candidates: [
      { name: "R", args: ["--no-save", "--no-restore", "--interactive", "-e", `options(prompt='${defaultReadyToken}', continue='${defaultContinuationToken}')`] },
      { name: "Rscript", args: ["-e", `options(prompt='${defaultReadyToken}', continue='${defaultContinuationToken}'); while(TRUE) { cat(getOption('prompt')); eval(parse(text=readline())) }`] }
    ],
    ready: { mode: "prompt_token", token: defaultReadyToken }
  },
  perl: {
    name: "perl",
    candidates: [{ name: "perl", args: ["-d", "-e", "1"] }],
    ready: { mode: "startup_delay", startupDelayMs: 200 }
  }
};

export function normalizeReplConfig(alias, custom) {
  let name = custom?.name || alias || "python";
  if (!custom && aliasAliases[name]) name = aliasAliases[name];
  const base = custom || builtinConfigs[name];
  if (!base || !base.candidates || base.candidates.length === 0) return null;
  if (custom && !name) return null;
  const baseReady = base.ready || {};
  const ready = {
    mode: baseReady.mode || "",
    token: baseReady.token || "",
    startupDelayMs: baseReady.startupDelayMs ?? baseReady.startup_delay_ms ?? 0
  };
  if (!ready.mode) ready.mode = ready.token ? "prompt_token" : "startup_delay";
  if (ready.mode === "prompt_token" && !ready.token) return null;
  if (ready.mode === "startup_delay" && ready.startupDelayMs < 0) return null;
  if (ready.mode !== "prompt_token" && ready.mode !== "startup_delay") return null;
  const basePrompt = base.prompt || {};
  const prompt = {
    customPrompt: basePrompt.customPrompt ?? basePrompt.custom_prompt ?? ""
  };
  return {
    ...base,
    name,
    candidates: base.candidates.map((c) => ({ name: c.name, args: [...(c.args || [])] })),
    env: (base.env || []).map((e) => ({
      name: e.name,
      value: e.value || "",
      valueFrom: e.valueFrom || e.value_from || ""
    })),
    ready,
    prompt
  };
}
