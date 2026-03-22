import { access, mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

function npmCommand() {
  return process.platform === "win32" ? "npm.cmd" : "npm";
}

async function pathExists(targetPath) {
  try {
    await access(targetPath);
    return true;
  } catch {
    return false;
  }
}

function run(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: options.capture ? ["ignore", "pipe", "inherit"] : "inherit",
      cwd: options.cwd,
      env: options.env ?? process.env,
    });

    let stdout = "";
    if (options.capture) {
      child.stdout.setEncoding("utf8");
      child.stdout.on("data", (chunk) => {
        stdout += chunk;
      });
    }

    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(
          new Error(
            `${command} ${args.join(" ")} exited with code ${code ?? "unknown"}`,
          ),
        );
        return;
      }
      resolve(stdout);
    });
  });
}

async function resolveSDKJSPath() {
  const configuredPath = process.env.SANDBOX0_LOCAL_SDK_JS_PATH;
  const candidates = [
    configuredPath,
    path.resolve(repoRoot, "../sdk-js"),
    path.resolve(repoRoot, ".external/sdk-js"),
  ].filter(Boolean);

  for (const candidate of candidates) {
    const packageJSONPath = path.join(candidate, "package.json");
    if (!(await pathExists(packageJSONPath))) {
      continue;
    }

    const packageJSON = JSON.parse(await readFile(packageJSONPath, "utf8"));
    if (packageJSON.name === "sandbox0") {
      return candidate;
    }
  }

  throw new Error(
    "could not find a local sdk-js checkout; set SANDBOX0_LOCAL_SDK_JS_PATH or place sdk-js next to sandbox0",
  );
}

async function ensureSDKDependencies(sdkPath) {
  if (await pathExists(path.join(sdkPath, "node_modules"))) {
    return;
  }

  await run(npmCommand(), ["ci"], { cwd: sdkPath });
}

async function main() {
  const sdkPath = await resolveSDKJSPath();
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "sandbox0-sdk-js-"));

  try {
    await ensureSDKDependencies(sdkPath);
    await run(npmCommand(), ["run", "build"], { cwd: sdkPath });

    const packOutput = await run(
      npmCommand(),
      ["pack", "--silent", "--pack-destination", tempDir],
      {
        cwd: sdkPath,
        capture: true,
      },
    );
    const tarballName = packOutput
      .trim()
      .split(/\r?\n/)
      .filter(Boolean)
      .at(-1);
    if (!tarballName) {
      throw new Error("npm pack did not produce a tarball name");
    }

    const tarballPath = path.join(tempDir, tarballName);
    await run(
      npmCommand(),
      [
        "install",
        "--no-save",
        "--package-lock=false",
        "--engine-strict=false",
        tarballPath,
      ],
      { cwd: repoRoot },
    );

    console.log(`linked sdk-js from ${sdkPath}`);
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

await main();
