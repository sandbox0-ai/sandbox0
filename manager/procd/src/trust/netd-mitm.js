import fs from "node:fs";
import path from "node:path";

const mitmCAFileEnv = "SANDBOX0_NETD_MITM_CA_FILE";
const mitmCABundleEnv = "SANDBOX0_NETD_CA_BUNDLE_FILE";
const defaultBundlePath = "/procd/state/netd/netd-ca-bundle.crt";
const tlsBundleEnvVars = [
  "NODE_EXTRA_CA_CERTS",
  "SSL_CERT_FILE",
  "REQUESTS_CA_BUNDLE",
  "CURL_CA_BUNDLE",
  "GIT_SSL_CAINFO",
  "AWS_CA_BUNDLE"
];
const systemCABundleCandidates = [
  "/etc/ssl/certs/ca-certificates.crt",
  "/etc/pki/tls/certs/ca-bundle.crt",
  "/etc/ssl/ca-bundle.pem",
  "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
  "/etc/ssl/cert.pem"
];

function firstSystemCABundle() {
  for (const candidate of systemCABundleCandidates) {
    try {
      const data = fs.readFileSync(candidate);
      if (String(data).trim() !== "") return data;
    } catch {
      // Try next path.
    }
  }
  return Buffer.alloc(0);
}

export function configureNetdMITMCATrust() {
  const mitmCAPath = (process.env[mitmCAFileEnv] || "").trim();
  if (!mitmCAPath) return "";
  const mitmCA = fs.readFileSync(mitmCAPath);
  const bundlePath = (process.env[mitmCABundleEnv] || defaultBundlePath).trim();
  const parts = [];
  const systemCA = firstSystemCABundle();
  if (systemCA.length > 0) parts.push(String(systemCA).trim());
  parts.push(String(mitmCA).trim());
  fs.mkdirSync(path.dirname(bundlePath), { recursive: true, mode: 0o755 });
  fs.writeFileSync(bundlePath, parts.join("\n") + "\n", { mode: 0o644 });
  process.env[mitmCABundleEnv] = bundlePath;
  for (const name of tlsBundleEnvVars) process.env[name] = bundlePath;
  return bundlePath;
}
