import crypto from "node:crypto";
import fs from "node:fs";
import { codes, writeError } from "./response.js";

const allowedCallers = new Set(["cluster-gateway", "manager", "ssh-gateway"]);
const clockSkewSeconds = 5;

function base64urlDecode(value) {
  return Buffer.from(value.replace(/-/g, "+").replace(/_/g, "/"), "base64");
}

function timingSafeEqualText(a, b) {
  const ab = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  return ab.length === bb.length && crypto.timingSafeEqual(ab, bb);
}

function decodePemBlock(data, type) {
  const text = Buffer.isBuffer(data) ? data.toString("utf8") : String(data);
  const escapedType = type.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = text.match(new RegExp(`-----BEGIN ${escapedType}-----([\\s\\S]*?)-----END ${escapedType}-----`));
  if (!match) return null;
  return Buffer.from(match[1].replace(/\s+/g, ""), "base64");
}

function loadEd25519PublicKey(publicKeyPath) {
  const data = fs.readFileSync(publicKeyPath);
  try {
    return crypto.createPublicKey(data);
  } catch (err) {
    const der = decodePemBlock(data, "ED25519 PUBLIC KEY");
    if (!der) throw err;
    return crypto.createPublicKey({ key: der, format: "der", type: "spki" });
  }
}

export class InternalAuthValidator {
  constructor(publicKeyPath, logger) {
    this.publicKeyPath = publicKeyPath;
    this.logger = logger;
    this.publicKey = loadEd25519PublicKey(publicKeyPath);
  }

  validate(token) {
    if (!token || typeof token !== "string") throw new Error("missing authentication token");
    const parts = token.split(".");
    if (parts.length !== 3) throw new Error("invalid token format");
    const [encodedHeader, encodedPayload, encodedSignature] = parts;
    const header = JSON.parse(base64urlDecode(encodedHeader).toString("utf8"));
    if (header.alg !== "EdDSA") throw new Error("invalid token signature");

    const signed = Buffer.from(`${encodedHeader}.${encodedPayload}`);
    const signature = base64urlDecode(encodedSignature);
    if (!crypto.verify(null, signed, this.publicKey, signature)) {
      throw new Error("invalid token signature");
    }

    const claims = JSON.parse(base64urlDecode(encodedPayload).toString("utf8"));
    const now = Math.floor(Date.now() / 1000);
    if (!claims.exp || now > Number(claims.exp) + clockSkewSeconds) throw new Error("token expired");
    if (!timingSafeEqualText(claims.aud, "procd") || !timingSafeEqualText(claims.target, "procd")) {
      throw new Error(`invalid target: expected procd, got ${claims.target || claims.aud || ""}`);
    }
    if (claims.caller !== claims.iss) throw new Error("invalid caller: caller mismatch");
    if (!allowedCallers.has(claims.caller)) {
      throw new Error(`invalid caller: caller ${claims.caller || ""} not in allowed list`);
    }
    return claims;
  }
}

export function requireInternalAuth(validator, handler) {
  return async (req, res, params) => {
    const token = req.headers["x-internal-token"];
    if (!token) {
      writeError(res, 401, codes.unauthorized, "missing authentication token");
      return;
    }
    try {
      req.internalClaims = validator.validate(Array.isArray(token) ? token[0] : token);
    } catch (err) {
      writeError(res, 401, codes.unauthorized, `unauthorized: ${err.message}`);
      return;
    }
    await handler(req, res, params);
  };
}
