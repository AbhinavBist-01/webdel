import crypto from "node:crypto";

/**
 * Generate a new API key for a customer.
 * Prefix `wh_live_` allows easy scanning by secret detection tools (e.g., GitHub secret scanning).
 */
export function generateApiKey(): string {
  const bytes = crypto.randomBytes(24).toString("hex");
  return `wh_live_${bytes}`;
}

/**
 * Hash an API key using SHA-256 for secure storage and constant-time lookup.
 */
export function hashApiKey(apiKey: string): string {
  return crypto.createHash("sha256").update(apiKey.trim()).digest("hex");
}

/**
 * Generate an endpoint signing secret prefixed with `whsec_`.
 * This secret is shared with the customer to verify HMAC-SHA256 signatures on delivery.
 */
export function generateSigningSecret(): string {
  const bytes = crypto.randomBytes(32).toString("hex");
  return `whsec_${bytes}`;
}
