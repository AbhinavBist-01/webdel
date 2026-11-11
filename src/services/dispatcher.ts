import crypto from "node:crypto";

export interface DispatchOptions {
  deliveryId: string;
  url: string;
  signingSecret: string;
  payload: Record<string, unknown>;
  timeoutMs?: number;
}

export interface DispatchResult {
  requestHeaders: Record<string, string>;
  responseCode: number | null;
  responseBody: string | null;
  durationMs: number;
  error: string | null;
  outcome: "success" | "client_error" | "server_error" | "timeout" | "connection_error";
}

/**
 * Compute the HMAC-SHA256 signature for a webhook payload.
 * Standard format: HMAC-SHA256(secret, `${timestamp}.${rawPayload}`)
 */
export function computeWebhookSignature(
  signingSecret: string,
  timestamp: number,
  rawPayload: string
): string {
  return crypto
    .createHmac("sha256", signingSecret)
    .update(`${timestamp}.${rawPayload}`)
    .digest("hex");
}

/**
 * Verify an incoming webhook signature using constant-time comparison
 * to protect against timing attacks.
 */
export function verifyWebhookSignature(
  signingSecret: string,
  timestamp: number,
  rawPayload: string,
  receivedSignature: string
): boolean {
  const expectedSignature = computeWebhookSignature(
    signingSecret,
    timestamp,
    rawPayload
  );

  const expectedBuffer = Buffer.from(expectedSignature, "hex");
  const receivedBuffer = Buffer.from(receivedSignature, "hex");

  if (expectedBuffer.length !== receivedBuffer.length) {
    return false;
  }

  return crypto.timingSafeEqual(expectedBuffer, receivedBuffer);
}

/**
 * Dispatches an HTTP POST request to an external endpoint with:
 * - HMAC-SHA256 signature
 * - Strict timeout via AbortController
 * - High-resolution latency tracking
 * - Truncated response body (protecting memory from oversized responses)
 */
export async function dispatchWebhook(
  options: DispatchOptions
): Promise<DispatchResult> {
  const { deliveryId, url, signingSecret, payload, timeoutMs = 5000 } = options;

  const rawPayload = JSON.stringify(payload);
  const timestamp = Math.floor(Date.now() / 1000);
  const signature = computeWebhookSignature(signingSecret, timestamp, rawPayload);

  const requestHeaders: Record<string, string> = {
    "content-type": "application/json",
    "user-agent": "WebhookDeliveryPlatform/1.0",
    "webhook-id": deliveryId,
    "webhook-timestamp": String(timestamp),
    "webhook-signature": `v1,${signature}`,
  };

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  const startTime = performance.now();

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: requestHeaders,
      body: rawPayload,
      signal: controller.signal,
    });

    clearTimeout(timer);
    const durationMs = Math.round(performance.now() - startTime);

    // Read response body with safety limit (max 10KB)
    let responseBody: string | null = null;
    try {
      const text = await response.text();
      responseBody = text.length > 10240 ? text.slice(0, 10240) + "... [truncated]" : text;
    } catch {
      responseBody = null;
    }

    const code = response.status;
    let outcome: DispatchResult["outcome"];

    if (code >= 200 && code < 300) {
      outcome = "success";
    } else if (code >= 400 && code < 500) {
      outcome = "client_error";
    } else {
      outcome = "server_error";
    }

    return {
      requestHeaders,
      responseCode: code,
      responseBody,
      durationMs,
      error: outcome === "success" ? null : `HTTP status ${code}`,
      outcome,
    };
  } catch (err: unknown) {
    clearTimeout(timer);
    const durationMs = Math.round(performance.now() - startTime);

    const isTimeout =
      err instanceof Error &&
      (err.name === "AbortError" || err.message.toLowerCase().includes("aborted"));

    const errorMessage =
      isTimeout
        ? `Request timed out after ${timeoutMs}ms`
        : err instanceof Error
        ? err.message
        : "Unknown network error";

    return {
      requestHeaders,
      responseCode: null,
      responseBody: null,
      durationMs,
      error: errorMessage,
      outcome: isTimeout ? "timeout" : "connection_error",
    };
  }
}
