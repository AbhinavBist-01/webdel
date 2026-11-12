export interface RetryConfig {
  baseMs?: number;         // Base delay in ms (default: 5000ms = 5s)
  maxDelayMs?: number;     // Maximum delay cap (default: 300,000ms = 5 mins)
  maxAttempts?: number;    // Maximum retry attempts before DLQ (default: 5)
  jitterFactor?: number;   // Randomized jitter percentage (default: 0.2 = 20%)
}

export const DEFAULT_RETRY_CONFIG: Required<RetryConfig> = {
  baseMs: 5000,
  maxDelayMs: 300000,
  maxAttempts: 5,
  jitterFactor: 0.2,
};

/**
 * Calculates the next retry delay in milliseconds using exponential backoff with full jitter.
 * Formula: delay = min(maxDelay, base * 2^(attempt - 1)) + random_jitter
 */
export function calculateNextRetryDelayMs(
  attemptNumber: number,
  customConfig: RetryConfig = {}
): number {
  const config = { ...DEFAULT_RETRY_CONFIG, ...customConfig };

  // Calculate exponential backoff
  const exponentialFactor = Math.pow(2, Math.max(0, attemptNumber - 1));
  const rawDelay = Math.min(config.maxDelayMs, config.baseMs * exponentialFactor);

  // Add jitter to prevent thundering herd / retry storms
  const jitter = Math.random() * (rawDelay * config.jitterFactor);
  return Math.round(rawDelay + jitter);
}

/**
 * Classifies whether an HTTP error is transient (retryable) or permanent.
 * - Retryable: 429 Too Many Requests, 5xx Server Errors, Network Timeouts, Connection Drops.
 * - Permanent: 4xx Client Errors (400, 401, 403, 404, 422) where retrying would produce the exact same error.
 */
export function isRetryableError(
  responseCode: number | null,
  error: string | null
): boolean {
  // If responseCode is null, it was a network drop, connection refused, or timeout -> retryable
  if (responseCode === null) {
    return true;
  }

  // Rate limited by destination -> retryable
  if (responseCode === 429) {
    return true;
  }

  // Destination server error -> retryable
  if (responseCode >= 500 && responseCode <= 599) {
    return true;
  }

  // 4xx errors are permanent client issues -> do NOT retry
  return false;
}
