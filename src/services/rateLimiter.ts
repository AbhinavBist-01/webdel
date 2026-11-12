import { redisConnection } from "../queue/redis.js";

export interface RateLimitResult {
  allowed: boolean;
  waitMs: number;
  remainingTokens: number;
}

/**
 * Atomic Lua script for Token Bucket rate limiting in Redis:
 * - Calculates token refills based on elapsed time.
 * - Caps tokens at capacity.
 * - Deducts 1 token if available.
 * - If not available, returns the exact wait time in ms until a token is ready.
 */
const TOKEN_BUCKET_LUA = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refillRate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'lastRefill')
local tokens = tonumber(data[1])
local lastRefill = tonumber(data[2])

if tokens == nil then
  tokens = capacity
  lastRefill = now
else
  local delta = math.max(0, now - lastRefill)
  tokens = math.min(capacity, tokens + (delta * refillRate))
  lastRefill = now
end

if tokens >= requested then
  tokens = tokens - requested
  redis.call('HMSET', key, 'tokens', tokens, 'lastRefill', lastRefill)
  redis.call('EXPIRE', key, 3600)
  return { 1, 0, math.floor(tokens) }
else
  local needed = requested - tokens
  local waitSeconds = needed / refillRate
  local waitMs = math.ceil(waitSeconds * 1000)
  redis.call('HMSET', key, 'tokens', tokens, 'lastRefill', lastRefill)
  return { 0, waitMs, math.floor(tokens) }
end
`;

export class TokenBucketRateLimiter {
  /**
   * Check if an endpoint can accept a new webhook delivery right now.
   *
   * @param endpointId Unique ID of the destination endpoint
   * @param rateLimitPerSec Allowed requests per second (refill rate)
   * @param burstCapacity Maximum burst tokens (default: 2x rate limit)
   */
  async consume(
    endpointId: string,
    rateLimitPerSec: number,
    burstCapacity: number = rateLimitPerSec
  ): Promise<RateLimitResult> {
    const key = `ratelimit:endpoint:${endpointId}`;
    const nowInSeconds = Date.now() / 1000;

    const res = (await redisConnection.eval(
      TOKEN_BUCKET_LUA,
      1,
      key,
      burstCapacity,
      rateLimitPerSec,
      nowInSeconds,
      1
    )) as [number, number, number];

    const allowed = res[0] === 1;
    const waitMs = res[1] ?? 0;
    const remainingTokens = res[2] ?? 0;

    return {
      allowed,
      waitMs,
      remainingTokens,
    };
  }

  /**
   * Reset rate limit state for a given endpoint (useful for testing).
   */
  async reset(endpointId: string): Promise<void> {
    await redisConnection.del(`ratelimit:endpoint:${endpointId}`);
  }
}

export const rateLimiter = new TokenBucketRateLimiter();
