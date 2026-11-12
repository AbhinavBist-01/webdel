import { redisConnection } from "../queue/redis.js";
import { circuitBreakerTripsTotal } from "../observability/metrics.js";

export type CircuitState = "CLOSED" | "OPEN" | "HALF_OPEN";

export interface CircuitBreakerConfig {
  failureThreshold?: number;  // Consecutive failures before tripping (default: 5)
  cooldownSeconds?: number;   // How long circuit stays OPEN before HALF_OPEN probe (default: 15s)
}

export class CircuitBreaker {
  private failureThreshold: number;
  private cooldownSeconds: number;

  constructor(config: CircuitBreakerConfig = {}) {
    this.failureThreshold = config.failureThreshold ?? 5;
    this.cooldownSeconds = config.cooldownSeconds ?? 15;
  }

  private getKey(endpointId: string): string {
    return `circuit:endpoint:${endpointId}`;
  }

  /**
   * Check whether a delivery should be attempted:
   * - CLOSED: Allowed
   * - OPEN: Blocked (fails fast without network request)
   * - HALF_OPEN: Single probe allowed through
   */
  async canAttempt(endpointId: string): Promise<{
    allow: boolean;
    state: CircuitState;
  }> {
    const key = this.getKey(endpointId);
    const data = await redisConnection.hgetall(key);

    const state = (data.state as CircuitState) || "CLOSED";

    if (state === "CLOSED") {
      return { allow: true, state: "CLOSED" };
    }

    if (state === "OPEN") {
      const openedAt = Number(data.openedAt) || 0;
      const elapsedSeconds = (Date.now() - openedAt) / 1000;

      if (elapsedSeconds >= this.cooldownSeconds) {
        // Cooldown passed: Transition to HALF_OPEN to probe endpoint
        await redisConnection.hset(key, "state", "HALF_OPEN");
        return { allow: true, state: "HALF_OPEN" };
      }

      // Still cooling down -> fast reject
      return { allow: false, state: "OPEN" };
    }

    // HALF_OPEN: Probe in progress, allow trial request
    return { allow: true, state: "HALF_OPEN" };
  }

  /**
   * Record a successful response (2xx).
   * Resets failures counter and restores circuit to CLOSED state.
   */
  async recordSuccess(endpointId: string): Promise<void> {
    const key = this.getKey(endpointId);
    await redisConnection.del(key);
  }

  /**
   * Record a delivery failure (5xx or timeout).
   * If failure threshold is breached, trips state to OPEN.
   */
  async recordFailure(endpointId: string): Promise<{
    tripped: boolean;
    consecutiveFailures: number;
  }> {
    const key = this.getKey(endpointId);

    const failures = await redisConnection.hincrby(key, "failures", 1);

    if (failures >= this.failureThreshold) {
      // Trip circuit to OPEN
      circuitBreakerTripsTotal.inc();

      await redisConnection.hmset(key, {
        state: "OPEN",
        openedAt: Date.now(),
      });
      await redisConnection.expire(key, this.cooldownSeconds * 2);

      return { tripped: true, consecutiveFailures: failures };
    }

    return { tripped: false, consecutiveFailures: failures };
  }

  /**
   * Reset circuit breaker state for testing.
   */
  async reset(endpointId: string): Promise<void> {
    await redisConnection.del(this.getKey(endpointId));
  }
}

export const circuitBreaker = new CircuitBreaker();
