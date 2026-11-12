import { describe, it, expect, beforeAll, afterAll } from "vitest";
import http, { type Server as HttpServer } from "node:http";
import type { Server } from "node:http";
import type { Worker } from "bullmq";
import { createApp } from "../src/app.js";
import { pool, db } from "../src/db/index.js";
import { deliveries } from "../src/db/schema.js";
import { OutboxPoller } from "../src/services/outboxPoller.js";
import { deliveryQueue, type DeliveryJobData } from "../src/queue/deliveryQueue.js";
import { redisConnection } from "../src/queue/redis.js";
import { createDeliveryWorker } from "../src/workers/deliveryWorker.js";
import { rateLimiter } from "../src/services/rateLimiter.js";
import { CircuitBreaker } from "../src/services/circuitBreaker.js";

describe("BOSS 6 FIGHT — Traffic Controller & Circuit Breakers", () => {
  let apiServer: Server;
  let apiBaseUrl: string;

  let mockServer: HttpServer;
  let mockBaseUrl: string;

  let customerAApiKey: string;
  let customerBApiKey: string;

  let worker: Worker<DeliveryJobData>;
  let poller: OutboxPoller;

  const deliveredA: number[] = [];
  const deliveredB: number[] = [];

  beforeAll(async () => {
    // 1. Start Platform API Server
    const app = createApp();
    await new Promise<void>((resolve) => {
      apiServer = app.listen(0, () => {
        const address = apiServer.address();
        if (address && typeof address === "object") {
          apiBaseUrl = `http://localhost:${address.port}`;
        }
        resolve();
      });
    });

    // 2. Start Mock Receiver Server
    mockServer = http.createServer((req, res) => {
      let body = "";
      req.on("data", (chunk) => (body += chunk));
      req.on("end", () => {
        const payload = JSON.parse(body);
        if (req.url === "/webhook/noisy-a") {
          deliveredA.push(payload.data.id);
        } else if (req.url === "/webhook/urgent-b") {
          deliveredB.push(payload.data.id);
        }
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
      });
    });

    await new Promise<void>((resolve) => {
      mockServer.listen(0, () => {
        const address = mockServer.address();
        if (address && typeof address === "object") {
          mockBaseUrl = `http://localhost:${address.port}`;
        }
        resolve();
      });
    });

    // 3. Register Customer A (Noisy Neighbor with Rate Limit = 5 req/sec)
    const resA = await fetch(`${apiBaseUrl}/v1/customers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "Noisy Customer A" }),
    });
    const dataA = await resA.json();
    customerAApiKey = dataA.apiKey;

    await fetch(`${apiBaseUrl}/v1/endpoints`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${customerAApiKey}`,
      },
      body: JSON.stringify({
        url: `${mockBaseUrl}/webhook/noisy-a`,
        rateLimit: 5,
      }),
    });

    // 4. Register Customer B (Urgent Customer)
    const resB = await fetch(`${apiBaseUrl}/v1/customers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "Urgent Customer B" }),
    });
    const dataB = await resB.json();
    customerBApiKey = dataB.apiKey;

    await fetch(`${apiBaseUrl}/v1/endpoints`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${customerBApiKey}`,
      },
      body: JSON.stringify({
        url: `${mockBaseUrl}/webhook/urgent-b`,
        rateLimit: 50,
      }),
    });

    await deliveryQueue.drain();
  });

  afterAll(async () => {
    poller?.stop();
    if (worker) {
      await worker.close();
    }
    await new Promise<void>((resolve) => apiServer.close(() => resolve()));
    await new Promise<void>((resolve) => mockServer.close(() => resolve()));
    await deliveryQueue.close();
    await redisConnection.quit();
    await pool.end();
  });

  it("should enforce atomic Token Bucket rate limits", async () => {
    const testEndpointId = "test_endpoint_token_bucket";
    await rateLimiter.reset(testEndpointId);

    // Consume 3 tokens from a capacity of 3
    const res1 = await rateLimiter.consume(testEndpointId, 3, 3);
    const res2 = await rateLimiter.consume(testEndpointId, 3, 3);
    const res3 = await rateLimiter.consume(testEndpointId, 3, 3);

    expect(res1.allowed).toBe(true);
    expect(res2.allowed).toBe(true);
    expect(res3.allowed).toBe(true);

    // 4th token request should be denied because bucket is empty
    const res4 = await rateLimiter.consume(testEndpointId, 3, 3);
    expect(res4.allowed).toBe(false);
    expect(res4.waitMs).toBeGreaterThan(0);
  });

  it("should trip Circuit Breaker to OPEN after threshold and probe in HALF_OPEN", async () => {
    const testEp = "test_endpoint_cb";
    const cb = new CircuitBreaker({ failureThreshold: 3, cooldownSeconds: 1 });
    await cb.reset(testEp);

    // Initial state: CLOSED
    const check1 = await cb.canAttempt(testEp);
    expect(check1.allow).toBe(true);
    expect(check1.state).toBe("CLOSED");

    // Record 2 failures
    await cb.recordFailure(testEp);
    await cb.recordFailure(testEp);

    // Still CLOSED (threshold is 3)
    const check2 = await cb.canAttempt(testEp);
    expect(check2.state).toBe("CLOSED");

    // 3rd failure: Trips circuit to OPEN!
    const tripResult = await cb.recordFailure(testEp);
    expect(tripResult.tripped).toBe(true);

    // Immediate check should be rejected without network request
    const checkOpen = await cb.canAttempt(testEp);
    expect(checkOpen.allow).toBe(false);
    expect(checkOpen.state).toBe("OPEN");

    // Wait for 1.2s cooldown to pass
    await new Promise((resolve) => setTimeout(resolve, 1200));

    // Next check enters HALF_OPEN probe state
    const checkHalfOpen = await cb.canAttempt(testEp);
    expect(checkHalfOpen.allow).toBe(true);
    expect(checkHalfOpen.state).toBe("HALF_OPEN");

    // Successful probe restores circuit to CLOSED
    await cb.recordSuccess(testEp);
    const checkRestored = await cb.canAttempt(testEp);
    expect(checkRestored.allow).toBe(true);
    expect(checkRestored.state).toBe("CLOSED");
  });

  it("BOSS FIGHT: Prevent Noisy Neighbor A from starving Urgent Customer B", async () => {
    const COUNT_A = 15;
    const COUNT_B = 5;

    // Ingest events for Customer A (Noisy)
    for (let i = 1; i <= COUNT_A; i++) {
      await fetch(`${apiBaseUrl}/v1/events`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${customerAApiKey}`,
        },
        body: JSON.stringify({
          eventType: `batch.event.${i}`,
          payload: { id: i, type: "bulk" },
          idempotencyKey: `bulk_a_${Date.now()}_${i}`,
        }),
      });
    }

    // Ingest events for Customer B (Urgent)
    for (let i = 1; i <= COUNT_B; i++) {
      await fetch(`${apiBaseUrl}/v1/events`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${customerBApiKey}`,
        },
        body: JSON.stringify({
          eventType: `urgent.event.${i}`,
          payload: { id: i, type: "urgent" },
          idempotencyKey: `urgent_b_${Date.now()}_${i}`,
        }),
      });
    }

    // Start poller & workers
    poller = new OutboxPoller({ batchSize: 50, intervalMs: 200 });
    poller.start();

    worker = createDeliveryWorker(10);

    // Wait for all deliveries to complete
    const startTime = Date.now();
    while (
      (deliveredA.length < COUNT_A || deliveredB.length < COUNT_B) &&
      Date.now() - startTime < 20000
    ) {
      await new Promise((resolve) => setTimeout(resolve, 200));
    }

    // Customer B's urgent events completed without starvation!
    expect(deliveredB).toHaveLength(COUNT_B);
    expect(deliveredA).toHaveLength(COUNT_A);
  }, 25000);
});
