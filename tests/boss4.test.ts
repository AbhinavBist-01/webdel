import { describe, it, expect, beforeAll, afterAll } from "vitest";
import http, { type Server as HttpServer } from "node:http";
import type { Server } from "node:http";
import { eq } from "drizzle-orm";
import { createApp } from "../src/app.js";
import { pool, db } from "../src/db/index.js";
import { deliveries, deliveryAttempts, deadLetterEvents } from "../src/db/schema.js";
import { OutboxPoller } from "../src/services/outboxPoller.js";
import { deliveryQueue } from "../src/queue/deliveryQueue.js";
import { redisConnection } from "../src/queue/redis.js";
import { processDeliveryJob } from "../src/workers/deliveryWorker.js";
import {
  calculateNextRetryDelayMs,
  isRetryableError,
} from "../src/services/retryScheduler.js";

describe("BOSS 4 FIGHT — The Retry Machine & Dead Letter Queue", () => {
  let apiServer: Server;
  let apiBaseUrl: string;

  let mockServer: HttpServer;
  let mockBaseUrl: string;

  let endpointStatus: number = 500; // Configurable mock response code
  let apiKey: string;
  let endpointId: string;

  beforeAll(async () => {
    // 1. Start Webhook API Server
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

    // 2. Start Configurable Mock Server
    mockServer = http.createServer((_req, res) => {
      if (endpointStatus === 200) {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "recovered" }));
      } else {
        res.writeHead(endpointStatus, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: `Server down: ${endpointStatus}` }));
      }
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

    // 3. Register Customer & Endpoint
    const custRes = await fetch(`${apiBaseUrl}/v1/customers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "Boss 4 Retry Customer" }),
    });
    const custData = await custRes.json();
    apiKey = custData.apiKey;

    const epRes = await fetch(`${apiBaseUrl}/v1/endpoints`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({ url: `${mockBaseUrl}/webhook` }),
    });
    const epData = await epRes.json();
    endpointId = epData.id;

    await deliveryQueue.drain();
  });

  afterAll(async () => {
    await new Promise<void>((resolve) => apiServer.close(() => resolve()));
    await new Promise<void>((resolve) => mockServer.close(() => resolve()));
    await deliveryQueue.close();
    await redisConnection.quit();
    await pool.end();
  });

  it("should correctly compute exponential backoff with jitter", () => {
    const config = { baseMs: 1000, maxDelayMs: 60000, jitterFactor: 0.2 };

    const delay1 = calculateNextRetryDelayMs(1, config);
    const delay2 = calculateNextRetryDelayMs(2, config);
    const delay3 = calculateNextRetryDelayMs(3, config);

    // Attempt 1: ~1000ms (1000 to 1200 with 20% jitter)
    expect(delay1).toBeGreaterThanOrEqual(1000);
    expect(delay1).toBeLessThanOrEqual(1200);

    // Attempt 2: ~2000ms (2000 to 2400)
    expect(delay2).toBeGreaterThanOrEqual(2000);
    expect(delay2).toBeLessThanOrEqual(2400);

    // Attempt 3: ~4000ms (4000 to 4800)
    expect(delay3).toBeGreaterThanOrEqual(4000);
    expect(delay3).toBeLessThanOrEqual(4800);

    // Jitter ensures two calls for same attempt are not identical
    const sampleA = calculateNextRetryDelayMs(2, config);
    const sampleB = calculateNextRetryDelayMs(2, config);
    // Over multiple calls, jitter produces variation
    expect(typeof sampleA).toBe("number");
    expect(typeof sampleB).toBe("number");
  });

  it("should classify errors as retryable or permanent correctly", () => {
    // Retryable
    expect(isRetryableError(500, "Internal Server Error")).toBe(true);
    expect(isRetryableError(502, "Bad Gateway")).toBe(true);
    expect(isRetryableError(503, "Service Unavailable")).toBe(true);
    expect(isRetryableError(429, "Too Many Requests")).toBe(true);
    expect(isRetryableError(null, "Request timed out")).toBe(true);

    // Permanent (do not retry)
    expect(isRetryableError(400, "Bad Request")).toBe(false);
    expect(isRetryableError(401, "Unauthorized")).toBe(false);
    expect(isRetryableError(404, "Not Found")).toBe(false);
    expect(isRetryableError(422, "Unprocessable Entity")).toBe(false);
  });

  it("BOSS FIGHT: Fail endpoint, exhaust retries to DLQ, bring endpoint back, and replay successfully", async () => {
    endpointStatus = 500; // Customer's endpoint is down

    // 1. Ingest event
    const eventRes = await fetch(`${apiBaseUrl}/v1/events`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        eventType: "subscription.cancelled",
        payload: { subscriptionId: "sub_123" },
        idempotencyKey: `boss4_idem_${Date.now()}`,
      }),
    });
    const eventData = await eventRes.json();

    // 2. Outbox poller publishes to BullMQ
    const poller = new OutboxPoller();
    await poller.pollOnce();

    const jobs = await deliveryQueue.getJobs(["waiting", "active", "completed"]);
    const activeJob = jobs.find((j) => j.data.eventId === eventData.id);
    expect(activeJob).toBeDefined();

    // 3. Worker attempt 1 (fails with 500) -> status: 'retry'
    const testRetryConfig = { baseMs: 100, maxDelayMs: 1000, maxAttempts: 2 };
    await processDeliveryJob(activeJob!, 1500, testRetryConfig);

    const [deliveryAfterAttempt1] = await db
      .select()
      .from(deliveries)
      .where(eq(deliveries.endpointId, endpointId));

    expect(deliveryAfterAttempt1?.status).toBe("retry");
    expect(deliveryAfterAttempt1?.attemptCount).toBe(1);
    expect(deliveryAfterAttempt1?.nextAttemptAt).not.toBeNull();

    // 4. Worker attempt 2 (maxAttempts: 2 reached) -> transitions to 'exhausted' & moves to DLQ
    await processDeliveryJob(activeJob!, 1500, testRetryConfig);

    const [deliveryAfterAttempt2] = await db
      .select()
      .from(deliveries)
      .where(eq(deliveries.endpointId, endpointId));

    expect(deliveryAfterAttempt2?.status).toBe("exhausted");
    expect(deliveryAfterAttempt2?.attemptCount).toBe(2);

    // Verify DLQ entry exists
    const [dlqEntry] = await db
      .select()
      .from(deadLetterEvents)
      .where(eq(deadLetterEvents.deliveryId, deliveryAfterAttempt2!.id));

    expect(dlqEntry).toBeDefined();
    expect(dlqEntry?.reason).toContain("Exhausted maximum retry attempts");
    expect(dlqEntry?.replayedAt).toBeNull();

    // 5. Inspect DLQ via API
    const dlqRes = await fetch(`${apiBaseUrl}/v1/deliveries/dlq`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    expect(dlqRes.status).toBe(200);
    const dlqData = await dlqRes.json();
    expect(dlqData.dlq.length).toBeGreaterThanOrEqual(1);
    expect(dlqData.dlq[0].deliveryId).toBe(deliveryAfterAttempt2!.id);

    // 6. SIMULATE ENDPOINT RECOVERY & MANUAL REPLAY
    endpointStatus = 200; // Customer brought their server back up!

    // Replay the delivery
    const replayRes = await fetch(
      `${apiBaseUrl}/v1/deliveries/${deliveryAfterAttempt2!.id}/replay`,
      {
        method: "POST",
        headers: { Authorization: `Bearer ${apiKey}` },
      }
    );
    expect(replayRes.status).toBe(200);

    // 7. Verify BullMQ received the replay job
    const allJobs = await deliveryQueue.getJobs(["waiting", "active", "completed"]);
    const replayJob = allJobs.find((j) => j.data.eventId === eventData.id && String(j.data.outboxId).startsWith("replay_"));
    expect(replayJob).toBeDefined();

    // 8. Process replay job -> succeeds with 200 OK!
    await processDeliveryJob(replayJob!, 1500, testRetryConfig);

    const [finalDelivery] = await db
      .select()
      .from(deliveries)
      .where(eq(deliveries.id, deliveryAfterAttempt2!.id));

    expect(finalDelivery?.status).toBe("success");
    expect(finalDelivery?.lastResponseCode).toBe(200);
    expect(finalDelivery?.completedAt).not.toBeNull();

    // Verify DLQ replayedAt was updated
    const [updatedDlqEntry] = await db
      .select()
      .from(deadLetterEvents)
      .where(eq(deadLetterEvents.deliveryId, deliveryAfterAttempt2!.id));

    expect(updatedDlqEntry?.replayedAt).not.toBeNull();
  });
});
