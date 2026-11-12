import { describe, it, expect, beforeAll, afterAll } from "vitest";
import http, { type Server as HttpServer } from "node:http";
import type { Server } from "node:http";
import type { Worker } from "bullmq";
import { createApp } from "../src/app.js";
import { pool, db } from "../src/db/index.js";
import { deliveries, deadLetterEvents } from "../src/db/schema.js";
import { OutboxPoller } from "../src/services/outboxPoller.js";
import { deliveryQueue, type DeliveryJobData } from "../src/queue/deliveryQueue.js";
import { redisConnection } from "../src/queue/redis.js";
import { createDeliveryWorker, processDeliveryJob } from "../src/workers/deliveryWorker.js";

describe("BOSS 7 FIGHT — The Recovery & Operability System", () => {
  let apiServer: Server;
  let apiBaseUrl: string;

  let mockServer: HttpServer;
  let mockBaseUrl: string;

  let apiKey: string;
  let endpointId: string;

  let shouldFail: boolean = false;
  const receivedWebhookIds: string[] = [];

  let worker: Worker<DeliveryJobData>;
  let poller: OutboxPoller;

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
      if (shouldFail) {
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Down for maintenance" }));
        return;
      }

      let body = "";
      req.on("data", (chunk) => (body += chunk));
      req.on("end", () => {
        const payload = JSON.parse(body);
        receivedWebhookIds.push(payload.id);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ received: true }));
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

    // 3. Register Customer & Endpoint
    const resA = await fetch(`${apiBaseUrl}/v1/customers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "Operator Customer" }),
    });
    const dataA = await resA.json();
    apiKey = dataA.apiKey;

    const epRes = await fetch(`${apiBaseUrl}/v1/endpoints`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({ url: `${mockBaseUrl}/webhook/recovery` }),
    });
    const epData = await epRes.json();
    endpointId = epData.id;

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

  it("should pause endpoint, buffer events, and dispatch on resume", async () => {
    // 1. Pause endpoint
    const pauseRes = await fetch(`${apiBaseUrl}/v1/endpoints/${endpointId}/pause`, {
      method: "PATCH",
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    expect(pauseRes.status).toBe(200);
    const pauseData = await pauseRes.json();
    expect(pauseData.endpoint.status).toBe("disabled");

    // 2. Ingest 2 events while paused
    const ev1Res = await fetch(`${apiBaseUrl}/v1/events`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        eventType: "maintenance.event.1",
        payload: { item: 1 },
        idempotencyKey: `maint_1_${Date.now()}`,
      }),
    });
    const ev1 = await ev1Res.json();

    // Outbox poller runs
    const localPoller = new OutboxPoller();
    await localPoller.pollOnce();

    // Worker attempts to process
    const jobs = await deliveryQueue.getJobs(["waiting", "active", "completed"]);
    const job1 = jobs.find((j) => j.data.eventId === ev1.id);
    if (job1) {
      await processDeliveryJob(job1);
    }

    // Verify NO request was made to mock server because endpoint is disabled
    expect(receivedWebhookIds).not.toContain(ev1.id);

    // 3. Resume endpoint
    const resumeRes = await fetch(`${apiBaseUrl}/v1/endpoints/${endpointId}/resume`, {
      method: "PATCH",
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    expect(resumeRes.status).toBe(200);
    const resumeData = await resumeRes.json();
    expect(resumeData.endpoint.status).toBe("active");
    expect(resumeData.requeuedCount).toBeGreaterThanOrEqual(1);

    // 4. Worker now processes resumed job
    const newJobs = await deliveryQueue.getJobs(["waiting", "active", "completed"]);
    const resumedJob = newJobs.find((j) => j.data.eventId === ev1.id && String(j.data.outboxId).startsWith("resume_"));
    expect(resumedJob).toBeDefined();

    await processDeliveryJob(resumedJob!);
    expect(receivedWebhookIds).toContain(ev1.id);
  });

  it("should filter deliveries by status and endpoint", async () => {
    const listRes = await fetch(
      `${apiBaseUrl}/v1/deliveries?status=success&endpointId=${endpointId}&limit=10`,
      { headers: { Authorization: `Bearer ${apiKey}` } }
    );
    expect(listRes.status).toBe(200);
    const body = await listRes.json();

    expect(Array.isArray(body.deliveries)).toBe(true);
    for (const d of body.deliveries) {
      expect(d.status).toBe("success");
      expect(d.endpointId).toBe(endpointId);
    }
  });

  it("BOSS FIGHT: Recover batch of DLQ deliveries via Bulk Replay", async () => {
    shouldFail = true; // Endpoint fails with 500

    const eventIds: string[] = [];
    const COUNT = 3;

    // Ingest 3 events that will exhaust retries into the DLQ
    for (let i = 1; i <= COUNT; i++) {
      const res = await fetch(`${apiBaseUrl}/v1/events`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify({
          eventType: `critical.fail.${i}`,
          payload: { batchId: i },
          idempotencyKey: `bulk_fail_${Date.now()}_${i}`,
        }),
      });
      const data = await res.json();
      eventIds.push(data.id);
    }

    const localPoller = new OutboxPoller();
    await localPoller.pollOnce();

    const jobs = await deliveryQueue.getJobs(["waiting", "active", "completed"]);
    const retryConfig = { baseMs: 50, maxDelayMs: 200, maxAttempts: 1 };

    // Process all 3 to exhaustion (maxAttempts: 1 with 500 status -> DLQ)
    for (const eid of eventIds) {
      const job = jobs.find((j) => j.data.eventId === eid);
      if (job) {
        await processDeliveryJob(job, 1500, retryConfig);
      }
    }

    // Verify all 3 are in DLQ
    const dlqRes = await fetch(`${apiBaseUrl}/v1/deliveries/dlq`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const dlqData = await dlqRes.json();
    expect(dlqData.dlq.length).toBeGreaterThanOrEqual(COUNT);

    // FIX THE ENDPOINT
    shouldFail = false;

    // INITIATE BULK REPLAY
    const bulkReplayRes = await fetch(`${apiBaseUrl}/v1/deliveries/dlq/bulk-replay`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({ endpointId }),
    });
    expect(bulkReplayRes.status).toBe(200);
    const bulkReplayData = await bulkReplayRes.json();
    expect(bulkReplayData.replayedCount).toBeGreaterThanOrEqual(COUNT);

    // Process replayed jobs
    const replayedJobs = await deliveryQueue.getJobs(["waiting", "active", "completed"]);
    for (const eid of eventIds) {
      const replayJob = replayedJobs.find(
        (j) => j.data.eventId === eid && String(j.data.outboxId).startsWith("bulk_replay_")
      );
      if (replayJob) {
        await processDeliveryJob(replayJob, 1500, retryConfig);
        expect(receivedWebhookIds).toContain(eid);
      }
    }
  });
});
