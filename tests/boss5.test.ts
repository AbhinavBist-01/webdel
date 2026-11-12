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

describe("BOSS 5 FIGHT — Strict Per-Endpoint Ordering & Concurrency", () => {
  let apiServer: Server;
  let apiBaseUrl: string;

  let mockServer: HttpServer;
  let mockBaseUrl: string;

  let customerAApiKey: string;
  let customerBApiKey: string;

  // Track the exact arrival order of HTTP requests at the customer endpoints
  const receivedSequenceA: number[] = [];
  const receivedSequenceB: number[] = [];

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

    // 2. Start Mock Receiver Server that captures receipt sequence
    mockServer = http.createServer((req, res) => {
      let body = "";
      req.on("data", (chunk) => (body += chunk));
      req.on("end", () => {
        const payload = JSON.parse(body);
        const seq = payload.data?.seq;

        if (req.url === "/webhook/customer-a") {
          receivedSequenceA.push(seq);
        } else if (req.url === "/webhook/customer-b") {
          receivedSequenceB.push(seq);
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

    // 3. Register Customer A and Endpoint A
    const resA = await fetch(`${apiBaseUrl}/v1/customers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "Ordered Customer A" }),
    });
    const dataA = await resA.json();
    customerAApiKey = dataA.apiKey;

    await fetch(`${apiBaseUrl}/v1/endpoints`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${customerAApiKey}`,
      },
      body: JSON.stringify({ url: `${mockBaseUrl}/webhook/customer-a` }),
    });

    // 4. Register Customer B and Endpoint B
    const resB = await fetch(`${apiBaseUrl}/v1/customers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "Independent Customer B" }),
    });
    const dataB = await resB.json();
    customerBApiKey = dataB.apiKey;

    await fetch(`${apiBaseUrl}/v1/endpoints`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${customerBApiKey}`,
      },
      body: JSON.stringify({ url: `${mockBaseUrl}/webhook/customer-b` }),
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

  it("BOSS FIGHT: Deliver ordered events concurrently for Customer A while Customer B processes independently", async () => {
    const TOTAL_EVENTS_A = 25;
    const TOTAL_EVENTS_B = 5;

    // 1. Concurrently ingest 25 sequential events for Customer A
    // (Simulating payment.created -> payment.authorized -> payment.captured, etc.)
    for (let seq = 1; seq <= TOTAL_EVENTS_A; seq++) {
      await fetch(`${apiBaseUrl}/v1/events`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${customerAApiKey}`,
        },
        body: JSON.stringify({
          eventType: `order.event.${seq}`,
          payload: { orderId: "ord_ordered_100", seq },
          idempotencyKey: `ord_a_${Date.now()}_${seq}`,
        }),
      });
    }

    // 2. Concurrently ingest 5 events for Customer B
    for (let seq = 1; seq <= TOTAL_EVENTS_B; seq++) {
      await fetch(`${apiBaseUrl}/v1/events`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${customerBApiKey}`,
        },
        body: JSON.stringify({
          eventType: `b.event.${seq}`,
          payload: { account: "b_acc", seq },
          idempotencyKey: `ord_b_${Date.now()}_${seq}`,
        }),
      });
    }

    // 3. Start Outbox Poller and a concurrent Worker pool (concurrency: 10)
    poller = new OutboxPoller({ batchSize: 50, intervalMs: 200 });
    poller.start();

    worker = createDeliveryWorker(10);

    // 4. Wait until all events for Customer A and Customer B are received
    const maxWaitTime = 20000;
    const startTime = Date.now();

    while (
      (receivedSequenceA.length < TOTAL_EVENTS_A ||
        receivedSequenceB.length < TOTAL_EVENTS_B) &&
      Date.now() - startTime < maxWaitTime
    ) {
      await new Promise((resolve) => setTimeout(resolve, 200));
    }

    // 5. VERIFY STRICT ORDERING FOR CUSTOMER A
    expect(receivedSequenceA).toHaveLength(TOTAL_EVENTS_A);

    // Verify each received sequence arrived in exact monotonic order: [1, 2, 3, ..., 25]
    for (let i = 0; i < TOTAL_EVENTS_A; i++) {
      expect(receivedSequenceA[i]).toBe(i + 1);
    }

    // 6. VERIFY CUSTOMER B WAS DELIVERED INDEPENDENTLY
    expect(receivedSequenceB).toHaveLength(TOTAL_EVENTS_B);
  }, 25000);
});
