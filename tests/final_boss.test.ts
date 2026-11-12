import { describe, it, expect, beforeAll, afterAll } from "vitest";
import http, { type Server as HttpServer } from "node:http";
import type { Server } from "node:http";
import type { Worker } from "bullmq";
import { createApp } from "../src/app.js";
import { pool, db } from "../src/db/index.js";
import { deliveries, events } from "../src/db/schema.js";
import { OutboxPoller } from "../src/services/outboxPoller.js";
import { deliveryQueue, type DeliveryJobData } from "../src/queue/deliveryQueue.js";
import { redisConnection } from "../src/queue/redis.js";
import { createDeliveryWorker } from "../src/workers/deliveryWorker.js";

describe("👑 FINAL BOSS FIGHT — High-Throughput Load & Observability", () => {
  let apiServer: Server;
  let apiBaseUrl: string;

  let mockClusterServer: HttpServer;
  let mockClusterUrl: string;

  let customer1ApiKey: string;
  let customer2ApiKey: string;

  let endpoint1Id: string;
  let endpoint2Id: string;

  let ep2RequestCount = 0;
  const receivedClusterEvents: { endpoint: string; id: string }[] = [];

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

    // 2. Start Multi-Endpoint Mock Cluster
    mockClusterServer = http.createServer((req, res) => {
      const url = req.url || "";

      if (url === "/webhook/cluster-1") {
        // Reliable endpoint: Always 200 OK
        let body = "";
        req.on("data", (c) => (body += c));
        req.on("end", () => {
          const payload = JSON.parse(body);
          receivedClusterEvents.push({ endpoint: "cluster-1", id: payload.id });
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: true }));
        });
        return;
      }

      if (url === "/webhook/cluster-2") {
        // Flaky endpoint: Fails on first attempt (500), succeeds on retry (200)
        ep2RequestCount++;
        let body = "";
        req.on("data", (c) => (body += c));
        req.on("end", () => {
          const payload = JSON.parse(body);
          if (ep2RequestCount % 2 === 1) {
            res.writeHead(500, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ error: "Transient spike" }));
          } else {
            receivedClusterEvents.push({ endpoint: "cluster-2", id: payload.id });
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ ok: true }));
          }
        });
        return;
      }

      res.writeHead(404);
      res.end();
    });

    await new Promise<void>((resolve) => {
      mockClusterServer.listen(0, () => {
        const address = mockClusterServer.address();
        if (address && typeof address === "object") {
          mockClusterUrl = `http://localhost:${address.port}`;
        }
        resolve();
      });
    });

    // 3. Register Customer 1 & Reliable Endpoint
    const res1 = await fetch(`${apiBaseUrl}/v1/customers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "Cluster Customer 1" }),
    });
    const data1 = await res1.json();
    customer1ApiKey = data1.apiKey;

    const ep1Res = await fetch(`${apiBaseUrl}/v1/endpoints`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${customer1ApiKey}`,
      },
      body: JSON.stringify({
        url: `${mockClusterUrl}/webhook/cluster-1`,
        rateLimit: 100,
      }),
    });
    const ep1Data = await ep1Res.json();
    endpoint1Id = ep1Data.id;

    // 4. Register Customer 2 & Flaky Endpoint
    const res2 = await fetch(`${apiBaseUrl}/v1/customers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "Cluster Customer 2" }),
    });
    const data2 = await res2.json();
    customer2ApiKey = data2.apiKey;

    const ep2Res = await fetch(`${apiBaseUrl}/v1/endpoints`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${customer2ApiKey}`,
      },
      body: JSON.stringify({
        url: `${mockClusterUrl}/webhook/cluster-2`,
        rateLimit: 100,
      }),
    });
    const ep2Data = await ep2Res.json();
    endpoint2Id = ep2Data.id;

    await deliveryQueue.drain();
  });

  afterAll(async () => {
    poller?.stop();
    if (worker) {
      await worker.close();
    }
    await new Promise<void>((resolve) => apiServer.close(() => resolve()));
    await new Promise<void>((resolve) => mockClusterServer.close(() => resolve()));
    await deliveryQueue.close();
    await redisConnection.quit();
    await pool.end();
  });

  it("FINAL BOSS: High-concurrency load, transient fault tolerance, and Prometheus metrics validation", async () => {
    const TOTAL_C1_EVENTS = 30;
    const TOTAL_C2_EVENTS = 10;
    const TOTAL_EVENTS = TOTAL_C1_EVENTS + TOTAL_C2_EVENTS;

    // 1. Rapidly ingest concurrent events across both customers
    const ingestionPromises = [];

    // Customer 1 events
    for (let i = 1; i <= TOTAL_C1_EVENTS; i++) {
      ingestionPromises.push(
        fetch(`${apiBaseUrl}/v1/events`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${customer1ApiKey}`,
          },
          body: JSON.stringify({
            eventType: "customer1.transaction",
            payload: { txId: `tx_1_${i}`, amount: i * 100 },
            idempotencyKey: `final_c1_${Date.now()}_${i}`,
          }),
        })
      );
    }

    // Customer 2 events
    for (let i = 1; i <= TOTAL_C2_EVENTS; i++) {
      ingestionPromises.push(
        fetch(`${apiBaseUrl}/v1/events`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${customer2ApiKey}`,
          },
          body: JSON.stringify({
            eventType: "customer2.alert",
            payload: { alertId: `alert_${i}` },
            idempotencyKey: `final_c2_${Date.now()}_${i}`,
          }),
        })
      );
    }

    const ingestionResponses = await Promise.all(ingestionPromises);
    for (const r of ingestionResponses) {
      expect(r.status).toBe(201);
    }

    // 2. Start Outbox Poller and Worker Pool (Concurrency: 15)
    poller = new OutboxPoller({ batchSize: 50, intervalMs: 150 });
    poller.start();

    worker = createDeliveryWorker(15, {
      baseMs: 100,
      maxDelayMs: 400,
      maxAttempts: 5,
    });

    // 3. Wait for workers to process deliveries and resolve retries
    const startTime = Date.now();
    const maxWaitTime = 25000;

    while (
      receivedClusterEvents.length < TOTAL_EVENTS &&
      Date.now() - startTime < maxWaitTime
    ) {
      await new Promise((resolve) => setTimeout(resolve, 250));
    }

    // 4. Verify Delivery Success Guarantees
    const c1Delivered = receivedClusterEvents.filter((e) => e.endpoint === "cluster-1");
    const c2Delivered = receivedClusterEvents.filter((e) => e.endpoint === "cluster-2");

    expect(c1Delivered).toHaveLength(TOTAL_C1_EVENTS);
    expect(c2Delivered).toHaveLength(TOTAL_C2_EVENTS);

    // 5. Scrape Prometheus Metrics (/metrics)
    const metricsRes = await fetch(`${apiBaseUrl}/metrics`);
    expect(metricsRes.status).toBe(200);
    const metricsText = await metricsRes.text();

    // Verify Ingestion Metrics
    expect(metricsText).toContain("webhook_events_ingested_total");
    expect(metricsText).toContain('event_type="customer1.transaction"');
    expect(metricsText).toContain('event_type="customer2.alert"');

    // Verify Delivery Attempt Metrics (recorded 200s and transient 500s)
    expect(metricsText).toContain("webhook_delivery_attempts_total");
    expect(metricsText).toContain('outcome="success"');
    expect(metricsText).toContain('response_code="200"');

    // Verify Latency Histogram
    expect(metricsText).toContain("webhook_delivery_duration_seconds_bucket");
    expect(metricsText).toContain("webhook_delivery_duration_seconds_count");

    // 6. Verify Database Audit State
    const allDeliveries = await db.select().from(deliveries);
    const successDeliveries = allDeliveries.filter((d) => d.status === "success");
    expect(successDeliveries.length).toBeGreaterThanOrEqual(TOTAL_EVENTS);
  }, 30000);
});
