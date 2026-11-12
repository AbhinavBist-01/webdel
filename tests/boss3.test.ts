import { describe, it, expect, beforeAll, afterAll } from "vitest";
import http, { type Server as HttpServer } from "node:http";
import type { Server } from "node:http";
import { eq } from "drizzle-orm";
import { createApp } from "../src/app.js";
import { pool, db } from "../src/db/index.js";
import { deliveries, deliveryAttempts } from "../src/db/schema.js";
import { OutboxPoller } from "../src/services/outboxPoller.js";
import { deliveryQueue } from "../src/queue/deliveryQueue.js";
import { redisConnection } from "../src/queue/redis.js";
import { processDeliveryJob } from "../src/workers/deliveryWorker.js";
import { verifyWebhookSignature } from "../src/services/dispatcher.js";

describe("BOSS 3 FIGHT — Webhook Delivery Engine & Classification", () => {
  let apiServer: Server;
  let apiBaseUrl: string;

  let mockServer: HttpServer;
  let mockBaseUrl: string;

  let apiKey: string;
  let endpointSecrets: Record<string, string> = {};
  let signatureVerified = false;

  beforeAll(async () => {
    // 1. Start Webhook Platform API Server
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

    // 2. Start Mock Customer Endpoint Server with failure simulations
    mockServer = http.createServer(async (req, res) => {
      const url = req.url || "";

      if (url === "/webhook/success") {
        let body = "";
        req.on("data", (chunk) => (body += chunk));
        req.on("end", () => {
          const timestamp = Number(req.headers["webhook-timestamp"]);
          const sigHeader = (req.headers["webhook-signature"] as string) || "";
          const signature = sigHeader.replace("v1,", "");

          const secret = endpointSecrets["success"] || "";
          signatureVerified = verifyWebhookSignature(secret, timestamp, body, signature);

          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ received: true }));
        });
        return;
      }

      if (url === "/webhook/client-error") {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Invalid payload format" }));
        return;
      }

      if (url === "/webhook/server-error") {
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Database unavailable" }));
        return;
      }

      if (url === "/webhook/timeout") {
        // Hang request for 10 seconds (exceeds dispatcher timeout)
        setTimeout(() => {
          if (!res.writableEnded) {
            res.writeHead(200);
            res.end("too late");
          }
        }, 10000);
        return;
      }

      res.writeHead(404);
      res.end();
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

    // 3. Register Customer
    const custRes = await fetch(`${apiBaseUrl}/v1/customers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "Boss 3 Delivery Target" }),
    });
    const custData = await custRes.json();
    apiKey = custData.apiKey;

    // Clean queue
    await deliveryQueue.drain();
  });

  afterAll(async () => {
    await new Promise<void>((resolve) => apiServer.close(() => resolve()));
    await new Promise<void>((resolve) => mockServer.close(() => resolve()));
    await deliveryQueue.close();
    await redisConnection.quit();
    await pool.end();
  });

  it(
    "BOSS FIGHT: Classify 200, 400, 500, and Timeout outcomes accurately with HMAC verification",
    async () => {
    // 1. Register 4 endpoints targeting our mock server
    const endpointsToRegister = [
      { key: "success", path: "/webhook/success" },
      { key: "clientError", path: "/webhook/client-error" },
      { key: "serverError", path: "/webhook/server-error" },
      { key: "timeout", path: "/webhook/timeout" },
    ];

    const endpointIds: Record<string, string> = {};

    for (const ep of endpointsToRegister) {
      const epRes = await fetch(`${apiBaseUrl}/v1/endpoints`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify({ url: `${mockBaseUrl}${ep.path}` }),
      });
      const epData = await epRes.json();
      endpointIds[ep.key] = epData.id;
      endpointSecrets[ep.key] = epData.signingSecret;
    }

    // 2. Ingest an event
    const eventRes = await fetch(`${apiBaseUrl}/v1/events`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        eventType: "payment.succeeded",
        payload: { paymentId: "pay_boss_3", amount: 10000 },
        idempotencyKey: `boss3_event_${Date.now()}`,
      }),
    });
    const eventData = await eventRes.json();

    // 3. Run outbox poller to publish to BullMQ
    const poller = new OutboxPoller();
    const published = await poller.pollOnce();
    expect(published).toBe(1);

    // 4. Worker claims job from BullMQ and processes delivery
    const job = await deliveryQueue.getJob(eventData.id); // Or outboxId
    const jobs = await deliveryQueue.getJobs(["waiting", "active", "completed"]);
    const activeJob = jobs.find((j) => j.data.eventId === eventData.id);
    expect(activeJob).toBeDefined();

    // Execute worker delivery pipeline with 1.5s timeout for fast test execution
    await processDeliveryJob(activeJob!, 1500);

    // 5. VERIFY ALL 4 OUTCOMES

    // Outcome A: 200 OK (Success + HMAC Signature Verification)
    const [successDelivery] = await db
      .select()
      .from(deliveries)
      .where(eq(deliveries.endpointId, endpointIds["success"]!));

    expect(successDelivery).toBeDefined();
    expect(successDelivery?.status).toBe("success");
    expect(successDelivery?.lastResponseCode).toBe(200);
    expect(successDelivery?.completedAt).not.toBeNull();
    expect(signatureVerified).toBe(true); // HMAC signature matched!

    // Outcome B: 400 Client Error
    const [clientErrDelivery] = await db
      .select()
      .from(deliveries)
      .where(eq(deliveries.endpointId, endpointIds["clientError"]!));

    expect(clientErrDelivery).toBeDefined();
    expect(clientErrDelivery?.status).toBe("retry");
    expect(clientErrDelivery?.lastResponseCode).toBe(400);

    // Outcome C: 500 Server Error
    const [serverErrDelivery] = await db
      .select()
      .from(deliveries)
      .where(eq(deliveries.endpointId, endpointIds["serverError"]!));

    expect(serverErrDelivery).toBeDefined();
    expect(serverErrDelivery?.status).toBe("retry");
    expect(serverErrDelivery?.lastResponseCode).toBe(500);

    // Outcome D: Timeout
    const [timeoutDelivery] = await db
      .select()
      .from(deliveries)
      .where(eq(deliveries.endpointId, endpointIds["timeout"]!));

    expect(timeoutDelivery).toBeDefined();
    expect(timeoutDelivery?.status).toBe("retry");
    expect(timeoutDelivery?.lastError).toContain("timed out");

    // 6. Test Delivery Inspection API
    const inspectRes = await fetch(`${apiBaseUrl}/v1/deliveries/${successDelivery!.id}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    expect(inspectRes.status).toBe(200);
    const inspectData = await inspectRes.json();

    expect(inspectData.delivery.id).toBe(successDelivery!.id);
    expect(inspectData.attempts).toHaveLength(1);
    expect(inspectData.attempts[0].attemptNumber).toBe(1);
    expect(inspectData.attempts[0].responseCode).toBe(200);
    expect(inspectData.attempts[0].durationMs).toBeGreaterThan(0);
    expect(inspectData.attempts[0].requestHeaders["webhook-signature"]).toBeDefined();
  }, 15000);
});
