import { describe, it, expect, beforeAll, afterAll } from "vitest";
import type { Server } from "node:http";
import { eq, inArray, sql } from "drizzle-orm";
import { createApp } from "../src/app.js";
import { pool, db } from "../src/db/index.js";
import { outbox, events } from "../src/db/schema.js";
import { OutboxPoller } from "../src/services/outboxPoller.js";
import { deliveryQueue } from "../src/queue/deliveryQueue.js";
import { redisConnection } from "../src/queue/redis.js";

describe("BOSS 2 FIGHT — Transactional Outbox & Crash Recovery", () => {
  let server: Server;
  let baseUrl: string;
  let apiKey: string;

  beforeAll(async () => {
    const app = createApp();
    await new Promise<void>((resolve) => {
      server = app.listen(0, () => {
        const address = server.address();
        if (address && typeof address === "object") {
          baseUrl = `http://localhost:${address.port}`;
        }
        resolve();
      });
    });

    // Create test customer
    const res = await fetch(`${baseUrl}/v1/customers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "Boss 2 Survivor" }),
    });
    const data = await res.json();
    apiKey = data.apiKey;

    // Clean queue before test
    await deliveryQueue.drain();
  });

  afterAll(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await deliveryQueue.close();
    await redisConnection.quit();
    await pool.end();
  });

  it("BOSS FIGHT: Event committed -> server crashes -> Redis never received event -> server restarts -> event eventually published", async () => {
    // 1. Ingest 5 events while outbox poller is NOT running (simulating failure between DB commit and queue dispatch)
    const eventIds: string[] = [];
    const outboxIds: string[] = [];

    for (let i = 1; i <= 5; i++) {
      const res = await fetch(`${baseUrl}/v1/events`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify({
          eventType: `order.step.${i}`,
          payload: { step: i, orderId: "boss_2_order" },
          idempotencyKey: `boss2_idem_${Date.now()}_${i}`,
        }),
      });

      expect(res.status).toBe(201);
      const body = await res.json();
      eventIds.push(body.id);
    }

    // 2. Verify all 5 events are committed to PostgreSQL outbox as 'pending'
    const pendingOutboxRows = await db
      .select()
      .from(outbox)
      .where(inArray(outbox.eventId, eventIds));

    expect(pendingOutboxRows).toHaveLength(5);
    for (const row of pendingOutboxRows) {
      expect(row.status).toBe("pending");
      expect(row.publishedAt).toBeNull();
      outboxIds.push(row.id);
    }

    // 3. Verify Redis has NOT received these events yet
    for (const outboxId of outboxIds) {
      const job = await deliveryQueue.getJob(outboxId);
      expect(job).toBeUndefined();
    }

    // 4. SIMULATE SERVER CRASH & RESTART
    // Stop the old server
    await new Promise<void>((resolve) => server.close(() => resolve()));

    // Reboot a fresh server instance and start the Outbox Poller
    const newApp = createApp();
    await new Promise<void>((resolve) => {
      server = newApp.listen(0, () => {
        const address = server.address();
        if (address && typeof address === "object") {
          baseUrl = `http://localhost:${address.port}`;
        }
        resolve();
      });
    });

    const recoveredPoller = new OutboxPoller({ batchSize: 50 });

    // 5. Poller runs after reboot
    const publishedCount = await recoveredPoller.pollOnce();
    expect(publishedCount).toBe(5);

    // 6. Verify that ALL 5 events are now marked 'published' in PostgreSQL
    const finalOutboxRows = await db
      .select()
      .from(outbox)
      .where(inArray(outbox.id, outboxIds));

    expect(finalOutboxRows).toHaveLength(5);
    for (const row of finalOutboxRows) {
      expect(row.status).toBe("published");
      expect(row.publishedAt).not.toBeNull();
    }

    // 7. Verify that ALL 5 jobs are now safely present in Redis BullMQ
    for (const row of finalOutboxRows) {
      const job = await deliveryQueue.getJob(row.id);
      expect(job).not.toBeNull();
      expect(job?.data.eventId).toBe(row.eventId);
      expect(job?.data.outboxId).toBe(row.id);
    }
  });

  it("should survive worker crash during 'processing' state and recover automatically", async () => {
    // 1. Ingest an event
    const res = await fetch(`${baseUrl}/v1/events`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        eventType: "critical.system.event",
        payload: { state: "unstable" },
        idempotencyKey: `crash_recovery_${Date.now()}`,
      }),
    });
    const body = await res.json();

    const [outboxRow] = await db
      .select()
      .from(outbox)
      .where(eq(outbox.eventId, body.id));

    // 2. Simulate worker crash: record locked in 'processing' and stalled for 15 seconds
    await db.execute(sql`
      UPDATE outbox
      SET status = 'processing',
          created_at = NOW() - INTERVAL '15 seconds'
      WHERE id = ${outboxRow!.id}
    `);

    // 3. Re-initialized poller recovers and completes delivery
    const poller = new OutboxPoller({ stuckThresholdSeconds: 5 });
    const recoveredCount = await poller.recoverStuckRecords();
    expect(recoveredCount).toBeGreaterThanOrEqual(1);

    const published = await poller.pollOnce();
    expect(published).toBeGreaterThanOrEqual(1);

    const [finalRow] = await db
      .select()
      .from(outbox)
      .where(eq(outbox.id, outboxRow!.id));

    expect(finalRow?.status).toBe("published");

    const job = await deliveryQueue.getJob(outboxRow!.id);
    expect(job).not.toBeNull();
  });
});
