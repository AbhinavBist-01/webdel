import { describe, it, expect, beforeAll, afterAll } from "vitest";
import type { Server } from "node:http";
import { eq, sql } from "drizzle-orm";
import { createApp } from "../src/app.js";
import { pool, db } from "../src/db/index.js";
import { outbox, events } from "../src/db/schema.js";
import { OutboxPoller } from "../src/services/outboxPoller.js";
import { deliveryQueue } from "../src/queue/deliveryQueue.js";
import { redisConnection } from "../src/queue/redis.js";

describe("Milestones 8 & 9: Outbox Poller & BullMQ Queue Publishing", () => {
  let server: Server;
  let baseUrl: string;
  let apiKey: string;
  let poller: OutboxPoller;

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
      body: JSON.stringify({ name: "Outbox Tester" }),
    });
    const data = await res.json();
    apiKey = data.apiKey;

    poller = new OutboxPoller({ batchSize: 10, stuckThresholdSeconds: 5 });

    // Clean queue before testing
    await deliveryQueue.drain();
  });

  afterAll(async () => {
    poller.stop();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await deliveryQueue.close();
    await redisConnection.quit();
    await pool.end();
  });

  it("should process pending outbox records and publish them to BullMQ", async () => {
    // 1. Ingest event
    const idempotencyKey = `outbox_test_${Date.now()}`;
    const postRes = await fetch(`${baseUrl}/v1/events`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        eventType: "invoice.paid",
        payload: { invoiceId: "inv_123", amount: 1500 },
        idempotencyKey,
      }),
    });
    const event = await postRes.json();

    // 2. Verify outbox row exists in 'pending' status
    const [initialOutbox] = await db
      .select()
      .from(outbox)
      .where(eq(outbox.eventId, event.id));

    expect(initialOutbox).toBeDefined();
    expect(initialOutbox?.status).toBe("pending");
    expect(initialOutbox?.publishedAt).toBeNull();

    // 3. Trigger outbox poller once
    const publishedCount = await poller.pollOnce();
    expect(publishedCount).toBeGreaterThanOrEqual(1);

    // 4. Verify outbox row in Postgres is now 'published'
    const [updatedOutbox] = await db
      .select()
      .from(outbox)
      .where(eq(outbox.id, initialOutbox!.id));

    expect(updatedOutbox?.status).toBe("published");
    expect(updatedOutbox?.publishedAt).not.toBeNull();

    // 5. Verify the job is present in BullMQ / Redis
    const job = await deliveryQueue.getJob(initialOutbox!.id);
    expect(job).not.toBeNull();
    expect(job?.data.eventId).toBe(event.id);
    expect(job?.data.outboxId).toBe(initialOutbox!.id);
  });

  it("should recover stuck outbox records after a simulated crash", async () => {
    // 1. Insert an outbox record that is stuck in 'processing'
    const idempotencyKey = `crash_sim_${Date.now()}`;
    const postRes = await fetch(`${baseUrl}/v1/events`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        eventType: "order.cancelled",
        payload: { orderId: "ord_crash" },
        idempotencyKey,
      }),
    });
    const event = await postRes.json();

    const [outboxRow] = await db
      .select()
      .from(outbox)
      .where(eq(outbox.eventId, event.id));

    // Simulate crash: record was claimed ('processing') and backdated past threshold
    await db.execute(sql`
      UPDATE outbox
      SET status = 'processing',
          created_at = NOW() - INTERVAL '10 seconds'
      WHERE id = ${outboxRow!.id}
    `);

    // 2. Run recovery
    const recovered = await poller.recoverStuckRecords();
    expect(recovered).toBeGreaterThanOrEqual(1);

    // 3. Verify it was reset to 'pending'
    const [recoveredRow] = await db
      .select()
      .from(outbox)
      .where(eq(outbox.id, outboxRow!.id));

    expect(recoveredRow?.status).toBe("pending");

    // 4. Poller now successfully processes it
    const published = await poller.pollOnce();
    expect(published).toBeGreaterThanOrEqual(1);

    const [finalRow] = await db
      .select()
      .from(outbox)
      .where(eq(outbox.id, outboxRow!.id));

    expect(finalRow?.status).toBe("published");
  });
});
