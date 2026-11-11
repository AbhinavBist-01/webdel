import { describe, it, expect, beforeAll, afterAll } from "vitest";
import type { Server } from "node:http";
import { eq } from "drizzle-orm";
import { createApp } from "../src/app.js";
import { pool, db } from "../src/db/index.js";
import { events, outbox } from "../src/db/schema.js";

describe("BOSS 1 FIGHT — Concurrent Event Ingestion & Idempotency", () => {
  let server: Server;
  let baseUrl: string;
  let apiKey: string;
  let customerId: string;

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

    // Create a test customer
    const res = await fetch(`${baseUrl}/v1/customers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "Boss 1 Challenger" }),
    });
    const data = await res.json();
    apiKey = data.apiKey;
    customerId = data.id;
  });

  afterAll(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await pool.end();
  });

  it("BOSS FIGHT: Send the same event 100 times concurrently with the same idempotency key", async () => {
    const idempotencyKey = `idem_boss1_${Date.now()}`;
    const payload = {
      orderId: "order_999",
      amount: 4999,
      currency: "INR",
    };

    // Fire 100 concurrent HTTP requests
    const CONCURRENCY = 100;
    const requests = Array.from({ length: CONCURRENCY }, () =>
      fetch(`${baseUrl}/v1/events`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify({
          eventType: "payment.created",
          payload,
          idempotencyKey,
        }),
      })
    );

    const responses = await Promise.all(requests);

    // 1. All 100 requests must succeed with 2xx status codes (200 or 201)
    const statuses = responses.map((r) => r.status);
    const non2xx = statuses.filter((s) => s !== 200 && s !== 201);
    expect(non2xx).toHaveLength(0);

    // 2. Parse all response bodies
    const bodies = await Promise.all(responses.map((r) => r.json()));

    // 3. Exactly ONE request should be the creator (201, isDuplicate: false)
    const createdCount = statuses.filter((s) => s === 201).length;
    const duplicateCount = statuses.filter((s) => s === 200).length;

    expect(createdCount).toBe(1);
    expect(duplicateCount).toBe(CONCURRENCY - 1);

    // 4. All 100 responses must return the EXACT same event ID
    const eventIds = new Set(bodies.map((b) => b.id));
    expect(eventIds.size).toBe(1);

    const uniqueEventId = Array.from(eventIds)[0];
    expect(uniqueEventId).toBeDefined();

    // 5. Query PostgreSQL directly to verify only 1 physical row was created
    const rows = await db
      .select()
      .from(events)
      .where(eq(events.idempotencyKey, idempotencyKey));

    expect(rows).toHaveLength(1);
    expect(rows[0]?.id).toBe(uniqueEventId);
    expect(rows[0]?.customerId).toBe(customerId);
    expect(rows[0]?.eventType).toBe("payment.created");

    // 6. Verify Outbox Table: Exactly ONE outbox entry exists with status 'pending'
    const outboxRows = await db
      .select()
      .from(outbox)
      .where(eq(outbox.eventId, uniqueEventId));

    expect(outboxRows).toHaveLength(1);
    expect(outboxRows[0]?.status).toBe("pending");
  });

  it("should retrieve the created event via GET /v1/events/:id", async () => {
    // Ingest an event
    const postRes = await fetch(`${baseUrl}/v1/events`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        eventType: "customer.subscription.renewed",
        payload: { tier: "enterprise" },
        idempotencyKey: `sub_event_${Date.now()}`,
      }),
    });
    const createdEvent = await postRes.json();

    // Fetch the event
    const getRes = await fetch(`${baseUrl}/v1/events/${createdEvent.id}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    expect(getRes.status).toBe(200);

    const fetched = await getRes.json();
    expect(fetched.event.id).toBe(createdEvent.id);
    expect(fetched.event.eventType).toBe("customer.subscription.renewed");
  });

  it("should allow another customer to use the same idempotency key without collision", async () => {
    // Register second customer
    const res2 = await fetch(`${baseUrl}/v1/customers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "Boss 1 Challenger 2" }),
    });
    const { apiKey: apiKey2, id: customerId2 } = await res2.json();

    const sharedKey = `shared_key_${Date.now()}`;

    // Customer 1 sends event
    const r1 = await fetch(`${baseUrl}/v1/events`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        eventType: "order.placed",
        payload: { user: "cust1" },
        idempotencyKey: sharedKey,
      }),
    });
    const b1 = await r1.json();
    expect(r1.status).toBe(201);

    // Customer 2 sends event with SAME idempotency key
    const r2 = await fetch(`${baseUrl}/v1/events`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey2}`,
      },
      body: JSON.stringify({
        eventType: "order.placed",
        payload: { user: "cust2" },
        idempotencyKey: sharedKey,
      }),
    });
    const b2 = await r2.json();
    expect(r2.status).toBe(201);

    // Must be two distinct events in different customer namespaces
    expect(b1.id).not.toBe(b2.id);
    expect(b1.customerId).toBe(customerId);
    expect(b2.customerId).toBe(customerId2);
  });
});
