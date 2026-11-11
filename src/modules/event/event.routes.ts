import { Router, type Request, type Response } from "express";
import { and, eq } from "drizzle-orm";
import { db } from "../../db/index.js";
import { events, outbox } from "../../db/schema.js";
import { authenticateCustomer } from "../../middleware/auth.js";
import { createEventSchema } from "./event.schema.js";

export const eventRouter = Router();

// Protect all event routes
eventRouter.use(authenticateCustomer);

/**
 * POST /v1/events
 * Ingests an event durably and prevents duplicate event creation using an idempotency key.
 */
eventRouter.post("/", async (req: Request, res: Response) => {
  const parsedBody = createEventSchema.parse(req.body);
  const customerId = req.customer!.id;

  // Extract idempotency key from body or standard HTTP headers
  const headerKey =
    req.headers["idempotency-key"] || req.headers["x-idempotency-key"];
  const idempotencyKey =
    parsedBody.idempotencyKey || (typeof headerKey === "string" ? headerKey.trim() : undefined);

  if (!idempotencyKey) {
    res.status(400).json({
      error: "ValidationError",
      message: "Idempotency key is required in body or Idempotency-Key header",
    });
    return;
  }

  // Atomic transaction: Insert event + outbox entry together
  const result = await db.transaction(async (tx) => {
    const [inserted] = await tx
      .insert(events)
      .values({
        customerId,
        eventType: parsedBody.eventType,
        payload: parsedBody.payload,
        idempotencyKey,
      })
      .onConflictDoNothing({
        target: [events.customerId, events.idempotencyKey],
      })
      .returning({
        id: events.id,
        customerId: events.customerId,
        eventType: events.eventType,
        payload: events.payload,
        idempotencyKey: events.idempotencyKey,
        createdAt: events.createdAt,
      });

    if (inserted) {
      // Transactional Outbox: Record pending dispatch entry in the same transaction
      await tx.insert(outbox).values({
        eventId: inserted.id,
        status: "pending",
      });

      return {
        event: inserted,
        isDuplicate: false,
        status: 201 as const,
      };
    }

    // If conflict occurred, query existing event (do not insert duplicate outbox record)
    const [existing] = await tx
      .select({
        id: events.id,
        customerId: events.customerId,
        eventType: events.eventType,
        payload: events.payload,
        idempotencyKey: events.idempotencyKey,
        createdAt: events.createdAt,
      })
      .from(events)
      .where(
        and(
          eq(events.customerId, customerId),
          eq(events.idempotencyKey, idempotencyKey)
        )
      )
      .limit(1);

    if (!existing) {
      throw new Error("Concurrency anomaly: record conflicted but could not be read");
    }

    return {
      event: existing,
      isDuplicate: true,
      status: 200 as const,
    };
  });

  res.status(result.status).json({
    ...result.event,
    isDuplicate: result.isDuplicate,
  });
});

/**
 * GET /v1/events/:id
 * Retrieve a previously ingested event by ID (isolated by customer).
 */
eventRouter.get("/:id", async (req: Request, res: Response) => {
  const customerId = req.customer!.id;
  const { id } = req.params;
  if (!id || typeof id !== "string") {
    res.status(400).json({ error: "BadRequest", message: "Missing or invalid id parameter" });
    return;
  }

  const [event] = await db
    .select({
      id: events.id,
      customerId: events.customerId,
      eventType: events.eventType,
      payload: events.payload,
      idempotencyKey: events.idempotencyKey,
      createdAt: events.createdAt,
    })
    .from(events)
    .where(and(eq(events.id, id), eq(events.customerId, customerId)))
    .limit(1);

  if (!event) {
    res.status(404).json({
      error: "NotFound",
      message: "Event not found",
    });
    return;
  }

  res.status(200).json({ event });
});
