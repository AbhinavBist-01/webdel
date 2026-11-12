import { Router, type Request, type Response } from "express";
import { eq, and, desc } from "drizzle-orm";
import { db } from "../../db/index.js";
import {
  deliveries,
  deliveryAttempts,
  deadLetterEvents,
  events,
} from "../../db/schema.js";
import { deliveryQueue } from "../../queue/deliveryQueue.js";
import { authenticateCustomer } from "../../middleware/auth.js";

export const deliveryRouter = Router();

// Protect all delivery inspection routes
deliveryRouter.use(authenticateCustomer);

/**
 * GET /v1/deliveries
 * List recent deliveries for the authenticated customer.
 */
deliveryRouter.get("/", async (req: Request, res: Response) => {
  const customerId = req.customer!.id;

  const results = await db
    .select({
      id: deliveries.id,
      eventId: deliveries.eventId,
      endpointId: deliveries.endpointId,
      status: deliveries.status,
      attemptCount: deliveries.attemptCount,
      lastResponseCode: deliveries.lastResponseCode,
      lastError: deliveries.lastError,
      createdAt: deliveries.createdAt,
      completedAt: deliveries.completedAt,
    })
    .from(deliveries)
    .innerJoin(events, eq(deliveries.eventId, events.id))
    .where(eq(events.customerId, customerId))
    .orderBy(desc(deliveries.createdAt))
    .limit(50);

  res.status(200).json({ deliveries: results });
});

/**
 * GET /v1/deliveries/dlq
 * List all exhausted/dead-letter deliveries for the authenticated customer.
 */
deliveryRouter.get("/dlq", async (req: Request, res: Response) => {
  const customerId = req.customer!.id;

  const dlqRecords = await db
    .select({
      deliveryId: deliveries.id,
      eventId: deliveries.eventId,
      endpointId: deliveries.endpointId,
      status: deliveries.status,
      attemptCount: deliveries.attemptCount,
      lastResponseCode: deliveries.lastResponseCode,
      lastError: deliveries.lastError,
      failedAt: deadLetterEvents.failedAt,
      replayedAt: deadLetterEvents.replayedAt,
      reason: deadLetterEvents.reason,
    })
    .from(deadLetterEvents)
    .innerJoin(deliveries, eq(deadLetterEvents.deliveryId, deliveries.id))
    .innerJoin(events, eq(deliveries.eventId, events.id))
    .where(eq(events.customerId, customerId))
    .orderBy(desc(deadLetterEvents.failedAt))
    .limit(50);

  res.status(200).json({ dlq: dlqRecords });
});

/**
 * GET /v1/deliveries/:id
 * Retrieve a specific delivery and all its attempt logs.
 */
deliveryRouter.get("/:id", async (req: Request, res: Response) => {
  const customerId = req.customer!.id;
  const { id } = req.params;

  if (!id || typeof id !== "string") {
    res.status(400).json({ error: "BadRequest", message: "Missing or invalid delivery ID" });
    return;
  }

  // Find delivery and verify ownership via event customerId
  const [delivery] = await db
    .select({
      id: deliveries.id,
      eventId: deliveries.eventId,
      endpointId: deliveries.endpointId,
      status: deliveries.status,
      attemptCount: deliveries.attemptCount,
      lastResponseCode: deliveries.lastResponseCode,
      lastError: deliveries.lastError,
      createdAt: deliveries.createdAt,
      completedAt: deliveries.completedAt,
    })
    .from(deliveries)
    .innerJoin(events, eq(deliveries.eventId, events.id))
    .where(and(eq(deliveries.id, id), eq(events.customerId, customerId)))
    .limit(1);

  if (!delivery) {
    res.status(404).json({ error: "NotFound", message: "Delivery not found" });
    return;
  }

  // Fetch all attempts for this delivery
  const attempts = await db
    .select({
      id: deliveryAttempts.id,
      attemptNumber: deliveryAttempts.attemptNumber,
      requestHeaders: deliveryAttempts.requestHeaders,
      responseCode: deliveryAttempts.responseCode,
      responseBody: deliveryAttempts.responseBody,
      durationMs: deliveryAttempts.durationMs,
      error: deliveryAttempts.error,
      createdAt: deliveryAttempts.createdAt,
    })
    .from(deliveryAttempts)
    .where(eq(deliveryAttempts.deliveryId, delivery.id))
    .orderBy(deliveryAttempts.attemptNumber);

  res.status(200).json({
    delivery,
    attempts,
  });
});

/**
 * POST /v1/deliveries/:id/replay
 * Replay an exhausted or failed delivery from the DLQ.
 */
deliveryRouter.post("/:id/replay", async (req: Request, res: Response) => {
  const customerId = req.customer!.id;
  const { id } = req.params;

  if (!id || typeof id !== "string") {
    res.status(400).json({ error: "BadRequest", message: "Missing or invalid delivery ID" });
    return;
  }

  // Find delivery and verify customer ownership
  const [delivery] = await db
    .select({
      id: deliveries.id,
      eventId: deliveries.eventId,
      endpointId: deliveries.endpointId,
      status: deliveries.status,
    })
    .from(deliveries)
    .innerJoin(events, eq(deliveries.eventId, events.id))
    .where(and(eq(deliveries.id, id), eq(events.customerId, customerId)))
    .limit(1);

  if (!delivery) {
    res.status(404).json({ error: "NotFound", message: "Delivery not found" });
    return;
  }

  // Reset delivery record and record replay in deadLetterEvents
  await db.transaction(async (tx) => {
    await tx
      .update(deliveries)
      .set({
        status: "pending",
        attemptCount: 0,
        nextAttemptAt: null,
        completedAt: null,
        lastError: null,
        lastResponseCode: null,
      })
      .where(eq(deliveries.id, delivery.id));

    await tx
      .update(deadLetterEvents)
      .set({ replayedAt: new Date() })
      .where(eq(deadLetterEvents.deliveryId, delivery.id));
  });

  // Re-enqueue delivery job to BullMQ immediately
  await deliveryQueue.add("deliver", {
    eventId: delivery.eventId,
    outboxId: `replay_${delivery.id}_${Date.now()}`,
  });

  res.status(200).json({
    message: "Delivery re-enqueued for replay successfully",
    deliveryId: delivery.id,
  });
});
