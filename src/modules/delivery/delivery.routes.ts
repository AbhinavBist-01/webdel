import { Router, type Request, type Response } from "express";
import { eq, and, desc, inArray } from "drizzle-orm";
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
 * List recent deliveries for the authenticated customer with optional status & endpoint filters.
 */
deliveryRouter.get("/", async (req: Request, res: Response) => {
  const customerId = req.customer!.id;
  const statusFilter = typeof req.query.status === "string" ? req.query.status : undefined;
  const endpointFilter = typeof req.query.endpointId === "string" ? req.query.endpointId : undefined;
  const limit = Math.min(100, Math.max(1, Number(req.query.limit) || 50));

  const conditions = [eq(events.customerId, customerId)];
  if (statusFilter) {
    conditions.push(eq(deliveries.status, statusFilter as any));
  }
  if (endpointFilter) {
    conditions.push(eq(deliveries.endpointId, endpointFilter));
  }

  const results = await db
    .select({
      id: deliveries.id,
      eventId: deliveries.eventId,
      endpointId: deliveries.endpointId,
      status: deliveries.status,
      sequenceNumber: deliveries.sequenceNumber,
      attemptCount: deliveries.attemptCount,
      lastResponseCode: deliveries.lastResponseCode,
      lastError: deliveries.lastError,
      createdAt: deliveries.createdAt,
      completedAt: deliveries.completedAt,
    })
    .from(deliveries)
    .innerJoin(events, eq(deliveries.eventId, events.id))
    .where(and(...conditions))
    .orderBy(desc(deliveries.createdAt))
    .limit(limit);

  res.status(200).json({ deliveries: results });
});

/**
 * POST /v1/deliveries/dlq/bulk-replay
 * Bulk replay all exhausted deliveries in the DLQ for this customer.
 */
deliveryRouter.post("/dlq/bulk-replay", async (req: Request, res: Response) => {
  const customerId = req.customer!.id;
  const endpointId = typeof req.body?.endpointId === "string" ? req.body.endpointId : undefined;

  const conditions = [
    eq(events.customerId, customerId),
    eq(deliveries.status, "exhausted"),
  ];
  if (endpointId) {
    conditions.push(eq(deliveries.endpointId, endpointId));
  }

  const exhaustedDeliveries = await db
    .select({
      id: deliveries.id,
      eventId: deliveries.eventId,
    })
    .from(deliveries)
    .innerJoin(events, eq(deliveries.eventId, events.id))
    .where(and(...conditions));

  if (exhaustedDeliveries.length === 0) {
    res.status(200).json({ message: "No exhausted deliveries found to replay", replayedCount: 0 });
    return;
  }

  const deliveryIds = exhaustedDeliveries.map((d) => d.id);

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
      .where(inArray(deliveries.id, deliveryIds));

    await tx
      .update(deadLetterEvents)
      .set({ replayedAt: new Date() })
      .where(inArray(deadLetterEvents.deliveryId, deliveryIds));
  });

  for (const del of exhaustedDeliveries) {
    await deliveryQueue.add("deliver", {
      eventId: del.eventId,
      outboxId: `bulk_replay_${del.id}_${Date.now()}`,
    });
  }

  res.status(200).json({
    message: "Bulk replay initiated successfully",
    replayedCount: exhaustedDeliveries.length,
  });
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
