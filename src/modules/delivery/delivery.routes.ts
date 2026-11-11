import { Router, type Request, type Response } from "express";
import { eq, and, desc } from "drizzle-orm";
import { db } from "../../db/index.js";
import { deliveries, deliveryAttempts, events } from "../../db/schema.js";
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
