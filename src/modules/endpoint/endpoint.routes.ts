import { Router, type Request, type Response } from "express";
import { eq, and, inArray } from "drizzle-orm";
import { db } from "../../db/index.js";
import { endpoints, deliveries } from "../../db/schema.js";
import { generateSigningSecret } from "../../utils/crypto.js";
import { deliveryQueue } from "../../queue/deliveryQueue.js";
import { authenticateCustomer } from "../../middleware/auth.js";
import { createEndpointSchema } from "./endpoint.schema.js";

export const endpointRouter = Router();

// Protect all endpoint routes
endpointRouter.use(authenticateCustomer);

/**
 * POST /v1/endpoints
 * Register a new webhook endpoint for the authenticated customer.
 */
endpointRouter.post("/", async (req: Request, res: Response) => {
  const { url, rateLimit } = createEndpointSchema.parse(req.body);
  const customerId = req.customer!.id;

  const signingSecret = generateSigningSecret();

  const [endpoint] = await db
    .insert(endpoints)
    .values({
      customerId,
      url,
      signingSecret,
      rateLimit,
      status: "active",
    })
    .returning({
      id: endpoints.id,
      customerId: endpoints.customerId,
      url: endpoints.url,
      signingSecret: endpoints.signingSecret,
      status: endpoints.status,
      rateLimit: endpoints.rateLimit,
      createdAt: endpoints.createdAt,
    });

  if (!endpoint) {
    throw new Error("Failed to register endpoint");
  }

  res.status(201).json(endpoint);
});

/**
 * GET /v1/endpoints
 * List all endpoints belonging to the authenticated customer.
 */
endpointRouter.get("/", async (req: Request, res: Response) => {
  const customerId = req.customer!.id;

  const customerEndpoints = await db
    .select({
      id: endpoints.id,
      customerId: endpoints.customerId,
      url: endpoints.url,
      signingSecret: endpoints.signingSecret,
      status: endpoints.status,
      rateLimit: endpoints.rateLimit,
      createdAt: endpoints.createdAt,
    })
    .from(endpoints)
    .where(eq(endpoints.customerId, customerId));

  res.status(200).json({
    endpoints: customerEndpoints,
  });
});

/**
 * PATCH /v1/endpoints/:id/pause
 * Pause an endpoint (status = 'disabled') to hold incoming webhook dispatches.
 */
endpointRouter.patch("/:id/pause", async (req: Request, res: Response) => {
  const customerId = req.customer!.id;
  const { id } = req.params;

  if (!id || typeof id !== "string") {
    res.status(400).json({ error: "BadRequest", message: "Missing endpoint ID" });
    return;
  }

  const [updated] = await db
    .update(endpoints)
    .set({ status: "disabled" })
    .where(and(eq(endpoints.id, id), eq(endpoints.customerId, customerId)))
    .returning();

  if (!updated) {
    res.status(404).json({ error: "NotFound", message: "Endpoint not found" });
    return;
  }

  res.status(200).json({
    message: "Endpoint paused successfully",
    endpoint: updated,
  });
});

/**
 * PATCH /v1/endpoints/:id/resume
 * Resume an endpoint (status = 'active') and re-enqueue all held deliveries.
 */
endpointRouter.patch("/:id/resume", async (req: Request, res: Response) => {
  const customerId = req.customer!.id;
  const { id } = req.params;

  if (!id || typeof id !== "string") {
    res.status(400).json({ error: "BadRequest", message: "Missing endpoint ID" });
    return;
  }

  const [updated] = await db
    .update(endpoints)
    .set({ status: "active" })
    .where(and(eq(endpoints.id, id), eq(endpoints.customerId, customerId)))
    .returning();

  if (!updated) {
    res.status(404).json({ error: "NotFound", message: "Endpoint not found" });
    return;
  }

  // Find all deliveries for this endpoint that were held while disabled
  const pendingDeliveries = await db
    .select({
      id: deliveries.id,
      eventId: deliveries.eventId,
    })
    .from(deliveries)
    .where(
      and(
        eq(deliveries.endpointId, updated.id),
        inArray(deliveries.status, ["pending", "retry"])
      )
    );

  // Re-enqueue all held deliveries
  for (const del of pendingDeliveries) {
    await deliveryQueue.add("deliver", {
      eventId: del.eventId,
      outboxId: `resume_${del.id}_${Date.now()}`,
    });
  }

  res.status(200).json({
    message: "Endpoint resumed successfully",
    endpoint: updated,
    requeuedCount: pendingDeliveries.length,
  });
});
