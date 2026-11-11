import { Router, type Request, type Response } from "express";
import { eq } from "drizzle-orm";
import { db } from "../../db/index.js";
import { endpoints } from "../../db/schema.js";
import { generateSigningSecret } from "../../utils/crypto.js";
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
