import { Router, type Request, type Response } from "express";
import { db } from "../../db/index.js";
import { customers } from "../../db/schema.js";
import { generateApiKey, hashApiKey } from "../../utils/crypto.js";
import { authenticateCustomer } from "../../middleware/auth.js";
import { createCustomerSchema } from "./customer.schema.js";

export const customerRouter = Router();

/**
 * POST /v1/customers
 * Register a new customer and return their raw API key (one-time display).
 */
customerRouter.post("/", async (req: Request, res: Response) => {
  const { name } = createCustomerSchema.parse(req.body);

  const rawApiKey = generateApiKey();
  const apiKeyHash = hashApiKey(rawApiKey);

  const [customer] = await db
    .insert(customers)
    .values({
      name,
      apiKeyHash,
    })
    .returning({
      id: customers.id,
      name: customers.name,
      createdAt: customers.createdAt,
    });

  if (!customer) {
    throw new Error("Failed to create customer");
  }

  res.status(201).json({
    id: customer.id,
    name: customer.name,
    apiKey: rawApiKey,
    createdAt: customer.createdAt,
    warning: "Save this API key safely. You will not be able to see it again.",
  });
});

/**
 * GET /v1/customers/me
 * Protected endpoint to verify authentication.
 */
customerRouter.get(
  "/me",
  authenticateCustomer,
  (req: Request, res: Response) => {
    res.status(200).json({
      customer: req.customer,
    });
  }
);
