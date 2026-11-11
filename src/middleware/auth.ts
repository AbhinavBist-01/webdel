import type { Request, Response, NextFunction } from "express";
import { eq } from "drizzle-orm";
import { db } from "../db/index.js";
import { customers } from "../db/schema.js";
import { hashApiKey } from "../utils/crypto.js";

export async function authenticateCustomer(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  const authHeader = req.headers["authorization"];
  const customHeader = req.headers["x-api-key"];

  let apiKey: string | undefined;

  if (typeof authHeader === "string" && authHeader.startsWith("Bearer ")) {
    apiKey = authHeader.slice(7).trim();
  } else if (typeof customHeader === "string") {
    apiKey = customHeader.trim();
  }

  if (!apiKey) {
    res.status(401).json({
      error: "Unauthorized",
      message: "Missing API key in Authorization (Bearer) or x-api-key header",
    });
    return;
  }

  const hashedKey = hashApiKey(apiKey);

  try {
    const customer = await db
      .select({
        id: customers.id,
        name: customers.name,
      })
      .from(customers)
      .where(eq(customers.apiKeyHash, hashedKey))
      .limit(1)
      .then((rows) => rows[0]);

    if (!customer) {
      res.status(401).json({
        error: "Unauthorized",
        message: "Invalid API key",
      });
      return;
    }

    req.customer = customer;
    next();
  } catch (err) {
    next(err);
  }
}
