import express, { type Express } from "express";
import { customerRouter } from "./modules/customer/index.js";
import { endpointRouter } from "./modules/endpoint/index.js";
import { eventRouter } from "./modules/event/index.js";
import { deliveryRouter } from "./modules/delivery/index.js";
import { errorHandler } from "./middleware/errorHandler.js";

export function createApp(): Express {
  const app = express();

  // Parse JSON request bodies
  app.use(express.json());

  // Healthcheck endpoint
  app.get("/health", (_req, res) => {
    res.status(200).json({ status: "ok" });
  });

  // Mount API modules
  app.use("/v1/customers", customerRouter);
  app.use("/v1/endpoints", endpointRouter);
  app.use("/v1/events", eventRouter);
  app.use("/v1/deliveries", deliveryRouter);

  // Global error handler (must be registered after routes)
  app.use(errorHandler);

  return app;
}
