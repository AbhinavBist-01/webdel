import express, { type Express } from "express";
import { customerRouter } from "./modules/customer/index.js";
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

  // Global error handler (must be registered after routes)
  app.use(errorHandler);

  return app;
}
