import type { Request, Response, NextFunction } from "express";
import { ZodError } from "zod";

export function errorHandler(
  err: unknown,
  _req: Request,
  res: Response,
  _next: NextFunction
): void {
  if (err instanceof ZodError) {
    res.status(400).json({
      error: "ValidationError",
      message: "Request validation failed",
      issues: err.issues.map((issue) => ({
        path: issue.path.join("."),
        message: issue.message,
      })),
    });
    return;
  }

  if (err instanceof SyntaxError && "status" in err && err.status === 400) {
    res.status(400).json({
      error: "BadRequest",
      message: "Malformed JSON payload",
    });
    return;
  }

  console.error("Unhandled Error:", err);
  res.status(500).json({
    error: "InternalServerError",
    message: "An unexpected error occurred",
  });
}
