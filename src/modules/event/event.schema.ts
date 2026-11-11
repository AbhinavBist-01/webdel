import { z } from "zod";

export const createEventSchema = z.object({
  eventType: z
    .string()
    .trim()
    .min(1, "Event type is required")
    .max(255, "Event type too long"),
  payload: z
    .record(z.string(), z.unknown())
    .refine((val) => typeof val === "object" && val !== null, "Payload must be a JSON object"),
  idempotencyKey: z
    .string()
    .trim()
    .min(1, "Idempotency key is required")
    .max(255, "Idempotency key too long")
    .optional(),
});

export type CreateEventInput = z.infer<typeof createEventSchema>;
