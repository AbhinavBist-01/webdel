import { z } from "zod";

export const createEndpointSchema = z.object({
  url: z
    .string()
    .url("Must be a valid URL")
    .refine(
      (val) => val.startsWith("http://") || val.startsWith("https://"),
      "URL must start with http:// or https://"
    ),
  rateLimit: z
    .number()
    .int("Rate limit must be an integer")
    .min(1, "Rate limit must be at least 1 req/sec")
    .max(1000, "Rate limit cannot exceed 1000 req/sec")
    .optional()
    .default(50),
});

export type CreateEndpointInput = z.infer<typeof createEndpointSchema>;
