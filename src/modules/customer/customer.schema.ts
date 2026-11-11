import { z } from "zod";

export const createCustomerSchema = z.object({
  name: z.string().trim().min(1, "Name is required").max(255),
});

export type CreateCustomerInput = z.infer<typeof createCustomerSchema>;
