import { Queue } from "bullmq";
import { redisConnection } from "./redis.js";

export const WEBHOOK_DELIVERY_QUEUE_NAME = "webhook-delivery";

export interface DeliveryJobData {
  eventId: string;
  outboxId: string;
}

export const deliveryQueue = new Queue<DeliveryJobData>(
  WEBHOOK_DELIVERY_QUEUE_NAME,
  {
    connection: redisConnection,
    defaultJobOptions: {
      removeOnComplete: true, // Auto-clean completed jobs from Redis memory
      removeOnFail: 5000,     // Keep last 5000 failed jobs for inspection
    },
  }
);
