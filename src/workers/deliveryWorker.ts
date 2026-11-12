import { Worker, type Job } from "bullmq";
import { eq, and } from "drizzle-orm";
import { db } from "../db/index.js";
import {
  events,
  endpoints,
  deliveries,
  deliveryAttempts,
  deadLetterEvents,
} from "../db/schema.js";
import { redisConnection } from "../queue/redis.js";
import {
  deliveryQueue,
  WEBHOOK_DELIVERY_QUEUE_NAME,
  type DeliveryJobData,
} from "../queue/deliveryQueue.js";
import { dispatchWebhook } from "../services/dispatcher.js";
import {
  isRetryableError,
  calculateNextRetryDelayMs,
  type RetryConfig,
} from "../services/retryScheduler.js";

/**
 * Process a webhook delivery job for a specific event:
 * Dispatches the event to all active endpoints registered for the customer.
 */
export async function processDeliveryJob(
  job: Job<DeliveryJobData>,
  timeoutMs: number = 3000,
  retryConfig?: RetryConfig
): Promise<void> {
  const { eventId } = job.data;

  // 1. Fetch the event
  const [event] = await db
    .select()
    .from(events)
    .where(eq(events.id, eventId))
    .limit(1);

  if (!event) {
    console.warn(`[Worker] Event ${eventId} not found, skipping delivery.`);
    return;
  }

  // 2. Fetch all active endpoints for this customer
  const activeEndpoints = await db
    .select()
    .from(endpoints)
    .where(
      and(
        eq(endpoints.customerId, event.customerId),
        eq(endpoints.status, "active")
      )
    );

  if (activeEndpoints.length === 0) {
    return;
  }

  // 3. Deliver to each endpoint
  for (const endpoint of activeEndpoints) {
    // Check if delivery record already exists for this (event, endpoint)
    let [delivery] = await db
      .select()
      .from(deliveries)
      .where(
        and(
          eq(deliveries.eventId, event.id),
          eq(deliveries.endpointId, endpoint.id)
        )
      )
      .limit(1);

    if (!delivery) {
      const [newDelivery] = await db
        .insert(deliveries)
        .values({
          eventId: event.id,
          endpointId: endpoint.id,
          status: "delivering",
          attemptCount: 0,
        })
        .returning();

      if (!newDelivery) {
        throw new Error(`Failed to create delivery record for endpoint ${endpoint.id}`);
      }
      delivery = newDelivery;
    } else {
      await db
        .update(deliveries)
        .set({ status: "delivering" })
        .where(eq(deliveries.id, delivery.id));
    }

    const currentAttemptNumber = delivery.attemptCount + 1;

    // 4. Dispatch the HTTP webhook
    const result = await dispatchWebhook({
      deliveryId: delivery.id,
      url: endpoint.url,
      signingSecret: endpoint.signingSecret,
      payload: {
        id: event.id,
        event: event.eventType,
        data: event.payload,
        createdAt: event.createdAt,
      },
      timeoutMs,
    });

    // 5. Record this attempt in delivery_attempts
    await db.insert(deliveryAttempts).values({
      deliveryId: delivery.id,
      attemptNumber: currentAttemptNumber,
      requestHeaders: result.requestHeaders,
      responseCode: result.responseCode,
      responseBody: result.responseBody,
      durationMs: result.durationMs,
      error: result.error,
    });

    // 6. Update the delivery record status based on retryability & attempts
    if (result.outcome === "success") {
      await db
        .update(deliveries)
        .set({
          status: "success",
          completedAt: new Date(),
          attemptCount: currentAttemptNumber,
          lastResponseCode: result.responseCode,
          lastError: null,
          nextAttemptAt: null,
        })
        .where(eq(deliveries.id, delivery.id));
    } else {
      const retryable = isRetryableError(result.responseCode, result.error);
      const maxAttempts = retryConfig?.maxAttempts ?? 5;
      const isExhausted = !retryable || currentAttemptNumber >= maxAttempts;

      if (isExhausted) {
        // Move to Dead-Letter Queue (exhausted or non-retryable)
        const reason = !retryable
          ? `Permanent client error: HTTP ${result.responseCode}`
          : `Exhausted maximum retry attempts (${maxAttempts})`;

        await db.transaction(async (tx) => {
          await tx
            .update(deliveries)
            .set({
              status: "exhausted",
              attemptCount: currentAttemptNumber,
              lastResponseCode: result.responseCode,
              lastError: result.error || reason,
              nextAttemptAt: null,
            })
            .where(eq(deliveries.id, delivery.id));

          await tx.insert(deadLetterEvents).values({
            deliveryId: delivery.id,
            reason,
          });
        });
      } else {
        // Schedule next retry with exponential backoff & jitter
        const delayMs = calculateNextRetryDelayMs(currentAttemptNumber, retryConfig);
        const nextAttemptAt = new Date(Date.now() + delayMs);

        await db
          .update(deliveries)
          .set({
            status: "retry",
            attemptCount: currentAttemptNumber,
            lastResponseCode: result.responseCode,
            lastError: result.error,
            nextAttemptAt,
          })
          .where(eq(deliveries.id, delivery.id));

        // Schedule delayed retry in BullMQ
        await deliveryQueue.add(
          "deliver",
          {
            eventId: event.id,
            outboxId: job.data.outboxId,
          },
          {
            delay: delayMs,
          }
        );
      }
    }
  }
}

/**
 * Factory to create and start a BullMQ delivery worker.
 */
export function createDeliveryWorker(concurrency: number = 10): Worker<DeliveryJobData> {
  const worker = new Worker<DeliveryJobData>(
    WEBHOOK_DELIVERY_QUEUE_NAME,
    async (job) => {
      await processDeliveryJob(job);
    },
    {
      connection: redisConnection,
      concurrency,
    }
  );

  worker.on("failed", (job, err) => {
    console.error(`[Worker] Job ${job?.id} failed:`, err.message);
  });

  return worker;
}
