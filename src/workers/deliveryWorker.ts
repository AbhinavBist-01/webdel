import { Worker, type Job } from "bullmq";
import { eq, and, lt, inArray, sql } from "drizzle-orm";
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
import { rateLimiter } from "../services/rateLimiter.js";
import { circuitBreaker } from "../services/circuitBreaker.js";
import {
  isRetryableError,
  calculateNextRetryDelayMs,
  type RetryConfig,
} from "../services/retryScheduler.js";
import {
  deliveryAttemptsTotal,
  deliveryDurationHistogram,
} from "../observability/metrics.js";

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
      delivery = await db.transaction(async (tx) => {
        const [maxRow] = await tx
          .select({
            maxSeq: sql<number>`COALESCE(MAX(${deliveries.sequenceNumber}), 0)`,
          })
          .from(deliveries)
          .where(eq(deliveries.endpointId, endpoint.id));

        const sequenceNumber = Number(maxRow?.maxSeq ?? 0) + 1;

        const [created] = await tx
          .insert(deliveries)
          .values({
            eventId: event.id,
            endpointId: endpoint.id,
            status: "delivering",
            sequenceNumber,
            attemptCount: 0,
          })
          .returning();

        return created!;
      });
    }

    // 4. Head-of-Line (HoL) Blocking Check:
    // Ensure no prior delivery for THIS endpoint is still pending, delivering, or retrying
    const [blocking] = await db
      .select({ id: deliveries.id, seq: deliveries.sequenceNumber })
      .from(deliveries)
      .where(
        and(
          eq(deliveries.endpointId, endpoint.id),
          lt(deliveries.sequenceNumber, delivery.sequenceNumber),
          inArray(deliveries.status, ["pending", "delivering", "retry"])
        )
      )
      .limit(1);

    if (blocking) {
      // Prior event for this endpoint is still in-flight. Defer this job to maintain strict per-endpoint ordering!
      await deliveryQueue.add(
        "deliver",
        { eventId: event.id, outboxId: job.data.outboxId },
        { delay: 100 }
      );
      return;
    }

    // 5. Circuit Breaker Check: Fail fast if endpoint is currently in OPEN state
    const circuit = await circuitBreaker.canAttempt(endpoint.id);
    if (!circuit.allow) {
      await deliveryQueue.add(
        "deliver",
        { eventId: event.id, outboxId: job.data.outboxId },
        { delay: 1000 }
      );
      return;
    }

    // 6. Token Bucket Rate Limiting: Enforce per-endpoint rate limits
    const rateCheck = await rateLimiter.consume(endpoint.id, endpoint.rateLimit);
    if (!rateCheck.allowed) {
      await deliveryQueue.add(
        "deliver",
        { eventId: event.id, outboxId: job.data.outboxId },
        { delay: Math.max(50, rateCheck.waitMs) }
      );
      return;
    }

    // Now that checks passed, mark as delivering
    await db
      .update(deliveries)
      .set({ status: "delivering" })
      .where(eq(deliveries.id, delivery.id));

    const currentAttemptNumber = delivery.attemptCount + 1;

    // 7. Dispatch the HTTP webhook
    const result = await dispatchWebhook({
      deliveryId: delivery.id,
      url: endpoint.url,
      signingSecret: endpoint.signingSecret,
      payload: {
        id: event.id,
        event: event.eventType,
        sequenceNumber: delivery.sequenceNumber,
        data: event.payload,
        createdAt: event.createdAt,
      },
      timeoutMs,
    });

    // Record Prometheus metrics
    deliveryDurationHistogram.observe(
      { outcome: result.outcome },
      result.durationMs / 1000
    );
    deliveryAttemptsTotal.inc({
      outcome: result.outcome,
      response_code: String(result.responseCode || "none"),
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

    // 8. Update delivery record & Circuit Breaker based on outcome
    if (result.outcome === "success") {
      await circuitBreaker.recordSuccess(endpoint.id);

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
      if (retryable) {
        await circuitBreaker.recordFailure(endpoint.id);
      }

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
export function createDeliveryWorker(
  concurrency: number = 10,
  retryConfig?: RetryConfig
): Worker<DeliveryJobData> {
  const worker = new Worker<DeliveryJobData>(
    WEBHOOK_DELIVERY_QUEUE_NAME,
    async (job) => {
      await processDeliveryJob(job, 3000, retryConfig);
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
