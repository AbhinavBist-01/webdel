import { sql, eq, inArray } from "drizzle-orm";
import { db } from "../db/index.js";
import { outbox } from "../db/schema.js";
import { deliveryQueue } from "../queue/deliveryQueue.js";

export interface OutboxPollerOptions {
  batchSize?: number;
  intervalMs?: number;
  stuckThresholdSeconds?: number;
}

export class OutboxPoller {
  private batchSize: number;
  private intervalMs: number;
  private stuckThresholdSeconds: number;
  private isRunning: boolean = false;
  private timer: NodeJS.Timeout | null = null;

  constructor(options: OutboxPollerOptions = {}) {
    this.batchSize = options.batchSize ?? 50;
    this.intervalMs = options.intervalMs ?? 1000;
    this.stuckThresholdSeconds = options.stuckThresholdSeconds ?? 60;
  }

  /**
   * Recovers any outbox records that were left in 'processing' status
   * due to sudden server/worker crashes.
   */
  async recoverStuckRecords(): Promise<number> {
    const result = await db.execute(sql`
      UPDATE outbox
      SET status = 'pending'
      WHERE status = 'processing'
        AND created_at < NOW() - (${this.stuckThresholdSeconds} || ' seconds')::interval
    `);

    return result.rowCount ?? 0;
  }

  /**
   * Performs a single atomic poll iteration:
   * 1. Claims pending records using FOR UPDATE SKIP LOCKED
   * 2. Pushes them to BullMQ/Redis with idempotent job IDs
   * 3. Marks them as 'published'
   */
  async pollOnce(): Promise<number> {
    return await db.transaction(async (tx) => {
      // 1. Atomically claim batch of pending outbox rows
      const pendingRowsResult = await tx.execute(sql`
        SELECT id, event_id as "eventId"
        FROM outbox
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT ${this.batchSize}
        FOR UPDATE SKIP LOCKED
      `);

      const rows = pendingRowsResult.rows as unknown as Array<{
        id: string;
        eventId: string;
      }>;

      if (!rows || rows.length === 0) {
        return 0;
      }

      const outboxIds = rows.map((r) => r.id);

      // 2. Mark them as processing in the current transaction
      await tx
        .update(outbox)
        .set({ status: "processing" })
        .where(inArray(outbox.id, outboxIds));

      // 3. Publish to BullMQ queue with deterministic jobId for idempotency
      for (const row of rows) {
        await deliveryQueue.add(
          "deliver",
          {
            eventId: row.eventId,
            outboxId: row.id,
          },
          {
            jobId: row.id, // Deduplication key in Redis
          }
        );
      }

      // 4. Mark rows as published
      await tx
        .update(outbox)
        .set({
          status: "published",
          publishedAt: new Date(),
        })
        .where(inArray(outbox.id, outboxIds));

      return rows.length;
    });
  }

  /**
   * Starts the continuous background polling loop.
   */
  start(): void {
    if (this.isRunning) return;
    this.isRunning = true;

    const runLoop = async () => {
      if (!this.isRunning) return;

      try {
        await this.recoverStuckRecords();
        const publishedCount = await this.pollOnce();

        // If we processed a full batch, poll immediately again; otherwise wait for interval
        const nextDelay =
          publishedCount >= this.batchSize ? 0 : this.intervalMs;
        this.timer = setTimeout(runLoop, nextDelay);
      } catch (err) {
        console.error("Outbox poller error:", err);
        this.timer = setTimeout(runLoop, this.intervalMs);
      }
    };

    runLoop();
  }

  /**
   * Gracefully stops the polling loop.
   */
  stop(): void {
    this.isRunning = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }
}
