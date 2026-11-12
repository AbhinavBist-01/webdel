import {
  pgTable,
  uuid,
  varchar,
  text,
  integer,
  timestamp,
  jsonb,
  pgEnum,
  uniqueIndex,
  index,
} from "drizzle-orm/pg-core";
import { relations } from "drizzle-orm";

// --- ENUMS ---
export const endpointStatusEnum = pgEnum("endpoint_status", [
  "active",
  "disabled",
]);

export const outboxStatusEnum = pgEnum("outbox_status", [
  "pending",
  "processing",
  "published",
  "failed",
]);

export const deliveryStatusEnum = pgEnum("delivery_status", [
  "pending",
  "delivering",
  "success",
  "retry",
  "exhausted",
]);

// --- 1. CUSTOMERS ---
export const customers = pgTable(
  "customers",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    name: varchar("name", { length: 255 }).notNull(),
    apiKeyHash: varchar("api_key_hash", { length: 64 }).notNull().unique(),
    createdAt: timestamp("created_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (table) => [
    uniqueIndex("customers_api_key_hash_idx").on(table.apiKeyHash),
  ]
);

// --- 2. ENDPOINTS ---
export const endpoints = pgTable(
  "endpoints",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    customerId: uuid("customer_id")
      .notNull()
      .references(() => customers.id, { onDelete: "cascade" }),
    url: text("url").notNull(),
    signingSecret: varchar("signing_secret", { length: 255 }).notNull(),
    status: endpointStatusEnum("status").default("active").notNull(),
    rateLimit: integer("rate_limit").default(50).notNull(), // max req/sec
    createdAt: timestamp("created_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (table) => [
    index("endpoints_customer_id_idx").on(table.customerId),
  ]
);

// --- 3. EVENTS ---
export const events = pgTable(
  "events",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    customerId: uuid("customer_id")
      .notNull()
      .references(() => customers.id, { onDelete: "cascade" }),
    eventType: varchar("event_type", { length: 255 }).notNull(),
    payload: jsonb("payload").notNull(),
    idempotencyKey: varchar("idempotency_key", { length: 255 }).notNull(),
    createdAt: timestamp("created_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (table) => [
    // CRITICAL: Prevent duplicate event creation when producers retry API requests
    uniqueIndex("events_customer_idempotency_idx").on(
      table.customerId,
      table.idempotencyKey
    ),
    index("events_customer_created_at_idx").on(table.customerId, table.createdAt),
  ]
);

// --- 4. OUTBOX ---
export const outbox = pgTable(
  "outbox",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    eventId: uuid("event_id")
      .notNull()
      .references(() => events.id, { onDelete: "cascade" }),
    status: outboxStatusEnum("status").default("pending").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    publishedAt: timestamp("published_at", { withTimezone: true }),
  },
  (table) => [
    // Essential for high-performance outbox polling (FOR UPDATE SKIP LOCKED)
    index("outbox_status_created_at_idx").on(table.status, table.createdAt),
    index("outbox_event_id_idx").on(table.eventId),
  ]
);

// --- 5. DELIVERIES ---
export const deliveries = pgTable(
  "deliveries",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    eventId: uuid("event_id")
      .notNull()
      .references(() => events.id, { onDelete: "cascade" }),
    endpointId: uuid("endpoint_id")
      .notNull()
      .references(() => endpoints.id, { onDelete: "cascade" }),
    status: deliveryStatusEnum("status").default("pending").notNull(),
    sequenceNumber: integer("sequence_number").default(1).notNull(),
    attemptCount: integer("attempt_count").default(0).notNull(),
    nextAttemptAt: timestamp("next_attempt_at", { withTimezone: true }),
    lastResponseCode: integer("last_response_code"),
    lastError: text("last_error"),
    createdAt: timestamp("created_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    completedAt: timestamp("completed_at", { withTimezone: true }),
  },
  (table) => [
    index("deliveries_endpoint_status_idx").on(table.endpointId, table.status),
    index("deliveries_endpoint_sequence_idx").on(table.endpointId, table.sequenceNumber),
    index("deliveries_status_next_attempt_idx").on(table.status, table.nextAttemptAt),
    index("deliveries_event_id_idx").on(table.eventId),
  ]
);

// --- 6. DELIVERY ATTEMPTS ---
export const deliveryAttempts = pgTable(
  "delivery_attempts",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    deliveryId: uuid("delivery_id")
      .notNull()
      .references(() => deliveries.id, { onDelete: "cascade" }),
    attemptNumber: integer("attempt_number").notNull(),
    requestHeaders: jsonb("request_headers").notNull(),
    responseCode: integer("response_code"),
    responseBody: text("response_body"),
    durationMs: integer("duration_ms").notNull(),
    error: text("error"),
    createdAt: timestamp("created_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (table) => [
    index("delivery_attempts_delivery_id_idx").on(table.deliveryId),
  ]
);

// --- 7. DEAD LETTER EVENTS ---
export const deadLetterEvents = pgTable(
  "dead_letter_events",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    deliveryId: uuid("delivery_id")
      .notNull()
      .references(() => deliveries.id, { onDelete: "cascade" }),
    reason: text("reason").notNull(),
    failedAt: timestamp("failed_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    replayedAt: timestamp("replayed_at", { withTimezone: true }),
  },
  (table) => [
    index("dead_letter_delivery_id_idx").on(table.deliveryId),
    index("dead_letter_failed_at_idx").on(table.failedAt),
  ]
);

// --- RELATIONS ---
export const customersRelations = relations(customers, ({ many }) => ({
  endpoints: many(endpoints),
  events: many(events),
}));

export const endpointsRelations = relations(endpoints, ({ one, many }) => ({
  customer: one(customers, {
    fields: [endpoints.customerId],
    references: [customers.id],
  }),
  deliveries: many(deliveries),
}));

export const eventsRelations = relations(events, ({ one, many }) => ({
  customer: one(customers, {
    fields: [events.customerId],
    references: [customers.id],
  }),
  deliveries: many(deliveries),
  outboxEntries: many(outbox),
}));

export const outboxRelations = relations(outbox, ({ one }) => ({
  event: one(events, {
    fields: [outbox.eventId],
    references: [events.id],
  }),
}));

export const deliveriesRelations = relations(deliveries, ({ one, many }) => ({
  event: one(events, {
    fields: [deliveries.eventId],
    references: [events.id],
  }),
  endpoint: one(endpoints, {
    fields: [deliveries.endpointId],
    references: [endpoints.id],
  }),
  attempts: many(deliveryAttempts),
  deadLetter: one(deadLetterEvents, {
    fields: [deliveries.id],
    references: [deadLetterEvents.deliveryId],
  }),
}));
