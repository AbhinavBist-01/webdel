import client, { Registry, Counter, Histogram, Gauge } from "prom-client";

export const register = new Registry();

// Collect standard Node.js runtime metrics (CPU, event loop lag, memory)
client.collectDefaultMetrics({ register, prefix: "webhook_platform_" });

// --- CUSTOM DISTRIBUTED SYSTEM METRICS ---

export const eventsIngestedTotal = new Counter({
  name: "webhook_events_ingested_total",
  help: "Total number of webhook events ingested through the API",
  labelNames: ["event_type", "is_duplicate"],
  registers: [register],
});

export const deliveryAttemptsTotal = new Counter({
  name: "webhook_delivery_attempts_total",
  help: "Total number of webhook delivery attempts made to external endpoints",
  labelNames: ["outcome", "response_code"],
  registers: [register],
});

export const deliveryDurationHistogram = new Histogram({
  name: "webhook_delivery_duration_seconds",
  help: "Histogram of external webhook HTTP delivery latency in seconds",
  labelNames: ["outcome"],
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5], // 50ms up to 5s
  registers: [register],
});

export const circuitBreakerTripsTotal = new Counter({
  name: "webhook_circuit_breaker_trips_total",
  help: "Total number of times a circuit breaker tripped to OPEN state",
  registers: [register],
});
