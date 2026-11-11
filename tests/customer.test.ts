import { describe, it, expect, beforeAll, afterAll } from "vitest";
import type { Server } from "node:http";
import { createApp } from "../src/app.js";
import { pool } from "../src/db/index.js";

describe("Milestone 3: Customer Module & Authentication", () => {
  let server: Server;
  let baseUrl: string;

  beforeAll(async () => {
    const app = createApp();
    await new Promise<void>((resolve) => {
      server = app.listen(0, () => {
        const address = server.address();
        if (address && typeof address === "object") {
          baseUrl = `http://localhost:${address.port}`;
        }
        resolve();
      });
    });
  });

  afterAll(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await pool.end();
  });

  it("should reject unauthenticated requests to /v1/customers/me", async () => {
    const res = await fetch(`${baseUrl}/v1/customers/me`);
    expect(res.status).toBe(401);
    const body = await res.json();
    expect(body.error).toBe("Unauthorized");
  });

  it("should register a new customer and issue an API key", async () => {
    const res = await fetch(`${baseUrl}/v1/customers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "Stripe Killer Inc" }),
    });

    expect(res.status).toBe(201);
    const data = await res.json();
    expect(data.id).toBeDefined();
    expect(data.name).toBe("Stripe Killer Inc");
    expect(data.apiKey).toMatch(/^wh_live_[a-f0-9]{48}$/);

    // Verify authentication works with the issued API key
    const meRes = await fetch(`${baseUrl}/v1/customers/me`, {
      headers: {
        Authorization: `Bearer ${data.apiKey}`,
      },
    });

    expect(meRes.status).toBe(200);
    const meData = await meRes.json();
    expect(meData.customer.id).toBe(data.id);
    expect(meData.customer.name).toBe("Stripe Killer Inc");
  });

  it("should also authenticate with x-api-key header", async () => {
    const createRes = await fetch(`${baseUrl}/v1/customers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "Acme Corp" }),
    });
    const { apiKey, id } = await createRes.json();

    const meRes = await fetch(`${baseUrl}/v1/customers/me`, {
      headers: {
        "x-api-key": apiKey,
      },
    });

    expect(meRes.status).toBe(200);
    const meData = await meRes.json();
    expect(meData.customer.id).toBe(id);
  });
});
