import { describe, it, expect, beforeAll, afterAll } from "vitest";
import type { Server } from "node:http";
import { createApp } from "../src/app.js";
import { pool } from "../src/db/index.js";

describe("Milestone 4: Endpoint Module & Customer Isolation", () => {
  let server: Server;
  let baseUrl: string;

  let customer1ApiKey: string;
  let customer2ApiKey: string;

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

    // Create Customer 1
    const res1 = await fetch(`${baseUrl}/v1/customers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "Customer Alpha" }),
    });
    const data1 = await res1.json();
    customer1ApiKey = data1.apiKey;

    // Create Customer 2
    const res2 = await fetch(`${baseUrl}/v1/customers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "Customer Beta" }),
    });
    const data2 = await res2.json();
    customer2ApiKey = data2.apiKey;
  });

  afterAll(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await pool.end();
  });

  it("should reject endpoint registration without authentication", async () => {
    const res = await fetch(`${baseUrl}/v1/endpoints`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url: "https://alpha.com/webhook" }),
    });
    expect(res.status).toBe(401);
  });

  it("should reject invalid URLs", async () => {
    const res = await fetch(`${baseUrl}/v1/endpoints`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${customer1ApiKey}`,
      },
      body: JSON.stringify({ url: "not-a-valid-url" }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toBe("ValidationError");
  });

  it("should register an endpoint with signing secret and default rate limit", async () => {
    const res = await fetch(`${baseUrl}/v1/endpoints`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${customer1ApiKey}`,
      },
      body: JSON.stringify({ url: "https://alpha.com/webhook" }),
    });

    expect(res.status).toBe(201);
    const body = await res.json();
    expect(body.id).toBeDefined();
    expect(body.url).toBe("https://alpha.com/webhook");
    expect(body.rateLimit).toBe(50);
    expect(body.status).toBe("active");
    expect(body.signingSecret).toMatch(/^whsec_[a-f0-9]{64}$/);
  });

  it("should enforce customer isolation for registered endpoints", async () => {
    // Register endpoint for Customer Beta
    await fetch(`${baseUrl}/v1/endpoints`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${customer2ApiKey}`,
      },
      body: JSON.stringify({ url: "https://beta.com/webhook", rateLimit: 100 }),
    });

    // Customer Alpha fetches endpoints
    const alphaRes = await fetch(`${baseUrl}/v1/endpoints`, {
      headers: { Authorization: `Bearer ${customer1ApiKey}` },
    });
    const alphaBody = await alphaRes.json();

    // Customer Beta fetches endpoints
    const betaRes = await fetch(`${baseUrl}/v1/endpoints`, {
      headers: { Authorization: `Bearer ${customer2ApiKey}` },
    });
    const betaBody = await betaRes.json();

    expect(alphaBody.endpoints.length).toBe(1);
    expect(alphaBody.endpoints[0].url).toBe("https://alpha.com/webhook");

    expect(betaBody.endpoints.length).toBe(1);
    expect(betaBody.endpoints[0].url).toBe("https://beta.com/webhook");
    expect(betaBody.endpoints[0].rateLimit).toBe(100);
  });
});
