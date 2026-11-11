import { Redis, type RedisOptions } from "ioredis";
import dotenv from "dotenv";

dotenv.config();

const redisUrl = process.env.REDIS_URL || "redis://localhost:6379";

export const redisConnectionOptions: RedisOptions = {
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
};

export const redisConnection = new Redis(redisUrl, redisConnectionOptions);

redisConnection.on("error", (err: Error) => {
  console.error("Redis connection error:", err.message);
});
