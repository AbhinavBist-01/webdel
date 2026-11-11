import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Run DB integration test files sequentially to avoid cross-test table races
    fileParallelism: false,
  },
});
