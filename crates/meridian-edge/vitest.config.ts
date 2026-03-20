import { defineConfig } from "vitest/config";
import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import path from "path";

export default defineConfig({
  plugins: [
    cloudflareTest({
      main: path.resolve(__dirname, "build/worker/shim.mjs"),
      wrangler: { configPath: path.resolve(__dirname, "wrangler.toml") },
    }) as any,
  ],
  test: {
    include: ["tests/**/*.test.ts"],
  },
});
