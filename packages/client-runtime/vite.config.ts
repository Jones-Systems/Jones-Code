import { typecheckTask } from "../../scripts/jones/tsrs/task-cache.ts";
import packageJson from "./package.json" with { type: "json" };
import "vite-plus/test/config";
import { defineConfig } from "vite-plus";

export default defineConfig({
  run: { tasks: { "tsrs:typecheck": typecheckTask(packageJson.scripts["typecheck:native"]) } },
  test: {
    environment: "node",
    include: ["src/**/*.test.ts"],
    setupFiles: ["../shared/src/testing/longTempDir.ts", "../shared/src/testing/providerEnv.ts"],
  },
});
