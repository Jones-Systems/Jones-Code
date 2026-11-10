import { defineConfig } from "vite-plus";
import { typecheckTask } from "../scripts/jones/tsrs/task-cache.ts";
import packageJson from "./package.json" with { type: "json" };

export default defineConfig({
  run: { tasks: { "tsrs:typecheck": typecheckTask(packageJson.scripts["typecheck:native"]) } },
});
