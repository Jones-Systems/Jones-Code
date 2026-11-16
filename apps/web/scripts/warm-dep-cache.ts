// @effect-diagnostics nodeBuiltinImport:off - plain Node script that dev-runner spawns on a cold cache.
/**
 * The cache cannot be shared between worktrees: Vite's config hash includes
 * the absolute project root, so each worktree must warm its own.
 */
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import {
  usesLocalMacCapacity,
  withLocalMacCapacity,
} from "../../../scripts/jones/tsrs/local-mac-capacity.ts";

const controller = new AbortController();
const cancel = () => controller.abort();
process.once("SIGTERM", cancel);
process.once("SIGINT", cancel);

const webRoot = NodePath.dirname(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)));

await withLocalMacCapacity(
  {
    root: NodePath.resolve(webRoot, "../.."),
    signal: controller.signal,
    jobs: 2,
    processLifetime: true,
  },
  async () => {
    // oxlint-disable-next-line t3code/no-global-process-runtime -- Native pool controls follow the physical host before Vite initializes.
    if (usesLocalMacCapacity(NodeOS.platform(), process.env)) {
      for (const key of Object.keys(process.env)) {
        if (
          ["rolldown_worker_threads", "rolldown_max_blocking_threads"].includes(key.toLowerCase())
        )
          delete process.env[key];
      }
      process.env.ROLLDOWN_WORKER_THREADS = "1";
      process.env.ROLLDOWN_MAX_BLOCKING_THREADS = "1";
    }
    process.removeListener("SIGTERM", cancel);
    process.removeListener("SIGINT", cancel);
    const { optimizeDeps, resolveConfig } = await import("vite");
    const configStart = performance.now();
    const config = await resolveConfig({ root: webRoot, logLevel: "error" }, "serve");
    if (process.env.T3CODE_STARTUP_PROFILE_DIR) {
      performance.measure("web.warmDepCache.resolveConfig", { start: configStart });
    }
    const optimizeStart = performance.now();
    await optimizeDeps(config);
    if (process.env.T3CODE_STARTUP_PROFILE_DIR) {
      performance.measure("web.warmDepCache.optimizeDeps", { start: optimizeStart });
    }
    console.log("[warm-dep-cache] web dependency cache is warm");
  },
);
process.removeListener("SIGTERM", cancel);
process.removeListener("SIGINT", cancel);
