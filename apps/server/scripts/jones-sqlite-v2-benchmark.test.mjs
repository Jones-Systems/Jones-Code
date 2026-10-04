import * as Assert from "node:assert/strict";
import * as Path from "node:path";
import { test } from "vite-plus/test";
import * as URL from "node:url";
import { validateV2BenchmarkRequest } from "./jones-sqlite-v2-benchmark.mjs";

test("V2 benchmark refuses another source checkout and oversized requests", () => {
  const worktreePath = Path.resolve(Path.dirname(URL.fileURLToPath(import.meta.url)), "../../..");
  const request = { candidate: { worktreePath }, parentPath: "/explicit/scratch", workload: { commands: 1, payloadBytes: 1, intervalMs: 0 } };
  Assert.equal(validateV2BenchmarkRequest(request), request);
  Assert.throws(() => validateV2BenchmarkRequest({ ...request, candidate: { worktreePath: "/other/source" } }));
  Assert.throws(() => validateV2BenchmarkRequest({ ...request, workload: { ...request.workload, commands: 65 } }));
  Assert.throws(() => validateV2BenchmarkRequest({ ...request, databasePath: "/existing.sqlite" }));
});
