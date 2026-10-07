import * as NodeAssert from "node:assert/strict";
import * as NodePath from "node:path";
import { test } from "vite-plus/test";
import * as NodeURL from "node:url";
import {
  validateV2BenchmarkRequest,
  v2BenchmarkMetadata,
  v2BenchmarkClosureKnown,
} from "./v2Benchmark.mjs";

test("V2 benchmark refuses another source checkout and oversized requests", () => {
  const worktreePath = NodePath.resolve(
    NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
    "../../../..",
  );
  const request = {
    candidate: { worktreePath },
    parentPath: "/explicit/scratch",
    workload: { commands: 1, payloadBytes: 1, intervalMs: 0 },
  };
  NodeAssert.equal(validateV2BenchmarkRequest(request), request);
  NodeAssert.throws(() =>
    validateV2BenchmarkRequest({ ...request, candidate: { worktreePath: "/other/source" } }),
  );
  NodeAssert.throws(() =>
    validateV2BenchmarkRequest({ ...request, workload: { ...request.workload, commands: 65 } }),
  );
  NodeAssert.throws(() =>
    validateV2BenchmarkRequest({ ...request, databasePath: "/existing.sqlite" }),
  );
});

test("benchmark metadata keeps execution explicitly unavailable by default", () => {
  const metadata = v2BenchmarkMetadata();
  NodeAssert.equal(metadata.outcome, "unavailable");
  NodeAssert.equal(metadata.reason, "explicit candidate/runtime binding required");
  NodeAssert.deepEqual(metadata.caps, { commands: 64, payloadBytes: 4096, intervalMs: 100 });
  NodeAssert.equal(metadata.sqlTiming, "unavailable");
});

test("benchmark disposal requires evidence, captured child closure and database scope closure", () => {
  for (const outcome of [
    "success",
    "failed",
    "cancelled",
    "timed_out",
    "output_limited",
    "spawn_refused",
  ]) {
    const child = { closed: true, reaped: true, outcome };
    NodeAssert.equal(v2BenchmarkClosureKnown(child, { databaseClosed: true }, true), true);
    NodeAssert.equal(v2BenchmarkClosureKnown(child, { databaseClosed: false }, true), false);
    NodeAssert.equal(v2BenchmarkClosureKnown(child, { databaseClosed: true }, false), false);
    NodeAssert.equal(
      v2BenchmarkClosureKnown({ ...child, closed: false }, { databaseClosed: true }, true),
      false,
    );
    NodeAssert.equal(
      v2BenchmarkClosureKnown({ ...child, reaped: false }, { databaseClosed: true }, true),
      false,
    );
  }
  NodeAssert.equal(
    v2BenchmarkClosureKnown(
      { closed: true, reaped: true, outcome: "unknown" },
      { databaseClosed: true },
      true,
    ),
    false,
  );
  NodeAssert.equal(v2BenchmarkClosureKnown(undefined, undefined, true), true);
  NodeAssert.equal(v2BenchmarkClosureKnown(undefined, undefined, false), false);
});

test.skip("explicit candidate/runtime binding required", () => {});
