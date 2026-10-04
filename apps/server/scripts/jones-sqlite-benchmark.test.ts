// @effect-diagnostics nodeBuiltinImport:off -- These tests capture native child receipts and exact creator-owned filesystem paths rather than relying on an Effect timeout.
// @effect-diagnostics globalConsole:off -- Failed native fixtures report only already captured bounded streams before assertions; the original failure and retained-root evidence remain primary.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import {
  createOwnedRoot,
  disposeOwnedRoot,
  type OwnedChildReceipt,
} from "../../../scripts/performance-staging/guard.mjs";
import { runOwnedChild } from "../../../scripts/performance-staging/lifecycle.mjs";
import {
  sourceParentEnvironment,
  syntheticDatabaseSource,
  syntheticSourceParent,
} from "../../../scripts/performance-staging/sources.mjs";
import {
  benchmarkMetadata,
  runSqliteBenchmark,
  type BenchmarkRequest,
} from "./jones-sqlite-benchmark.ts";

const scriptPath = NodeURL.fileURLToPath(new URL("./jones-sqlite-benchmark.ts", import.meta.url));
const worktree = NodePath.resolve(NodePath.dirname(scriptPath), "../../..");
const binding = {
  repository: "Jones-Systems/Jones-Code" as const,
  sourceRevision: "de9728389183c271489844ead3d1297d362f24bf",
  taskRef: "spec.jones-performance-portfolio#task.d-bench.001" as const,
  runId: "benchmark-tests",
};
const policy = { homePath: worktree, worktreePaths: [worktree], protectedPaths: [] };

function nativeDiagnostic(child: OwnedChildReceipt) {
  const capturedText = (text: string, limit: number) =>
    Buffer.from(text, "utf8")
      .subarray(0, limit - 3)
      .toString("utf8");
  return `Native CLI outcome=${child.outcome} exit=${child.exitCode ?? "null"} signal=${child.signal ?? "null"} closed=${child.closed} reaped=${child.reaped} truncated=${child.truncated}; stdout=${Buffer.byteLength(child.stdout)} bytes (within existing 96 KiB capture): ${capturedText(child.stdout, 96 * 1024)}; stderr=${Buffer.byteLength(child.stderr)} bytes (first 16 KiB): ${capturedText(child.stderr, 16 * 1024)}`;
}

async function withInvocation<Value>(
  use: (input: {
    request: BenchmarkRequest;
    native: (args: readonly string[]) => Promise<OwnedChildReceipt>;
    run: (request: BenchmarkRequest) => ReturnType<typeof runSqliteBenchmark>;
    capture: (child: OwnedChildReceipt) => void;
  }) => Promise<Value>,
) {
  const owner = createOwnedRoot({
    parentPath: NodePath.dirname(scriptPath),
    // One exclusive creator-owned invocation root is captured and disposed after all genuine child receipts are closed.
    childName: `.benchmark-test-${NodeCrypto.randomUUID()}`,
    binding,
    policy,
  });
  const outer = owner.creationReceipt.canonicalRootPath;
  const nativeChildren: OwnedChildReceipt[] = [];
  const actualChildren: OwnedChildReceipt[] = [];
  const cancellation = new AbortController();
  let originalFixtureRetained = false;
  let failure: unknown;
  let value: Value | undefined;
  try {
    value = await use({
      request: {
        parentPath: outer,
        binding,
        databaseSource: syntheticDatabaseSource("e5a31aceec91484b64315c63dcce80f6e7581604"),
        trials: 1,
        turns: 2,
        historyTurns: 3,
        payloadBytes: 128,
        arrival: "burst",
        intervalMs: 0,
        burstSize: 2,
        timeoutMs: 30000,
      },
      native: async (args) => {
        const child = await runOwnedChild({
          owner,
          executable: process.execPath,
          args,
          env: {
            HOME: worktree,
            LANG: "C.UTF-8",
            TZ: "UTC",
            NODE_NO_WARNINGS: "1",
            [sourceParentEnvironment]: syntheticSourceParent(),
          },
          timeoutMs: 45000,
          terminateGraceMs: 1000,
          reapTimeoutMs: 3000,
          maxOutputBytes: 96 * 1024,
          signal: cancellation.signal,
        });
        nativeChildren.push(child);
        if (child.outcome !== "success" && child.exitCode !== 2)
          console.error(nativeDiagnostic(child));
        if (child.stdout) {
          try {
            const envelope = Schema.decodeUnknownSync(
              Schema.fromJsonString(
                Schema.Struct({
                  cleanup: Schema.Struct({ outcome: Schema.String }),
                }),
              ),
            )(child.stdout);
            if (envelope.cleanup.outcome !== "complete") originalFixtureRetained = true;
          } catch {
            if (args.includes("--run-json") && child.exitCode !== 2) originalFixtureRetained = true;
          }
        } else if (args.includes("--run-json") && child.exitCode !== 2)
          originalFixtureRetained = true;
        return child;
      },
      run: async (request) => {
        let capturedChild: OwnedChildReceipt | undefined;
        const report = await runSqliteBenchmark(request, {
          policy,
          runChild: async (options) => {
            capturedChild = await runOwnedChild(options);
            actualChildren.push(capturedChild);
            return capturedChild;
          },
        });
        if (report.status !== "completed")
          console.error(
            `Benchmark report status=${report.status} reason=${report.reason ?? "null"} primaryFailure=${JSON.stringify(report.primaryFailure)}; ${capturedChild ? nativeDiagnostic(capturedChild) : "native child receipt unavailable"}`,
          );
        if (report.cleanup.outcome !== "complete") originalFixtureRetained = true;
        return report;
      },
      capture: (child) => actualChildren.push(child),
    });
  } catch (error) {
    failure = error;
  } finally {
    cancellation.abort();
    const all = [...nativeChildren, ...actualChildren];
    if (
      originalFixtureRetained ||
      all.some((child) => !child.closed || !child.reaped || child.outcome === "unknown")
    ) {
      const cleanupFailure = new Error(`Unproved native child closure; retained ${outer}`);
      failure = failure
        ? new AggregateError([failure, cleanupFailure], "Test failed and scratch retained", {
            cause: failure,
          })
        : cleanupFailure;
    } else {
      const cleanup = disposeOwnedRoot(owner, { childReceipts: nativeChildren });
      if (cleanup.outcome !== "complete") {
        const cleanupFailure = new Error(`Creator cleanup retained ${outer}: ${cleanup.reason}`);
        failure = failure
          ? new AggregateError([failure, cleanupFailure], "Test failed and scratch retained", {
              cause: failure,
            })
          : cleanupFailure;
      }
    }
  }
  if (failure) throw failure;
  return value;
}

function output(child: OwnedChildReceipt): unknown {
  expect(child.closed).toBe(true);
  expect(child.reaped).toBe(true);
  expect(child.truncated).toBe(false);
  if (!child.stdout.endsWith("\n"))
    throw new Error(`Native CLI output missing; ${nativeDiagnostic(child)}`);
  try {
    return Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown))(child.stdout);
  } catch (error) {
    throw new Error(`Native CLI output invalid; ${nativeDiagnostic(child)}`, { cause: error });
  }
}

describe("bounded synthetic SQLite benchmark", () => {
  it("defaults to metadata through native Node without opening a fixture", async () => {
    await withInvocation(async ({ native }) => {
      const child = await native([scriptPath]);
      expect(child.outcome).toBe("success");
      expect(output(child)).toEqual(benchmarkMetadata());
    });
  });

  it("refuses arbitrary SQL, omitted bounds, excessive scales and unknown fields through the native CLI", async () => {
    await withInvocation(async ({ native, request }) => {
      for (const args of [
        ["--sql", "DELETE FROM orchestration_events"],
        ["--run-json", "{}"],
        ["--run-json", JSON.stringify({ ...request, turns: 65 })],
        ["--run-json", JSON.stringify({ ...request, databasePath: "/not-a-fixture.sqlite" })],
        ["--run-json", "x".repeat(24 * 1024 + 1)],
      ]) {
        const child = await native([scriptPath, ...args]);
        expect(child.exitCode).toBe(2);
        expect(output(child)).toEqual({
          schema: "jones-sqlite-benchmark/v1",
          status: "refused",
          reason: "invalid_request",
        });
      }
    });
  });

  it("dispatches real engine transactions and proves replay, rejection, rollback and retry consistency", async () => {
    await withInvocation(async ({ native, request }) => {
      const child = await native([scriptPath, "--run-json", JSON.stringify(request)]);
      expect(child.outcome).toBe("success");
      const report = output(child);
      expect(report).toMatchObject({
        schema: "jones-sqlite-benchmark/v1",
        status: "completed",
        reason: null,
        cleanup: { outcome: "complete", absent: true, retainedRoot: null },
        child: { outcome: "success", closed: true, reaped: true },
        trials: [
          {
            runtime: {
              profile: "benchmark-wal",
              pragmas: { journal_mode: "wal", synchronous: 2, foreign_keys: 1, busy_timeout: 5000 },
            },
            traffic: { offered: 8, attempts: 8, accepted: 8, finalBacklog: 0 },
            outcomes: {
              offered: 12,
              attempts: 13,
              accepted: 9,
              fulfilledTerminals: 10,
              failures: 3,
              replays: 2,
              rejections: 2,
              retries: 1,
            },
            consistency: {
              acceptedReplayUnchanged: true,
              rejectedReplayUnchanged: true,
              rollbackUnchanged: true,
              rejectionPersisted: true,
              retryAccepted: true,
              eventDelta: 11,
              receiptDelta: 10,
              replayEquivalent: true,
              integrity: true,
              foreignKeyViolations: 0,
            },
            timing: {
              schedulingLag: { samples: 2 },
              traffic: {
                harnessQueue: { samples: 8 },
                engineCompletion: { samples: 8 },
                arrivalCompletion: { samples: 8 },
              },
              protocol: { engineCompletion: { samples: 5 } },
              sqlExecution: { status: "unavailable" },
              transaction: { status: "unavailable" },
              pureWriterLockWait: { status: "unavailable" },
            },
          },
        ],
      });
      expect(child.stdout).not.toContain("b".repeat(request.payloadBytes));
      expect(child.stdout).not.toContain("Synthetic native prompt");
    });
  }, 60000);

  it("supports finite steady arrivals on fresh independent trials", async () => {
    await withInvocation(async ({ request, run }) => {
      const report = await run({
        ...request,
        trials: 2,
        turns: 1,
        arrival: "steady",
        intervalMs: 1,
      });
      expect(report.status).toBe("completed");
      expect(report.cleanup).toEqual({ outcome: "complete", absent: true, retainedRoot: null });
      expect(report.trials).toHaveLength(2);
      for (const trial of report.trials)
        expect(trial).toMatchObject({
          traffic: { offered: 4, finalBacklog: 0 },
          consistency: { eventDelta: 6, receiptDelta: 6 },
          timing: {
            schedulingLag: { samples: 1 },
            traffic: { engineCompletion: { samples: 4 } },
            protocol: { engineCompletion: { samples: 5 } },
          },
        });
    });
  }, 60000);

  it("cleans a cancelled invocation when no fixture child was started", async () => {
    await withInvocation(async ({ request }) => {
      const cancellation = new AbortController();
      cancellation.abort();
      const report = await runSqliteBenchmark(request, { policy, signal: cancellation.signal });
      expect(report).toMatchObject({
        status: "failed",
        reason: "cancelled",
        child: { pid: null, closed: true, reaped: true },
        cleanup: { outcome: "complete", absent: true, retainedRoot: null },
        trials: [],
      });
    });
  });

  it("enforces a hard synchronous-child deadline and retains unproved fixture cleanup", async () => {
    await withInvocation(async ({ request, capture }) => {
      const report = await runSqliteBenchmark(
        { ...request, timeoutMs: 200 },
        {
          policy,
          runChild: async (options) => {
            const child = await runOwnedChild({ ...options, args: ["-e", "for (;;) {}"] });
            capture(child);
            return child;
          },
        },
      );
      expect(report).toMatchObject({
        status: "failed",
        reason: "timed_out",
        child: { outcome: "timed_out", closed: true, reaped: true },
        cleanup: { outcome: "retained", absent: false },
      });
      expect(report.cleanup.retainedRoot && NodeFS.existsSync(report.cleanup.retainedRoot)).toBe(
        true,
      );
    });
  });

  it("bounds output before accumulation and excludes raw worker text from failure reports", async () => {
    await withInvocation(async ({ request, capture }) => {
      const report = await runSqliteBenchmark(request, {
        policy,
        runChild: async (options) => {
          const child = await runOwnedChild({
            ...options,
            args: ["-e", "require('node:fs').writeSync(1,'PRIVATE'.repeat(20000)); for (;;) {}"],
          });
          capture(child);
          return child;
        },
      });
      expect(report.child).toMatchObject({ outcome: "output_limited", closed: true, reaped: true });
      expect(JSON.stringify(report)).not.toContain("PRIVATE");
      expect(report.cleanup.outcome).toBe("retained");
    });
  });

  it("cancels only the owned child after an observable ready barrier and retains its incomplete fixture outcome", async () => {
    await withInvocation(async ({ request, capture }) => {
      const cancellation = new AbortController();
      const report = await runSqliteBenchmark(request, {
        policy,
        signal: cancellation.signal,
        runChild: async (options) => {
          const root = options.owner.creationReceipt.canonicalRootPath;
          const ready = NodePath.join(root, "ready");
          const watcher = NodeFS.watch(root, (_event, name) => {
            if (name === "ready" && NodeFS.existsSync(ready)) cancellation.abort();
          });
          try {
            const child = await runOwnedChild({
              ...options,
              args: [
                "-e",
                `require('node:fs').writeFileSync(${JSON.stringify(ready)},'ready'); for (;;) {}`,
              ],
            });
            capture(child);
            return child;
          } finally {
            watcher.close();
          }
        },
      });
      expect(report).toMatchObject({
        status: "failed",
        reason: "cancelled",
        child: { outcome: "cancelled", closed: true, reaped: true },
        cleanup: { outcome: "retained", absent: false },
      });
    });
  });

  it("retains an explicit fixture cleanup failure even when the actual child is closed and reaped", async () => {
    await withInvocation(async ({ request, capture }) => {
      const report = await runSqliteBenchmark(request, {
        policy,
        runChild: async (options) => {
          const frame = {
            schema: "jones-sqlite-benchmark-worker/v1",
            status: "failed",
            fixtureCleanupKnown: false,
            primaryFailure: null,
            trials: [],
          };
          const child = await runOwnedChild({
            ...options,
            args: [
              "-e",
              `process.stdout.write(${JSON.stringify(`${JSON.stringify(frame)}\n`)}); process.exitCode=1`,
            ],
          });
          capture(child);
          return child;
        },
      });
      expect(report).toMatchObject({
        status: "failed",
        reason: "failed",
        child: { closed: true, reaped: true },
        cleanup: { outcome: "retained", absent: false },
      });
    });
  });

  it("retains unknown custody and cannot turn missing child evidence into cleanup permission", async () => {
    await withInvocation(async ({ request, capture }) => {
      const report = await runSqliteBenchmark(request, {
        policy,
        runChild: async (options) => {
          const child = await runOwnedChild({ ...options, args: ["-e", ""] });
          capture(child);
          return { ...child, outcome: "unknown", closed: false, reaped: false };
        },
      });
      expect(report).toMatchObject({
        status: "failed",
        reason: "unknown_child_close",
        cleanup: { outcome: "retained", absent: false },
      });
      const missingReceipt = await runSqliteBenchmark(request, {
        policy,
        runChild: async () => {
          throw new Error("unobserved child effect");
        },
      });
      expect(missingReceipt.cleanup).toMatchObject({ outcome: "retained", absent: false });
    });
  });
});
