import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeConsole from "node:console";
// @effect-diagnostics-next-line nodeBuiltinImport:off - Custody checks require native device/inode observations and raw SQLite header reads.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off - Bind canonical fixture paths synchronously before runtime acquisition.
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { produceCurrentFixture } from "./currentFixtures.ts";
import type { CurrentFixtureOptions, CurrentProductionResult } from "./currentFixtures.ts";
import {
  disposeOwnedRoot,
  validateSyntheticFixture,
  syntheticFixtureReceiptSha256,
} from "../../../../scripts/jones/performance/guard.mjs";
import {
  fixtureCustodyReceipt,
  withClosedSyntheticFixture,
} from "../../../../scripts/jones/performance/fixtures.mjs";
import { currentDatabaseSource } from "../../../../scripts/jones/performance/sources.mjs";

const worktree = NodeFS.realpathSync(NodePath.resolve(import.meta.dirname, "../../../.."));
async function invocation<A>(
  use: (
    options: CurrentFixtureOptions,
    observe: (result: CurrentProductionResult<unknown>) => void,
  ) => Promise<A>,
) {
  const source = currentDatabaseSource(worktree);
  const parent = NodeFS.mkdtempSync(
    NodePath.join(NodePath.dirname(worktree), ".current-fixture-test-"),
  );
  const identity = NodeFS.lstatSync(parent);
  let unknown = false;
  const results: CurrentProductionResult<unknown>[] = [];
  const options: CurrentFixtureOptions = {
    parentPath: parent,
    childName: "fixture",
    producer: "current-v2",
    databaseSource: source,
    binding: {
      repository: source.repository,
      sourceRevision: source.sourceRevision,
      taskRef: "jones-salvage-fixtures",
      runId: NodeCrypto.randomUUID(),
    },
    policy: {
      homePath: parent,
      worktreePaths: [worktree],
      protectedPaths: [],
      maxFiles: 32,
      maxFileBytes: 32 * 1024 * 1024,
      maxTotalBytes: 64 * 1024 * 1024,
      maxReceiptBytes: 24 * 1024,
    },
  };
  try {
    return await use(options, (result) => results.push(result));
  } catch (error) {
    const evidence = (
      error as {
        evidence?: {
          cleanup?: { outcome?: string };
          childReceipt?: { closed: boolean; reaped: boolean; outcome: string };
          receipt?: { closure?: { completed: boolean } };
        };
      }
    ).evidence;
    if (
      evidence?.cleanup?.outcome === "retained" &&
      (!evidence.childReceipt?.closed ||
        !evidence.childReceipt.reaped ||
        evidence.childReceipt.outcome === "unknown" ||
        !evidence.receipt?.closure?.completed)
    )
      unknown = true;
    throw error;
  } finally {
    for (const result of results) {
      if (!result.closeKnown || disposeOwnedRoot(result.owner).outcome !== "complete")
        unknown = true;
    }
    const current = NodeFS.lstatSync(parent);
    if (
      !unknown &&
      current.dev === identity.dev &&
      current.ino === identity.ino &&
      !current.isSymbolicLink()
    )
      NodeFS.rmSync(parent, { recursive: true });
    // @effect-diagnostics-next-line globalConsole:off - Preserve retained-root evidence without a manual test runtime.
    else NodeConsole.error(`fixture test scratch retained: ${parent}`);
  }
}

describe("receiving V2 synthetic fixtures", () => {
  it.each(["health-offline-delete", "benchmark-wal"] as const)(
    "seeds coherent V2 history and closes %s with observed durability",
    async (profile) => {
      await invocation(async (options, observe) => {
        const result = await produceCurrentFixture({ ...options, profile }, () => undefined);
        observe(result);
        NodeAssert.equal(result.error, undefined);
        NodeAssert.equal(result.closeKnown, true);
        NodeAssert.equal(result.receipt?.schema, "jones-performance-fixture/v2");
        NodeAssert.equal(result.receipt?.producer, "current-v2");
        NodeAssert.deepEqual(result.receipt?.databaseSource, options.databaseSource);
        NodeAssert.equal(result.capture?.tables.orchestration_v2_projection_threads?.count, 2);
        NodeAssert.equal(result.capture?.tables.orchestration_v2_projection_runs?.count, 6);
        NodeAssert.equal(result.capture?.tables.orchestration_v2_projection_messages?.count, 12);
        // Receiving V2 commands use the shared application receipt table.
        NodeAssert.equal(result.capture?.tables.orchestration_v2_command_receipts?.count, 0);
        NodeAssert.equal(result.capture?.tables.orchestration_command_receipts?.count, 9);
        NodeAssert.deepEqual(result.capture?.commandReceipts, {
          total: 9,
          project: 1,
          thread: 8,
          replayed: 9,
          invalidEventLinks: 0,
        });
        NodeAssert.equal(result.capture?.tables.orchestration_events?.count, 21);
        NodeAssert.equal(result.capture?.ledgers.jones_sql_migrations?.length, 6);
        NodeAssert.equal(result.capture?.integrity.ok, true);
        NodeAssert.equal(result.capture?.foreignKeys.violations, 0);
        NodeAssert.equal(result.capture?.profile.productionObservations.length, 5);
        NodeAssert.equal(result.capture?.runtime.pragmas.journal_mode, "wal");
        NodeAssert.equal(result.capture?.runtime.pragmas.foreign_keys, 1);
        NodeAssert.equal(result.capture?.runtime.pragmas.busy_timeout, 5000);
        NodeAssert.equal(result.capture?.runtime.pragmas.journal_size_limit, 32 * 1024 * 1024);
        NodeAssert.ok(result.receipt);
        const custody = fixtureCustodyReceipt(result.receipt);
        const validated = await validateSyntheticFixture({
          receipt: custody,
          expectedReceiptSha256: syntheticFixtureReceiptSha256(custody),
          expectedBinding: options.binding,
          policy: options.policy,
        });
        const header = NodeFS.readFileSync(validated.canonicalPath).subarray(0, 100);
        NodeAssert.equal(header[18], profile === "health-offline-delete" ? 1 : 2);
        NodeAssert.equal(header[19], profile === "health-offline-delete" ? 1 : 2);
        if (profile === "health-offline-delete") {
          NodeAssert.deepEqual(result.capture?.profile.maintenance?.sidecars, {
            wal: false,
            shm: false,
            journal: false,
          });
          const db = new NodeSqlite.DatabaseSync(validated.canonicalPath, { readOnly: true });
          try {
            NodeAssert.deepEqual(
              db
                .prepare("PRAGMA integrity_check")
                .all()
                .map((row) => Object.values(row)[0]),
              ["ok"],
            );
          } finally {
            db.close();
          }
        }
      });
    },
  );
  it("oversized recipes refuse before allocating a fixture root", async () => {
    await invocation(async (options) => {
      for (const recipe of [
        { historyTurns: 257 },
        { threads: 17 },
        { threads: 16, historyTurns: 256, payloadBytes: 65536 },
      ]) {
        await NodeAssert.rejects(
          produceCurrentFixture({ ...options, recipe }, () => undefined),
          { code: "invalid_recipe" },
        );
        NodeAssert.equal(
          NodeFS.existsSync(NodePath.join(options.parentPath, options.childName)),
          false,
        );
      }
    });
  });
  it("callback failure closes the database and preserves the original error", async () => {
    await invocation(async (options, observe) => {
      const original = new Error("deliberate synthetic callback failure");
      const result = await produceCurrentFixture(options, () => {
        throw original;
      });
      observe(result);
      NodeAssert.equal(result.error, original);
      NodeAssert.equal(result.closeKnown, true);
      NodeAssert.equal(result.receipt, undefined);
    });
  });
  it("cancellation closes acquired resources before cleanup", async () => {
    await invocation(async (options, observe) => {
      const cancellation = new AbortController();
      const result = await produceCurrentFixture(
        { ...options, signal: cancellation.signal },
        async (context) => {
          cancellation.abort();
          await context.run(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              return yield* sql`SELECT 1`;
            }),
          );
        },
      );
      observe(result);
      NodeAssert.ok(result.error);
      NodeAssert.equal(result.closeKnown, true);
    });
  });
  it("abort interrupts an active effect and waits for its finalizer before closing", async () => {
    await invocation(async (options, observe) => {
      const cancellation = new AbortController();
      let effectStarted = false;
      let effectFinalized = false;
      let admittedAfterAbort = false;
      const result = await produceCurrentFixture(
        { ...options, signal: cancellation.signal },
        async (context) => {
          let acknowledgeStarted: () => void = () => {};
          const started = new Promise<void>((resolve) => {
            acknowledgeStarted = resolve;
          });
          const pending = context.run(
            Effect.sync(() => {
              effectStarted = true;
              acknowledgeStarted();
            }).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(
                Effect.sync(() => {
                  effectFinalized = true;
                }),
              ),
            ),
          );
          void pending.catch(() => {});
          await started;
          const reason = new Error("synthetic fixture cancellation");
          cancellation.abort(reason);
          await NodeAssert.rejects(
            context.run(
              Effect.sync(() => {
                admittedAfterAbort = true;
              }),
            ),
            reason,
          );
          await pending;
        },
      );
      observe(result);
      NodeAssert.ok(result.error);
      NodeAssert.equal(effectStarted, true);
      NodeAssert.equal(effectFinalized, true);
      NodeAssert.equal(admittedAfterAbort, false);
      NodeAssert.equal(result.closeKnown, true);
      NodeAssert.equal(result.receipt, undefined);
    });
  });
  it("raw consumer report retains its known closed fixture", async () => {
    await invocation(async (options) => {
      const rawReport = { schema: "jones.sqlite-health/v1", outcome: "passed" } as const;
      await NodeAssert.rejects(
        // @ts-expect-error A raw diagnostic report cannot acknowledge fixture release.
        withClosedSyntheticFixture(options, () => rawReport),
        (error: unknown) => {
          const failure = error as {
            code?: string;
            evidence?: {
              cleanup?: { outcome?: string; reason?: string };
              childReceipt?: { closed: boolean; reaped: boolean };
            };
          };
          NodeAssert.equal(failure.code, "invalid_consumer_outcome");
          NodeAssert.equal(failure.evidence?.cleanup?.outcome, "retained");
          NodeAssert.equal(failure.evidence?.cleanup?.reason, "invalid_consumer_outcome");
          NodeAssert.equal(failure.evidence?.childReceipt?.closed, true);
          NodeAssert.equal(failure.evidence?.childReceipt?.reaped, true);
          return true;
        },
      );
    });
  });
  it("closed current fixture releases only through matching typed consumer acknowledgment", async () => {
    await invocation(async (options) => {
      const result = await withClosedSyntheticFixture(options, (context) => {
        NodeAssert.equal(context.childReceipt.closed, true);
        NodeAssert.equal(context.childReceipt.reaped, true);
        NodeAssert.equal(context.receipt.schema, "jones-performance-fixture/v2");
        return {
          schema: "jones-performance-fixture-consumer/v1",
          fixtureReceiptSha256: context.receiptSha256,
          disposition: "release",
          value: context.capture.integrity.ok,
        };
      });
      NodeAssert.equal(result.value, true);
      NodeAssert.equal(result.cleanup.outcome, "complete");
      NodeAssert.equal(result.cleanup.absent, true);
    });
  });
});
