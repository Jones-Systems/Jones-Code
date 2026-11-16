import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeTest from "node:test";
import * as NodeURL from "node:url";

import {
  captureSyntheticFixture,
  withClosedSyntheticFixture,
  withOpenSyntheticFixture,
} from "./fixtures.mjs";
import { produceFixture } from "./fixtures-historical-worker.mjs";
import {
  assertOwnedDatabase,
  createOwnedRoot,
  disposeOwnedRoot,
  syntheticFixtureReceiptSha256,
  validateSyntheticFixture,
} from "./guard.mjs";

import {
  assertSyntheticDatabaseSource,
  sourceParentEnvironment,
  syntheticDatabaseSource,
} from "./sources.mjs";

function historicalTest(name, ...args) {
  if (!process.env[sourceParentEnvironment])
    return NodeTest.test(name, { skip: "historical source parent unbound" }, () => {});
  return NodeTest.test(name, ...args);
}

const directory = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const worktree = NodePath.resolve(directory, "../../..");
const oldSource = process.env[sourceParentEnvironment]
  ? syntheticDatabaseSource("e5a31aceec91484b64315c63dcce80f6e7581604")
  : undefined;
const liveSource = process.env[sourceParentEnvironment]
  ? syntheticDatabaseSource("414bb8da204c3275cd0b76b2ec4d74dfb09a97e4")
  : undefined;
const producerBinding = {
  repository: "Jones-Systems/Jones-Code",
  sourceRevision: "67e203c3306b25bca104efbc449e10ebae384763",
  taskRef: "spec.jones-performance-portfolio#task.e-fixture.001",
  runId: "fixture-tests",
};
const liveProjectorNames = [
  "projection.projects",
  "projection.threads",
  "projection.thread-messages",
  "projection.thread-proposed-plans",
  "projection.thread-activities",
  "projection.thread-sessions",
  "projection.thread-turns",
  "projection.checkpoints",
  "projection.pending-approvals",
];

async function withInvocation(body) {
  const outer = await NodeFSP.mkdtemp(
    NodePath.join(NodePath.dirname(worktree), ".historical-fixture-test-"),
  );
  const identity = await NodeFSP.lstat(outer, { bigint: true });
  const canary = NodePath.join(outer, "synthetic-protected.sqlite");
  const cancellation = new AbortController();
  const pending = [];
  const observations = [];
  const roots = [];
  const policy = {
    homePath: worktree,
    worktreePaths: [worktree],
    protectedPaths: [canary],
    maxFiles: 32,
    maxFileBytes: 32 * 1024 * 1024,
    maxTotalBytes: 64 * 1024 * 1024,
    maxReceiptBytes: 24 * 1024,
  };
  const options = (source = oldSource, overrides = {}) => {
    const childName = `fixture-${roots.length}`;
    roots.push(NodePath.join(outer, childName));
    return {
      parentPath: outer,
      childName,
      binding: { ...producerBinding, runId: NodeCrypto.randomUUID() },
      policy,
      producer: "historical-v1",
      databaseSource: source,
      ...overrides,
      signal: overrides.signal
        ? AbortSignal.any([cancellation.signal, overrides.signal])
        : cancellation.signal,
    };
  };
  const track = (promise) => {
    const observed = promise.then(
      (result) => {
        observations.push(result);
        return result;
      },
      (error) => {
        observations.push(error.evidence ?? {});
        throw error;
      },
    );
    pending.push(observed);
    void observed.catch(() => {});
    return observed;
  };
  const scope = {
    outer,
    canary,
    policy,
    options,
    open: (input, use) => track(withOpenSyntheticFixture(input, use)),
    closed: (input, use) => track(withClosedSyntheticFixture(input, use)),
    produce: (input, use) => track(produceFixture(input, use)),
  };
  let value;
  let failure;
  let canaryReady = false;
  try {
    await NodeFSP.writeFile(canary, "synthetic protected canary\n", { flag: "wx", mode: 0o600 });
    canaryReady = true;
    value = await body(scope);
  } catch (error) {
    failure = error;
  } finally {
    cancellation.abort();
    await Promise.allSettled(pending);
    try {
      const children = observations.flatMap((observation) =>
        observation.childReceipt ? [observation.childReceipt] : [],
      );
      if (
        children.some((child) => !child.closed || !child.reaped || child.outcome === "unknown") ||
        observations.some(
          (observation) => observation.cleanup && observation.cleanup.outcome !== "complete",
        ) ||
        roots.some((root) => NodeFS.existsSync(root))
      )
        throw new Error(`Fixture scratch retained at ${outer}; closure or cleanup is unproved`);
      if (canaryReady)
        NodeAssert.equal(await NodeFSP.readFile(canary, "utf8"), "synthetic protected canary\n");
      const entries = await NodeFSP.readdir(outer);
      NodeAssert.ok(entries.every((entry) => entry === NodePath.basename(canary)));
      const current = await NodeFSP.lstat(outer, { bigint: true });
      NodeAssert.equal(current.dev, identity.dev);
      NodeAssert.equal(current.ino, identity.ino);
      if (entries.length) {
        const canaryIdentity = await NodeFSP.lstat(canary);
        NodeAssert.equal(canaryIdentity.isFile(), true);
        NodeAssert.equal(canaryIdentity.nlink, 1);
        await NodeFSP.unlink(canary);
      }
      await NodeFSP.rmdir(outer);
      if (failure instanceof Error)
        failure.scratchEvidence = { outer, absent: !NodeFS.existsSync(outer), children };
    } catch (cleanupError) {
      failure = failure
        ? new AggregateError([failure, cleanupError], "Body failure and retained fixture scratch", {
            cause: failure,
          })
        : cleanupError;
    }
  }
  if (failure) throw failure;
  return value;
}

async function withInjectedConsumerOutcome(use, inspect) {
  return withInvocation(async (scope) => {
    const testOwner = createOwnedRoot({
      parentPath: scope.outer,
      childName: "consumer-test-owner",
      binding: { ...producerBinding, runId: NodeCrypto.randomUUID() },
      policy: { ...scope.policy, maxFiles: 64 },
    });
    const cancellation = new AbortController();
    const options = {
      parentPath: testOwner.creationReceipt.canonicalRootPath,
      childName: "consumer",
      binding: { ...producerBinding, runId: NodeCrypto.randomUUID() },
      policy: scope.policy,
      producer: "historical-v1",
      databaseSource: liveSource,
      signal: cancellation.signal,
    };
    let pending;
    let context;
    let rejected;
    let cleanup;
    let failure;
    try {
      pending = withClosedSyntheticFixture(options, (fixture) => {
        context = fixture;
        return use(fixture);
      });
      rejected = await pending.then(
        () => {
          throw new Error("injected consumer outcome unexpectedly released the fixture");
        },
        (error) => error,
      );
      NodeAssert.equal(rejected.evidence.cleanup.outcome, "retained");
      NodeAssert.equal(rejected.evidence.cleanup.absent, false);
      NodeAssert.equal(
        NodeFS.existsSync(rejected.evidence.creationReceipt.canonicalRootPath),
        true,
      );
      NodeAssert.equal(NodeFS.existsSync(context.fixture.canonicalPath), true);
      await inspect(rejected, context);
    } catch (error) {
      failure = error;
    } finally {
      cancellation.abort();
      if (pending) await pending.catch(() => {});
      try {
        // These injected consumers open no resources. Only this original test owner plus the captured
        // producer close/reap and unchanged sealed fixture permit cleanup; an unknown producer retains it.
        NodeAssert.ok(context, "producer closure was not captured; retain the test enclosure");
        NodeAssert.strictEqual(rejected.evidence.childReceipt, context.childReceipt);
        NodeAssert.equal(context.childReceipt.closed, true);
        NodeAssert.equal(context.childReceipt.reaped, true);
        NodeAssert.equal(context.childReceipt.outcome, "success");
        NodeAssert.equal(context.childReceipt.truncated, false);
        NodeAssert.ok(
          ["invalid_consumer_outcome", "consumer_retained", "consumer_outcome_unproved"].includes(
            rejected.evidence.cleanup.reason,
          ),
        );
        const consumerRoot = NodePath.join(
          testOwner.creationReceipt.canonicalRootPath,
          options.childName,
        );
        NodeAssert.equal(rejected.evidence.creationReceipt.canonicalRootPath, consumerRoot);
        NodeAssert.equal(
          context.receipt.creationReceipt.canonicalRootPath,
          NodePath.join(consumerRoot, "fixture"),
        );
        NodeAssert.equal(context.receipt.closure.completed, true);
        NodeAssert.strictEqual(rejected.evidence.receipt, context.receipt);
        NodeAssert.equal(context.fixture.receiptSha256, context.receiptSha256);
        const pinned = await validateSyntheticFixture({
          receipt: context.receipt,
          expectedReceiptSha256: context.receiptSha256,
          expectedBinding: options.binding,
          policy: scope.policy,
        });
        NodeAssert.equal(pinned.canonicalPath, context.fixture.canonicalPath);
        cleanup = disposeOwnedRoot(testOwner);
        NodeAssert.equal(cleanup.outcome, "complete");
        NodeAssert.equal(cleanup.absent, true);
      } catch (cleanupError) {
        const primary = failure ?? rejected;
        failure = primary
          ? new AggregateError(
              [primary, cleanupError],
              "Consumer test failure and retained scratch",
              { cause: primary },
            )
          : cleanupError;
      }
    }
    if (failure) throw failure;
    return { rejected, cleanup };
  });
}

function assertCoherent(capture, source, profile = "observed-production-defaults") {
  NodeAssert.deepEqual(capture.databaseSource, source);
  NodeAssert.equal(capture.runtime.profile, profile);
  NodeAssert.equal(capture.runtime.pragmas.journal_mode, "wal");
  NodeAssert.equal(capture.runtime.pragmas.foreign_keys, 1);
  NodeAssert.equal(capture.ledgers.effect_sql_migrations.at(-1).id, 54);
  NodeAssert.equal(capture.ledgers.jones_sql_migrations.at(-1).id, source === oldSource ? 4 : 2);
  NodeAssert.ok(capture.tables.orchestration_events.count > 20);
  NodeAssert.ok(capture.tables.orchestration_command_receipts.count > 20);
  NodeAssert.equal(capture.tables.projection_thread_messages.count, 6);
  NodeAssert.equal(capture.tables.projection_turns.count, 3);
  NodeAssert.equal(capture.tables.provider_session_runtime.count, 1);
  NodeAssert.equal(capture.tables.checkpoint_diff_blobs.count, 1);
  NodeAssert.equal(capture.tables.worktree_ownership_leases.count, 1);
  NodeAssert.equal(capture.coupling.missing_receipt_events, 0);
  NodeAssert.equal(capture.coupling.missing_thread_projects, 0);
  NodeAssert.equal(capture.coupling.missing_message_threads, 0);
  NodeAssert.equal(capture.coupling.noncontiguous_streams, 0);
  NodeAssert.equal(capture.coupling.snapshotSequence, capture.coupling.maxSequence);
  NodeAssert.equal(capture.coupling.projectionCursors.length, 10);
  NodeAssert.equal(capture.tables.projection_state.count, 10);
  const liveCursors = capture.coupling.projectionCursors.filter((row) =>
    liveProjectorNames.includes(row.projector),
  );
  NodeAssert.deepEqual(
    liveCursors.map((row) => row.projector).toSorted(),
    liveProjectorNames.toSorted(),
  );
  NodeAssert.ok(liveCursors.every((row) => row.sequence === capture.coupling.maxSequence));
  const bootstrapCursors = capture.coupling.projectionCursors.filter(
    (row) => row.projector === "projection.attachment-cleanup",
  );
  NodeAssert.equal(bootstrapCursors.length, 1);
  NodeAssert.equal(bootstrapCursors[0].sequence, 0);
  NodeAssert.ok(capture.coupling.maxSequence > bootstrapCursors[0].sequence);
  NodeAssert.equal(capture.readModel.projectCount, 1);
  NodeAssert.equal(capture.readModel.threadCount, source === oldSource ? 4 : 3);
  NodeAssert.equal(capture.readModel.historyMessages, 6);
  NodeAssert.equal(capture.readModel.equivalent, true);
  NodeAssert.equal(capture.readModel.snapshotSha256, capture.readModel.replaySha256);
  NodeAssert.equal(capture.pages.recentMessageIds.length, 4);
  NodeAssert.equal(capture.pages.olderMessageIds.length, 2);
  NodeAssert.equal(capture.pages.overlap, 0);
  NodeAssert.deepEqual(
    [...capture.pages.recentMessageIds, ...capture.pages.olderMessageIds].sort(),
    [
      "fixture-assistant-1",
      "fixture-assistant-2",
      "fixture-assistant-3",
      "fixture-user-1",
      "fixture-user-2",
      "fixture-user-3",
    ],
  );
  NodeAssert.equal(capture.leaseFencing.staleRenewed, false);
  NodeAssert.equal(capture.leaseFencing.staleReleasePreservedCurrent, true);
  NodeAssert.equal(capture.leaseFencing.foreignAcquired, false);
  NodeAssert.deepEqual(capture.integrity, { results: ["ok"], ok: true });
  NodeAssert.equal(capture.foreignKeys.violations, 0);
  NodeAssert.equal(capture.files.length, 6);
  NodeAssert.equal(capture.attachmentFiles.length, 1);
  NodeAssert.ok(
    capture.files.some(
      (file) =>
        file.relativePath === capture.attachmentFiles[0].relativePath &&
        file.sha256 === capture.attachmentFiles[0].sha256,
    ),
  );
  NodeAssert.deepEqual(capture.references.workspaces, ["workspace"]);
  NodeAssert.deepEqual(capture.references.worktrees, ["worktrees/fixture"]);
  NodeAssert.deepEqual(capture.references.checkpointFiles, ["worktrees/fixture/fixture.txt"]);
  NodeAssert.equal(capture.blobs.length, 1);
  NodeAssert.equal(capture.blobs[0].toTurnCount, 3);
}

historicalTest(
  "e5 genuine open context produces coupled state, native history and historical closed evidence",
  async () => {
    await withInvocation(async (scope) => {
      let originalContext;
      const result = await scope
        .open(scope.options(), async (context) => {
          originalContext = context;
          const capture = await captureSyntheticFixture(context);
          assertCoherent(capture, oldSource);
          const empty = await context.run(
            context.snapshotQuery.getThreadDetailById("fixture-empty"),
          );
          NodeAssert.equal(empty._tag, "Some");
          NodeAssert.equal(empty.value.messages.length, 0);
          NodeAssert.equal(empty.value.activities.length, 0);
          NodeAssert.equal(capture.native.status, "present");
          NodeAssert.deepEqual(capture.native.effectPhases, ["started", "completed"]);
          NodeAssert.match(capture.native.normalizedCommandDigest, /^[a-f0-9]{64}$/);
          NodeAssert.equal(capture.tables.native_creation_intents.count, 1);
          NodeAssert.equal(capture.tables.native_creation_reserved_command_identities.count, 2);
          NodeAssert.equal(capture.tables.native_creation_reserved_commands.count, 2);
          NodeAssert.equal(capture.tables.native_creation_normalized_commands.count, 1);
          NodeAssert.equal(capture.tables.native_creation_effect_facts.count, 2);
          return "open-result";
        })
        .catch((error) => {
          const capture = error?.evidence?.primaryEvidence;
          if (error instanceof Error && capture?.schema === "jones-performance-capture/v1") {
            const diagnostic = JSON.stringify({
              sourceRevision: capture.databaseSource.sourceRevision,
              counts: {
                events: capture.tables.orchestration_events.count,
                receipts: capture.tables.orchestration_command_receipts.count,
                projects: capture.readModel.projectCount,
                threads: capture.readModel.threadCount,
                historyMessages: capture.readModel.historyMessages,
                cursors: capture.coupling.projectionCursors.length,
              },
              coupling: {
                ...capture.coupling,
                projectionCursors: capture.coupling.projectionCursors.slice(0, 10),
              },
              readModel: capture.readModel,
              pages: {
                overlap: capture.pages.overlap,
                recentCount: capture.pages.recentMessageIds.length,
                olderCount: capture.pages.olderMessageIds.length,
              },
              integrity: {
                ok: capture.integrity.ok,
                resultCount: capture.integrity.results.length,
              },
              foreignKeyViolations: capture.foreignKeys.violations,
              native: {
                status: capture.native.status,
                effectPhases: capture.native.effectPhases?.slice(0, 4),
              },
            });
            const suffix =
              Buffer.byteLength(diagnostic) <= 4 * 1024
                ? `\nfixture capture: ${diagnostic}`
                : "\nfixture capture: 4 KiB diagnostic budget exceeded";
            if (Buffer.byteLength(error.message + suffix) <= 8 * 1024) error.message += suffix;
          }
          throw error;
        });
      NodeAssert.equal(result.value, "open-result");
      NodeAssert.equal(result.cleanup.outcome, "complete");
      NodeAssert.equal(result.cleanup.absent, true);
      NodeAssert.equal(result.receipt.closure.completed, true);
      NodeAssert.equal(result.receipt.producerStep, producerBinding.taskRef);
      NodeAssert.equal(result.receiptSha256, syntheticFixtureReceiptSha256(result.receipt));
      NodeAssert.equal(NodeFS.existsSync(result.receipt.creationReceipt.canonicalRootPath), false);
      await NodeAssert.rejects(captureSyntheticFixture(originalContext), {
        code: "invalid_context",
      });
      NodeAssert.throws(() => originalContext.run(originalContext.engine.latestSequence), {
        code: "closed_context",
      });
    });
  },
);

historicalTest(
  "414 closed leaf pins a compact full receipt, absent native schema and root-contained files",
  async () => {
    await withInvocation(async (scope) => {
      const options = scope.options(liveSource, {
        recipe: { kind: "coherent-v1", historyTurns: 3, payloadBytes: 64 * 1024 },
      });
      const result = await scope.closed(options, async (context) => {
        assertCoherent(context.capture, liveSource);
        NodeAssert.equal(context.capture.native.status, "absent");
        for (const [table, summary] of Object.entries(context.capture.tables))
          if (table.startsWith("native_creation_"))
            NodeAssert.deepEqual(summary, { status: "absent" });
        NodeAssert.equal(context.fixture.access, "readonly");
        NodeAssert.equal(context.childReceipt.closed, true);
        NodeAssert.equal(context.childReceipt.reaped, true);
        NodeAssert.equal(context.childReceipt.outcome, "success");
        NodeAssert.equal(context.childReceipt.stderr, "");
        NodeAssert.ok(Buffer.byteLength(context.childReceipt.stdout) < 49 * 1024);
        NodeAssert.ok(Buffer.byteLength(`${JSON.stringify(context.receipt)}\n`) < 24 * 1024);
        NodeAssert.ok(
          context.fixture.verifiedBytes > 10 * Buffer.byteLength(context.childReceipt.stdout),
        );
        NodeAssert.equal(
          context.receipt.creationReceipt.binding.sourceRevision,
          producerBinding.sourceRevision,
        );
        NodeAssert.notEqual(
          context.receipt.creationReceipt.binding.sourceRevision,
          context.databaseSource.sourceRevision,
        );
        await NodeAssert.rejects(
          validateSyntheticFixture({
            receipt: context.receipt,
            expectedReceiptSha256: "0".repeat(64),
            expectedBinding: options.binding,
            policy: scope.policy,
          }),
          { code: "receipt_mismatch" },
        );
        const pinned = await validateSyntheticFixture({
          receipt: JSON.parse(JSON.stringify(context.receipt)),
          expectedReceiptSha256: context.receiptSha256,
          expectedBinding: options.binding,
          policy: scope.policy,
        });
        NodeAssert.equal(pinned.canonicalPath, context.fixture.canonicalPath);
        const file = await NodeFSP.open(pinned.canonicalPath, "r");
        try {
          const header = Buffer.alloc(20);
          await file.read(header, 0, header.length, 0);
          NodeAssert.equal(header.subarray(0, 16).toString(), "SQLite format 3\u0000");
          NodeAssert.equal(header[18], 2);
          NodeAssert.equal(header[19], 2);
        } finally {
          await file.close();
        }
        return {
          schema: "jones-performance-fixture-consumer/v1",
          fixtureReceiptSha256: context.receiptSha256,
          disposition: "release",
          value: context.fixture.canonicalPath,
        };
      });
      NodeAssert.equal(result.cleanup.outcome, "complete");
      NodeAssert.equal(result.childReceipt.closed, true);
      NodeAssert.equal(result.childReceipt.reaped, true);
      NodeAssert.equal(NodeFS.existsSync(result.value), false);
      NodeAssert.equal(NodeFS.existsSync(result.cleanup.creationReceipt.canonicalRootPath), false);
    });
  },
);

historicalTest(
  "callback failure closes the real runtime before exact local cleanup and preserves the error",
  async () => {
    await withInvocation(async (scope) => {
      const original = new Error("deliberate callback failure");
      const rejected = await scope
        .open(scope.options(), (context) => {
          void context.run(context.engine.latestSequence);
          throw original;
        })
        .catch((error) => error);
      NodeAssert.equal(rejected, original);
      NodeAssert.equal(rejected.evidence.cleanup.outcome, "complete");
      NodeAssert.equal(rejected.evidence.cleanup.absent, true);
      NodeAssert.equal(rejected.evidence.receipt, undefined);
    });
  },
);

historicalTest(
  "body failure after a leaf starts aborts and reaps it before outer scratch removal",
  async () => {
    const original = new Error("deliberate body failure after child start");
    const rejected = await withInvocation((scope) => {
      void scope.closed(scope.options(), () => {
        throw new Error("cancelled callback must not run");
      });
      throw original;
    }).catch((error) => error);
    NodeAssert.equal(rejected, original);
    NodeAssert.equal(rejected.scratchEvidence.absent, true);
    NodeAssert.equal(rejected.scratchEvidence.children.length, 1);
    NodeAssert.ok(rejected.scratchEvidence.children[0].pid > 0);
    NodeAssert.equal(rejected.scratchEvidence.children[0].closed, true);
    NodeAssert.equal(rejected.scratchEvidence.children[0].reaped, true);
    NodeAssert.equal(rejected.scratchEvidence.children[0].outcome, "cancelled");
  },
);

historicalTest(
  "pre-cancellation records no spawned child and cleans its original enclosing root",
  async () => {
    await withInvocation(async (scope) => {
      const cancellation = new AbortController();
      cancellation.abort();
      const rejected = await scope
        .closed(scope.options(oldSource, { signal: cancellation.signal }), () => undefined)
        .catch((error) => error);
      NodeAssert.equal(rejected.code, "cancelled");
      NodeAssert.equal(rejected.evidence.childReceipt.pid, null);
      NodeAssert.equal(rejected.evidence.childReceipt.closed, true);
      NodeAssert.equal(rejected.evidence.cleanup.outcome, "complete");
    });
  },
);

historicalTest(
  "source aliases, unsupported profile selectors, recipe overflow and cloned contexts are refused",
  async () => {
    await withInvocation(async (scope) => {
      await NodeAssert.rejects(
        scope.open(
          scope.options({ ...oldSource, worktreePath: `${oldSource.worktreePath}/.` }),
          () => undefined,
        ),
        { code: "invalid_source" },
      );
      await NodeAssert.rejects(
        scope.open(scope.options(oldSource, { profile: "copied-wal" }), () => undefined),
        { code: "unsupported_profile" },
      );
      await NodeAssert.rejects(
        scope.open(scope.options(oldSource, { recipe: { historyTurns: 2 } }), () => undefined),
        { code: "invalid_recipe" },
      );
      await NodeAssert.rejects(
        scope.open(scope.options(oldSource, { recipe: { payloadBytes: 65537 } }), () => undefined),
        { code: "invalid_recipe" },
      );
      await NodeAssert.rejects(captureSyntheticFixture({ owner: {}, paths: {} }), {
        code: "invalid_context",
      });
    });
  },
);

historicalTest(
  "oversized fixed request refuses before child spawn and cleans only the allocated root",
  async () => {
    await withInvocation(async (scope) => {
      const options = scope.options(oldSource, {
        policy: {
          ...scope.policy,
          protectedPaths: Array.from({ length: 160 }, (_, index) =>
            NodePath.join(scope.outer, `${"x".repeat(320)}-${index}`),
          ),
        },
      });
      const rejected = await scope.closed(options, () => undefined).catch((error) => error);
      NodeAssert.equal(rejected.code, "request_limit");
      NodeAssert.equal(rejected.evidence.childReceipt, undefined);
      NodeAssert.equal(rejected.evidence.cleanup.outcome, "complete");
    });
  },
);

historicalTest("closed consumer missing outcome retains its known closed fixture", async () => {
  const result = await withInjectedConsumerOutcome(
    () => undefined,
    (error) => {
      NodeAssert.equal(error.code, "invalid_consumer_outcome");
      NodeAssert.equal(error.evidence.cleanup.reason, "invalid_consumer_outcome");
      NodeAssert.equal(Object.hasOwn(error.evidence, "value"), false);
    },
  );
  NodeAssert.equal(result.cleanup.absent, true);
});

historicalTest(
  "closed consumer malformed outcome cannot release even with the genuine pin",
  async () => {
    await withInjectedConsumerOutcome(
      (context) => ({
        schema: "jones-performance-fixture-consumer/v1",
        fixtureReceiptSha256: context.receiptSha256,
        disposition: "release",
      }),
      (error) => {
        NodeAssert.equal(error.code, "invalid_consumer_outcome");
        NodeAssert.equal(error.evidence.cleanup.reason, "invalid_consumer_outcome");
        NodeAssert.equal(Object.hasOwn(error.evidence, "value"), false);
      },
    );
  },
);

historicalTest("closed consumer raw diagnostic envelope does not acknowledge release", async () => {
  await withInjectedConsumerOutcome(
    () => ({ status: "refused", classification: "unknown", findings: [] }),
    (error) => {
      NodeAssert.equal(error.code, "invalid_consumer_outcome");
      NodeAssert.equal(error.evidence.cleanup.reason, "invalid_consumer_outcome");
      NodeAssert.equal(Object.hasOwn(error.evidence, "value"), false);
    },
  );
});

historicalTest("closed consumer release for a different receipt retains the fixture", async () => {
  await withInjectedConsumerOutcome(
    () => ({
      schema: "jones-performance-fixture-consumer/v1",
      fixtureReceiptSha256: "0".repeat(64),
      disposition: "release",
      value: "wrong fixture",
    }),
    (error) => {
      NodeAssert.equal(error.code, "invalid_consumer_outcome");
      NodeAssert.equal(error.evidence.cleanup.reason, "invalid_consumer_outcome");
      NodeAssert.equal(Object.hasOwn(error.evidence, "value"), false);
    },
  );
});

historicalTest(
  "closed consumer retains an injected unknown diagnostic despite genuine producer closure",
  async () => {
    const diagnostic = Object.freeze({ classification: "unknown" });
    await withInjectedConsumerOutcome(
      (context) => ({
        schema: "jones-performance-fixture-consumer/v1",
        fixtureReceiptSha256: context.receiptSha256,
        disposition: "retain",
        value: diagnostic,
      }),
      (error) => {
        NodeAssert.equal(error.code, "cleanup_retained");
        NodeAssert.equal(error.evidence.cleanup.reason, "consumer_retained");
        NodeAssert.strictEqual(error.evidence.value, diagnostic);
      },
    );
  },
);

historicalTest(
  "closed consumer rejection retains evidence and preserves its original failure",
  async () => {
    const original = new Error("deliberate closed consumer rejection");
    const result = await withInjectedConsumerOutcome(
      () => Promise.reject(original),
      (error) => {
        NodeAssert.strictEqual(error, original);
        NodeAssert.equal(error.evidence.cleanup.reason, "consumer_outcome_unproved");
      },
    );
    NodeAssert.strictEqual(result.rejected, original);
    NodeAssert.equal(result.cleanup.absent, true);
  },
);

historicalTest(
  "closed consumer diagnostic failure releases only with a matching explicit outcome",
  async () => {
    await withInvocation(async (scope) => {
      const diagnostic = Object.freeze({
        status: "failed",
        classification: "closed",
        findings: ["synthetic diagnostic failure"],
      });
      const result = await scope.closed(scope.options(liveSource), (context) => {
        NodeAssert.equal(context.childReceipt.closed, true);
        NodeAssert.equal(context.childReceipt.reaped, true);
        return {
          schema: "jones-performance-fixture-consumer/v1",
          fixtureReceiptSha256: context.receiptSha256,
          disposition: "release",
          value: diagnostic,
        };
      });
      NodeAssert.strictEqual(result.value, diagnostic);
      NodeAssert.equal(result.cleanup.outcome, "complete");
      NodeAssert.equal(result.cleanup.absent, true);
      NodeAssert.equal(NodeFS.existsSync(result.receipt.creationReceipt.canonicalRootPath), false);
    });
  },
);

const profilePhases = [
  "before-seed",
  "after-seed",
  "before-callback",
  "after-callback",
  "before-production-close",
];

async function appendProfileActivity(context, suffix) {
  const createdAt = "2026-10-02T13:00:00.000Z";
  await context.run(
    context.engine.dispatch({
      type: "thread.activity.append",
      commandId: `profile-activity-${suffix}`,
      threadId: "fixture-empty",
      activity: {
        id: `profile-activity-${suffix}`,
        kind: "fixture.note",
        summary: "Synthetic profile workload",
        tone: "info",
        turnId: null,
        payload: { synthetic: true },
        createdAt,
      },
      createdAt,
    }),
  );
}

async function withKnownClosedProfileFailure(profile, use, inspect, closeAuxiliary = () => {}) {
  return withInvocation(async (scope) => {
    let produced;
    let failure;
    try {
      produced = await scope.produce(scope.options(oldSource, { profile }), use);
      NodeAssert.ok(produced.error);
      NodeAssert.equal(produced.receipt, undefined);
      NodeAssert.ok(produced.retainReason);
      NodeAssert.equal(NodeFS.existsSync(produced.owner.creationReceipt.canonicalRootPath), true);
      await inspect(produced);
    } catch (error) {
      failure = error;
    } finally {
      try {
        await closeAuxiliary();
        NodeAssert.equal(
          produced?.closeKnown,
          true,
          "unknown resource closure retains this fixture",
        );
        produced.cleanup = disposeOwnedRoot(produced.owner);
        NodeAssert.equal(produced.cleanup.outcome, "complete");
        NodeAssert.equal(produced.cleanup.absent, true);
      } catch (cleanupError) {
        failure = failure
          ? new AggregateError(
              [failure, cleanupError],
              "Profile test failure and retained scratch",
              {
                cause: failure,
              },
            )
          : cleanupError;
      }
    }
    if (failure) throw failure;
    return produced;
  });
}

for (const [label, source] of [
  ["e5", oldSource],
  ["414", liveSource],
]) {
  historicalTest(
    `${label} named profiles seed independent roots and close health before readonly use`,
    async () => {
      await withInvocation(async (scope) => {
        const recipe = { kind: "coherent-v1", historyTurns: 3, payloadBytes: 256 };
        const healthOptions = scope.options(source, { profile: "health-offline-delete", recipe });
        let healthPath;
        let benchmarkContext;
        const health = await scope.closed(healthOptions, async (context) => {
          healthPath = context.fixture.canonicalPath;
          assertCoherent(context.capture, source, "health-offline-delete");
          const profile = context.capture.profile;
          NodeAssert.equal(profile.stage, "sealed");
          NodeAssert.equal(profile.productionClosed, true);
          NodeAssert.deepEqual(
            profile.productionObservations.map((entry) => entry.phase),
            profilePhases,
          );
          NodeAssert.ok(
            profile.productionObservations.every((entry) => entry.pragmas.journal_mode === "wal"),
          );
          NodeAssert.equal(
            profile.canonicalContent.originalSha256,
            profile.canonicalContent.beforeSha256,
          );
          NodeAssert.equal(
            profile.canonicalContent.originalSha256,
            profile.canonicalContent.afterSha256,
          );
          NodeAssert.equal(profile.maintenance.beforePragmas.journal_mode, "wal");
          NodeAssert.equal(profile.maintenance.returnedMode, "delete");
          NodeAssert.equal(profile.maintenance.afterPragmas.journal_mode, "delete");
          NodeAssert.equal(profile.maintenance.checkpoint.busy, 0);
          NodeAssert.equal(
            profile.maintenance.checkpoint.logFrames,
            profile.maintenance.checkpoint.checkpointedFrames,
          );
          NodeAssert.equal(profile.maintenance.closed, true);
          NodeAssert.equal(profile.maintenance.integrity.ok, true);
          NodeAssert.equal(profile.maintenance.foreignKeys.violations, 0);
          NodeAssert.deepEqual(profile.maintenance.header, {
            bytesRead: 20,
            writeVersion: 1,
            readVersion: 1,
          });
          NodeAssert.deepEqual(profile.maintenance.sidecars, {
            wal: false,
            shm: false,
            journal: false,
          });
          NodeAssert.ok(context.receipt.layout.slice(1).every((entry) => entry.present === false));
          const reader = new NodeSqlite.DatabaseSync(healthPath, { readOnly: true });
          try {
            NodeAssert.equal(reader.prepare("PRAGMA journal_mode").get().journal_mode, "delete");
            NodeAssert.equal(
              reader.prepare("SELECT count(*) AS n FROM projection_thread_messages").get().n,
              6,
            );
          } finally {
            reader.close();
          }
          const verifiedAfterRead = await validateSyntheticFixture({
            receipt: context.receipt,
            expectedReceiptSha256: context.receiptSha256,
            expectedBinding: healthOptions.binding,
            policy: scope.policy,
          });
          NodeAssert.deepEqual(verifiedAfterRead.layout, context.fixture.layout);
          const benchmark = await scope.open(
            scope.options(source, { profile: "benchmark-wal", recipe }),
            async (open) => {
              benchmarkContext = open;
              NodeAssert.notEqual(
                open.owner.creationReceipt.rootId,
                context.receipt.creationReceipt.rootId,
              );
              NodeAssert.notEqual(open.paths.dbPath, healthPath);
              const [healthIdentity, benchmarkIdentity] = await Promise.all([
                NodeFSP.stat(healthPath, { bigint: true }),
                NodeFSP.stat(open.paths.dbPath, { bigint: true }),
              ]);
              NodeAssert.ok(
                healthIdentity.dev !== benchmarkIdentity.dev ||
                  healthIdentity.ino !== benchmarkIdentity.ino,
              );
              NodeAssert.throws(
                () =>
                  assertOwnedDatabase(open.owner, {
                    databaseRelativePath: NodePath.relative(
                      open.owner.creationReceipt.canonicalRootPath,
                      open.paths.dbPath,
                    ),
                    access: "readwrite",
                  }),
                { code: "unknown_close" },
              );
              const during = await captureSyntheticFixture(open);
              assertCoherent(during, source, "benchmark-wal");
              NodeAssert.equal(during.profile.productionObservations.length, 3);
              await appendProfileActivity(open, label);
              return "bounded workload";
            },
          );
          assertCoherent(benchmark.capture, source, "benchmark-wal");
          NodeAssert.deepEqual(benchmark.capture.recipe, context.capture.recipe);
          NodeAssert.deepEqual(benchmark.capture.databaseSource, context.capture.databaseSource);
          NodeAssert.deepEqual(
            benchmark.capture.profile.productionObservations.map((entry) => entry.phase),
            profilePhases,
          );
          NodeAssert.ok(
            benchmark.capture.profile.productionObservations.every(
              (entry) =>
                JSON.stringify(entry.pragmas) ===
                JSON.stringify(benchmark.capture.profile.productionObservations[0].pragmas),
            ),
          );
          NodeAssert.equal(benchmark.capture.profile.productionClosed, true);
          NodeAssert.equal(benchmark.capture.profile.maintenance, undefined);
          NodeAssert.equal(benchmark.value, "bounded workload");
          NodeAssert.equal(benchmark.cleanup.absent, true);
          NodeAssert.equal(NodeFS.existsSync(healthPath), true);
          NodeAssert.ok(Buffer.byteLength(context.childReceipt.stdout) <= 49 * 1024);
          NodeAssert.ok(Buffer.byteLength(`${JSON.stringify(context.receipt)}\n`) <= 24 * 1024);
          NodeAssert.ok(context.receipt.manifest.length <= 32);
          return {
            schema: "jones-performance-fixture-consumer/v1",
            fixtureReceiptSha256: context.receiptSha256,
            disposition: "release",
            value: "readonly complete",
          };
        });
        NodeAssert.equal(health.value, "readonly complete");
        NodeAssert.equal(health.cleanup.absent, true);
        NodeAssert.equal(NodeFS.existsSync(healthPath), false);
        NodeAssert.throws(() => benchmarkContext.run(benchmarkContext.engine.latestSequence), {
          code: "closed_context",
        });
      });
    },
  );
}

historicalTest(
  "benchmark profile refuses an actual changed synchronous observation",
  async (test) => {
    const prepare = NodeSqlite.DatabaseSync.prototype.prepare;
    let database;
    test.mock.method(NodeSqlite.DatabaseSync.prototype, "prepare", function (sql) {
      database ??= this;
      return prepare.call(this, sql);
    });
    try {
      await withKnownClosedProfileFailure(
        "benchmark-wal",
        () => {
          database.exec("PRAGMA synchronous = OFF");
        },
        (produced) => {
          NodeAssert.equal(produced.error.code, "profile_mismatch");
          NodeAssert.equal(produced.retainReason, "profile_mismatch");
          NodeAssert.equal(produced.profile.failedStage, "after-callback");
          NodeAssert.equal(produced.profile.productionObservations.at(-1).pragmas.synchronous, 0);
          NodeAssert.notEqual(produced.profile.productionObservations[0].pragmas.synchronous, 0);
          NodeAssert.equal(produced.profile.productionClosed, true);
        },
      );
    } finally {
      test.mock.restoreAll();
    }
  },
);

historicalTest(
  "health profile refuses a real owned reader's busy checkpoint and closes the blocker",
  async () => {
    let blocker;
    await withKnownClosedProfileFailure(
      "health-offline-delete",
      async (context) => {
        blocker = new NodeSqlite.DatabaseSync(context.paths.dbPath, { readOnly: true });
        blocker.exec("BEGIN");
        NodeAssert.equal(
          blocker.prepare("SELECT count(*) AS n FROM checkpoint_diff_blobs").get().n,
          1,
        );
        await appendProfileActivity(context, "busy-reader");
      },
      (produced) => {
        NodeAssert.equal(produced.error.code, "profile_checkpoint_failed");
        NodeAssert.equal(produced.profile.maintenance.checkpoint.busy, 1);
        NodeAssert.equal(produced.profile.maintenance.returnedMode, undefined);
        NodeAssert.equal(produced.profile.maintenance.closed, true);
        NodeAssert.equal(produced.profile.failedStage, "checkpoint");
      },
      () => {
        if (blocker) {
          blocker.close();
          blocker = undefined;
        }
      },
    );
  },
);

historicalTest(
  "health profile closes production before maintenance and refuses a shutdown content change",
  async (test) => {
    const close = NodeSqlite.DatabaseSync.prototype.close;
    let closes = 0;
    test.mock.method(NodeSqlite.DatabaseSync.prototype, "close", function () {
      closes += 1;
      if (closes === 1)
        this.exec("UPDATE checkpoint_diff_blobs SET diff = diff || ' synthetic shutdown mutation'");
      return close.call(this);
    });
    try {
      await withKnownClosedProfileFailure(
        "health-offline-delete",
        () => undefined,
        (produced) => {
          NodeAssert.equal(produced.error.code, "profile_content_changed");
          NodeAssert.equal(produced.profile.productionClosed, true);
          NodeAssert.equal(produced.profile.maintenance.closed, true);
          NodeAssert.equal(produced.profile.maintenance.checkpoint, undefined);
          NodeAssert.notEqual(
            produced.profile.canonicalContent.originalSha256,
            produced.profile.canonicalContent.beforeSha256,
          );
          NodeAssert.equal(produced.profile.failedStage, "pre-transition-content");
          NodeAssert.equal(closes, 2);
        },
      );
    } finally {
      test.mock.restoreAll();
    }
  },
);

historicalTest(
  "health profile cancellation preserves the primary failure and its original owner",
  async () => {
    const cancellation = new AbortController();
    const primary = new Error("deliberate named-profile cancellation");
    await withInvocation(async (scope) => {
      const produced = await scope.produce(
        scope.options(oldSource, {
          profile: "health-offline-delete",
          signal: cancellation.signal,
        }),
        () => {
          cancellation.abort(primary);
          throw primary;
        },
      );
      let failure;
      try {
        NodeAssert.strictEqual(produced.error, primary);
        NodeAssert.equal(produced.retainReason, "profile_cancelled");
        NodeAssert.equal(produced.receipt, undefined);
        NodeAssert.equal(produced.profile.productionClosed, true);
        NodeAssert.equal(produced.profile.maintenance, undefined);
      } catch (error) {
        failure = error;
      }
      try {
        NodeAssert.equal(produced.closeKnown, true);
        produced.cleanup = disposeOwnedRoot(produced.owner);
        NodeAssert.equal(produced.cleanup.outcome, "complete");
      } catch (error) {
        failure = failure
          ? new AggregateError([failure, error], "Cancellation test retained scratch", {
              cause: failure,
            })
          : error;
      }
      if (failure) throw failure;
    });
  },
);

historicalTest(
  "health profile injected close rejection retains despite separately captured native close",
  async (test) => {
    await withInvocation(async (scope) => {
      const testOwner = createOwnedRoot({
        parentPath: scope.outer,
        childName: "unknown-close-enclosure",
        policy: scope.policy,
        binding: { ...producerBinding, runId: NodeCrypto.randomUUID() },
      });
      const prepare = NodeSqlite.DatabaseSync.prototype.prepare;
      const close = NodeSqlite.DatabaseSync.prototype.close;
      const resources = new Set();
      const closed = new Set();
      test.mock.method(NodeSqlite.DatabaseSync.prototype, "prepare", function (sql) {
        resources.add(this);
        return prepare.call(this, sql);
      });
      test.mock.method(NodeSqlite.DatabaseSync.prototype, "close", function () {
        close.call(this);
        closed.add(this);
        throw new Error("injected rejection after actual native close");
      });
      let failure;
      let produced;
      try {
        produced = await produceFixture(
          {
            parentPath: testOwner.creationReceipt.canonicalRootPath,
            childName: "producer",
            policy: scope.policy,
            binding: { ...producerBinding, runId: NodeCrypto.randomUUID() },
            producer: "historical-v1",
            databaseSource: oldSource,
            profile: "health-offline-delete",
          },
          () => undefined,
        );
        NodeAssert.equal(produced.closeKnown, false);
        NodeAssert.equal(produced.profile.productionClosed, false);
        NodeAssert.equal(produced.retainReason, "unknown_resource_close");
        NodeAssert.equal(produced.receipt, undefined);
        NodeAssert.equal(produced.profile.maintenance, undefined);
        NodeAssert.equal(disposeOwnedRoot(produced.owner).outcome, "retained");
        NodeAssert.equal(NodeFS.existsSync(produced.owner.creationReceipt.canonicalRootPath), true);
      } catch (error) {
        failure = error;
      } finally {
        test.mock.restoreAll();
        try {
          // Only this injected failure has independent actual native-close evidence. A genuine unknown
          // close cannot use the producer result to remove its retained tree or retry that resource.
          NodeAssert.equal(resources.size, 1);
          NodeAssert.equal(closed.size, resources.size);
          NodeAssert.ok([...resources].every((resource) => closed.has(resource)));
          NodeAssert.equal(disposeOwnedRoot(testOwner).outcome, "complete");
        } catch (error) {
          failure = failure
            ? new AggregateError([failure, error], "Injected close test retained scratch", {
                cause: failure,
              })
            : error;
        }
      }
      if (failure) throw failure;
    });
  },
);

for (const defect of ["header", "sidecar"]) {
  historicalTest(
    `health profile refuses an injected closed ${defect} defect before sealing`,
    async (test) => {
      const prepare = NodeSqlite.DatabaseSync.prototype.prepare;
      const close = NodeSqlite.DatabaseSync.prototype.close;
      let dbPath;
      let closes = 0;
      test.mock.method(NodeSqlite.DatabaseSync.prototype, "close", function () {
        close.call(this);
        closes += 1;
        if (closes === 2) {
          if (defect === "header") {
            const fd = NodeFS.openSync(
              dbPath,
              NodeFS.constants.O_WRONLY | NodeFS.constants.O_NOFOLLOW,
            );
            try {
              NodeFS.writeSync(fd, Buffer.from([2, 2]), 0, 2, 18);
            } finally {
              NodeFS.closeSync(fd);
            }
          } else
            NodeFS.writeFileSync(`${dbPath}-journal`, "synthetic sidecar", {
              flag: "wx",
              mode: 0o600,
            });
        }
      });
      const stages = [];
      test.mock.method(NodeSqlite.DatabaseSync.prototype, "prepare", function (sql) {
        if (/^PRAGMA journal_mode\s*=\s*WAL;?$/i.test(sql.trim())) stages.push("production-wal");
        if (/^PRAGMA wal_checkpoint\(TRUNCATE\)$/i.test(sql.trim()))
          stages.push(`checkpoint-after-${closes}-closes`);
        if (/^PRAGMA journal_mode\s*=\s*DELETE$/i.test(sql.trim()))
          stages.push(`delete-after-${closes}-closes`);
        return prepare.call(this, sql);
      });
      try {
        await withKnownClosedProfileFailure(
          "health-offline-delete",
          (context) => {
            dbPath = context.paths.dbPath;
          },
          (produced) => {
            NodeAssert.equal(
              produced.error.code,
              defect === "header" ? "profile_header_failed" : "profile_sidecars_present",
            );
            NodeAssert.equal(produced.profile.maintenance.closed, true);
            NodeAssert.equal(closes, 2);
            NodeAssert.deepEqual(stages, [
              "production-wal",
              "checkpoint-after-1-closes",
              "delete-after-1-closes",
            ]);
            NodeAssert.equal(
              produced.profile.canonicalContent.originalSha256,
              produced.profile.canonicalContent.afterSha256,
            );
          },
        );
      } finally {
        test.mock.restoreAll();
      }
    },
  );
}

historicalTest(
  "closed named profile pre-cancellation retains bounded evidence with no child or database opened",
  async () => {
    await withInvocation(async (scope) => {
      const testOwner = createOwnedRoot({
        parentPath: scope.outer,
        childName: "cancelled-profile-enclosure",
        policy: scope.policy,
        binding: { ...producerBinding, runId: NodeCrypto.randomUUID() },
      });
      const cancellation = new AbortController();
      cancellation.abort();
      let rejected;
      let failure;
      try {
        rejected = await withClosedSyntheticFixture(
          {
            parentPath: testOwner.creationReceipt.canonicalRootPath,
            childName: "consumer",
            policy: scope.policy,
            binding: { ...producerBinding, runId: NodeCrypto.randomUUID() },
            producer: "historical-v1",
            databaseSource: oldSource,
            profile: "health-offline-delete",
            signal: cancellation.signal,
          },
          () => undefined,
        ).catch((error) => error);
        NodeAssert.equal(rejected.evidence.cleanup.outcome, "retained");
        NodeAssert.equal(rejected.evidence.cleanup.reason, "profile_cancelled");
        NodeAssert.equal(rejected.evidence.profile.kind, "health-offline-delete");
        NodeAssert.equal(rejected.evidence.profile.stage, "producer-transport");
        NodeAssert.equal(rejected.evidence.receipt, undefined);
        NodeAssert.equal(rejected.evidence.childReceipt.pid, null);
      } catch (error) {
        failure = error;
      } finally {
        try {
          NodeAssert.equal(rejected.evidence.childReceipt.pid, null);
          NodeAssert.equal(rejected.evidence.childReceipt.closed, true);
          NodeAssert.equal(rejected.evidence.childReceipt.reaped, true);
          NodeAssert.equal(rejected.evidence.childReceipt.truncated, false);
          NodeAssert.deepEqual(
            await NodeFSP.readdir(
              NodePath.join(testOwner.creationReceipt.canonicalRootPath, "consumer"),
            ),
            [".jones-performance-root.json"],
          );
          NodeAssert.equal(disposeOwnedRoot(testOwner).outcome, "complete");
        } catch (error) {
          failure = failure
            ? new AggregateError([failure, error], "Pre-cancelled profile test retained scratch", {
                cause: failure,
              })
            : error;
        }
      }
      if (failure) throw failure;
    });
  },
);

historicalTest(
  "source binding keeps pinned names and rejects substituted descriptors",
  async () => {
    await withInvocation(async ({ outer }) => {
      const environment = { [sourceParentEnvironment]: outer };
      const source = syntheticDatabaseSource(oldSource.sourceRevision, environment);
      NodeAssert.equal(source.worktreePath, NodePath.join(outer, "baseline"));
      NodeAssert.equal(source.repository, oldSource.repository);
      NodeAssert.equal(source.sourceRevision, oldSource.sourceRevision);
      for (const changed of [
        { ...source, worktreePath: oldSource.worktreePath },
        { ...source, sourceRevision: "0000000000000000000000000000000000000000" },
        { ...source, repository: "other/repository" },
        { ...source, extra: true },
      ])
        NodeAssert.throws(() => assertSyntheticDatabaseSource(changed, environment), {
          code: "invalid_source",
        });
    });
  },
);

for (const target of ["fixture", "migration"]) {
  NodeTest.test(
    `actual ${target} import loader drains rejected siblings and retains unfinished preflight custody`,
    (t) => {
      const base = NodeFS.mkdtempSync(
        NodePath.join(process.env.TMPDIR ?? "/var/tmp", "import-barrier-"),
      );
      const identity = NodeFS.lstatSync(base);
      let childClosed = false;
      t.after(() => {
        if (childClosed && !NodeFS.existsSync(NodePath.join(base, "retained.json"))) {
          const current = NodeFS.lstatSync(base);
          NodeAssert.equal(current.dev, identity.dev);
          NodeAssert.equal(current.ino, identity.ino);
          NodeAssert.equal(current.uid, identity.uid);
          NodeFS.rmSync(base, { recursive: true });
        } else t.diagnostic(`retained synthetic import evidence/root: ${base}`);
      });
      const driver = NodePath.join(base, "driver.mjs");
      NodeFS.writeFileSync(
        driver,
        `
import * as A from "node:assert/strict";
import * as FS from "node:fs";
import * as Path from "node:path";
import * as URL from "node:url";
import * as Crypto from "node:crypto";
import { mock } from "node:test";
const directory = ${JSON.stringify(directory)};
const base = ${JSON.stringify(base)};
const sourcesURL = URL.pathToFileURL(Path.join(directory, "sources.mjs")).href;
const originals = await import(sourcesURL);
mock.module(sourcesURL, { namedExports: { ...originals,
  assertQualificationDatabaseSource: (source) => source,
  assertCurrentDatabaseSource: (source) => source,
} });
const { probeHistoricalFixtureImports } = await import(URL.pathToFileURL(Path.join(directory, "fixtures-historical-worker.mjs")));
const { probeMigrationRestoreImports } = await import(URL.pathToFileURL(Path.join(directory, "migration-restore-worker.mjs")));
const { installHistoricalResolutionGuard } = await import(URL.pathToFileURL(Path.join(directory, "historical-resolution.mjs")));
const { consumeHistoricalImportPreflight, finalizeHistoricalImportPreflight, historicalQualificationClosureKnown } = await import(URL.pathToFileURL(Path.join(directory, "qualify-historical.mjs")));
const { createHistoricalPreparation, finishHistoricalPreparation, cleanupHistoricalPreparation } = await import(URL.pathToFileURL(Path.join(directory, "historical-preparation.mjs")));
const modes = ["drain", "deadline"];
const retained = [];
for (const [worker, probe] of [["fixture", (source, options) => probeHistoricalFixtureImports(source, options)], ["migration", (source, options) => probeMigrationRestoreImports(source, false, options)]].filter(([worker]) => worker === ${JSON.stringify(target)})) {
 for (const mode of modes) {
  const home = Path.join(base, worker + "-" + mode);
  FS.mkdirSync(home); FS.mkdirSync(Path.join(home, "runs")); FS.mkdirSync(Path.join(home, "evidence"));
  const owner = createHistoricalPreparation({ parentPath: Path.join(home, "runs"), evidenceParent: Path.join(home, "evidence"), taskId: "synthetic-import-barrier", runId: "one", retention: "release-after-closed-evidence", bounds: { maxBytes: 1048576, maxEntries: 1000, maxFileBytes: 65536, maxCacheBytes: 65536, maxOutputBytes: 131072, deadlineMs: 4000, terminateGraceMs: 100, reapTimeoutMs: 1000, maxProcesses: 4, estimatedPeakBytes: 10 * 1024 ** 3 } });
  const root = Path.join(owner.creation.root, "sources/history");
  const write = (relative, text) => { const path = Path.join(root, relative); FS.mkdirSync(Path.dirname(path), { recursive: true }); FS.writeFileSync(path, text); };
  write("package.json", '{"name":"synthetic-source","type":"module"}');
  write("apps/server/package.json", '{"name":"synthetic-server","type":"module"}');
  write("pnpm-lock.yaml", "packages:\\n  effect@1.0.0:\\n    resolution: {}\\n  '@effect/platform-node@1.0.0':\\n    resolution: {}\\nsnapshots: {}\\n");
  write("node_modules/effect/package.json", '{"name":"effect","version":"1.0.0","type":"module","exports":{"./*":"./*.mjs"}}');
  write("node_modules/@effect/platform-node/package.json", '{"name":"@effect/platform-node","version":"1.0.0","type":"module","exports":{"./NodeServices":"./NodeServices.mjs"}}');
  write("node_modules/@effect/platform-node/NodeServices.mjs", 'export const fake = true;');
  const primary = new Error("original synthetic import rejection");
  let release, rejected, entered;
  const rejectedPromise = new Promise((resolve) => { rejected = resolve; });
  const enteredPromise = new Promise((resolve) => { entered = resolve; });
  globalThis.a12Barrier = { primary, rejected, entered, promise: new Promise((resolve) => { release = resolve; }), finished: false };
  for (const name of ["Effect", "Layer", "ManagedRuntime", "Schema", "Option", "Stream", "Logger", "unstable/sql/SqlClient"])
    write("node_modules/effect/" + name + ".mjs", name === "Effect" ? 'globalThis.a12Barrier.rejected(); throw globalThis.a12Barrier.primary;' : name === "Layer" ? 'globalThis.a12Barrier.entered(); await globalThis.a12Barrier.promise; globalThis.a12Barrier.finished = true; export const fake = true;' : 'export const fake = true;');
  for (const name of ["packages/contracts/src/index.ts", "apps/server/src/config.ts", "apps/server/src/persistence/Layers/Sqlite.ts", "apps/server/src/persistence/Migrations.ts", "packages/shared/src/nodeSqliteClient.ts", "apps/server/src/orchestration/Layers/OrchestrationEngine.ts", "apps/server/src/orchestration/Services/OrchestrationEngine.ts", "apps/server/src/orchestration/Layers/ProjectionSnapshotQuery.ts", "apps/server/src/orchestration/Services/ProjectionSnapshotQuery.ts", "apps/server/src/orchestration/Layers/ProjectionPipeline.ts", "apps/server/src/persistence/Layers/OrchestrationEventStore.ts", "apps/server/src/persistence/Layers/OrchestrationCommandReceipts.ts", "apps/server/src/orchestration/ThreadBackgroundLiveness.ts", "apps/server/src/orchestration/ThreadPlanProgress.ts", "apps/server/src/project/RepositoryIdentityResolver.ts", "apps/server/src/orchestration/WorktreeOwnershipLease.ts", "apps/server/src/persistence/ProviderSessionRuntime.ts", "apps/server/src/orchestration/projector.ts", "packages/contracts/src/settings.ts", "packages/contracts/src/keybindings.ts", "apps/server/src/attachmentStore.ts"])
    write(name, 'export const fake = true;');
  const source = { repository: "synthetic", sourceRevision: "414bb8da204c3275cd0b76b2ec4d74dfb09a97e4", worktreePath: root };
  const receivingRoot = Path.resolve(directory, "../../..");
  const hash = (path) => Crypto.createHash("sha256").update(FS.readFileSync(path)).digest("hex");
  const guard = installHistoricalResolutionGuard({ receivingRoot, historicalRoots: [root], sourceInputs: [receivingRoot, root].map((root) => ({ root, lockSha256: hash(Path.join(root, "pnpm-lock.yaml")) })), maxRecords: 128, maxFileBytes: 1048576 });
  // Bind ahead of launch to isolate draining from the separate bridge regression.
  const importerURL = URL.pathToFileURL(Path.join(directory, worker === "fixture" ? "fixtures-historical-worker.mjs" : "migration-restore-worker.mjs")).href;
  for (const specifier of ["effect/Effect", "effect/Layer", "effect/ManagedRuntime", "effect/Schema", "effect/Option", "effect/Stream", "effect/Logger", "effect/unstable/sql/SqlClient", "@effect/platform-node/NodeServices"])
    guard.dependencyURL(root, specifier, importerURL);
  const preflight = {};
  const abort = new AbortController();
  let returned = false, loaderReturned = false;
  let loader;
  const pending = consumeHistoricalImportPreflight([async (importState) => {
    loader = probe(source, { resolutionGuard: guard, importState });
    try { return await loader; } finally { loaderReturned = true; }
  }], preflight, abort.signal).then(() => { returned = true; }, (error) => { returned = true; return error; });
  await Promise.all([enteredPromise, rejectedPromise]);
  // Explicit milestones, followed by an event-loop barrier, expose fail-fast Promise.all.
  await new Promise(setImmediate);
  A.equal(loaderReturned, false, "loader returned while its sibling was still evaluating");
  A.equal(returned, false);
  await A.rejects(import(URL.pathToFileURL(Path.join(base, "outside.mjs")).href), /outside all bound checkouts/);
  if (mode === "deadline") {
    abort.abort(new Error("synthetic bounded deadline"));
    A.strictEqual(await pending, primary, "deadline replaced the already observed original failure");
    finalizeHistoricalImportPreflight(preflight, guard);
    A.equal(preflight.importsSettled, false);
    A.equal(preflight.guardClosed, false);
    // The real guard is still active: an unbound dependency URL remains refused.
    await A.rejects(import(URL.pathToFileURL(Path.join(base, "outside.mjs")).href), /outside all bound checkouts/);
    const closed = historicalQualificationClosureKnown([], undefined, undefined, preflight);
    A.equal(closed, false);
    const final = finishHistoricalPreparation(owner, { originalError: { message: primary.message }, consumerClosed: closed });
    A.equal(final.cleanup.outcome, "retained"); A.equal(FS.existsSync(owner.creation.root), true);
    A.equal(JSON.parse(FS.readFileSync(final.evidence.path)).originalError.message, primary.message);
    retained.push({ root: owner.creation.root, identity: owner.creation.rootIdentity, reason: "synthetic unknown import closure retained", evidence: final.finalEvidence.path });
  }
  release();
  await A.rejects(loader, (error) => error === primary);
  A.strictEqual(await pending, primary);
  A.equal(globalThis.a12Barrier.finished, true);
  finalizeHistoricalImportPreflight(preflight, guard);
  A.equal(preflight.importsSettled, true); A.equal(preflight.guardClosed, true);
  A.equal((await import(URL.pathToFileURL(Path.join(base, "outside.mjs")).href)).outside, true);
  A.equal(historicalQualificationClosureKnown([], undefined, undefined, preflight), false);
  if (mode === "drain") {
    const final = finishHistoricalPreparation(owner, { originalError: { message: primary.message }, consumerClosed: false });
    A.equal(final.cleanup.outcome, "retained");
    // This fake evaluation opened no resources; this enclosing original test owner
    // has independent barrier/drain proof. Production import failure never supplies it.
    A.equal(cleanupHistoricalPreparation(owner, { evidencePreserved: true, consumerClosed: true }).absent, true);
  }
 }
}
FS.writeFileSync(Path.join(base, "retained.json"), JSON.stringify(retained));
console.log(JSON.stringify({ loaders: 1, barriers: 2, retained }));
`,
      );
      NodeFS.writeFileSync(NodePath.join(base, "outside.mjs"), "export const outside = true;");
      const child = NodeChildProcess.spawnSync(
        process.execPath,
        ["--experimental-test-module-mocks", driver],
        {
          encoding: "utf8",
          timeout: 10000,
          maxBuffer: 65536,
          env: { PATH: process.env.PATH, TMPDIR: process.env.TMPDIR },
        },
      );
      childClosed = !child.error && child.status !== null;
      NodeAssert.equal(child.status, 0, child.stderr);
      const report = JSON.parse(child.stdout);
      NodeAssert.equal(report.barriers, 2);
      NodeAssert.equal(report.retained.length, 1);
      t.diagnostic(JSON.stringify(report));
    },
  );
}
