import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeURL from "node:url";

import {
  captureSyntheticFixture,
  withClosedSyntheticFixture,
  withOpenSyntheticFixture,
} from "./fixtures.mjs";
import {
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

const directory = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const worktree = NodePath.resolve(directory, "../..");
const oldSource = syntheticDatabaseSource("e5a31aceec91484b64315c63dcce80f6e7581604");
const liveSource = syntheticDatabaseSource("414bb8da204c3275cd0b76b2ec4d74dfb09a97e4");
const producerBinding = {
  repository: "Jones-Systems/Jones-Code",
  sourceRevision: "da83ffbbfa2dd1bd3c67b7d9185b3c8bbb180d7f",
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
  const outer = await NodeFSP.mkdtemp(NodePath.join(directory, ".fixture-test-"));
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

function assertCoherent(capture, source) {
  NodeAssert.deepEqual(capture.databaseSource, source);
  NodeAssert.equal(capture.runtime.profile, "observed-production-defaults");
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

NodeTest.test(
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

NodeTest.test(
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

NodeTest.test(
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

NodeTest.test(
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

NodeTest.test(
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

NodeTest.test(
  "source aliases, profile selectors, recipe overflow and cloned contexts are refused",
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
        scope.open(scope.options(oldSource, { profile: "health-offline-delete" }), () => undefined),
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

NodeTest.test(
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

NodeTest.test("closed consumer missing outcome retains its known closed fixture", async () => {
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

NodeTest.test(
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

NodeTest.test("closed consumer raw diagnostic envelope does not acknowledge release", async () => {
  await withInjectedConsumerOutcome(
    () => ({ status: "refused", classification: "unknown", findings: [] }),
    (error) => {
      NodeAssert.equal(error.code, "invalid_consumer_outcome");
      NodeAssert.equal(error.evidence.cleanup.reason, "invalid_consumer_outcome");
      NodeAssert.equal(Object.hasOwn(error.evidence, "value"), false);
    },
  );
});

NodeTest.test("closed consumer release for a different receipt retains the fixture", async () => {
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

NodeTest.test(
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

NodeTest.test(
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

NodeTest.test(
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

NodeTest.test("source binding keeps pinned names and rejects substituted descriptors", async () => {
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
});
