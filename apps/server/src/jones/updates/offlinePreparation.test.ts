import { assert, it } from "@effect/vitest";
import * as NodeAssert from "node:assert/strict";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Tests inspect only their internally owned synthetic roots and consume retained evidence before cleanup.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- These paths identify captured fixture evidence, never installed state.
import * as NodePath from "node:path";
import type { OrchestrationV2ProviderCapabilities } from "@t3tools/contracts";
import {
  OfflinePreparationError,
  offlinePreparationStatus,
  withSyntheticPreparationFixture,
} from "./offlinePreparation.ts";
import {
  decodeOfflinePreparationPlan,
  encodeOfflinePreparationPlan,
} from "./offlinePreparationPlan.ts";

const capabilities = {
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: false,
    supportsProviderSwitchingViaHandoff: false,
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: false,
    canReadThreadSnapshot: false,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: false,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: false,
    emitsTurnCompleted: false,
    supportsInterrupt: false,
    supportsActiveSteering: false,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: false,
    terminalStatusQuality: "none",
  },
  streaming: {
    streamsAssistantText: false,
    streamsReasoning: false,
    streamsToolOutput: false,
    streamsPlanText: false,
    emitsMessageCompleted: false,
  },
  tools: {
    exposesToolItemIds: false,
    emitsToolStarted: false,
    emitsToolCompleted: false,
    emitsToolOutput: false,
    supportsMcpTools: false,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: false,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: false,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: false,
    approvalCallbacksAreLiveOnly: false,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: false,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: false,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: false,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: false,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: false,
    supportsDeltaHandoff: false,
    supportsFullThreadHandoff: false,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: false,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: false,
  },
  identity: {
    nativeThreadIds: "none",
    nativeTurnIds: "none",
    nativeItemIds: "none",
    nativeRequestIds: "none",
  },
  runtimePolicy: {
    enforcement: "client-boundary",
  },
} as const satisfies OrchestrationV2ProviderCapabilities;
const seed = {
  capabilities,
  settings: JSON.stringify({ continueThreadsAfterServerUpdate: true }),
  threads: [{ id: "thread:one" }, { id: "thread:two" }],
};
type Fixture = Parameters<Parameters<typeof withSyntheticPreparationFixture>[1]>[0];
const absent = async (path: string) => {
  await NodeAssert.rejects(NodeFSP.lstat(path), /ENOENT/);
};

it("holds the standalone production contract without accepting authority input", () => {
  assert.deepEqual(offlinePreparationStatus(), {
    status: "held",
    reason: "maintenance-owner-binding-unqualified",
    databaseOpened: false,
    productionInvocation: false,
    activation: false,
  });
});

it("uses the full historical recovery reader and canonical enqueue after durable planning", async () => {
  let root = "";
  await withSyntheticPreparationFixture(
    {
      ...seed,
      threads: [
        ...seed.threads,
        { id: "thread:existing", existingMarker: true },
        { id: "thread:completed", status: "completed" },
      ],
    },
    async (fixture) => {
      const before = await fixture.inspect();
      root = before.scratchRoot;
      const plan = await fixture.plan();
      assert.lengthOf(plan.expectedRows, 2);
      assert.lengthOf(plan.preservedRows, 1);
      assert.lengthOf(plan.selections, 3);
      assert.equal(plan.source, "bd20d2d2fc0ba89797698192b877a7f1312f8fe3");
      assert.deepEqual(JSON.parse((await fixture.inspect()).planOnDisk!), plan);
      assert.equal((await fixture.apply(plan)).status, "applied");
      const after = await fixture.inspect();
      assert.lengthOf(after.rows, 3);
      for (const row of plan.expectedRows) {
        assert.deepEqual(
          after.rows.find((candidate) => candidate.effect_id === row.effect_id),
          row,
        );
        assert.equal(row.available_at, plan.timestamp);
        assert.equal(row.created_at, plan.timestamp);
        assert.equal(row.updated_at, plan.timestamp);
        assert.deepEqual(JSON.parse(row.payload_json), {
          type: "provider-runtime.continue",
          sourceRunId: "run:" + row.thread_id,
          preparedForRestart: true,
          continueWithoutPreference: false,
        });
      }
      assert.deepEqual(
        after.rows.find((row) => row.effect_id === before.rows[0]!.effect_id),
        before.rows[0],
      );
      const index = (event: string) => after.audit.indexOf(event);
      assert.isBelow(index("snapshot-pair:synced"), index("plan:file-synced"));
      assert.isBelow(index("plan:directory-synced"), index("database:write-open"));
      assert.isBelow(index("transaction:committed"), index("evidence:file-synced"));
      assert.isAtLeast(index("evidence:directory-synced"), 0);
      assert.equal((await fixture.reconcile(plan)).status, "applied");
    },
  );
  await absent(root);
});

it("preserves missing/default/invalid preferences and project overrides", async () => {
  for (const [settings, count] of [
    [undefined, 0],
    ["{}", 0],
    [
      JSON.stringify({ continueThreadsAfterServerUpdate: true, responseStreamingMode: "invalid" }),
      0,
    ],
    [
      JSON.stringify({
        continueThreadsAfterServerUpdate: true,
        projectSettingsOverrides: {
          "project:synthetic": { continueThreadsAfterServerUpdate: false },
        },
      }),
      0,
    ],
    [
      JSON.stringify({
        projectSettingsOverrides: {
          "project:synthetic": { continueThreadsAfterServerUpdate: true },
        },
      }),
      2,
    ],
  ] as const) {
    await withSyntheticPreparationFixture(
      {
        capabilities,
        threads: seed.threads,
        ...(settings === undefined ? {} : { settings }),
      },
      async (fixture) => assert.lengthOf((await fixture.plan()).expectedRows, count),
    );
  }
});

it("fails on unreadable recovery messages even though the predicate does not consume messages", async () => {
  let root = "";
  await withSyntheticPreparationFixture(
    {
      ...seed,
      threads: [{ id: "thread:bad-message", malformedMessage: true }],
    },
    async (fixture) => {
      root = (await fixture.inspect()).scratchRoot;
      await NodeAssert.rejects(fixture.plan());
      const state = await fixture.inspect();
      assert.notInclude(state.audit, "database:write-open");
      assert.lengthOf(state.rows, 0);
    },
  );
  await absent(root);
});

it("rejects serialized, fabricated, cross-scope, and expired plan authority", async () => {
  let expired: Fixture | undefined;
  await withSyntheticPreparationFixture(seed, async (first) => {
    expired = first;
    const plan = await first.plan();
    await NodeAssert.rejects(first.apply(JSON.parse(JSON.stringify(plan))), /unowned-plan/);
    const decoded = decodeOfflinePreparationPlan(encodeOfflinePreparationPlan(plan));
    assert.deepEqual(decoded.plan, plan);
    assert.notStrictEqual(decoded.plan, plan);
    await NodeAssert.rejects(first.apply(decoded.plan), /unowned-plan/);
    await withSyntheticPreparationFixture(seed, async (second) => {
      await NodeAssert.rejects(second.apply(plan), /unowned-plan/);
      assert.notInclude((await second.inspect()).audit, "database:write-open");
    });
    assert.notInclude((await first.inspect()).audit, "database:write-open");
  });
  await NodeAssert.rejects(expired!.plan(), /expired-fixture/);
});

it("never opens writable after either plan durability step fails", async () => {
  for (const point of ["plan-file-sync", "plan-directory-sync"] as const) {
    let root = "";
    await withSyntheticPreparationFixture(seed, async (fixture) => {
      root = (await fixture.inspect()).scratchRoot;
      fixture.injectFailure(point);
      await NodeAssert.rejects(fixture.plan(), new RegExp(point));
      const state = await fixture.inspect();
      assert.lengthOf(state.rows, 0);
      assert.notInclude(state.audit, "database:write-open");
    });
    await absent(root);
  }
});

it("refuses missing files and incomplete WAL without creating a database", async () => {
  for (const point of ["missing-database", "incomplete-wal"] as const) {
    let root = "";
    await withSyntheticPreparationFixture(seed, async (fixture) => {
      root = (await fixture.inspect()).scratchRoot;
      const plan = await fixture.plan();
      await fixture.mutate(point);
      await NodeAssert.rejects(fixture.apply(plan));
      if (point === "missing-database") await absent(NodePath.join(root, "state.sqlite"));
      const persisted = JSON.parse(
        await NodeFSP.readFile(NodePath.join(root, "continuation-plan.json"), "utf8"),
      );
      assert.deepEqual(persisted, plan);
    });
    await absent(root);
  }
});

it("rejects source-incompatible historical ledgers and table definitions during read-only planning", async () => {
  for (const schema of ["unsupported-ledger", "unsupported-schema"] as const) {
    let root = "";
    await withSyntheticPreparationFixture({ ...seed, schema }, async (fixture) => {
      root = (await fixture.inspect()).scratchRoot;
      await NodeAssert.rejects(fixture.plan(), new RegExp(schema));
      const state = await fixture.inspect();
      assert.notInclude(state.audit, "database:write-open");
      assert.isNull(state.planOnDisk);
    });
    await absent(root);
  }
});

it("rolls back every new marker on transaction failure and retains preexisting preparation", async () => {
  await withSyntheticPreparationFixture(
    {
      ...seed,
      threads: [...seed.threads, { id: "thread:previous", existingMarker: true }],
    },
    async (fixture) => {
      const before = await fixture.inspect();
      const plan = await fixture.plan();
      fixture.injectFailure("before-commit");
      await NodeAssert.rejects(fixture.apply(plan), /before-commit/);
      assert.deepEqual((await fixture.inspect()).rows, before.rows);
      assert.equal((await fixture.reconcile(plan)).status, "not-applied");
    },
  );
});

it("reconciles exact rows after commit or either evidence durability step is interrupted", async () => {
  for (const point of ["after-commit", "evidence-file-sync", "evidence-directory-sync"] as const) {
    await withSyntheticPreparationFixture(seed, async (fixture) => {
      const plan = await fixture.plan();
      fixture.injectFailure(point);
      await NodeAssert.rejects(fixture.apply(plan), new RegExp(point));
      await NodeAssert.rejects(fixture.apply(plan), /reconciliation-required/);
      assert.equal((await fixture.reconcile(plan)).status, "applied");
      assert.deepEqual(
        (await fixture.inspect()).rows,
        [...plan.expectedRows].sort((a, b) => a.effect_id.localeCompare(b.effect_id)),
      );
    });
  }
});

it("holds collisions, partial rows, advanced effects, and projection drift until the synthetic pair is restored", async () => {
  for (const mutation of [
    "marker-collision",
    "partial-rows",
    "advanced-row",
    "projection-drift",
  ] as const) {
    await withSyntheticPreparationFixture(
      {
        ...seed,
        threads: [...seed.threads, { id: "thread:previous", existingMarker: true }],
      },
      async (fixture) => {
        const before = await fixture.inspect();
        const plan = await fixture.plan();
        if (mutation !== "marker-collision") {
          fixture.injectFailure("after-commit");
          await NodeAssert.rejects(fixture.apply(plan), /after-commit/);
        }
        await fixture.mutate(mutation, plan);
        if (mutation === "marker-collision")
          await NodeAssert.rejects(fixture.apply(plan), /baseline-changed/);
        assert.equal((await fixture.reconcile(plan)).status, "held");
        await fixture.restoreSnapshot();
        assert.deepEqual((await fixture.inspect()).rows, before.rows);
        assert.equal((await fixture.reconcile(plan)).status, "not-applied");
      },
    );
  }
});

it("cancels before write and requires reconciliation after possible commit", async () => {
  await withSyntheticPreparationFixture(seed, async (fixture) => {
    const plan = await fixture.plan();
    assert.equal(await fixture.cancel(), "not-applied");
    await NodeAssert.rejects(fixture.apply(plan), /preparation-cancelled/);
    assert.notInclude((await fixture.inspect()).audit, "database:write-open");
  });
  for (const point of ["cancel-before-write", "cancel-after-commit"] as const) {
    await withSyntheticPreparationFixture(seed, async (fixture) => {
      const plan = await fixture.plan();
      fixture.injectFailure(point);
      await NodeAssert.rejects(fixture.apply(plan), new RegExp(point));
      assert.equal(
        await fixture.cancel(),
        point === "cancel-before-write" ? "not-applied" : "reconciliation-required",
      );
      if (point === "cancel-after-commit") {
        assert.equal((await fixture.reconcile(plan)).status, "applied");
        await fixture.restoreSnapshot();
        assert.lengthOf((await fixture.inspect()).rows, 0);
      } else assert.notInclude((await fixture.inspect()).audit, "database:write-open");
    });
  }
});

it("drains a started operation before scope cleanup and preserves ordinary callback failure", async () => {
  let root = "";
  let planFinished = false;
  await withSyntheticPreparationFixture(seed, async (fixture) => {
    root = (await fixture.inspect()).scratchRoot;
    void fixture.plan().then(() => {
      planFinished = true;
    });
  });
  assert.isTrue(planFinished);
  await absent(root);
  const original = new Error("synthetic callback failure");
  try {
    await withSyntheticPreparationFixture(seed, async (fixture) => {
      root = (await fixture.inspect()).scratchRoot;
      throw original;
    });
    assert.fail("Expected the callback failure.");
  } catch (cause) {
    assert.strictEqual(cause, original);
  }
  await absent(root);
});

it("drains fire-and-forget apply through internal reconciliation after closing external admission", async () => {
  let root = "";
  let pending: ReturnType<Fixture["apply"]> | undefined;
  let observed: Promise<void> | undefined;
  let escaped: Fixture | undefined;
  let admissionClosed = false;
  await withSyntheticPreparationFixture(seed, async (fixture) => {
    escaped = fixture;
    root = (await fixture.inspect()).scratchRoot;
    const plan = await fixture.plan();
    pending = fixture.apply(plan);
    // This reaction cannot run until apply's admitted transaction/reconciliation
    // completes; the callback has already returned and closed external admission.
    observed = pending.then(async () => {
      await NodeAssert.rejects(fixture.cancel(), /expired-fixture/);
      admissionClosed = true;
    });
    void observed.catch(() => {});
  });
  assert.isDefined(pending);
  assert.equal((await pending!).status, "applied");
  await NodeAssert.rejects(escaped!.inspect(), /expired-fixture/);
  await observed;
  assert.isTrue(admissionClosed);
  await absent(root);
});

it("retains SQLite custody uncertainty after actual read, write, and mutation handles close", async () => {
  for (const source of ["read-scope", "write-scope", "mutation"] as const) {
    const callbackFailure = new Error("callback failed after custody loss");
    let root: string | undefined;
    let closeFailure: unknown;
    let cleanupProved = false;
    try {
      await withSyntheticPreparationFixture(seed, async (fixture) => {
        root = (await fixture.inspect()).scratchRoot;
        const plan = await fixture.plan();
        if (source === "read-scope") await fixture.apply(plan);
        fixture.injectFailure(
          source === "mutation"
            ? "mutation-close-after-close"
            : source === "read-scope"
              ? "read-scope-after-close"
              : "write-scope-after-close",
        );
        await NodeAssert.rejects(
          source === "mutation"
            ? fixture.mutate("projection-drift", plan)
            : source === "read-scope"
              ? fixture.reconcile(plan)
              : fixture.apply(plan),
          (cause: unknown) => {
            assert.instanceOf(cause, OfflinePreparationError);
            closeFailure = cause;
            assert.equal((cause as OfflinePreparationError).reason, "sqlite-scope-unresolved");
            return true;
          },
        );
        const before = await NodeFSP.readFile(NodePath.join(root, "state.sqlite"));
        await NodeAssert.rejects(fixture.reconcile(plan), /sqlite-custody-unresolved/);
        await NodeAssert.rejects(fixture.restoreSnapshot(), /sqlite-custody-unresolved/);
        await NodeAssert.rejects(fixture.apply(plan), /sqlite-custody-unresolved/);
        await NodeAssert.rejects(fixture.mutate("missing-database"), /sqlite-custody-unresolved/);
        await NodeAssert.rejects(fixture.inspect(), /sqlite-custody-unresolved/);
        assert.deepEqual(await NodeFSP.readFile(NodePath.join(root, "state.sqlite")), before);
        // Swallowing the operation error cannot restore cleanup authority.
        if (source === "mutation") throw callbackFailure;
      });
      assert.fail("Expected sticky custody retention.");
    } catch (cause) {
      assert.instanceOf(cause, OfflinePreparationError);
      const error = cause as OfflinePreparationError;
      assert.equal(error.retainedRoot, root);
      const evidence = JSON.parse(
        await NodeFSP.readFile(NodePath.join(root!, "retained-evidence.json"), "utf8"),
      );
      const closed = evidence.audit.lastIndexOf(source + ":native-close-completed");
      cleanupProved = closed >= 0 && evidence.audit[closed + 1] === source + ":close-unresolved";
      assert.isTrue(cleanupProved);
      assert.equal(error.reason, "unresolved-sqlite-custody");
      if (source === "mutation") {
        assert.instanceOf(error.cause, AggregateError);
        const errors = (error.cause as AggregateError).errors as unknown[];
        assert.strictEqual(errors[0], callbackFailure);
        assert.strictEqual(errors[1], closeFailure);
      } else assert.strictEqual(error.cause, closeFailure);
      assert.deepEqual(evidence.custody, [{ source, status: "unresolved" }]);
      assert.notInclude(evidence.audit, "snapshot-pair:restored");
      await NodeFSP.stat(NodePath.join(root!, "previous.sqlite"));
    } finally {
      // These named faults occur after native close, proved in retained evidence.
      // An actual close failure lacks that proof and must retain its exact root.
      if (cleanupProved && root !== undefined)
        await NodeFSP.rm(root, { recursive: true, force: true });
    }
    await absent(root!);
  }
});

it("retains initializer finalization uncertainty before exposing a fixture", async () => {
  let root: string | undefined;
  let cleanupProved = false;
  let exposed = false;
  try {
    await withSyntheticPreparationFixture(
      { ...seed, failure: "initializer-close-after-close" },
      async () => {
        exposed = true;
      },
    );
    assert.fail("Expected initializer custody retention.");
  } catch (cause) {
    assert.instanceOf(cause, OfflinePreparationError);
    const error = cause as OfflinePreparationError;
    root = error.retainedRoot;
    const evidence = JSON.parse(
      await NodeFSP.readFile(NodePath.join(root!, "retained-evidence.json"), "utf8"),
    );
    cleanupProved =
      evidence.audit[0] === "initializer:native-close-completed" &&
      evidence.audit[1] === "initializer:close-unresolved";
    assert.isTrue(cleanupProved);
    assert.isFalse(exposed);
    assert.equal(error.reason, "unresolved-sqlite-custody");
    assert.deepEqual(evidence.custody, [{ source: "initializer", status: "unresolved" }]);
    assert.instanceOf(error.cause, OfflinePreparationError);
    assert.equal(
      ((error.cause as OfflinePreparationError).cause as OfflinePreparationError).reason,
      "initializer-close-after-close",
    );
  } finally {
    if (cleanupProved && root !== undefined)
      await NodeFSP.rm(root, { recursive: true, force: true });
  }
  await absent(root!);
});

it("preserves both an operation rejection and a lost SQLite close confirmation", async () => {
  for (const source of ["read-scope", "mutation"] as const) {
    let root: string | undefined;
    let cleanupProved = false;
    try {
      await withSyntheticPreparationFixture(
        { ...seed, threads: [{ id: "thread:malformed", malformedMessage: true }] },
        async (fixture) => {
          root = (await fixture.inspect()).scratchRoot;
          fixture.injectFailure(
            source === "mutation" ? "mutation-close-after-close" : "read-scope-after-close",
          );
          if (source === "mutation") await fixture.mutate("projection-drift");
          else await fixture.plan();
        },
      );
      assert.fail("Expected both operation and finalization failures.");
    } catch (cause) {
      assert.instanceOf(cause, OfflinePreparationError);
      const error = cause as OfflinePreparationError;
      const evidence = JSON.parse(
        await NodeFSP.readFile(NodePath.join(root!, "retained-evidence.json"), "utf8"),
      );
      const closed = evidence.audit.lastIndexOf(source + ":native-close-completed");
      cleanupProved = closed >= 0 && evidence.audit[closed + 1] === source + ":close-unresolved";
      assert.isTrue(cleanupProved);
      assert.equal(error.reason, "unresolved-sqlite-custody");
      const custody = error.cause as OfflinePreparationError;
      assert.instanceOf(custody, OfflinePreparationError);
      assert.instanceOf(custody.cause, AggregateError);
      const errors = (custody.cause as AggregateError).errors as unknown[];
      assert.lengthOf(errors, 2);
      if (source === "mutation")
        assert.equal((errors[0] as OfflinePreparationError).reason, "missing-fixture-plan");
      else assert.match(String(errors[0]), /Projection|projection|decode|Decode/);
      assert.equal(
        (errors[1] as OfflinePreparationError).reason,
        source === "mutation" ? "mutation-close-after-close" : "read-scope-after-close",
      );
    } finally {
      if (cleanupProved && root !== undefined)
        await NodeFSP.rm(root, { recursive: true, force: true });
    }
    await absent(root!);
  }
});

it("retains bounded recovery evidence and the original failure when commit is unresolved", async () => {
  let retained: string | undefined;
  let cleanupProved = false;
  try {
    await withSyntheticPreparationFixture(seed, async (fixture) => {
      const plan = await fixture.plan();
      fixture.injectFailure("after-commit");
      await fixture.apply(plan);
    });
    assert.fail("Expected retained recovery evidence.");
  } catch (cause) {
    assert.instanceOf(cause, OfflinePreparationError);
    const error = cause as OfflinePreparationError;
    retained = error.retainedRoot;
    assert.isString(retained);
    assert.equal(error.reason, "unresolved-synthetic-effect");
    assert.instanceOf(error.cause, OfflinePreparationError);
    assert.equal((error.cause as OfflinePreparationError).reason, "after-commit");
    const evidence = JSON.parse(
      await NodeFSP.readFile(NodePath.join(retained!, "retained-evidence.json"), "utf8"),
    );
    assert.deepEqual(evidence.custody, []);
    cleanupProved = true;
    assert.include(evidence.audit, "transaction:committed");
    assert.isFalse(evidence.productionInvocation);
    assert.equal(evidence.source, "bd20d2d2fc0ba89797698192b877a7f1312f8fe3");
    await NodeFSP.stat(NodePath.join(retained!, "previous.sqlite"));
    await NodeFSP.stat(NodePath.join(retained!, "continuation-plan.json"));
  } finally {
    // This test owns the returned synthetic root; all scoped SQLite clients
    // have already closed, and the assertions consumed its required evidence.
    if (cleanupProved && retained !== undefined)
      await NodeFSP.rm(retained, { recursive: true, force: true });
  }
  await absent(retained!);
});

it("preserves the original callback failure and exact root when cleanup also fails", async () => {
  const original = new Error("synthetic callback failure");
  let root = "";
  try {
    await withSyntheticPreparationFixture(seed, async (fixture) => {
      root = (await fixture.inspect()).scratchRoot;
      fixture.injectFailure("cleanup");
      throw original;
    });
    assert.fail("Expected both failures.");
  } catch (cause) {
    assert.instanceOf(cause, AggregateError);
    const errors = (cause as AggregateError).errors as unknown[];
    assert.strictEqual(errors[0], original);
    assert.instanceOf(errors[1], OfflinePreparationError);
    assert.equal((errors[1] as OfflinePreparationError).retainedRoot, root);
    await NodeFSP.stat(NodePath.join(root, "previous.sqlite"));
  } finally {
    if (root !== "") await NodeFSP.rm(root, { recursive: true, force: true });
  }
  await absent(root);
});
