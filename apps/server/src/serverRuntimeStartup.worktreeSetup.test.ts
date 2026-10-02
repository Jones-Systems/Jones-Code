import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import nativeCreationMigration from "./persistence/Migrations/003_JonesNativeCreationIntents.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  EventId,
  type OrchestrationCommand,
  ThreadId,
  WORKTREE_SETUP_ACTIVITY_KIND,
  WorktreeSetupSnapshot,
  worktreeSetupActivityId,
  type WorktreeSetupPhase,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as OrchestrationEngine from "./orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "./orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ServerRuntimeStartup from "./serverRuntimeStartup.ts";

const startedAt = "2026-08-20T12:00:00.000Z";

const snapshotFor = (
  threadId: ThreadId,
  phase: WorktreeSetupPhase,
  agentStatus: "pending" | "done" = phase === "running" ? "pending" : "done",
): WorktreeSetupSnapshot => ({
  threadId,
  phase,
  startedAt,
  endedAt: phase === "running" ? null : startedAt,
  branch: "feature",
  baseRef: "main",
  worktreePath: null,
  setupScript: null,
  stages: [
    {
      id: "checkout",
      status: "done",
      startedAt,
      endedAt: startedAt,
      percent: null,
      detail: null,
      tail: [],
    },
    {
      id: "setup-script",
      status: phase === "running" ? "running" : "done",
      startedAt,
      endedAt: phase === "running" ? null : startedAt,
      percent: null,
      detail: null,
      tail: [],
    },
    {
      id: "agent",
      status: agentStatus,
      startedAt: null,
      endedAt: null,
      percent: null,
      detail: null,
      tail: [],
    },
  ],
  error: null,
  sequence: 4,
});

const recordedSetup = (id: string, phase: WorktreeSetupPhase, agentStatus?: "pending" | "done") => {
  const threadId = ThreadId.make(id);
  return {
    id: EventId.make(worktreeSetupActivityId(threadId)),
    tone: "info" as const,
    kind: WORKTREE_SETUP_ACTIVITY_KIND,
    summary: "Setting up worktree",
    payload: snapshotFor(threadId, phase, agentStatus),
    turnId: null,
    createdAt: startedAt,
  };
};

const run = (
  activities: ReadonlyArray<ReturnType<typeof recordedSetup>>,
  claimedThreadId?: string,
) =>
  Effect.gen(function* () {
    const dispatched: Array<OrchestrationCommand> = [];
    const sql = yield* SqlClient.SqlClient;
    yield* nativeCreationMigration;
    if (claimedThreadId !== undefined)
      yield* sql`INSERT INTO native_creation_intents (claim_id, operation_id, preparation_id, command_id, thread_id, message_id, project_cwd, branch, worktree_path, canonical_preparation, intent_json) VALUES ('synthetic-claim', 'synthetic-operation', 'synthetic-preparation', 'synthetic-command', ${claimedThreadId}, 'synthetic-message', '/synthetic/project', 'synthetic-branch', '/synthetic/worktree', '{}', '{}')`;
    yield* ServerRuntimeStartup.reconcileWorktreeSetups.pipe(
      Effect.provideService(ProjectionSnapshotQuery.ProjectionSnapshotQuery, {
        listActivitiesByKind: (kind: string) =>
          Effect.succeed(kind === WORKTREE_SETUP_ACTIVITY_KIND ? activities : []),
      } as unknown as ProjectionSnapshotQuery.ProjectionSnapshotQuery["Service"]),
      Effect.provideService(OrchestrationEngine.OrchestrationEngineService, {
        readEvents: () => Stream.empty,
        readThreadEvents: () => Stream.empty,
        getThreadReplayStats: () => Effect.die("unused"),
        dispatch: (command) =>
          Effect.sync(() => {
            dispatched.push(command);
            return { sequence: dispatched.length };
          }),
        acquireWorktreeOwnership: () => Effect.die("unused ownership acquisition"),
        releaseWorktreeOwnership: () => Effect.die("unused ownership release"),
        getThreadOwnershipIncarnation: () => Effect.die("unused ownership incarnation"),
        listWorktreeOwnershipLeases: Effect.die("unused ownership list"),
        streamDomainEvents: Stream.empty,
        subscribeDomainEvents: Effect.succeed(Stream.empty),
        latestSequence: Effect.succeed(0),
      }),
      Effect.provide(NodeServices.layer),
    );
    return dispatched;
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" })));

it.effect("marks setups still recorded as running failed after a restart", () =>
  Effect.gen(function* () {
    const dispatched = yield* run([
      recordedSetup("thread-running", "running"),
      recordedSetup("thread-done", "done"),
      recordedSetup("thread-failed", "failed"),
    ]);

    assert.equal(dispatched.length, 1);
    const command = dispatched[0]!;
    assert.equal(command.type, "thread.activity.append");
    if (command.type !== "thread.activity.append") return;
    assert.equal(command.threadId, ThreadId.make("thread-running"));
    assert.equal(command.activity.id, worktreeSetupActivityId(ThreadId.make("thread-running")));
    assert.equal(command.activity.tone, "error");
    const payload = yield* Schema.decodeUnknownEffect(WorktreeSetupSnapshot)(
      command.activity.payload,
    );
    assert.equal(payload.phase, "failed");
    assert.isNotNull(payload.endedAt);
    assert.equal(payload.sequence, 5);
    assert.deepEqual(
      payload.stages.map((stage) => stage.status),
      ["done", "failed", "failed"],
    );
  }),
);

it.effect(
  "settles an async setup script whose turn already started without failing the setup",
  () =>
    Effect.gen(function* () {
      const dispatched = yield* run([recordedSetup("thread-async", "running", "done")]);

      assert.equal(dispatched.length, 1);
      const command = dispatched[0]!;
      if (command.type !== "thread.activity.append") return assert.fail(command.type);
      const payload = yield* Schema.decodeUnknownEffect(WorktreeSetupSnapshot)(
        command.activity.payload,
      );
      // The turn is live; only the background script was lost. Nothing asks the
      // user to resend, and the setup reads as done with a failed script stage.
      assert.equal(payload.phase, "done");
      assert.isNull(payload.error);
      assert.deepEqual(
        payload.stages.map((stage) => stage.status),
        ["done", "failed", "done"],
      );
    }),
);

it.effect(
  "restart preserves claimed creation setup without command replay or forced readiness",
  () =>
    Effect.gen(function* () {
      const dispatched = yield* run(
        [recordedSetup("synthetic-claimed", "running", "done")],
        "synthetic-claimed",
      );
      assert.deepEqual(dispatched, []);
    }),
);
