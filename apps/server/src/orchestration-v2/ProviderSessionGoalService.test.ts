import { it } from "@effect/vitest";
import { expect, vi } from "vite-plus/test";
import {
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import type { ProviderAdapterV2SessionRuntime } from "./ProviderAdapter.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderSessionGoalService from "./ProviderSessionGoalService.ts";

function harness(
  input: {
    readonly resident?: boolean;
    readonly changeGeneration?: boolean;
    readonly changeCursor?: boolean;
    readonly driver?: string;
  } = {},
) {
  const threadId = ThreadId.make("goal-thread");
  const instanceId = ProviderInstanceId.make("codex-goal");
  const sessionId = ProviderSessionId.make("goal-session");
  const providerThreadId = ProviderThreadId.make("goal-provider-thread");
  let generation = "generation-one";
  let cursor = "native-goal-thread";
  const getGoal = vi.fn(() =>
    Effect.sync(() => {
      if (input.changeGeneration) generation = "generation-two";
      if (input.changeCursor) cursor = "replacement-native-thread";
      return {
        nativeThreadId: "native-goal-thread",
        state: "inactive" as const,
        reasonCode: "goal_null" as const,
      };
    }),
  );
  const now = DateTime.makeUnsafe("2026-10-04T00:00:00.000Z");
  const driver = ProviderDriverKind.make(input.driver ?? "codex");
  const modelSelection = { instanceId, model: "gpt-5.4" };
  const providerSession: OrchestrationV2ProviderSession = {
    id: sessionId,
    driver,
    providerInstanceId: instanceId,
    status: "ready",
    cwd: "/fixture/goal-workspace",
    model: modelSelection.model,
    capabilities: CodexProviderCapabilitiesV2,
    createdAt: now,
    updatedAt: now,
    lastError: null,
  };
  const unused = () => Effect.die("Goal observation must not use this runtime capability.");
  const runtime: ProviderAdapterV2SessionRuntime = {
    instanceId,
    driver,
    providerSessionId: sessionId,
    providerSession,
    events: Stream.empty,
    get runtimeGeneration() {
      return generation;
    },
    getGoal,
    ensureThread: unused,
    resumeThread: unused,
    startTurn: unused,
    steerTurn: unused,
    interruptTurn: unused,
    respondToRuntimeRequest: unused,
    readThreadSnapshot: unused,
    rollbackThread: unused,
    forkThread: unused,
  };
  const thread: OrchestrationV2AppThread = {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId: ProjectId.make("goal-project"),
    title: "Goal observation",
    providerInstanceId: instanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: providerThreadId,
    lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  const providerThread = (nativeId: string): OrchestrationV2ProviderThread => ({
    id: providerThreadId,
    driver,
    providerInstanceId: instanceId,
    providerSessionId: sessionId,
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef: { driver, nativeId, strength: "strong" },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  });
  const open = vi.fn(() => Effect.die("Goal observation must not open a runtime."));
  const layer = ProviderSessionGoalService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThreadProviderContext: () =>
            Effect.succeed({
              thread,
              providerThreads: [providerThread(cursor)],
              providerSessions: [providerSession],
            }),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          get: () =>
            Effect.succeed(input.resident === false ? Option.none() : Option.some(runtime)),
          open,
        }),
      ),
    ),
  );
  return {
    open,
    getGoal,
    read: Effect.gen(function* () {
      return yield* (yield* ProviderSessionGoalService.ProviderSessionGoalService).get({
        threadId,
        expectedInstanceId: instanceId,
      });
    }).pipe(Effect.provide(layer)),
  };
}

it.effect("reads an inactive goal from the resident runtime without opening it", () =>
  Effect.gen(function* () {
    const test = harness();
    expect(yield* test.read).toEqual({
      nativeThreadId: "native-goal-thread",
      state: "inactive",
      reasonCode: "goal_null",
    });
    expect(test.getGoal).toHaveBeenCalledOnce();
    expect(test.open).not.toHaveBeenCalled();
  }),
);

it.effect("does not resurrect a historical session to observe its goal", () =>
  Effect.gen(function* () {
    const test = harness({ resident: false });
    expect(yield* test.read).toMatchObject({ state: "unknown", reasonCode: "no_session" });
    expect(test.getGoal).not.toHaveBeenCalled();
    expect(test.open).not.toHaveBeenCalled();
  }),
);

it.effect("rejects a goal reply across an actual runtime replacement", () =>
  Effect.gen(function* () {
    expect(yield* harness({ changeGeneration: true }).read).toMatchObject({
      state: "unknown",
      reasonCode: "context_changed",
    });
  }),
);

it.effect("rejects a goal reply when the native cursor changed during observation", () =>
  Effect.gen(function* () {
    expect(yield* harness({ changeCursor: true }).read).toMatchObject({
      state: "unknown",
      reasonCode: "context_changed",
    });
  }),
);

it.effect("reports unsupported native goals for other providers without issuing a request", () =>
  Effect.gen(function* () {
    const test = harness({ driver: "claude-code" });
    expect(yield* test.read).toMatchObject({ state: "unknown", reasonCode: "unsupported" });
    expect(test.getGoal).not.toHaveBeenCalled();
    expect(test.open).not.toHaveBeenCalled();
  }),
);
