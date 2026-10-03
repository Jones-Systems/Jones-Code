import { it } from "@effect/vitest";
import { expect, vi } from "vite-plus/test";
import { ProviderDriverKind, ProviderInstanceId, ProviderSessionId, ProviderThreadId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import type { ProviderAdapterV2SessionRuntime } from "./ProviderAdapter.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderSessionGoalService from "./ProviderSessionGoalService.ts";

function harness(input: { readonly resident?: boolean; readonly changeGeneration?: boolean; readonly changeCursor?: boolean; readonly driver?: string } = {}) {
  const threadId = ThreadId.make("goal-thread");
  const instanceId = ProviderInstanceId.make("codex-goal");
  const sessionId = ProviderSessionId.make("goal-session");
  const providerThreadId = ProviderThreadId.make("goal-provider-thread");
  let generation = "generation-one";
  let cursor = "native-goal-thread";
  const getGoal = vi.fn(() => Effect.sync(() => {
    if (input.changeGeneration) generation = "generation-two";
    if (input.changeCursor) cursor = "replacement-native-thread";
    return { nativeThreadId: "native-goal-thread", state: "inactive" as const, reasonCode: "goal_null" as const };
  }));
  const runtime = {
    instanceId,
    driver: ProviderDriverKind.make(input.driver ?? "codex"),
    providerSessionId: sessionId,
    providerSession: { status: "ready" },
    get runtimeGeneration() { return generation; },
    getGoal,
  } as ProviderAdapterV2SessionRuntime;
  const open = vi.fn(() => Effect.die("Goal observation must not open a runtime."));
  const layer = ProviderSessionGoalService.layer.pipe(Layer.provide(Layer.mergeAll(
    Layer.mock(ProjectionStore.ProjectionStoreV2)({
      getThreadProviderContext: () => Effect.succeed({
        thread: { modelSelection: { instanceId }, activeProviderThreadId: providerThreadId },
        providerThreads: [{ id: providerThreadId, appThreadId: threadId, driver: runtime.driver, providerInstanceId: instanceId, providerSessionId: sessionId, nativeThreadRef: { nativeId: cursor } }],
        providerSessions: [{ id: sessionId, providerInstanceId: instanceId, driver: runtime.driver, status: "ready" }],
      } as ProjectionStore.ProjectionThreadProviderContext),
    }),
    Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
      get: () => Effect.succeed(input.resident === false ? Option.none() : Option.some(runtime)),
      open,
    }),
  )));
  return {
    open, getGoal,
    read: Effect.gen(function* () {
      return yield* (yield* ProviderSessionGoalService.ProviderSessionGoalService).get({ threadId, expectedInstanceId: instanceId });
    }).pipe(Effect.provide(layer)),
  };
}

it.effect("reads an inactive goal from the resident runtime without opening it", () => Effect.gen(function* () {
  const test = harness();
  expect(yield* test.read).toEqual({ nativeThreadId: "native-goal-thread", state: "inactive", reasonCode: "goal_null" });
  expect(test.getGoal).toHaveBeenCalledOnce();
  expect(test.open).not.toHaveBeenCalled();
}));

it.effect("does not resurrect a historical session to observe its goal", () => Effect.gen(function* () {
  const test = harness({ resident: false });
  expect(yield* test.read).toMatchObject({ state: "unknown", reasonCode: "no_session" });
  expect(test.getGoal).not.toHaveBeenCalled();
  expect(test.open).not.toHaveBeenCalled();
}));

it.effect("rejects a goal reply across an actual runtime replacement", () => Effect.gen(function* () {
  expect(yield* harness({ changeGeneration: true }).read).toMatchObject({ state: "unknown", reasonCode: "context_changed" });
}));

it.effect("rejects a goal reply when the native cursor changed during observation", () => Effect.gen(function* () {
  expect(yield* harness({ changeCursor: true }).read).toMatchObject({ state: "unknown", reasonCode: "context_changed" });
}));

it.effect("reports unsupported native goals for other providers without issuing a request", () => Effect.gen(function* () {
  const test = harness({ driver: "claude-code" });
  expect(yield* test.read).toMatchObject({ state: "unknown", reasonCode: "unsupported" });
  expect(test.getGoal).not.toHaveBeenCalled();
  expect(test.open).not.toHaveBeenCalled();
}));
