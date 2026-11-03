import { assert, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Switch from "../../orchestration-v2/ProviderSwitchService.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import { prepareExecutionSession } from "./prepareExecutionSession.ts";

function fixture(driverName: "codex" | "opencode", shared: boolean, worktreePath: string | null) {
  const driver = ProviderDriverKind.make(driverName);
  const instanceId = ProviderInstanceId.make("workspace-provider");
  const threadId = ThreadId.make("second-workspace-thread");
  const sessionId = ProviderSessionId.make("shared-opener-session");
  const providerThreadId = ProviderThreadId.make("second-workspace-conversation");
  const selection = { instanceId, model: "current-model" };
  const now = DateTime.makeUnsafe("2026-10-29T00:00:00.000Z");
  const capabilities = {
    ...CodexProviderCapabilitiesV2,
    sessions: {
      ...CodexProviderCapabilitiesV2.sessions,
      supportsMultipleProviderThreadsPerSession: shared,
    },
  };
  const projection = {
    thread: {
      id: threadId,
      modelSelection: selection,
      activeProviderThreadId: providerThreadId,
      runtimeMode: "full-access",
      interactionMode: "default",
      worktreePath,
    },
    runs: [],
    providerSessions: [
      {
        id: sessionId,
        providerInstanceId: instanceId,
        driver,
        status: "ready",
        cwd: "/workspace/opener",
        capabilities,
        updatedAt: now,
      },
    ],
    providerThreads: [
      {
        id: providerThreadId,
        appThreadId: threadId,
        ownerNodeId: null,
        providerInstanceId: instanceId,
        providerSessionId: sessionId,
        driver,
        nativeThreadRef: { driver, nativeId: "saved-conversation", strength: "strong" },
        updatedAt: now,
      },
    ],
  } as unknown as OrchestrationV2ThreadProjection;
  const layer = Switch.layer.pipe(
    Layer.provide(
      Layer.mock(Registry.ProviderAdapterRegistryV2)({
        getMetadata: () =>
          Effect.succeed({ driver, continuationKey: "same-account", enabled: true, capabilities }),
        get: () =>
          Effect.succeed({
            instanceId,
            driver,
            getCapabilities: () => Effect.succeed(capabilities),
            planSelectionTransition: () => Effect.succeed({ type: "restart_session" as const }),
            openSession: () => Effect.die("Planning must not open a provider process."),
          }),
      }),
    ),
  );
  return { projection, selection, layer };
}

it.effect.each(["codex", "opencode"] as const)(
  "%s follow-up turns retain their conversation when a shared process opened in another workspace",
  (driver) => {
    const { projection, selection, layer } = fixture(driver, true, "/workspace/current");
    return Effect.gen(function* () {
      const service = yield* Switch.ProviderSwitchServiceV2;
      const provider = projection.providerThreads[0]!;
      for (const ordinal of [2, 3]) {
        const plan = yield* service.plan({ projection, targetModelSelection: selection });
        assert.equal(plan.transition.type, "reuse");
        assert.deepEqual(plan.releaseProviderSessionIds, []);
        const run = {
          id: RunId.make(`follow-up-${ordinal}`),
          threadId: projection.thread.id,
          activeAttemptId: RunAttemptId.make(`attempt-${ordinal}`),
          providerInstanceId: selection.instanceId,
          modelSelection: selection,
        } as OrchestrationV2ThreadProjection["runs"][number];
        const prepared = yield* prepareExecutionSession(run, provider, projection);
        assert.strictEqual(prepared, provider);
        assert.equal(prepared.providerSessionId, projection.providerSessions[0]!.id);
        assert.equal(prepared.nativeThreadRef?.nativeId, "saved-conversation");
      }
      assert.equal(projection.providerSessions[0]!.cwd, "/workspace/opener");
      assert.equal(projection.thread.worktreePath, "/workspace/current");
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          layer,
          Layer.mock(Sessions.ProviderSessionManagerV2)({
            detach: () =>
              Effect.die("An unchanged shared conversation must not retire its writer."),
          }),
          Layer.mock(Sink.EventSinkV2)({
            writeIfRunCurrent: () => Effect.die("Reuse must not persist a replacement binding."),
          }),
        ),
      ),
    );
  },
);

it.effect("a single-thread session still retires when its workspace changes", () => {
  const { projection, selection, layer } = fixture("codex", false, "/workspace/current");
  return Effect.gen(function* () {
    const service = yield* Switch.ProviderSwitchServiceV2;
    const plan = yield* service.plan({ projection, targetModelSelection: selection });
    assert.equal(plan.transition.type, "restart_and_resume");
    assert.deepEqual(plan.releaseProviderSessionIds, [projection.providerSessions[0]!.id]);
  }).pipe(Effect.provide(layer));
});

it.effect("a shared session still honors an adapter-required selection restart", () => {
  const { projection, selection, layer } = fixture("codex", true, "/workspace/current");
  return Effect.gen(function* () {
    const service = yield* Switch.ProviderSwitchServiceV2;
    const plan = yield* service.plan({
      projection,
      targetModelSelection: { ...selection, model: "replacement-model" },
    });
    assert.equal(plan.transition.type, "restart_and_resume");
    assert.deepEqual(plan.releaseProviderSessionIds, [projection.providerSessions[0]!.id]);
  }).pipe(Effect.provide(layer));
});

it.effect("a thread without a worktree retains the session workspace fallback", () => {
  const { projection, selection, layer } = fixture("codex", true, null);
  return Effect.gen(function* () {
    const service = yield* Switch.ProviderSwitchServiceV2;
    const plan = yield* service.plan({ projection, targetModelSelection: selection });
    assert.equal(plan.transition.type, "reuse");
    assert.deepEqual(plan.releaseProviderSessionIds, []);
  }).pipe(Effect.provide(layer));
});
