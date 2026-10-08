import { assert, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import { acpSelectionTransition } from "./ProviderSelectionTransition.ts";
import * as ProviderSwitch from "./ProviderSwitchService.ts";

const driver = ProviderDriverKind.make("codex");
const currentInstanceId = ProviderInstanceId.make("codex_primary");
const currentSessionId = ProviderSessionId.make("session_primary");
const now = DateTime.makeUnsafe("2026-06-20T00:00:00.000Z");
const missingConversationMessage =
  "Cannot switch Codex accounts without a valid saved conversation. Check that both accounts share the Codex sessions directory.";
const incompatibleConversationMessage =
  "Cannot switch Codex accounts because the saved conversation is not compatible with the target account. Check that both accounts share the Codex sessions directory.";
const capabilitiesWithoutModelSwitch = {
  ...CodexProviderCapabilitiesV2,
  sessions: {
    ...CodexProviderCapabilitiesV2.sessions,
    supportsModelSwitchInSession: false,
  },
};

function projection(): OrchestrationV2ThreadProjection {
  return {
    thread: {
      id: ThreadId.make("thread_switch_service"),
      modelSelection: { instanceId: currentInstanceId, model: "gpt-5.1-codex" },
      runtimeMode: "full-access",
      interactionMode: "default",
      worktreePath: "/repo",
    },
    providerSessions: [
      {
        id: currentSessionId,
        providerInstanceId: currentInstanceId,
        status: "ready",
        cwd: "/repo",
        capabilities: capabilitiesWithoutModelSwitch,
        updatedAt: now,
      },
    ],
    providerThreads: [],
  } as unknown as OrchestrationV2ThreadProjection;
}

function deadSessionRecord(
  id: string,
  status: "stopped" | "error",
  updatedAt: DateTime.Utc = DateTime.add(now, { seconds: 1 }),
) {
  return {
    ...projection().providerSessions[0]!,
    id: ProviderSessionId.make(id),
    status,
    updatedAt,
  };
}

function layerTest(
  metadata: Readonly<Record<string, { continuationKey: string; driver?: typeof driver }>>,
  planSelectionTransition: ProviderAdapterV2Shape["planSelectionTransition"] = () =>
    Effect.succeed({ type: "restart_session" }),
) {
  const adapter = (instanceId: ProviderInstanceId): ProviderAdapterV2Shape => ({
    instanceId,
    driver: metadata[instanceId]?.driver ?? driver,
    getCapabilities: () => Effect.succeed(capabilitiesWithoutModelSwitch),
    planSelectionTransition,
    openSession: () => Effect.die("ProviderSwitchService tests do not open sessions."),
  });
  const layerRegistry = Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
    get: (instanceId) =>
      metadata[instanceId] === undefined
        ? Effect.fail(
            new ProviderAdapterRegistry.ProviderAdapterRegistryLookupError({ instanceId }),
          )
        : Effect.succeed(adapter(instanceId)),
    list: () => Effect.succeed(Object.keys(metadata).map((id) => ProviderInstanceId.make(id))),
    getMetadata: (instanceId) => {
      const value = metadata[instanceId];
      return value === undefined
        ? Effect.fail(
            new ProviderAdapterRegistry.ProviderAdapterRegistryLookupError({ instanceId }),
          )
        : Effect.succeed({
            driver: value.driver ?? driver,
            continuationKey: value.continuationKey,
            enabled: true,
            capabilities: capabilitiesWithoutModelSwitch,
          });
    },
  });
  return ProviderSwitch.layer.pipe(Layer.provide(layerRegistry));
}

it.effect(
  "restarts and releases the current session for unsupported in-session model changes",
  () =>
    Effect.gen(function* () {
      const service = yield* ProviderSwitch.ProviderSwitchServiceV2;
      const result = yield* service.plan({
        projection: projection(),
        targetModelSelection: { instanceId: currentInstanceId, model: "gpt-5.2-codex" },
      });
      assert.equal(result.transition.type, "restart_and_resume");
      assert.deepEqual(result.releaseProviderSessionIds, [currentSessionId]);
    }).pipe(
      Effect.provide(
        layerTest({ [currentInstanceId]: { continuationKey: "codex:account:primary" } }),
      ),
    ),
);

it.effect.each(["stopped", "error"] as const)(
  "restarts and releases the live session when a newer %s session exists",
  (deadStatus) =>
    Effect.gen(function* () {
      const service = yield* ProviderSwitch.ProviderSwitchServiceV2;
      const thread = projection();
      const result = yield* service.plan({
        projection: {
          ...thread,
          providerSessions: [
            ...thread.providerSessions,
            deadSessionRecord("dead_session", deadStatus),
          ],
        },
        targetModelSelection: { instanceId: currentInstanceId, model: "gpt-5.2-codex" },
      });
      assert.equal(result.transition.type, "restart_and_resume");
      assert.deepEqual(result.releaseProviderSessionIds, [currentSessionId]);
    }).pipe(
      Effect.provide(
        layerTest({ [currentInstanceId]: { continuationKey: "codex:account:primary" } }),
      ),
    ),
);

it.effect("releases the newest live session, not the newest record overall", () =>
  Effect.gen(function* () {
    const service = yield* ProviderSwitch.ProviderSwitchServiceV2;
    const thread = projection();
    const newerLiveSessionId = ProviderSessionId.make("session_newer_live");
    const result = yield* service.plan({
      projection: {
        ...thread,
        providerSessions: [
          ...thread.providerSessions,
          {
            ...thread.providerSessions[0]!,
            id: newerLiveSessionId,
            updatedAt: DateTime.add(now, { seconds: 1 }),
          },
          deadSessionRecord("dead_session", "stopped", DateTime.add(now, { seconds: 2 })),
        ],
      },
      targetModelSelection: { instanceId: currentInstanceId, model: "gpt-5.2-codex" },
    });
    assert.equal(result.transition.type, "restart_and_resume");
    assert.deepEqual(result.releaseProviderSessionIds, [newerLiveSessionId]);
  }).pipe(
    Effect.provide(
      layerTest({ [currentInstanceId]: { continuationKey: "codex:account:primary" } }),
    ),
  ),
);

it.effect("creates a fresh session with handoff when every recorded session is dead", () =>
  Effect.gen(function* () {
    const service = yield* ProviderSwitch.ProviderSwitchServiceV2;
    const thread = projection();
    const result = yield* service.plan({
      projection: {
        ...thread,
        providerSessions: [
          deadSessionRecord("dead_session_older", "stopped"),
          deadSessionRecord("dead_session_newer", "error", DateTime.add(now, { seconds: 2 })),
        ],
      },
      targetModelSelection: { instanceId: currentInstanceId, model: "gpt-5.2-codex" },
    });
    assert.equal(result.transition.type, "create_with_handoff");
    assert.deepEqual(result.releaseProviderSessionIds, []);
  }).pipe(
    Effect.provide(
      layerTest({ [currentInstanceId]: { continuationKey: "codex:account:primary" } }),
    ),
  ),
);

function deadNativeThreadProjection(
  status: "stopped" | "error",
  capabilities = capabilitiesWithoutModelSwitch,
): OrchestrationV2ThreadProjection {
  const thread = projection();
  return {
    ...thread,
    thread: {
      ...thread.thread,
      activeProviderThreadId: ProviderThreadId.make("provider-thread:native"),
    },
    providerSessions: [{ ...deadSessionRecord("dead_session", status), capabilities }],
    providerThreads: [
      {
        id: ProviderThreadId.make("provider-thread:native"),
        driver,
        providerInstanceId: currentInstanceId,
        providerSessionId: ProviderSessionId.make("dead_session"),
        appThreadId: thread.thread.id,
        ownerNodeId: null,
        nativeThreadRef: {
          driver,
          nativeId: "native-thread:abc",
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      },
    ],
  } as OrchestrationV2ThreadProjection;
}

it.effect("falls back to the native provider thread when every recorded session is dead", () =>
  Effect.gen(function* () {
    const service = yield* ProviderSwitch.ProviderSwitchServiceV2;
    const result = yield* service.plan({
      projection: deadNativeThreadProjection("stopped"),
      targetModelSelection: { instanceId: currentInstanceId, model: "gpt-5.2-codex" },
    });
    assert.equal(result.transition.type, "restart_and_resume");
    assert.deepEqual(result.releaseProviderSessionIds, []);
  }).pipe(
    Effect.provide(
      layerTest({ [currentInstanceId]: { continuationKey: "codex:account:primary" } }),
    ),
  ),
);

it.effect("rejects incompatible Codex account continuation after its session detaches", () =>
  Effect.gen(function* () {
    const service = yield* ProviderSwitch.ProviderSwitchServiceV2;
    const result = yield* service
      .plan({
        projection: { ...deadNativeThreadProjection("stopped"), providerSessions: [] },
        targetModelSelection: {
          instanceId: ProviderInstanceId.make("codex_other"),
          model: "gpt-5.2-codex",
        },
      })
      .pipe(Effect.flip);
    assert.instanceOf(result, ProviderSwitch.ProviderSwitchPlanError);
    assert.nestedPropertyVal(result, "cause.cause", incompatibleConversationMessage);
  }).pipe(
    Effect.provide(
      layerTest({
        [currentInstanceId]: { continuationKey: "codex:account:primary" },
        codex_other: { continuationKey: "codex:account:other" },
      }),
    ),
  ),
);

it.effect.each(["stopped", "error"] as const)(
  "applies a model change on next turn when a %s session negotiated model switching",
  (deadStatus) =>
    Effect.gen(function* () {
      const service = yield* ProviderSwitch.ProviderSwitchServiceV2;
      const result = yield* service.plan({
        projection: deadNativeThreadProjection(deadStatus, CodexProviderCapabilitiesV2),
        targetModelSelection: { instanceId: currentInstanceId, model: "gpt-5.2-codex" },
      });
      // Static capabilities report no in-session switch, but the dead
      // record's negotiated capabilities describe the provider: without
      // them the ACP classification rejects the selection instead of
      // reopening with the requested model on the next run.
      assert.equal(result.transition.type, "switch_model_in_session");
      assert.deepEqual(result.releaseProviderSessionIds, []);
    }).pipe(
      Effect.provide(
        layerTest({ [currentInstanceId]: { continuationKey: "codex:account:primary" } }, (input) =>
          Effect.succeed(acpSelectionTransition(input)),
        ),
      ),
    ),
);

it.effect("rejects a model change the dead record never negotiated support for", () =>
  Effect.gen(function* () {
    const service = yield* ProviderSwitch.ProviderSwitchServiceV2;
    const result = yield* service
      .plan({
        projection: deadNativeThreadProjection("stopped"),
        targetModelSelection: { instanceId: currentInstanceId, model: "gpt-5.2-codex" },
      })
      .pipe(Effect.flip);
    assert.instanceOf(result, ProviderSwitch.ProviderSwitchPlanError);
  }).pipe(
    Effect.provide(
      layerTest({ [currentInstanceId]: { continuationKey: "codex:account:primary" } }, (input) =>
        Effect.succeed(acpSelectionTransition(input)),
      ),
    ),
  ),
);

it.effect("distinguishes compatible and incompatible instances of the same driver", () =>
  Effect.gen(function* () {
    const service = yield* ProviderSwitch.ProviderSwitchServiceV2;
    const compatibleId = ProviderInstanceId.make("codex_compatible");
    const incompatibleId = ProviderInstanceId.make("codex_incompatible");
    const compatible = yield* service.plan({
      projection: deadNativeThreadProjection("stopped"),
      targetModelSelection: { instanceId: compatibleId, model: "gpt-5.1-codex" },
    });
    const incompatible = yield* service
      .plan({
        projection: deadNativeThreadProjection("stopped"),
        targetModelSelection: { instanceId: incompatibleId, model: "gpt-5.1-codex" },
      })
      .pipe(Effect.flip);
    assert.equal(compatible.transition.type, "restart_and_resume");
    assert.instanceOf(incompatible, ProviderSwitch.ProviderSwitchPlanError);
  }).pipe(
    Effect.provide(
      layerTest({
        [currentInstanceId]: { continuationKey: "codex:account:primary" },
        codex_compatible: { continuationKey: "codex:account:primary" },
        codex_incompatible: { continuationKey: "codex:account:other" },
      }),
    ),
  ),
);

it.effect.each(["stopped", "error"] as const)(
  "resumes the saved native binding on a compatible Codex account after a %s session",
  (status) =>
    Effect.gen(function* () {
      const service = yield* ProviderSwitch.ProviderSwitchServiceV2;
      const target = ProviderInstanceId.make("codex_work");
      const input = deadNativeThreadProjection(status);
      const result = yield* service.plan({
        projection: input,
        targetModelSelection: { instanceId: target, model: "gpt-5.4" },
      });
      assert.equal(result.instanceChanged, true);
      assert.equal(result.transition.type, "restart_and_resume");
      assert.deepEqual(result.releaseProviderSessionIds, []);
      assert.equal(input.providerThreads[0]?.nativeThreadRef?.nativeId, "native-thread:abc");
      assert.equal(input.thread.worktreePath, "/repo");
    }).pipe(
      Effect.provide(
        layerTest({
          [currentInstanceId]: { continuationKey: "codex:home:/shared/sessions" },
          codex_work: { continuationKey: "codex:home:/shared/sessions" },
        }),
      ),
    ),
);

it.effect.each(["missing", "null-id", "empty", "blank", "wrong-driver", "wrong-thread"] as const)(
  "rejects a %s saved Codex cursor before planning an account switch",
  (invalid) =>
    Effect.gen(function* () {
      const service = yield* ProviderSwitch.ProviderSwitchServiceV2;
      const input = deadNativeThreadProjection("stopped");
      const providerThread = input.providerThreads[0]!;
      const result = yield* service
        .plan({
          projection: {
            ...input,
            providerThreads: [
              {
                ...providerThread,
                appThreadId:
                  invalid === "wrong-thread" ? ThreadId.make("another-thread") : input.thread.id,
                nativeThreadRef:
                  invalid === "missing"
                    ? null
                    : {
                        driver:
                          invalid === "wrong-driver" ? ProviderDriverKind.make("claude") : driver,
                        nativeId:
                          invalid === "null-id"
                            ? null
                            : invalid === "empty"
                              ? ""
                              : invalid === "blank"
                                ? "  "
                                : "saved-native-thread",
                        strength: "strong",
                      },
              },
            ],
          },
          targetModelSelection: {
            instanceId: ProviderInstanceId.make("codex_work"),
            model: "gpt-5.4",
          },
        })
        .pipe(Effect.flip);
      assert.instanceOf(result, ProviderSwitch.ProviderSwitchPlanError);
      assert.nestedPropertyVal(result, "cause.cause", missingConversationMessage);
    }).pipe(
      Effect.provide(
        layerTest({
          [currentInstanceId]: { continuationKey: "codex:home:/shared/sessions" },
          codex_work: { continuationKey: "codex:home:/shared/sessions" },
        }),
      ),
    ),
);

it.effect("rejects a stopped Codex account switch when its saved cursor record is absent", () =>
  Effect.gen(function* () {
    const service = yield* ProviderSwitch.ProviderSwitchServiceV2;
    const input = projection();
    const result = yield* service
      .plan({
        projection: { ...input, providerSessions: [deadSessionRecord("stopped", "stopped")] },
        targetModelSelection: {
          instanceId: ProviderInstanceId.make("codex_work"),
          model: "gpt-5.4",
        },
      })
      .pipe(Effect.flip);
    assert.instanceOf(result, ProviderSwitch.ProviderSwitchPlanError);
    assert.nestedPropertyVal(result, "cause.cause", missingConversationMessage);
  }).pipe(
    Effect.provide(
      layerTest({
        [currentInstanceId]: { continuationKey: "codex:home:/shared/sessions" },
        codex_work: { continuationKey: "codex:home:/shared/sessions" },
      }),
    ),
  ),
);

it.effect("allows account selection on a fresh thread without provider history", () =>
  Effect.gen(function* () {
    const service = yield* ProviderSwitch.ProviderSwitchServiceV2;
    const input = projection();
    const result = yield* service.plan({
      projection: { ...input, providerSessions: [] },
      targetModelSelection: {
        instanceId: ProviderInstanceId.make("codex_other"),
        model: "gpt-5.4",
      },
    });
    assert.equal(result.transition.type, "create_with_handoff");
    assert.deepEqual(result.releaseProviderSessionIds, []);
  }).pipe(
    Effect.provide(
      layerTest({
        [currentInstanceId]: { continuationKey: "codex:home:/primary" },
        codex_other: { continuationKey: "codex:home:/other" },
      }),
    ),
  ),
);

it.effect("preserves cross-driver handoff for an existing Codex conversation", () =>
  Effect.gen(function* () {
    const service = yield* ProviderSwitch.ProviderSwitchServiceV2;
    const result = yield* service.plan({
      projection: deadNativeThreadProjection("stopped"),
      targetModelSelection: {
        instanceId: ProviderInstanceId.make("claude_target"),
        model: "claude-sonnet",
      },
    });
    assert.equal(result.transition.type, "create_with_handoff");
    assert.deepEqual(result.releaseProviderSessionIds, []);
  }).pipe(
    Effect.provide(
      layerTest({
        [currentInstanceId]: { continuationKey: "codex:home:/primary" },
        claude_target: {
          driver: ProviderDriverKind.make("claude"),
          continuationKey: "claude:account:target",
        },
      }),
    ),
  ),
);
