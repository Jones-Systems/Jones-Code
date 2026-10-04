import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  ProviderSetupError,
  type ProviderInstanceConfigMap,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as ProviderAuthFlow from "../provider/ProviderAuthFlow.ts";
import type { ProviderAuthController } from "../provider/Services/ProviderAuthService.ts";
import type { ProviderInstance } from "../provider/ProviderDriver.ts";
import * as ProviderInstanceRegistry from "../provider/Services/ProviderInstanceRegistry.ts";
import {
  ProviderAdapterOpenSessionError,
  ProviderAdapterResumeThreadError,
  withProviderNativeEffect,
  withProviderNativeEffects,
  type ProviderAdapterV2Shape,
  type ProviderAdapterV2SessionRuntime,
  type ProviderNativeOperationContext,
} from "./ProviderAdapter.ts";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
} from "./ProviderAdapterDriver.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";

const driver = ProviderDriverKind.make("codex");
const personalId = ProviderInstanceId.make("codex_personal");
const workId = ProviderInstanceId.make("codex_work");

const makeAdapter = (instanceId: ProviderInstanceId): ProviderAdapterV2Shape =>
  ({
    instanceId,
    driver,
    getCapabilities: () => Effect.die("capabilities are not used by this registry test"),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: () => Effect.die("sessions are not used by this registry test"),
  }) as ProviderAdapterV2Shape;

const makeInstance = (
  instanceId: ProviderInstanceId,
  orchestrationAdapter: ProviderAdapterV2Shape,
): ProviderInstance => ({
  instanceId,
  driverKind: driver,
  continuationIdentity: {
    driverKind: driver,
    continuationKey: `codex:test:${instanceId}`,
  },
  displayName: String(instanceId),
  enabled: true,
  snapshot: {} as ProviderInstance["snapshot"],
  orchestrationAdapter,
  textGeneration: {} as ProviderInstance["textGeneration"],
});

const personalAdapter = makeAdapter(personalId);
const workAdapter = makeAdapter(workId);
const instances = [
  makeInstance(personalId, personalAdapter),
  makeInstance(workId, workAdapter),
] as const;
const instanceRegistryLayer = Layer.succeed(ProviderInstanceRegistry.ProviderInstanceRegistry, {
  getInstance: (instanceId) =>
    Effect.succeed(instances.find((instance) => instance.instanceId === instanceId)),
  listInstances: Effect.succeed(instances),
  listUnavailable: Effect.succeed([]),
  streamChanges: Stream.empty,
  subscribeChanges: Effect.never,
});
const TestLayer = ProviderAdapterRegistry.layerFromProviderInstanceRegistry.pipe(
  Layer.provide(instanceRegistryLayer),
);

it.effect("routes two configured instances of the same driver independently", () =>
  Effect.gen(function* () {
    const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;

    assert.strictEqual(yield* registry.get(personalId), personalAdapter);
    assert.strictEqual(yield* registry.get(workId), workAdapter);
    assert.deepEqual(yield* registry.list(), [personalId, workId]);
  }).pipe(Effect.provide(TestLayer)),
);

it.effect("reads declared delivery without capability probes and preserves missing evidence", () =>
  Effect.gen(function* () {
    const declared = Object.freeze({
      canConsumeHandoffSummaries: true,
      supportsFullThreadHandoff: true,
      supportsProviderSwitchingViaHandoff: true,
    });
    // The adapter's capability and open methods die if review invokes either one.
    const adapter = { ...makeAdapter(personalId), declaredHandoffDelivery: declared };
    const live = yield* Ref.make<ReadonlyArray<ProviderInstance>>([
      makeInstance(personalId, adapter),
    ]);
    const registry = yield* Effect.service(ProviderAdapterRegistry.ProviderAdapterRegistryV2).pipe(
      Effect.provide(
        ProviderAdapterRegistry.layerFromProviderInstanceRegistry.pipe(
          Layer.provide(
            Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
              getInstance: (instanceId) =>
                Ref.get(live).pipe(
                  Effect.map((entries) => entries.find((entry) => entry.instanceId === instanceId)),
                ),
            }),
          ),
        ),
      ),
    );
    const descriptor = yield* registry.getHandoffDeliveryDescriptor!(personalId);
    assert.deepEqual(descriptor, { instanceId: personalId, driver, enabled: true, declared });
    assert.isFalse("continuationKey" in descriptor);

    yield* Ref.set(live, [{ ...makeInstance(personalId, adapter), enabled: false }]);
    assert.isFalse((yield* registry.getHandoffDeliveryDescriptor!(personalId)).enabled);

    yield* Ref.set(live, [makeInstance(personalId, makeAdapter(personalId))]);
    assert.isUndefined((yield* registry.getHandoffDeliveryDescriptor!(personalId)).declared);
  }),
);

it.effect("reads canonical instance rebuilds and removals without a second registry", () =>
  Effect.gen(function* () {
    const live = yield* Ref.make<ReadonlyArray<ProviderInstance>>([instances[0]]);
    const registry = yield* Effect.service(ProviderAdapterRegistry.ProviderAdapterRegistryV2).pipe(
      Effect.provide(
        ProviderAdapterRegistry.layerFromProviderInstanceRegistry.pipe(
          Layer.provide(
            Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
              getInstance: (instanceId) =>
                Ref.get(live).pipe(
                  Effect.map((entries) => entries.find((entry) => entry.instanceId === instanceId)),
                ),
              listInstances: Ref.get(live),
            }),
          ),
        ),
      ),
    );

    assert.strictEqual(yield* registry.get(personalId), personalAdapter);
    const replacement = makeAdapter(personalId);
    yield* Ref.set(live, [makeInstance(personalId, replacement), instances[1]]);
    assert.strictEqual(yield* registry.get(personalId), replacement);
    assert.deepEqual(yield* registry.list(), [personalId, workId]);

    yield* Ref.set(live, [instances[1]]);
    assert.deepEqual(yield* registry.list(), [workId]);
    const removed = yield* registry.get(personalId).pipe(Effect.flip);
    assert.instanceOf(removed, ProviderAdapterRegistry.ProviderAdapterRegistryLookupError);
    assert.strictEqual(yield* registry.get(workId), workAdapter);
  }),
);

const lifecycleDriver = ProviderDriverKind.make("lifecycle-test");
const lifecycleInstanceId = ProviderInstanceId.make("lifecycle-test");
const lifecycleConfigMap: ProviderInstanceConfigMap = {
  [lifecycleInstanceId]: {
    driver: lifecycleDriver,
    config: {},
  },
};
const lifecycleAdapter = makeAdapter(lifecycleInstanceId);

const makeLifecycleDriver = (
  create: Effect.Effect<ProviderAdapterV2Shape, ProviderAdapterDriverCreateError, Scope.Scope>,
): ProviderAdapterDriver<Record<string, never>> => ({
  driverKind: lifecycleDriver,
  configSchema: Schema.Struct({}),
  defaultConfig: () => ({}),
  create: () => create,
});

const trackedCreate = <A, E, R>(
  releases: Ref.Ref<number>,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R | Scope.Scope> =>
  Effect.gen(function* () {
    yield* Effect.addFinalizer(() => Ref.update(releases, (count) => count + 1));
    return yield* effect;
  });

it.effect("closes a partially-created adapter scope immediately on typed failure", () =>
  Effect.gen(function* () {
    const releases = yield* Ref.make(0);
    const createError = new ProviderAdapterDriverCreateError({
      driver: lifecycleDriver,
      instanceId: lifecycleInstanceId,
      detail: "expected test failure",
    });

    yield* Effect.scoped(
      Effect.gen(function* () {
        const exit = yield* ProviderAdapterRegistry.makeRegistryFromConfigMap({
          drivers: [makeLifecycleDriver(trackedCreate(releases, Effect.fail(createError)))],
          configMap: lifecycleConfigMap,
        }).pipe(Effect.exit);

        assert.isTrue(Exit.isFailure(exit));
        assert.strictEqual(yield* Ref.get(releases), 1);
      }),
    );

    assert.strictEqual(yield* Ref.get(releases), 1);
  }),
);

it.effect("closes a partially-created adapter scope immediately on defect", () =>
  Effect.gen(function* () {
    const releases = yield* Ref.make(0);

    yield* Effect.scoped(
      Effect.gen(function* () {
        const exit = yield* ProviderAdapterRegistry.makeRegistryFromConfigMap({
          drivers: [
            makeLifecycleDriver(trackedCreate(releases, Effect.die("expected test defect"))),
          ],
          configMap: lifecycleConfigMap,
        }).pipe(Effect.exit);

        assert.isTrue(Exit.hasDies(exit));
        assert.strictEqual(yield* Ref.get(releases), 1);
      }),
    );

    assert.strictEqual(yield* Ref.get(releases), 1);
  }),
);

it.effect("closes a partially-created adapter scope immediately on interruption", () =>
  Effect.gen(function* () {
    const releases = yield* Ref.make(0);
    const createStarted = yield* Deferred.make<void>();

    yield* Effect.scoped(
      Effect.gen(function* () {
        const fiber = yield* ProviderAdapterRegistry.makeRegistryFromConfigMap({
          drivers: [
            makeLifecycleDriver(
              trackedCreate(
                releases,
                Deferred.succeed(createStarted, undefined).pipe(Effect.andThen(Effect.never)),
              ),
            ),
          ],
          configMap: lifecycleConfigMap,
        }).pipe(Effect.forkChild({ startImmediately: true }));

        yield* Deferred.await(createStarted);
        yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);

        assert.isTrue(Exit.hasInterrupts(exit));
        assert.strictEqual(yield* Ref.get(releases), 1);
      }),
    );

    assert.strictEqual(yield* Ref.get(releases), 1);
  }),
);

it.effect("keeps a successfully-created adapter scope open until normal release", () =>
  Effect.gen(function* () {
    const releases = yield* Ref.make(0);

    yield* Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* ProviderAdapterRegistry.makeRegistryFromConfigMap({
          drivers: [makeLifecycleDriver(trackedCreate(releases, Effect.succeed(lifecycleAdapter)))],
          configMap: lifecycleConfigMap,
        });

        assert.strictEqual(yield* registry.get(lifecycleInstanceId), lifecycleAdapter);
        assert.strictEqual(yield* Ref.get(releases), 0);
      }),
    );

    assert.strictEqual(yield* Ref.get(releases), 1);
  }),
);

it.effect(
  "blocks a new session while another instance changes their shared provider credentials",
  () =>
    Effect.gen(function* () {
      const unused = () => Effect.die("unused auth operation");
      const auth: ProviderAuthController = {
        credentialBinding: { owner: "provider", key: "shared-cli" },
        isChangingCredentials: Effect.succeed(false),
        start: unused,
        complete: unused,
        cancel: unused,
        logout: unused,
        subscribe: () => Stream.empty,
      };
      const related = [
        { ...instances[0], auth },
        { ...instances[1], auth: { ...auth, isChangingCredentials: Effect.succeed(true) } },
      ];
      const registry = yield* Effect.service(
        ProviderAdapterRegistry.ProviderAdapterRegistryV2,
      ).pipe(
        Effect.provide(
          ProviderAdapterRegistry.layerFromProviderInstanceRegistry.pipe(
            Layer.provide(
              Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
                getInstance: (id) =>
                  Effect.succeed(related.find((instance) => instance.instanceId === id)),
                listInstances: Effect.succeed(related),
              }),
            ),
          ),
        ),
      );
      const adapter = yield* registry.get(personalId);
      const error = yield* adapter
        .openSession({
          threadId: ThreadId.make("new-thread"),
          providerSessionId: ProviderSessionId.make("new-session"),
          modelSelection: { instanceId: personalId, model: "test-model" },
          runtimePolicy: {
            runtimeMode: "full-access",
            interactionMode: "default",
            cwd: "/workspace",
          },
        })
        .pipe(Effect.flip);
      assert.instanceOf(error, ProviderAdapterOpenSessionError);
      assert.instanceOf(error.cause, ProviderSetupError);
    }),
);

it.effect("interrupts admitted session startup when a shared peer signs out", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const stopped = yield* Deferred.make<void>();
    const binding = { owner: "provider" as const, key: "shared-cli" };
    const auth = yield* ProviderAuthFlow.make({
      instanceId: personalId,
      credentialBinding: binding,
      methods: Effect.succeed([]),
      authenticate: () => Effect.void,
      logout: Effect.void,
    });
    const peerAuth = yield* ProviderAuthFlow.make({
      instanceId: workId,
      credentialBinding: binding,
      methods: Effect.succeed([]),
      authenticate: () => Effect.void,
      logout: Effect.void,
    });
    const adapter: ProviderAdapterV2Shape = {
      ...workAdapter,
      openSession: () =>
        Effect.gen(function* () {
          yield* Deferred.succeed(entered, undefined);
          return yield* Effect.never;
        }).pipe(Effect.ensuring(Deferred.succeed(stopped, undefined))),
    };
    const related = [
      { ...instances[0], auth },
      { ...instances[1], auth: peerAuth, orchestrationAdapter: adapter },
    ];
    const registry = yield* Effect.service(ProviderAdapterRegistry.ProviderAdapterRegistryV2).pipe(
      Effect.provide(
        ProviderAdapterRegistry.layerFromProviderInstanceRegistry.pipe(
          Layer.provide(
            Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
              getInstance: (id) =>
                Effect.succeed(related.find((instance) => instance.instanceId === id)),
              listInstances: Effect.succeed(related),
            }),
          ),
        ),
      ),
    );
    const guarded = yield* registry.get(workId);
    const startup = yield* guarded
      .openSession({
        threadId: ThreadId.make("shared-startup"),
        providerSessionId: ProviderSessionId.make("shared-session"),
        modelSelection: { instanceId: workId, model: "test-model" },
        runtimePolicy: {
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: "/workspace",
        },
      })
      .pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    yield* auth.logout(Effect.void);
    yield* Deferred.await(stopped);
    assert.isTrue(Exit.isFailure(yield* Fiber.await(startup)));
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("holds missing and stale native effect proof while preserving the typed failure", () =>
  Effect.gen(function* () {
    const providerSessionId = ProviderSessionId.make("resume-session");
    const providerThreadId = ProviderThreadId.make("resume-thread");
    const context: ProviderNativeOperationContext = {
      operationId: "resume-operation",
      operation: "resume_thread",
      instanceId: personalId,
      providerSessionId,
      providerThreadId,
      runtimeGeneration: "current-runtime",
    };
    const cause = new Error("native reply lost");
    const original = new ProviderAdapterResumeThreadError({
      driver,
      providerSessionId,
      providerThreadId,
      cause,
    });
    const held = yield* withProviderNativeEffect(Effect.fail(original), context).pipe(Effect.flip);
    assert.instanceOf(held, ProviderAdapterResumeThreadError);
    assert.strictEqual(held.cause, cause);
    assert.strictEqual(held.message, original.message);
    assert.deepEqual(held.nativeEffect, { ...context, outcome: "unknown" });
    assert.isUndefined(original.nativeEffect);

    const proved = new ProviderAdapterResumeThreadError({
      driver,
      providerSessionId,
      providerThreadId,
      nativeEffect: { ...context, outcome: "known_no_effect" },
    });
    const matched = yield* withProviderNativeEffect(Effect.fail(proved), context).pipe(Effect.flip);
    assert.strictEqual(matched, proved);
    const stale = yield* withProviderNativeEffect(Effect.fail(proved), {
      ...context,
      runtimeGeneration: "replacement-runtime",
    }).pipe(Effect.flip);
    assert.deepEqual(stale.nativeEffect, {
      ...context,
      runtimeGeneration: "replacement-runtime",
      outcome: "unknown",
    });
    assert.strictEqual(proved.nativeEffect?.outcome, "known_no_effect");
  }),
);

it.effect(
  "preserves native incarnation getters and the original receiver through error decoration",
  () =>
    Effect.gen(function* () {
      let generation = "initial-runtime";
      const providerSessionId = ProviderSessionId.make("decorated-session");
      const error = new ProviderAdapterResumeThreadError({
        driver,
        providerSessionId,
        providerThreadId: ProviderThreadId.make("decorated-thread"),
      });
      const unused = () => Effect.die("this capability is not exercised");
      const runtime: ProviderAdapterV2SessionRuntime = {
        instanceId: personalId,
        driver,
        providerSessionId,
        providerSession: {} as ProviderAdapterV2SessionRuntime["providerSession"],
        events: Stream.empty,
        get runtimeGeneration() {
          return generation;
        },
        ensureThread() {
          assert.strictEqual(this, runtime);
          return Effect.fail(error);
        },
        resumeThread: unused,
        startTurn: unused,
        steerTurn: unused,
        interruptTurn: unused,
        respondToRuntimeRequest: unused,
        readThreadSnapshot: unused,
        rollbackThread: unused,
        forkThread: unused,
      };
      const decorated = withProviderNativeEffects(runtime);
      assert.strictEqual(decorated.runtimeGeneration, "initial-runtime");
      generation = "replacement-runtime";
      assert.strictEqual(decorated.runtimeGeneration, "replacement-runtime");
      assert.strictEqual(decorated.events, runtime.events);
      assert.isUndefined(decorated.getGoal);
      assert.isUndefined(decorated.injectHistory);

      const failure = yield* decorated
        .ensureThread({
          threadId: ThreadId.make("decorated-app-thread"),
          modelSelection: { instanceId: personalId, model: "test-model" },
          runtimePolicy: {
            runtimeMode: "full-access",
            interactionMode: "default",
            cwd: "/fixture/workspace",
          },
          nativeOperation: {
            operationId: "decorated-operation",
            operation: "ensure_thread",
            runtimeGeneration: decorated.runtimeGeneration,
          },
        })
        .pipe(Effect.flip);
      assert.instanceOf(failure, ProviderAdapterResumeThreadError);
      if (!Schema.is(ProviderAdapterResumeThreadError)(failure)) {
        return yield* Effect.die("The decorated operation must retain its typed failure.");
      }
      assert.strictEqual(failure.nativeEffect?.outcome, "unknown");
      assert.strictEqual(failure.nativeEffect?.runtimeGeneration, "replacement-runtime");
      assert.isUndefined(error.nativeEffect);
    }),
);
