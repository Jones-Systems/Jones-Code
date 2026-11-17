import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  OrchestrationV2ProviderThread,
  OrchestratorMcpFailure,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Locks from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as ProviderSessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Sessions from "../../mcp/McpSessionRegistry.ts";
import type { McpThreadInvocationScope } from "../../mcp/McpInvocationContext.ts";
import * as Fixture from "../threadResultDependencies/testkit.ts";
import * as Owner from "./FollowUpCommandOwner.ts";
import * as CheckoutIntent from "../ordinaryCheckout/CheckoutIntent.ts";

const isMcpFailure = Schema.is(OrchestratorMcpFailure);
const layer = Fixture.layer.pipe(Layer.provideMerge(Locks.layer));
const decodeProviderThread = Schema.decodeUnknownSync(OrchestrationV2ProviderThread);

it.effect(
  "holds ordinary follow-up before running a writer callback until full-command outside-SQL preparation is available",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      let callbacks = 0;
      const port = yield* Owner.make(
        () => Effect.die("must not dispatch inside SQL"),
        () => f.receiver.id,
      ).pipe(
        Effect.provide(
          Layer.mock(CheckoutIntent.CheckoutIntent)({
            prepare: () => Effect.die("must not prepare inside SQL"),
          }),
        ),
      );
      const before = yield* f.sink.latestSequence();
      const result = yield* port
        .inParentTransaction(
          f.receiver.id,
          Effect.sync(() => {
            callbacks++;
          }),
        )
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
      assert.equal(callbacks, 0);
      assert.equal(yield* f.sink.latestSequence(), before);
    }).pipe(Effect.provide(layer)),
);
const seedNativeParent = (f: Effect.Success<typeof Fixture.seed>) =>
  Effect.gen(function* () {
    const provider = decodeProviderThread({
      id: "followup-native-thread",
      driver: "codex",
      providerInstanceId: f.receiver.providerInstanceId,
      providerSessionId: "actual-native-session",
      appThreadId: f.receiver.id,
      ownerNodeId: null,
      nativeThreadRef: null,
      nativeConversationHeadRef: null,
      status: "active",
      firstRunOrdinal: 1,
      lastRunOrdinal: 1,
      handoffIds: [],
      forkedFrom: null,
      createdAt: Fixture.time,
      updatedAt: Fixture.time,
    });
    const run = { ...f.receiverRun, providerThreadId: provider.id };
    yield* f.sink.write({
      events: [
        f.event({ type: "provider-thread.updated", threadId: f.receiver.id, payload: provider }),
        f.event({
          type: "thread.metadata-updated",
          threadId: f.receiver.id,
          payload: { ...f.receiver, activeProviderThreadId: provider.id },
        }),
        f.event({ type: "run.updated", threadId: f.receiver.id, runId: run.id, payload: run }),
      ],
    });
    const scope: McpThreadInvocationScope = {
      environmentId: EnvironmentId.make("local"),
      requestNamespace: "captured-credential",
      thread: {
        threadId: f.receiver.id,
        providerInstanceId: f.receiver.providerInstanceId,
        providerSessionId: "captured-credential",
        subagent: false,
      },
      client: undefined,
      capabilities: new Set(["orchestration"]),
      issuedAt: 0,
    };
    return { provider, run, scope };
  });

it.effect(
  "requires the exact parent transaction and rolls local reservation plus dispatch back together",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const port = yield* Owner.make(
        () => f.send("followup-core").pipe(Effect.as({ storedEvents: [] })),
        () => f.receiver.id,
      );
      const command = {
        type: "queue.resume" as const,
        commandId: CommandId.make("followup-core"),
        threadId: f.receiver.id,
      };
      assert.isTrue(Exit.isFailure(yield* port.dispatch(command).pipe(Effect.exit)));
      assert.isTrue(
        Exit.isFailure(
          yield* port.inParentTransaction(f.waiter.id, port.dispatch(command)).pipe(Effect.exit),
        ),
      );
      assert.isUndefined(yield* f.store.get("dependency:followup-core"));
      const before = yield* f.sink.latestSequence();
      const failed = yield* port
        .inParentTransaction(
          f.receiver.id,
          port.dispatch(command).pipe(Effect.andThen(Effect.fail("synthetic reservation failure"))),
        )
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(failed));
      assert.equal(yield* f.sink.latestSequence(), before);
      assert.isUndefined(yield* f.store.get("dependency:followup-core"));
      yield* port.inParentTransaction(f.receiver.id, port.dispatch(command));
      assert.equal((yield* f.store.get("dependency:followup-core"))?.state, "awaiting");
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "rechecks the original credential and exact current parent run inside the owning transaction",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const { scope } = yield* seedNativeParent(f);
      let current: McpThreadInvocationScope | undefined = scope;
      const port = yield* Owner.make(
        () => Effect.succeed({ storedEvents: [] }),
        () => f.receiver.id,
      ).pipe(
        Effect.provide(
          Layer.merge(
            Layer.mock(Sessions.McpSessionRegistry)({
              lookupByCredentialId: () => Effect.sync(() => current),
            }),
            Layer.mock(ProviderSessions.ProviderSessionManagerV2)({
              isMcpCallerAttached: () => Effect.succeed(true),
            }),
          ),
        ),
      );
      yield* port.inParentTransaction(
        f.receiver.id,
        port.assertParentCaller(scope, f.receiverRun.id),
      );
      current = { ...scope, thread: { ...scope.thread, providerSessionId: "replacement-session" } };
      assert.isTrue(
        Exit.isFailure(
          yield* port
            .inParentTransaction(f.receiver.id, port.assertParentCaller(scope, f.receiverRun.id))
            .pipe(Effect.exit),
        ),
      );
      current = scope;
      const newer = f.run("newer-parent-run", f.receiver.id, "running", 2);
      yield* f.sink.write({
        events: [
          f.event({
            type: "run.created",
            threadId: f.receiver.id,
            runId: newer.id,
            payload: newer,
          }),
        ],
      });
      assert.isTrue(
        Exit.isFailure(
          yield* port
            .inParentTransaction(f.receiver.id, port.assertParentCaller(scope, f.receiverRun.id))
            .pipe(Effect.exit),
        ),
      );
      current = undefined;
      assert.isTrue(
        Exit.isFailure(
          yield* port
            .inParentTransaction(f.receiver.id, port.assertParentCaller(scope, newer.id))
            .pipe(Effect.exit),
        ),
      );
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "rejects recursive transaction entry and missing caller authentication before reservation",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const port = yield* Owner.make(
        () => Effect.succeed({ storedEvents: [] }),
        () => f.receiver.id,
      );
      const nested = yield* port
        .inParentTransaction(f.receiver.id, port.inParentTransaction(f.receiver.id, Effect.void))
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(nested));
      const caller = {
        environmentId: EnvironmentId.make("local"),
        requestNamespace: "not-an-authenticated-session",
        thread: {
          threadId: f.receiver.id,
          providerInstanceId: f.receiver.providerInstanceId,
          providerSessionId: "original-session",
        },
      };
      const result = yield* port
        .inParentTransaction(f.receiver.id, port.assertParentCaller(caller, f.receiverRun.id))
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
      assert.equal((yield* f.store.requests(f.receiver.id)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["stream", () => new EventSink.EventSinkStreamError({ afterSequence: 0 })],
  ["write", () => new EventSink.EventSinkWriteError({ eventCount: 1 })],
] as const)(
  "maps EventSink %s failures to the held follow-up failure without committing",
  ([, failure]) =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const port = yield* Owner.make(
        () => Effect.succeed({ storedEvents: [] }),
        () => f.receiver.id,
      );
      const before = yield* f.sink.latestSequence();
      const error = yield* port
        .inParentTransaction(f.receiver.id, Effect.fail(failure()))
        .pipe(Effect.flip);
      assert.equal(isMcpFailure(error) ? error.code : error._tag, "orchestration_error");
      assert.equal(yield* f.sink.latestSequence(), before);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "stale-credential",
  "detached",
  "native-session-replaced",
  "missing-native-session",
  "missing-active-binding",
  "run-provider-mismatch",
  "provider-instance-mismatch",
  "provider-thread-owner-mismatch",
  "new-parent-generation",
])(
  "holds a follow-up before reservation for %s despite a retained authenticated registry entry",
  (scenario) =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const { provider, run, scope } = yield* seedNativeParent(f);
      if (scenario === "missing-active-binding")
        yield* f.sink.write({
          events: [
            f.event({
              type: "thread.metadata-updated",
              threadId: f.receiver.id,
              payload: { ...f.receiver, activeProviderThreadId: null },
            }),
          ],
        });
      if (scenario === "run-provider-mismatch")
        yield* f.sink.write({
          events: [
            f.event({
              type: "run.updated",
              threadId: f.receiver.id,
              runId: run.id,
              payload: {
                ...run,
                providerThreadId: ProviderThreadId.make("another-run-provider-thread"),
              },
            }),
          ],
        });
      if (
        scenario === "native-session-replaced" ||
        scenario === "missing-native-session" ||
        scenario === "provider-instance-mismatch" ||
        scenario === "provider-thread-owner-mismatch"
      )
        yield* f.sink.write({
          events: [
            f.event({
              type: "provider-thread.updated",
              threadId: f.receiver.id,
              payload: {
                ...provider,
                ...(scenario === "native-session-replaced"
                  ? { providerSessionId: ProviderSessionId.make("replacement-native-session") }
                  : {}),
                ...(scenario === "missing-native-session" ? { providerSessionId: null } : {}),
                ...(scenario === "provider-instance-mismatch"
                  ? { providerInstanceId: ProviderInstanceId.make("other-instance") }
                  : {}),
                ...(scenario === "provider-thread-owner-mismatch"
                  ? { appThreadId: ThreadId.make("another-parent-thread") }
                  : {}),
              },
            }),
          ],
        });
      if (scenario === "new-parent-generation") {
        const newer = {
          ...f.run("replacement-parent-generation", f.receiver.id, "running", 2),
          providerThreadId: provider.id,
        };
        yield* f.sink.write({
          events: [
            f.event({
              type: "run.created",
              threadId: f.receiver.id,
              runId: newer.id,
              payload: newer,
            }),
          ],
        });
      }
      let proofCalls = 0;
      const port = yield* Owner.make(
        () => f.send("attachment-denied").pipe(Effect.as({ storedEvents: [] })),
        () => f.receiver.id,
      ).pipe(
        Effect.provide(
          Layer.merge(
            Layer.mock(Sessions.McpSessionRegistry)({
              lookupByCredentialId: () => Effect.succeed(scope),
            }),
            Layer.mock(ProviderSessions.ProviderSessionManagerV2)({
              isMcpCallerAttached: (input) =>
                Effect.gen(function* () {
                  assert.isTrue(
                    Option.isSome(yield* Effect.serviceOption(f.sql.transactionService)),
                  );
                  assert.equal(input.threadId, f.receiver.id);
                  assert.equal(input.providerInstanceId, scope.thread.providerInstanceId);
                  assert.equal(input.mcpCredentialId, "captured-credential");
                  assert.equal(
                    input.providerSessionId,
                    scenario === "native-session-replaced"
                      ? "replacement-native-session"
                      : "actual-native-session",
                  );
                  proofCalls++;
                  // This fake models the external manager's exact attachment association,
                  // while the real owner, lock, projection reads and acceptance store run here.
                  const liveCredential =
                    scenario === "stale-credential"
                      ? "replacement-credential"
                      : "captured-credential";
                  return (
                    scenario !== "detached" &&
                    input.providerSessionId === provider.providerSessionId &&
                    input.mcpCredentialId === liveCredential
                  );
                }),
            }),
          ),
        ),
      );
      const before = yield* f.sink.latestSequence();
      const result = yield* port
        .inParentTransaction(
          f.receiver.id,
          Effect.gen(function* () {
            yield* port.assertParentCaller(scope, run.id);
            yield* port.dispatch({
              type: "queue.resume",
              commandId: CommandId.make("attachment-denied"),
              threadId: f.receiver.id,
            });
          }),
        )
        .pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure.code, "capability_denied");
        if (scenario !== "new-parent-generation")
          assert.include(
            result.failure.message,
            "not attached to this parent run's native session",
          );
      }
      assert.equal(
        proofCalls,
        ["stale-credential", "detached", "native-session-replaced"].includes(scenario) ? 1 : 0,
      );
      assert.equal(yield* f.sink.latestSequence(), before);
      assert.isUndefined(yield* f.store.get("dependency:attachment-denied"));
      assert.equal(
        (yield* f.sql`SELECT 1 FROM orchestration_command_receipts WHERE command_id = 'send:attachment-denied'`)
          .length,
        0,
      );
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["running", "waiting"] as const)(
  "admits the exact attached %s parent without deriving identity from its next selection",
  (status) =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const { provider, run, scope } = yield* seedNativeParent(f);
      const nextInstance = ProviderInstanceId.make("next-turn-instance");
      yield* f.sink.write({
        events: [
          f.event({
            type: "thread.metadata-updated",
            threadId: f.receiver.id,
            payload: {
              ...f.receiver,
              activeProviderThreadId: provider.id,
              providerInstanceId: nextInstance,
              modelSelection: { instanceId: nextInstance, model: "next-turn-model" },
            },
          }),
          f.event({
            type: "run.updated",
            threadId: f.receiver.id,
            runId: run.id,
            payload: { ...run, status },
          }),
        ],
      });
      let proofCalls = 0;
      const port = yield* Owner.make(
        () => f.send("attachment-accepted").pipe(Effect.as({ storedEvents: [] })),
        () => f.receiver.id,
      ).pipe(
        Effect.provide(
          Layer.merge(
            Layer.mock(Sessions.McpSessionRegistry)({
              lookupByCredentialId: () => Effect.succeed(scope),
            }),
            Layer.mock(ProviderSessions.ProviderSessionManagerV2)({
              isMcpCallerAttached: (input) =>
                Effect.gen(function* () {
                  assert.isTrue(
                    Option.isSome(yield* Effect.serviceOption(f.sql.transactionService)),
                  );
                  assert.deepEqual(input, {
                    threadId: f.receiver.id,
                    providerSessionId: provider.providerSessionId,
                    providerInstanceId: scope.thread.providerInstanceId,
                    mcpCredentialId: scope.thread.providerSessionId,
                  });
                  assert.notEqual(input.providerSessionId, input.mcpCredentialId);
                  proofCalls++;
                  return true;
                }),
            }),
          ),
        ),
      );
      yield* port.inParentTransaction(
        f.receiver.id,
        Effect.gen(function* () {
          yield* port.assertParentCaller(scope, run.id);
          yield* port.dispatch({
            type: "queue.resume",
            commandId: CommandId.make("attachment-accepted"),
            threadId: f.receiver.id,
          });
        }),
      );
      assert.equal(proofCalls, 1);
      assert.equal((yield* f.store.get("dependency:attachment-accepted"))?.state, "awaiting");
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "rechecks detachment between the accepted reservation and the SEND eligibility transaction",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const { run, scope } = yield* seedNativeParent(f);
      let attached = true;
      let proofCalls = 0;
      let networkEligible = false;
      const port = yield* Owner.make(
        () => f.send("reserved-followup").pipe(Effect.as({ storedEvents: [] })),
        () => f.receiver.id,
      ).pipe(
        Effect.provide(
          Layer.merge(
            Layer.mock(Sessions.McpSessionRegistry)({
              lookupByCredentialId: () => Effect.succeed(scope),
            }),
            Layer.mock(ProviderSessions.ProviderSessionManagerV2)({
              isMcpCallerAttached: () =>
                Effect.sync(() => {
                  proofCalls++;
                  return attached;
                }),
            }),
          ),
        ),
      );
      yield* port.inParentTransaction(
        f.receiver.id,
        Effect.gen(function* () {
          yield* port.assertParentCaller(scope, run.id);
          yield* port.dispatch({
            type: "queue.resume",
            commandId: CommandId.make("reserved-followup"),
            threadId: f.receiver.id,
          });
        }),
      );
      attached = false;
      const before = yield* f.sink.latestSequence();
      const result = yield* port
        .inParentTransaction(f.receiver.id, port.assertParentCaller(scope, run.id))
        .pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              networkEligible = true;
            }),
          ),
          Effect.result,
        );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.equal(result.failure.code, "capability_denied");
      assert.equal(proofCalls, 2);
      assert.isFalse(networkEligible);
      assert.equal(yield* f.sink.latestSequence(), before);
      assert.equal((yield* f.store.get("dependency:reserved-followup"))?.state, "awaiting");
    }).pipe(Effect.provide(layer)),
);

it.effect("keeps follow-up reservation held when the native attachment owner is unavailable", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const { run, scope } = yield* seedNativeParent(f);
    const port = yield* Owner.make(
      () => f.send("unknown-attachment").pipe(Effect.as({ storedEvents: [] })),
      () => f.receiver.id,
    ).pipe(
      Effect.provide(
        Layer.mock(Sessions.McpSessionRegistry)({
          lookupByCredentialId: () => Effect.succeed(scope),
        }),
      ),
    );
    const result = yield* port
      .inParentTransaction(f.receiver.id, port.assertParentCaller(scope, run.id))
      .pipe(Effect.result);
    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") {
      assert.equal(result.failure.code, "capability_denied");
      assert.include(result.failure.message, "not attached to this parent run's native session");
    }
    assert.isUndefined(yield* f.store.get("dependency:unknown-attachment"));
  }).pipe(Effect.provide(layer)),
);
