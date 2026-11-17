import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as Origin from "@t3tools/provider-core/server/jones/ProviderEventOrigin";
import {
  confirmProviderControlTerminalIngested,
  findProviderControlOutcomeUnknown,
  ProviderControlDeadline,
} from "@t3tools/provider-core/server/jones/providerControlOutcome";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as CursorAgentSdk from "../server/CursorAgentSdk.ts";
import { makeCursorAdapterV2 } from "../server/adapter.ts";

const layer = Layer.mergeAll(NodeServices.layer, IdAllocator.layer, McpProviderSessions.layer);
const identity = {
  productId: "jones-code",
  productName: "Jones Code",
  version: "synthetic-origin",
  source: null,
} as const;

const makeHarness = Effect.fnUntraced(function* (
  options: {
    readonly mode?:
      | "cancel rejection"
      | "silent cancel"
      | "adapter completion"
      | "native failure"
      | "missing witness"
      | "missing handle"
      | "unwitnessed resume";
    readonly persistence?: boolean;
  } = {},
) {
  const workspace = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped({
    prefix: "cursor-origin-",
  });
  const instanceId = ProviderInstanceId.make("cursor-origin");
  const threadId = ThreadId.make("cursor-origin-thread");
  const modelSelection = { instanceId, model: "composer-2.5" };
  const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
    runtimeMode: "full-access",
    interactionMode: "default",
    cwd: workspace,
  });
  const resultGate = yield* Deferred.make<void>();
  const nativeGate = yield* Deferred.make<void>();
  const cancelGate = yield* Deferred.make<void>();
  const closed = yield* Deferred.make<void>();
  const sendEntered = yield* Deferred.make<void>();
  const releaseSend = yield* Deferred.make<void>();
  const terminal =
    yield* Deferred.make<
      Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }>
    >();
  const successorTerminal =
    yield* Deferred.make<
      Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }>
    >();
  const emitted: ProviderAdapter.ProviderAdapterV2Event[] = [];
  const callbacks: NonNullable<CursorAgentSdk.CursorAgentSdkSendInput<never>["onDelta"]>[] = [];
  const messages: CursorAgentSdk.CursorAgentSdkSendInput<never>["message"][] = [];
  const counts = { opens: 0, sends: 0, closes: 0, cancels: 0 };
  const adapter = yield* Effect.gen(function* () {
    const host = yield* ProviderHost.ProviderHost;
    return yield* makeCursorAdapterV2({ instanceId, environment: { HOME: workspace } }).pipe(
      Effect.provideService(ProviderHost.ProviderHost, { ...host, appIdentity: identity }),
      Effect.provideService(CursorAgentSdk.CursorAgentSdkRunner, {
        assertComplete: Effect.void,
        open: () =>
          Effect.sync(() => {
            const agentId = `captured-agent-${++counts.opens}`;
            return {
              agentId,
              listMessages: Effect.succeed([]),
              close: Effect.sync(() => {
                counts.closes++;
              }).pipe(Effect.andThen(Deferred.succeed(closed, undefined))),
              send: <E>(input: CursorAgentSdk.CursorAgentSdkSendInput<E>) =>
                Effect.sync(() => {
                  counts.sends++;
                  messages.push(input.message);
                  // The adapter's callback has no error channel; retain the exact callback for late delivery.
                  if (input.onDelta !== undefined)
                    callbacks.push(input.onDelta as (typeof callbacks)[number]);
                  const result = {
                    id: `captured-run-${counts.sends}`,
                    requestId: "synthetic",
                    status: "finished" as const,
                    model: { id: "composer-2.5" },
                    durationMs: 1,
                  };
                  const run = {
                    agentId,
                    runId: result.id,
                    wait: Deferred.await(resultGate).pipe(Effect.as(result)),
                    nativeWait:
                      options.mode === "native failure"
                        ? Effect.fail(
                            new CursorAgentSdk.CursorAgentSdkRunnerError({
                              method: "run.wait",
                              cause: "synthetic native uncertainty",
                            }),
                          )
                        : Deferred.await(nativeGate).pipe(Effect.as(result)),
                    cancel: Effect.sync(() => {
                      counts.cancels++;
                    }).pipe(
                      Effect.andThen(
                        options.mode === "silent cancel"
                          ? Deferred.await(cancelGate)
                          : options.mode === "cancel rejection"
                            ? Effect.fail(
                                new CursorAgentSdk.CursorAgentSdkRunnerError({
                                  method: "run.cancel",
                                  cause: "synthetic rejection",
                                }),
                              )
                            : Effect.void,
                      ),
                    ),
                  };
                  if (options.mode === "missing witness") {
                    const { nativeWait: _missingWitness, ...withoutWitness } = run;
                    // Deliberately malformed provider result: absence must never become exit proof.
                    // @ts-expect-error nativeWait stays required; this negative fixture omits it to exercise the refusal.
                    return withoutWitness as CursorAgentSdk.CursorAgentSdkRun;
                  }
                  return run;
                }).pipe(
                  Effect.tap(() => Deferred.succeed(sendEntered, undefined)),
                  Effect.tap(() =>
                    options.mode === "missing handle" ? Deferred.await(releaseSend) : Effect.void,
                  ),
                ),
            };
          }),
      }),
    );
  }).pipe(Effect.provide(TestProviderHost.layer({ cwd: workspace })));
  const runtimeScope = yield* Scope.make();
  yield* Effect.addFinalizer(() =>
    Scope.close(runtimeScope, Exit.void).pipe(
      Effect.exit,
      Effect.map((exit) => {
        if (options.mode === undefined) assert.isTrue(Exit.isSuccess(exit));
        else {
          assert.isTrue(Exit.isFailure(exit));
          if (Exit.isFailure(exit)) assert.isDefined(findProviderControlOutcomeUnknown(exit.cause));
        }
      }),
    ),
  );
  yield* Effect.addFinalizer(() =>
    Effect.all(
      [
        Deferred.succeed(resultGate, undefined),
        Deferred.succeed(nativeGate, undefined),
        Deferred.succeed(cancelGate, undefined),
        Deferred.succeed(releaseSend, undefined),
      ],
      { discard: true },
    ),
  );
  const runtime = yield* adapter
    .openSession({
      threadId,
      providerSessionId: ProviderSessionId.make("cursor-origin-session"),
      modelSelection,
      runtimePolicy,
      ...(options.persistence === true
        ? {
            runtimeLifecycle: {
              reserve: () => Effect.succeed("synthetic-generation"),
              admit: () => Effect.void,
              bind: ({ providerThread }) => Effect.succeed(providerThread),
              abandon: () => Effect.void,
              invalidate: () => Effect.void,
            },
          }
        : {}),
    })
    .pipe(Effect.provideService(Scope.Scope, runtimeScope));
  const providerThread = yield* runtime.ensureThread({ threadId, modelSelection, runtimePolicy });
  yield* runtime.events.pipe(
    Stream.runForEach((event) =>
      Effect.gen(function* () {
        emitted.push(event);
        if (event.type === "turn.terminal") {
          yield* Deferred.succeed(
            Deferred.isDoneUnsafe(terminal) ? successorTerminal : terminal,
            event,
          );
        }
      }),
    ),
    Effect.forkScoped,
  );
  const now = yield* DateTime.now;
  const turn: ProviderAdapter.ProviderAdapterV2TurnInput = {
    threadId,
    providerThread,
    modelSelection,
    runtimePolicy,
    runId: RunId.make("cursor-origin-run"),
    runOrdinal: 1,
    providerTurnOrdinal: 1,
    attemptId: RunAttemptId.make("cursor-origin-attempt"),
    rootNodeId: NodeId.make("cursor-origin-root"),
    appThread: {
      id: threadId,
      projectId: ProjectId.make("cursor-origin-project"),
      createdBy: "user",
      creationSource: "web",
      title: "Synthetic origin",
      providerInstanceId: instanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: providerThread.id,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    },
    message: {
      messageId: MessageId.make("cursor-origin-message"),
      text: "Synthetic prompt",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
  };
  const providerTurnId = (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
    driver: CursorAgentSdk.CURSOR_PROVIDER,
    nativeTurnId: "captured-run-1",
  });
  return {
    runtime,
    providerThread,
    turn,
    providerTurnId,
    counts,
    callbacks,
    messages,
    emitted,
    terminal,
    successorTerminal,
    resultGate,
    nativeGate,
    closed,
    sendEntered,
    releaseSend,
  };
});

describe("Cursor adapter origin composition", () => {
  it.effect.each([
    "cancel rejection",
    "silent cancel",
    "adapter completion",
    "native failure",
    "missing witness",
    "missing handle",
  ] as const)("retains unknown retirement and refuses replacement after %s", (mode) =>
    Effect.gen(function* () {
      const h = yield* makeHarness({ mode });
      const starting = yield* h.runtime.startTurn(h.turn).pipe(Effect.forkChild);
      yield* Deferred.await(h.sendEntered);
      if (mode !== "missing handle") yield* Fiber.join(starting);
      const stop = {
        providerThread: h.providerThread,
        providerTurnId: h.providerTurnId,
        requestRuntimeRestart: true,
      };
      const stopping = yield* h.runtime.interruptTurn(stop).pipe(Effect.exit, Effect.forkChild);
      yield* TestClock.adjust("2 seconds");
      yield* Deferred.await(h.closed);
      if (mode === "adapter completion") {
        yield* Deferred.succeed(h.resultGate, undefined);
        yield* Deferred.await(h.terminal);
      }
      yield* TestClock.adjust("8 seconds");
      const exit = yield* Fiber.join(stopping);
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit))
        assert.isDefined(findProviderControlOutcomeUnknown(Cause.squash(exit.cause)));
      yield* Deferred.succeed(h.releaseSend, undefined);
      yield* Fiber.join(starting);
      yield* Deferred.succeed(h.resultGate, undefined);
      const late = yield* Deferred.await(h.terminal);
      assert.equal(late.status, "completed");
      assert.equal(late.evidenceKind, "provider_result");
      assert.equal(late.providerTurnId, h.providerTurnId);
      yield* Deferred.succeed(h.nativeGate, undefined);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(h.runtime.interruptTurn(stop))));
      assert.isTrue(Exit.isFailure(yield* Effect.exit(h.runtime.startTurn(h.turn))));
      assert.equal(h.counts.opens, 1);
      assert.equal(h.counts.sends, 1);
      assert.equal(h.counts.closes, 1);
      assert.equal(h.counts.cancels, mode === "missing handle" ? 0 : 1);
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );

  it.effect("honors an inherited deadline without synthesizing a terminal", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness({ mode: "silent cancel" });
      yield* h.runtime.startTurn(h.turn);
      const deadline = (yield* Clock.currentTimeMillis) + 1_000;
      const stopping = yield* h.runtime
        .interruptTurn({ providerThread: h.providerThread, providerTurnId: h.providerTurnId })
        .pipe(
          Effect.provideService(ProviderControlDeadline, deadline),
          Effect.exit,
          Effect.forkChild,
        );
      yield* TestClock.adjust("1 second");
      const exit = yield* Fiber.join(stopping);
      assert.isTrue(Exit.isFailure(exit));
      assert.isFalse(h.emitted.some((event) => event.type === "turn.terminal"));
      yield* Deferred.await(h.closed);
      assert.equal(h.counts.closes, 1);
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );

  it.effect("refuses a stale turn control without cancelling the current run", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      yield* h.runtime.startTurn(h.turn);
      const exit = yield* Effect.exit(
        h.runtime.interruptTurn({
          providerThread: h.providerThread,
          providerTurnId: ProviderTurnId.make("stale-turn"),
          requestRuntimeRestart: true,
        }),
      );
      assert.isTrue(Exit.isFailure(exit));
      assert.equal(h.counts.cancels, 0);
      assert.equal(h.counts.closes, 0);
      yield* Deferred.succeed(h.resultGate, undefined);
      yield* Deferred.await(h.terminal);
      yield* Deferred.succeed(h.nativeGate, undefined);
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );

  it.effect("does not infer retirement from a resumed conversation with no captured run", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness({ mode: "unwitnessed resume" });
      const historical = {
        ...h.providerThread,
        nativeThreadRef: { ...h.providerThread.nativeThreadRef!, nativeId: "historical-agent" },
      };
      yield* h.runtime.readThreadSnapshot({ providerThread: historical });
      const exit = yield* Effect.exit(
        h.runtime.ensureThread({
          threadId: h.turn.threadId,
          modelSelection: h.turn.modelSelection,
          runtimePolicy: h.turn.runtimePolicy,
        }),
      );
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) assert.isDefined(findProviderControlOutcomeUnknown(exit.cause));
      assert.equal(h.counts.sends, 0);
      assert.equal(h.counts.opens, 2);
      assert.equal(h.counts.closes, 2);
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );

  it.effect(
    "drains persisted terminals before retiring the captured producer and replacing its agent",
    () =>
      Effect.gen(function* () {
        const h = yield* makeHarness({ persistence: true });
        yield* h.runtime.startTurn(h.turn);
        assert.include(String(h.messages[0]), "Jones Code");
        assert.include(String(h.messages[0]), "synthetic-origin");
        yield* Deferred.succeed(h.resultGate, undefined);
        const terminal = yield* Deferred.await(h.terminal);
        const origin = Origin.readProviderEventOrigin(terminal);
        assert.isDefined(origin);
        yield* origin!.producer.revalidateCurrent;
        yield* Deferred.succeed(h.nativeGate, undefined);
        const replacing = yield* h.runtime
          .ensureThread({
            threadId: h.turn.threadId,
            modelSelection: h.turn.modelSelection,
            runtimePolicy: h.turn.runtimePolicy,
          })
          .pipe(Effect.forkChild);
        yield* Deferred.await(h.closed);
        assert.equal(h.counts.opens, 1);
        yield* origin!.producer.revalidateCurrent;
        confirmProviderControlTerminalIngested(terminal, 0);
        const rawTerminal = h.emitted.find(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.completedAt !== null,
        );
        assert.isDefined(rawTerminal);
        confirmProviderControlTerminalIngested(rawTerminal!, 0);
        yield* Effect.yieldNow;
        assert.equal(h.counts.opens, 1, "zero raw writes do not confirm terminal persistence");
        confirmProviderControlTerminalIngested(rawTerminal!, 1);
        const replacement = yield* Fiber.join(replacing);
        assert.equal(h.counts.opens, 2);
        assert.isTrue(Exit.isFailure(yield* Effect.exit(origin!.producer.revalidateCurrent)));
        const oldCallback = h.callbacks[0]!;
        yield* oldCallback({ type: "text-delta", text: "stale callback" });
        yield* h.runtime.startTurn({
          ...h.turn,
          providerThread: replacement,
          runId: RunId.make("successor"),
          attemptId: RunAttemptId.make("successor"),
          runOrdinal: 2,
          providerTurnOrdinal: 2,
        });
        const successor = yield* Deferred.await(h.successorTerminal);
        const successorOrigin = Origin.readProviderEventOrigin(successor);
        assert.isDefined(successorOrigin);
        assert.notStrictEqual(successorOrigin!.producer.token, origin!.producer.token);
        yield* successorOrigin!.producer.revalidateCurrent;
        assert.notInclude(JSON.stringify(h.emitted), "stale callback");
        for (const event of h.emitted) confirmProviderControlTerminalIngested(event, 1);
      }).pipe(Effect.scoped, Effect.provide(layer)),
  );
});
