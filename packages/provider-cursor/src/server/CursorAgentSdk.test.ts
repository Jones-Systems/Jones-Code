import type { InteractionUpdate, ListRunsOptions, RunResult } from "@cursor/sdk";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { ProviderSessionId, ThreadId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import { vi } from "vite-plus/test";

import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as CursorAgentSdk from "./CursorAgentSdk.ts";
import * as CursorSdk from "./CursorSdk.ts";
import { loadCursorSdk, loadCursorSqlite } from "./sdk.ts";

const cursorSdkMock = vi.hoisted(() => {
  const closeExecutionOrder: Array<string> = [];
  const runOperations: Array<string> = [];
  let agentCloseFailure: unknown;
  // The local store lists runs oldest first, like the real SDK.
  let storedRuns: Array<{ readonly id: string; readonly status: string }> = [];
  const runWait = vi.fn(async (): Promise<RunResult> => ({
    id: "run-cursor-agent-sdk-test",
    requestId: "request-cursor-agent-sdk-test",
    status: "finished",
    result: "ok",
    model: { id: "default" },
    durationMs: 1,
  }));
  const runCancel = vi.fn(async () => {});
  const runSteer = vi.fn(async () => "complete_delivered" as const);
  const agentClose = vi.fn(() => {
    closeExecutionOrder.push("sdk:agent.close");
    if (agentCloseFailure !== undefined) {
      throw agentCloseFailure;
    }
  });
  const send = vi.fn(
    async (
      _message: unknown,
      options: { readonly onDelta?: (input: { readonly update: unknown }) => Promise<void> },
    ) => {
      runOperations.push("run.start");
      if (storedRuns.some((run) => run.status === "running")) {
        throw new Error("Agent agent-cursor-agent-sdk-test already has active run");
      }
      await options.onDelta?.({
        update: {
          type: "assistant-message-chunk",
          text: "hello",
        },
      });
      return {
        id: "run-cursor-agent-sdk-test",
        agentId: "agent-cursor-agent-sdk-test",
        wait: runWait,
        cancel: runCancel,
        steer: runSteer as typeof runSteer | undefined,
      };
    },
  );
  const create = vi.fn(async () => ({
    agentId: "agent-cursor-agent-sdk-test",
    send,
    close: agentClose,
  }));
  const resume = vi.fn(async () => ({
    agentId: "agent-cursor-agent-sdk-test",
    send,
    close: agentClose,
  }));
  const listRuns = vi.fn(async (_agentId: string, options?: ListRunsOptions) => {
    runOperations.push("agent.listRuns");
    // Mirrors the SDK's opaque cursor pagination with its default page size of 50.
    const offset = options?.cursor === undefined ? 0 : Number(options.cursor);
    const end = offset + (options?.limit ?? 50);
    return {
      items: storedRuns.slice(offset, end),
      ...(end < storedRuns.length ? { nextCursor: String(end) } : {}),
    };
  });
  const cancelRun = vi.fn(async (runId: string) => {
    runOperations.push("agent.cancelRun");
    storedRuns = storedRuns.map((run) =>
      run.id === runId ? { ...run, status: "cancelled" } : run,
    );
  });

  return {
    agentClose,
    cancelRun,
    closeExecutionOrder,
    create,
    listRuns,
    resume,
    runOperations,
    runCancel,
    runSteer,
    runWait,
    send,
    setAgentCloseFailure: (failure: unknown) => {
      agentCloseFailure = failure;
    },
    setStoredRuns: (runs: Array<{ readonly id: string; readonly status: string }>) => {
      storedRuns = runs;
    },
  };
});

vi.mock("./sdk.ts", () => ({
  loadCursorSdk: () => ({
    Agent: {
      create: cursorSdkMock.create,
      resume: cursorSdkMock.resume,
      listRuns: cursorSdkMock.listRuns,
      cancelRun: cursorSdkMock.cancelRun,
      messages: {
        list: vi.fn(async () => []),
      },
    },
    createAgentPlatform: vi.fn(),
  }),
  loadCursorSqlite: () => ({ SqliteLocalAgentStore: { open: vi.fn() } }),
}));

const layerTest = CursorAgentSdk.layer.pipe(
  Layer.provide(
    Layer.succeed(ProviderEventLoggers.ProviderEventLoggers, {
      native: {
        filePath: "cursor-agent-sdk-test.log",
        write: (event: unknown) =>
          Effect.sync(() => {
            if (
              typeof event === "object" &&
              event !== null &&
              Reflect.get(Reflect.get(event, "event"), "payload")?.type === "agent.close"
            ) {
              cursorSdkMock.closeExecutionOrder.push("protocol:agent.close");
            }
          }),
        close: () => Effect.void,
      },
      canonical: undefined,
    }),
  ),
  Layer.provide(CursorSdk.layer),
  Layer.provide(NodeServices.layer),
);

const nativeWitnessResult: RunResult = {
  id: "run-native-witness",
  requestId: "request-native-witness",
  status: "finished",
  result: "native result",
  model: { id: "default" },
  durationMs: 1,
};

const makeNativeWitnessHarness = Effect.fn("makeNativeWitnessHarness")(function* (
  input: {
    readonly onDelta?: (update: InteractionUpdate) => Effect.Effect<void, Error>;
    readonly log?: CursorAgentSdk.CursorAgentSdkProtocolLogger;
    readonly bufferedUpdate?: boolean;
  } = {},
) {
  const nativeResult = yield* Deferred.make<RunResult, Error>();
  const nativeStarted = yield* Deferred.make<void>();
  const nativeFinished = yield* Deferred.make<void>();
  let resolveNative!: (result: RunResult) => void;
  let rejectNative!: (error: Error) => void;
  const nativePromise = new Promise<RunResult>((resolve, reject) => {
    resolveNative = resolve;
    rejectNative = reject;
  });
  yield* Deferred.await(nativeResult).pipe(
    Effect.match({ onFailure: rejectNative, onSuccess: resolveNative }),
    Effect.ensuring(Deferred.succeed(nativeFinished, undefined)),
    Effect.forkChild,
  );
  const wait = vi.fn(() => {
    Deferred.doneUnsafe(nativeStarted, Effect.void);
    return nativePromise;
  });
  const cancel = vi.fn(async () => {});
  let publish: ((input: { readonly update: unknown }) => Promise<void>) | undefined;
  cursorSdkMock.send.mockImplementationOnce(async (_message, options) => {
    publish = options.onDelta;
    if (input.bufferedUpdate) {
      await publish?.({ update: { type: "assistant-message-chunk", text: "buffered" } });
    }
    return {
      id: nativeWitnessResult.id,
      agentId: "agent-cursor-agent-sdk-test",
      wait,
      cancel,
      steer: cursorSdkMock.runSteer,
    };
  });
  const events: Array<CursorAgentSdk.CursorAgentSdkProtocolLogEvent> = [];
  const runner = yield* CursorAgentSdk.makeCursorAgentSdkRunner(
    () => (event) =>
      Effect.sync(() => events.push(event)).pipe(Effect.andThen(input.log?.(event) ?? Effect.void)),
  ).pipe(
    Effect.provideService(CursorSdk.CursorSdk, {
      load: loadCursorSdk,
      loadSqlite: loadCursorSqlite,
    }),
  );
  const session = yield* runner.open({
    operation: "create",
    options: {},
    threadId: ThreadId.make("thread-native-witness"),
    providerSessionId: ProviderSessionId.make("session-native-witness"),
  });
  yield* Effect.addFinalizer(() =>
    Deferred.succeed(nativeResult, nativeWitnessResult).pipe(
      Effect.andThen(session.close),
      Effect.ignore,
    ),
  );
  return {
    nativeResult,
    nativeStarted,
    nativeFinished,
    wait,
    cancel,
    events,
    session,
    // Forward the callback only when the case supplies one, as exact optional input requires.
    send: session.send({
      message: "synthetic input",
      ...(input.onDelta === undefined ? {} : { onDelta: input.onDelta }),
    }),
    publish: Effect.promise(() =>
      publish!({ update: { type: "assistant-message-chunk", text: "live update" } }),
    ),
  };
});

describe("CursorAgentSdkRunner", () => {
  it.effect.each(["run.started", "buffered callback"] as const)(
    "observes native completion before a blocked %s",
    (blockedStage) =>
      Effect.gen(function* () {
        const blocked = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined));
        const block = Deferred.succeed(blocked, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
        );
        const h = yield* makeNativeWitnessHarness({
          bufferedUpdate: blockedStage === "buffered callback",
          onDelta: () => block,
          log: (event) =>
            blockedStage === "run.started" && event.payload.type === "run.started"
              ? block
              : Effect.void,
        });
        const sending = yield* h.send.pipe(Effect.forkChild);
        yield* Deferred.await(blocked);
        yield* Deferred.await(h.nativeStarted);
        assert.strictEqual(h.wait.mock.calls.length, 1);
        yield* Deferred.succeed(h.nativeResult, nativeWitnessResult);
        yield* Deferred.await(h.nativeFinished);
        assert.isUndefined(sending.pollUnsafe());

        yield* Deferred.succeed(release, undefined);
        const run = yield* Fiber.join(sending);
        assert.isDefined(run.nativeWait);
        assert.strictEqual(yield* run.nativeWait!, nativeWitnessResult);
        assert.strictEqual(yield* run.nativeWait!, nativeWitnessResult);
        assert.strictEqual(yield* run.wait, nativeWitnessResult);
        assert.strictEqual(h.wait.mock.calls.length, 1);
      }).pipe(Effect.scoped),
  );

  it.effect(
    "keeps native completion independent of live callbacks and completion diagnostics",
    () =>
      Effect.gen(function* () {
        const callbackStarted = yield* Deferred.make<void>();
        const callbackRelease = yield* Deferred.make<void>();
        const shellStarted = yield* Deferred.make<void>();
        const shellRelease = yield* Deferred.make<void>();
        const diagnosticStarted = yield* Deferred.make<void>();
        const diagnosticRelease = yield* Deferred.make<void>();
        yield* Effect.addFinalizer(() =>
          Deferred.succeed(callbackRelease, undefined).pipe(
            Effect.andThen(Deferred.succeed(shellRelease, undefined)),
            Effect.andThen(Deferred.succeed(diagnosticRelease, undefined)),
          ),
        );
        let updates = 0;
        const h = yield* makeNativeWitnessHarness({
          onDelta: (update) =>
            Effect.sync(() => {
              updates += 1;
            }).pipe(
              Effect.andThen(
                update.type === "shell-output-delta"
                  ? Deferred.succeed(shellStarted, undefined).pipe(
                      Effect.andThen(Deferred.await(shellRelease)),
                    )
                  : Deferred.succeed(callbackStarted, undefined).pipe(
                      Effect.andThen(Deferred.await(callbackRelease)),
                    ),
              ),
            ),
          log: (event) =>
            event.payload.type === "run.completed"
              ? Deferred.succeed(diagnosticStarted, undefined).pipe(
                  Effect.andThen(Deferred.await(diagnosticRelease)),
                )
              : Effect.void,
        });
        const run = yield* h.send;
        const publishing = yield* h.publish.pipe(Effect.forkChild);
        yield* Deferred.await(callbackStarted);
        const projected = yield* run.wait.pipe(Effect.forkChild);
        yield* Deferred.succeed(h.nativeResult, nativeWitnessResult);
        assert.strictEqual(yield* run.nativeWait!, nativeWitnessResult);
        assert.isUndefined(projected.pollUnsafe());
        assert.isFalse(Deferred.isDoneUnsafe(diagnosticStarted));

        (Reflect.get(globalThis, Symbol.for("@cursor/sdk.shellStreamOutput")) as Function)({
          conversationId: "agent-cursor-agent-sdk-test",
          toolCallId: "shell-during-callback-drain",
          event: { case: "stdout", value: { data: "last native output" } },
        });
        yield* Deferred.succeed(callbackRelease, undefined);
        yield* Fiber.join(publishing);
        yield* Deferred.await(shellStarted);
        assert.isUndefined(projected.pollUnsafe());
        assert.isFalse(Deferred.isDoneUnsafe(diagnosticStarted));
        yield* Deferred.succeed(shellRelease, undefined);
        yield* Deferred.await(diagnosticStarted);
        assert.isUndefined(projected.pollUnsafe());
        assert.strictEqual(yield* run.nativeWait!, nativeWitnessResult);
        yield* Deferred.succeed(diagnosticRelease, undefined);
        assert.strictEqual(yield* Fiber.join(projected), nativeWitnessResult);
        (Reflect.get(globalThis, Symbol.for("@cursor/sdk.shellStreamOutput")) as Function)({
          conversationId: "agent-cursor-agent-sdk-test",
          toolCallId: "shell-after-projected-wait",
          event: { case: "stdout", value: { data: "already unsubscribed" } },
        });
        assert.strictEqual(yield* run.wait, nativeWitnessResult);
        assert.strictEqual(updates, 2);
        assert.strictEqual(h.wait.mock.calls.length, 1);
      }).pipe(Effect.scoped),
  );

  it.effect("retains native success when a later callback rejects", () =>
    Effect.gen(function* () {
      const callbackStarted = yield* Deferred.make<void>();
      const callbackRelease = yield* Deferred.make<void>();
      const failure = new Error("late callback failure");
      yield* Effect.addFinalizer(() => Deferred.succeed(callbackRelease, undefined));
      const h = yield* makeNativeWitnessHarness({
        onDelta: () =>
          Deferred.succeed(callbackStarted, undefined).pipe(
            Effect.andThen(Deferred.await(callbackRelease)),
            Effect.andThen(Effect.fail(failure)),
          ),
      });
      const run = yield* h.send;
      const publishing = yield* h.publish.pipe(Effect.forkChild);
      yield* Deferred.await(callbackStarted);
      const projected = yield* run.wait.pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.succeed(h.nativeResult, nativeWitnessResult);
      assert.strictEqual(yield* run.nativeWait!, nativeWitnessResult);
      assert.isUndefined(projected.pollUnsafe());
      yield* Deferred.succeed(callbackRelease, undefined);
      yield* Fiber.join(publishing);
      const failed = yield* Fiber.join(projected);
      assert(Exit.isFailure(failed));
      assert.deepStrictEqual(
        Cause.squash(failed.cause),
        new CursorAgentSdk.CursorAgentSdkRunnerError({ method: "run.wait", cause: failure }),
      );
      assert.strictEqual(yield* run.nativeWait!, nativeWitnessResult);
      assert.isFalse(h.events.some((event) => event.payload.type === "run.completed"));
      assert.strictEqual(h.wait.mock.calls.length, 1);
    }).pipe(Effect.scoped),
  );

  it.effect.each([false, true] as const)(
    "drains callbacks after late native rejection (callback failure: %s)",
    (callbackFails) =>
      Effect.gen(function* () {
        const callbackStarted = yield* Deferred.make<void>();
        const callbackRelease = yield* Deferred.make<void>();
        const nativeFailure = new Error("late native failure");
        const callbackFailure = new Error("late callback failure");
        yield* Effect.addFinalizer(() => Deferred.succeed(callbackRelease, undefined));
        const h = yield* makeNativeWitnessHarness({
          onDelta: () =>
            Deferred.succeed(callbackStarted, undefined).pipe(
              Effect.andThen(Deferred.await(callbackRelease)),
              Effect.andThen(callbackFails ? Effect.fail(callbackFailure) : Effect.void),
            ),
        });
        const run = yield* h.send;
        const publishing = yield* h.publish.pipe(Effect.forkChild);
        yield* Deferred.await(callbackStarted);
        const projected = yield* run.wait.pipe(Effect.exit, Effect.forkChild);
        yield* Deferred.fail(h.nativeResult, nativeFailure);
        const nativeExit = yield* run.nativeWait!.pipe(Effect.exit);
        assert(Exit.isFailure(nativeExit));
        const nativeError = new CursorAgentSdk.CursorAgentSdkRunnerError({
          method: "run.wait",
          cause: nativeFailure,
        });
        assert.deepStrictEqual(Cause.squash(nativeExit.cause), nativeError);
        assert.isUndefined(projected.pollUnsafe());
        yield* Deferred.succeed(callbackRelease, undefined);
        yield* Fiber.join(publishing);
        const failed = yield* Fiber.join(projected);
        assert(Exit.isFailure(failed));
        const errors = failed.cause.reasons
          .filter(Cause.isFailReason)
          .map((reason) => reason.error);
        assert.deepStrictEqual(
          errors,
          callbackFails
            ? [
                nativeError,
                new CursorAgentSdk.CursorAgentSdkRunnerError({
                  method: "run.wait",
                  cause: callbackFailure,
                }),
              ]
            : [nativeError],
        );
        assert.deepStrictEqual(yield* run.nativeWait!.pipe(Effect.exit), nativeExit);
        assert.isFalse(h.events.some((event) => event.payload.type === "run.completed"));
        assert.strictEqual(h.wait.mock.calls.length, 1);
      }).pipe(Effect.scoped),
  );

  it.effect(
    "shares one pending native wait across readers without treating cancel or close as exit",
    () =>
      Effect.gen(function* () {
        const h = yield* makeNativeWitnessHarness();
        const run = yield* h.send;
        yield* Deferred.await(h.nativeStarted);
        const first = yield* run.nativeWait!.pipe(Effect.forkChild);
        const second = yield* run.nativeWait!.pipe(Effect.forkChild);
        const abandoned = yield* run.nativeWait!.pipe(Effect.forkChild);
        yield* Fiber.interrupt(abandoned);
        const projected = yield* run.wait.pipe(Effect.forkChild);
        yield* run.cancel;
        yield* h.session.close;
        assert.isUndefined(first.pollUnsafe());
        assert.isUndefined(second.pollUnsafe());
        assert.isUndefined(projected.pollUnsafe());
        assert.isFalse(Deferred.isDoneUnsafe(h.nativeFinished));
        assert.strictEqual(h.cancel.mock.calls.length, 1);
        assert.strictEqual(h.wait.mock.calls.length, 1);
        yield* Deferred.succeed(h.nativeResult, nativeWitnessResult);
        assert.strictEqual(yield* Fiber.join(first), nativeWitnessResult);
        assert.strictEqual(yield* Fiber.join(second), nativeWitnessResult);
        assert.strictEqual(yield* Fiber.join(projected), nativeWitnessResult);
        assert.strictEqual(yield* run.nativeWait!, nativeWitnessResult);
        assert.strictEqual(yield* run.wait, nativeWitnessResult);
        assert.strictEqual(h.wait.mock.calls.length, 1);
      }).pipe(Effect.scoped),
  );

  it.effect("resumes from the original store after the working directory changes", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const workspace = yield* fileSystem.makeTempDirectoryScoped({ prefix: "cursor-resume-" });
      const loaders = yield* Effect.promise(() =>
        vi.importActual<typeof import("./sdk.ts")>("./sdk.ts"),
      );
      const sdk = loaders.loadCursorSdk();
      const store = new sdk.JsonlLocalAgentStore(workspace);
      const storeCwd = `${workspace}/checkout`;
      const cwd = `${workspace}/worktree`;
      const platform = yield* Effect.promise(() =>
        sdk.createAgentPlatform({ localStore: store, workspaceRef: storeCwd }),
      );
      const { agent, run } = yield* Effect.promise(() =>
        platform.store.createAgent({ agentId: "agent-moved", workspaceRef: storeCwd }),
      );
      const runner = yield* CursorAgentSdk.makeCursorAgentSdkRunner(() => undefined).pipe(
        Effect.provideService(CursorSdk.CursorSdk, {
          load: loaders.loadCursorSdk,
          loadSqlite: loaders.loadCursorSqlite,
        }),
      );
      const session = yield* runner.open({
        operation: "resume",
        agentId: agent.agentId,
        storeCwd,
        options: { apiKey: "synthetic-cursor-key", local: { cwd, store } },
        threadId: ThreadId.make("moved-workspace"),
        providerSessionId: ProviderSessionId.make("moved-workspace"),
      });
      assert.equal(session.agentId, agent.agentId);
      assert.deepStrictEqual(yield* session.listMessages, []);
      assert.strictEqual(yield* session.readContextUsage!, undefined);

      // Cursor's conversation holds a run's request only once that run saved its own checkpoint.
      assert.isFalse(yield* session.runCheckpointed!());
      yield* Effect.promise(() =>
        platform.store.patchCheckpoint(agent.agentId, run.runId, {
          blobId: "checkpoint-1",
          storeKind: "local",
        }),
      );
      assert.isTrue(yield* session.runCheckpointed!());
      yield* Effect.promise(() =>
        platform.store.markRunTerminal(agent.agentId, run.runId, { status: "CANCELLED" }),
      );
      const followUp = yield* Effect.promise(() =>
        platform.store.createFollowUpRun(agent.agentId, {}),
      );
      assert.isFalse(yield* session.runCheckpointed!(), "the follow-up has no checkpoint yet");
      assert.isTrue(yield* session.runCheckpointed!(run.runId));
      assert.isFalse(yield* session.runCheckpointed!(followUp.runId));
      yield* session.close;
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("constructs without loading and reports first-use load failures as runner errors", () =>
    Effect.gen(function* () {
      const failure = new Error("missing SDK chunk");
      const load = vi.fn(() => {
        throw failure;
      });
      const runner = yield* CursorAgentSdk.makeCursorAgentSdkRunner(() => undefined).pipe(
        Effect.provideService(CursorSdk.CursorSdk, {
          load,
          loadSqlite: () => {
            throw new Error("unused SQLite");
          },
        }),
      );
      assert.strictEqual(load.mock.calls.length, 0);
      const result = yield* runner
        .open({
          operation: "create",
          options: {},
          threadId: ThreadId.make("lazy-load"),
          providerSessionId: ProviderSessionId.make("lazy-load"),
        })
        .pipe(Effect.exit);
      assert(Exit.isFailure(result));
      assert.deepStrictEqual(
        Cause.squash(result.cause),
        new CursorAgentSdk.CursorAgentSdkRunnerError({
          method: "agent.create",
          cause: failure,
        }),
      );
      assert.strictEqual(load.mock.calls.length, 1);
    }),
  );

  it.effect("retries a resumed send after cancelling its abandoned local run", () =>
    Effect.gen(function* () {
      cursorSdkMock.runOperations.length = 0;
      cursorSdkMock.listRuns.mockClear();
      cursorSdkMock.cancelRun.mockClear();
      cursorSdkMock.setStoredRuns([
        ...Array.from({ length: 50 }, (_, index) => ({
          id: `run-terminal-${index}`,
          status: index === 0 ? "cancelled" : "finished",
        })),
        { id: "run-stale", status: "running" },
      ]);

      const runner = yield* CursorAgentSdk.CursorAgentSdkRunner;
      const session = yield* runner.open({
        operation: "resume",
        agentId: "agent-cursor-agent-sdk-test",
        options: { model: { id: "default" }, mode: "agent", local: { cwd: process.cwd() } },
        threadId: ThreadId.make("thread-cursor-agent-sdk-stale-run-test"),
        providerSessionId: ProviderSessionId.make(
          "provider-session-cursor-agent-sdk-stale-run-test",
        ),
      });
      assert.deepStrictEqual(cursorSdkMock.runOperations, []);
      const run = yield* session.send({ message: "continue" });
      yield* run.wait;

      assert.deepStrictEqual(cursorSdkMock.runOperations, [
        "run.start",
        "agent.listRuns",
        "agent.listRuns",
        "agent.cancelRun",
        "run.start",
      ]);
      assert.deepStrictEqual(cursorSdkMock.listRuns.mock.calls[0], [
        "agent-cursor-agent-sdk-test",
        { runtime: "local", cwd: process.cwd() },
      ]);
      assert.strictEqual(cursorSdkMock.listRuns.mock.calls.length, 2);
      assert.deepStrictEqual(cursorSdkMock.listRuns.mock.calls[1], [
        "agent-cursor-agent-sdk-test",
        { runtime: "local", cwd: process.cwd(), cursor: "50" },
      ]);
      assert.deepStrictEqual(cursorSdkMock.cancelRun.mock.calls[0], [
        "run-stale",
        {
          runtime: "local",
          cwd: process.cwd(),
        },
      ]);
    }).pipe(Effect.provide(layerTest)),
  );

  it.effect("forwards the agent's own shell stream output as shell-output-delta", () =>
    Effect.gen(function* () {
      const publish = (conversationId: string, data: string) =>
        (Reflect.get(globalThis, Symbol.for("@cursor/sdk.shellStreamOutput")) as Function)({
          conversationId,
          toolCallId: "tool-shell",
          event: { case: "stdout", value: { data } },
        });
      cursorSdkMock.runWait.mockImplementationOnce(async () => {
        publish("agent-cursor-agent-sdk-test", "tick 1\n");
        publish("agent-someone-else", "not ours\n");
        return {
          id: "run-cursor-agent-sdk-test",
          requestId: "request-cursor-agent-sdk-test",
          status: "finished",
          result: "ok",
          model: { id: "default" },
          durationMs: 1,
        };
      });
      const updates: Array<unknown> = [];
      const runner = yield* CursorAgentSdk.CursorAgentSdkRunner;
      const session = yield* runner.open({
        operation: "create",
        options: {},
        threadId: ThreadId.make("thread-cursor-agent-sdk-shell"),
        providerSessionId: ProviderSessionId.make("provider-session-cursor-agent-sdk-shell"),
      });
      const run = yield* session.send({
        message: "run it",
        onDelta: (update) => Effect.sync(() => updates.push(update)),
      });
      yield* run.wait;
      publish("agent-cursor-agent-sdk-test", "after the run\n");

      assert.deepStrictEqual(
        updates.filter(
          (update) => (update as { readonly type: string }).type === "shell-output-delta",
        ),
        [
          {
            type: "shell-output-delta",
            event: { case: "stdout", value: { data: "tick 1\n" }, toolCallId: "tool-shell" },
          },
        ],
      );
    }).pipe(Effect.provide(layerTest)),
  );

  it.effect("forwards Run.steer and reports a missing method as a follow-up", () =>
    Effect.gen(function* () {
      cursorSdkMock.runSteer.mockClear();
      const runner = yield* CursorAgentSdk.CursorAgentSdkRunner;
      const session = yield* runner.open({
        operation: "create",
        options: {},
        threadId: ThreadId.make("thread-cursor-agent-sdk-steer"),
        providerSessionId: ProviderSessionId.make("provider-session-cursor-agent-sdk-steer"),
      });
      const run = yield* session.send({ message: "continue" });
      assert.equal(yield* run.steer!("keep going"), "complete_delivered");
      assert.deepStrictEqual(cursorSdkMock.runSteer.mock.calls, [["keep going"]]);

      cursorSdkMock.runSteer.mockImplementationOnce(async () => {
        throw new Error("steer transport failed");
      });
      const failed = yield* run.steer!("again").pipe(Effect.exit);
      assert(Exit.isFailure(failed));

      cursorSdkMock.send.mockImplementationOnce(async () => ({
        id: "run-without-steer",
        agentId: "agent-cursor-agent-sdk-test",
        wait: cursorSdkMock.runWait,
        cancel: cursorSdkMock.runCancel,
        steer: undefined,
      }));
      const plain = yield* session.send({ message: "plain" });
      assert.equal(yield* plain.steer!("later"), "revert_to_followup");
    }).pipe(Effect.provide(layerTest)),
  );
});
