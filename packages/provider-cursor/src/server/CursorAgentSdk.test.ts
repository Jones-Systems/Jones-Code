import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { ProviderSessionId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import { vi } from "vite-plus/test";

import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as CursorAgentSdk from "./CursorAgentSdk.ts";
import * as CursorSdk from "./CursorSdk.ts";

const cursorSdkMock = vi.hoisted(() => {
  const closeExecutionOrder: Array<string> = [];
  const runOperations: Array<string> = [];
  let agentCloseFailure: unknown;
  let staleRunActive = false;
  const runWait = vi.fn(async () => ({
    id: "run-cursor-agent-sdk-test",
    requestId: "request-cursor-agent-sdk-test",
    status: "finished",
    result: "ok",
    model: { id: "default" },
    durationMs: 1,
  }));
  const runCancel = vi.fn(async () => {});
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
      if (staleRunActive) {
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
  const listRuns = vi.fn(async () => {
    runOperations.push("agent.listRuns");
    return {
      items: staleRunActive ? [{ id: "run-stale", status: "running" }] : [],
    };
  });
  const cancelRun = vi.fn(async () => {
    runOperations.push("agent.cancelRun");
    staleRunActive = false;
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
    runWait,
    send,
    setAgentCloseFailure: (failure: unknown) => {
      agentCloseFailure = failure;
    },
    setStaleRunActive: (active: boolean) => {
      staleRunActive = active;
    },
  };
});

vi.mock("./sdk.ts", () => ({
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
  SqliteLocalAgentStore: { open: vi.fn() },
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

describe("CursorAgentSdkRunner", () => {
  it.effect.each([
    { operation: "cancel", sink: "blocked" },
    { operation: "cancel", sink: "failed" },
    { operation: "close", sink: "blocked" },
    { operation: "close", sink: "failed" },
  ] as const)("invokes native $operation before a $sink diagnostic sink", ({ operation, sink }) =>
    Effect.gen(function* () {
      const diagnosticStarted = yield* Deferred.make<void>();
      const releaseDiagnostic = yield* Deferred.make<void>();
      yield* Effect.addFinalizer(() => Deferred.succeed(releaseDiagnostic, undefined));
      cursorSdkMock.setAgentCloseFailure(undefined);
      cursorSdkMock.setStaleRunActive(false);
      cursorSdkMock.agentClose.mockClear();
      cursorSdkMock.runCancel.mockClear();
      const runner = yield* CursorAgentSdk.makeCursorAgentSdkRunner(
        () => (event) =>
          event.payload.type === (operation === "cancel" ? "run.cancel" : "agent.close")
            ? Deferred.succeed(diagnosticStarted, undefined).pipe(
                Effect.andThen(
                  sink === "blocked"
                    ? Deferred.await(releaseDiagnostic)
                    : Effect.die("synthetic diagnostic failure"),
                ),
              )
            : Effect.void,
      );
      const session = yield* runner.open({
        operation: "create",
        options: { model: { id: "default" }, mode: "agent" },
        threadId: ThreadId.make("synthetic-control-thread"),
        providerSessionId: ProviderSessionId.make("synthetic-control-session"),
      });
      const run = yield* session.send({ message: "synthetic prompt" });
      const signal = yield* (operation === "cancel" ? run.cancel : session.close).pipe(
        Effect.exit,
        Effect.forkChild,
      );
      yield* Deferred.await(diagnosticStarted);
      assert.equal(
        operation === "cancel"
          ? cursorSdkMock.runCancel.mock.calls.length
          : cursorSdkMock.agentClose.mock.calls.length,
        1,
      );
      yield* Deferred.succeed(releaseDiagnostic, undefined);
      yield* Fiber.join(signal);
    }).pipe(Effect.provide(Layer.mergeAll(CursorSdk.layer, NodeServices.layer))),
  );

  it.effect("observes native completion independently of blocked completion diagnostics", () =>
    Effect.gen(function* () {
      const diagnosticStarted = yield* Deferred.make<void>();
      const releaseDiagnostic = yield* Deferred.make<void>();
      yield* Effect.addFinalizer(() => Deferred.succeed(releaseDiagnostic, undefined));
      cursorSdkMock.runWait.mockClear();
      cursorSdkMock.setStaleRunActive(false);
      const runner = yield* CursorAgentSdk.makeCursorAgentSdkRunner(
        () => (event) =>
          event.payload.type === "run.completed"
            ? Deferred.succeed(diagnosticStarted, undefined).pipe(
                Effect.andThen(Deferred.await(releaseDiagnostic)),
              )
            : Effect.void,
      );
      const session = yield* runner.open({
        operation: "create",
        options: { model: { id: "default" }, mode: "agent" },
        threadId: ThreadId.make("synthetic-wait-thread"),
        providerSessionId: ProviderSessionId.make("synthetic-wait-session"),
      });
      const run = yield* session.send({ message: "synthetic prompt" });
      const drained = yield* run.wait.pipe(Effect.forkChild);
      yield* Deferred.await(diagnosticStarted);
      assert.equal((yield* run.nativeWait).status, "finished");
      assert.equal((yield* run.nativeWait).status, "finished");
      assert.equal(cursorSdkMock.runWait.mock.calls.length, 1);
      assert.isUndefined(drained.pollUnsafe());
      yield* Deferred.succeed(releaseDiagnostic, undefined);
      yield* Fiber.join(drained);
    }).pipe(Effect.provide(Layer.mergeAll(CursorSdk.layer, NodeServices.layer))),
  );

  it.effect("retries a resumed send after cancelling its abandoned local run", () =>
    Effect.gen(function* () {
      cursorSdkMock.runOperations.length = 0;
      cursorSdkMock.listRuns.mockClear();
      cursorSdkMock.cancelRun.mockClear();
      cursorSdkMock.setStaleRunActive(true);

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
        "agent.cancelRun",
        "run.start",
      ]);
      assert.deepStrictEqual(cursorSdkMock.listRuns.mock.calls[0], [
        "agent-cursor-agent-sdk-test",
        { runtime: "local", cwd: process.cwd(), limit: 1 },
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
});
