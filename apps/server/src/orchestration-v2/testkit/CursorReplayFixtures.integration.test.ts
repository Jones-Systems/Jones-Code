import { assert, describe, it } from "@effect/vitest";
import {
  ProviderSessionId,
  ThreadId,
  type ProviderReplayEntry,
  type ProviderReplayTranscript,
} from "@t3tools/contracts";
import * as CursorAgentSdk from "@t3tools/provider-cursor/server/CursorAgentSdk";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";

import {
  CursorOrchestratorReplayHarness,
  CursorReplayFrameMismatchError,
  makeCursorAgentSdkReplayRunner,
  type CursorAgentSdkReplayTranscript,
} from "../Adapters/CursorAdapterV2.testkit.ts";
import { messageRestartInput } from "./fixtures/message_steering/input.ts";
import {
  findReplayFixture,
  itRunsReplayFixtures,
  runReplayFixture,
} from "./ProviderReplayFixtures.ts";

type CursorReplayFrame = CursorAgentSdk.CursorAgentSdkProtocolLogEvent["payload"];

function withCursorRetirementBoundaries(
  transcript: ProviderReplayTranscript,
): ProviderReplayTranscript {
  let open: Extract<CursorReplayFrame, { readonly type: "agent.open" }> | undefined;
  let agentId: string | undefined;
  let cancelledRunId: string | undefined;
  return {
    ...transcript,
    entries: transcript.entries.flatMap<ProviderReplayEntry>((entry, index) => {
      if (entry.type === "runtime_exit") return [entry];
      const frame = entry.frame as CursorReplayFrame;
      if (entry.type === "expect_outbound" && frame.type === "agent.open") open = frame;
      if (entry.type === "emit_inbound" && frame.type === "agent.opened") agentId = frame.agentId;
      if (entry.type === "expect_outbound" && frame.type === "run.cancel") {
        cancelledRunId = frame.runId;
      }
      const next = transcript.entries[index + 1];
      if (
        entry.type !== "emit_inbound" ||
        frame.type !== "run.completed" ||
        frame.result.status !== "cancelled" ||
        next?.type !== "expect_outbound" ||
        (next.frame as CursorReplayFrame).type !== "run.start"
      ) {
        return [entry];
      }
      assert.equal(frame.result.id, cancelledRunId);
      assert.isDefined(open);
      assert.isDefined(agentId);
      // Older recordings reused the agent after cancel. Require the captured close/resume
      // boundary now, retaining every recorded frame and its native completion witness.
      return [
        entry,
        {
          type: "expect_outbound",
          label: "retirement.agent.close",
          frame: { type: "agent.close", agentId },
        },
        {
          type: "expect_outbound",
          label: "retirement.agent.resume",
          frame: { type: "agent.open", operation: "resume", agentId, options: open.options },
        },
        {
          type: "emit_inbound",
          label: "retirement.agent.resumed",
          frame: { type: "agent.opened", agentId },
        },
      ];
    }),
  };
}

function cancellationTranscript(): CursorAgentSdkReplayTranscript {
  return {
    provider: CursorAgentSdk.CURSOR_PROVIDER,
    protocol: CursorAgentSdk.CURSOR_AGENT_SDK_PROTOCOL,
    version: "1",
    scenario: "cancelled-wait-caller",
    entries: [
      { type: "expect_outbound", frame: { type: "agent.open", operation: "create", options: {} } },
      { type: "emit_inbound", frame: { type: "agent.opened", agentId: "agent-replay" } },
      { type: "expect_outbound", frame: { type: "run.start", message: "work", options: {} } },
      {
        type: "emit_inbound",
        frame: { type: "run.started", agentId: "agent-replay", runId: "run-replay" },
      },
      {
        type: "emit_inbound",
        frame: {
          type: "interaction.update",
          runId: "run-replay",
          update: { type: "thinking-delta", text: "working" },
        },
      },
      { type: "expect_outbound", frame: { type: "run.cancel", runId: "run-replay" } },
      {
        type: "emit_inbound",
        frame: {
          type: "run.completed",
          result: {
            id: "run-replay",
            requestId: "request-replay",
            status: "cancelled",
            model: { id: "composer-2.5" },
            durationMs: 1,
          },
        },
      },
      { type: "expect_outbound", frame: { type: "agent.close", agentId: "agent-replay" } },
    ],
  };
}

describe("Cursor replay fixtures", () => {
  itRunsReplayFixtures("cursor", (input) =>
    runReplayFixture({
      ...input,
      harness: CursorOrchestratorReplayHarness,
      transformTranscript: withCursorRetirementBoundaries,
    }),
  );

  it.effect("keeps the native replay reader alive after its wait caller leaves", () =>
    Effect.gen(function* () {
      const callbackStarted = yield* Deferred.make<void>();
      const releaseCallback = yield* Deferred.make<void>();
      const closeCompleted = yield* Deferred.make<void>();
      yield* Effect.addFinalizer(() => Deferred.succeed(releaseCallback, undefined));
      const callbacks: Array<string> = [];
      const runner = makeCursorAgentSdkReplayRunner(cancellationTranscript());
      const session = yield* runner.open({
        operation: "create",
        options: {},
        threadId: ThreadId.make("thread-replay"),
        providerSessionId: ProviderSessionId.make("session-replay"),
      });
      const run = yield* session.send({
        message: "work",
        onDelta: () =>
          Effect.gen(function* () {
            callbacks.push("started");
            yield* Deferred.succeed(callbackStarted, undefined);
            yield* Deferred.await(releaseCallback);
            callbacks.push("delivered");
          }),
      });
      const caller = yield* run.wait.pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(callbackStarted);
      yield* Fiber.interrupt(caller);
      yield* run.cancel;
      const closing = yield* session.close.pipe(
        Effect.andThen(Deferred.succeed(closeCompleted, undefined)),
        Effect.forkScoped({ startImmediately: true }),
      );
      assert.isFalse(Deferred.isDoneUnsafe(closeCompleted));
      assert.deepEqual(callbacks, ["started"]);
      yield* Deferred.succeed(releaseCallback, undefined);
      yield* Fiber.join(closing);
      const result = yield* run.nativeWait;
      assert.equal(result.id, "run-replay");
      assert.equal(result.status, "cancelled");
      assert.strictEqual(yield* run.wait, result);
      assert.deepEqual(callbacks, ["started", "delivered"]);
      yield* runner.assertComplete;
    }).pipe(Effect.scoped),
  );

  it.effect("rejects close before the recorded cancel and retains the native wait failure", () =>
    Effect.gen(function* () {
      const runner = makeCursorAgentSdkReplayRunner(cancellationTranscript());
      const session = yield* runner.open({
        operation: "create",
        options: {},
        threadId: ThreadId.make("thread-replay"),
        providerSessionId: ProviderSessionId.make("session-replay"),
      });
      const run = yield* session.send({ message: "work" });
      const error = yield* session.close.pipe(Effect.flip);
      assert.instanceOf(error.cause, CursorReplayFrameMismatchError);
      assert.deepEqual(error.cause.expected, { type: "run.cancel", runId: "run-replay" });
      assert.deepEqual(error.cause.actual, { type: "agent.close", agentId: "agent-replay" });
      const nativeFailure = yield* run.nativeWait.pipe(Effect.flip);
      assert.strictEqual(nativeFailure.cause, error.cause);
      const incomplete = yield* runner.assertComplete.pipe(Effect.flip);
      assert.strictEqual(incomplete.cause, error.cause);
    }),
  );

  const cursorSteeringProvider = findReplayFixture("message_steering")?.providers.find(
    (provider) => provider.driver === "cursor",
  );
  if (cursorSteeringProvider !== undefined) {
    it.effect("executes explicit Cursor restart_active through the recorded SDK boundary", () =>
      runReplayFixture({
        fixtureName: "message_steering",
        buildInput: messageRestartInput,
        driver: cursorSteeringProvider,
        harness: CursorOrchestratorReplayHarness,
        transformTranscript: withCursorRetirementBoundaries,
      }),
    );
  }
});
