import { describe, expect, it } from "@effect/vitest";
import { ContextHandoffId, MessageId, NodeId, RunAttemptId, RunId } from "@t3tools/contracts";
import type { OrchestrationV2ContextHandoff } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { deliverContextHandoffs } from "../../orchestration-v2/ContextHandoffDelivery.ts";
import type { ProviderAdapterV2TurnInput } from "../../orchestration-v2/ProviderAdapter.ts";
import { workModeFixture } from "../workMode/Fixtures.testkit.ts";
import { continuationPrompt } from "./continuationPrompt.ts";

function fixture() {
  const projection = workModeFixture();
  const providerThread = projection.providerThreads[0]!;
  const input: ProviderAdapterV2TurnInput = {
    appThread: projection.thread,
    threadId: projection.thread.id,
    runId: RunId.make("continuation-run"),
    runOrdinal: 2,
    providerTurnOrdinal: 2,
    restartContinuationOfRunId: projection.runs[0]!.id,
    attemptId: RunAttemptId.make("continuation-attempt"),
    rootNodeId: NodeId.make("continuation-node"),
    providerThread,
    message: {
      messageId: MessageId.make("continuation-message"),
      text: "Continue the interrupted task.",
      attachments: [],
      createdBy: "system",
      creationSource: "server",
    },
    modelSelection: projection.thread.modelSelection,
    runtimePolicy: { runtimeMode: "full-access", interactionMode: "default", cwd: null },
  };
  const handoff: OrchestrationV2ContextHandoff = {
    id: ContextHandoffId.make("replacement-history"),
    threadId: input.threadId,
    targetRunId: input.runId,
    fromProviderThreadIds: [providerThread.id],
    toProviderThreadId: providerThread.id,
    coveredRunOrdinals: { from: 1, to: 1 },
    strategy: "full_thread_summary",
    status: "ready",
    summaryMessageId: null,
    summaryText: "The user asked for a migration. The first file is already edited.",
    createdByProviderInstanceId: null,
    createdAt: projection.updatedAt,
    updatedAt: projection.updatedAt,
  };
  return { input, handoff };
}

describe("restart continuation prompt delivery", () => {
  it.effect("delivers replacement history inline when native injection is unsupported", () =>
    Effect.gen(function* () {
      const { input, handoff } = fixture();
      let durable = handoff;
      const delivery = yield* deliverContextHandoffs({
        handoffs: [handoff],
        providerThread: input.providerThread,
        budget: 16_000,
        alreadyDeliveredItemIds: new Set<string>(),
        inject: () => Effect.succeed(false),
        persist: (value) =>
          Effect.sync(() => {
            durable = value;
          }),
      });
      const started = continuationPrompt(input, {
        sameNativeThread: false,
        noteContinuation: false,
        context: delivery.context,
        userText: input.message.text,
      });
      expect(started.restartContinuationOfRunId).toBeUndefined();
      expect(started.message.text).toContain(handoff.summaryText);
      expect(started.message.text).toContain(input.message.text);
      expect(durable.delivery?.status).toBe("pending");
      yield* delivery.delivered;
      expect(durable.delivery?.status).toBe("inline");
    }),
  );

  it("prompts a replacement even when its history was injected natively", () => {
    const { input } = fixture();
    const started = continuationPrompt(input, {
      sameNativeThread: false,
      noteContinuation: false,
      context: "",
      userText: input.message.text,
    });
    expect(started.restartContinuationOfRunId).toBeUndefined();
    expect(started.message).toEqual(input.message);
  });

  it("preserves native resume only for the original thread without new prompt context", () => {
    const { input } = fixture();
    const options = {
      sameNativeThread: true,
      noteContinuation: false,
      context: "",
      userText: input.message.text,
    };
    expect(continuationPrompt(input, options).restartContinuationOfRunId).toBe(
      input.restartContinuationOfRunId,
    );
    for (const patch of [{ context: "Lost background task." }, { noteContinuation: true }]) {
      expect(
        continuationPrompt(input, { ...options, ...patch }).restartContinuationOfRunId,
      ).toBeUndefined();
    }
  });
});
