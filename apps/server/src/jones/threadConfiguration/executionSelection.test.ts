import { assert, describe, it } from "@effect/vitest";
import {
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  RunAttemptId,
  ThreadId,
  type OrchestrationV2Run,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type ModelSelection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import { executionProjection, selectionAheadOfQueuedPicks } from "./executionSelection.ts";

const inEffect: ModelSelection = { instanceId: ProviderInstanceId.make("claude"), model: "opus" };
const picked: ModelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.5" };
const queuedAt = DateTime.makeUnsafe("2026-11-10T10:00:00Z");
const later = DateTime.makeUnsafe("2026-11-10T10:00:01Z");

describe("selectionAheadOfQueuedPicks", () => {
  it("takes the selection in effect when the saved default is a queued pick", () => {
    assert.deepEqual(
      selectionAheadOfQueuedPicks({ modelSelection: picked, updatedAt: queuedAt }, inEffect, [
        { modelSelection: picked, requestedAt: queuedAt },
      ]),
      inEffect,
    );
  });

  it("keeps a default saved after the pick was queued", () => {
    assert.deepEqual(
      selectionAheadOfQueuedPicks({ modelSelection: picked, updatedAt: later }, inEffect, [
        { modelSelection: picked, requestedAt: queuedAt },
      ]),
      picked,
    );
  });

  it("keeps the saved default when no queued pick carries it", () => {
    assert.deepEqual(
      selectionAheadOfQueuedPicks({ modelSelection: picked, updatedAt: queuedAt }, inEffect, [
        { modelSelection: inEffect, requestedAt: queuedAt },
      ]),
      picked,
    );
    assert.deepEqual(
      selectionAheadOfQueuedPicks({ modelSelection: picked, updatedAt: queuedAt }, inEffect, []),
      picked,
    );
  });
});

it("uses the captured starting run ahead of saved settings and earlier execution", () => {
  const providerThreadId = ProviderThreadId.make("starting-native-owner");
  const captured = {
    id: RunId.make("starting"),
    activeAttemptId: RunAttemptId.make("starting-attempt"),
    ordinal: 2,
    providerThreadId,
    modelSelection: inEffect,
    providerInstanceId: inEffect.instanceId,
    status: "starting",
    startedAt: null,
  } as unknown as OrchestrationV2Run;
  const prior = {
    ...captured,
    id: RunId.make("prior"),
    ordinal: 1,
    status: "completed",
    startedAt: queuedAt,
  } as OrchestrationV2Run;
  const projection = {
    thread: {
      id: ThreadId.make("frozen-selection"),
      modelSelection: picked,
      providerInstanceId: picked.instanceId,
      activeProviderThreadId: providerThreadId,
    } as OrchestrationV2AppThread,
    providerThreads: [
      {
        id: providerThreadId,
        providerInstanceId: inEffect.instanceId,
      } as OrchestrationV2ProviderThread,
    ],
    runs: [prior, captured],
  };
  assert.deepEqual(executionProjection(projection).thread.modelSelection, inEffect);
  assert.deepEqual(
    executionProjection({
      ...projection,
      runs: [prior, { ...captured, status: "queued", modelSelection: picked }],
    }).thread.modelSelection,
    inEffect,
  );
  assert.deepEqual(projection.thread.modelSelection, picked);
});
