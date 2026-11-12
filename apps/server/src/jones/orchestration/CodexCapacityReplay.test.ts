import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ProviderDriverKind, type ProviderReplayEntry } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import { materializeReplayTranscriptWorkspace } from "@t3tools/provider-testing/replayTranscript";

import { CodexOrchestratorReplayHarness } from "../../orchestration-v2/Adapters/CodexAdapterV2.testkit.ts";
import { provideDeterministicTestRuntime } from "../../orchestration-v2/testkit/DeterministicRuntime.ts";
import { readReplayFixtureTranscript as readTranscript } from "../../orchestration-v2/testkit/ProviderReplayFixtures.ts";
import { runOrchestratorV2ProviderReplayScenario } from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { queuedTurnInput } from "../../orchestration-v2/testkit/fixtures/queued_turn/input.ts";
import {
  CODEX_MODEL_SELECTION,
  materializeFixtureInput,
} from "../../orchestration-v2/testkit/fixtures/shared.ts";

it.effect("completes a recovered Codex capacity run and dispatches its queued followup", () =>
  Effect.gen(function* () {
    const fixtureName = "codex_capacity_queued_followup";
    const workspace = yield* checkpointWorkspace(fixtureName);
    const recorded = yield* readTranscript(
      new URL(
        "../../orchestration-v2/testkit/fixtures/queued_turn/codex_transcript.ndjson",
        import.meta.url,
      ),
    );
    const startIndex = recorded.entries.findIndex(
      (entry) => entry.type === "expect_outbound" && entry.label === "turn/start",
    );
    const startedIndex = recorded.entries.findIndex(
      (entry) => entry.type === "emit_inbound" && entry.label === "turn/started",
    );
    const completedIndex = recorded.entries.findIndex(
      (entry) => entry.type === "emit_inbound" && entry.label === "turn/completed",
    );
    const start = recorded.entries[startIndex] as Extract<
      ProviderReplayEntry,
      { type: "expect_outbound" }
    >;
    const startFrame = start.frame as { id: number; params: Record<string, unknown> };
    const started = recorded.entries[startedIndex] as Extract<
      ProviderReplayEntry,
      { type: "emit_inbound" }
    >;
    const startedFrame = started.frame as {
      params: { threadId: string; turn: { id: string } };
    };
    const nativeThreadId = startedFrame.params.threadId;
    const originalTurnId = startedFrame.params.turn.id;
    const retryTurnId = "synthetic-capacity-successor";
    const remapTurn = (entry: ProviderReplayEntry): ProviderReplayEntry =>
      JSON.parse(JSON.stringify(entry).replaceAll(originalTurnId, retryTurnId));
    const retryAck = recorded.entries.find(
      (entry) =>
        entry.type === "emit_inbound" &&
        entry.label === "turn/start" &&
        (entry.frame as { id?: number }).id === startFrame.id,
    ) as Extract<ProviderReplayEntry, { type: "emit_inbound" }>;
    const error = {
      message: "Synthetic native capacity failure",
      codexErrorInfo: "serverOverloaded",
      additionalDetails: null,
    };
    const transcript = yield* CodexOrchestratorReplayHarness.decodeTranscript(
      materializeReplayTranscriptWorkspace(
        {
          ...recorded,
          scenario: fixtureName,
          entries: [
            ...recorded.entries.slice(0, startedIndex + 1),
            {
              type: "emit_inbound",
              label: "capacity-failure",
              frame: {
                method: "error",
                params: {
                  threadId: nativeThreadId,
                  turnId: originalTurnId,
                  willRetry: false,
                  error,
                },
              },
            },
            {
              type: "emit_inbound",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: nativeThreadId,
                  turn: { ...startedFrame.params.turn, status: "failed", error },
                },
              },
            },
            {
              ...start,
              frame: {
                ...startFrame,
                id: 4,
                params: { ...startFrame.params, input: [] },
              },
            },
            remapTurn({ ...retryAck, frame: { ...(retryAck.frame as object), id: 4 } }),
            remapTurn(started),
            ...recorded.entries.slice(startedIndex + 1, completedIndex + 1).map(remapTurn),
            ...recorded.entries
              .slice(completedIndex + 1)
              .map((entry) =>
                (entry.type === "expect_outbound" || entry.type === "emit_inbound") &&
                (entry.frame as { id?: number }).id === 4
                  ? { ...entry, frame: { ...(entry.frame as object), id: 5 } }
                  : entry,
              ),
          ],
        },
        workspace,
      ),
    );
    const materialized = yield* materializeFixtureInput({
      scenario: fixtureName,
      fixtureInput: queuedTurnInput(),
      driver: ProviderDriverKind.make("codex"),
      modelSelection: CODEX_MODEL_SELECTION,
    }).pipe(Effect.provide(IdAllocator.layer));
    const threadId = materialized.projectionThreadIds[0]!;
    const ids = yield* IdAllocator.IdAllocatorV2;
    const firstRunId = ids.derive.run({ threadId, ordinal: 1 });
    const secondRunId = ids.derive.run({ threadId, ordinal: 2 });
    const barrier = materialized.steps.findIndex((step) => step.type === "await");
    const result = yield* runOrchestratorV2ProviderReplayScenario(
      {
        name: fixtureName,
        transcript,
        commands: materialized.commands,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { cwd: workspace },
        steps: [
          ...materialized.steps.slice(0, barrier),
          { type: "await_run_status", threadId, runId: secondRunId, status: "queued" },
          { type: "release_replay_gate", label: "capacity-failure" },
          { type: "await_run_turn_item", threadId, runId: firstRunId, itemType: "error" },
          { type: "advance_clock", duration: "5 seconds" },
          ...materialized.steps.slice(barrier),
        ],
      },
      CodexOrchestratorReplayHarness,
    );
    const projection = result.projections.get(threadId)!;
    assert.deepEqual(
      projection.runs.map((run) => [run.ordinal, run.status]),
      [
        [1, "completed"],
        [2, "completed"],
      ],
    );
    assert.isTrue(
      projection.providerTurns.some((turn) => turn.nativeTurnRef?.nativeId === retryTurnId),
    );
    assert.isTrue(
      projection.turnItems.some(
        (item) => item.type === "error" && item.title === "Provider recovered",
      ),
    );
  }).pipe(
    Effect.scoped,
    Effect.provide(IdAllocator.layer),
    provideDeterministicTestRuntime,
    Effect.provide(NodeServices.layer),
  ),
);
