import { assert, it } from "@effect/vitest";
import { CommandId, MessageId, ThreadId, ProjectId, ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Deferred from "effect/Deferred";
import * as FileSystem from "effect/FileSystem";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Replay from "effect-codex-app-server/replay";
import {
  decodeProviderReplayNdjson,
  materializeReplayTranscriptWorkspace,
} from "@t3tools/provider-testing/replayTranscript";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as Codex from "../../orchestration-v2/Adapters/CodexAdapterV2.testkit.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Worker from "../../orchestration-v2/EffectWorker.ts";
import * as Harness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";

it.live(
  "persists a null-bound Codex attempt's interrupted terminal before explicit replacement",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("codex-selection-restart");
        const read = (name: string) =>
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem;
            return JSON.parse(
              JSON.stringify(
                yield* decodeProviderReplayNdjson(
                  yield* fs.readFileString(
                    new URL(
                      "../../orchestration-v2/testkit/fixtures/" +
                        name +
                        "/codex_transcript.ndjson",
                      import.meta.url,
                    ).pathname,
                  ),
                ),
              ),
            );
          }).pipe(Effect.provide(NodeServices.layer));
        const raw = yield* read("turn_interrupt");
        const queued = yield* read("queued_turn");
        const initial = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6-luna" };
        const selection = { ...initial, model: "gpt-6-astra" };
        const firstStart = raw.entries.find(
          (entry: { type: string; frame?: { method?: string } }) =>
            entry.type === "expect_outbound" && entry.frame?.method === "turn/start",
        );
        const firstText = firstStart.frame.params.input[0].text;
        const nativeThread = firstStart.frame.params.threadId;
        const startResult = raw.entries.find(
          (entry: { type: string; frame?: { id?: number } }) =>
            entry.type === "emit_inbound" && entry.frame?.id === 2,
        ).frame.result;
        const secondIndex = queued.entries.findIndex(
          (entry: { type: string; frame?: { id?: number; method?: string } }) =>
            entry.type === "expect_outbound" &&
            entry.frame?.method === "turn/start" &&
            entry.frame.id === 4,
        );
        const secondEntries = JSON.parse(
          JSON.stringify(queued.entries.slice(secondIndex)).replaceAll(
            queued.entries[secondIndex].frame.params.threadId,
            nativeThread,
          ),
        );
        secondEntries[0].frame.id = 6;
        secondEntries[1].frame.id = 6;
        const secondText = secondEntries[0].frame.params.input[0].text;
        for (const [entry, model] of [
          [firstStart, initial.model],
          [secondEntries[0], selection.model],
        ]) {
          entry.frame.params.model = model;
          entry.frame.params.sandboxPolicy = { type: "dangerFullAccess" };
        }
        raw.entries = raw.entries.filter(
          (entry: { type: string }) => entry.type !== "runtime_exit",
        );
        raw.entries.push(
          {
            type: "expect_outbound",
            label: "replacement/resume",
            frame: {
              id: 5,
              method: "thread/resume",
              params: {
                threadId: nativeThread,
                excludeTurns: true,
                config: { "tools.update_plan.enabled": true, mcp_optional_startup_grace_ms: 10000 },
              },
            },
          },
          {
            type: "emit_inbound",
            label: "replacement/resume",
            frame: { id: 5, result: startResult },
          },
          ...secondEntries,
        );
        const transcript = yield* Codex.CodexOrchestratorReplayHarness.decodeTranscript(
          materializeReplayTranscriptWorkspace(raw, cwd),
        );
        const reached = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let beforeReplacement: Effect.Effect<void> = Effect.void;
        const replay = yield* Replay.makeReplayDriver(transcript, {
          beforeEmitInbound: (entry) =>
            entry.label === "account/updated"
              ? Deferred.succeed(reached, undefined).pipe(Effect.andThen(Deferred.await(release)))
              : entry.label === "replacement/resume"
                ? beforeReplacement
                : Effect.void,
        });
        const threadId = ThreadId.make("codex-selection-restart");
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* Worker.OrchestrationEffectWorkerV2;
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("restart:create"),
            threadId,
            projectId: ProjectId.make("test"),
            title: "Codex restart",
            modelSelection: initial,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("restart:first"),
            threadId,
            messageId: MessageId.make("restart:first"),
            text: firstText,
            attachments: [],
            createdBy: "user",
            creationSource: "web",
            dispatchMode: { type: "start_immediately" },
          });
          yield* Deferred.await(reached).pipe(
            Effect.timeout("8 seconds"),
            Effect.catch(() =>
              Effect.gen(function* () {
                const state = yield* Ref.get(replay.state);
                return yield* Effect.die(state.failure ?? "Codex running barrier not reached");
              }),
            ),
          );
          const before = yield* Effect.gen(function* () {
            while (true) {
              const projection = yield* orchestrator.getThreadProjection(threadId);
              if (projection.providerTurns.some((turn) => turn.status === "running"))
                return projection;
              yield* Effect.sleep("10 millis");
            }
          }).pipe(Effect.timeout("5 seconds"));
          const oldAttempt = before.attempts[0]!;
          const oldTurn = before.providerTurns.find((turn) => turn.runAttemptId === oldAttempt.id)!;
          assert.isNull(oldAttempt.providerTurnId);
          assert.isNull(oldAttempt.providerSettlement);
          assert.equal(oldTurn.status, "running");
          let replacementObserved = 0;
          beforeReplacement = Effect.gen(function* () {
            const projection = yield* orchestrator.getThreadProjection(threadId);
            const retired = projection.attempts.find((attempt) => attempt.id === oldAttempt.id)!;
            assert.equal(retired.providerTurnId, oldTurn.id);
            assert.isNull(
              retired.providerSettlement,
              "supersession does not fabricate attempt settlement",
            );
            assert.equal(
              projection.providerTurns.find((turn) => turn.id === oldTurn.id)?.status,
              "interrupted",
            );
            replacementObserved++;
          }).pipe(Effect.orDie);
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("restart:second"),
            threadId,
            messageId: MessageId.make("restart:second"),
            text: secondText,
            attachments: [],
            createdBy: "user",
            creationSource: "web",
            modelSelection: selection,
            dispatchMode: { type: "restart_active", targetRunId: before.runs[0]!.id },
          });
          yield* Deferred.succeed(release, undefined);
          yield* worker.drain().pipe(
            Effect.timeout("8 seconds"),
            Effect.catch(() =>
              Effect.gen(function* () {
                const state = yield* Ref.get(replay.state);
                const projection = yield* orchestrator.getThreadProjection(threadId);
                return yield* Effect.die(
                  state.failure ??
                    JSON.stringify({
                      cursor: state.cursor,
                      next: transcript.entries[state.cursor],
                      attempts: projection.attempts,
                      turns: projection.providerTurns,
                    }),
                );
              }),
            ),
          );
          const final = yield* Effect.gen(function* () {
            while (true) {
              const projection = yield* orchestrator.getThreadProjection(threadId);
              if (
                projection.runs[0]?.status === "completed" &&
                projection.checkpoints.some((checkpoint) => checkpoint.status === "ready")
              )
                return projection;
              const state = yield* Ref.get(replay.state);
              if (state.failure) return yield* Effect.die(state.failure);
              yield* Effect.sleep("10 millis");
            }
          }).pipe(Effect.timeout("8 seconds"));
          const state = yield* Ref.get(replay.state);
          assert.isNull(state.failure, state.failure?.message);
          assert.equal(replacementObserved, 1);
          assert.lengthOf(final.attempts, 2);
          assert.equal(final.attempts[0]?.status, "superseded");
          const successor = final.attempts.find((attempt) => attempt.id !== oldAttempt.id)!;
          assert.equal(successor.status, "completed");
          assert.deepEqual(final.runs[0]?.modelSelection, selection);
          assert.isTrue(
            final.checkpoints.some(
              (checkpoint) => checkpoint.runId === successor.runId && checkpoint.status === "ready",
            ),
          );
          assert.equal(state.cursor, transcript.entries.length);
        }).pipe(
          Effect.provide(
            Harness.layerWithRegistry(
              { name: "codex-selection-restart", runtimePolicyOverride: { cwd } },
              Codex.layer({ transcript, driver: replay }),
            ),
          ),
        );
      }),
    ),
  30_000,
);
