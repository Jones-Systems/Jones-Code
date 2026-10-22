import { assert, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  CommandId,
  MessageId,
  ThreadId,
  ProjectId,
  EnvironmentId,
  ProviderInstanceId,
  ProviderDriverKind,
  ThreadConfigurationEvidence,
  ThreadSelfConfigurationResult,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as FileSystem from "effect/FileSystem";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { McpServer, McpSchema } from "effect/ai";
import * as Replay from "effect-codex-app-server/replay";
import {
  decodeProviderReplayNdjson,
  materializeReplayTranscriptWorkspace,
} from "@t3tools/provider-testing/replayTranscript";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as Codex from "../../orchestration-v2/Adapters/CodexAdapterV2.testkit.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Worker from "../../orchestration-v2/EffectWorker.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import * as Harness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Catalog from "../../provider/ProviderRegistry.ts";
import * as ThreadSearch from "../../orchestration-v2/ThreadSearch.ts";
import * as Scheduled from "../../scheduledTasks/ScheduledTaskService.ts";
import * as Peer from "../../peer/PeerForwarding.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import * as Mcp from "../../mcp/McpHttpServer.ts";
import { buildCodexAdditionalContext } from "../../provider/CodexDeveloperInstructions.ts";
import * as Service from "./ThreadSelfConfiguration.ts";

const decodeEvidence = Schema.decodeUnknownEffect(ThreadConfigurationEvidence);
const decodeReceipt = Schema.decodeUnknownEffect(ThreadSelfConfigurationResult);

it.live(
  "registered save completes inside real Codex run; subsequent outbound turn/start carries model, effort and tier",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("codex-self-configure");
        const reached = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const initial = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6-luna" };
        const selection = {
          ...initial,
          model: "gpt-6-astra",
          options: [
            { id: "reasoningEffort", value: "high" },
            { id: "serviceTier", value: "priority" },
          ],
        };
        const fixture = yield* Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          return yield* fs.readFileString(
            new URL(
              "../../orchestration-v2/testkit/fixtures/queued_turn/codex_transcript.ndjson",
              import.meta.url,
            ).pathname,
          );
        }).pipe(Effect.provide(NodeServices.layer));
        const raw = JSON.parse(JSON.stringify(yield* decodeProviderReplayNdjson(fixture)));
        const secondIndex = raw.entries.findIndex(
          (entry: { type: string; frame?: { id?: number; method?: string } }) =>
            entry.type === "expect_outbound" &&
            entry.frame?.method === "turn/start" &&
            entry.frame.id === 4,
        );
        const first = raw.entries.find(
          (entry: { type: string; frame?: { method?: string } }) =>
            entry.type === "expect_outbound" && entry.frame?.method === "turn/start",
        );
        first.frame.params.additionalContext = buildCodexAdditionalContext(
          { model: initial.model, reasoningEffort: "medium" },
          { browser: true, device: false },
        );
        first.frame.params.collaborationMode = {
          mode: "default",
          settings: {
            model: initial.model,
            reasoning_effort: "medium",
            developer_instructions: "<ignored>",
          },
        };
        const second = raw.entries[secondIndex];
        second.frame.id = 5;
        second.frame.params = {
          ...second.frame.params,
          model: selection.model,
          effort: "high",
          serviceTier: "priority",
          collaborationMode: {
            mode: "default",
            settings: {
              model: selection.model,
              reasoning_effort: "high",
              developer_instructions: "<ignored>",
            },
          },
        };
        second.frame.params.additionalContext = buildCodexAdditionalContext(
          { model: selection.model, reasoningEffort: "high" },
          { browser: true, device: false },
        );
        raw.entries[secondIndex + 1].frame.id = 5;
        const startResult = raw.entries.find(
          (entry: { type: string; frame?: { id?: number; result?: unknown } }) =>
            entry.type === "emit_inbound" && entry.frame?.id === 2,
        ).frame.result;
        raw.entries.splice(
          secondIndex,
          0,
          {
            type: "expect_outbound",
            label: "thread/resume",
            frame: {
              id: 4,
              method: "thread/resume",
              params: {
                threadId: second.frame.params.threadId,
                excludeTurns: true,
                config: { "tools.update_plan.enabled": true, mcp_optional_startup_grace_ms: 10000 },
              },
            },
          },
          { type: "emit_inbound", label: "thread/resume", frame: { id: 4, result: startResult } },
        );
        const transcript = yield* Codex.CodexOrchestratorReplayHarness.decodeTranscript(
          materializeReplayTranscriptWorkspace(raw, cwd),
        );
        let held = false;
        const replay = yield* Replay.makeReplayDriver(transcript, {
          beforeEmitInbound: (entry) =>
            !held && entry.label === "item/agentMessage/delta"
              ? Effect.gen(function* () {
                  held = true;
                  yield* Deferred.succeed(reached, undefined);
                  yield* Deferred.await(release);
                })
              : Effect.void,
        });
        const threadId = ThreadId.make("codex-self-configure");
        const scope: Invocation.McpInvocationScope = {
          environmentId: EnvironmentId.make("environment:mcp-test"),
          requestNamespace: "codex-self-configure",
          thread: {
            threadId,
            providerSessionId: "mcp-test:" + threadId,
            providerInstanceId: initial.instanceId,
          },
          client: undefined,
          issuedAt: 0,
          capabilities: new Set(["orchestration"]),
        };
        const runtime = Harness.layerWithRegistryAndServices(
          { name: "codex-self-configure", runtimePolicyOverride: { cwd } },
          Codex.layer({ transcript, driver: replay }),
          { configureMcp: true },
        );
        const catalog: ServerProvider = {
          instanceId: initial.instanceId,
          driver: ProviderDriverKind.make("codex"),
          enabled: true,
          installed: true,
          version: "synthetic",
          status: "ready",
          auth: { status: "authenticated" },
          checkedAt: "2026-10-22T00:00:00.000Z",
          models: [initial, selection].map(({ model }) => ({
            slug: model,
            name: model,
            isCustom: true,
            capabilities: {
              optionDescriptors: [
                {
                  id: "reasoningEffort",
                  label: "Effort",
                  type: "select",
                  options: [{ id: "high", label: "High" }],
                },
                {
                  id: "serviceTier",
                  label: "Tier",
                  type: "select",
                  options: [{ id: "priority", label: "Priority" }],
                },
              ],
            },
          })),
          slashCommands: [],
          skills: [],
        };
        const service = Service.layer.pipe(
          Layer.provide(runtime),
          Layer.provide(
            Layer.mock(Catalog.ProviderRegistry)({ getProviders: Effect.succeed([catalog]) }),
          ),
        );
        const tools = Layer.mergeAll(
          Mcp.layerThreadToolkit,
          Mcp.layerThreadSelfConfigurationToolkit,
        ).pipe(
          Layer.provide(service),
          Layer.provideMerge(McpServer.McpServer.layer),
          Layer.provide(NodeCrypto.layer),
          Layer.provide(runtime),
          Layer.provide(Layer.mock(ThreadSearch.ThreadSearch)({})),
          Layer.provide(Layer.mock(Scheduled.ScheduledTaskService)({})),
          Layer.provide(Layer.mock(Peer.PeerForwarding)({})),
        );
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const sink = yield* Sink.EventSinkV2;
          const worker = yield* Worker.OrchestrationEffectWorkerV2;
          const server = yield* McpServer.McpServer;
          const client = McpSchema.McpServerClient.of({
            clientId: 1,
            protocolVersion: "2025-06-18",
            clientCapabilities: {},
            clientInfo: { name: "test", version: "1" },
            initializePayload: {
              protocolVersion: "2025-06-18",
              capabilities: {},
              clientInfo: { name: "test", version: "1" },
            },
            getClient: Effect.die("unused"),
          });
          const call = (name: string, args: Record<string, unknown> = {}) =>
            server
              .callTool({ name, arguments: args })
              .pipe(
                Effect.provideService(Invocation.McpInvocationContext, scope),
                Effect.provideService(McpSchema.McpServerClient, client),
              );
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("codex:create"),
            threadId,
            projectId: ProjectId.make("test"),
            title: "Codex",
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
            commandId: CommandId.make("codex:first"),
            threadId,
            messageId: MessageId.make("codex:first"),
            text: "Respond with exactly: first fixture turn complete",
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
                return yield* Effect.die(state.failure ?? "Codex barrier not reached");
              }),
            ),
          );
          const before = yield* decodeEvidence(
            (yield* call("t3_thread_configuration")).structuredContent,
          );
          assert.isNotNull(before.executionPrecondition);
          const receipt = yield* decodeReceipt(
            (yield* call("t3_thread_self_configure", {
              clientRequestId: "codex-save",
              modelSelection: selection,
              expectedConfigurationRevision: before.configurationRevision,
              executionPrecondition: before.executionPrecondition,
            })).structuredContent,
          );
          assert.deepEqual(receipt.freshObservation.currentExecution?.requestedSelection, initial);
          assert.notEqual((yield* call("t3_thread_organize", { action: "pin" })).isError, true);
          const done = yield* sink.stream({ threadId }).pipe(
            Stream.filter(
              ({ event }) =>
                event.type === "run.updated" &&
                event.payload.ordinal === 1 &&
                event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(done);
          yield* worker.drain();
          const first = yield* orchestrator.getThreadProjection(threadId);
          assert.lengthOf(first.attempts, 1);
          assert.equal(first.attempts[0]?.id, before.executionPrecondition!.attemptId);
          assert.isTrue(
            first.turnItems.some(
              (item) =>
                item.type === "assistant_message" && item.text === "first fixture turn complete",
            ),
          );
          const next = yield* sink.stream({ threadId }).pipe(
            Stream.filter(
              ({ event }) =>
                event.type === "run.updated" &&
                event.payload.ordinal === 2 &&
                ["completed", "failed"].includes(event.payload.status),
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("codex:second"),
            threadId,
            messageId: MessageId.make("codex:second"),
            text: "Respond with exactly: second fixture turn complete",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
            dispatchMode: { type: "start_immediately" },
          });
          yield* Fiber.join(next);
          yield* worker.drain();
          const replayState = yield* Ref.get(replay.state);
          assert.isNull(replayState.failure, replayState.failure?.message);
          const final = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(final.runs[1]?.status, "completed");
          assert.deepEqual(final.runs[1]?.modelSelection, selection);
          assert.equal(replayState.cursor, transcript.entries.length);
        }).pipe(Effect.provide(Layer.merge(runtime, tools)));
      }),
    ),
  30_000,
);
