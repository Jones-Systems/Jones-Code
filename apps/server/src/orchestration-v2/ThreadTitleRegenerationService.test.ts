import { assert, describe, it, vi } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  ChatAttachmentId,
  type ChatAttachment,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  TextGenerationError,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import { ATTACHMENT_ONLY_BOOTSTRAP_PROMPT } from "@t3tools/shared/composerContextReferences";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as TitleStore from "../jones/threadTitle/store.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as ThreadTitleRegeneration from "./ThreadTitleRegenerationService.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { placeholderThreadTitle } from "./placeholderThreadTitle.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const projectId = ProjectId.make("project:title-regeneration");
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.1-codex",
} as const;

const adapter = {
  instanceId: modelSelection.instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("provider execution is disabled in title regeneration tests"),
} as ProviderAdapter.ProviderAdapterV2["Service"];

function makeHarness(
  options: {
    readonly generateTitle?: TextGeneration.TextGeneration["Service"]["generateThreadTitle"];
    readonly automaticTitles?: boolean;
  } = {},
) {
  const layerDatabase = SqlitePersistence.layerMemory;
  const layerRegistry = ProviderAdapterRegistry.layerFromAdapters([adapter]);
  const layerOrchestrator = ProviderReplayHarness.layerWithRegistry(
    { name: "thread-title-regeneration" },
    layerRegistry,
    { databaseLayer: layerDatabase, runEffectWorker: false },
  );
  const layerServerSettings = ServerSettings.layerTest({});
  const layerThreadManagement = ThreadManagement.layer.pipe(
    Layer.provide(layerOrchestrator),
    Layer.provide(layerServerSettings),
    Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
  );
  const layerOutbox = EffectOutbox.layer.pipe(Layer.provide(layerDatabase));
  const generateThreadTitle = vi.fn(
    options.generateTitle ?? (() => Effect.succeed({ title: "Generated title" })),
  );
  const layerProjectedProjects = Layer.mock(ProjectStore.ProjectStoreV2)({
    get: (requestedProjectId) =>
      Effect.succeed(
        requestedProjectId === projectId
          ? Option.some({
              projectId,
              title: "Project",
              workspaceRoot: "/repo",
              defaultModelSelection: modelSelection,
              defaultThreadEnvMode: null,
              hasCustomTitle: false,
              autoPull: false,
              faviconPath: null,
              projectIcon: null,
              scripts: [],
              pinnedAt: null,
              archivedAt: null,
              createdAt: "2026-06-20T00:00:00.000Z",
              updatedAt: "2026-06-20T00:00:00.000Z",
              deletedAt: null,
            })
          : Option.none(),
      ),
  });
  const layerTitleRegeneration = ThreadTitleRegeneration.layer.pipe(
    // Exercise the optional upstream composition and the Jones reader on the same command DB.
    Layer.provide(
      options.automaticTitles ? TitleStore.layer.pipe(Layer.provide(layerDatabase)) : Layer.empty,
    ),
    Layer.provide(
      Layer.mergeAll(
        layerThreadManagement,
        layerProjectedProjects,
        Layer.mock(TextGeneration.TextGeneration)({ generateThreadTitle }),
        layerServerSettings,
        NodeServices.layer,
      ),
    ),
  );
  return {
    layer: Layer.mergeAll(
      layerOrchestrator,
      layerThreadManagement,
      layerTitleRegeneration,
      layerOutbox,
      layerDatabase,
    ),
    generateThreadTitle,
  };
}

function createThread(input: {
  readonly command: string;
  readonly thread: string;
  readonly worktreePath?: string;
  readonly title?: string;
}) {
  return Effect.gen(function* () {
    const threads = yield* ThreadManagement.ThreadManagementService;
    const threadId = ThreadId.make(input.thread);
    yield* threads.dispatch({
      type: "thread.create",
      commandId: CommandId.make(input.command),
      threadId,
      projectId,
      title: input.title ?? "Seed title",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: input.worktreePath ?? null,
      createdBy: "user",
      creationSource: "web",
    });
    return threadId;
  });
}

function dispatchUserMessage(input: {
  readonly command: string;
  readonly threadId: ThreadId;
  readonly text: string;
  readonly attachments?: ReadonlyArray<ChatAttachment>;
  readonly titleSeed?: string;
}) {
  return Effect.gen(function* () {
    const threads = yield* ThreadManagement.ThreadManagementService;
    yield* threads.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(input.command),
      threadId: input.threadId,
      messageId: MessageId.make(`${input.command}:message`),
      text: input.text,
      ...(input.titleSeed === undefined ? {} : { titleSeed: input.titleSeed }),
      attachments: input.attachments ?? [],
      modelSelection,
      dispatchMode: { type: "defer_start" },
      createdBy: "user",
      creationSource: "web",
    });
  });
}

/** Records a settled source run with one exchange, as a provider turn would. */
function recordRun(input: {
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly ordinal: number;
  readonly user: string;
  readonly assistant: string;
  readonly status?: "completed" | "rolled_back";
  readonly nativeRootReply?: boolean;
}) {
  return Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const providerThreadId = ProviderThreadId.make(`${input.threadId}:provider`);
    const rootNodeId = NodeId.make(`${input.runId}:root`);
    const replyNodeId = NodeId.make(`${input.runId}:reply`);
    const message = (role: "user" | "assistant", text: string) => ({
      id: EventId.make(`${input.runId}:${role}`),
      type: "message.updated" as const,
      threadId: input.threadId,
      runId: input.runId,
      occurredAt: now,
      payload: {
        id: MessageId.make(`${input.runId}:${role}`),
        threadId: input.threadId,
        runId: input.runId,
        nodeId: input.nativeRootReply && role === "assistant" ? replyNodeId : null,
        role,
        text,
        attachments: [],
        streaming: false,
        createdBy: "user" as const,
        creationSource: "web" as const,
        createdAt: now,
        updatedAt: now,
      },
    });
    yield* eventSink.write({
      events: [
        {
          id: EventId.make(`${input.runId}:created`),
          type: "run.created",
          threadId: input.threadId,
          runId: input.runId,
          occurredAt: now,
          payload: {
            id: input.runId,
            threadId: input.threadId,
            ordinal: input.ordinal,
            providerInstanceId: modelSelection.instanceId,
            modelSelection,
            providerThreadId,
            userMessageId: MessageId.make(`${input.runId}:user`),
            rootNodeId: input.nativeRootReply ? rootNodeId : null,
            activeAttemptId: null,
            status: input.status ?? "completed",
            requestedAt: now,
            startedAt: now,
            completedAt: now,
            checkpointId: null,
            contextHandoffId: null,
          },
        },
        {
          id: EventId.make(`${input.runId}:provider-thread`),
          type: "provider-thread.updated",
          threadId: input.threadId,
          occurredAt: now,
          payload: {
            id: providerThreadId,
            driver: adapter.driver,
            providerInstanceId: modelSelection.instanceId,
            providerSessionId: null,
            appThreadId: input.threadId,
            ownerNodeId: null,
            nativeThreadRef: { driver: adapter.driver, nativeId: "native", strength: "strong" },
            nativeConversationHeadRef: null,
            status: "idle",
            firstRunOrdinal: 1,
            lastRunOrdinal: input.ordinal,
            handoffIds: [],
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
          },
        },
        message("user", input.user),
        message("assistant", input.assistant),
        ...(input.nativeRootReply
          ? [
              ...(["root_turn", "assistant_message"] as const).map((kind) => ({
                id: EventId.make(`${input.runId}:node:${kind}`),
                type: "node.updated" as const,
                threadId: input.threadId,
                runId: input.runId,
                occurredAt: now,
                payload: {
                  id: kind === "root_turn" ? rootNodeId : replyNodeId,
                  threadId: input.threadId,
                  runId: input.runId,
                  parentNodeId: kind === "root_turn" ? null : rootNodeId,
                  rootNodeId,
                  kind,
                  status: "completed" as const,
                  countsForRun: true,
                  providerThreadId,
                  providerTurnId: null,
                  nativeItemRef: null,
                  runtimeRequestId: null,
                  checkpointScopeId: null,
                  startedAt: now,
                  completedAt: now,
                },
              })),
              {
                id: EventId.make(`${input.runId}:reply-item`),
                type: "turn-item.updated" as const,
                threadId: input.threadId,
                runId: input.runId,
                occurredAt: now,
                payload: {
                  id: TurnItemId.make(`${input.runId}:reply-item`),
                  threadId: input.threadId,
                  runId: input.runId,
                  nodeId: replyNodeId,
                  providerThreadId,
                  providerTurnId: null,
                  nativeItemRef: null,
                  parentItemId: null,
                  ordinal: 1,
                  status: "completed" as const,
                  title: null,
                  startedAt: now,
                  completedAt: now,
                  updatedAt: now,
                  type: "assistant_message" as const,
                  messageId: MessageId.make(`${input.runId}:assistant`),
                  text: input.assistant,
                  streaming: false,
                },
              },
            ]
          : []),
      ],
    });
  });
}

function armRegeneration(input: { readonly command: string; readonly threadId: ThreadId }) {
  return Effect.gen(function* () {
    const threads = yield* ThreadManagement.ThreadManagementService;
    const requestId = CommandId.make(input.command);
    yield* threads.dispatch({
      type: "thread.metadata.update",
      commandId: requestId,
      threadId: input.threadId,
      regenerateTitle: true,
    });
    return requestId;
  });
}

const executePendingAutomaticTitle = Effect.fn("test.title.executePendingAutomaticTitle")(
  function* (threadId: ThreadId) {
    const store = yield* TitleStore.make;
    const state = yield* store.read(threadId);
    assert.isOk(state?.pending);
    const pending = state!.pending!;
    const service = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
    yield* service.execute({
      threadId,
      requestId: pending.requestId,
      kind:
        state!.covered.length === 0
          ? { type: "initial", messageId: pending.evidence[0]! }
          : { type: "regenerate" },
    });
    return pending.requestId;
  },
);

/** Forks a source thread at its middle run, between a rolled-back run and later work. */
function forkSeededSource(prefix: string, sideChat = false, rollbackPriorRun = true) {
  return Effect.gen(function* () {
    const threads = yield* ThreadManagement.ThreadManagementService;
    const sourceThreadId = yield* createThread({
      command: `${prefix}:create`,
      thread: `${prefix}:source`,
    });
    yield* recordRun({
      threadId: sourceThreadId,
      runId: RunId.make(`${prefix}:run:0`),
      ordinal: 1,
      user: "Rolled back attempt",
      assistant: "Discarded.",
      status: rollbackPriorRun ? "rolled_back" : "completed",
    });
    yield* TestClock.adjust(1);
    const forkRunId = RunId.make(`${prefix}:run:1`);
    yield* recordRun({
      threadId: sourceThreadId,
      runId: forkRunId,
      ordinal: 2,
      user: "Move session storage from Redis to Postgres",
      assistant: "Sessions now persist in the Postgres sessions table.",
    });
    yield* recordRun({
      threadId: sourceThreadId,
      runId: RunId.make(`${prefix}:run:2`),
      ordinal: 3,
      user: "Later work the fork never saw",
      assistant: "Done.",
    });
    const forkThreadId = ThreadId.make(`${prefix}:target`);
    yield* threads.dispatch({
      type: "thread.fork",
      commandId: CommandId.make(`${prefix}:fork`),
      sourceThreadId,
      targetThreadId: forkThreadId,
      sourcePoint: { type: "run", runId: forkRunId },
      sideChat,
      createdBy: "user",
      creationSource: "web",
    });
    return { sourceThreadId, forkRunId, forkThreadId };
  });
}

describe("formatThreadTitleContext", () => {
  it("pins the first user message ahead of the retained tail once content stops fitting", () => {
    const context = ThreadTitleRegeneration.formatThreadTitleContext([
      { role: "user", text: `Ancient context that anchors the topic ${"x".repeat(600)}` },
      { role: "assistant", text: "y".repeat(6_000) },
      { role: "user", text: "z".repeat(1_500) },
    ]);
    assert.isTrue(context.message.includes("USER:\nAncient context that anchors the topic"));
    assert.isTrue(context.message.includes("[Earlier content truncated]\n\n"));
    assert.isTrue(context.message.includes("y".repeat(100)));
  });
});

describe("ThreadTitleRegenerationService", () => {
  it.effect("keeps a title set before the first message unless it is a placeholder", () =>
    Effect.gen(function* () {
      const seed = "work on this and pr to dev";
      const harness = makeHarness();
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        // An MCP launch names the thread, or leaves a placeholder, and sends no message.
        for (const [key, title, expectedTitle, generates] of [
          ["preset", "[API] #4821 — retry webhook deliveries after a timeout", undefined, false],
          ["placeholder", placeholderThreadTitle("Parent", 0), seed, true],
        ] as const) {
          const threadId = ThreadId.make(`thread:title:${key}`);
          yield* threads.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`command:title:${key}:create`),
            threadId,
            projectId,
            title,
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdBy: "agent",
            creationSource: "mcp",
          });
          const requestId = CommandId.make(`command:title:${key}:message`);
          yield* threads.dispatch({
            type: "message.dispatch",
            commandId: requestId,
            threadId,
            messageId: MessageId.make(`command:title:${key}:message:message`),
            text: seed,
            titleSeed: seed,
            attachments: [],
            modelSelection,
            dispatchMode: { type: "defer_start" },
            createdBy: "user",
            creationSource: "web",
          });

          const settled = yield* threads.getThreadProjection(threadId);
          assert.equal(settled.thread.title, expectedTitle ?? title, key);
          assert.equal(
            (yield* outbox.listByCommandId(requestId)).some(
              (effect) => effect.request.type === "thread-title.generate",
            ),
            generates,
            key,
          );
        }
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect(
    "titles an image-only first message without sending bootstrap text to the title model",
    () =>
      Effect.gen(function* () {
        for (const prefix of ["", "Ultrathink:\n"]) {
          const harness = makeHarness();
          yield* Effect.gen(function* () {
            const threads = yield* ThreadManagement.ThreadManagementService;
            const service = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
            const threadId = yield* createThread({
              command: "image:create",
              thread: "image:thread",
            });
            const text = prefix + ATTACHMENT_ONLY_BOOTSTRAP_PROMPT;
            const attachments = [
              {
                type: "image" as const,
                id: ChatAttachmentId.make("diagram"),
                name: "diagram.png",
                mimeType: "image/png",
                sizeBytes: 10,
              },
            ];
            yield* dispatchUserMessage({ command: "image:message", threadId, text, attachments });
            const requestId = yield* armRegeneration({ command: "image:title", threadId });
            const messageId = MessageId.make("image:message:message");
            yield* service.execute({ threadId, requestId, kind: { type: "initial", messageId } });
            const call = harness.generateThreadTitle.mock.calls[0]?.[0];
            assert.equal(call?.message, "");
            assert.deepEqual(call?.attachments, attachments);
            const projection = yield* threads.getThreadProjection(threadId);
            assert.equal(projection.messages[0]?.text, text);
            assert.equal(projection.thread.title, "Generated title");
          }).pipe(Effect.provide(harness.layer));
        }
      }),
  );

  it.effect("arms and clears the regeneration marker through metadata commands", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const threadId = yield* createThread({
          command: "command:title:arm:create",
          thread: "thread:title:arm",
        });

        const firstRequest = yield* armRegeneration({ command: "command:title:arm:1", threadId });
        const armed = yield* threads.getThreadProjection(threadId);
        assert.equal(armed.thread.titleRegeneration?.requestId, firstRequest);
        assert.deepEqual(
          (yield* outbox.listByCommandId(firstRequest)).map((effect) => effect.request),
          [{ type: "thread-title.generate", kind: { type: "regenerate" } }],
        );

        yield* threads.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("command:title:arm:abandon"),
          threadId,
          regenerateTitle: false,
        });
        const abandoned = yield* threads.getThreadProjection(threadId);
        assert.isNotOk(abandoned.thread.titleRegeneration);

        yield* armRegeneration({ command: "command:title:arm:2", threadId });
        yield* threads.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("command:title:arm:rename"),
          threadId,
          title: "Manual title",
        });
        const renamed = yield* threads.getThreadProjection(threadId);
        assert.equal(renamed.thread.title, "Manual title");
        assert.isNotOk(renamed.thread.titleRegeneration);
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect("skips execution when the marker was superseded by a newer request", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
        const threadId = yield* createThread({
          command: "command:title:stale:create",
          thread: "thread:title:stale",
        });
        const staleRequest = yield* armRegeneration({ command: "command:title:stale:1", threadId });
        const currentRequest = yield* armRegeneration({
          command: "command:title:stale:2",
          threadId,
        });

        yield* titleRegeneration.execute({
          threadId,
          requestId: staleRequest,
          kind: { type: "regenerate" },
        });

        assert.equal(harness.generateThreadTitle.mock.calls.length, 0);
        const projection = yield* threads.getThreadProjection(threadId);
        assert.equal(projection.thread.title, "Seed title");
        assert.equal(projection.thread.titleRegeneration?.requestId, currentRequest);
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect("lands the regenerated title from the conversation digest", () =>
    Effect.gen(function* () {
      const harness = makeHarness({
        generateTitle: () => Effect.succeed({ title: "Fresh title" }),
      });
      const fs = yield* FileSystem.FileSystem;
      const existingWorktree = yield* fs.makeTempDirectoryScoped({ prefix: "title-worktree-" });
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
        const cases = [
          { name: "removed", worktreePath: "/repo-worktrees/removed-by-cleanup", cwd: "/repo" },
          { name: "existing", worktreePath: existingWorktree, cwd: existingWorktree },
        ];
        for (const [index, testCase] of cases.entries()) {
          const threadId = yield* createThread({
            command: `command:title:landing:${testCase.name}:create`,
            thread: `thread:title:landing:${testCase.name}`,
            worktreePath: testCase.worktreePath,
          });
          yield* dispatchUserMessage({
            command: `command:title:landing:${testCase.name}:message`,
            threadId,
            text: "Investigate the flaky login test",
          });
          const requestId = yield* armRegeneration({
            command: `command:title:landing:${testCase.name}:1`,
            threadId,
          });

          yield* titleRegeneration.execute({ threadId, requestId, kind: { type: "regenerate" } });

          const call = harness.generateThreadTitle.mock.calls[index]?.[0];
          assert.equal(call?.previousTitle, "Seed title");
          assert.equal(call?.cwd, testCase.cwd);
          assert.include(call?.message, "USER:\nInvestigate the flaky login test");
          const projection = yield* threads.getThreadProjection(threadId);
          assert.equal(projection.thread.title, "Fresh title");
          assert.isNotOk(projection.thread.titleRegeneration);
          assert.deepInclude(projection.thread.lastTitleRegeneration, {
            requestId,
            status: "renamed",
          });
        }
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.each([
    { name: "its first message", followUp: true },
    { name: "no message of its own yet", followUp: false },
  ])("titles a fork with $name from the source history up to the fork point", ({ followUp }) =>
    Effect.gen(function* () {
      const harness = makeHarness();
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
        const { forkThreadId } = yield* forkSeededSource("thread:title:fork");
        const requestId = CommandId.make("command:title:fork:request");
        const messageId = MessageId.make("command:title:fork:request:message");
        if (followUp) {
          yield* threads.dispatch({
            type: "message.dispatch",
            commandId: requestId,
            threadId: forkThreadId,
            messageId,
            text: "Try SQLite instead",
            titleSeed: "Try SQLite instead",
            attachments: [],
            modelSelection,
            dispatchMode: { type: "defer_start" },
            createdBy: "user",
            creationSource: "web",
          });
        } else {
          yield* armRegeneration({ command: requestId, threadId: forkThreadId });
        }

        yield* titleRegeneration.execute({
          threadId: forkThreadId,
          requestId,
          kind: followUp ? { type: "initial", messageId } : { type: "regenerate" },
        });

        const call = harness.generateThreadTitle.mock.calls[0]?.[0];
        assert.equal(call?.message, followUp ? "Try SQLite instead" : "");
        assert.equal(call?.forkSource?.title, "Seed title");
        assert.include(call?.forkSource?.history, "Move session storage from Redis to Postgres");
        assert.include(call?.forkSource?.history, "Sessions now persist in the Postgres");
        assert.notInclude(call?.forkSource?.history, "Later work the fork never saw");
        assert.notInclude(call?.forkSource?.history, "Rolled back attempt");
        const fork = yield* threads.getThreadRecords(forkThreadId, []);
        assert.equal(fork.thread.title, "Generated title");
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect.each(["plain", "deleted source"] as const)(
    "refuses to title an empty thread with %s history",
    (history) =>
      Effect.gen(function* () {
        const harness = makeHarness();
        yield* Effect.gen(function* () {
          const threads = yield* ThreadManagement.ThreadManagementService;
          const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
          const { forkThreadId } = yield* forkSeededSource("thread:title:empty");
          const { thread } = yield* threads.getThreadProjection(forkThreadId);
          const eventSink = yield* EventSink.EventSinkV2;
          yield* eventSink.write({
            events: [
              {
                id: EventId.make("empty:lineage"),
                type: "thread.metadata-updated",
                threadId: forkThreadId,
                occurredAt: yield* DateTime.now,
                payload: {
                  ...thread,
                  forkedFrom:
                    history === "plain"
                      ? null
                      : {
                          type: "run",
                          threadId: ThreadId.make("absent"),
                          runId: RunId.make("absent"),
                        },
                },
              },
            ],
          });
          const requestId = yield* armRegeneration({
            command: "empty:request",
            threadId: forkThreadId,
          });
          yield* titleRegeneration.execute({
            threadId: forkThreadId,
            requestId,
            kind: { type: "regenerate" },
          });
          assert.equal(harness.generateThreadTitle.mock.calls.length, 0);
          const fork = yield* threads.getThreadRecords(forkThreadId, []);
          assert.deepInclude(fork.thread.lastTitleRegeneration, {
            requestId,
            status: "failed",
            message: "This thread has no messages to title yet.",
          });
        }).pipe(Effect.provide(harness.layer));
      }),
  );

  it.effect("keeps the fork point when the source rolls it back after forking", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
        const { sourceThreadId, forkRunId, forkThreadId } = yield* forkSeededSource(
          "thread:title:rollback",
          false,
          false,
        );
        yield* TestClock.adjust(1);
        const source = yield* threads.getThreadRecords(sourceThreadId, ["runs"]);
        const run = source.runs.find((run) => run.id === forkRunId);
        assert.isDefined(run);
        const now = yield* DateTime.now;
        const eventSink = yield* EventSink.EventSinkV2;
        yield* eventSink.write({
          events: [
            {
              id: EventId.make("source:rollback"),
              type: "run.updated",
              threadId: sourceThreadId,
              runId: run.id,
              occurredAt: now,
              payload: { ...run, status: "rolled_back", completedAt: now },
            },
          ],
        });
        const requestId = yield* armRegeneration({
          command: "rollback:request",
          threadId: forkThreadId,
        });
        yield* titleRegeneration.execute({
          threadId: forkThreadId,
          requestId,
          kind: { type: "regenerate" },
        });
        const call = harness.generateThreadTitle.mock.calls[0]?.[0];
        assert.include(call?.forkSource?.history, "Move session storage from Redis to Postgres");
        assert.include(call?.forkSource?.history, "Rolled back attempt");
        assert.notInclude(call?.forkSource?.history, "Later work the fork never saw");
        const fork = yield* threads.getThreadRecords(forkThreadId, []);
        assert.equal(fork.thread.title, "Generated title");
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect("does not inherit imported history when the fork point is missing", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
        const { sourceThreadId, forkThreadId } = yield* forkSeededSource("thread:title:invalid");
        yield* dispatchUserMessage({
          command: "imported:message",
          threadId: sourceThreadId,
          text: "Imported history",
        });
        const source = yield* threads.getThreadRecords(sourceThreadId, []);
        const fork = yield* threads.getThreadRecords(forkThreadId, []);
        const eventSink = yield* EventSink.EventSinkV2;
        yield* eventSink.write({
          events: [
            {
              id: EventId.make("source:imported"),
              type: "thread.metadata-updated",
              threadId: sourceThreadId,
              occurredAt: yield* DateTime.now,
              payload: { ...source.thread, historyOrigin: "v1_import" },
            },
            {
              id: EventId.make("fork:missing-run"),
              type: "thread.metadata-updated",
              threadId: forkThreadId,
              occurredAt: yield* DateTime.now,
              payload: {
                ...fork.thread,
                forkedFrom: {
                  type: "run",
                  threadId: sourceThreadId,
                  runId: RunId.make("absent:run"),
                },
              },
            },
          ],
        });
        const requestId = yield* armRegeneration({
          command: "invalid:request",
          threadId: forkThreadId,
        });
        yield* titleRegeneration.execute({
          threadId: forkThreadId,
          requestId,
          kind: { type: "regenerate" },
        });
        assert.equal(harness.generateThreadTitle.mock.calls.length, 0);
        const updated = yield* threads.getThreadRecords(forkThreadId, []);
        assert.deepInclude(updated.thread.lastTitleRegeneration, {
          requestId,
          status: "failed",
          message: "This thread has no messages to title yet.",
        });
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect("inherits a side chat's own exchange without its parent's history", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
        const { forkThreadId: sideChatId } = yield* forkSeededSource(
          "thread:title:side-ancestor",
          true,
        );
        const runId = RunId.make("side:run");
        yield* recordRun({
          threadId: sideChatId,
          runId,
          ordinal: 1,
          user: "Side topic",
          assistant: "Side answer",
        });
        const threadId = ThreadId.make("side:fork");
        yield* threads.dispatch({
          type: "thread.fork",
          commandId: CommandId.make("side:fork"),
          sourceThreadId: sideChatId,
          targetThreadId: threadId,
          sourcePoint: { type: "run", runId },
          title: "Side fork",
          createdBy: "user",
          creationSource: "web",
        });
        const requestId = yield* armRegeneration({ command: "side:request", threadId });
        yield* titleRegeneration.execute({ threadId, requestId, kind: { type: "regenerate" } });
        const call = harness.generateThreadTitle.mock.calls[0]?.[0];
        assert.equal(call?.forkSource?.title, "Seed title fork");
        assert.include(call?.forkSource?.history, "Side topic");
        assert.include(call?.forkSource?.history, "Side answer");
        assert.notInclude(call?.forkSource?.history, "Move session storage");
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect("clears the marker and keeps the title when generation fails", () =>
    Effect.gen(function* () {
      const harness = makeHarness({
        generateTitle: () =>
          Effect.fail(
            new TextGenerationError({
              operation: "generateThreadTitle",
              detail:
                "Codex CLI command failed: OpenAI Codex\nuser\nConversation text\nmodel unavailable\n",
            }),
          ),
      });
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
        const threadId = yield* createThread({
          command: "command:title:failure:create",
          thread: "thread:title:failure",
        });
        yield* dispatchUserMessage({
          command: "command:title:failure:message",
          threadId,
          text: "Some conversation",
        });
        const requestId = yield* armRegeneration({ command: "command:title:failure:1", threadId });

        yield* titleRegeneration.execute({ threadId, requestId, kind: { type: "regenerate" } });

        assert.equal(harness.generateThreadTitle.mock.calls.length, 1);
        const projection = yield* threads.getThreadProjection(threadId);
        assert.equal(projection.thread.title, "Seed title");
        assert.isNotOk(projection.thread.titleRegeneration);
        assert.deepInclude(projection.thread.lastTitleRegeneration, {
          requestId,
          status: "failed",
          message: "model unavailable",
        });
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect("records an unchanged outcome when the model keeps the current title", () =>
    Effect.gen(function* () {
      const harness = makeHarness({
        generateTitle: () => Effect.succeed({ title: "Seed title" }),
      });
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
        const threadId = yield* createThread({
          command: "command:title:unchanged:create",
          thread: "thread:title:unchanged",
        });
        yield* dispatchUserMessage({
          command: "command:title:unchanged:message",
          threadId,
          text: "Some conversation",
        });
        const requestId = yield* armRegeneration({
          command: "command:title:unchanged:1",
          threadId,
        });

        yield* titleRegeneration.execute({ threadId, requestId, kind: { type: "regenerate" } });

        const projection = yield* threads.getThreadProjection(threadId);
        assert.deepInclude(projection.thread.lastTitleRegeneration, {
          requestId,
          status: "unchanged",
        });
      }).pipe(Effect.provide(harness.layer));
    }),
  );
});

describe("ThreadTitleRegenerationService automatic state composition", () => {
  it.effect(
    "uses pending owner evidence and persisted root replies, then commits an unchanged sufficient refinement",
    () => {
      let calls = 0;
      const harness = makeHarness({
        automaticTitles: true,
        generateTitle: () =>
          Effect.succeed({
            title: "Installation and update choices",
            assessment: ++calls === 1 ? "insufficient" : "sufficient",
          }),
      });
      return Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const store = yield* TitleStore.make;
        const threadId = yield* createThread({
          command: "create",
          thread: "thread",
          title: "New thread",
        });
        yield* dispatchUserMessage({ command: "maintenance", threadId, text: "/compact" });
        assert.isNull(yield* store.read(threadId));
        const attachments = Array.from({ length: 8 }, (_, index) => ({
          type: "image" as const,
          id: ChatAttachmentId.make(`image-${index}`),
          name: `image-${index}.png`,
          mimeType: "image/png",
          sizeBytes: 1,
        }));
        yield* dispatchUserMessage({
          command: "one",
          threadId,
          text:
            "Goal: installation and update choices\n" +
            "handoff history ".repeat(2_000) +
            "\nAdopt this goal",
          titleSeed: "New thread",
          attachments,
        });
        const beforeReply = yield* threads.getThreadRecords(threadId, ["runs"]);
        yield* recordRun({
          threadId,
          runId: RunId.make("settled-root"),
          ordinal: Math.max(0, ...beforeReply.runs.map((run) => run.ordinal)) + 1,
          user: "UNACCEPTED OWNER EVIDENCE",
          assistant: "The installation options include storage and backups.",
          nativeRootReply: true,
        });
        yield* dispatchUserMessage({
          command: "two",
          threadId,
          text: "Choose full backup versus clone",
        });
        assert.deepEqual((yield* store.read(threadId))?.pending?.evidence, [
          MessageId.make("one:message"),
        ]);
        yield* executePendingAutomaticTitle(threadId);
        const first = harness.generateThreadTitle.mock.calls[0]![0]!;
        assert.isTrue(first.titleAssessment);
        assert.equal(first.linkedContext, "");
        assert.isUndefined(first.forkSource);
        assert.isUndefined(first.previousTitle);
        assert.equal(first.projectTitle, "Project");
        assert.equal(first.projectId, projectId);
        assert.isAtMost(first.message.length, 8_000);
        assert.include(first.message, "Goal: installation and update choices");
        assert.include(first.message, "Adopt this goal");
        assert.include(first.message, "ASSISTANT:\nThe installation options");
        assert.notInclude(first.message, "UNACCEPTED OWNER EVIDENCE");
        assert.notInclude(first.message, "full backup versus clone");
        assert.notInclude(first.message, "/compact");
        assert.deepEqual(first.attachments, [...attachments.slice(0, 1), ...attachments.slice(-3)]);
        const next = yield* store.read(threadId);
        assert.deepEqual(next?.covered, [MessageId.make("one:message")]);
        assert.equal(next?.lastRequestId, "one");
        assert.deepEqual(next?.pending?.evidence, [
          MessageId.make("one:message"),
          MessageId.make("two:message"),
        ]);
        const refinement = yield* executePendingAutomaticTitle(threadId);
        const second = harness.generateThreadTitle.mock.calls[1]![0]!;
        assert.isTrue(second.titleAssessment);
        assert.equal(second.linkedContext, "");
        assert.equal(second.previousTitle, "Installation and update choices");
        assert.include(second.message, "full backup versus clone");
        const completed = yield* store.read(threadId);
        assert.equal(completed?.status, "sufficient");
        assert.isTrue(completed?.knownInsufficient);
        assert.equal(completed?.lastRequestId, refinement);
        assert.isNull(completed?.pending);
        const snapshot = yield* threads.getThreadProjection(threadId);
        assert.equal(snapshot.thread.title, "Installation and update choices");
        assert.deepInclude(snapshot.thread.lastTitleRegeneration, {
          requestId: refinement,
          status: "unchanged",
        });
        yield* dispatchUserMessage({ command: "three", threadId, text: "Keep updates manual" });
        assert.isNotOk((yield* threads.getThreadProjection(threadId)).thread.titleRegeneration);
        assert.equal(calls, 2);
      }).pipe(Effect.provide(harness.layer));
    },
  );

  it.effect.each([
    {
      label: "sufficient",
      result: { title: "Installation choices", assessment: "sufficient" as const },
      status: "sufficient",
      insufficient: false,
    },
    {
      label: "missing",
      result: { title: "Installation choices" },
      status: "active",
      insufficient: false,
    },
    // Deliberately malformed provider output bypasses the static union at this external seam.
    {
      label: "malformed",
      result: { title: "Installation choices", assessment: "invalid" as "unknown" },
      status: "active",
      insufficient: false,
    },
    {
      label: "explicit refinement",
      result: {
        title: "Installation choices",
        assessment: "sufficient" as const,
        needsRefinement: true,
      },
      status: "active",
      insufficient: true,
    },
    {
      label: "truthy refinement",
      result: { title: "Installation choices", needsRefinement: "true" as unknown as boolean },
      status: "active",
      insufficient: false,
    },
  ])("commits $label assessment without inventing a pass", ({ result, status, insufficient }) => {
    const harness = makeHarness({
      automaticTitles: true,
      generateTitle: () => Effect.succeed(result),
    });
    return Effect.gen(function* () {
      const store = yield* TitleStore.make;
      const threadId = yield* createThread({
        command: "create",
        thread: "thread",
        title: "New thread",
      });
      yield* dispatchUserMessage({
        command: "one",
        threadId,
        text: "Choose installation options",
        titleSeed: "New thread",
      });
      yield* executePendingAutomaticTitle(threadId);
      const state = yield* store.read(threadId);
      assert.equal(state?.status, status);
      assert.equal(state?.knownInsufficient, insufficient);
      assert.deepEqual(state?.accepted, [MessageId.make("one:message")]);
      assert.deepEqual(state?.covered, [MessageId.make("one:message")]);
      assert.equal(state?.lastRequestId, "one");
      assert.isNull(state?.pending);
      assert.equal(harness.generateThreadTitle.mock.calls.length, 1);
      assert.isTrue(harness.generateThreadTitle.mock.calls[0]![0]!.titleAssessment);
    }).pipe(Effect.provide(harness.layer));
  });

  it.effect(
    "rolls the assessed title, evidence, events, receipt and next effect back on a state write failure",
    () => {
      const harness = makeHarness({
        automaticTitles: true,
        generateTitle: () =>
          Effect.succeed({ title: "Installation choices", assessment: "insufficient" }),
      });
      return Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const store = yield* TitleStore.make;
        const events = yield* EventStore.EventStoreV2.pipe(Effect.provide(EventStore.layer));
        const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2.pipe(
          Effect.provide(CommandReceiptStore.layer),
        );
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const threadId = yield* createThread({
          command: "create",
          thread: "thread",
          title: "New thread",
        });
        yield* dispatchUserMessage({
          command: "one",
          threadId,
          text: "Choose installation options",
          titleSeed: "New thread",
        });
        yield* dispatchUserMessage({
          command: "two",
          threadId,
          text: "Include storage and backups",
        });
        const before = yield* store.read(threadId);
        const sequence = yield* events.latestSequence({ threadId });
        const completionId = CommandId.make("one:title-complete");
        yield* sql`CREATE TEMP TRIGGER reject_title_state BEFORE UPDATE ON jones_thread_title_refinement
        BEGIN SELECT RAISE(ABORT, 'synthetic title state failure'); END`;
        const failed = yield* executePendingAutomaticTitle(threadId).pipe(Effect.exit);
        assert.equal(failed._tag, "Failure");
        assert.deepEqual(yield* store.read(threadId), before);
        assert.equal((yield* threads.getThreadProjection(threadId)).thread.title, "New thread");
        assert.equal(yield* events.latestSequence({ threadId }), sequence);
        assert.isTrue(Option.isNone(yield* receipts.getByCommandId(completionId)));
        assert.deepEqual(yield* outbox.listByCommandId(completionId), []);
        yield* sql`DROP TRIGGER reject_title_state`;
        yield* executePendingAutomaticTitle(threadId);
        const committed = yield* store.read(threadId);
        assert.equal(committed?.lastRequestId, "one");
        assert.deepEqual(committed?.covered, [MessageId.make("one:message")]);
        assert.deepEqual(committed?.pending?.evidence, [
          MessageId.make("one:message"),
          MessageId.make("two:message"),
        ]);
        const receipt = yield* receipts.getByCommandId(completionId);
        assert.isTrue(Option.isSome(receipt));
        const effects = yield* outbox.listByCommandId(completionId);
        assert.equal(
          effects.filter((effect) => effect.request.type === "thread-title.generate").length,
          1,
        );
        const committedSequence = yield* events.latestSequence({ threadId });
        yield* threads.dispatch({
          type: "thread.title.regeneration.complete",
          threadId,
          requestId: CommandId.make("one"),
          commandId: completionId,
          title: "Replay must not win",
        });
        assert.deepEqual(yield* store.read(threadId), committed);
        assert.deepEqual(yield* receipts.getByCommandId(completionId), receipt);
        assert.deepEqual(yield* outbox.listByCommandId(completionId), effects);
        assert.equal(yield* events.latestSequence({ threadId }), committedSequence);
        assert.equal(
          (yield* threads.getThreadProjection(threadId)).thread.title,
          "Installation choices",
        );
      }).pipe(Effect.provide(harness.layer));
    },
  );

  it.effect(
    "rejects a late automatic verdict after manual replacement and does not assess the replacement",
    () => {
      let started: Deferred.Deferred<void>;
      let release: Deferred.Deferred<void>;
      let calls = 0;
      const harness = makeHarness({
        automaticTitles: true,
        generateTitle: () =>
          Effect.gen(function* () {
            if (++calls === 1) {
              yield* Deferred.succeed(started, undefined);
              yield* Deferred.await(release);
              return { title: "Late automatic title", assessment: "sufficient" as const };
            }
            return { title: "Manual regeneration", assessment: "insufficient" as const };
          }),
      });
      return Effect.gen(function* () {
        started = yield* Deferred.make<void>();
        release = yield* Deferred.make<void>();
        const store = yield* TitleStore.make;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const service = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
        const threadId = yield* createThread({
          command: "create",
          thread: "thread",
          title: "New thread",
        });
        yield* dispatchUserMessage({
          command: "one",
          threadId,
          text: "Choose installation options",
          titleSeed: "New thread",
        });
        const worker = yield* executePendingAutomaticTitle(threadId).pipe(Effect.forkChild);
        yield* Deferred.await(started).pipe(Effect.raceFirst(Fiber.join(worker)));
        const replacement = yield* armRegeneration({ command: "manual", threadId });
        const stopped = yield* store.read(threadId);
        assert.equal(stopped?.status, "stopped");
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(worker);
        assert.deepEqual(yield* store.read(threadId), stopped);
        const afterLate = yield* threads.getThreadProjection(threadId);
        assert.equal(afterLate.thread.title, "New thread");
        assert.equal(afterLate.thread.titleRegeneration?.requestId, replacement);
        yield* service.execute({ threadId, requestId: replacement, kind: { type: "regenerate" } });
        assert.isUndefined(harness.generateThreadTitle.mock.calls[1]![0]!.titleAssessment);
        assert.isUndefined(harness.generateThreadTitle.mock.calls[1]![0]!.linkedContext);
        assert.deepEqual(yield* store.read(threadId), stopped);
        assert.equal(
          (yield* threads.getThreadProjection(threadId)).thread.title,
          "Manual regeneration",
        );
        yield* dispatchUserMessage({ command: "two", threadId, text: "More detail" });
        assert.isNotOk((yield* threads.getThreadProjection(threadId)).thread.titleRegeneration);
        assert.equal(calls, 2);
      }).pipe(Effect.provide(harness.layer));
    },
  );

  it.effect(
    "keeps custom and imported legacy titles outside automatic policy with the real reader installed",
    () => {
      const harness = makeHarness({ automaticTitles: true });
      return Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const store = yield* TitleStore.make;
        const service = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
        const custom = yield* createThread({
          command: "custom:create",
          thread: "custom",
          title: "Custom title",
        });
        yield* dispatchUserMessage({
          command: "custom:one",
          threadId: custom,
          text: "Choose installation options",
          titleSeed: "New thread",
        });
        assert.isNull(yield* store.read(custom));
        const customSnapshot = yield* threads.getThreadProjection(custom);
        assert.equal(customSnapshot.thread.title, "Custom title");
        assert.isNotOk(customSnapshot.thread.titleRegeneration);
        const manual = yield* armRegeneration({ command: "custom:manual", threadId: custom });
        yield* service.execute({
          threadId: custom,
          requestId: manual,
          kind: { type: "regenerate" },
        });
        assert.isUndefined(harness.generateThreadTitle.mock.calls[0]![0]!.titleAssessment);
        assert.equal((yield* store.read(custom))?.status, "stopped");

        const legacy = yield* createThread({
          command: "legacy:create",
          thread: "legacy",
          title: "New thread",
        });
        const { thread } = yield* threads.getThreadProjection(legacy);
        const sink = yield* EventSink.EventSinkV2;
        yield* sink.write({
          events: [
            {
              id: EventId.make("legacy:history"),
              type: "thread.metadata-updated",
              threadId: legacy,
              occurredAt: yield* DateTime.now,
              payload: { ...thread, historyOrigin: "v1_import" },
            },
          ],
        });
        yield* dispatchUserMessage({
          command: "legacy:one",
          threadId: legacy,
          text: "Legacy subject",
          titleSeed: "New thread",
        });
        assert.isNull(yield* store.read(legacy));
        yield* service.execute({
          threadId: legacy,
          requestId: CommandId.make("legacy:one"),
          kind: { type: "initial", messageId: MessageId.make("legacy:one:message") },
        });
        assert.equal(harness.generateThreadTitle.mock.calls.length, 2);
        assert.isUndefined(harness.generateThreadTitle.mock.calls[1]![0]!.titleAssessment);
        assert.isNull(yield* store.read(legacy));
        assert.equal((yield* threads.getThreadProjection(legacy)).thread.title, "Generated title");
      }).pipe(Effect.provide(harness.layer));
    },
  );
});
