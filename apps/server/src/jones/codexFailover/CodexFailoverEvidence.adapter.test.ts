import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CodexSettings,
  MessageId,
  NodeId,
  ProjectId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as CodexReplay from "effect-codex-app-server/replay";
import type { ServerNotification__CodexErrorInfo } from "effect-codex-app-server/schema";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import { TestClock } from "effect/testing";

import packageJson from "../../../package.json" with { type: "json" };
import * as CodexAdapterV2 from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import {
  makeCodexReplayClientFactory,
  makeReplayServerConfig,
} from "../../orchestration-v2/Adapters/CodexAdapterV2.testkit.ts";
import { migrationManifest, runMigrations } from "../../persistence/Migrations.ts";
import migration from "../persistence/Migrations/129_JonesCodexFailover.ts";
import * as ControlAdmission from "../orchestration/ControlAdmission.ts";
import { materializeCodexReplayAppIdentity } from "../runtimeIdentity/CodexReplayIdentity.ts";
import { codexCapacityRetryPolicy } from "../provider/codex/CodexCapacityContinuation.ts";
import * as Evidence from "./CodexFailoverEvidence.ts";

const DEFAULT_CODEX_SETTINGS = Schema.decodeSync(CodexSettings)({});
const CODEX_TEST_MODEL_SELECTION = {
  instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
  model: "gpt-5.4",
} satisfies ModelSelection;
const CODEX_TEST_RUNTIME_POLICY = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: "/workspace",
});

function makeCodexTestAppThread(input: {
  readonly threadId: ThreadId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly now: DateTime.Utc;
}): OrchestrationV2AppThread {
  return {
    createdBy: "user",
    creationSource: "web",
    id: input.threadId,
    projectId: ProjectId.make(`project-${input.threadId}`),
    title: "Codex continuation test",
    providerInstanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
    modelSelection: CODEX_TEST_MODEL_SELECTION,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: input.providerThread.id,
    lineage: {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: input.threadId,
    },
    forkedFrom: null,
    createdAt: input.now,
    updatedAt: input.now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
}

function makeCodexTestTurnInput(input: {
  readonly threadId: ThreadId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly now: DateTime.Utc;
  readonly attemptId: RunAttemptId;
  readonly text: string;
}): ProviderAdapter.ProviderAdapterV2TurnInput {
  return {
    appThread: makeCodexTestAppThread(input),
    threadId: input.threadId,
    runId: RunId.make(`run-${input.attemptId}`),
    runOrdinal: 1,
    providerTurnOrdinal: 1,
    attemptId: input.attemptId,
    rootNodeId: NodeId.make(`node-${input.attemptId}`),
    providerThread: input.providerThread,
    message: {
      createdBy: "user",
      creationSource: "web",
      messageId: MessageId.make(`message-${input.attemptId}`),
      text: input.text,
      attachments: [],
    },
    modelSelection: CODEX_TEST_MODEL_SELECTION,
    runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
  };
}

function makeCodexReplayTurn(input: {
  readonly id: string;
  readonly status: "inProgress" | "completed" | "interrupted" | "failed";
}): Record<string, unknown> {
  const terminal =
    input.status === "completed" || input.status === "interrupted" || input.status === "failed";
  return {
    id: input.id,
    items: [],
    itemsView: "notLoaded",
    status: input.status,
    error: null,
    startedAt: 1782622440,
    completedAt: terminal ? 1782622450 : null,
    durationMs: null,
  };
}

function codexReplayPreamble(input: {
  readonly nativeThreadId: string;
  readonly nativeTurnId: string;
  readonly prompt: string;
  readonly sentPrompt?: string;
}): Array<CodexReplay.CodexAppServerReplayEntry> {
  return [
    {
      type: "expect_outbound",
      label: "initialize",
      frame: {
        id: 1,
        method: "initialize",
        params: {
          clientInfo: { name: "T3 Code", title: "T3 Code", version: packageJson.version },
          capabilities: {
            experimentalApi: true,
            optOutNotificationMethods: ["turn/diff/updated"],
            extensions: {
              "io.modelcontextprotocol/ui": { mimeTypes: ["text/html;profile=mcp-app"] },
            },
          },
        },
      },
    },
    {
      type: "emit_inbound",
      label: "initialize",
      frame: {
        id: 1,
        result: {
          userAgent: "T3 Code/0.156.1",
          codexHome: "/tmp/codex-home",
          platformFamily: "unix",
          platformOs: "macos",
        },
      },
    },
    { type: "expect_outbound", label: "initialized", frame: { method: "initialized" } },
    {
      type: "expect_outbound",
      label: "thread/start",
      frame: {
        id: 2,
        method: "thread/start",
        params: { config: CodexAdapterV2.CODEX_THREAD_CONFIG },
      },
    },
    {
      type: "emit_inbound",
      label: "thread/start",
      frame: {
        id: 2,
        result: {
          thread: {
            id: input.nativeThreadId,
            sessionId: input.nativeThreadId,
            forkedFromId: null,
            preview: "",
            ephemeral: false,
            modelProvider: "openai",
            createdAt: 1782622440,
            updatedAt: 1782622440,
            status: { type: "idle" },
            path: `/tmp/${input.nativeThreadId}.jsonl`,
            cwd: "/workspace",
            cliVersion: "0.144.0",
            source: "vscode",
            threadSource: null,
            agentNickname: null,
            agentRole: null,
            gitInfo: null,
            name: null,
            turns: [],
          },
          model: "gpt-5.4",
          modelProvider: "openai",
          serviceTier: null,
          cwd: "/workspace",
          instructionSources: [],
          approvalPolicy: "on-request",
          approvalsReviewer: "user",
          sandbox: { type: "workspaceWrite", writableRoots: [], networkAccess: false },
          reasoningEffort: "medium",
        },
      },
    },
    {
      type: "expect_outbound",
      label: "turn/start",
      frame: {
        id: 3,
        method: "turn/start",
        params: {
          threadId: input.nativeThreadId,
          input: [{ type: "text", text: input.sentPrompt ?? input.prompt }],
          cwd: "/workspace",
          model: "gpt-5.4",
          collaborationMode: { mode: "default", settings: { model: "gpt-5.4" } },
          approvalPolicy: "never",
          approvalsReviewer: "user",
          sandboxPolicy: { type: "dangerFullAccess" },
          summary: "detailed",
        },
      },
    },
    {
      type: "emit_inbound",
      label: "turn/start",
      frame: {
        id: 3,
        result: { turn: makeCodexReplayTurn({ id: input.nativeTurnId, status: "inProgress" }) },
      },
    },
    {
      type: "emit_inbound",
      label: "turn/started",
      frame: {
        method: "turn/started",
        params: {
          threadId: input.nativeThreadId,
          turn: makeCodexReplayTurn({ id: input.nativeTurnId, status: "inProgress" }),
        },
      },
    },
  ];
}

const decodeMaterializedTranscript = Schema.decodeUnknownSync(
  CodexReplay.CodexAppServerReplayTranscript,
);

function makeCodexReplayTranscript(input: {
  readonly scenario: string;
  readonly entries: ReadonlyArray<CodexReplay.CodexAppServerReplayEntry>;
}): CodexReplay.CodexAppServerReplayTranscript {
  return decodeMaterializedTranscript(
    materializeCodexReplayAppIdentity({
      provider: "codex",
      protocol: "codex.app-server",
      version: "0.144.0",
      scenario: input.scenario,
      entries: input.entries,
    }),
  );
}

interface Scenario {
  readonly name: string;
  readonly notification?: ServerNotification__CodexErrorInfo;
  readonly willRetry?: boolean;
  readonly replacementNotification?: boolean;
  readonly completion?: ServerNotification__CodexErrorInfo;
  readonly replacementMessage?: boolean;
  readonly child?: boolean;
  readonly localFailure?: boolean;
  readonly status?: "failed" | "completed" | "interrupted";
  readonly absentService?: boolean;
  readonly expected: "usage_limit" | "unavailable" | null;
}
const nativeThreadId = "native-failover-evidence";
const nativeTurnId = "native-failover-turn";
const message = "Provider stopped this request.";

const notification = (
  turnId: string,
  errorMessage: string,
  info: ServerNotification__CodexErrorInfo | undefined,
  willRetry = false,
): CodexReplay.CodexAppServerReplayEntry => ({
  type: "emit_inbound",
  frame: {
    method: "error",
    params: {
      threadId: nativeThreadId,
      turnId,
      willRetry,
      error: {
        message: errorMessage,
        ...(info === undefined ? {} : { codexErrorInfo: info }),
        additionalDetails: null,
      },
    },
  },
});
const completion = (
  turnId: string,
  errorMessage: string,
  info: ServerNotification__CodexErrorInfo | undefined,
  status: "failed" | "completed" | "interrupted" = "failed",
): CodexReplay.CodexAppServerReplayEntry => ({
  type: "emit_inbound",
  frame: {
    method: "turn/completed",
    params: {
      threadId: nativeThreadId,
      turn: {
        ...makeCodexReplayTurn({ id: turnId, status }),
        error: { message: errorMessage, ...(info === undefined ? {} : { codexErrorInfo: info }) },
      },
    },
  },
});

const openHarness = Effect.fn("CodexFailoverEvidence.adapter.openHarness")(function* (
  entries: ReadonlyArray<CodexReplay.CodexAppServerReplayEntry>,
  absentService = false,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
  yield* migration;
  const recorder = yield* Evidence.CodexFailoverEvidence.pipe(Effect.provide(Evidence.layer));
  const serverConfig = yield* makeReplayServerConfig("failover-evidence");
  const fileSystem = yield* FileSystem.FileSystem;
  yield* Effect.addFinalizer(() =>
    fileSystem.remove(serverConfig.baseDir, { recursive: true }).pipe(Effect.orDie),
  );
  const transcript = makeCodexReplayTranscript({ scenario: "failover-evidence", entries });
  const recorded: Array<Evidence.CodexFailoverEvidenceInput> = [];
  const adapter = yield* CodexAdapterV2.makeCodexAdapterV2({
    instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
    settings: DEFAULT_CODEX_SETTINGS,
    environment: {},
    clientFactory: makeCodexReplayClientFactory({ transcript }),
    crypto: yield* Crypto.Crypto,
    fileSystem,
    idAllocator: yield* IdAllocator.IdAllocatorV2,
    serverConfig,
    ...(absentService
      ? {}
      : {
          failoverEvidence: {
            record: (input: Evidence.CodexFailoverEvidenceInput) =>
              Effect.sync(() => recorded.push(input)).pipe(Effect.andThen(recorder.record(input))),
          },
        }),
  });
  const threadId = ThreadId.make("thread-failover-evidence");
  const runtime = yield* adapter.openSession({
    threadId,
    providerSessionId: ProviderSessionId.make("session-failover-evidence"),
    modelSelection: CODEX_TEST_MODEL_SELECTION,
    runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
  });
  const providerThread = yield* runtime.ensureThread({
    threadId,
    modelSelection: CODEX_TEST_MODEL_SELECTION,
    runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
  });
  const turnInput = makeCodexTestTurnInput({
    threadId,
    providerThread,
    now: yield* DateTime.now,
    attemptId: RunAttemptId.make("attempt-failover-evidence"),
    text: "Continue.",
  });
  yield* sql`INSERT INTO orchestration_v2_projection_runs
    (run_id, thread_id, ordinal, provider, provider_instance_id, provider_thread_id,
      status, requested_at, payload_json)
    VALUES (${turnInput.runId}, ${threadId}, 1, 'codex', ${providerThread.providerInstanceId},
      ${providerThread.id}, 'running', '2026-10-26T20:00:00.000Z', '{}')`;
  yield* sql`INSERT INTO orchestration_v2_projection_run_attempts
    (attempt_id, thread_id, run_id, attempt_ordinal, root_node_id, provider,
      provider_instance_id, provider_thread_id, status, payload_json)
    VALUES (${turnInput.attemptId}, ${threadId}, ${turnInput.runId}, 1, ${turnInput.rootNodeId}, 'codex',
      ${providerThread.providerInstanceId}, ${providerThread.id}, 'running', '{}')`;
  const terminal = yield* Deferred.make<{
    readonly event: Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }>;
    readonly rows: ReadonlyArray<Record<string, unknown>>;
  }>();
  const retryReady = yield* Queue.unbounded<void>();
  const providerTurnIds: Array<string> = [];
  yield* runtime.events.pipe(
    Stream.runForEach((event) =>
      Effect.gen(function* () {
        if (event.type === "provider_turn.updated") providerTurnIds.push(event.providerTurn.id);
        if (
          event.type === "turn_item.updated" &&
          event.turnItem.type === "error" &&
          event.turnItem.status === "running"
        )
          yield* Queue.offer(retryReady, undefined);
        if (event.type === "turn.terminal") {
          const rows = yield* sql`SELECT * FROM jones_codex_failover_evidence`;
          yield* Deferred.succeed(terminal, { event, rows });
        }
      }),
    ),
    Effect.forkScoped,
  );
  return {
    runtime,
    turnInput,
    terminal: Deferred.await(terminal),
    retryReady,
    recorded,
    providerTurnIds,
  };
});

const dependencies = Layer.mergeAll(
  IdAllocator.layer,
  McpProviderSessions.layer,
  NodeServices.layer,
  ControlAdmission.layer,
  NodeSqliteClient.layer({ filename: ":memory:" }),
);
const scenarios: ReadonlyArray<Scenario> = [
  { name: "completion only", completion: "usageLimitExceeded", expected: "usage_limit" },
  { name: "notification only", notification: "usageLimitExceeded", expected: "usage_limit" },
  {
    name: "notification 503",
    notification: { responseStreamDisconnected: { httpStatusCode: 503 } },
    expected: "unavailable",
  },
  {
    name: "completion 401 wins",
    notification: "usageLimitExceeded",
    completion: { httpConnectionFailed: { httpStatusCode: 401 } },
    expected: null,
  },
  {
    name: "completion other wins",
    notification: "usageLimitExceeded",
    completion: "other",
    expected: null,
  },
  {
    name: "replacement completion unknown",
    notification: "usageLimitExceeded",
    replacementMessage: true,
    expected: null,
  },
  {
    name: "replacement notification unknown",
    notification: "usageLimitExceeded",
    replacementNotification: true,
    expected: null,
  },
  {
    name: "retrying notification ignored",
    notification: "usageLimitExceeded",
    willRetry: true,
    expected: null,
  },
  {
    name: "completed terminal ignored",
    notification: "usageLimitExceeded",
    status: "completed",
    expected: null,
  },
  {
    name: "interrupted terminal ignored",
    notification: "usageLimitExceeded",
    status: "interrupted",
    expected: null,
  },
  {
    name: "local failure ignored",
    notification: "usageLimitExceeded",
    localFailure: true,
    expected: null,
  },
  {
    name: "child turns continue",
    notification: "usageLimitExceeded",
    child: true,
    expected: "usage_limit",
  },
  {
    name: "absent service fails closed",
    completion: "usageLimitExceeded",
    absentService: true,
    expected: null,
  },
];

it.effect.each(scenarios)("records $name before the terminal is observable", (scenario) =>
  Effect.gen(function* () {
    const entries = codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Continue." });
    if (scenario.child)
      entries.push(
        {
          type: "emit_inbound",
          frame: {
            method: "item/completed",
            params: {
              threadId: nativeThreadId,
              turnId: nativeTurnId,
              item: {
                type: "subAgentActivity",
                id: "child-spawn",
                kind: "started",
                agentThreadId: "native-child",
                agentPath: "/root/child",
              },
            },
          },
        },
        {
          type: "emit_inbound",
          frame: {
            method: "turn/started",
            params: {
              threadId: "native-child",
              turn: makeCodexReplayTurn({ id: "native-child-turn", status: "inProgress" }),
            },
          },
        },
      );
    if (scenario.notification !== undefined)
      entries.push(notification(nativeTurnId, message, scenario.notification, scenario.willRetry));
    if (scenario.replacementNotification)
      entries.push(notification(nativeTurnId, message, undefined));
    entries.push(
      scenario.localFailure
        ? { type: "runtime_exit", status: "success" }
        : completion(
            nativeTurnId,
            scenario.replacementMessage ? "replacement" : message,
            scenario.completion,
            scenario.status,
          ),
    );
    const harness = yield* openHarness(entries, scenario.absentService);
    yield* harness.runtime.startTurn(harness.turnInput);
    const { event, rows } = yield* harness.terminal;
    assert.lengthOf(rows, scenario.expected === null ? 0 : 1);
    if (scenario.expected !== null) {
      assert.equal(event.status, "failed");
      assert.deepEqual(rows[0], {
        provider_turn_id: event.providerTurnId,
        thread_id: harness.turnInput.threadId,
        run_id: harness.turnInput.runId,
        attempt_id: harness.turnInput.attemptId,
        provider_thread_id: harness.turnInput.providerThread.id,
        provider_instance_id: harness.turnInput.providerThread.providerInstanceId,
        disposition: scenario.expected,
        occurred_at: harness.recorded[0]!.occurredAt,
      });
      assert.equal(harness.recorded[0]!.providerTurnId, event.providerTurnId);
    }
    if (scenario.child && event.status === "failed") assert.isTrue(event.childTurnsContinue);
    if (scenario.localFailure) assert.equal(event.evidenceKind, "local_failure");
    if (scenario.absentService || scenario.localFailure || scenario.status !== undefined)
      assert.lengthOf(harness.recorded, 0);
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

it.effect(
  "records exhausted capacity with code other on the logical turn after promptless retries",
  () =>
    Effect.gen(function* () {
      const capacityMessage = "Selected model is at capacity. Please try a different model.";
      const entries = codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Continue." });
      for (let retry = 0; retry <= codexCapacityRetryPolicy.maxRetries; retry++) {
        const turnId = retry === 0 ? nativeTurnId : `capacity-retry-${retry}`;
        if (retry > 0) {
          entries.push(
            {
              type: "expect_outbound",
              frame: {
                id: 3 + retry,
                method: "turn/start",
                params: {
                  threadId: nativeThreadId,
                  input: [],
                  cwd: "/workspace",
                  model: "gpt-5.4",
                  collaborationMode: { mode: "default", settings: { model: "gpt-5.4" } },
                  approvalPolicy: "never",
                  approvalsReviewer: "user",
                  sandboxPolicy: { type: "dangerFullAccess" },
                  summary: "detailed",
                },
              },
            },
            {
              type: "emit_inbound",
              frame: {
                id: 3 + retry,
                result: { turn: makeCodexReplayTurn({ id: turnId, status: "inProgress" }) },
              },
            },
            {
              type: "emit_inbound",
              frame: {
                method: "turn/started",
                params: {
                  threadId: nativeThreadId,
                  turn: makeCodexReplayTurn({ id: turnId, status: "inProgress" }),
                },
              },
            },
          );
        }
        entries.push(
          notification(turnId, capacityMessage, "other"),
          completion(turnId, capacityMessage, "other"),
        );
      }
      const harness = yield* openHarness(entries);
      yield* harness.runtime.startTurn(harness.turnInput);
      for (let retry = 0; retry < codexCapacityRetryPolicy.maxRetries; retry++) {
        yield* Queue.take(harness.retryReady);
        yield* TestClock.adjust(codexCapacityRetryPolicy.delayMs);
      }
      const { event, rows } = yield* harness.terminal;
      assert.equal(event.status, "failed");
      assert.lengthOf(rows, 1);
      assert.equal(rows[0]!.provider_turn_id, event.providerTurnId);
      assert.equal(event.providerTurnId, harness.providerTurnIds[0]);
      assert.equal(
        event.providerTurn?.nativeTurnRef?.nativeId,
        `capacity-retry-${codexCapacityRetryPolicy.maxRetries}`,
      );
      assert.equal(rows[0]!.disposition, "unavailable");
      assert.lengthOf(harness.recorded, 1);
      assert.isTrue(harness.recorded[0]!.capacityExhausted);
      assert.equal(harness.recorded[0]!.rawErrorInfo, "other");
      assert.equal(harness.recorded[0]!.attemptId, harness.turnInput.attemptId);
    }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

it.effect("the recorder blocks contrary HTTP info even when passed capacityExhausted", () =>
  Effect.gen(function* () {
    const harness = yield* openHarness([
      ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Continue." }),
      completion(nativeTurnId, message, "usageLimitExceeded"),
    ]);
    yield* harness.runtime.startTurn(harness.turnInput);
    yield* harness.terminal;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`DELETE FROM jones_codex_failover_evidence`;
    const recorder = yield* Evidence.make;
    for (const variant of [
      "httpConnectionFailed",
      "responseStreamConnectionFailed",
      "responseStreamDisconnected",
      "responseTooManyFailedAttempts",
    ] as const) {
      for (const httpStatusCode of [400, 401, 403]) {
        yield* recorder.record({
          ...harness.recorded[0]!,
          capacityExhausted: true,
          rawErrorInfo: { [variant]: { httpStatusCode } } as ServerNotification__CodexErrorInfo,
        });
      }
    }
    assert.deepEqual(yield* sql`SELECT * FROM jones_codex_failover_evidence`, []);
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);
