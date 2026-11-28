import packageJson from "../../../package.json" with { type: "json" };
import * as Deferred from "effect/Deferred";
import * as ControlAdmission from "../../jones/orchestration/ControlAdmission.ts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { CodexSettings, type ProviderReplayTranscript } from "@t3tools/contracts";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexError from "effect-codex-app-server/errors";
import * as CodexReplay from "effect-codex-app-server/replay";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

import { materializeCodexReplayAppIdentity } from "../../jones/runtimeIdentity/CodexReplayIdentity.ts";
import { codexStartReplyObservationOptions } from "../../jones/provider/codex/CodexLateStartRecovery.ts";

import * as ServerConfig from "../../config.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { ProviderAdapterDriverCreateError } from "@t3tools/provider-core/server/adapterDriver";
import * as PortScanner from "../../preview/PortScanner.ts";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import {
  makeReplayServerConfig,
  type OrchestratorV2ProviderReplayHarness,
} from "../testkit/ProviderReplayHarness.ts";
import type { ProviderReplayGate } from "@t3tools/provider-testing/replayGate";
import * as CodexAdapterV2 from "./CodexAdapterV2.ts";

export { makeReplayServerConfig } from "../testkit/ProviderReplayHarness.ts";

const DEFAULT_CODEX_SETTINGS = Schema.decodeSync(CodexSettings)({});

export class CodexReplayTranscriptDecodeError extends Schema.TaggedError<CodexReplayTranscriptDecodeError>()(
  "CodexReplayTranscriptDecodeError",
  {
    driver: Schema.optional(Schema.String),
    protocol: Schema.optional(Schema.String),
    scenario: Schema.optional(Schema.String),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to decode Codex app-server replay transcript for scenario ${this.scenario ?? "<unknown>"}.`;
  }
}

export const CodexOrchestratorReplayHarnessError = Schema.Union([
  CodexReplayTranscriptDecodeError,
  CodexReplay.CodexAppServerReplayError,
  ProviderAdapterDriverCreateError,
]);
export type CodexOrchestratorReplayHarnessError = typeof CodexOrchestratorReplayHarnessError.Type;

export function withCodexReplayChildMetadata(
  client: CodexClient.CodexAppServerClient["Service"],
  transcript: CodexReplay.CodexAppServerReplayTranscript,
  readMetadata: (
    threadId: string,
    method: "thread/read" | "thread/resume",
  ) => Effect.Effect<unknown, CodexError.CodexAppServerError> = (threadId) =>
    Effect.succeed({ thread: { id: threadId }, model: null }),
): CodexClient.CodexAppServerClient["Service"] {
  const childThreadIds = new Set(
    transcript.entries.flatMap((entry) => {
      if (entry.type !== "emit_inbound" || !Predicate.isObject(entry.frame)) return [];
      const params = entry.frame.params;
      if (!Predicate.isObject(params) || !Predicate.isObject(params.item)) return [];
      const item = params.item;
      if (item.type === "subAgentActivity" && typeof item.agentThreadId === "string") {
        return [item.agentThreadId];
      }
      return item.type === "collabAgentToolCall" && Array.isArray(item.receiverThreadIds)
        ? item.receiverThreadIds.filter(Predicate.isString)
        : [];
    }),
  );
  return {
    ...client,
    raw: {
      ...client.raw,
      request: (method, params) =>
        method === "server/diagnostics"
          ? Effect.fail(
              new CodexError.CodexAppServerRequestError({
                code: -32601,
                errorMessage: "Synthetic diagnostics unsupported",
              }),
            )
          : (method === "thread/read" || method === "thread/resume") &&
              Predicate.isObject(params) &&
              (method === "thread/read"
                ? params.includeTurns === false
                : params.excludeTurns === true) &&
              typeof params.threadId === "string" &&
              childThreadIds.has(params.threadId)
            ? readMetadata(params.threadId, method)
            : client.raw.request(method, params),
    },
  };
}

function metadataFromTranscript(transcript: ProviderReplayTranscript): {
  readonly provider?: string;
  readonly protocol?: string;
  readonly scenario?: string;
} {
  return {
    provider: transcript.provider,
    protocol: transcript.protocol,
    scenario: transcript.scenario,
  };
}

export function makeCodexReplayClientFactory(input: {
  readonly transcript: CodexReplay.CodexAppServerReplayTranscript;
  readonly driver?: CodexReplay.CodexAppServerReplayDriver;
  readonly observeFrame?: (frame: unknown) => void;
  readonly observeOutgoingFrame?: (frame: unknown) => void;
}): CodexAdapterV2.CodexAppServerClientFactoryShape {
  const replayLayer = (options: CodexClient.CodexAppServerClientOptions) =>
    input.driver === undefined
      ? CodexReplay.layerReplay(input.transcript, options)
      : CodexReplay.layerReplayWithDriver(input.driver, options);
  const recordedGoalControl = input.transcript.entries.some(
    (entry) =>
      entry.type === "expect_outbound" &&
      Predicate.isObject(entry.frame) &&
      (entry.frame.method === "thread/goal/get" || entry.frame.method === "thread/goal/set"),
  );
  return {
    open: (openInput) =>
      Effect.gen(function* () {
        const processExited = yield* Deferred.make<void>();
        openInput.captureProcessExit?.(Deferred.await(processExited));
        yield* Effect.addFinalizer(() => Deferred.succeed(processExited, undefined));
        const context = yield* Layer.build(
          replayLayer({
            ...codexStartReplyObservationOptions(
              (frame) => {
                openInput.observeDecodedReply?.(frame);
                input.observeFrame?.(frame);
              },
              input.observeOutgoingFrame === undefined
                ? undefined
                : (event) =>
                    Effect.sync(() => {
                      if (event.direction === "outgoing" && event.stage === "decoded")
                        input.observeOutgoingFrame?.(event.payload);
                    }),
            ),
            ...(openInput.onTermination === undefined
              ? {}
              : { onTermination: openInput.onTermination }),
          }),
        ).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                providerSessionId: openInput.providerSessionId,
                cause,
              }),
          ),
        );
        return yield* Effect.service(CodexClient.CodexAppServerClient).pipe(
          Effect.map((replayClient): CodexClient.CodexAppServerClient["Service"] => {
            const client = withCodexReplayChildMetadata(replayClient, input.transcript);
            return {
              ...client,
              raw: {
                ...client.raw,
                request: (method, params) =>
                  // Older fixtures model no active goal. Recorded goal control and all writes
                  // stay on strict replay so compatibility cannot hide an unexpected mutation.
                  !recordedGoalControl && method === "thread/goal/get"
                    ? Effect.succeed({ goal: null })
                    : client.raw.request(method, params),
              },
            };
          }),
          Effect.provide(context),
        );
      }),
  };
}

export function layer(
  input: Parameters<typeof makeCodexReplayClientFactory>[0] & {
    readonly onSessionOpened?: (runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime) => void;
    readonly onTurnStartReturned?: () => void;
  },
) {
  const layerReplayClientFactory = Layer.succeed(
    CodexAdapterV2.CodexAppServerClientFactory,
    makeCodexReplayClientFactory(input),
  );
  const layerServerConfig = Layer.effect(
    ServerConfig.ServerConfig,
    makeReplayServerConfig(input.transcript.scenario).pipe(Effect.orDie),
  ).pipe(Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)));
  const layerRegistry = ProviderAdapterRegistry.layerFromAdaptersEffect(
    CodexAdapterV2.createCodexAdapterV2({
      instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
      displayName: undefined,
      environment: [],
      enabled: true,
      config: DEFAULT_CODEX_SETTINGS,
    }).pipe(
      Effect.map((adapter) => [
        input.onSessionOpened === undefined && input.onTurnStartReturned === undefined
          ? adapter
          : {
              ...adapter,
              openSession: (value: ProviderAdapter.ProviderAdapterV2OpenSessionInput) =>
                adapter.openSession(value).pipe(
                  Effect.tap((runtime) => Effect.sync(() => input.onSessionOpened?.(runtime))),
                  Effect.map((runtime) =>
                    input.onTurnStartReturned === undefined
                      ? runtime
                      : {
                          ...runtime,
                          get providerSession() {
                            return runtime.providerSession;
                          },
                          startTurn: (value: ProviderAdapter.ProviderAdapterV2TurnInput) =>
                            runtime
                              .startTurn(value)
                              .pipe(Effect.tap(() => Effect.sync(input.onTurnStartReturned!))),
                        },
                  ),
                ),
            },
      ]),
    ),
  ).pipe(
    Layer.provide(ControlAdmission.layer),
    Layer.provide(
      Layer.mergeAll(
        layerReplayClientFactory,
        layerServerConfig,
        NodeServices.layer,
        NodeCrypto.layer,
        IdAllocator.layer,
        PortScanner.layerTest,
      ),
    ),
  );

  return layerRegistry;
}

const decodeCodexAppServerReplayTranscript = Schema.decodeUnknownEffect(
  CodexReplay.CodexAppServerReplayTranscript,
);

export const CodexOrchestratorReplayHarness: OrchestratorV2ProviderReplayHarness<
  CodexReplay.CodexAppServerReplayTranscript,
  CodexOrchestratorReplayHarnessError
> = {
  driver: CodexAdapterV2.CODEX_DRIVER_KIND,
  decodeTranscript: (transcript) =>
    decodeCodexAppServerReplayTranscript(materializeCodexReplayAppIdentity(transcript)).pipe(
      Effect.mapError(
        (cause) =>
          new CodexReplayTranscriptDecodeError({
            ...metadataFromTranscript(transcript),
            cause,
          }),
      ),
    ),
  makeProviderAdapterRegistryLayer: (
    transcript,
    options: { readonly replayGate?: ProviderReplayGate } = {},
  ) => {
    return Layer.effectContext(
      Effect.gen(function* () {
        const replayGate = options.replayGate;
        if (replayGate !== undefined) {
          yield* Effect.addFinalizer(() => Effect.sync(() => replayGate.releaseAll()));
        }
        const driver = yield* CodexReplay.makeReplayDriver(
          transcript,
          replayGate === undefined
            ? {}
            : {
                beforeEmitInbound: (entry) =>
                  Effect.promise((signal) => replayGate.beforeEmit(entry.label, signal)),
              },
        );
        return yield* Layer.build(layer({ transcript, driver }));
      }),
    );
  },
};

export function makeCodexReplayTurn(input: {
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

export function codexReplayPreamble(input: {
  readonly nativeThreadId: string;
  readonly nativeTurnId: string;
  readonly prompt: string;
  readonly cwd?: string;
  readonly sentPrompt?: string;
  readonly effort?: string;
  readonly turnPolicy?: Pick<
    typeof CodexClient.V2TurnStartParams.Type,
    "approvalPolicy" | "sandboxPolicy"
  >;
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
            projectId: null,
            forkedFromId: null,
            preview: "",
            ephemeral: false,
            modelProvider: "openai",
            createdAt: 1782622440,
            updatedAt: 1782622440,
            status: { type: "idle" },
            path: `/tmp/${input.nativeThreadId}.jsonl`,
            cwd: input.cwd ?? "/workspace",
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
          cwd: input.cwd ?? "/workspace",
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
          cwd: input.cwd ?? "/workspace",
          model: "gpt-5.4",
          approvalPolicy: "never",
          approvalsReviewer: "user",
          sandboxPolicy: { type: "dangerFullAccess" },
          ...input.turnPolicy,
          summary: "auto",
          ...(input.effort === undefined ? {} : { effort: input.effort }),
          collaborationMode: {
            mode: "default",
            settings: {
              model: "gpt-5.4",
              ...(input.effort === undefined ? {} : { reasoning_effort: input.effort }),
            },
          },
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

export const CODEX_TEST_THREAD_TITLE = "Codex continuation test";

export const REPLAY_NATIVE_OPEN_RETRY = "retry native conversation open";

/** Add the setup exchanges for logical thread opens and turn starts, keeping request IDs paired. */
export function withCodexSetupExchanges(
  entries: ReadonlyArray<CodexReplay.CodexAppServerReplayEntry>,
  threadName = CODEX_TEST_THREAD_TITLE,
): Array<CodexReplay.CodexAppServerReplayEntry> {
  const result: Array<CodexReplay.CodexAppServerReplayEntry> = [];
  let offset = 0;
  let clientIds = new Map<number, number>();
  let startedThreads = new Set<unknown>();
  let namedThreads = new Set<unknown>();
  let previousMethod: string | undefined;
  for (const entry of entries) {
    const frame = entry.type === "runtime_exit" ? undefined : entry.frame;
    if (!Predicate.isObject(frame) || typeof frame.id !== "number") {
      result.push(entry);
      continue;
    }
    if (entry.type === "expect_outbound" && typeof frame.method === "string") {
      if (frame.method === "initialize") {
        offset = 0;
        clientIds = new Map();
        startedThreads = new Set();
        namedThreads = new Set();
      }
      const params = Predicate.isObject(frame.params) ? frame.params : {};
      if (frame.method === "thread/name/set") namedThreads.add(params.threadId);
      // Metadata-only resumes and a native retry with the same parameters reuse existing config.
      const threadOpen =
        (frame.method === "thread/start" ||
          frame.method === "thread/resume" ||
          frame.method === "thread/fork") &&
        "config" in params &&
        previousMethod !== "config/read" &&
        entry.label !== REPLAY_NATIVE_OPEN_RETRY;
      // Capacity retries reuse the original turn parameters and omit its input.
      const capacityRetry =
        frame.method === "turn/start" &&
        Array.isArray(params.input) &&
        params.input.length === 0 &&
        startedThreads.has(params.threadId);
      if (frame.method === "turn/start") startedThreads.add(params.threadId);
      const turnStart =
        frame.method === "turn/start" && !capacityRetry && previousMethod !== "config/read";
      const exchanges: Array<readonly [string, unknown, unknown]> = [];
      if (turnStart && !namedThreads.has(params.threadId)) {
        namedThreads.add(params.threadId);
        exchanges.push(["thread/name/set", { threadId: params.threadId, name: threadName }, {}]);
      }
      if (threadOpen || turnStart) {
        exchanges.push([
          "config/read",
          { ...(params.cwd === undefined ? {} : { cwd: params.cwd }), includeLayers: false },
          { config: {}, origins: {} },
        ]);
      }
      for (const [method, params, response] of exchanges) {
        const id = frame.id + offset;
        result.push(
          { type: "expect_outbound", label: method, frame: { id, method, params } },
          { type: "emit_inbound", label: method, frame: { id, result: response } },
        );
        offset += 1;
      }
      previousMethod = frame.method;
      clientIds.set(frame.id, frame.id + offset);
      result.push(withReplayRequestId(entry, frame.id + offset));
      continue;
    }
    const mapped = clientIds.get(frame.id);
    result.push(
      entry.type === "emit_inbound" && frame.method === undefined && mapped !== undefined
        ? withReplayRequestId(entry, mapped)
        : entry,
    );
  }
  return result;
}

export function withCodexSetup(
  transcript: CodexReplay.CodexAppServerReplayTranscript,
  threadName?: string,
): CodexReplay.CodexAppServerReplayTranscript {
  return { ...transcript, entries: withCodexSetupExchanges(transcript.entries, threadName) };
}

const decodeMaterializedTranscript = Schema.decodeUnknownSync(
  CodexReplay.CodexAppServerReplayTranscript,
);

// The adapter sends app identity on every turn/start without a T3 MCP session; the
// replay still compares exact frames, so fixtures that omit it gain the current value.
export function makeCodexReplayTranscript(input: {
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

export function withReplayRequestId(
  entry: CodexReplay.CodexAppServerReplayEntry,
  id: number,
): CodexReplay.CodexAppServerReplayEntry {
  return entry.type !== "runtime_exit" && Predicate.isObject(entry.frame)
    ? { ...entry, frame: { ...entry.frame, id } }
    : entry;
}

export const producerCompletion = (
  threadId: string,
  id: string,
): CodexReplay.CodexAppServerReplayEntry => ({
  type: "emit_inbound",
  frame: {
    method: "turn/completed",
    params: {
      threadId,
      turn: makeCodexReplayTurn({ id, status: "completed" }),
    },
  },
});
