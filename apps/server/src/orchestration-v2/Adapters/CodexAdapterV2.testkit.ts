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

import * as ServerConfig from "../../config.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { ProviderAdapterDriverCreateError } from "@t3tools/provider-core/server/adapterDriver";
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
          replayLayer(
            openInput.onTermination === undefined ? {} : { onTermination: openInput.onTermination },
          ),
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

function makeCodexProviderAdapterRegistryReplayLayer(input: {
  readonly transcript: CodexReplay.CodexAppServerReplayTranscript;
  readonly driver?: CodexReplay.CodexAppServerReplayDriver;
}) {
  const replayClientFactoryLayer = Layer.succeed(
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
    }).pipe(Effect.map((adapter) => [adapter])),
  ).pipe(
    Layer.provide(
      Layer.mergeAll(
        replayClientFactoryLayer,
        layerServerConfig,
        NodeServices.layer,
        NodeCrypto.layer,
        IdAllocator.layer,
        ControlAdmission.layer,
      ),
    ),
  );

  return layerRegistry;
}

export const layer = makeCodexProviderAdapterRegistryReplayLayer;

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
