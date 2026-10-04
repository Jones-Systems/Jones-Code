import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { randomUUID } from "node:crypto";
import {
  ModelSelection,
  OrchestrationV2DomainEvent,
  OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  OrchestrationV2RuntimeRequest,
  ProviderInstanceId,
  ProviderSessionId,
  type ProviderThreadId,
  type ProviderTurnId,
  type RunId,
  type RunAttemptId,
  type ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { ProviderWorkspaceMissingError } from "../provider/Errors.ts";
import * as ServerConfig from "../config.ts";
import * as DeviceService from "../device/DeviceService.ts";
import { ensureAgentDeviceShim } from "../device/AgentDeviceShim.ts";
import * as ProjectService from "../project/ProjectService.ts";
import { NativeCreationRepository } from "../persistence/Services/NativeCreationRepository.ts";
import * as McpProviderSession from "../mcp/McpProviderSession.ts";
import * as ServerSettings from "../serverSettings.ts";
import { getCodexServiceTierOptionValue } from "../codexModelOptions.ts";
import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "./IdAllocator.ts";
import { makeKeyedSerialExecutor } from "./KeyedSerialExecutor.ts";
import { getNativeCreationExecutionReference } from "./NativeCreationAuthority.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import { readProviderEventOrigin } from "./ProviderEventOrigin.ts";
import {
  ProviderAdapterEventStreamError,
  ProviderAdapterOpenSessionError,
  ProviderAdapterProtocolError,
  ProviderAdapterTurnStartError,
  ProviderAdapterV2RuntimePolicy,
  withProviderNativeEffect,
  type ProviderNativeOperationContext,
  type ProviderNativeEffectOperation,
  type ProviderRuntimeBinding,
  type ProviderRuntimeObservation,
  type ProviderPendingStartStopInput,
  type ProviderPendingStartStopResult,
  type ProviderAdapterV2OpenSessionInput,
  type ProviderAdapterV2TurnInput,
  type ProviderAdapterV2Error,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2EventSubscription,
  type ProviderAdapterV2SessionRuntime,
} from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import { nativeEffectEvidenceFor, ProviderNativeOperationUnknownError } from "./ProviderFailure.ts";
import {
  providerThreadActivityObservation,
  ProviderOperatingCountsError,
  type ProviderOperatingCounts,
  type ProviderThreadRuntimeAttachment,
} from "./ProviderThreadRuntimeObservation.ts";

const DEFAULT_IDLE_TIMEOUT_MS = 30 * 60 * 1000;
const DEFAULT_MAX_IDLE_PIN_MS = 4 * 60 * 60 * 1000;
const RELEASE_SCOPE_CLOSE_TIMEOUT_MS = 30 * 1000;
const UNLOAD_THREAD_TIMEOUT_MS = 10 * 1000;

export const ProviderSessionReleaseReason = Schema.Literals([
  "idle_timeout",
  "runtime_error",
  "manual_shutdown",
  "server_shutdown",
]);
export type ProviderSessionReleaseReason = typeof ProviderSessionReleaseReason.Type;

/**
 * ProviderSessionManager owns live session residency: open sessions, idle release,
 * explicit shutdown, and release-on-runtime-failure.
 *
 * Persisted rows do not prove a live runtime. Process-loss recovery preserves
 * unresolved native effects for reconciliation; eligible commands open lazily.
 */
export class ProviderSessionOpenError extends Schema.TaggedError<ProviderSessionOpenError>()(
  "ProviderSessionOpenError",
  {
    instanceId: ProviderInstanceId,
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to open provider instance ${this.instanceId} session ${this.providerSessionId}.`;
  }
}

export class ProviderSessionLookupError extends Schema.TaggedError<ProviderSessionLookupError>()(
  "ProviderSessionLookupError",
  {
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to look up provider session ${this.providerSessionId}.`;
  }
}

export class ProviderSessionCloseError extends Schema.TaggedError<ProviderSessionCloseError>()(
  "ProviderSessionCloseError",
  {
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to close provider session ${this.providerSessionId}.`;
  }
}

export class ProviderSessionReleaseError extends Schema.TaggedError<ProviderSessionReleaseError>()(
  "ProviderSessionReleaseError",
  {
    providerSessionId: ProviderSessionId,
    reason: ProviderSessionReleaseReason,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to release provider session ${this.providerSessionId}.`;
  }
}

export class ProviderSessionActivityError extends Schema.TaggedError<ProviderSessionActivityError>()(
  "ProviderSessionActivityError",
  {
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to update provider session activity for ${this.providerSessionId}.`;
  }
}

class RuntimeReplacementAttachmentError extends Schema.TaggedError<RuntimeReplacementAttachmentError>()(
  "RuntimeReplacementAttachmentError",
  { providerSessionId: ProviderSessionId },
) {}

export const ProviderSessionManagerV2Error = Schema.Union([
  ProviderSessionOpenError,
  ProviderWorkspaceMissingError,
  ProviderSessionLookupError,
  ProviderSessionCloseError,
  ProviderSessionReleaseError,
  ProviderSessionActivityError,
]);
export type ProviderSessionManagerV2Error = typeof ProviderSessionManagerV2Error.Type;

export interface ProviderPinnedRuntimeStopInputV1 {
  readonly operationId: string;
  readonly binding: EventSink.ProviderBindingExpectationV2 & { readonly runtimeGeneration: string };
  readonly expectedEvidenceRevision: number;
  readonly deletionBindingSha256?: string;
}

export type ProviderPinnedRuntimeStopResultV1 =
  | {
      readonly status: "stopped";
      readonly operationId: string;
      readonly binding: ProviderPinnedRuntimeStopInputV1["binding"];
      readonly cancelledPendingStart: boolean;
      readonly interruptedProviderTurnIds: ReadonlyArray<ProviderTurnId>;
      readonly readback: { readonly threadAttached: false };
    }
  | { readonly status: "unknown"; readonly reason: string };

export interface ProviderSessionManagerV2Shape {
  readonly isMcpCallerAttached: (input: {
    readonly threadId: ThreadId;
    readonly providerSessionId: ProviderSessionId;
    readonly providerInstanceId: ProviderInstanceId;
    readonly mcpCredentialId: string;
  }) => Effect.Effect<boolean>;

  readonly shutdown: Effect.Effect<void>;
  readonly open: (input: {
    readonly threadId: ThreadId;
    readonly providerSessionId: ProviderSessionId;
    readonly modelSelection: ModelSelection;
    readonly runtimePolicy: ProviderAdapterV2RuntimePolicy;
    readonly resumeFromSession?: OrchestrationV2ProviderSession;
    readonly initialNativeThreadId?: string;
    readonly initialProviderItemIdentityVersion?: 2;
    readonly nativeOperation?: ProviderNativeOperationContext;
    readonly nativeCreationExecution?: ProviderAdapterV2OpenSessionInput["nativeCreationExecution"];
  }) => Effect.Effect<ProviderAdapterV2SessionRuntime, ProviderSessionManagerV2Error>;
  readonly get: (
    providerSessionId: ProviderSessionId,
  ) => Effect.Effect<Option.Option<ProviderAdapterV2SessionRuntime>, ProviderSessionManagerV2Error>;
  readonly observeThreadRuntime: (
    binding: ProviderRuntimeBinding,
  ) => Effect.Effect<ProviderRuntimeObservation>;
  readonly registerRuntimeBinding: (input: {
    readonly threadId: ThreadId;
    readonly providerSessionId: ProviderSessionId;
    readonly providerThreadId: ProviderThreadId;
    readonly runId?: RunId;
    readonly attemptId?: RunAttemptId;
  }) => Effect.Effect<void, ProviderSessionActivityError>;
  readonly onNativeEffectConfirmed: (
    input: Parameters<NonNullable<ProviderAdapterV2SessionRuntime["onNativeEffectConfirmed"]>>[0],
  ) => Effect.Effect<void, ProviderSessionActivityError>;
  readonly observeCurrentThreadRuntime: (
    threadId: ThreadId,
  ) => Effect.Effect<ProviderRuntimeObservation>;
  readonly readCurrentThreadRuntimeAttachment: (
    threadId: ThreadId,
  ) => Effect.Effect<ProviderThreadRuntimeAttachment>;
  readonly getOperatingCounts: (input?: {
    readonly projectId?: ProjectId;
  }) => Effect.Effect<ProviderOperatingCounts, ProviderOperatingCountsError>;
  readonly stopPinnedRuntime: (
    input: ProviderPinnedRuntimeStopInputV1,
  ) => Effect.Effect<ProviderPinnedRuntimeStopResultV1>;
  readonly close: (
    providerSessionId: ProviderSessionId,
  ) => Effect.Effect<void, ProviderSessionManagerV2Error>;
  /** Closes every live runtime owned by one provider instance. */
  readonly closeInstance: (
    instanceId: ProviderInstanceId,
  ) => Effect.Effect<void, ProviderSessionManagerV2Error>;
  readonly release: (input: {
    readonly providerSessionId: ProviderSessionId;
    readonly reason: ProviderSessionReleaseReason;
    readonly detail?: string;
  }) => Effect.Effect<void, ProviderSessionManagerV2Error>;
  readonly detach: (input: {
    readonly providerSessionId: ProviderSessionId;
    readonly threadId: ThreadId;
    readonly detail?: string;
    readonly expectedBinding?: ProviderRuntimeBinding;
    /**
     * True for terminal detaches (thread archived or deleted): the thread's
     * MCP credentials are revoked immediately instead of surviving for a
     * potential re-attach.
     */
    readonly revokeMcpCredential?: boolean;
  }) => Effect.Effect<void, ProviderSessionManagerV2Error>;
}

export class ProviderSessionManagerV2 extends Context.Service<
  ProviderSessionManagerV2,
  ProviderSessionManagerV2Shape
>()("t3/orchestration-v2/ProviderSessionManager/ProviderSessionManagerV2") {}

interface LiveSessionEntry {
  readonly attachedThreadIds: ReadonlySet<ThreadId>;
  readonly managedStopReservation?: {
    readonly input: ProviderPinnedRuntimeStopInputV1;
    readonly terminalIds: Set<ProviderTurnId>;
    readonly terminalPublished: Deferred.Deferred<void>;
  };
  readonly runtimeReplacementReservation?:
    | { readonly nextGeneration: string }
    | {
        readonly kind: "pending_start_stop";
        readonly request: ResidentStartRequest;
        readonly terminalPublished: Deferred.Deferred<void>;
      };
  readonly startRequests: ReadonlyMap<ProviderThreadId, ResidentStartRequest>;
  readonly loadedProviderThreadKeyByThread: ReadonlyMap<ThreadId, string>;
  /**
   * MCP credential session id issued for each attached thread. Revocation on
   * detach/release is scoped to these ids so tearing down a superseded
   * session cannot revoke a replacement session's credential for the same
   * thread (the workspace-handoff sequence opens the replacement before the
   * outbox executes the old session's detach).
   */
  readonly mcpCredentialIdByThread: ReadonlyMap<ThreadId, string>;
  readonly supportsMultipleProviderThreads: boolean;
  readonly runtime: ProviderAdapterV2SessionRuntime;
  readonly exposedRuntime: ProviderAdapterV2SessionRuntime;
  readonly eventSubscribers: Ref.Ref<
    ReadonlyMap<number, Queue.Queue<ProviderSessionEventSignal, Cause.Done>>
  >;
  readonly requestEventPermit: Semaphore.Semaphore;
  readonly pendingRuntimeIdentity: Ref.Ref<
    ReadonlyMap<
      ThreadId,
      Extract<ProviderAdapterV2Event, { readonly type: "runtime_identity.observed" }>
    >
  >;
  readonly scope: Scope.Closeable;
  readonly idleGeneration: number;
  readonly busyCount: number;
  readonly lastActivityAtMs: number;
  readonly idleFiber: Fiber.Fiber<void, never> | null;
  /** Set when idle release is deferred for pending background work; bounds total deferral. */
  readonly pinnedSinceMs: number | null;
}

interface ResidentStartRequest {
  readonly threadId: ThreadId;
  readonly providerThreadId: ProviderThreadId;
  readonly runId: RunId;
  readonly runOrdinal: number;
  readonly attemptId: RunAttemptId;
  readonly startOperation: ProviderNativeOperationContext;
  readonly stopRequested: boolean;
  readonly startReturned: boolean;
  readonly providerTurnIds: ReadonlySet<
    Extract<
      ProviderAdapterV2Event,
      { readonly type: "provider_turn.updated" }
    >["providerTurn"]["id"]
  >;
}

function isPendingStartStopReservation(entry: LiveSessionEntry): boolean {
  return (
    entry.runtimeReplacementReservation !== undefined &&
    "kind" in entry.runtimeReplacementReservation
  );
}

type ProviderSessionEventSignal =
  | { readonly type: "event"; readonly event: ProviderAdapterV2Event }
  | {
      readonly type: "failure";
      readonly cause: Cause.Cause<ProviderAdapterV2Error>;
    };

export interface ProviderSessionManagerV2LayerOptions {
  readonly idleTimeoutMs?: number;
  /** Cap on how long idle release may be deferred for pending background work. */
  readonly maxIdlePinMs?: number;
  /** Test replay harnesses can omit T3's MCP server from provider protocol fixtures. */
  readonly configureMcp?: boolean;
}

function releaseStatusFor(
  reason: ProviderSessionReleaseReason,
): OrchestrationV2ProviderSession["status"] {
  return reason === "runtime_error" ? "error" : "stopped";
}

function releasedRuntimeRequestStatusFor(
  reason: ProviderSessionReleaseReason,
): OrchestrationV2RuntimeRequest["status"] {
  return reason === "manual_shutdown" || reason === "server_shutdown" ? "cancelled" : "expired";
}

function sessionKey(providerSessionId: ProviderSessionId): string {
  return String(providerSessionId);
}

/**
 * Runtime requests with no provider turn belong to the live session itself.
 * Their node and transcript item are runless too, so they bypass the normal
 * per-run subscriber and are persisted by the session event pump.
 */
function sessionScopedRuntimeRequestThreadId(event: ProviderAdapterV2Event): ThreadId | undefined {
  switch (event.type) {
    case "runtime_request.updated":
      return event.runtimeRequest.providerTurnId === null ? event.threadId : undefined;
    case "node.updated":
      return event.node.runId === null && event.node.runtimeRequestId !== null
        ? event.node.threadId
        : undefined;
    case "turn_item.updated":
      return event.turnItem.runId === null &&
        (event.turnItem.type === "approval_request" || event.turnItem.type === "user_input_request")
        ? event.turnItem.threadId
        : undefined;
    default:
      return undefined;
  }
}

function providerThreadRuntimeKey(
  providerThread: Parameters<ProviderAdapterV2SessionRuntime["resumeThread"]>[0]["providerThread"],
): string {
  const nativeThreadRef = providerThread.nativeThreadRef;
  return nativeThreadRef === null
    ? String(providerThread.id)
    : `${nativeThreadRef.driver}:${nativeThreadRef.nativeId}`;
}

function providerThreadLoadKey(input: {
  readonly providerThread: Parameters<
    ProviderAdapterV2SessionRuntime["resumeThread"]
  >[0]["providerThread"];
  readonly modelSelection?: ModelSelection;
  readonly runtimePolicy?: ProviderAdapterV2RuntimePolicy;
}): string {
  return JSON.stringify({
    providerThread: providerThreadRuntimeKey(input.providerThread),
    modelSelection: input.modelSelection ?? null,
    runtimePolicy: input.runtimePolicy ?? null,
  });
}

export const layerWithOptions = (
  options: ProviderSessionManagerV2LayerOptions = {},
): Layer.Layer<
  ProviderSessionManagerV2,
  never,
  | EventSink.EventSinkV2
  | FileSystem.FileSystem
  | IdAllocator.IdAllocatorV2
  | McpSessionRegistry.McpSessionRegistry
  | ProjectionStore.ProjectionStoreV2
  | ProviderEventIngestor.ProviderEventIngestorV2
  | ProviderAdapterRegistry.ProviderAdapterRegistryV2
> =>
  Layer.effect(
    ProviderSessionManagerV2,
    Effect.gen(function* () {
      const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
      const fileSystem = yield* FileSystem.FileSystem;
      const mcpSessionRegistry = yield* McpSessionRegistry.McpSessionRegistry;
      /**
       * Optional so the many focused tests that assemble this layer by hand do
       * not each need a settings stub; the production composition always
       * provides it. When present, an unreadable settings file withholds
       * browser access rather than granting it — an explicit "off" silently
       * becoming "on" would violate the user's stated choice, whereas the
       * reverse costs an agent one toolset and is visible immediately (#7083).
       */
      const serverSettings = yield* Effect.serviceOption(ServerSettings.ServerSettingsService);
      const projectService = yield* Effect.serviceOption(ProjectService.ProjectService);
      const serverConfig = yield* Effect.serviceOption(ServerConfig.ServerConfig);
      const pathService = yield* Effect.serviceOption(Path.Path);
      const hostPlatform = yield* HostProcessPlatform;
      const eventSink = yield* EventSink.EventSinkV2;
      const nativeRepository = yield* Effect.serviceOption(NativeCreationRepository);
      const nativeSql = yield* Effect.serviceOption(SqlClient.SqlClient);
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const providerEventIngestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const currentConfiguredModelDefault = (
        runtime: ProviderAdapterV2SessionRuntime,
        input: ProviderAdapterV2TurnInput,
      ): Effect.Effect<ProviderAdapterV2TurnInput["configuredDefaultModelSelection"]> =>
        Effect.gen(function* () {
          if (
            runtime.driver !== "codex" ||
            input.modelSelection.options?.some((option) => option.id === "reasoningEffort") ||
            Option.isNone(serverSettings)
          )
            return undefined;
          const currentSettings = yield* serverSettings.value.getSettings.pipe(Effect.option);
          if (Option.isNone(currentSettings)) return undefined;
          let settings = currentSettings.value;
          const thread = yield* projectionStore.getThread(input.threadId).pipe(Effect.option);
          if (Option.isSome(thread)) {
            const project = Option.isSome(projectService)
              ? yield* projectService.value
                  .getById(thread.value.projectId)
                  .pipe(Effect.orElseSucceed(() => Option.none()))
              : Option.none();
            settings = resolveProjectSettings(
              settings,
              thread.value.projectId,
              Option.getOrUndefined(project),
            ).settings;
          }
          const modelSelection = settings.defaultModelSelection;
          if (modelSelection == null) return undefined;
          let driver = settings.providerInstances[modelSelection.instanceId]?.driver;
          if (driver === undefined && modelSelection.instanceId === runtime.instanceId)
            driver = runtime.driver;
          if (driver === undefined && registry.getHandoffDeliveryDescriptor !== undefined) {
            const descriptor = yield* registry
              .getHandoffDeliveryDescriptor(modelSelection.instanceId)
              .pipe(Effect.option);
            if (Option.isSome(descriptor)) driver = descriptor.value.driver;
          }
          return driver === undefined ? undefined : { modelSelection, driver };
        });
      const agentAccessSettings = Effect.fn("ProviderSessionManagerV2.agentAccessSettings")(
        function* (threadId: ThreadId) {
          if (Option.isNone(serverSettings)) return { browser: true, device: false };
          return yield* Effect.gen(function* () {
            const settings = yield* serverSettings.value.getSettings;
            const thread = yield* projectionStore.getThread(threadId);
            const entries = Object.values(settings.projectSettingsOverrides);
            const browserOverridden = entries.some(
              (entry) => entry.enableAgentBrowserAccess !== undefined,
            );
            const deviceOverridden = entries.some(
              (entry) => entry.enableAgentDeviceAccess !== undefined,
            );
            if (browserOverridden || deviceOverridden) {
              const project = Option.isSome(projectService)
                ? yield* projectService.value.getById(thread.projectId)
                : Option.none();
              if (Option.isNone(project))
                return {
                  browser: browserOverridden ? false : settings.enableAgentBrowserAccess,
                  device: deviceOverridden ? false : settings.enableAgentDeviceAccess,
                };
            }
            const effective = resolveProjectSettings(settings, thread.projectId).settings;
            return {
              browser: effective.enableAgentBrowserAccess,
              device: effective.enableAgentDeviceAccess,
            };
          }).pipe(
            Effect.catch((cause) =>
              Effect.logWarning(
                "Could not resolve agent access; withholding browser and device tools.",
                { threadId, cause },
              ).pipe(Effect.as({ browser: false, device: false })),
            ),
          );
        },
      );
      const layerScope = yield* Effect.scope;
      const sessions = yield* Ref.make(new Map<string, LiveSessionEntry>());
      const pendingCleanupSources = new Map<
        string,
        {
          readonly runtime: ProviderAdapterV2SessionRuntime;
          readonly readOwner: Effect.Effect<EventSink.ProviderRuntimeEvidenceV2 | null, unknown>;
        }
      >();
      const nextSubscriberId = yield* Ref.make(0);
      const sessionOpen = yield* makeKeyedSerialExecutor<ProviderSessionId>();
      // Orders a thread's attach against a detach unloading it on the same session.
      const threadAttachment = yield* makeKeyedSerialExecutor<string>();
      const threadAttachmentKey = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
      }) => `${input.providerSessionId}\u0000${input.threadId}`;
      const idleTimeoutMs = Math.max(1, options.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS);
      const maxIdlePinMs = Math.max(0, options.maxIdlePinMs ?? DEFAULT_MAX_IDLE_PIN_MS);
      interface PreparedMcpCredential {
        readonly mcpCredentialId: string | undefined;
        /** True when this call minted the credential (vs reusing a live one). */
        readonly issued: boolean;
      }
      /**
       * Reservations protect a credential between prepareMcpSession handing it
       * out and the owning session entry becoming visible in `sessions`.
       * Adapters like ACP and OpenCode consume the credential eagerly during
       * openSession, so a racing release must not revoke it in that window
       * (rotating afterwards cannot repair an already-configured process).
       * The holder MUST drop the reservation once the entry is recorded or the
       * open fails.
       */
      const mcpCredentialReservations = new Map<string, number>();
      const mcpReservationKey = (threadId: ThreadId, mcpCredentialId: string) =>
        `${threadId}\0${mcpCredentialId}`;
      const reserveMcpCredential = (threadId: ThreadId, mcpCredentialId: string) => {
        const key = mcpReservationKey(threadId, mcpCredentialId);
        mcpCredentialReservations.set(key, (mcpCredentialReservations.get(key) ?? 0) + 1);
      };
      const dropMcpCredentialReservation = (threadId: ThreadId, mcpCredentialId: string) => {
        const key = mcpReservationKey(threadId, mcpCredentialId);
        const count = mcpCredentialReservations.get(key) ?? 0;
        if (count <= 1) {
          mcpCredentialReservations.delete(key);
        } else {
          mcpCredentialReservations.set(key, count - 1);
        }
      };
      const isMcpCredentialReserved = (threadId: ThreadId, mcpCredentialId: string) =>
        (mcpCredentialReservations.get(mcpReservationKey(threadId, mcpCredentialId)) ?? 0) > 0;
      const mcpPrepareLock = yield* makeKeyedSerialExecutor<ThreadId>();
      const agentDeviceEnvironment = Effect.gen(function* () {
        const devices = yield* Effect.serviceOption(DeviceService.DeviceService);
        if (Option.isNone(devices) || Option.isNone(serverConfig) || Option.isNone(pathService))
          return undefined;
        const entryPath = yield* devices.value.agentCli.pipe(
          Effect.catch(() =>
            Effect.logWarning("Agent device CLI unavailable").pipe(Effect.as(null)),
          ),
        );
        if (!entryPath) return undefined;
        const shimDir = yield* ensureAgentDeviceShim({
          entryPath,
          stateDir: serverConfig.value.stateDir,
        }).pipe(
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.provideService(Path.Path, pathService.value),
          Effect.orElseSucceed(() => undefined),
        );
        return shimDir === undefined
          ? undefined
          : {
              PATH: shimDir,
              PATH_SEPARATOR: hostPlatform === "win32" ? ";" : ":",
              AGENT_DEVICE_NO_UPDATE_NOTIFIER: "1",
            };
      });
      /**
       * Resolves (or mints) the thread's MCP credential and returns it with a
       * reservation held; the caller must drop the reservation exactly once.
       * Serialized per thread so two concurrent prepares cannot interleave
       * their rotate steps and revoke each other's freshly minted credential.
       */
      const prepareMcpSession = (
        threadId: ThreadId,
        providerInstanceId: ProviderInstanceId,
      ): Effect.Effect<PreparedMcpCredential> =>
        options.configureMcp === false
          ? Effect.sync((): PreparedMcpCredential => {
              McpProviderSession.clearMcpProviderSession(threadId);
              return { mcpCredentialId: undefined, issued: false };
            })
          : mcpPrepareLock.withLock(
              threadId,
              Effect.gen(function* () {
                // Reuse a still-valid credential for this thread instead of
                // rotating: long-lived provider processes (codex app-server)
                // build their MCP client once per conversation and keep using
                // the credential it started with, so a thread that detaches and
                // re-attaches across a workspace handoff must come back to the
                // same token or the process's tool calls fail auth.
                const { browser: browserToolsAvailable, device: deviceToolsAvailable } =
                  yield* agentAccessSettings(threadId);
                const capabilities = new Set<
                  import("../mcp/McpInvocationContext.ts").McpCapability
                >([
                  "orchestration",
                  "worktree",
                  "pull-requests",
                  "organization",
                  "decision-snapshot",
                ]);
                if (browserToolsAvailable) capabilities.add("preview");
                if (deviceToolsAvailable) capabilities.add("device");
                const existing = McpProviderSession.readMcpProviderSession(threadId);
                if (existing !== undefined) {
                  // Reserve before the async resolve so a release cannot
                  // revoke the credential between validation and reservation.
                  reserveMcpCredential(threadId, existing.providerSessionId);
                  const rawToken = existing.authorizationHeader.replace(/^Bearer\s+/, "");
                  const resolved = yield* mcpSessionRegistry.resolve(rawToken);
                  if (
                    resolved !== undefined &&
                    resolved.threadId === threadId &&
                    resolved.providerInstanceId === providerInstanceId &&
                    // Reuse only the complete current grant, including Jones
                    // tools and optional browser/device access.
                    resolved.capabilities.size === capabilities.size &&
                    Array.from(capabilities).every((capability) =>
                      resolved.capabilities.has(capability),
                    )
                  ) {
                    return { mcpCredentialId: existing.providerSessionId, issued: false };
                  }
                  dropMcpCredentialReservation(threadId, existing.providerSessionId);
                }
                yield* mcpSessionRegistry.revokeThread(threadId);
                const credential = yield* mcpSessionRegistry.issue({
                  threadId,
                  providerInstanceId,
                  browserToolsAvailable,
                  capabilities,
                });
                const deviceEnvironment = deviceToolsAvailable
                  ? yield* agentDeviceEnvironment
                  : undefined;
                McpProviderSession.setMcpProviderSession({
                  ...credential.config,
                  ...(deviceEnvironment === undefined
                    ? {}
                    : { agentDeviceEnvironment: deviceEnvironment }),
                });
                reserveMcpCredential(threadId, credential.config.providerSessionId);
                return { mcpCredentialId: credential.config.providerSessionId, issued: true };
              }),
            );
      /**
       * With a credential id, revocation is scoped to that credential and the
       * config slot is cleared only while it still holds it; a replacement
       * session's newer credential survives. Without one (attach failed before
       * a credential was recorded), fall back to thread-wide revocation.
       */
      const clearMcpSession = (threadId: ThreadId, mcpCredentialId?: string) =>
        mcpCredentialId === undefined
          ? mcpSessionRegistry
              .revokeThread(threadId)
              .pipe(
                Effect.tap(() =>
                  Effect.sync(() => McpProviderSession.clearMcpProviderSession(threadId)),
                ),
              )
          : mcpSessionRegistry.revokeProviderSession(mcpCredentialId).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  if (
                    McpProviderSession.readMcpProviderSession(threadId)?.providerSessionId ===
                    mcpCredentialId
                  ) {
                    McpProviderSession.clearMcpProviderSession(threadId);
                  }
                }),
              ),
            );

      const publishToSubscribers = (
        subscribers: Ref.Ref<
          ReadonlyMap<number, Queue.Queue<ProviderSessionEventSignal, Cause.Done>>
        >,
        signal: ProviderSessionEventSignal,
      ) =>
        Ref.get(subscribers).pipe(
          Effect.flatMap((current) =>
            Effect.forEach(current.values(), (queue) => Queue.offer(queue, signal), {
              discard: true,
            }),
          ),
        );

      const failSubscribers = (entry: LiveSessionEntry, detail: string) =>
        Effect.gen(function* () {
          const error = new ProviderAdapterEventStreamError({
            driver: entry.runtime.driver,
            providerSessionId: entry.runtime.providerSessionId,
            cause: detail,
          });
          const subscribers = yield* Ref.getAndSet(entry.eventSubscribers, new Map());
          yield* Effect.forEach(
            subscribers.values(),
            (queue) =>
              Queue.offer(queue, {
                type: "failure",
                cause: Cause.fail(error),
              }),
            { discard: true },
          );
        });

      const closeSubscribers = (entry: LiveSessionEntry) =>
        Effect.gen(function* () {
          const subscribers = yield* Ref.getAndSet(entry.eventSubscribers, new Map());
          yield* Effect.forEach(
            subscribers.values(),
            (queue) => Queue.clear(queue).pipe(Effect.andThen(Queue.end(queue))),
            { discard: true },
          );
        });

      // Preserve already-published terminal events while ending subscriptions.
      // Server shutdown intentionally clears them; a provider-announced Stop
      // must let consumers drain them before the stream completes.
      const endSubscribers = (entry: LiveSessionEntry) =>
        Effect.gen(function* () {
          const subscribers = yield* Ref.getAndSet(entry.eventSubscribers, new Map());
          yield* Effect.forEach(subscribers.values(), (queue) => Queue.end(queue), {
            discard: true,
          });
        });

      const cancelIdleFiber = (fiber: Fiber.Fiber<void, never> | null) =>
        fiber === null ? Effect.void : Fiber.interrupt(fiber).pipe(Effect.ignore);

      const flushResidentAssistantOutput = (entry: LiveSessionEntry) =>
        Effect.gen(function* () {
          for (const threadId of entry.attachedThreadIds) {
            const owner =
              (yield* eventSink.readCurrentProviderRuntimeOwner(threadId)) ??
              (yield* eventSink.readProviderRuntimeEvidence(threadId));
            if (
              owner === null ||
              owner.binding.nativeThreadId === null ||
              owner.binding.runtimeGeneration === null
            )
              continue;
            if (
              owner.binding.providerSessionId !== entry.runtime.providerSessionId ||
              owner.binding.instanceId !== entry.runtime.instanceId ||
              owner.binding.driver !== entry.runtime.driver ||
              owner.binding.runtimeGeneration !== entry.runtime.runtimeGeneration
            )
              continue;
            const binding: ProviderRuntimeBinding & { readonly nativeThreadId: string } = {
              threadId,
              providerThreadId: owner.binding.providerThreadId,
              providerSessionId: owner.binding.providerSessionId,
              instanceId: owner.binding.instanceId,
              nativeThreadId: owner.binding.nativeThreadId,
              runtimeGeneration: owner.binding.runtimeGeneration,
            };
            yield* providerEventIngestor.flushAssistantOutput({
              binding,
              revalidateCurrentOwner: revalidateRuntimeIdentityOwner(entry, binding),
            });
          }
        });

      const writeProviderSessionEvents = (input: {
        readonly runtime: ProviderAdapterV2SessionRuntime;
        readonly threadIds: Iterable<ThreadId>;
        readonly type: "provider-session.attached" | "provider-session.updated";
        readonly payload: OrchestrationV2ProviderSession;
      }) =>
        Effect.gen(function* () {
          if (input.payload.status === "ready") {
            const entry = (yield* Ref.get(sessions)).get(
              sessionKey(input.runtime.providerSessionId),
            );
            if (entry?.runtime === input.runtime) yield* flushResidentAssistantOutput(entry);
          }
          const now = yield* DateTime.now;
          const events = yield* Effect.forEach(input.threadIds, (threadId) =>
            Effect.gen(function* () {
              return {
                id: yield* idAllocator.allocate.event({
                  threadId,
                  providerSessionId: input.runtime.providerSessionId,
                }),
                type: input.type,
                threadId,
                driver: input.runtime.driver,
                providerInstanceId: input.runtime.instanceId,
                occurredAt: now,
                payload: input.payload,
              } satisfies OrchestrationV2DomainEvent;
            }),
          );
          if (events.length > 0) {
            yield* eventSink.write({ events });
          }
        });

      const writeReleasedSessionEvents = (input: {
        readonly entry: LiveSessionEntry;
        readonly reason: ProviderSessionReleaseReason;
        readonly detail?: string;
      }) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          const payload: OrchestrationV2ProviderSession = {
            ...input.entry.runtime.providerSession,
            status: releaseStatusFor(input.reason),
            updatedAt: now,
            lastError:
              input.reason === "runtime_error"
                ? (input.detail ?? "Provider runtime failed.")
                : null,
          };
          yield* writeProviderSessionEvents({
            runtime: input.entry.runtime,
            threadIds: input.entry.attachedThreadIds,
            type: "provider-session.updated",
            payload,
          });
        });

      const writeReleasedRuntimeRequestEvents = (input: {
        readonly entry: LiveSessionEntry;
        readonly reason: ProviderSessionReleaseReason;
      }) =>
        Effect.gen(function* () {
          const providerSessionId = input.entry.runtime.providerSessionId;
          const now = yield* DateTime.now;
          const status = releasedRuntimeRequestStatusFor(input.reason);
          const reason =
            input.reason === "runtime_error"
              ? "Provider session failed before this runtime request was resolved."
              : "Provider session was closed before this runtime request was resolved.";

          const events: Array<OrchestrationV2DomainEvent> = [];
          for (const threadId of input.entry.attachedThreadIds) {
            const projection = yield* projectionStore.getThreadRecords(
              threadId,
              ["runtimeRequests", "nodes", "turnItems"],
              { turnItemTypes: ["approval_request", "user_input_request"] },
            );
            const releasedRequests = projection.runtimeRequests.filter(
              (request) =>
                request.status === "pending" &&
                request.responseCapability.type === "live" &&
                request.responseCapability.providerSessionId === providerSessionId,
            );

            for (const request of releasedRequests) {
              events.push({
                id: yield* idAllocator.allocate.event({
                  threadId,
                  providerSessionId,
                }),
                type: "runtime-request.updated",
                threadId,
                nodeId: request.nodeId,
                driver: input.entry.runtime.driver,
                occurredAt: now,
                payload: {
                  ...request,
                  status,
                  responseCapability: {
                    type: "not_resumable",
                    reason,
                  },
                  resolvedAt: now,
                },
              });

              const requestNode = projection.nodes.find((node) => node.id === request.nodeId);
              if (requestNode !== undefined) {
                events.push({
                  id: yield* idAllocator.allocate.event({
                    threadId,
                    providerSessionId,
                  }),
                  type: "node.updated",
                  threadId,
                  ...(requestNode.runId === null ? {} : { runId: requestNode.runId }),
                  nodeId: requestNode.id,
                  driver: input.entry.runtime.driver,
                  occurredAt: now,
                  payload: {
                    ...requestNode,
                    status: input.reason === "runtime_error" ? "failed" : "cancelled",
                    completedAt: now,
                  },
                });
              }

              const turnItem = projection.turnItems.find(
                (item) =>
                  (item.type === "approval_request" || item.type === "user_input_request") &&
                  item.requestId === request.id,
              );
              if (turnItem !== undefined) {
                events.push({
                  id: yield* idAllocator.allocate.event({
                    threadId,
                    providerSessionId,
                  }),
                  type: "turn-item.updated",
                  threadId,
                  ...(turnItem.runId === null ? {} : { runId: turnItem.runId }),
                  ...(turnItem.nodeId === null ? {} : { nodeId: turnItem.nodeId }),
                  driver: input.entry.runtime.driver,
                  occurredAt: now,
                  payload: {
                    ...turnItem,
                    status: input.reason === "runtime_error" ? "failed" : "cancelled",
                    completedAt: now,
                    updatedAt: now,
                  },
                });
              }
            }
          }

          if (events.length > 0) {
            yield* eventSink.write({ events });
          }
        });

      const releaseEntry = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly reason: ProviderSessionReleaseReason;
        readonly detail?: string;
        readonly cancelIdleFiber?: boolean;
        readonly onlyIfIdleGeneration?: number;
        readonly gracefulSubscribers?: boolean;
      }) =>
        Effect.acquireUseRelease(
          Effect.gen(function* () {
            const resident = (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId));
            if (
              resident !== undefined &&
              !isPendingStartStopReservation(resident) &&
              resident.managedStopReservation === undefined
            )
              yield* flushResidentAssistantOutput(resident);
            return yield* Ref.modify(sessions, (current) => {
              const key = sessionKey(input.providerSessionId);
              const existing = current.get(key);
              if (existing === undefined) {
                return [Option.none<LiveSessionEntry>(), current] as const;
              }
              if (
                isPendingStartStopReservation(existing) ||
                existing.managedStopReservation !== undefined
              ) {
                return [Option.none<LiveSessionEntry>(), current] as const;
              }
              if (
                input.onlyIfIdleGeneration !== undefined &&
                (existing.busyCount > 0 || existing.idleGeneration !== input.onlyIfIdleGeneration)
              ) {
                return [Option.none<LiveSessionEntry>(), current] as const;
              }
              const updated = new Map(current);
              updated.delete(key);
              return [Option.some(existing), updated] as const;
            });
          }),
          (entry) =>
            Option.match(entry, {
              onNone: () => Effect.void,
              onSome: (entry) =>
                Effect.gen(function* () {
                  if (input.cancelIdleFiber !== false) {
                    yield* cancelIdleFiber(entry.idleFiber);
                  }
                  if (input.gracefulSubscribers === true) {
                    yield* endSubscribers(entry);
                  } else if (input.reason === "server_shutdown") {
                    yield* closeSubscribers(entry);
                  } else {
                    yield* failSubscribers(
                      entry,
                      input.detail ?? `Provider session released: ${input.reason}.`,
                    );
                  }
                  // Scope close can wedge on a misbehaving adapter finalizer
                  // (e.g. a provider process that never yields its message
                  // stream). Time-box it so release still persists released
                  // events and leaves a diagnosable trail instead of silently
                  // parking the session as "ready" forever.
                  const closeFiber = yield* Scope.close(entry.scope, Exit.void).pipe(
                    Effect.exit,
                    Effect.forkDetach({ startImmediately: true }),
                  );
                  const closeExit = yield* Fiber.join(closeFiber).pipe(
                    Effect.timeoutOption(RELEASE_SCOPE_CLOSE_TIMEOUT_MS),
                  );
                  if (Option.isNone(closeExit)) {
                    yield* Effect.logWarning(
                      "orchestration-v2.provider-session-scope-close-timeout",
                      {
                        providerSessionId: input.providerSessionId,
                        reason: input.reason,
                        timeoutMs: RELEASE_SCOPE_CLOSE_TIMEOUT_MS,
                      },
                    );
                    yield* Fiber.join(closeFiber).pipe(
                      Effect.flatMap((exit) =>
                        Exit.isFailure(exit)
                          ? Effect.logWarning(
                              "orchestration-v2.provider-session-scope-close-failed",
                              {
                                providerSessionId: input.providerSessionId,
                                reason: input.reason,
                                cause: exit.cause,
                              },
                            )
                          : Effect.logInfo(
                              "orchestration-v2.provider-session-scope-close-completed-late",
                              {
                                providerSessionId: input.providerSessionId,
                                reason: input.reason,
                              },
                            ),
                      ),
                      Effect.forkDetach,
                    );
                  }
                  yield* writeReleasedSessionEvents({
                    entry,
                    reason: input.reason,
                    ...(input.detail === undefined ? {} : { detail: input.detail }),
                  });
                  yield* writeReleasedRuntimeRequestEvents({
                    entry,
                    reason: input.reason,
                  }).pipe(entry.requestEventPermit.withPermits(1));
                  if (Option.isSome(closeExit) && Exit.isFailure(closeExit.value)) {
                    return yield* Effect.failCause(closeExit.value.cause);
                  }
                }),
            }),
          (entry) =>
            Option.match(entry, {
              onNone: () => Effect.void,
              onSome: (entry) =>
                // Revoke every credential this session recorded, including for
                // threads that detached without re-attaching: the provider
                // process is gone, so nothing holds them anymore. Skip threads
                // a live replacement session took over, since credential reuse
                // means the replacement may hold this very credential.
                Ref.get(sessions).pipe(
                  Effect.flatMap((current) =>
                    Effect.forEach(
                      entry.mcpCredentialIdByThread,
                      ([threadId, mcpCredentialId]) => {
                        // Id-sensitive: a stale record for the same thread but
                        // a DIFFERENT credential (left behind by an old session
                        // the thread rotated away from) must not veto revoking
                        // this session's own credential, or it leaks forever.
                        // A reservation means an in-flight open is configuring
                        // a provider process with this credential right now;
                        // revoking it here would strand that process (eager
                        // adapters cannot pick up a rotated token).
                        const heldElsewhere =
                          isMcpCredentialReserved(threadId, mcpCredentialId) ||
                          Array.from(current.values()).some(
                            (other) =>
                              other !== entry &&
                              (other.attachedThreadIds.has(threadId) ||
                                other.mcpCredentialIdByThread.get(threadId) === mcpCredentialId),
                          );
                        return heldElsewhere
                          ? Effect.void
                          : clearMcpSession(threadId, mcpCredentialId);
                      },
                      { discard: true },
                    ),
                  ),
                ),
            }),
        ).pipe(
          Effect.catchCause((cause) =>
            Effect.fail(
              new ProviderSessionReleaseError({
                providerSessionId: input.providerSessionId,
                reason: input.reason,
                cause,
              }),
            ),
          ),
        );

      // Annotated to break the releaseIfStillIdle <-> scheduleIdleReleaseInternal
      // inference cycle introduced by the pin re-arm below.
      const releaseIfStillIdle = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly generation: number;
      }): Effect.Effect<void> =>
        Effect.gen(function* () {
          const current = yield* Ref.get(sessions);
          const key = sessionKey(input.providerSessionId);
          const entry = current.get(key);
          if (
            entry === undefined ||
            entry.busyCount > 0 ||
            entry.idleGeneration !== input.generation
          ) {
            return;
          }
          // Capture runtime identity before yielding: a replacement session
          // can reuse the same providerSessionId while this fiber is parked.
          const probedRuntime = entry.runtime;
          const hasPendingWork =
            probedRuntime.hasPendingBackgroundWork === undefined
              ? false
              : yield* probedRuntime.hasPendingBackgroundWork.pipe(
                  Effect.catchCause(() => Effect.succeed(false)),
                );
          if (hasPendingWork) {
            const now = yield* Clock.currentTimeMillis;
            const pinnedSinceMs = entry.pinnedSinceMs ?? now;
            if (now - pinnedSinceMs < maxIdlePinMs) {
              const shouldContinuePin = yield* Ref.modify(sessions, (latest) => {
                const latestEntry = latest.get(key);
                if (
                  latestEntry === undefined ||
                  latestEntry.busyCount > 0 ||
                  latestEntry.idleGeneration !== input.generation ||
                  latestEntry.runtime !== probedRuntime
                ) {
                  return [false, latest] as const;
                }
                const updated = new Map(latest);
                updated.set(key, { ...latestEntry, pinnedSinceMs });
                return [true, updated] as const;
              });
              if (!shouldContinuePin) {
                // Generation or runtime advanced while we probed pending work;
                // the current owner of the entry owns idle release.
                return;
              }
              yield* Effect.logInfo("orchestration-v2.driver-session.idle-release-deferred", {
                providerSessionId: input.providerSessionId,
                pinnedForMs: now - pinnedSinceMs,
              });
              // Re-check on this fiber after another idle window. Do not call
              // scheduleIdleReleaseInternal: that cancels entry.idleFiber, which
              // is this fiber, and can self-deadlock on Fiber.interrupt.
              yield* Effect.sleep(Duration.millis(idleTimeoutMs));
              return yield* releaseIfStillIdle(input);
            }
            yield* Effect.logWarning("orchestration-v2.driver-session.idle-release-pin-expired", {
              providerSessionId: input.providerSessionId,
              pinnedForMs: now - pinnedSinceMs,
            });
          }
          // hasPendingBackgroundWork yields to the adapter, so the idle
          // decision above can go stale; the generation guard revalidates
          // busyCount and idleGeneration inside releaseEntry's atomic
          // entry removal.
          yield* releaseEntry({
            providerSessionId: input.providerSessionId,
            reason: "idle_timeout",
            cancelIdleFiber: false,
            onlyIfIdleGeneration: input.generation,
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("orchestration-v2.driver-session.idle-release-failed", {
                providerSessionId: input.providerSessionId,
                cause,
              }),
            ),
          );
        });

      const withActivityError = <A, E, R>(
        providerSessionId: ProviderSessionId,
        effect: Effect.Effect<A, E, R>,
      ): Effect.Effect<A, ProviderSessionActivityError, R> =>
        effect.pipe(
          Effect.catchCause((cause) =>
            Effect.fail(
              new ProviderSessionActivityError({
                providerSessionId,
                cause,
              }),
            ),
          ),
        );

      const scheduleIdleReleaseInternal = (providerSessionId: ProviderSessionId) =>
        Effect.gen(function* () {
          const key = sessionKey(providerSessionId);
          const current = yield* Ref.get(sessions);
          const entry = current.get(key);
          if (entry === undefined || entry.busyCount > 0) {
            return;
          }

          yield* cancelIdleFiber(entry.idleFiber);
          const generation = entry.idleGeneration + 1;
          const idleFiber = yield* Effect.sleep(Duration.millis(idleTimeoutMs)).pipe(
            Effect.andThen(releaseIfStillIdle({ providerSessionId, generation })),
            Effect.forkIn(layerScope),
          );
          const lastActivityAtMs = yield* Clock.currentTimeMillis;
          yield* Ref.update(sessions, (latest) => {
            const latestEntry = latest.get(key);
            if (latestEntry === undefined || latestEntry.busyCount > 0) {
              return latest;
            }
            const updated = new Map(latest);
            updated.set(key, {
              ...latestEntry,
              idleGeneration: generation,
              idleFiber,
              lastActivityAtMs,
            });
            return updated;
          });
        });

      const scheduleIdleRelease = (providerSessionId: ProviderSessionId) =>
        withActivityError(providerSessionId, scheduleIdleReleaseInternal(providerSessionId));

      const touchActivity = (providerSessionId: ProviderSessionId) =>
        withActivityError(
          providerSessionId,
          Effect.gen(function* () {
            const lastActivityAtMs = yield* Clock.currentTimeMillis;
            yield* Ref.update(sessions, (current) => {
              const entry = current.get(sessionKey(providerSessionId));
              if (entry === undefined) {
                return current;
              }
              const updated = new Map(current);
              updated.set(sessionKey(providerSessionId), {
                ...entry,
                lastActivityAtMs,
              });
              return updated;
            });
            yield* scheduleIdleReleaseInternal(providerSessionId);
          }),
        );

      const attachThread = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
      }) =>
        withActivityError(
          input.providerSessionId,
          Ref.modify(
            sessions,
            (current): readonly [boolean | "reserved", Map<string, LiveSessionEntry>] => {
              const entry = current.get(sessionKey(input.providerSessionId));
              if (
                entry?.managedStopReservation !== undefined &&
                (!entry.attachedThreadIds.has(input.threadId) ||
                  entry.managedStopReservation.input.binding.threadId === input.threadId)
              )
                return ["reserved", current] as const;
              if (entry === undefined || entry.attachedThreadIds.has(input.threadId)) {
                return [false, current] as const;
              }
              if (entry.runtimeReplacementReservation !== undefined) {
                return ["reserved", current] as const;
              }
              const updated = new Map(current);
              updated.set(sessionKey(input.providerSessionId), {
                ...entry,
                attachedThreadIds: new Set([...entry.attachedThreadIds, input.threadId]),
              });
              return [true, updated] as const;
            },
          ),
        ).pipe(
          Effect.flatMap((result) =>
            result === "reserved"
              ? Effect.fail(
                  new RuntimeReplacementAttachmentError({
                    providerSessionId: input.providerSessionId,
                  }),
                )
              : Effect.succeed(result),
          ),
        );

      const removeThreadAttachment = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
      }) =>
        Ref.update(sessions, (current) => {
          const key = sessionKey(input.providerSessionId);
          const entry = current.get(key);
          if (entry === undefined || !entry.attachedThreadIds.has(input.threadId)) {
            return current;
          }
          const attachedThreadIds = new Set(entry.attachedThreadIds);
          attachedThreadIds.delete(input.threadId);
          const loadedProviderThreadKeyByThread = new Map(entry.loadedProviderThreadKeyByThread);
          loadedProviderThreadKeyByThread.delete(input.threadId);
          const updated = new Map(current);
          updated.set(key, {
            ...entry,
            attachedThreadIds,
            loadedProviderThreadKeyByThread,
          });
          return updated;
        });

      const isProviderThreadLoaded = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly providerThreadKey: string;
      }) =>
        Ref.get(sessions).pipe(
          Effect.map(
            (current) =>
              current
                .get(sessionKey(input.providerSessionId))
                ?.loadedProviderThreadKeyByThread.get(input.threadId) === input.providerThreadKey,
          ),
        );

      const markProviderThreadLoaded = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly providerThreadKey: string;
      }) =>
        Ref.update(sessions, (current) => {
          const key = sessionKey(input.providerSessionId);
          const entry = current.get(key);
          if (entry === undefined) {
            return current;
          }
          const loadedProviderThreadKeyByThread = new Map(entry.loadedProviderThreadKeyByThread);
          loadedProviderThreadKeyByThread.set(input.threadId, input.providerThreadKey);
          const updated = new Map(current);
          updated.set(key, { ...entry, loadedProviderThreadKeyByThread });
          return updated;
        });

      const ensureThreadAttached = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly providerInstanceId: ProviderInstanceId;
      }) =>
        Effect.suspend(() => {
          let preparedForCleanup: PreparedMcpCredential | undefined;
          let reservationDropped = false;
          let attachedForCleanup = false;
          const dropReservation = () => {
            if (!reservationDropped && preparedForCleanup?.mcpCredentialId !== undefined) {
              reservationDropped = true;
              dropMcpCredentialReservation(input.threadId, preparedForCleanup.mcpCredentialId);
            }
          };
          return Effect.gen(function* () {
            const attached = yield* threadAttachment.withLock(
              threadAttachmentKey(input),
              attachThread(input),
            );
            if (attached) {
              attachedForCleanup = true;
              const prepared = yield* prepareMcpSession(input.threadId, input.providerInstanceId);
              preparedForCleanup = prepared;
              if (prepared.mcpCredentialId !== undefined) {
                const mcpCredentialId = prepared.mcpCredentialId;
                yield* Ref.update(sessions, (current) => {
                  const key = sessionKey(input.providerSessionId);
                  const entry = current.get(key);
                  if (entry === undefined) return current;
                  const mcpCredentialIdByThread = new Map(entry.mcpCredentialIdByThread);
                  mcpCredentialIdByThread.set(input.threadId, mcpCredentialId);
                  const updated = new Map(current);
                  updated.set(key, { ...entry, mcpCredentialIdByThread });
                  return updated;
                });
              }
              const entry = (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId));
              if (entry !== undefined) {
                yield* withActivityError(
                  input.providerSessionId,
                  writeProviderSessionEvents({
                    runtime: entry.runtime,
                    threadIds: [input.threadId],
                    type: "provider-session.attached",
                    payload: entry.runtime.providerSession,
                  }),
                );
              }
            }
          }).pipe(
            Effect.tapError(() =>
              (attachedForCleanup ? removeThreadAttachment(input) : Effect.void).pipe(
                // Revoke only a credential this attach freshly minted: a REUSED
                // credential is by definition held by another live provider
                // process, and revoking it thread-wide would break that
                // process's MCP client mid-conversation.
                Effect.andThen(
                  Effect.suspend(() => {
                    dropReservation();
                    return preparedForCleanup?.issued === true
                      ? clearMcpSession(input.threadId, preparedForCleanup.mcpCredentialId)
                      : Effect.void;
                  }),
                ),
              ),
            ),
            // The entry's own record (written above while the thread is
            // attached) guards the credential from here on; the reservation
            // is only needed until then. Ensuring covers defects/interrupts.
            Effect.ensuring(Effect.sync(dropReservation)),
          );
        });

      const markBusy = (providerSessionId: ProviderSessionId) =>
        withActivityError(
          providerSessionId,
          Effect.gen(function* () {
            const key = sessionKey(providerSessionId);
            const now = yield* Clock.currentTimeMillis;
            const idleFiber = yield* Ref.modify(sessions, (current) => {
              const entry = current.get(key);
              if (entry === undefined) {
                return [null, current] as const;
              }
              const updated = new Map(current);
              updated.set(key, {
                ...entry,
                busyCount: entry.busyCount + 1,
                idleFiber: null,
                lastActivityAtMs: now,
                pinnedSinceMs: null,
              });
              return [entry.idleFiber, updated] as const;
            });
            yield* cancelIdleFiber(idleFiber);
          }),
        );

      const markIdle = (providerSessionId: ProviderSessionId) =>
        withActivityError(
          providerSessionId,
          Effect.gen(function* () {
            const key = sessionKey(providerSessionId);
            const now = yield* Clock.currentTimeMillis;
            yield* Ref.update(sessions, (current) => {
              const entry = current.get(key);
              if (entry === undefined) {
                return current;
              }
              const updated = new Map(current);
              updated.set(key, {
                ...entry,
                busyCount: Math.max(0, entry.busyCount - 1),
                lastActivityAtMs: now,
              });
              return updated;
            });
            yield* scheduleIdleReleaseInternal(providerSessionId);
          }),
        );

      const observeActivity = (
        providerSessionId: ProviderSessionId,
        activity: Effect.Effect<void, ProviderSessionActivityError>,
      ) =>
        activity.pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("orchestration-v2.driver-session.activity-failed", {
              providerSessionId,
              cause,
            }),
          ),
        );

      const observeThreadAttachment = (
        runtime: ProviderAdapterV2SessionRuntime,
        attachment: Effect.Effect<
          void,
          ProviderSessionActivityError | RuntimeReplacementAttachmentError
        >,
      ) =>
        Effect.gen(function* () {
          const entry = (yield* Ref.get(sessions)).get(sessionKey(runtime.providerSessionId));
          if (entry?.runtime === runtime && isPendingStartStopReservation(entry))
            return yield* new RuntimeReplacementAttachmentError({
              providerSessionId: runtime.providerSessionId,
            });
          yield* attachment;
        }).pipe(
          Effect.catchCause((cause) =>
            cause.reasons.some(
              (reason) =>
                Cause.isFailReason(reason) &&
                Schema.is(RuntimeReplacementAttachmentError)(reason.error),
            )
              ? Effect.fail(
                  new ProviderAdapterProtocolError({
                    driver: runtime.driver,
                    detail: "The resident runtime is reserved for a provider lifecycle change.",
                    cause,
                  }),
                )
              : Effect.logWarning("orchestration-v2.driver-session.activity-failed", {
                  providerSessionId: runtime.providerSessionId,
                  cause,
                }),
          ),
        );

      const validateProviderEventOrigin = (
        runtime: ProviderAdapterV2SessionRuntime,
        event: ProviderAdapterV2Event,
      ) =>
        Effect.gen(function* () {
          const resident = (yield* Ref.get(sessions)).get(sessionKey(runtime.providerSessionId));
          if (resident !== undefined && resident.runtime !== runtime) return false;
          const origin = readProviderEventOrigin(event);
          if (origin === undefined) return true;
          const producer = origin.producer;
          const matches = () =>
            producer.driver === runtime.driver &&
            event.driver === producer.driver &&
            producer.instanceId === runtime.instanceId &&
            producer.providerSessionId === runtime.providerSessionId &&
            (producer.runtimeGeneration === undefined ||
              producer.runtimeGeneration === runtime.runtimeGeneration);
          if (!matches()) return false;
          const owner = origin.turn;
          if (
            owner !== undefined &&
            (owner.binding.providerSessionId !== producer.providerSessionId ||
              owner.binding.instanceId !== producer.instanceId ||
              (producer.runtimeGeneration !== undefined &&
                owner.binding.runtimeGeneration !== producer.runtimeGeneration) ||
              (event.type === "turn.terminal" &&
                (event.providerThreadId !== owner.binding.providerThreadId ||
                  event.providerTurnId !== owner.providerTurnId)) ||
              (event.type === "provider_turn.updated" &&
                (event.providerTurn.providerThreadId !== owner.binding.providerThreadId ||
                  event.providerTurn.id !== owner.providerTurnId ||
                  event.providerTurn.runAttemptId !== owner.attemptId)))
          )
            return false;
          const matchesRequest = (entry: LiveSessionEntry | undefined) => {
            if (owner === undefined) return true;
            const request = entry?.startRequests.get(owner.binding.providerThreadId);
            if (request === undefined) return true;
            return (
              request.threadId === owner.binding.threadId &&
              request.providerThreadId === owner.binding.providerThreadId &&
              request.runId === owner.runId &&
              request.attemptId === owner.attemptId &&
              (event.type !== "turn.terminal" || event.runOrdinal === request.runOrdinal) &&
              (event.type === "provider_turn.updated" ||
                request.providerTurnIds.size === 0 ||
                request.providerTurnIds.has(owner.providerTurnId))
            );
          };
          if (!matchesRequest(resident)) return false;
          yield* producer.revalidateCurrent;
          const current = (yield* Ref.get(sessions)).get(sessionKey(runtime.providerSessionId));
          return (
            matches() &&
            matchesRequest(current) &&
            (current === undefined || current.runtime === runtime)
          );
        }).pipe(Effect.catchCause(() => Effect.succeed(false)));

      const revalidateProviderEventOrigin = (
        runtime: ProviderAdapterV2SessionRuntime,
        event: ProviderAdapterV2Event,
      ) =>
        validateProviderEventOrigin(runtime, event).pipe(
          Effect.flatMap((valid) =>
            valid
              ? Effect.void
              : Effect.fail(
                  new ProviderAdapterEventStreamError({
                    driver: runtime.driver,
                    providerSessionId: runtime.providerSessionId,
                    cause: "The captured provider event source is no longer current.",
                  }),
                ),
          ),
        );

      const makeEventSubscription = (
        runtime: ProviderAdapterV2SessionRuntime,
        subscribers: Ref.Ref<
          ReadonlyMap<number, Queue.Queue<ProviderSessionEventSignal, Cause.Done>>
        >,
      ): Effect.Effect<ProviderAdapterV2EventSubscription> =>
        Effect.gen(function* () {
          const queue = yield* Queue.unbounded<ProviderSessionEventSignal, Cause.Done>();
          const subscriberId = yield* Ref.getAndUpdate(nextSubscriberId, (value) => value + 1);
          yield* Ref.update(subscribers, (current) => {
            const updated = new Map(current);
            updated.set(subscriberId, queue);
            return updated;
          });
          const close = Ref.modify(subscribers, (current) => {
            if (!current.has(subscriberId)) {
              return [false, current] as const;
            }
            const updated = new Map(current);
            updated.delete(subscriberId);
            return [true, updated] as const;
          }).pipe(
            Effect.flatMap((removed) =>
              removed
                ? Queue.clear(queue).pipe(Effect.andThen(Queue.end(queue)), Effect.asVoid)
                : Effect.void,
            ),
          );
          const events = Stream.fromQueue(queue).pipe(
            Stream.filterEffect((signal) =>
              signal.type === "event"
                ? validateProviderEventOrigin(runtime, signal.event)
                : Effect.succeed(true),
            ),
            Stream.mapEffect((signal) =>
              signal.type === "event"
                ? Effect.succeed(signal.event)
                : Effect.failCause(signal.cause),
            ),
            Stream.ensuring(close),
          );
          return { events, close } satisfies ProviderAdapterV2EventSubscription;
        });

      const revalidateRuntimeIdentityOwner = (
        entry: LiveSessionEntry,
        binding: ProviderRuntimeBinding,
      ) =>
        Effect.gen(function* () {
          const current = (yield* Ref.get(sessions)).get(sessionKey(binding.providerSessionId));
          if (
            current?.runtime !== entry.runtime ||
            !current.attachedThreadIds.has(binding.threadId) ||
            current.runtime.providerSessionId !== binding.providerSessionId ||
            current.runtime.instanceId !== binding.instanceId ||
            current.runtime.runtimeGeneration !== binding.runtimeGeneration ||
            current.runtime.providerSession.status === "stopped" ||
            current.runtime.providerSession.status === "error"
          )
            return yield* new ProviderSessionActivityError({
              providerSessionId: binding.providerSessionId,
              cause: "The runtime emitting this identity is no longer current.",
            });
        });

      const registerRuntimeBindingInternal = (
        input: Parameters<ProviderSessionManagerV2Shape["registerRuntimeBinding"]>[0],
        flushIdentity = true,
        reservedGeneration?: string,
      ) =>
        Effect.gen(function* () {
          const entry = (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId));
          if (entry === undefined || !entry.attachedThreadIds.has(input.threadId))
            return yield* new ProviderSessionActivityError({
              providerSessionId: input.providerSessionId,
              cause: "The current thread runtime is not resident.",
            });
          const runtime = entry.runtime;
          const generation = reservedGeneration ?? runtime.runtimeGeneration;
          const projection = yield* projectionStore.getThreadProviderContext(
            input.threadId,
            runtime.instanceId,
          );
          const providerThread = projection.providerThreads.find(
            (thread) => thread.id === input.providerThreadId,
          );
          if (
            generation === undefined ||
            providerThread === undefined ||
            projection.thread.activeProviderThreadId !== providerThread.id ||
            providerThread.appThreadId !== input.threadId ||
            providerThread.providerSessionId !== input.providerSessionId ||
            providerThread.providerInstanceId !== runtime.instanceId ||
            providerThread.driver !== runtime.driver ||
            projection.thread.modelSelection.instanceId !== runtime.instanceId
          )
            return yield* new ProviderSessionActivityError({
              providerSessionId: input.providerSessionId,
              cause: "The current provider binding changed.",
            });
          const previous = yield* eventSink.readProviderRuntimeEvidence(input.threadId);
          const actual: ProviderRuntimeBinding = {
            threadId: input.threadId,
            providerThreadId: providerThread.id,
            providerSessionId: input.providerSessionId,
            instanceId: runtime.instanceId,
            runtimeGeneration: generation,
            ...(providerThread.nativeThreadRef?.nativeId == null
              ? {}
              : { nativeThreadId: providerThread.nativeThreadRef.nativeId }),
          };
          const expected: EventSink.ProviderBindingExpectationV2 = {
            ...actual,
            driver: runtime.driver,
            nativeThreadId: actual.nativeThreadId ?? null,
            runtimeGeneration: previous?.binding.runtimeGeneration ?? null,
          };
          const sourceIdentity =
            reservedGeneration === undefined &&
            actual.nativeThreadId !== undefined &&
            runtime.continuationSourceIdentity?.runtimeGeneration === generation &&
            runtime.continuationSourceIdentity.driverKind === runtime.driver
              ? runtime.continuationSourceIdentity
              : undefined;
          const result = yield* eventSink.registerProviderRuntime({
            expectedBinding: expected,
            expectedRegisteredBinding: previous?.binding ?? null,
            expectedEvidenceRevision: previous?.evidenceRevision ?? 0,
            actualBinding: actual,
            ...(sourceIdentity === undefined
              ? {}
              : { actualContinuationSourceIdentity: sourceIdentity }),
            ...(input.runId === undefined ? {} : { expectedRunId: input.runId }),
            ...(input.attemptId === undefined ? {} : { expectedRunAttemptId: input.attemptId }),
          });
          const current = (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId));
          if (
            !result.committed ||
            current?.runtime !== runtime ||
            !current.attachedThreadIds.has(input.threadId) ||
            (reservedGeneration === undefined && runtime.runtimeGeneration !== generation)
          )
            return yield* new ProviderSessionActivityError({
              providerSessionId: input.providerSessionId,
              cause: result.committed
                ? "Runtime generation changed during registration."
                : result.rejection,
            });
          if (!flushIdentity) return;
          const pending = (yield* Ref.get(entry.pendingRuntimeIdentity)).get(input.threadId);
          if (
            pending === undefined ||
            pending.binding.runtimeGeneration !== generation ||
            pending.binding.providerThreadId !== actual.providerThreadId ||
            pending.binding.nativeThreadId !== actual.nativeThreadId
          )
            return;
          if (!(yield* validateProviderEventOrigin(runtime, pending))) return;
          const published = yield* providerEventIngestor.ingestNormalized({
            threadId: input.threadId,
            providerSessionId: input.providerSessionId,
            providerInstanceId: runtime.instanceId,
            event: pending,
            revalidateCurrentOwner: revalidateRuntimeIdentityOwner(entry, pending.binding).pipe(
              Effect.andThen(revalidateProviderEventOrigin(runtime, pending)),
            ),
            ...(input.runId === undefined || input.attemptId === undefined
              ? {}
              : {
                  writeIfRunCurrent: {
                    runId: input.runId,
                    activeAttemptId: input.attemptId,
                    expectedStatus: "running" as const,
                  },
                }),
          });
          if (published.length > 0)
            yield* Ref.update(entry.pendingRuntimeIdentity, (events) => {
              if (events.get(input.threadId) !== pending) return events;
              const updated = new Map(events);
              updated.delete(input.threadId);
              return updated;
            });
        }).pipe(
          Effect.mapError((cause) =>
            Schema.is(ProviderSessionActivityError)(cause)
              ? cause
              : new ProviderSessionActivityError({
                  providerSessionId: input.providerSessionId,
                  cause,
                }),
          ),
        );
      const registerRuntimeBinding: ProviderSessionManagerV2Shape["registerRuntimeBinding"] = (
        input,
      ) => registerRuntimeBindingInternal(input);

      const onNativeEffectConfirmed: ProviderSessionManagerV2Shape["onNativeEffectConfirmed"] = (
        input,
      ) =>
        Effect.gen(function* () {
          const proof = input.confirmation;
          const binding = proof.binding;
          const reject = () =>
            new ProviderSessionActivityError({
              providerSessionId: binding.providerSessionId,
              cause:
                "The committed native confirmation no longer matches its current runtime context.",
            });
          const reference = getNativeCreationExecutionReference(input.context);
          if (
            Option.isNone(nativeRepository) ||
            Option.isNone(nativeSql) ||
            Option.isSome(yield* Effect.serviceOption(nativeSql.value.transactionService)) ||
            reference === null ||
            proof.nativeExecutionReference === null ||
            (["version", "claimId", "stageCommandId", "effectId", "stage"] as const).some(
              (key) => reference[key] !== proof.nativeExecutionReference![key],
            )
          )
            return yield* reject();
          const entry = (yield* Ref.get(sessions)).get(sessionKey(binding.providerSessionId));
          if (
            entry === undefined ||
            !entry.attachedThreadIds.has(binding.threadId) ||
            entry.runtime.instanceId !== binding.instanceId ||
            entry.runtime.runtimeGeneration !== binding.runtimeGeneration
          )
            return yield* reject();
          const stored = yield* nativeRepository.value.readNativeEffectConfirmation(proof.effectId);
          const registered = yield* eventSink.readCurrentProviderRuntimeOwner(binding.threadId);
          const projection = yield* projectionStore.getRuntimeRecoveryProjection(binding.threadId);
          const run = projection.runs.find((candidate) => candidate.id === proof.runId);
          const attempt = projection.attempts.find((candidate) => candidate.id === proof.attemptId);
          const providerThread = projection.providerThreads.find(
            (candidate) => candidate.id === binding.providerThreadId,
          );
          const current = (yield* Ref.get(sessions)).get(sessionKey(binding.providerSessionId));
          if (
            stored === null ||
            JSON.stringify(stored) !== JSON.stringify(proof) ||
            current?.runtime !== entry.runtime ||
            !current.attachedThreadIds.has(binding.threadId) ||
            entry.runtime.runtimeGeneration !== binding.runtimeGeneration ||
            registered === null ||
            registered.evidenceRevision !== proof.evidenceRevision ||
            registered.binding.driver !== entry.runtime.driver ||
            (
              [
                "threadId",
                "providerThreadId",
                "providerSessionId",
                "instanceId",
                "nativeThreadId",
                "runtimeGeneration",
              ] as const
            ).some((key) => registered.binding[key] !== binding[key]) ||
            projection.thread.activeProviderThreadId !== binding.providerThreadId ||
            projection.thread.modelSelection.instanceId !== binding.instanceId ||
            providerThread?.providerSessionId !== binding.providerSessionId ||
            providerThread.providerInstanceId !== binding.instanceId ||
            providerThread.nativeThreadRef?.nativeId !== binding.nativeThreadId ||
            providerThread.driver !== entry.runtime.driver ||
            run?.activeAttemptId !== proof.attemptId ||
            run.providerThreadId !== binding.providerThreadId ||
            run.providerInstanceId !== binding.instanceId ||
            attempt?.runId !== proof.runId ||
            attempt.providerThreadId !== binding.providerThreadId
          )
            return yield* reject();
          if (entry.runtime.onNativeEffectConfirmed !== undefined)
            yield* entry.runtime.onNativeEffectConfirmed(input);
        }).pipe(
          Effect.mapError((cause) =>
            Schema.is(ProviderSessionActivityError)(cause)
              ? cause
              : new ProviderSessionActivityError({
                  providerSessionId: input.confirmation.binding.providerSessionId,
                  cause,
                }),
          ),
        );

      const invalidateChangedRuntimeRequest = (input: {
        readonly runtime: ProviderAdapterV2SessionRuntime;
        readonly threadId: ThreadId;
        readonly providerThreadId: ProviderThreadId;
        readonly modelSelection: ModelSelection | undefined;
        readonly runId?: RunId;
        readonly attemptId?: RunAttemptId;
      }) =>
        Effect.gen(function* () {
          const { runtime, modelSelection: selection } = input;
          if (selection === undefined || selection.instanceId !== runtime.instanceId) return;
          const context = yield* projectionStore.getThreadProviderContext(
            input.threadId,
            runtime.instanceId,
          );
          const session = context.providerSessions.find(
            (candidate) => candidate.id === runtime.providerSessionId,
          );
          const identity = session?.runtimeIdentity;
          if (session === undefined || identity === undefined) return;
          const serviceTier =
            runtime.driver === "codex"
              ? (getCodexServiceTierOptionValue(selection) ?? null)
              : (getModelSelectionStringOptionValue(selection, "serviceTier") ?? null);
          const modelChanged = identity.requested.model !== selection.model;
          const tierChanged = identity.requested.serviceTier !== serviceTier;
          if (!modelChanged && !tierChanged) return;
          const reject = () =>
            new ProviderSessionActivityError({
              providerSessionId: runtime.providerSessionId,
              cause: "The resident request identity changed before invalidation.",
            });
          const entry = (yield* Ref.get(sessions)).get(sessionKey(runtime.providerSessionId));
          const owner = yield* eventSink.readCurrentProviderRuntimeOwner(input.threadId);
          if (
            entry?.runtime !== runtime ||
            !entry.attachedThreadIds.has(input.threadId) ||
            owner === null ||
            owner.binding.providerThreadId !== input.providerThreadId ||
            owner.binding.providerSessionId !== runtime.providerSessionId ||
            owner.binding.instanceId !== runtime.instanceId ||
            owner.binding.driver !== runtime.driver ||
            owner.binding.runtimeGeneration !== runtime.runtimeGeneration ||
            identity.runtimeGeneration !== runtime.runtimeGeneration ||
            identity.requested.providerInstanceId !== runtime.instanceId ||
            identity.requested.providerDriver !== runtime.driver
          )
            return yield* reject();
          const binding: ProviderRuntimeBinding = {
            threadId: owner.binding.threadId,
            providerThreadId: owner.binding.providerThreadId,
            providerSessionId: owner.binding.providerSessionId,
            instanceId: owner.binding.instanceId,
            runtimeGeneration: owner.binding.runtimeGeneration!,
            ...(owner.binding.nativeThreadId === null
              ? {}
              : { nativeThreadId: owner.binding.nativeThreadId }),
          };
          const revalidateCurrentOwner = revalidateRuntimeIdentityOwner(entry, binding).pipe(
            Effect.andThen(
              Effect.gen(function* () {
                const latest = yield* projectionStore.getThreadProviderContext(
                  input.threadId,
                  runtime.instanceId,
                );
                const latestIdentity = latest.providerSessions.find(
                  (candidate) => candidate.id === runtime.providerSessionId,
                )?.runtimeIdentity;
                if (JSON.stringify(latestIdentity) !== JSON.stringify(identity))
                  return yield* reject();
              }),
            ),
          );
          const result = yield* eventSink.writeIfCurrentProviderRuntimeOwner({
            expectedBinding: owner.binding,
            expectedEvidenceRevision: owner.evidenceRevision,
            ...(input.runId === undefined ? {} : { expectedRunId: input.runId }),
            ...(input.attemptId === undefined ? {} : { expectedRunAttemptId: input.attemptId }),
            revalidateCurrentOwner,
            events: [
              {
                id: yield* idAllocator.allocate.event({
                  threadId: input.threadId,
                  providerSessionId: runtime.providerSessionId,
                }),
                type: "provider-session.updated",
                threadId: input.threadId,
                providerInstanceId: runtime.instanceId,
                driver: runtime.driver,
                occurredAt: yield* DateTime.now,
                payload: {
                  ...session,
                  updatedAt: yield* DateTime.now,
                  runtimeIdentity: {
                    ...identity,
                    requested: { ...identity.requested, model: selection.model, serviceTier },
                    observed: {
                      ...identity.observed,
                      ...(modelChanged ? { model: { status: "unknown" as const } } : {}),
                      ...(tierChanged ? { serviceTier: { status: "unknown" as const } } : {}),
                    },
                  },
                },
              },
            ],
          });
          if (!result.committed) return yield* reject();
          yield* Ref.update(entry.pendingRuntimeIdentity, (pending) => {
            const event = pending.get(input.threadId);
            if (
              event === undefined ||
              event.binding.runtimeGeneration !== binding.runtimeGeneration ||
              event.binding.providerThreadId !== binding.providerThreadId ||
              (event.attestation.requested.model === selection.model &&
                event.attestation.requested.serviceTier === serviceTier)
            )
              return pending;
            const updated = new Map(pending);
            updated.delete(input.threadId);
            return updated;
          });
        });

      const decorateRuntime = (
        runtime: ProviderAdapterV2SessionRuntime,
        eventSubscribers: Ref.Ref<
          ReadonlyMap<number, Queue.Queue<ProviderSessionEventSignal, Cause.Done>>
        >,
      ): ProviderAdapterV2SessionRuntime => {
        const providerSessionId = runtime.providerSessionId;
        const subscribeEvents = makeEventSubscription(runtime, eventSubscribers);
        const operation = (
          kind: ProviderNativeEffectOperation,
          supplied: ProviderNativeOperationContext | undefined,
          target: {
            readonly threadId?: ThreadId;
            readonly providerThreadId?: ProviderRuntimeBinding["providerThreadId"];
            readonly attemptId?: ProviderNativeOperationContext["attemptId"];
          } = {},
        ): ProviderNativeOperationContext => ({
          ...supplied,
          ...target,
          operationId: supplied?.operationId ?? `${kind}:${randomUUID()}`,
          operation: kind,
          instanceId: runtime.instanceId,
          providerSessionId,
          ...(runtime.runtimeGeneration === undefined
            ? {}
            : { runtimeGeneration: runtime.runtimeGeneration }),
        });
        return {
          ...runtime,
          get runtimeGeneration() {
            return runtime.runtimeGeneration;
          },
          get ownedRuntimeIdentity() {
            return runtime.ownedRuntimeIdentity;
          },
          get providerSession() {
            return runtime.providerSession;
          },
          get continuationSourceIdentity() {
            return runtime.continuationSourceIdentity;
          },
          onNativeEffectConfirmed,
          subscribeEvents,
          events: Stream.unwrap(
            subscribeEvents.pipe(Effect.map((subscription) => subscription.events)),
          ),
          ensureThread: (input) =>
            observeThreadAttachment(
              runtime,
              ensureThreadAttached({
                providerSessionId,
                threadId: input.threadId,
                providerInstanceId: runtime.instanceId,
              }),
            ).pipe(
              Effect.andThen(
                runtime.ensureThread({
                  ...input,
                  nativeOperation: operation("ensure_thread", input.nativeOperation, {
                    threadId: input.threadId,
                    ...(input.existingProviderThread === undefined
                      ? {}
                      : { providerThreadId: input.existingProviderThread.id }),
                  }),
                }),
              ),
              Effect.tap((providerThread) =>
                markProviderThreadLoaded({
                  providerSessionId,
                  threadId: input.threadId,
                  providerThreadKey: providerThreadLoadKey({
                    providerThread,
                    modelSelection: input.modelSelection,
                    runtimePolicy: input.runtimePolicy,
                  }),
                }),
              ),
            ),
          resumeThread: (input) => {
            const threadId = input.threadId ?? input.providerThread.appThreadId;
            const contextualInput = {
              ...input,
              nativeOperation: operation("resume_thread", input.nativeOperation, {
                ...(threadId == null ? {} : { threadId }),
                providerThreadId: input.providerThread.id,
              }),
            };
            if (threadId === null || threadId === undefined) {
              return runtime.resumeThread(contextualInput);
            }
            const providerThreadKey = providerThreadLoadKey({
              providerThread: input.providerThread,
              ...(input.modelSelection === undefined
                ? {}
                : { modelSelection: input.modelSelection }),
              ...(input.runtimePolicy === undefined ? {} : { runtimePolicy: input.runtimePolicy }),
            });
            return observeThreadAttachment(
              runtime,
              ensureThreadAttached({
                providerSessionId,
                threadId,
                providerInstanceId: runtime.instanceId,
              }),
            ).pipe(
              Effect.andThen(
                withProviderNativeEffect(
                  invalidateChangedRuntimeRequest({
                    runtime,
                    threadId,
                    providerThreadId: input.providerThread.id,
                    modelSelection: input.modelSelection,
                  }),
                  contextualInput.nativeOperation,
                ),
              ),
              Effect.andThen(
                isProviderThreadLoaded({ providerSessionId, threadId, providerThreadKey }),
              ),
              Effect.flatMap((loaded) =>
                loaded
                  ? withProviderNativeEffect(
                      (input.beforeNativeResume === undefined
                        ? Effect.void
                        : input.beforeNativeResume(runtime.continuationSourceIdentity)
                      ).pipe(Effect.as(input.providerThread)),
                      contextualInput.nativeOperation,
                    )
                  : runtime.resumeThread(contextualInput),
              ),
              Effect.tap((providerThread) =>
                markProviderThreadLoaded({
                  providerSessionId,
                  threadId,
                  providerThreadKey: providerThreadLoadKey({
                    providerThread,
                    ...(input.modelSelection === undefined
                      ? {}
                      : { modelSelection: input.modelSelection }),
                    ...(input.runtimePolicy === undefined
                      ? {}
                      : { runtimePolicy: input.runtimePolicy }),
                  }),
                }),
              ),
            );
          },
          forkThread: (input) =>
            observeThreadAttachment(
              runtime,
              ensureThreadAttached({
                providerSessionId,
                threadId: input.targetThreadId,
                providerInstanceId: runtime.instanceId,
              }),
            ).pipe(
              Effect.andThen(
                runtime.forkThread({
                  ...input,
                  nativeOperation: operation("fork_thread", input.nativeOperation, {
                    threadId: input.targetThreadId,
                  }),
                }),
              ),
              Effect.tap((providerThread) =>
                markProviderThreadLoaded({
                  providerSessionId,
                  threadId: input.targetThreadId,
                  providerThreadKey: providerThreadLoadKey({
                    providerThread,
                    ...(input.modelSelection === undefined
                      ? {}
                      : { modelSelection: input.modelSelection }),
                    ...(input.runtimePolicy === undefined
                      ? {}
                      : { runtimePolicy: input.runtimePolicy }),
                  }),
                }),
              ),
            ),
          startTurn: (input) =>
            observeThreadAttachment(
              runtime,
              ensureThreadAttached({
                providerSessionId,
                threadId: input.threadId,
                providerInstanceId: runtime.instanceId,
              }),
            ).pipe(
              Effect.andThen(
                withProviderNativeEffect(
                  invalidateChangedRuntimeRequest({
                    runtime,
                    threadId: input.threadId,
                    providerThreadId: input.providerThread.id,
                    modelSelection: input.modelSelection,
                    runId: input.runId,
                    attemptId: input.attemptId,
                  }),
                  operation("start_turn", input.nativeOperation, {
                    threadId: input.threadId,
                    providerThreadId: input.providerThread.id,
                    attemptId: input.attemptId,
                  }),
                ),
              ),
              Effect.andThen(observeActivity(providerSessionId, markBusy(providerSessionId))),
              Effect.andThen(
                currentConfiguredModelDefault(runtime, input).pipe(
                  Effect.flatMap((configuredDefaultModelSelection) => {
                    const { configuredDefaultModelSelection: _suppliedDefault, ...turnInput } =
                      input;
                    const startOperation = operation("start_turn", input.nativeOperation, {
                      threadId: input.threadId,
                      providerThreadId: input.providerThread.id,
                      attemptId: input.attemptId,
                    });
                    return Effect.gen(function* () {
                      const recorded = yield* Ref.modify(sessions, (current) => {
                        const entry = current.get(sessionKey(providerSessionId));
                        if (entry?.runtime !== runtime || isPendingStartStopReservation(entry))
                          return [false, current] as const;
                        const previous = entry.startRequests.get(input.providerThread.id);
                        if (previous?.stopRequested && previous.attemptId === input.attemptId)
                          return [false, current] as const;
                        const startRequests = new Map(entry.startRequests);
                        startRequests.set(input.providerThread.id, {
                          threadId: input.threadId,
                          providerThreadId: input.providerThread.id,
                          runId: input.runId,
                          runOrdinal: input.runOrdinal,
                          attemptId: input.attemptId,
                          startOperation,
                          stopRequested: false,
                          startReturned: false,
                          providerTurnIds: new Set(),
                        });
                        const updated = new Map(current);
                        updated.set(sessionKey(providerSessionId), { ...entry, startRequests });
                        return [true, updated] as const;
                      });
                      if (!recorded)
                        return yield* new ProviderAdapterProtocolError({
                          driver: runtime.driver,
                          detail:
                            "The resident runtime changed before recording its start operation.",
                        });
                      // Capacity continuations retain this original correlation after the first ACK.
                      yield* runtime.startTurn({
                        ...turnInput,
                        ...(configuredDefaultModelSelection === undefined
                          ? {}
                          : { configuredDefaultModelSelection }),
                        nativeOperation: startOperation,
                      });
                      yield* Ref.update(sessions, (current) => {
                        const entry = current.get(sessionKey(providerSessionId));
                        const request = entry?.startRequests.get(input.providerThread.id);
                        if (
                          entry?.runtime !== runtime ||
                          request?.startOperation !== startOperation
                        )
                          return current;
                        const startRequests = new Map(entry.startRequests);
                        startRequests.set(input.providerThread.id, {
                          ...request,
                          startReturned: true,
                        });
                        const updated = new Map(current);
                        updated.set(sessionKey(providerSessionId), { ...entry, startRequests });
                        return updated;
                      });
                    });
                  }),
                ),
              ),
              Effect.tap(() =>
                registerRuntimeBinding({
                  threadId: input.threadId,
                  providerSessionId,
                  providerThreadId: input.providerThread.id,
                  runId: input.runId,
                  attemptId: input.attemptId,
                }).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderAdapterTurnStartError({
                        driver: runtime.driver,
                        threadId: input.threadId,
                        providerThreadId: input.providerThread.id,
                        runId: input.runId,
                        nativeEffect: {
                          ...operation("start_turn", input.nativeOperation, {
                            threadId: input.threadId,
                            providerThreadId: input.providerThread.id,
                            attemptId: input.attemptId,
                          }),
                          outcome: "unknown",
                        },
                        cause,
                      }),
                  ),
                ),
              ),
              Effect.catch((error) =>
                observeActivity(providerSessionId, markIdle(providerSessionId)).pipe(
                  Effect.andThen(Effect.fail(error)),
                ),
              ),
            ),
          steerTurn: (input) =>
            observeActivity(providerSessionId, touchActivity(providerSessionId)).pipe(
              Effect.andThen(
                runtime.steerTurn({
                  ...input,
                  nativeOperation: operation("steer_turn", input.nativeOperation, {
                    providerThreadId: input.providerThread.id,
                  }),
                }),
              ),
            ),
          interruptTurn: (input) =>
            observeActivity(providerSessionId, touchActivity(providerSessionId)).pipe(
              Effect.andThen(
                runtime.interruptTurn({
                  ...input,
                  nativeOperation: operation("interrupt_turn", input.nativeOperation, {
                    providerThreadId: input.providerThread.id,
                  }),
                }),
              ),
            ),
          respondToRuntimeRequest: (input) =>
            observeActivity(providerSessionId, touchActivity(providerSessionId)).pipe(
              Effect.andThen(
                runtime.respondToRuntimeRequest({
                  ...input,
                  nativeOperation: operation("respond_to_request", input.nativeOperation),
                }),
              ),
            ),
        };
      };

      const persistProviderSessionUpdate = (
        entry: LiveSessionEntry,
        event: Extract<ProviderAdapterV2Event, { readonly type: "provider_session.updated" }>,
      ) =>
        Effect.gen(function* () {
          const current = (yield* Ref.get(sessions)).get(
            sessionKey(entry.runtime.providerSessionId),
          );
          if (current?.runtime !== entry.runtime) {
            return;
          }
          yield* eventSink.withTransaction(
            revalidateProviderEventOrigin(entry.runtime, event).pipe(
              Effect.andThen(
                writeProviderSessionEvents({
                  runtime: entry.runtime,
                  threadIds: current.attachedThreadIds,
                  type: "provider-session.updated",
                  payload: event.providerSession,
                }),
              ),
              Effect.andThen(revalidateProviderEventOrigin(entry.runtime, event)),
            ),
          );
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("orchestration-v2.driver-session.status-persist-failed", {
              providerSessionId: entry.runtime.providerSessionId,
              cause,
            }),
          ),
        );

      const startEventPump = (entry: LiveSessionEntry) => {
        let stoppedByProvider:
          | Extract<ProviderAdapterV2Event, { readonly type: "provider_session.updated" }>
          | undefined;
        return entry.runtime.events.pipe(
          Stream.runForEach((event) =>
            Effect.gen(function* () {
              if (!(yield* validateProviderEventOrigin(entry.runtime, event))) return;
              if (event.type === "runtime_identity.observed") {
                if (
                  event.binding.runtimeGeneration !== entry.runtime.runtimeGeneration ||
                  event.attestation.runtimeGeneration !== event.binding.runtimeGeneration ||
                  event.binding.providerSessionId !== entry.runtime.providerSessionId ||
                  event.binding.instanceId !== entry.runtime.instanceId
                )
                  return;
                return yield* Ref.update(entry.pendingRuntimeIdentity, (current) => {
                  const updated = new Map(current);
                  updated.set(event.binding.threadId, event);
                  return updated;
                }).pipe(
                  Effect.andThen(
                    Effect.gen(function* () {
                      const current = (yield* Ref.get(sessions)).get(
                        sessionKey(entry.runtime.providerSessionId),
                      );
                      if (
                        current?.runtime !== entry.runtime ||
                        !current.attachedThreadIds.has(event.binding.threadId) ||
                        current.runtime.runtimeGeneration !== event.binding.runtimeGeneration
                      )
                        return;
                      const published = yield* providerEventIngestor.ingestNormalized({
                        providerSessionId: event.binding.providerSessionId,
                        providerInstanceId: event.binding.instanceId,
                        threadId: event.binding.threadId,
                        event,
                        revalidateCurrentOwner: revalidateRuntimeIdentityOwner(
                          entry,
                          event.binding,
                        ).pipe(Effect.andThen(revalidateProviderEventOrigin(entry.runtime, event))),
                      });
                      if (published.length > 0)
                        yield* Ref.update(entry.pendingRuntimeIdentity, (events) => {
                          if (events.get(event.binding.threadId) !== event) return events;
                          const updated = new Map(events);
                          updated.delete(event.binding.threadId);
                          return updated;
                        });
                    }).pipe(
                      Effect.catchCause(() =>
                        Effect.logWarning("orchestration-v2.runtime-identity.publication-held", {
                          threadId: event.binding.threadId,
                        }),
                      ),
                    ),
                  ),
                );
              }
              if (
                event.type === "provider_session.updated" &&
                event.providerSession.status === "stopped"
              ) {
                stoppedByProvider = event;
              }
              return yield* observeActivity(
                entry.runtime.providerSessionId,
                event.type === "turn.terminal"
                  ? markIdle(entry.runtime.providerSessionId)
                  : touchActivity(entry.runtime.providerSessionId),
              ).pipe(
                Effect.andThen(
                  event.type === "provider_session.updated"
                    ? persistProviderSessionUpdate(entry, event)
                    : Effect.void,
                ),
                Effect.andThen(
                  Effect.gen(function* () {
                    // Some providers can block before a run subscriber exists
                    // (project trust, login, or session-switch hooks). Persist
                    // their runless request artifacts directly so the normal T3
                    // request UI can answer them and unblock session setup.
                    const threadId = sessionScopedRuntimeRequestThreadId(event);
                    if (threadId !== undefined) {
                      yield* Effect.gen(function* () {
                        const current = (yield* Ref.get(sessions)).get(
                          sessionKey(entry.runtime.providerSessionId),
                        );
                        if (current?.runtime !== entry.runtime) return;
                        if (!(yield* validateProviderEventOrigin(entry.runtime, event))) return;
                        yield* providerEventIngestor
                          .ingestNormalized({
                            providerSessionId: entry.runtime.providerSessionId,
                            providerInstanceId: entry.runtime.instanceId,
                            threadId,
                            event,
                            revalidateCurrentOwner: revalidateProviderEventOrigin(
                              entry.runtime,
                              event,
                            ),
                          })
                          .pipe(
                            Effect.mapError(
                              (cause) =>
                                new ProviderAdapterEventStreamError({
                                  driver: entry.runtime.driver,
                                  providerSessionId: entry.runtime.providerSessionId,
                                  cause,
                                }),
                            ),
                          );
                      }).pipe(entry.requestEventPermit.withPermits(1));
                      return;
                    }
                    if (!(yield* validateProviderEventOrigin(entry.runtime, event))) return;
                    yield* publishToSubscribers(entry.eventSubscribers, { type: "event", event });
                    if (!(yield* validateProviderEventOrigin(entry.runtime, event))) return;
                    if (event.type === "provider_turn.updated")
                      yield* Ref.update(sessions, (current) => {
                        const latest = current.get(sessionKey(entry.runtime.providerSessionId));
                        const request = latest?.startRequests.get(
                          event.providerTurn.providerThreadId,
                        );
                        if (
                          latest?.runtime !== entry.runtime ||
                          request === undefined ||
                          request.threadId !== event.threadId ||
                          request.attemptId !== event.providerTurn.runAttemptId
                        )
                          return current;
                        const startRequests = new Map(latest.startRequests);
                        startRequests.set(event.providerTurn.providerThreadId, {
                          ...request,
                          providerTurnIds: new Set([
                            ...request.providerTurnIds,
                            event.providerTurn.id,
                          ]),
                        });
                        const updated = new Map(current);
                        updated.set(sessionKey(entry.runtime.providerSessionId), {
                          ...latest,
                          startRequests,
                        });
                        return updated;
                      });
                    const latest = (yield* Ref.get(sessions)).get(
                      sessionKey(entry.runtime.providerSessionId),
                    );
                    const reservation = latest?.runtimeReplacementReservation;
                    const request =
                      reservation !== undefined && "kind" in reservation
                        ? latest?.startRequests.get(reservation.request.providerThreadId)
                        : undefined;
                    if (
                      event.type === "turn.terminal" &&
                      reservation !== undefined &&
                      "kind" in reservation &&
                      event.providerThreadId === reservation.request.providerThreadId &&
                      event.runOrdinal === reservation.request.runOrdinal &&
                      request?.attemptId === reservation.request.attemptId &&
                      request.startOperation.operationId ===
                        reservation.request.startOperation.operationId &&
                      request.providerTurnIds.has(event.providerTurnId)
                    )
                      yield* Deferred.succeed(reservation.terminalPublished, undefined);
                    const managedStop = latest?.managedStopReservation;
                    if (
                      event.type === "turn.terminal" &&
                      managedStop !== undefined &&
                      event.providerThreadId === managedStop.input.binding.providerThreadId &&
                      managedStop.terminalIds.delete(event.providerTurnId) &&
                      managedStop.terminalIds.size === 0
                    )
                      yield* Deferred.succeed(managedStop.terminalPublished, undefined);
                  }),
                ),
              );
            }),
          ),
          Effect.exit,
          Effect.flatMap((exit) =>
            Effect.gen(function* () {
              const current = (yield* Ref.get(sessions)).get(
                sessionKey(entry.runtime.providerSessionId),
              );
              if (current?.runtime !== entry.runtime) {
                return;
              }
              if (
                stoppedByProvider !== undefined &&
                Exit.isSuccess(exit) &&
                (yield* validateProviderEventOrigin(entry.runtime, stoppedByProvider))
              ) {
                yield* releaseEntry({
                  providerSessionId: entry.runtime.providerSessionId,
                  reason: "manual_shutdown",
                  gracefulSubscribers: true,
                }).pipe(Effect.ignore);
                return;
              }
              const cause = Exit.isFailure(exit)
                ? exit.cause
                : Cause.fail(
                    new ProviderAdapterEventStreamError({
                      driver: entry.runtime.driver,
                      providerSessionId: entry.runtime.providerSessionId,
                      cause: "Provider event stream ended unexpectedly.",
                    }),
                  );
              yield* publishToSubscribers(entry.eventSubscribers, {
                type: "failure",
                cause,
              });
              yield* Ref.set(entry.eventSubscribers, new Map());
              yield* releaseEntry({
                providerSessionId: entry.runtime.providerSessionId,
                reason: "runtime_error",
                detail: Cause.pretty(cause),
              }).pipe(Effect.ignore);
            }),
          ),
          Effect.forkIn(layerScope),
        );
      };

      const shutdown = Effect.gen(function* () {
        const activeSessions = [...(yield* Ref.get(sessions)).values()];
        yield* Effect.forEach(
          activeSessions,
          (entry) =>
            releaseEntry({
              providerSessionId: entry.runtime.providerSessionId,
              reason: "server_shutdown",
            }).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("orchestration-v2.driver-session.shutdown-release-failed", {
                  providerSessionId: entry.runtime.providerSessionId,
                  cause,
                }),
              ),
            ),
          { discard: true },
        );
      });
      yield* Effect.addFinalizer(() => shutdown);

      const observeThreadRuntime: ProviderSessionManagerV2Shape["observeThreadRuntime"] = (
        binding,
      ) =>
        Effect.gen(function* () {
          const unknown = (reason: string): ProviderRuntimeObservation => ({
            status: "unknown",
            binding,
            reason,
          });
          const key = sessionKey(binding.providerSessionId);
          const entry = (yield* Ref.get(sessions)).get(key);
          if (entry === undefined || !entry.attachedThreadIds.has(binding.threadId))
            return unknown("runtime_not_resident");
          const runtime = entry.runtime;
          const matchesRuntime = () =>
            runtime.instanceId === binding.instanceId &&
            runtime.providerSessionId === binding.providerSessionId &&
            runtime.runtimeGeneration === binding.runtimeGeneration &&
            runtime.providerSession.status !== "stopped" &&
            runtime.providerSession.status !== "error";
          const matchesProjection = (context: ProjectionStore.ProjectionThreadProviderContext) => {
            const thread = context.providerThreads.find(
              (candidate) => candidate.id === binding.providerThreadId,
            );
            const session = context.providerSessions.find(
              (candidate) => candidate.id === binding.providerSessionId,
            );
            return (
              context.thread.activeProviderThreadId === binding.providerThreadId &&
              thread?.appThreadId === binding.threadId &&
              thread.providerSessionId === binding.providerSessionId &&
              thread.providerInstanceId === binding.instanceId &&
              thread.nativeThreadRef?.nativeId === binding.nativeThreadId &&
              session !== undefined &&
              session.providerInstanceId === binding.instanceId &&
              session.driver === runtime.driver &&
              thread.driver === runtime.driver &&
              session.status !== "stopped" &&
              session.status !== "error"
            );
          };
          if (!matchesRuntime()) return unknown("runtime_binding_changed");
          const before = yield* projectionStore
            .getThreadProviderContext(binding.threadId, binding.instanceId)
            .pipe(Effect.option);
          if (Option.isNone(before) || !matchesProjection(before.value))
            return unknown("runtime_binding_changed");
          const evidence = yield* eventSink
            .readCurrentProviderRuntimeOwner(binding.threadId)
            .pipe(Effect.option);
          if (Option.isNone(evidence) || evidence.value === null)
            return unknown("runtime_generation_unregistered");
          const registered = evidence.value;
          if (
            registered.binding.providerThreadId !== binding.providerThreadId ||
            registered.binding.providerSessionId !== binding.providerSessionId ||
            registered.binding.instanceId !== binding.instanceId ||
            registered.binding.driver !== runtime.driver ||
            registered.binding.runtimeGeneration !== binding.runtimeGeneration ||
            registered.binding.nativeThreadId !== (binding.nativeThreadId ?? null)
          )
            return unknown("runtime_generation_unregistered");
          if (runtime.observeThreadRuntime === undefined)
            return unknown("native_activity_unavailable");
          const startedAt = yield* Clock.currentTimeMillis;
          const observation = yield* runtime.observeThreadRuntime(binding).pipe(
            Effect.orElseSucceed(() => unknown("native_activity_probe_failed")),
            Effect.timeoutOption("3 seconds"),
            Effect.map(Option.getOrElse(() => unknown("native_activity_probe_timeout"))),
          );
          const current = (yield* Ref.get(sessions)).get(key);
          const after = yield* projectionStore
            .getThreadProviderContext(binding.threadId, binding.instanceId)
            .pipe(Effect.option);
          if (
            current?.runtime !== runtime ||
            !current.attachedThreadIds.has(binding.threadId) ||
            !matchesRuntime() ||
            Option.isNone(after) ||
            !matchesProjection(after.value)
          )
            return unknown("runtime_binding_changed");
          if (observation.status === "unknown") return unknown(observation.reason);
          const observedBinding = observation.binding;
          if (
            observedBinding.threadId !== binding.threadId ||
            observedBinding.providerThreadId !== binding.providerThreadId ||
            observedBinding.providerSessionId !== binding.providerSessionId ||
            observedBinding.instanceId !== binding.instanceId ||
            observedBinding.runtimeGeneration !== binding.runtimeGeneration ||
            observedBinding.nativeThreadId !== binding.nativeThreadId
          )
            return unknown("native_activity_binding_mismatch");
          if (
            !Number.isFinite(Date.parse(observation.observedAt)) ||
            Date.parse(observation.observedAt) < startedAt
          )
            return unknown("native_activity_stale");
          const currentOwner = yield* eventSink
            .readCurrentProviderRuntimeOwner(binding.threadId)
            .pipe(Effect.option);
          const latest = (yield* Ref.get(sessions)).get(key);
          return Option.isSome(currentOwner) &&
            currentOwner.value !== null &&
            currentOwner.value.evidenceRevision === registered.evidenceRevision &&
            (
              [
                "threadId",
                "providerThreadId",
                "providerSessionId",
                "instanceId",
                "driver",
                "nativeThreadId",
                "runtimeGeneration",
              ] as const
            ).every((key) => currentOwner.value!.binding[key] === registered.binding[key]) &&
            matchesRuntime() &&
            latest?.runtime === runtime &&
            latest.attachedThreadIds.has(binding.threadId)
            ? observation
            : unknown("runtime_binding_changed");
        });

      const readCurrentThreadRuntimeAttachment: ProviderSessionManagerV2Shape["readCurrentThreadRuntimeAttachment"] =
        (threadId) =>
          Effect.gen(function* () {
            const observedAt = DateTime.formatIso(yield* DateTime.now);
            const unknown = (reason: string): ProviderThreadRuntimeAttachment => ({
              status: "unknown",
              reason,
              observedAt,
            });
            const entries = yield* Ref.get(sessions);
            if (
              !Array.from(entries.values()).some((entry) => entry.attachedThreadIds.has(threadId))
            )
              return { status: "stopped", reason: "runtime_not_resident", observedAt } as const;
            const registered = yield* eventSink.readCurrentProviderRuntimeOwner(threadId);
            if (registered === null) return unknown("runtime_binding_changed");
            const binding = registered.binding;
            const entry = entries.get(sessionKey(binding.providerSessionId));
            if (entry === undefined || !entry.attachedThreadIds.has(threadId))
              return unknown("runtime_binding_changed");
            const generation = entry.runtime.runtimeGeneration;
            if (
              generation === undefined ||
              binding.runtimeGeneration !== generation ||
              binding.instanceId !== entry.runtime.instanceId ||
              binding.driver !== entry.runtime.driver ||
              binding.providerSessionId !== entry.runtime.providerSessionId
            )
              return unknown("runtime_generation_unregistered");
            const currentOwner = yield* eventSink.readCurrentProviderRuntimeOwner(threadId);
            if (
              currentOwner === null ||
              currentOwner.evidenceRevision !== registered.evidenceRevision ||
              (
                [
                  "threadId",
                  "providerThreadId",
                  "providerSessionId",
                  "instanceId",
                  "driver",
                  "nativeThreadId",
                  "runtimeGeneration",
                ] as const
              ).some((key) => currentOwner.binding[key] !== binding[key])
            )
              return unknown("runtime_binding_changed");
            const current = (yield* Ref.get(sessions)).get(sessionKey(binding.providerSessionId));
            if (
              current?.runtime !== entry.runtime ||
              !current.attachedThreadIds.has(threadId) ||
              current.runtime.runtimeGeneration !== generation
            )
              return unknown("runtime_binding_changed");
            return {
              status: "attached",
              driver: entry.runtime.driver,
              runtimeStatus: entry.runtime.providerSession.status,
              evidenceRevision: registered.evidenceRevision,
              observedAt,
              binding: {
                threadId,
                providerThreadId: binding.providerThreadId,
                providerSessionId: binding.providerSessionId,
                instanceId: entry.runtime.instanceId,
                runtimeGeneration: generation,
                ...(registered.binding.nativeThreadId === null
                  ? {}
                  : { nativeThreadId: registered.binding.nativeThreadId }),
              },
            } as const;
          }).pipe(
            Effect.catch(() =>
              DateTime.now.pipe(
                Effect.map((now) => ({
                  status: "unknown" as const,
                  reason: "runtime_binding_unavailable",
                  observedAt: DateTime.formatIso(now),
                })),
              ),
            ),
          );
      const observeCurrentThreadRuntime: ProviderSessionManagerV2Shape["observeCurrentThreadRuntime"] =
        (threadId) =>
          Effect.gen(function* () {
            if (
              !Array.from((yield* Ref.get(sessions)).values()).some((entry) =>
                entry.attachedThreadIds.has(threadId),
              )
            )
              return { status: "unknown", reason: "runtime_not_resident" } as const;
            const registered = yield* eventSink.readProviderRuntimeEvidence(threadId);
            if (registered === null || registered.binding.runtimeGeneration === null)
              return { status: "unknown", reason: "runtime_generation_unregistered" } as const;
            const {
              driver: _driver,
              nativeThreadId,
              runtimeGeneration,
              ...binding
            } = registered.binding;
            return yield* observeThreadRuntime({
              ...binding,
              runtimeGeneration,
              ...(nativeThreadId === null ? {} : { nativeThreadId }),
            });
          }).pipe(
            Effect.orElseSucceed(() => ({
              status: "unknown" as const,
              reason: "runtime_binding_unavailable",
            })),
          );
      const getOperatingCounts: ProviderSessionManagerV2Shape["getOperatingCounts"] = (input) =>
        Effect.gen(function* () {
          const snapshot = yield* projectionStore.getOperatingCountsCandidates(input);
          const counts = {
            total: 0,
            operating: 0,
            foregroundWaitingApproval: 0,
            foregroundWaitingInput: 0,
            foregroundWaitingPlan: 0,
            backgroundOperating: 0,
            backgroundUnknown: 0,
          };
          const backgroundSampledAt = DateTime.formatIso(yield* DateTime.now);
          for (const thread of snapshot.threads) {
            counts.total++;
            const sampledAtMs = yield* Clock.currentTimeMillis;
            const native = yield* observeCurrentThreadRuntime(thread.id);
            const activity = providerThreadActivityObservation(thread, native, sampledAtMs);
            if (activity.foreground === "waiting_approval") counts.foregroundWaitingApproval++;
            if (activity.foreground === "waiting_input") counts.foregroundWaitingInput++;
            if (activity.foreground === "waiting_plan") counts.foregroundWaitingPlan++;
            if (activity.background !== null) counts.backgroundOperating++;
            if (activity.backgroundStatus === "unknown") counts.backgroundUnknown++;
            if (activity.foreground === "working" || activity.background !== null)
              counts.operating++;
          }
          return {
            ...counts,
            snapshotSequence: snapshot.snapshotSequence,
            backgroundSampledAt,
            observedAt: DateTime.formatIso(yield* DateTime.now),
          };
        }).pipe(Effect.mapError((cause) => new ProviderOperatingCountsError({ cause })));

      const sameStopBinding = (
        actual: EventSink.ProviderBindingExpectationV2,
        expected: EventSink.ProviderBindingExpectationV2,
      ) =>
        (
          [
            "threadId",
            "providerThreadId",
            "providerSessionId",
            "instanceId",
            "driver",
            "nativeThreadId",
            "runtimeGeneration",
          ] as const
        ).every((field) => actual[field] === expected[field]);
      const readPinnedStopOwner = (
        input: ProviderPinnedRuntimeStopInputV1,
      ): Effect.Effect<EventSink.ProviderRuntimeEvidenceV2 | null, unknown> =>
        Effect.gen(function* () {
          if (input.deletionBindingSha256 !== undefined) {
            const task = yield* eventSink.readDeletionCleanupTask(input.operationId);
            if (
              task === null ||
              task.bindingSha256 !== input.deletionBindingSha256 ||
              task.task.kind !== "provider" ||
              task.threadId !== input.binding.threadId ||
              task.task.evidenceRevision !== input.expectedEvidenceRevision ||
              !sameStopBinding(task.task.expectedBinding, input.binding)
            )
              return null;
            const birth = yield* eventSink.readDeletionCleanupTaskOwnerBirth(input.operationId);
            if (
              birth === null ||
              !birth.matchesOriginal ||
              birth.latestApplicationBirth.eventId !== task.ownerBirth.eventId ||
              birth.latestApplicationBirth.sequence !== task.ownerBirth.sequence ||
              birth.latestApplicationBirth.threadId !== task.threadId
            )
              return null;
            const owner = yield* eventSink.readProviderRuntimeEvidence(input.binding.threadId);
            return owner !== null &&
              owner.evidenceRevision === input.expectedEvidenceRevision &&
              sameStopBinding(owner.binding, input.binding)
              ? owner
              : null;
          }
          const owner = yield* eventSink.readCurrentProviderRuntimeOwner(input.binding.threadId);
          return owner !== null &&
            owner.evidenceRevision === input.expectedEvidenceRevision &&
            sameStopBinding(owner.binding, input.binding)
            ? owner
            : null;
        });
      const stopPinnedRuntime: ProviderSessionManagerV2Shape["stopPinnedRuntime"] = (input) =>
        Effect.gen(function* () {
          const unknown = (reason: string): ProviderPinnedRuntimeStopResultV1 => ({
            status: "unknown",
            reason,
          });
          const binding = input.binding;
          const key = sessionKey(binding.providerSessionId);
          const original = (yield* Ref.get(sessions)).get(key);
          const matchesRuntime = (entry: LiveSessionEntry | undefined) =>
            entry?.runtime === original?.runtime &&
            entry !== undefined &&
            entry.runtime.instanceId === binding.instanceId &&
            entry.runtime.driver === binding.driver &&
            entry.runtime.providerSessionId === binding.providerSessionId &&
            entry.runtime.runtimeGeneration === binding.runtimeGeneration &&
            entry.attachedThreadIds.has(binding.threadId);
          if (
            original === undefined ||
            !matchesRuntime(original) ||
            (yield* readPinnedStopOwner(input)) === null
          )
            return unknown("pinned_stop_owner_unavailable");
          const request = original.startRequests.get(binding.providerThreadId);
          if (
            request !== undefined &&
            binding.nativeThreadId !== null &&
            original.runtime.stopPendingStart !== undefined
          ) {
            const stopInput: ProviderPendingStartStopInput = {
              binding: {
                threadId: binding.threadId,
                providerThreadId: binding.providerThreadId,
                providerSessionId: binding.providerSessionId,
                instanceId: binding.instanceId,
                nativeThreadId: binding.nativeThreadId,
                runtimeGeneration: binding.runtimeGeneration,
              },
              runId: request.runId,
              attemptId: request.attemptId,
              startOperation: request.startOperation,
            };
            const source = { runtime: original.runtime, readOwner: readPinnedStopOwner(input) };
            pendingCleanupSources.set(key, source);
            const result = yield* original.runtime.stopPendingStart(stopInput).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  if (pendingCleanupSources.get(key) === source) pendingCleanupSources.delete(key);
                }),
              ),
            );
            if (result.status === "unknown") return result;
            if (
              result.runId !== request.runId ||
              result.attemptId !== request.attemptId ||
              result.startOperationId !== request.startOperation.operationId ||
              (
                Object.keys(stopInput.binding) as ReadonlyArray<keyof typeof stopInput.binding>
              ).some((field) => result.binding[field] !== stopInput.binding[field])
            )
              return unknown("pinned_stop_pending_result_changed");
            if (result.status === "cancelled") {
              if (
                (yield* Ref.get(sessions)).get(key)?.attachedThreadIds.has(binding.threadId) ===
                true
              )
                return unknown("pinned_stop_attachment_retained");
              return {
                status: "stopped",
                operationId: input.operationId,
                binding,
                cancelledPendingStart: true,
                interruptedProviderTurnIds: [],
                readback: { threadAttached: false },
              };
            }
          } else if (request !== undefined && !request.startReturned)
            return unknown("pinned_stop_pending_control_unavailable");
          const cleanup = sessionOpen
            .withLock(
              binding.providerSessionId,
              Effect.acquireUseRelease(
                Effect.gen(function* () {
                  if ((yield* readPinnedStopOwner(input)) === null) return null;
                  const projection = yield* projectionStore.getThreadRecords(binding.threadId, [
                    "providerThreads",
                    "providerTurns",
                  ]);
                  const target = projection.providerThreads.find(
                    (thread) => thread.id === binding.providerThreadId,
                  );
                  if (
                    target === undefined ||
                    target.appThreadId !== binding.threadId ||
                    target.providerSessionId !== binding.providerSessionId ||
                    target.providerInstanceId !== binding.instanceId ||
                    target.driver !== binding.driver ||
                    (target.nativeThreadRef?.nativeId ?? null) !== binding.nativeThreadId
                  )
                    return null;
                  const turns = projection.providerTurns.filter(
                    (turn) =>
                      turn.providerThreadId === binding.providerThreadId &&
                      turn.status === "running",
                  );
                  const terminalPublished = yield* Deferred.make<void>();
                  const reservation = {
                    input,
                    terminalIds: new Set(turns.map((turn) => turn.id)),
                    terminalPublished,
                  };
                  if (turns.length === 0) yield* Deferred.succeed(terminalPublished, undefined);
                  return yield* Ref.modify(sessions, (current) => {
                    const entry = current.get(key);
                    if (
                      !matchesRuntime(entry) ||
                      entry!.runtimeReplacementReservation !== undefined ||
                      entry!.managedStopReservation !== undefined ||
                      (entry!.attachedThreadIds.size > 1 &&
                        (binding.nativeThreadId === null ||
                          entry!.runtime.unloadThread === undefined))
                    )
                      return [null, current] as const;
                    const updated = new Map(current);
                    updated.set(key, { ...entry!, managedStopReservation: reservation });
                    return [{ entry: entry!, reservation, target, turns }, updated] as const;
                  });
                }),
                (reserved): Effect.Effect<ProviderPinnedRuntimeStopResultV1, unknown> =>
                  Effect.gen(function* () {
                    if (reserved === null) return unknown("pinned_stop_target_unavailable");
                    const current = Effect.gen(function* () {
                      const entry = (yield* Ref.get(sessions)).get(key);
                      return (
                        matchesRuntime(entry) &&
                        entry!.managedStopReservation === reserved.reservation &&
                        (yield* readPinnedStopOwner(input)) !== null
                      );
                    });
                    if (!(yield* current)) return unknown("pinned_stop_binding_changed");
                    if (binding.nativeThreadId !== null)
                      yield* providerEventIngestor.flushAssistantOutput({
                        binding: {
                          threadId: binding.threadId,
                          providerThreadId: binding.providerThreadId,
                          providerSessionId: binding.providerSessionId,
                          instanceId: binding.instanceId,
                          nativeThreadId: binding.nativeThreadId,
                          runtimeGeneration: binding.runtimeGeneration,
                        },
                        revalidateCurrentOwner: current.pipe(
                          Effect.flatMap((valid) =>
                            valid
                              ? Effect.void
                              : Effect.fail("Pinned stop changed before output flush"),
                          ),
                        ),
                      });
                    for (const turn of reserved.turns) {
                      if (!(yield* current)) return unknown("pinned_stop_binding_changed");
                      yield* reserved.entry.runtime.interruptTurn({
                        providerThread: reserved.target,
                        providerTurnId: turn.id,
                        ...(input.deletionBindingSha256 === undefined
                          ? { requestRuntimeRestart: true }
                          : {}),
                        nativeOperation: {
                          operationId: `${input.operationId}:interrupt:${turn.id}`,
                          operation: "interrupt_turn",
                          threadId: binding.threadId,
                          providerThreadId: binding.providerThreadId,
                          providerSessionId: binding.providerSessionId,
                          instanceId: binding.instanceId,
                          runtimeGeneration: binding.runtimeGeneration,
                          attemptId: turn.runAttemptId,
                        },
                      });
                    }
                    yield* Deferred.await(reserved.reservation.terminalPublished);
                    if (!(yield* current)) return unknown("pinned_stop_binding_changed");
                    const soleOwner = reserved.entry.attachedThreadIds.size === 1;
                    if (soleOwner) {
                      yield* cancelIdleFiber(
                        (yield* Ref.get(sessions)).get(key)?.idleFiber ?? null,
                      );
                      yield* endSubscribers(reserved.entry);
                      yield* Scope.close(reserved.entry.scope, Exit.void);
                      if (!(yield* current)) return unknown("pinned_stop_binding_changed");
                      yield* writeReleasedSessionEvents({
                        entry: reserved.entry,
                        reason: "manual_shutdown",
                      });
                      yield* writeReleasedRuntimeRequestEvents({
                        entry: reserved.entry,
                        reason: "manual_shutdown",
                      }).pipe(reserved.entry.requestEventPermit.withPermits(1));
                    } else {
                      yield* reserved.entry.runtime.unloadThread!({
                        providerThread: reserved.target,
                        nativeOperation: {
                          operationId: input.operationId,
                          operation: "unload_thread",
                          threadId: binding.threadId,
                          providerThreadId: binding.providerThreadId,
                          providerSessionId: binding.providerSessionId,
                          instanceId: binding.instanceId,
                          runtimeGeneration: binding.runtimeGeneration,
                        },
                      });
                      if (!(yield* current)) return unknown("pinned_stop_binding_changed");
                    }
                    const removed = yield* Ref.modify(sessions, (entries) => {
                      const entry = entries.get(key);
                      if (
                        !matchesRuntime(entry) ||
                        entry!.managedStopReservation !== reserved.reservation
                      )
                        return [false, entries] as const;
                      const updated = new Map(entries);
                      if (soleOwner) updated.delete(key);
                      else {
                        const attachedThreadIds = new Set(entry!.attachedThreadIds);
                        attachedThreadIds.delete(binding.threadId);
                        const loadedProviderThreadKeyByThread = new Map(
                          entry!.loadedProviderThreadKeyByThread,
                        );
                        loadedProviderThreadKeyByThread.delete(binding.threadId);
                        const { managedStopReservation: _reservation, ...remaining } = entry!;
                        updated.set(key, {
                          ...remaining,
                          attachedThreadIds,
                          loadedProviderThreadKeyByThread,
                        });
                      }
                      return [true, updated] as const;
                    });
                    if (
                      !removed ||
                      (yield* Ref.get(sessions))
                        .get(key)
                        ?.attachedThreadIds.has(binding.threadId) === true
                    )
                      return unknown("pinned_stop_attachment_retained");
                    return {
                      status: "stopped",
                      operationId: input.operationId,
                      binding,
                      cancelledPendingStart: false,
                      interruptedProviderTurnIds: reserved.turns.map((turn) => turn.id),
                      readback: { threadAttached: false },
                    };
                  }),
                (reserved) =>
                  reserved === null
                    ? Effect.void
                    : Ref.update(sessions, (entries) => {
                        const entry = entries.get(key);
                        if (entry?.managedStopReservation !== reserved.reservation) return entries;
                        const { managedStopReservation: _reservation, ...remaining } = entry;
                        const updated = new Map(entries);
                        updated.set(key, remaining);
                        return updated;
                      }),
              ),
            )
            .pipe(
              Effect.catchCause(() => Effect.succeed(unknown("pinned_stop_cleanup_unconfirmed"))),
            );
          return yield* cleanup.pipe(
            Effect.forkIn(layerScope),
            Effect.flatMap((fiber) =>
              Fiber.join(fiber).pipe(
                Effect.timeoutOption(RELEASE_SCOPE_CLOSE_TIMEOUT_MS),
                Effect.map(Option.getOrElse(() => unknown("pinned_stop_cleanup_timeout"))),
              ),
            ),
          );
        }).pipe(
          Effect.catchCause(() =>
            Effect.succeed({ status: "unknown" as const, reason: "pinned_stop_unavailable" }),
          ),
        );

      return ProviderSessionManagerV2.of({
        isMcpCallerAttached: (input) =>
          Ref.get(sessions).pipe(
            Effect.map((entries) => {
              const entry = entries.get(sessionKey(input.providerSessionId));
              return (
                entry !== undefined &&
                entry.managedStopReservation === undefined &&
                entry.runtimeReplacementReservation === undefined &&
                entry.runtime.instanceId === input.providerInstanceId &&
                entry.attachedThreadIds.has(input.threadId) &&
                entry.mcpCredentialIdByThread.get(input.threadId) === input.mcpCredentialId
              );
            }),
          ),
        onNativeEffectConfirmed,
        readCurrentThreadRuntimeAttachment,
        observeCurrentThreadRuntime,
        getOperatingCounts,
        stopPinnedRuntime,
        shutdown,
        open: (input) =>
          sessionOpen.withLock(
            input.providerSessionId,
            Effect.gen(function* () {
              const cwd = input.runtimePolicy.cwd;
              if (cwd !== null) {
                const workspaceIsDirectory = yield* fileSystem.stat(cwd).pipe(
                  Effect.map((stat) => stat.type === "Directory"),
                  Effect.catch((error) => Effect.succeed(error.reason._tag !== "NotFound")),
                );
                if (!workspaceIsDirectory) {
                  return yield* new ProviderWorkspaceMissingError({
                    threadId: input.threadId,
                    cwd,
                  });
                }
              }
              const key = sessionKey(input.providerSessionId);
              const existing = (yield* Ref.get(sessions)).get(key);
              if (existing !== undefined) {
                if (isPendingStartStopReservation(existing))
                  return yield* new ProviderSessionOpenError({
                    instanceId: existing.runtime.instanceId,
                    providerSessionId: input.providerSessionId,
                    cause: "The resident runtime is closing its pending start.",
                  });
                if (
                  !existing.attachedThreadIds.has(input.threadId) &&
                  !existing.supportsMultipleProviderThreads
                ) {
                  return yield* new ProviderSessionOpenError({
                    instanceId: input.modelSelection.instanceId,
                    providerSessionId: input.providerSessionId,
                    cause: `Provider ${existing.runtime.driver} does not support attaching multiple app threads to one session.`,
                  });
                }
                yield* ensureThreadAttached({
                  providerSessionId: input.providerSessionId,
                  threadId: input.threadId,
                  providerInstanceId: existing.runtime.instanceId,
                }).pipe(
                  Effect.catchTag("RuntimeReplacementAttachmentError", (cause) =>
                    Effect.fail(
                      new ProviderSessionOpenError({
                        instanceId: existing.runtime.instanceId,
                        providerSessionId: input.providerSessionId,
                        cause,
                      }),
                    ),
                  ),
                );
                yield* touchActivity(input.providerSessionId);
                return existing.exposedRuntime;
              }

              const adapter = yield* registry.get(input.modelSelection.instanceId).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderSessionOpenError({
                      instanceId: input.modelSelection.instanceId,
                      providerSessionId: input.providerSessionId,
                      cause,
                    }),
                ),
              );
              const prepared = yield* prepareMcpSession(
                input.threadId,
                input.modelSelection.instanceId,
              );
              const mcpCredentialId = prepared.mcpCredentialId;
              // The reservation from prepare protects the credential (which
              // eager adapters bake into the provider process during
              // openSession) from racing releases until this session's entry
              // is recorded below. Dropped exactly once on every path.
              let reservationDropped = mcpCredentialId === undefined;
              const dropReservation = Effect.sync(() => {
                if (!reservationDropped && mcpCredentialId !== undefined) {
                  reservationDropped = true;
                  dropMcpCredentialReservation(input.threadId, mcpCredentialId);
                }
              });
              const sessionScope = yield* Scope.make();
              const nativeOperation: ProviderNativeOperationContext = {
                ...input.nativeOperation,
                operationId: input.nativeOperation?.operationId ?? `open-session:${randomUUID()}`,
                operation: "open_session",
                instanceId: input.modelSelection.instanceId,
                threadId: input.threadId,
                providerSessionId: input.providerSessionId,
                runtimeGeneration: randomUUID(),
              };
              const runtime = yield* withProviderNativeEffect(
                adapter.openSession({
                  nativeOperation,
                  ...(input.nativeCreationExecution === undefined
                    ? {}
                    : { nativeCreationExecution: input.nativeCreationExecution }),
                  withRuntimeReplacement: (nextGeneration, replace) =>
                    sessionOpen.withLock(
                      input.providerSessionId,
                      Effect.acquireUseRelease(
                        Effect.gen(function* () {
                          const reserved = yield* Ref.modify(sessions, (current) => {
                            const entry = current.get(key);
                            if (
                              entry === undefined ||
                              entry.runtime.instanceId !== input.modelSelection.instanceId ||
                              entry.attachedThreadIds.size !== 1 ||
                              entry.runtimeReplacementReservation !== undefined ||
                              entry.managedStopReservation !== undefined ||
                              nextGeneration.trim().length === 0 ||
                              entry.runtime.runtimeGeneration === undefined ||
                              entry.runtime.runtimeGeneration === nextGeneration
                            )
                              return [null, current] as const;
                            const reservation = { nextGeneration };
                            const updated = new Map(current);
                            updated.set(key, {
                              ...entry,
                              runtimeReplacementReservation: reservation,
                              loadedProviderThreadKeyByThread: new Map(),
                            });
                            return [
                              {
                                runtime: entry.runtime,
                                threadId: [...entry.attachedThreadIds][0]!,
                                reservation,
                              },
                              updated,
                            ] as const;
                          });
                          if (reserved === null)
                            return yield* new ProviderSessionActivityError({
                              providerSessionId: input.providerSessionId,
                              cause:
                                "Managed replacement requires one current resident owner with no replacement in progress.",
                            });
                          return reserved;
                        }),
                        (reserved) =>
                          replace.pipe(
                            Effect.andThen(
                              Effect.gen(function* () {
                                const current = (yield* Ref.get(sessions)).get(key);
                                const source = reserved.runtime.continuationSourceIdentity;
                                if (
                                  current?.runtime !== reserved.runtime ||
                                  current.runtimeReplacementReservation !== reserved.reservation ||
                                  current.attachedThreadIds.size !== 1 ||
                                  !current.attachedThreadIds.has(reserved.threadId) ||
                                  reserved.runtime.runtimeGeneration !== nextGeneration ||
                                  source?.runtimeGeneration !== nextGeneration ||
                                  source.driverKind !== reserved.runtime.driver
                                )
                                  return yield* new ProviderSessionActivityError({
                                    providerSessionId: input.providerSessionId,
                                    cause:
                                      "Managed replacement has no actual initialized single-owner incarnation.",
                                  });
                                const context = yield* projectionStore.getThreadProviderContext(
                                  reserved.threadId,
                                  reserved.runtime.instanceId,
                                );
                                const providerThread = context.providerThreads.find(
                                  (thread) => thread.id === context.thread.activeProviderThreadId,
                                );
                                if (
                                  providerThread === undefined ||
                                  providerThread.providerSessionId !== input.providerSessionId ||
                                  providerThread.providerInstanceId !==
                                    reserved.runtime.instanceId ||
                                  providerThread.driver !== reserved.runtime.driver ||
                                  providerThread.nativeThreadRef?.nativeId == null
                                )
                                  return yield* new ProviderSessionActivityError({
                                    providerSessionId: input.providerSessionId,
                                    cause:
                                      "Managed replacement lost its exact restored native thread binding.",
                                  });
                                yield* registerRuntimeBindingInternal({
                                  threadId: reserved.threadId,
                                  providerSessionId: input.providerSessionId,
                                  providerThreadId: providerThread.id,
                                });
                              }),
                            ),
                          ),
                        (reserved) =>
                          Ref.update(sessions, (current) => {
                            const entry = current.get(key);
                            if (
                              entry?.runtime !== reserved.runtime ||
                              entry.runtimeReplacementReservation !== reserved.reservation
                            )
                              return current;
                            const { runtimeReplacementReservation: _reservation, ...released } =
                              entry;
                            const updated = new Map(current);
                            updated.set(key, released);
                            return updated;
                          }),
                      ).pipe(
                        Effect.mapError(
                          (cause) =>
                            new ProviderAdapterOpenSessionError({
                              driver: adapter.driver,
                              providerSessionId: input.providerSessionId,
                              nativeEffect: {
                                ...nativeOperation,
                                runtimeGeneration: nextGeneration,
                                outcome: "unknown",
                              },
                              cause,
                            }),
                        ),
                      ),
                    ),
                  withPendingStartStop: (stopInput: ProviderPendingStartStopInput, stop) => {
                    const unknown = (reason: string): ProviderPendingStartStopResult => ({
                      status: "unknown",
                      reason,
                    });
                    const binding = stopInput.binding;
                    const capturedSource = pendingCleanupSources.get(key);
                    const readOwner =
                      capturedSource?.readOwner ??
                      eventSink.readCurrentProviderRuntimeOwner(binding.threadId);
                    const matchesOwner = (
                      owner: EventSink.ProviderRuntimeEvidenceV2 | null,
                      runtime: ProviderAdapterV2SessionRuntime,
                    ) =>
                      owner !== null &&
                      owner.binding.threadId === binding.threadId &&
                      owner.binding.providerThreadId === binding.providerThreadId &&
                      owner.binding.providerSessionId === binding.providerSessionId &&
                      owner.binding.instanceId === binding.instanceId &&
                      owner.binding.driver === runtime.driver &&
                      owner.binding.nativeThreadId === binding.nativeThreadId &&
                      owner.binding.runtimeGeneration === binding.runtimeGeneration &&
                      runtime.runtimeGeneration === binding.runtimeGeneration;
                    const matchesRun = Effect.gen(function* () {
                      const context = yield* projectionStore.getThreadRecords(
                        binding.threadId,
                        ["runs", "attempts"],
                        { runIds: [stopInput.runId] },
                      );
                      const run = context.runs.find(
                        (candidate) => candidate.id === stopInput.runId,
                      );
                      const attempt = context.attempts.find(
                        (candidate) => candidate.id === stopInput.attemptId,
                      );
                      return (
                        run?.activeAttemptId === stopInput.attemptId &&
                        run.providerThreadId === binding.providerThreadId &&
                        run.providerInstanceId === binding.instanceId &&
                        attempt?.runId === stopInput.runId &&
                        attempt.providerThreadId === binding.providerThreadId &&
                        attempt.providerInstanceId === binding.instanceId
                      );
                    });
                    const cleanup = sessionOpen
                      .withLock(
                        input.providerSessionId,
                        Effect.acquireUseRelease(
                          Effect.gen(function* () {
                            const owner = yield* readOwner;
                            if (!(yield* matchesRun)) return null;
                            const terminalPublished = yield* Deferred.make<void>();
                            return yield* Ref.modify(sessions, (current) => {
                              const entry = current.get(key);
                              const request = entry?.startRequests.get(binding.providerThreadId);
                              const operation = stopInput.startOperation;
                              if (
                                entry === undefined ||
                                request === undefined ||
                                request.stopRequested ||
                                (capturedSource !== undefined &&
                                  capturedSource.runtime !== entry.runtime) ||
                                entry.runtime.instanceId !== binding.instanceId ||
                                entry.runtime.providerSessionId !== binding.providerSessionId ||
                                binding.providerSessionId !== input.providerSessionId ||
                                entry.attachedThreadIds.size !== 1 ||
                                !entry.attachedThreadIds.has(binding.threadId) ||
                                entry.runtimeReplacementReservation !== undefined ||
                                entry.managedStopReservation !== undefined ||
                                !matchesOwner(owner, entry.runtime) ||
                                request.threadId !== binding.threadId ||
                                request.runId !== stopInput.runId ||
                                request.attemptId !== stopInput.attemptId ||
                                operation.operation !== "start_turn" ||
                                request.startOperation.operationId !== operation.operationId ||
                                operation.instanceId !== binding.instanceId ||
                                operation.threadId !== binding.threadId ||
                                operation.providerSessionId !== binding.providerSessionId ||
                                operation.providerThreadId !== binding.providerThreadId ||
                                operation.attemptId !== stopInput.attemptId
                              )
                                return [null, current] as const;
                              const stoppedRequest = { ...request, stopRequested: true };
                              const reservation = {
                                kind: "pending_start_stop" as const,
                                request: stoppedRequest,
                                terminalPublished,
                              };
                              const startRequests = new Map(entry.startRequests);
                              startRequests.set(binding.providerThreadId, stoppedRequest);
                              const updated = new Map(current);
                              updated.set(key, {
                                ...entry,
                                startRequests,
                                runtimeReplacementReservation: reservation,
                              });
                              return [
                                { entry, reservation, evidenceRevision: owner!.evidenceRevision },
                                updated,
                              ] as const;
                            });
                          }),
                          (reserved): Effect.Effect<ProviderPendingStartStopResult, unknown> =>
                            Effect.gen(function* () {
                              if (reserved === null)
                                return unknown("pending_start_ownership_unavailable");
                              const stillCurrent = Effect.gen(function* () {
                                const entry = (yield* Ref.get(sessions)).get(key);
                                const owner = yield* readOwner;
                                return (
                                  entry?.runtime === reserved.entry.runtime &&
                                  entry.runtimeReplacementReservation === reserved.reservation &&
                                  entry.attachedThreadIds.size === 1 &&
                                  entry.attachedThreadIds.has(binding.threadId) &&
                                  matchesOwner(owner, entry.runtime) &&
                                  owner!.evidenceRevision === reserved.evidenceRevision &&
                                  (yield* matchesRun)
                                );
                              });
                              if (!(yield* stillCurrent))
                                return unknown("pending_start_binding_changed");
                              yield* providerEventIngestor.flushAssistantOutput({
                                binding,
                                revalidateCurrentOwner: stillCurrent.pipe(
                                  Effect.flatMap((valid) =>
                                    valid
                                      ? Effect.void
                                      : Effect.fail("Pending stop changed before output flush"),
                                  ),
                                ),
                              });
                              yield* stop;
                              if (!(yield* stillCurrent))
                                return unknown("pending_start_binding_changed");
                              if (reserved.reservation.request.startReturned)
                                yield* Deferred.await(reserved.reservation.terminalPublished);
                              yield* cancelIdleFiber(
                                (yield* Ref.get(sessions)).get(key)?.idleFiber ?? null,
                              );
                              yield* endSubscribers(reserved.entry);
                              yield* Scope.close(reserved.entry.scope, Exit.void);
                              if (!(yield* stillCurrent))
                                return unknown("pending_start_binding_changed");
                              yield* writeReleasedSessionEvents({
                                entry: reserved.entry,
                                reason: "manual_shutdown",
                              });
                              yield* writeReleasedRuntimeRequestEvents({
                                entry: reserved.entry,
                                reason: "manual_shutdown",
                              }).pipe(reserved.entry.requestEventPermit.withPermits(1));
                              const removed = yield* Ref.modify(sessions, (current) => {
                                const entry = current.get(key);
                                if (
                                  entry?.runtime !== reserved.entry.runtime ||
                                  entry.runtimeReplacementReservation !== reserved.reservation
                                )
                                  return [false, current] as const;
                                const updated = new Map(current);
                                updated.delete(key);
                                return [true, updated] as const;
                              });
                              if (removed)
                                yield* Effect.forEach(
                                  reserved.entry.mcpCredentialIdByThread,
                                  ([threadId, credentialId]) =>
                                    Ref.get(sessions).pipe(
                                      Effect.flatMap((current) =>
                                        isMcpCredentialReserved(threadId, credentialId) ||
                                        Array.from(current.values()).some(
                                          (other) =>
                                            other.attachedThreadIds.has(threadId) ||
                                            other.mcpCredentialIdByThread.get(threadId) ===
                                              credentialId,
                                        )
                                          ? Effect.void
                                          : clearMcpSession(threadId, credentialId),
                                      ),
                                    ),
                                  { discard: true },
                                );
                              return removed
                                ? {
                                    status: "cancelled",
                                    binding,
                                    runId: stopInput.runId,
                                    attemptId: stopInput.attemptId,
                                    startOperationId: stopInput.startOperation.operationId,
                                  }
                                : unknown("pending_start_binding_changed");
                            }),
                          (reserved) =>
                            reserved === null
                              ? Effect.void
                              : Ref.update(sessions, (current) => {
                                  const entry = current.get(key);
                                  if (
                                    entry?.runtime !== reserved.entry.runtime ||
                                    entry.runtimeReplacementReservation !== reserved.reservation
                                  )
                                    return current;
                                  const {
                                    runtimeReplacementReservation: _reservation,
                                    ...released
                                  } = entry;
                                  const updated = new Map(current);
                                  updated.set(key, released);
                                  return updated;
                                }),
                        ),
                      )
                      .pipe(
                        Effect.catchCause(() =>
                          Effect.succeed(unknown("pending_start_cleanup_unconfirmed")),
                        ),
                      );
                    // A timed-out caller leaves the actual cleanup reservation with its owned fiber.
                    return cleanup.pipe(
                      Effect.forkIn(layerScope),
                      Effect.flatMap((fiber) =>
                        Fiber.join(fiber).pipe(
                          Effect.timeoutOption(RELEASE_SCOPE_CLOSE_TIMEOUT_MS),
                          Effect.map(
                            Option.getOrElse(() => unknown("pending_start_cleanup_timeout")),
                          ),
                        ),
                      ),
                    );
                  },
                  beforeRuntimeReplacement: (nextGeneration) =>
                    Effect.gen(function* () {
                      const existing = (yield* Ref.get(sessions)).get(
                        sessionKey(input.providerSessionId),
                      );
                      // Reservation invalidates retained evidence before creation;
                      // it supplies neither a live handle nor a historical key.
                      if (existing === undefined) {
                        const previous = yield* eventSink.readProviderRuntimeEvidence(
                          input.threadId,
                        );
                        if (previous === null) return;
                        const context = yield* projectionStore.getThreadProviderContext(
                          input.threadId,
                          input.modelSelection.instanceId,
                        );
                        const providerThread = context.providerThreads.find(
                          (thread) => thread.id === context.thread.activeProviderThreadId,
                        );
                        if (
                          providerThread === undefined ||
                          providerThread.providerSessionId !== input.providerSessionId ||
                          providerThread.providerInstanceId !== input.modelSelection.instanceId ||
                          providerThread.driver !== adapter.driver
                        )
                          return yield* new ProviderSessionActivityError({
                            providerSessionId: input.providerSessionId,
                            cause: "Replacement has no current provider binding.",
                          });
                        const actual: ProviderRuntimeBinding = {
                          threadId: input.threadId,
                          providerThreadId: providerThread.id,
                          providerSessionId: input.providerSessionId,
                          instanceId: input.modelSelection.instanceId,
                          runtimeGeneration: nextGeneration,
                          ...(providerThread.nativeThreadRef?.nativeId == null
                            ? {}
                            : { nativeThreadId: providerThread.nativeThreadRef.nativeId }),
                        };
                        const result = yield* eventSink.registerProviderRuntime({
                          expectedBinding: {
                            ...actual,
                            driver: adapter.driver,
                            nativeThreadId: actual.nativeThreadId ?? null,
                            runtimeGeneration: previous.binding.runtimeGeneration,
                          },
                          expectedRegisteredBinding: previous.binding,
                          expectedEvidenceRevision: previous.evidenceRevision,
                          actualBinding: actual,
                        });
                        if (!result.committed)
                          return yield* new ProviderSessionActivityError({
                            providerSessionId: input.providerSessionId,
                            cause: result.rejection,
                          });
                        return;
                      }
                      for (const threadId of existing.attachedThreadIds) {
                        const previous = yield* eventSink.readProviderRuntimeEvidence(threadId);
                        if (previous === null) continue;
                        if (
                          existing.runtime.runtimeGeneration !== nextGeneration &&
                          existing.runtime.runtimeGeneration !== previous.binding.runtimeGeneration
                        )
                          return yield* new ProviderSessionActivityError({
                            providerSessionId: input.providerSessionId,
                            cause: "Replacement reservation lost the current runtime generation.",
                          });
                        const context = yield* projectionStore.getThreadProviderContext(
                          threadId,
                          existing.runtime.instanceId,
                        );
                        const providerThreadId = context.thread.activeProviderThreadId;
                        if (providerThreadId === null)
                          return yield* new ProviderSessionActivityError({
                            providerSessionId: input.providerSessionId,
                            cause: "Replacement has no current provider binding.",
                          });
                        yield* registerRuntimeBindingInternal(
                          {
                            threadId,
                            providerSessionId: input.providerSessionId,
                            providerThreadId,
                          },
                          false,
                          nextGeneration,
                        );
                      }
                    }).pipe(
                      Effect.mapError(
                        (cause) =>
                          new ProviderAdapterOpenSessionError({
                            driver: adapter.driver,
                            providerSessionId: input.providerSessionId,
                            nativeEffect: {
                              ...nativeOperation,
                              runtimeGeneration: nextGeneration,
                              outcome: "unknown",
                            },
                            cause,
                          }),
                      ),
                    ),
                  threadId: input.threadId,
                  providerSessionId: input.providerSessionId,
                  modelSelection: input.modelSelection,
                  runtimePolicy: input.runtimePolicy,
                  ...(input.resumeFromSession === undefined
                    ? {}
                    : { resumeFromSession: input.resumeFromSession }),
                  ...(input.initialNativeThreadId === undefined
                    ? {}
                    : { initialNativeThreadId: input.initialNativeThreadId }),
                  ...(input.initialProviderItemIdentityVersion === undefined
                    ? {}
                    : {
                        initialProviderItemIdentityVersion:
                          input.initialProviderItemIdentityVersion,
                      }),
                }),
                nativeOperation,
              ).pipe(
                Effect.provideService(Scope.Scope, sessionScope),
                Effect.tapError(() =>
                  Scope.close(sessionScope, Exit.void).pipe(
                    Effect.ignore,
                    Effect.andThen(dropReservation),
                    // Revoke only a credential this open freshly minted: a
                    // reused credential is held by another live provider
                    // process and must survive this open's failure.
                    Effect.andThen(
                      prepared.issued
                        ? clearMcpSession(input.threadId, mcpCredentialId)
                        : Effect.void,
                    ),
                  ),
                ),
                Effect.onInterrupt(() => dropReservation),
                Effect.mapError(
                  (cause) =>
                    new ProviderSessionOpenError({
                      instanceId: input.modelSelection.instanceId,
                      providerSessionId: input.providerSessionId,
                      cause,
                    }),
                ),
              );
              const eventSubscribers = yield* Ref.make<
                ReadonlyMap<number, Queue.Queue<ProviderSessionEventSignal, Cause.Done>>
              >(new Map());
              const exposedRuntime = decorateRuntime(runtime, eventSubscribers);
              const now = yield* Clock.currentTimeMillis;
              const entry: LiveSessionEntry = {
                attachedThreadIds: new Set([input.threadId]),
                startRequests: new Map(),
                loadedProviderThreadKeyByThread: new Map(),
                mcpCredentialIdByThread:
                  mcpCredentialId === undefined
                    ? new Map()
                    : new Map([[input.threadId, mcpCredentialId]]),
                supportsMultipleProviderThreads:
                  runtime.providerSession.capabilities.sessions
                    .supportsMultipleProviderThreadsPerSession,
                runtime,
                exposedRuntime,
                eventSubscribers,
                requestEventPermit: yield* Semaphore.make(1),
                pendingRuntimeIdentity: yield* Ref.make(new Map()),
                scope: sessionScope,
                idleGeneration: 0,
                busyCount: 0,
                lastActivityAtMs: now,
                idleFiber: null,
                pinnedSinceMs: null,
              };
              yield* Ref.update(sessions, (current) => {
                const updated = new Map(current);
                updated.set(key, entry);
                return updated;
              });
              // The entry now guards the credential via its recorded id, so
              // the pre-open reservation can be dropped.
              yield* dropReservation;
              yield* withActivityError(
                input.providerSessionId,
                writeProviderSessionEvents({
                  runtime,
                  threadIds: [input.threadId],
                  type: "provider-session.attached",
                  payload: runtime.providerSession,
                }),
              ).pipe(
                Effect.tapError(() =>
                  releaseEntry({
                    providerSessionId: input.providerSessionId,
                    reason: "runtime_error",
                    detail: "Failed to persist the provider-session attachment.",
                  }).pipe(Effect.ignore),
                ),
                Effect.mapError(
                  (cause) =>
                    new ProviderSessionOpenError({
                      instanceId: input.modelSelection.instanceId,
                      providerSessionId: input.providerSessionId,
                      cause: new ProviderNativeOperationUnknownError({
                        nativeEffect: { ...nativeOperation, outcome: "unknown" },
                        cause,
                      }),
                    }),
                ),
              );
              yield* startEventPump(entry);
              yield* scheduleIdleRelease(input.providerSessionId);
              return exposedRuntime;
            }),
          ),
        observeThreadRuntime,
        registerRuntimeBinding,
        get: (providerSessionId) =>
          Effect.gen(function* () {
            const entry = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
            if (entry === undefined) {
              return Option.none<ProviderAdapterV2SessionRuntime>();
            }
            yield* touchActivity(providerSessionId);
            return Option.some(entry.exposedRuntime);
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderSessionLookupError({
                  providerSessionId,
                  cause,
                }),
            ),
          ),
        close: (providerSessionId) =>
          releaseEntry({ providerSessionId, reason: "manual_shutdown" }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderSessionCloseError({
                  providerSessionId,
                  cause,
                }),
            ),
          ),
        closeInstance: (instanceId) =>
          Effect.gen(function* () {
            const active = [...(yield* Ref.get(sessions)).values()].filter(
              (entry) => entry.runtime.instanceId === instanceId,
            );
            const outcomes = yield* Effect.forEach(
              active,
              (entry) =>
                releaseEntry({
                  providerSessionId: entry.runtime.providerSessionId,
                  reason: "manual_shutdown",
                  detail: `Provider instance ${instanceId} logged out.`,
                }).pipe(Effect.exit),
              { concurrency: "unbounded" },
            );
            const failure = outcomes.find(Exit.isFailure);
            if (failure !== undefined && Exit.isFailure(failure)) {
              return yield* Effect.failCause(failure.cause);
            }
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderSessionCloseError({
                  providerSessionId: ProviderSessionId.make(
                    `provider-session:provider-instance:${instanceId}`,
                  ),
                  cause,
                }),
            ),
          ),
        release: releaseEntry,
        detach: (input) =>
          Effect.gen(function* () {
            const key = sessionKey(input.providerSessionId);
            const currentEntry = (yield* Ref.get(sessions)).get(key);
            if (
              currentEntry?.runtimeReplacementReservation !== undefined ||
              currentEntry?.managedStopReservation !== undefined
            )
              return yield* new ProviderSessionActivityError({
                providerSessionId: input.providerSessionId,
                cause: "The resident lifecycle is reserved before detach.",
              });
            if (input.expectedBinding !== undefined) {
              const current = yield* readCurrentThreadRuntimeAttachment(input.threadId);
              const expected = input.expectedBinding;
              if (
                current.status !== "attached" ||
                current.binding.providerSessionId !== input.providerSessionId ||
                current.binding.providerThreadId !== expected.providerThreadId ||
                current.binding.instanceId !== expected.instanceId ||
                current.binding.threadId !== expected.threadId ||
                current.binding.runtimeGeneration !== expected.runtimeGeneration ||
                current.binding.nativeThreadId !== expected.nativeThreadId
              )
                return yield* new ProviderSessionActivityError({
                  providerSessionId: input.providerSessionId,
                  cause: "The stop target is no longer the current attached runtime.",
                });
            }
            let detachedProviderThreads: ReadonlyArray<OrchestrationV2ProviderThread> = [];
            if (currentEntry?.supportsMultipleProviderThreads === true) {
              const projection = yield* Effect.option(
                projectionStore.getThreadRecords(input.threadId, [
                  "providerThreads",
                  "providerTurns",
                ]),
              );
              if (input.expectedBinding !== undefined) {
                const expected = input.expectedBinding;
                const target = Option.isSome(projection)
                  ? projection.value.providerThreads.find(
                      (thread) => thread.id === expected.providerThreadId,
                    )
                  : undefined;
                if (
                  target === undefined ||
                  target.providerSessionId !== expected.providerSessionId ||
                  target.providerInstanceId !== expected.instanceId ||
                  target.nativeThreadRef?.nativeId !== expected.nativeThreadId ||
                  currentEntry.runtime.runtimeGeneration !== expected.runtimeGeneration
                ) {
                  return yield* new ProviderSessionActivityError({
                    providerSessionId: input.providerSessionId,
                    cause:
                      "The pinned native stop target could not be read or changed before detach.",
                  });
                }
              }
              if (Option.isSome(projection)) {
                const providerThreads = new Map(
                  projection.value.providerThreads
                    .filter(
                      (thread) =>
                        thread.providerSessionId === input.providerSessionId &&
                        (input.expectedBinding === undefined ||
                          thread.id === input.expectedBinding.providerThreadId),
                    )
                    .map((thread) => [thread.id, thread] as const),
                );
                detachedProviderThreads = [...providerThreads.values()];
                const activeTurns = projection.value.providerTurns.filter(
                  (turn) => turn.status === "running" && providerThreads.has(turn.providerThreadId),
                );
                yield* Effect.forEach(
                  activeTurns,
                  (turn) => {
                    const nativeOperation: ProviderNativeOperationContext = {
                      operationId: `detach-interrupt:${randomUUID()}`,
                      operation: "interrupt_turn",
                      instanceId: currentEntry.runtime.instanceId,
                      threadId: input.threadId,
                      providerSessionId: input.providerSessionId,
                      providerThreadId: turn.providerThreadId,
                      ...(currentEntry.runtime.runtimeGeneration === undefined
                        ? {}
                        : { runtimeGeneration: currentEntry.runtime.runtimeGeneration }),
                    };
                    return currentEntry.exposedRuntime
                      .interruptTurn({
                        nativeOperation,
                        providerThread: providerThreads.get(turn.providerThreadId)!,
                        providerTurnId: turn.id,
                      })
                      .pipe(
                        Effect.catchCause((cause) => {
                          const evidence = nativeEffectEvidenceFor(cause, nativeOperation);
                          return evidence.outcome === "unknown"
                            ? Effect.fail(
                                new ProviderNativeOperationUnknownError({
                                  nativeEffect: evidence,
                                  cause,
                                }),
                              )
                            : Effect.void;
                        }),
                      );
                  },
                  { concurrency: 1, discard: true },
                );
              }
            }
            let lifecycleReserved = false;
            const detached = yield* Ref.modify(sessions, (current) => {
              const entry = current.get(key);
              if (
                entry?.runtimeReplacementReservation !== undefined ||
                entry?.managedStopReservation !== undefined
              ) {
                lifecycleReserved = true;
                return [Option.none<LiveSessionEntry>(), current] as const;
              }
              if (entry === undefined || !entry.attachedThreadIds.has(input.threadId)) {
                return [Option.none<LiveSessionEntry>(), current] as const;
              }
              const attachedThreadIds = new Set(entry.attachedThreadIds);
              attachedThreadIds.delete(input.threadId);
              const loadedProviderThreadKeyByThread = new Map(
                entry.loadedProviderThreadKeyByThread,
              );
              loadedProviderThreadKeyByThread.delete(input.threadId);
              // For a plain (workspace-change) detach, the credential id stays
              // recorded: the thread may re-attach and reuse it, and
              // releaseEntry revokes it when the provider process finally goes
              // away. A terminal detach (archive/delete) prunes the record so
              // nothing vetoes the revocation below.
              const mcpCredentialIdByThread =
                input.revokeMcpCredential === true
                  ? (() => {
                      const pruned = new Map(entry.mcpCredentialIdByThread);
                      pruned.delete(input.threadId);
                      return pruned;
                    })()
                  : entry.mcpCredentialIdByThread;
              const updatedEntry = {
                ...entry,
                attachedThreadIds,
                loadedProviderThreadKeyByThread,
                mcpCredentialIdByThread,
              };
              const updated = new Map(current);
              updated.set(key, updatedEntry);
              return [Option.some(updatedEntry), updated] as const;
            });
            if (lifecycleReserved)
              return yield* new ProviderSessionActivityError({
                providerSessionId: input.providerSessionId,
                cause: "The resident lifecycle became reserved before detach.",
              });
            // Plain detaches deliberately do not revoke: a detached thread's
            // provider process may still be alive (shared multi-thread codex
            // session across a workspace handoff) and holds its MCP client's
            // credential for the thread it will re-attach with. Credentials
            // are revoked when the session entry is released (process gone)
            // or rotated on the next attach if they stopped resolving.
            // Terminal detaches (thread archived or deleted) revoke the
            // thread's credentials immediately, even on a retry where the
            // entry is already gone: there is no legitimate future re-attach,
            // and the token must not outlive the thread.
            if (input.revokeMcpCredential === true) {
              yield* clearMcpSession(input.threadId);
            }
            if (Option.isNone(detached)) {
              return;
            }
            if (
              detached.value.attachedThreadIds.size === 0 &&
              !detached.value.supportsMultipleProviderThreads
            ) {
              yield* releaseEntry({
                providerSessionId: input.providerSessionId,
                reason: "manual_shutdown",
                ...(input.detail === undefined ? {} : { detail: input.detail }),
              });
              return;
            }
            // The shared runtime stays up for other threads, so unload this
            // thread's native state rather than leaving it (and its MCP
            // servers) resident until the whole runtime is released.
            const unloadThread = detached.value.exposedRuntime.unloadThread;
            if (detached.value.supportsMultipleProviderThreads && unloadThread !== undefined) {
              // Serialized with re-attachment: a thread whose next turn
              // attaches first stays loaded, and one that attaches during the
              // unload waits for it, so its resume reloads the native thread.
              yield* threadAttachment.withLock(
                threadAttachmentKey(input),
                Effect.gen(function* () {
                  const entry = (yield* Ref.get(sessions)).get(key);
                  if (
                    entry?.runtime !== detached.value.runtime ||
                    entry.attachedThreadIds.has(input.threadId)
                  ) {
                    return;
                  }
                  yield* Effect.forEach(
                    detachedProviderThreads.filter((thread) => thread.nativeThreadRef !== null),
                    (providerThread) => {
                      const nativeOperation: ProviderNativeOperationContext = {
                        operationId: `detach-unload:${randomUUID()}`,
                        operation: "unload_thread",
                        instanceId: entry.runtime.instanceId,
                        threadId: input.threadId,
                        providerSessionId: input.providerSessionId,
                        providerThreadId: providerThread.id,
                        ...(entry.runtime.runtimeGeneration === undefined
                          ? {}
                          : { runtimeGeneration: entry.runtime.runtimeGeneration }),
                      };
                      return unloadThread({ providerThread, nativeOperation }).pipe(
                        // Bounded so a wedged provider cannot hold up the
                        // thread's next attach.
                        Effect.timeout(UNLOAD_THREAD_TIMEOUT_MS),
                        Effect.catchCause((cause) => {
                          const evidence = nativeEffectEvidenceFor(cause, nativeOperation);
                          return evidence.outcome === "confirmed_success"
                            ? Effect.void
                            : Effect.fail(
                                new ProviderNativeOperationUnknownError({
                                  nativeEffect: { ...evidence, outcome: "unknown" },
                                  cause,
                                }),
                              );
                        }),
                      );
                    },
                    { concurrency: 1, discard: true },
                  );
                }),
              );
            }
            yield* scheduleIdleRelease(input.providerSessionId);
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.fail(
                new ProviderSessionReleaseError({
                  providerSessionId: input.providerSessionId,
                  reason: "manual_shutdown",
                  cause,
                }),
              ),
            ),
          ),
      } satisfies ProviderSessionManagerV2Shape);
    }),
  );

export const layer = layerWithOptions();
