import * as NativeProvider from "../jones/nativeCreation/NativeCreationProviderGuard.ts";
import type { CapturedRuntimeStop } from "./ProviderAdapter.ts";
import type * as RuntimeAttachment from "../jones/runtime/CurrentThreadRuntimeAttachment.ts";
import type * as RuntimeObservation from "../jones/provider/observations/ProviderThreadRuntimeObservation.ts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  ModelSelection,
  OrchestrationV2DomainEvent,
  OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  OrchestrationV2RuntimeRequest,
  ProviderRuntimeBinding,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
  type ProviderThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FiberSet from "effect/FiberSet";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import { normalizeModelMetricLabel } from "../observability/Attributes.ts";
import {
  providerSessionsTotal,
  providerTurnDuration,
  providerTurnsTotal,
  withMetrics,
} from "../observability/Metrics.ts";
import { ProviderWorkspaceMissingError } from "../provider/Errors.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as McpProviderSession from "../mcp/McpProviderSession.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as WorkModeRetention from "../jones/workMode/Retention.ts";
import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import * as ProviderEventOrigin from "../jones/orchestration/ProviderEventOrigin.ts";
import {
  ProviderAdapterEventStreamError,
  ProviderRuntimeBindingError,
  runtimeBinding,
  unobservedRuntimeIdentity,
  type ProviderRuntimeLifecycle,
  ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2Error,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2EventSubscription,
  type ProviderAdapterV2SessionRuntime,
} from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const DEFAULT_IDLE_TIMEOUT_MS = 30 * 60 * 1000;
const DEFAULT_MAX_IDLE_PIN_MS = 4 * 60 * 60 * 1000;
const RELEASE_SCOPE_CLOSE_TIMEOUT_MS = 30 * 1000;

const busyTurnPrefix = (providerThreadId: ProviderThreadId) => `${providerThreadId}#`;
/** The identity a turn's start and its `turn.terminal` share. */
const busyTurnKey = (providerThreadId: ProviderThreadId, runOrdinal: number) =>
  `${busyTurnPrefix(providerThreadId)}${runOrdinal}`;
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
 * It intentionally does not resurrect persisted sessions. Process-loss recovery
 * terminalizes provider-bound work and retires non-replayable effects; a later
 * user command or durable replay-safe operation opens a session lazily.
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

export class ProviderRuntimeStopCaptureError extends Schema.TaggedError<ProviderRuntimeStopCaptureError>()(
  "ProviderRuntimeStopCaptureError",
  { threadId: ThreadId, cause: Schema.Defect() },
) {}
export const ProviderSessionManagerV2Error = Schema.Union([
  ProviderRuntimeStopCaptureError,
  ProviderSessionOpenError,
  ProviderWorkspaceMissingError,
  ProviderSessionLookupError,
  ProviderSessionCloseError,
  ProviderSessionReleaseError,
  ProviderSessionActivityError,
]);
export type ProviderSessionManagerV2Error = typeof ProviderSessionManagerV2Error.Type;

export interface ProviderSessionManagerV2Shape {
  readonly captureCurrentThreadRuntimeStop?: (
    threadId: ThreadId,
  ) => Effect.Effect<CapturedRuntimeStop | null, ProviderSessionManagerV2Error>;
  readonly isMcpCallerAttached: (input: {
    readonly threadId: ThreadId;
    readonly providerSessionId: ProviderSessionId;
    readonly providerInstanceId: ProviderInstanceId;
    readonly mcpCredentialId: string;
  }) => Effect.Effect<boolean>;

  readonly readCurrentThreadRuntimeAttachment?: (
    threadId: ThreadId,
  ) => Effect.Effect<RuntimeAttachment.CurrentThreadRuntimeAttachment>;

  readonly observeThreadActivity?: (
    threadId: ThreadId,
  ) => Effect.Effect<RuntimeObservation.ProviderRuntimeObservation>;

  readonly shutdown: Effect.Effect<void>;
  readonly open: (input: {
    readonly nativeCreationGuard?: NativeProvider.NativeProviderExecutionGuard;
    readonly threadId: ThreadId;
    readonly providerSessionId: ProviderSessionId;
    readonly modelSelection: ModelSelection;
    readonly runtimePolicy: ProviderAdapterV2RuntimePolicy;
    readonly resumeFromSession?: OrchestrationV2ProviderSession;
    readonly initialNativeThreadId?: string;
    readonly initialProviderItemIdentityVersion?: 2;
  }) => Effect.Effect<ProviderAdapterV2SessionRuntime, ProviderSessionManagerV2Error>;
  readonly get: (
    providerSessionId: ProviderSessionId,
  ) => Effect.Effect<Option.Option<ProviderAdapterV2SessionRuntime>, ProviderSessionManagerV2Error>;
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
  readonly scope: Scope.Closeable;
  readonly idleGeneration: number;
  /**
   * Turns this session is running, keyed by `busyTurnKey`. A turn's start adds
   * it and its `turn.terminal` (or a failed start) removes it, so a turn can
   * only clear itself and the session is idle when the set is empty.
   */
  readonly busyTurns: ReadonlySet<string>;
  readonly lastActivityAtMs: number;
  readonly idleFiber: Fiber.Fiber<void, never> | null;
  /** Set when idle release is deferred for pending background work; bounds total deferral. */
  readonly pinnedSinceMs: number | null;
  /**
   * Shared runtimes only: the provider thread each attached app thread last
   * started a turn on, and the timer that unloads it once it has been idle
   * for `idleTimeoutMs`.
   */
  readonly idleThreadUnloads: ReadonlyMap<ThreadId, IdleThreadUnload>;
}

interface IdleThreadUnload {
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly generation: number;
  readonly fiber: Fiber.Fiber<void, never> | null;
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
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const providerEventIngestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const makeRuntimeLifecycle = (
        providerSessionId: ProviderSessionId,
        instanceId: ProviderInstanceId,
        driver: ProviderAdapterV2SessionRuntime["driver"],
        nativeCreationGuard?: NativeProvider.NativeProviderExecutionGuard,
      ): ProviderRuntimeLifecycle => {
        const reservations = new Map<
          string,
          {
            readonly previous: ReadonlyMap<string, string | null>;
            readonly bindings: Map<string, ThreadId>;
          }
        >();
        const protocolError = (cause: unknown) =>
          new ProviderRuntimeBindingError({
            driver,
            detail:
              "Failed to establish the actual provider runtime binding; native effects must not be replayed.",
            cause,
          });
        const readThread = (threadId: ThreadId, id: string) =>
          projectionStore
            .getThreadRecords(threadId, ["providerThreads"])
            .pipe(
              Effect.map((records) => records.providerThreads.find((thread) => thread.id === id)),
            );
        return {
          reserve: (threadId) =>
            Effect.gen(function* () {
              yield* NativeProvider.revalidateNativeProviderGuard(nativeCreationGuard, {
                threadId,
              });
              const rows = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"]);
              const generation = String(
                yield* idAllocator.allocate.event({ threadId, providerSessionId }),
              );
              reservations.set(generation, {
                previous: new Map(
                  rows.providerThreads
                    .filter(
                      (thread) =>
                        thread.providerInstanceId === instanceId && thread.driver === driver,
                    )
                    .map((thread) => [
                      String(thread.id),
                      thread.runtimeIdentity?.runtimeGeneration ?? null,
                    ]),
                ),
                bindings: new Map(),
              });
              return generation;
            }).pipe(Effect.mapError(protocolError)),
          bind: (binding) =>
            Effect.gen(function* () {
              const reservation = reservations.get(binding.runtimeGeneration);
              const thread = binding.providerThread;
              if (
                reservation === undefined ||
                thread.appThreadId === null ||
                thread.nativeThreadRef === null ||
                thread.nativeThreadRef.nativeId === null ||
                thread.nativeThreadRef.driver !== driver ||
                thread.driver !== driver ||
                thread.providerInstanceId !== instanceId ||
                thread.providerSessionId !== providerSessionId ||
                binding.requested.providerInstanceId !== instanceId ||
                binding.requested.providerDriver !== driver
              )
                return yield* protocolError("The launch reservation or native ownership was lost.");
              const observedEvent: ProviderAdapterV2Event = {
                type: "runtime_identity.observed",
                driver,
                binding: {
                  threadId: thread.appThreadId,
                  providerThreadId: thread.id,
                  providerSessionId,
                  providerInstanceId: instanceId,
                  driver,
                  nativeThreadId: thread.nativeThreadRef.nativeId,
                  runtimeGeneration: binding.runtimeGeneration,
                },
                requested: binding.requested,
                observed: binding.observed,
              };
              if (binding.producerOrigin !== undefined) {
                ProviderEventOrigin.stampProviderEvent(observedEvent, {
                  producer: binding.producerOrigin,
                });
              }
              const revalidateObservation = ProviderEventOrigin.revalidateProviderEventOrigin(
                observedEvent,
                { driver, instanceId, providerSessionId },
              );
              yield* revalidateObservation;
              const current = yield* readThread(thread.appThreadId, thread.id);
              const previous = current?.runtimeIdentity?.runtimeGeneration ?? null;
              const expected = reservation.bindings.has(thread.id)
                ? binding.runtimeGeneration
                : (reservation.previous.get(thread.id) ?? null);
              if (previous !== expected)
                return yield* protocolError(
                  "Another producer already owns this native conversation.",
                );
              const id = yield* idAllocator.allocate.event({
                threadId: thread.appThreadId,
                providerSessionId,
              });
              const boundaryThread = {
                ...thread,
                runtimeIdentity: {
                  runtimeGeneration: binding.runtimeGeneration,
                  requested: binding.requested,
                  observed: unobservedRuntimeIdentity(),
                },
              };
              const boundary = yield* eventSink.write({
                runtimeIdentityBoundary: { expectedGeneration: expected },
                events: [
                  {
                    id,
                    type: "provider-thread.updated",
                    threadId: thread.appThreadId,
                    occurredAt: yield* DateTime.now,
                    payload: boundaryThread,
                  },
                ],
              });
              if (boundary.length === 0)
                return yield* protocolError(
                  "The native binding changed before its boundary committed.",
                );
              reservation.bindings.set(thread.id, thread.appThreadId);
              yield* revalidateObservation;
              yield* providerEventIngestor.ingestNormalized({
                providerSessionId,
                providerInstanceId: instanceId,
                threadId: thread.appThreadId,
                event: observedEvent,
              });
              const committedThread = yield* readThread(thread.appThreadId, thread.id);
              if (nativeCreationGuard !== undefined) {
                const captured =
                  committedThread === undefined
                    ? undefined
                    : runtimeBinding(committedThread, binding.runtimeGeneration);
                if (
                  captured === undefined ||
                  committedThread?.runtimeIdentity?.runtimeGeneration !== binding.runtimeGeneration
                )
                  return yield* protocolError(
                    "Native execution has no committed physical runtime binding.",
                  );
                yield* NativeProvider.bindNativeProviderGuard(nativeCreationGuard, captured);
              }
              return committedThread ?? boundaryThread;
            }).pipe(Effect.mapError(protocolError)),
          abandon: (generation) =>
            Effect.gen(function* () {
              const reservation = reservations.get(generation);
              reservations.delete(generation);
              if (reservation === undefined) return;
              for (const [id, threadId] of reservation.bindings) {
                const current = yield* readThread(threadId, id);
                if (current?.runtimeIdentity?.runtimeGeneration !== generation) continue;
                yield* eventSink.write({
                  runtimeIdentityBoundary: { expectedGeneration: generation },
                  events: [
                    {
                      id: yield* idAllocator.allocate.event({ threadId, providerSessionId }),
                      type: "provider-thread.updated",
                      threadId,
                      occurredAt: yield* DateTime.now,
                      payload: {
                        ...current,
                        runtimeIdentity: {
                          requested: current.runtimeIdentity.requested,
                          observed: unobservedRuntimeIdentity(),
                        },
                      },
                    },
                  ],
                });
              }
            }).pipe(Effect.mapError(protocolError)),
          invalidate: (binding) =>
            Effect.gen(function* () {
              const current = yield* readThread(binding.threadId, binding.providerThreadId);
              if (
                current?.runtimeIdentity?.runtimeGeneration !== binding.runtimeGeneration ||
                current.nativeThreadRef?.nativeId !== binding.nativeThreadId
              )
                return;
              yield* eventSink.write({
                runtimeEvidence: binding,
                runtimeIdentityBoundary: { expectedGeneration: binding.runtimeGeneration },
                events: [
                  {
                    id: yield* idAllocator.allocate.event({
                      threadId: binding.threadId,
                      providerSessionId,
                    }),
                    type: "provider-thread.updated",
                    threadId: binding.threadId,
                    occurredAt: yield* DateTime.now,
                    payload: {
                      ...current,
                      runtimeIdentity: {
                        requested: current.runtimeIdentity.requested,
                        observed: unobservedRuntimeIdentity(),
                      },
                    },
                  },
                ],
              });
            }).pipe(Effect.mapError(protocolError)),
        };
      };

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
      // Ctrl+C, or a stop that signals the whole process group, reaches the
      // provider CLIs with the server. They report their own background work
      // stopped before shutdown captures restart continuations, so provider
      // events after the signal are dropped; restart recovery owns that state.
      const shutdownSignal = { received: false };
      const onShutdownSignal = () => {
        shutdownSignal.received = true;
      };
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          process.on("SIGINT", onShutdownSignal);
          process.on("SIGTERM", onShutdownSignal);
        }),
        () =>
          Effect.sync(() => {
            process.off("SIGINT", onShutdownSignal);
            process.off("SIGTERM", onShutdownSignal);
          }),
      );
      const sessions = yield* Ref.make(new Map<string, LiveSessionEntry>());
      // One retry per released entry, so a later release with the same id
      // cannot drop cleanup for threads only the earlier session served.
      const releaseRecordRetries = yield* FiberSet.make();
      const nextSubscriberId = yield* Ref.make(0);
      const sessionOpen = yield* KeyedLock.make<ProviderSessionId>();
      // Orders a thread's attach against a detach unloading it on the same session.
      const threadAttachment = yield* KeyedLock.make<string>();
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
      const mcpPrepareLock = yield* KeyedLock.make<ThreadId>();
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
                >(["orchestration", "worktree", "pull-requests"]);
                if (browserToolsAvailable) capabilities.add("preview");
                if (deviceToolsAvailable) capabilities.add("device");
                const existing = McpProviderSession.readMcpProviderSession(threadId);
                if (existing !== undefined) {
                  // Reserve before the async resolve so a release cannot
                  // revoke the credential between validation and reservation.
                  reserveMcpCredential(threadId, existing.providerSessionId);
                  const rawToken = existing.authorizationHeader.replace(/^Bearer\s+/, "");
                  // The caller only learns of the reservation once this returns,
                  // so a stop while resolving must drop it here.
                  const resolved = yield* mcpSessionRegistry
                    .resolve(rawToken)
                    .pipe(
                      Effect.onInterrupt(() =>
                        Effect.sync(() =>
                          dropMcpCredentialReservation(threadId, existing.providerSessionId),
                        ),
                      ),
                    );
                  if (
                    resolved !== undefined &&
                    resolved.thread.threadId === threadId &&
                    resolved.thread.providerInstanceId === providerInstanceId &&
                    // A flipped browser-access setting must not survive through
                    // credential reuse: rotate so the new scope reflects it.
                    resolved.capabilities.has("preview") === browserToolsAvailable &&
                    resolved.capabilities.has("device") === deviceToolsAvailable
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
                McpProviderSession.setMcpProviderSession(credential.config);
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

      const writeProviderSessionEvents = (input: {
        readonly runtime: ProviderAdapterV2SessionRuntime;
        readonly threadIds: Iterable<ThreadId>;
        readonly type: "provider-session.attached" | "provider-session.updated";
        readonly payload: OrchestrationV2ProviderSession;
      }) =>
        Effect.gen(function* () {
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
        /** Requests created later belong to a replacement session with the same id. */
        readonly releasedAt: DateTime.Utc;
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
                request.responseCapability.providerSessionId === providerSessionId &&
                DateTime.isLessThanOrEqualTo(request.createdAt, input.releasedAt),
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

      // Records a released session as stopped and resolves the live runtime
      // requests it left. Each write runs even if the other fails. Once a
      // replacement session opens with the same id, it owns the session status,
      // so only the requests are settled.
      const writeReleaseRecords = (input: {
        readonly entry: LiveSessionEntry;
        readonly reason: ProviderSessionReleaseReason;
        readonly detail?: string;
        readonly releasedAt: DateTime.Utc;
        readonly replaced: boolean;
      }) =>
        Effect.all(
          [
            input.replaced
              ? Effect.succeed(Exit.void)
              : Effect.exit(writeReleasedSessionEvents(input)),
            Effect.exit(
              writeReleasedRuntimeRequestEvents(input).pipe(
                input.entry.requestEventPermit.withPermits(1),
              ),
            ),
          ],
          { concurrency: 1 },
        ).pipe(Effect.flatMap(Exit.asVoidAll));

      // The session already left the live map, so a later release finds
      // nothing to do. Without a retry the UI would keep a ready session and
      // answerable approvals until a server restart. Each attempt holds the
      // session's open lock, so it sees a replacement that opened meanwhile.
      const retryReleaseRecords = (
        input: Omit<Parameters<typeof writeReleaseRecords>[0], "replaced">,
      ) => {
        const providerSessionId = input.entry.runtime.providerSessionId;
        const attempt = Effect.gen(function* () {
          const exit = yield* Effect.exit(
            sessionOpen.withLock(
              providerSessionId,
              Effect.gen(function* () {
                const replaced = (yield* Ref.get(sessions)).has(sessionKey(providerSessionId));
                yield* writeReleaseRecords({ ...input, replaced });
              }),
            ),
          );
          if (Exit.isSuccess(exit) || Cause.hasInterruptsOnly(exit.cause)) return yield* exit;
          yield* Effect.logWarning("orchestration-v2.provider-session-release-records-failed", {
            providerSessionId,
            cause: exit.cause,
          });
          // A failed SQL commit is a defect, so every failure but interruption
          // is retried.
          return yield* Effect.fail(exit.cause);
        });
        return attempt.pipe(
          Effect.retry({
            schedule: Schedule.exponential("1 second").pipe(
              Schedule.modifyDelay(({ duration }) =>
                Effect.succeed(Duration.min(duration, Duration.seconds(30))),
              ),
            ),
          }),
          Effect.delay("1 second"),
          FiberSet.run(releaseRecordRetries),
        );
      };

      const logReleaseFailure =
        (providerSessionId: ProviderSessionId) =>
        <E, R>(release: Effect.Effect<void, E, R>) =>
          release.pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("orchestration-v2.provider-session-release-failed", {
                providerSessionId,
                cause,
              }),
            ),
          );

      // Removes the live entry and reads the request cleanup cutoff while
      // holding the entry's request permit. A request the event pump is
      // persisting for this runtime lands before the cutoff, and once the
      // entry is gone the pump persists no more for it. A replacement's
      // requests come after its own open.
      const removeLiveEntry = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly onlyIfIdleGeneration?: number;
        readonly onlyIfRuntime?: ProviderAdapterV2SessionRuntime;
      }): Effect.Effect<readonly [Option.Option<LiveSessionEntry>, DateTime.Utc]> =>
        Effect.gen(function* () {
          const key = sessionKey(input.providerSessionId);
          const candidate = (yield* Ref.get(sessions)).get(key);
          if (candidate === undefined) {
            return [Option.none<LiveSessionEntry>(), yield* DateTime.now] as const;
          }
          const removed = yield* Effect.zip(
            Ref.modify(sessions, (current) => {
              const existing = current.get(key);
              if (existing !== candidate) {
                return [existing === undefined ? "gone" : "changed", current] as const;
              }
              if (
                (input.onlyIfIdleGeneration !== undefined &&
                  (existing.busyTurns.size > 0 ||
                    existing.idleGeneration !== input.onlyIfIdleGeneration)) ||
                (input.onlyIfRuntime !== undefined && existing.runtime !== input.onlyIfRuntime)
              ) {
                return ["kept", current] as const;
              }
              const updated = new Map(current);
              updated.delete(key);
              return ["removed", updated] as const;
            }),
            DateTime.now,
          ).pipe(candidate.requestEventPermit.withPermits(1));
          const [outcome, releasedAt] = removed;
          // Another entry took this id while the permit was held; release it instead.
          if (outcome === "changed") return yield* removeLiveEntry(input);
          return [
            outcome === "removed" ? Option.some(candidate) : Option.none<LiveSessionEntry>(),
            releasedAt,
          ] as const;
        });

      // Scope close can wedge on a misbehaving adapter finalizer (e.g. a
      // provider process that never yields its message stream). Time-box it so
      // the caller, and any lock or worker it holds, moves on and leaves a
      // diagnosable trail. A close that finishes late is still logged.
      const closeScopeWithin = (
        scope: Scope.Closeable,
        annotations: { readonly providerSessionId?: ProviderSessionId; readonly reason: string },
      ) =>
        Effect.gen(function* () {
          const closeFiber = yield* Scope.close(scope, Exit.void).pipe(
            Effect.exit,
            Effect.forkDetach({ startImmediately: true }),
          );
          const closeExit = yield* Fiber.join(closeFiber).pipe(
            Effect.timeoutOption(RELEASE_SCOPE_CLOSE_TIMEOUT_MS),
          );
          if (Option.isNone(closeExit)) {
            yield* Effect.logWarning("orchestration-v2.provider-session-scope-close-timeout", {
              ...annotations,
              timeoutMs: RELEASE_SCOPE_CLOSE_TIMEOUT_MS,
            });
            yield* Fiber.join(closeFiber).pipe(
              Effect.flatMap((exit) =>
                Exit.isFailure(exit)
                  ? Effect.logWarning("orchestration-v2.provider-session-scope-close-failed", {
                      ...annotations,
                      cause: exit.cause,
                    })
                  : Effect.logInfo(
                      "orchestration-v2.provider-session-scope-close-completed-late",
                      annotations,
                    ),
              ),
              Effect.forkDetach,
            );
          }
          return closeExit;
        });

      const releaseEntry = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly reason: ProviderSessionReleaseReason;
        readonly detail?: string;
        readonly cancelIdleFiber?: boolean;
        readonly onlyIfIdleGeneration?: number;
        readonly onlyIfRuntime?: ProviderAdapterV2SessionRuntime;
        readonly gracefulSubscribers?: boolean;
      }) =>
        Effect.acquireUseRelease(
          removeLiveEntry(input),
          ([entry, releasedAt]) =>
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
                  const closeExit = yield* closeScopeWithin(entry.scope, {
                    providerSessionId: input.providerSessionId,
                    reason: input.reason,
                  });
                  const records = {
                    entry,
                    reason: input.reason,
                    ...(input.detail === undefined ? {} : { detail: input.detail }),
                    releasedAt,
                  };
                  const recorded = yield* Effect.exit(
                    writeReleaseRecords({ ...records, replaced: false }),
                  );
                  if (Exit.isFailure(recorded)) {
                    yield* retryReleaseRecords(records);
                    return yield* recorded;
                  }
                  if (Option.isSome(closeExit) && Exit.isFailure(closeExit.value)) {
                    return yield* Effect.failCause(closeExit.value.cause);
                  }
                }).pipe(
                  withMetrics({
                    counter: providerSessionsTotal,
                    attributes: {
                      provider: entry.runtime.driver,
                      operation: "release",
                      reason: input.reason,
                    },
                  }),
                ),
            }),
          ([entry]) =>
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
            entry.busyTurns.size > 0 ||
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
          const retainForWorkMode = Option.isNone(serverSettings)
            ? false
            : yield* Effect.gen(function* () {
                const settings = yield* serverSettings.value.getSettings;
                if (!settings.workModeEnabled) return false;
                for (const threadId of entry.attachedThreadIds) {
                  const eligible = yield* Effect.gen(function* () {
                    const thread = yield* projectionStore.getThread(threadId);
                    if (
                      thread.archivedAt !== null ||
                      thread.deletedAt !== null ||
                      thread.settledAt !== null ||
                      thread.settledOverride === "settled" ||
                      thread.lineage.parentThreadId !== null ||
                      thread.lineage.relationshipToParent !== null ||
                      thread.activeProviderThreadId === null
                    )
                      return false;
                    return WorkModeRetention.workModeRetainsSession({
                      projection: yield* projectionStore.getThreadProjection(threadId),
                      providerSessionId: input.providerSessionId,
                      providerInstanceId: probedRuntime.instanceId,
                      nowMs: yield* Clock.currentTimeMillis,
                    });
                  }).pipe(Effect.catchCause(() => Effect.succeed(false)));
                  if (eligible) return true;
                }
                return false;
              }).pipe(Effect.catchCause(() => Effect.succeed(false)));
          if (hasPendingWork) {
            const now = yield* Clock.currentTimeMillis;
            const pinnedSinceMs = entry.pinnedSinceMs ?? now;
            if (now - pinnedSinceMs < maxIdlePinMs) {
              const shouldContinuePin = yield* Ref.modify(sessions, (latest) => {
                const latestEntry = latest.get(key);
                if (
                  latestEntry === undefined ||
                  latestEntry.busyTurns.size > 0 ||
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
              // Re-check on this fiber. Do not call
              // scheduleIdleReleaseInternal: that cancels entry.idleFiber, which
              // is this fiber, and can self-deadlock on Fiber.interrupt.
              yield* Effect.sleep(
                Duration.millis(
                  retainForWorkMode
                    ? WorkModeRetention.WORK_MODE_RETENTION_RECHECK_MS
                    : idleTimeoutMs,
                ),
              );
              return yield* releaseIfStillIdle(input);
            }
            yield* Effect.logWarning("orchestration-v2.driver-session.idle-release-pin-expired", {
              providerSessionId: input.providerSessionId,
              pinnedForMs: now - pinnedSinceMs,
            });
          }
          if (retainForWorkMode) {
            const latestEntry = (yield* Ref.get(sessions)).get(key);
            if (
              latestEntry === undefined ||
              latestEntry.busyTurns.size > 0 ||
              latestEntry.idleGeneration !== input.generation ||
              latestEntry.runtime !== probedRuntime
            )
              return;
            // Keep the same fiber: re-arming through scheduleIdleReleaseInternal
            // would interrupt itself. Work-mode residency has no background-pin cap.
            yield* Effect.sleep(Duration.millis(WorkModeRetention.WORK_MODE_RETENTION_RECHECK_MS));
            return yield* releaseIfStillIdle(input);
          }
          // Pending-work and Work-mode probes yield, so the idle decision can
          // go stale. Revalidate busyTurns, generation and runtime identity
          // inside releaseEntry's atomic entry removal.
          yield* releaseEntry({
            providerSessionId: input.providerSessionId,
            reason: "idle_timeout",
            cancelIdleFiber: false,
            onlyIfIdleGeneration: input.generation,
            onlyIfRuntime: probedRuntime,
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
          if (entry === undefined || entry.busyTurns.size > 0) {
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
            if (latestEntry === undefined || latestEntry.busyTurns.size > 0) {
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

      /** Returns the runtime the thread was attached to, or undefined if it already was. */
      const attachThread = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
      }) =>
        withActivityError(
          input.providerSessionId,
          Ref.modify(sessions, (current) => {
            const entry = current.get(sessionKey(input.providerSessionId));
            if (entry === undefined || entry.attachedThreadIds.has(input.threadId)) {
              return [undefined, current] as const;
            }
            const updated = new Map(current);
            updated.set(sessionKey(input.providerSessionId), {
              ...entry,
              attachedThreadIds: new Set([...entry.attachedThreadIds, input.threadId]),
            });
            return [entry.runtime, updated] as const;
          }),
        );

      /**
       * Undoes an attach to `runtime`. A replacement session that reopened under
       * the same id since is left alone.
       */
      const removeThreadAttachment = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly runtime: ProviderAdapterV2SessionRuntime;
      }) =>
        Ref.update(sessions, (current) => {
          const key = sessionKey(input.providerSessionId);
          const entry = current.get(key);
          if (
            entry === undefined ||
            entry.runtime !== input.runtime ||
            !entry.attachedThreadIds.has(input.threadId)
          ) {
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
          let attachedTo: ProviderAdapterV2SessionRuntime | undefined;
          let preparedForCleanup: PreparedMcpCredential | undefined;
          let reservationDropped = false;
          const dropReservation = () => {
            if (!reservationDropped && preparedForCleanup?.mcpCredentialId !== undefined) {
              reservationDropped = true;
              dropMcpCredentialReservation(input.threadId, preparedForCleanup.mcpCredentialId);
            }
          };
          // The whole attach, including undoing a failed one, holds the
          // thread's lock: a concurrent attach of the same thread waits, so it
          // never sees an attachment that this call is about to roll back.
          const attach = Effect.gen(function* () {
            const attached = yield* attachThread(input).pipe(
              // Recorded with no gap for an interrupt: cleanup undoes only an
              // attach this call made, never one an earlier open made.
              Effect.tap((runtime) => Effect.sync(() => (attachedTo = runtime))),
              Effect.uninterruptible,
            );
            if (attached !== undefined) {
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
            // An interrupted attach is undone too, so the next attach writes
            // the attachment instead of finding the thread already attached.
            Effect.onError(() =>
              attachedTo === undefined
                ? Effect.void
                : removeThreadAttachment({ ...input, runtime: attachedTo }).pipe(
                    Effect.andThen(
                      Effect.suspend(() => {
                        dropReservation();
                        // Revoke only a credential this attach freshly minted: a
                        // REUSED credential is by definition held by another
                        // live provider process, and revoking it thread-wide
                        // would break that process's MCP client mid-conversation.
                        if (preparedForCleanup?.issued !== true) return Effect.void;
                        const mcpCredentialId = preparedForCleanup.mcpCredentialId;
                        const attachedRuntime = attachedTo;
                        // As in release: a replacement session (or an open
                        // configuring one) may have taken the credential up.
                        return Ref.get(sessions).pipe(
                          Effect.flatMap((current) =>
                            (mcpCredentialId !== undefined &&
                              isMcpCredentialReserved(input.threadId, mcpCredentialId)) ||
                            Array.from(current.values()).some(
                              (other) =>
                                other.runtime !== attachedRuntime &&
                                (other.attachedThreadIds.has(input.threadId) ||
                                  (mcpCredentialId !== undefined &&
                                    other.mcpCredentialIdByThread.get(input.threadId) ===
                                      mcpCredentialId)),
                            )
                              ? Effect.void
                              : clearMcpSession(input.threadId, mcpCredentialId),
                          ),
                        );
                      }),
                    ),
                  ),
            ),
          );
          return threadAttachment.withLock(threadAttachmentKey(input), attach).pipe(
            // The entry's own record (written above while the thread is
            // attached) guards the credential from here on; the reservation
            // is only needed until then. Ensuring covers defects/interrupts.
            Effect.ensuring(Effect.sync(dropReservation)),
          );
        });

      const markBusy = (providerSessionId: ProviderSessionId, turnKey: string) =>
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
                busyTurns: new Set(entry.busyTurns).add(turnKey),
                idleFiber: null,
                lastActivityAtMs: now,
                pinnedSinceMs: null,
              });
              return [entry.idleFiber, updated] as const;
            });
            yield* cancelIdleFiber(idleFiber);
          }),
        );

      // Clearing a turn that is not marked busy (one whose failed start already
      // cleared it, or a subagent turn the manager never started) only
      // records activity.
      const markIdle = (
        providerSessionId: ProviderSessionId,
        providerThreadId: ProviderThreadId,
        runOrdinal: number,
      ) =>
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
              const busyTurns = new Set(entry.busyTurns);
              busyTurns.delete(busyTurnKey(providerThreadId, runOrdinal));
              const updated = new Map(current);
              updated.set(key, {
                ...entry,
                busyTurns,
                lastActivityAtMs: now,
              });
              return updated;
            });
            yield* scheduleIdleReleaseInternal(providerSessionId);
            yield* scheduleThreadUnload(providerSessionId, providerThreadId);
          }),
        );

      const hasBusyTurn = (entry: LiveSessionEntry, providerThreadId: ProviderThreadId) => {
        const prefix = busyTurnPrefix(providerThreadId);
        for (const turnKey of entry.busyTurns) {
          if (turnKey.startsWith(prefix)) return true;
        }
        return false;
      };

      const updateIdleThreadUnload = (
        providerSessionId: ProviderSessionId,
        threadId: ThreadId,
        update: (current: IdleThreadUnload | undefined) => IdleThreadUnload | undefined,
      ) =>
        Ref.modify(sessions, (current) => {
          const key = sessionKey(providerSessionId);
          const entry = current.get(key);
          if (entry === undefined) return [undefined, current] as const;
          const previous = entry.idleThreadUnloads.get(threadId);
          const next = update(previous);
          const idleThreadUnloads = new Map(entry.idleThreadUnloads);
          if (next === undefined) idleThreadUnloads.delete(threadId);
          else idleThreadUnloads.set(threadId, next);
          const updated = new Map(current);
          updated.set(key, { ...entry, idleThreadUnloads });
          return [previous, updated] as const;
        });

      /**
       * Starts tracking the provider thread an app thread runs its turns on, and
       * stops any unload pending for it. Called before the thread is resumed or
       * given a turn, so an unload cannot land between a resume that found the
       * thread loaded and the turn that relies on it.
       */
      const holdThreadLoaded = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly providerThread: OrchestrationV2ProviderThread;
      }) =>
        Effect.gen(function* () {
          const entry = (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId));
          if (
            entry === undefined ||
            !entry.supportsMultipleProviderThreads ||
            entry.exposedRuntime.unloadThread === undefined ||
            input.providerThread.nativeThreadRef === null
          ) {
            return;
          }
          const previous = yield* updateIdleThreadUnload(
            input.providerSessionId,
            input.threadId,
            (current) => ({
              providerThread: input.providerThread,
              generation: (current?.generation ?? 0) + 1,
              fiber: null,
            }),
          );
          yield* cancelIdleFiber(previous?.fiber ?? null);
        });

      /**
       * A shared runtime never goes idle while any of its threads is in use, so
       * its threads get the idle timeout one by one: a thread with no turn for
       * `idleTimeoutMs` is unloaded from the runtime, along with the native MCP
       * servers it started. Its next turn's resume loads it again.
       */
      const scheduleThreadUnload = (
        providerSessionId: ProviderSessionId,
        providerThreadId: ProviderThreadId,
      ) =>
        Effect.gen(function* () {
          const entry = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
          if (entry === undefined || hasBusyTurn(entry, providerThreadId)) return;
          const tracked = Array.from(entry.idleThreadUnloads).find(
            ([, pending]) => pending.providerThread.id === providerThreadId,
          );
          if (tracked === undefined) return;
          const [threadId, pending] = tracked;
          const generation = pending.generation + 1;
          const fiber = yield* Effect.sleep(Duration.millis(idleTimeoutMs)).pipe(
            Effect.andThen(unloadIdleThread({ providerSessionId, threadId, generation })),
            Effect.forkIn(layerScope),
          );
          // A turn that started meanwhile already moved the generation on.
          const previous = yield* updateIdleThreadUnload(providerSessionId, threadId, (current) =>
            current?.generation === pending.generation
              ? { ...current, generation, fiber }
              : current,
          );
          yield* cancelIdleFiber(
            previous?.generation === pending.generation ? previous.fiber : fiber,
          );
        });

      const unloadIdleThread = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly generation: number;
      }): Effect.Effect<void> =>
        Effect.gen(function* () {
          const outcome = yield* threadAttachment.withLock(
            threadAttachmentKey(input),
            Effect.gen(function* () {
              const key = sessionKey(input.providerSessionId);
              const entry = (yield* Ref.get(sessions)).get(key);
              const pending = entry?.idleThreadUnloads.get(input.threadId);
              const unloadThread = entry?.exposedRuntime.unloadThread;
              if (
                entry === undefined ||
                pending === undefined ||
                pending.generation !== input.generation ||
                unloadThread === undefined ||
                !entry.attachedThreadIds.has(input.threadId) ||
                hasBusyTurn(entry, pending.providerThread.id)
              ) {
                return "skipped" as const;
              }
              // Unloading stops the native thread's background terminals, so a
              // thread still running background work stays loaded.
              const hasPendingWork =
                entry.runtime.hasPendingBackgroundWorkForThread === undefined
                  ? false
                  : yield* entry.runtime
                      .hasPendingBackgroundWorkForThread(pending.providerThread)
                      .pipe(Effect.catchCause(() => Effect.succeed(false)));
              if (hasPendingWork) return "deferred" as const;
              const unloading = yield* Ref.modify(sessions, (current) => {
                const latest = current.get(key);
                if (
                  latest?.runtime !== entry.runtime ||
                  latest.idleThreadUnloads.get(input.threadId)?.generation !== input.generation
                ) {
                  return [false, current] as const;
                }
                const loadedProviderThreadKeyByThread = new Map(
                  latest.loadedProviderThreadKeyByThread,
                );
                loadedProviderThreadKeyByThread.delete(input.threadId);
                const idleThreadUnloads = new Map(latest.idleThreadUnloads);
                idleThreadUnloads.delete(input.threadId);
                const updated = new Map(current);
                updated.set(key, {
                  ...latest,
                  loadedProviderThreadKeyByThread,
                  idleThreadUnloads,
                });
                return [true, updated] as const;
              });
              if (!unloading) return "skipped" as const;
              yield* unloadThread({ providerThread: pending.providerThread }).pipe(
                Effect.timeout(UNLOAD_THREAD_TIMEOUT_MS),
                Effect.catchCause((cause) =>
                  Effect.logWarning("orchestration-v2.driver-session.idle-unload-failed", {
                    providerSessionId: input.providerSessionId,
                    threadId: input.threadId,
                    providerThreadId: pending.providerThread.id,
                    cause,
                  }),
                ),
              );
              return "unloaded" as const;
            }),
          );
          // Re-check on this fiber after another idle window, outside the
          // lock so the thread's next attach is not held up meanwhile.
          if (outcome === "deferred") {
            yield* Effect.sleep(Duration.millis(idleTimeoutMs));
            return yield* unloadIdleThread(input);
          }
        });

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

      const validateProviderEvent = (
        runtime: ProviderAdapterV2SessionRuntime,
        event: ProviderAdapterV2Event,
      ) =>
        Effect.gen(function* () {
          const current = (yield* Ref.get(sessions)).get(sessionKey(runtime.providerSessionId));
          if (current !== undefined && current.runtime !== runtime) return false;
          yield* ProviderEventOrigin.revalidateProviderEventOrigin(event, runtime);
          const latest = (yield* Ref.get(sessions)).get(sessionKey(runtime.providerSessionId));
          return latest === undefined || latest.runtime === runtime;
        }).pipe(Effect.catchCause(() => Effect.succeed(false)));

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
            Stream.mapEffect((signal) =>
              signal.type === "event"
                ? Effect.succeed(signal.event)
                : Effect.failCause(signal.cause),
            ),
            Stream.filterEffect((event) => validateProviderEvent(runtime, event)),
            Stream.ensuring(close),
          );
          return { events, close } satisfies ProviderAdapterV2EventSubscription;
        });

      const decorateRuntime = (
        runtime: ProviderAdapterV2SessionRuntime,
        eventSubscribers: Ref.Ref<
          ReadonlyMap<number, Queue.Queue<ProviderSessionEventSignal, Cause.Done>>
        >,
      ): ProviderAdapterV2SessionRuntime => {
        const providerSessionId = runtime.providerSessionId;
        const subscribeEvents = makeEventSubscription(runtime, eventSubscribers);
        // Every provider's turn operations pass through here, so this is where they are
        // counted. Only turn starts are timed: until the provider accepts the turn.
        const turnMetrics = (operation: string, model?: string) =>
          withMetrics({
            counter: providerTurnsTotal,
            ...(operation === "send" ? { timer: providerTurnDuration } : {}),
            attributes: {
              provider: runtime.driver,
              operation,
              modelFamily: normalizeModelMetricLabel(model),
            },
          });
        return {
          ...runtime,
          subscribeEvents,
          events: Stream.unwrap(
            subscribeEvents.pipe(Effect.map((subscription) => subscription.events)),
          ),
          ensureThread: (input) =>
            observeActivity(
              providerSessionId,
              ensureThreadAttached({
                providerSessionId,
                threadId: input.threadId,
                providerInstanceId: runtime.instanceId,
              }),
            ).pipe(
              Effect.andThen(runtime.ensureThread(input)),
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
            if (threadId === null || threadId === undefined) {
              return runtime.resumeThread(input);
            }
            const providerThreadKey = providerThreadLoadKey({
              providerThread: input.providerThread,
              ...(input.modelSelection === undefined
                ? {}
                : { modelSelection: input.modelSelection }),
              ...(input.runtimePolicy === undefined ? {} : { runtimePolicy: input.runtimePolicy }),
            });
            return observeActivity(
              providerSessionId,
              ensureThreadAttached({
                providerSessionId,
                threadId,
                providerInstanceId: runtime.instanceId,
              }),
            ).pipe(
              Effect.andThen(
                holdThreadLoaded({
                  providerSessionId,
                  threadId,
                  providerThread: input.providerThread,
                }),
              ),
              Effect.andThen(
                isProviderThreadLoaded({ providerSessionId, threadId, providerThreadKey }),
              ),
              Effect.flatMap((loaded) =>
                loaded ? Effect.succeed(input.providerThread) : runtime.resumeThread(input),
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
            observeActivity(
              providerSessionId,
              ensureThreadAttached({
                providerSessionId,
                threadId: input.targetThreadId,
                providerInstanceId: runtime.instanceId,
              }),
            ).pipe(
              Effect.andThen(runtime.forkThread(input)),
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
            observeActivity(
              providerSessionId,
              ensureThreadAttached({
                providerSessionId,
                threadId: input.threadId,
                providerInstanceId: runtime.instanceId,
              }),
            ).pipe(
              // A start that fails or is stopped may never emit turn.terminal,
              // so it clears its own turn or the session never goes idle. If
              // the adapter emits the terminal anyway, clearing the same turn
              // again changes nothing, so another thread's turn on a shared
              // session stays busy either way.
              Effect.andThen(
                holdThreadLoaded({
                  providerSessionId,
                  threadId: input.threadId,
                  providerThread: input.providerThread,
                }),
              ),
              Effect.andThen(
                Effect.acquireUseRelease(
                  observeActivity(
                    providerSessionId,
                    markBusy(
                      providerSessionId,
                      busyTurnKey(input.providerThread.id, input.runOrdinal),
                    ),
                  ),
                  () =>
                    runtime.startTurn(input).pipe(turnMetrics("send", input.modelSelection.model)),
                  (_, exit) =>
                    Exit.isFailure(exit)
                      ? observeActivity(
                          providerSessionId,
                          markIdle(providerSessionId, input.providerThread.id, input.runOrdinal),
                        )
                      : Effect.void,
                ),
              ),
            ),
          steerTurn: (input) =>
            observeActivity(providerSessionId, touchActivity(providerSessionId)).pipe(
              Effect.andThen(runtime.steerTurn(input).pipe(turnMetrics("steer"))),
            ),
          interruptTurn: (input) =>
            observeActivity(providerSessionId, touchActivity(providerSessionId)).pipe(
              Effect.andThen(runtime.interruptTurn(input).pipe(turnMetrics("interrupt"))),
            ),
          respondToRuntimeRequest: (input) =>
            observeActivity(providerSessionId, touchActivity(providerSessionId)).pipe(
              Effect.andThen(
                runtime
                  .respondToRuntimeRequest(input)
                  .pipe(turnMetrics("runtime-request-response")),
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
          yield* writeProviderSessionEvents({
            runtime: entry.runtime,
            threadIds: current.attachedThreadIds,
            type: "provider-session.updated",
            payload: event.providerSession,
          });
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("orchestration-v2.driver-session.status-persist-failed", {
              providerSessionId: entry.runtime.providerSessionId,
              cause,
            }),
          ),
        );

      const startEventPump = (entry: LiveSessionEntry) => {
        let stoppedByProvider = false;
        return entry.runtime.events.pipe(
          Stream.filterEffect((event) => validateProviderEvent(entry.runtime, event)),
          Stream.runForEach((event) => {
            if (shutdownSignal.received) return Effect.void;
            if (event.type === "runtime_identity.observed") {
              return providerEventIngestor
                .ingestNormalized({
                  providerSessionId: entry.runtime.providerSessionId,
                  providerInstanceId: entry.runtime.instanceId,
                  threadId: event.binding.threadId,
                  event,
                })
                .pipe(
                  Effect.asVoid,
                  Effect.mapError(
                    (cause) =>
                      new ProviderAdapterEventStreamError({
                        driver: entry.runtime.driver,
                        providerSessionId: entry.runtime.providerSessionId,
                        cause,
                      }),
                  ),
                );
            }
            if (
              event.type === "provider_session.updated" &&
              event.providerSession.status === "stopped"
            ) {
              stoppedByProvider = true;
            }
            return observeActivity(
              entry.runtime.providerSessionId,
              event.type === "turn.terminal"
                ? markIdle(
                    entry.runtime.providerSessionId,
                    event.providerThreadId,
                    event.runOrdinal,
                  )
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
                      yield* providerEventIngestor
                        .ingestNormalized({
                          providerSessionId: entry.runtime.providerSessionId,
                          providerInstanceId: entry.runtime.instanceId,
                          threadId,
                          event,
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
                  yield* publishToSubscribers(entry.eventSubscribers, { type: "event", event });
                }),
              ),
            );
          }),
          Effect.exit,
          Effect.flatMap((exit) =>
            Effect.gen(function* () {
              // A provider that exits on the shutdown signal is released by shutdown.
              if (shutdownSignal.received) return;
              const current = (yield* Ref.get(sessions)).get(
                sessionKey(entry.runtime.providerSessionId),
              );
              if (current?.runtime !== entry.runtime) {
                return;
              }
              if (stoppedByProvider && Exit.isSuccess(exit)) {
                yield* releaseEntry({
                  providerSessionId: entry.runtime.providerSessionId,
                  reason: "manual_shutdown",
                  gracefulSubscribers: true,
                }).pipe(logReleaseFailure(entry.runtime.providerSessionId));
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
              }).pipe(logReleaseFailure(entry.runtime.providerSessionId));
            }),
          ),
          Effect.forkIn(layerScope),
        );
      };

      // Parent of every session scope. On layer close, shutdown releases the
      // live sessions first, then closes any session whose open is still in
      // flight, time-boxed so a stuck adapter cannot hold up server shutdown.
      // Parallel, so one session whose close hangs does not stop the rest from
      // closing within the time box.
      const sessionScopes = yield* Scope.make("parallel");
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
      yield* Effect.addFinalizer(() =>
        shutdown.pipe(
          Effect.ensuring(closeScopeWithin(sessionScopes, { reason: "server_shutdown" })),
        ),
      );

      const equivalentBinding = Schema.toEquivalence(ProviderRuntimeBinding);
      const readCurrentThreadRuntimeAttachment = Effect.fn(
        "ProviderSessionManager.readCurrentThreadRuntimeAttachment",
      )(function* (threadId: ThreadId) {
        const observedAt = DateTime.formatIso(yield* DateTime.now);
        const unknown = (reason: string): RuntimeAttachment.CurrentThreadRuntimeAttachment => ({
          status: "unknown",
          reason,
          observedAt,
        });
        return yield* Effect.gen(function* () {
          const before = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"]);
          if (before.thread.archivedAt !== null || before.thread.deletedAt !== null)
            return unknown("thread_unavailable");
          const entries = yield* Ref.get(sessions);
          const noResident = Ref.get(sessions).pipe(
            Effect.map(
              (current) =>
                ![...current.values()].some((entry) => entry.attachedThreadIds.has(threadId)),
            ),
          );
          if (![...entries.values()].some((entry) => entry.attachedThreadIds.has(threadId)))
            return { status: "stopped", observedAt, isCurrent: noResident } as const;
          const provider = before.providerThreads.find(
            (thread) => thread.id === before.thread.activeProviderThreadId,
          );
          const generation = provider?.runtimeIdentity?.runtimeGeneration;
          const binding =
            provider === undefined || generation === undefined
              ? undefined
              : runtimeBinding(provider, generation);
          if (binding === undefined || binding.threadId !== threadId)
            return unknown("native_binding_unavailable");
          const key = sessionKey(binding.providerSessionId);
          const entry = entries.get(key);
          if (
            entry === undefined ||
            !entry.attachedThreadIds.has(threadId) ||
            entry.runtime.instanceId !== binding.providerInstanceId ||
            entry.runtime.driver !== binding.driver ||
            entry.runtime.providerSessionId !== binding.providerSessionId
          )
            return unknown("runtime_binding_changed");
          const runtime = entry.runtime;
          const loadedKey = entry.loadedProviderThreadKeyByThread.get(threadId);
          const attachedThreadIds = entry.attachedThreadIds;
          const evidenceRevision = provider?.runtimeIdentity?.evidenceRevision ?? null;
          const isCurrent = Effect.gen(function* () {
            const after = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"]);
            const currentProvider = after.providerThreads.find(
              (thread) => thread.id === after.thread.activeProviderThreadId,
            );
            const currentGeneration = currentProvider?.runtimeIdentity?.runtimeGeneration;
            const currentBinding =
              currentProvider === undefined || currentGeneration === undefined
                ? undefined
                : runtimeBinding(currentProvider, currentGeneration);
            const current = (yield* Ref.get(sessions)).get(key);
            return (
              currentBinding !== undefined &&
              equivalentBinding(binding, currentBinding) &&
              (currentProvider?.runtimeIdentity?.evidenceRevision ?? null) === evidenceRevision &&
              current?.runtime === runtime &&
              current.attachedThreadIds === attachedThreadIds &&
              current.attachedThreadIds.has(threadId) &&
              current.loadedProviderThreadKeyByThread.get(threadId) === loadedKey &&
              DateTime.toEpochMillis(after.thread.createdAt) ===
                DateTime.toEpochMillis(before.thread.createdAt) &&
              after.thread.archivedAt === null &&
              after.thread.deletedAt === null
            );
          }).pipe(Effect.catchCause(() => Effect.succeed(false)));
          if (!(yield* isCurrent)) return unknown("runtime_binding_changed");
          return {
            status: "attached",
            binding: Object.freeze({ ...binding }),
            evidenceRevision,
            observedAt,
            isCurrent,
            physicalIncarnation: {
              status: "unknown",
              reason: "physical_incarnation_capture_unavailable",
            },
          } as const;
        }).pipe(Effect.catchCause(() => Effect.succeed(unknown("runtime_binding_unavailable"))));
      });
      const observeThreadActivity = Effect.fn("ProviderSessionManager.observeThreadActivity")(
        function* (threadId: ThreadId) {
          const unavailable = (reason: string): RuntimeObservation.ProviderRuntimeObservation => ({
            status: "unknown",
            reason,
          });
          const read = Effect.gen(function* () {
            const before = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"]);
            const provider = before.providerThreads.find(
              (thread) => thread.id === before.thread.activeProviderThreadId,
            );
            if (provider?.providerSessionId == null)
              return unavailable("native_binding_unavailable");
            const key = sessionKey(provider.providerSessionId);
            const entry = (yield* Ref.get(sessions)).get(key);
            if (entry === undefined) return unavailable("runtime_not_resident");
            const runtime = entry.runtime;
            if (
              !entry.attachedThreadIds.has(threadId) ||
              runtime.providerSessionId !== provider.providerSessionId ||
              runtime.instanceId !== provider.providerInstanceId ||
              runtime.driver !== provider.driver
            )
              return unavailable("runtime_binding_changed");
            if (runtime.readThreadActivity === undefined)
              return unavailable("native_activity_unsupported");
            const generation = provider.runtimeIdentity?.runtimeGeneration;
            const binding =
              generation === undefined ? undefined : runtimeBinding(provider, generation);
            if (binding === undefined || binding.threadId !== threadId)
              return unavailable("native_binding_unavailable");
            const loadedKey = entry.loadedProviderThreadKeyByThread.get(threadId);
            const sampledAt = yield* Clock.currentTimeMillis;
            const observation = yield* runtime
              .readThreadActivity(provider)
              .pipe(Effect.timeout("3 seconds"));
            const after = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"]);
            const current = after.providerThreads.find(
              (thread) => thread.id === after.thread.activeProviderThreadId,
            );
            const currentGeneration = current?.runtimeIdentity?.runtimeGeneration;
            const currentBinding =
              current === undefined || currentGeneration === undefined
                ? undefined
                : runtimeBinding(current, currentGeneration);
            const currentEntry = (yield* Ref.get(sessions)).get(key);
            if (
              currentBinding === undefined ||
              !equivalentBinding(binding, currentBinding) ||
              currentEntry?.runtime !== runtime ||
              !currentEntry.attachedThreadIds.has(threadId) ||
              currentEntry.loadedProviderThreadKeyByThread.get(threadId) !== loadedKey ||
              after.thread.archivedAt !== null ||
              after.thread.deletedAt !== null
            )
              return unavailable("runtime_binding_changed");
            if (observation.status === "unknown") return observation;
            if (!equivalentBinding(binding, observation.binding))
              return unavailable("runtime_binding_changed");
            const observedAt = Date.parse(observation.observedAt);
            if (!Number.isFinite(observedAt) || observedAt < sampledAt)
              return unavailable("native_activity_stale");
            return observation;
          });
          return yield* read.pipe(
            Effect.catchCause(() => Effect.succeed(unavailable("native_activity_unavailable"))),
          );
        },
      );

      const captureCurrentThreadRuntimeStop = Effect.fn(
        "ProviderSessionManager.captureCurrentThreadRuntimeStop",
      )(
        function* (threadId: ThreadId) {
          const before = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"]);
          const provider = before.providerThreads.find(
            (thread) => thread.id === before.thread.activeProviderThreadId,
          );
          if (
            provider?.providerSessionId == null ||
            before.thread.deletedAt !== null ||
            before.thread.archivedAt !== null
          )
            return null;
          const key = sessionKey(provider.providerSessionId);
          const entry = (yield* Ref.get(sessions)).get(key);
          if (!entry || !entry.attachedThreadIds.has(threadId) || !entry.runtime.captureRuntimeStop)
            return null;
          const captured = yield* entry.runtime.captureRuntimeStop(provider);
          if (captured === null || captured.binding.threadId !== threadId) return null;
          const loadedKey = entry.loadedProviderThreadKeyByThread.get(threadId);
          const isCurrent = Effect.gen(function* () {
            if (!(yield* captured.isCurrent)) return false;
            const after = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"]);
            const current = after.providerThreads.find(
              (thread) => thread.id === after.thread.activeProviderThreadId,
            );
            const generation = current?.runtimeIdentity?.runtimeGeneration;
            const binding =
              current === undefined || generation === undefined
                ? undefined
                : runtimeBinding(current, generation);
            const currentEntry = (yield* Ref.get(sessions)).get(key);
            return (
              binding !== undefined &&
              equivalentBinding(binding, captured.binding) &&
              current?.runtimeIdentity?.evidenceRevision === captured.evidenceRevision &&
              currentEntry?.runtime === entry.runtime &&
              currentEntry.attachedThreadIds.has(threadId) &&
              currentEntry.loadedProviderThreadKeyByThread.get(threadId) === loadedKey &&
              after.thread.deletedAt === null &&
              after.thread.archivedAt === null
            );
          }).pipe(Effect.catchCause(() => Effect.succeed(false)));
          if (!(yield* isCurrent)) return null;
          return { ...captured, isCurrent };
        },
        (effect, threadId) =>
          effect.pipe(
            Effect.mapError((cause) => new ProviderRuntimeStopCaptureError({ threadId, cause })),
          ),
      );
      return ProviderSessionManagerV2.of({
        captureCurrentThreadRuntimeStop,
        readCurrentThreadRuntimeAttachment,
        observeThreadActivity,
        isMcpCallerAttached: (input) =>
          Ref.get(sessions).pipe(
            Effect.map((entries) => {
              const entry = entries.get(sessionKey(input.providerSessionId));
              return (
                entry !== undefined &&
                entry.runtime.instanceId === input.providerInstanceId &&
                entry.attachedThreadIds.has(input.threadId) &&
                entry.mcpCredentialIdByThread.get(input.threadId) === input.mcpCredentialId
              );
            }),
          ),
        shutdown,
        open: (input) =>
          sessionOpen.withLock(
            input.providerSessionId,
            Effect.gen(function* () {
              yield* NativeProvider.revalidateNativeProviderGuard(input.nativeCreationGuard, {
                threadId: input.threadId,
                cwd: input.runtimePolicy.cwd,
              }).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderSessionOpenError({
                      instanceId: input.modelSelection.instanceId,
                      providerSessionId: input.providerSessionId,
                      cause,
                    }),
                ),
              );
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
                if (input.nativeCreationGuard !== undefined)
                  return yield* new ProviderSessionOpenError({
                    instanceId: input.modelSelection.instanceId,
                    providerSessionId: input.providerSessionId,
                    cause: "Native creation cannot borrow an existing provider runtime.",
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
                });
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
              if (
                input.nativeCreationGuard !== undefined &&
                (adapter.driver !== "codex" || adapter.nativeCreationExecution !== true)
              )
                return yield* new ProviderSessionOpenError({
                  instanceId: input.modelSelection.instanceId,
                  providerSessionId: input.providerSessionId,
                  cause: "Native creation requires the direct Codex executor.",
                });
              yield* NativeProvider.revalidateNativeProviderGuard(input.nativeCreationGuard, {
                threadId: input.threadId,
                cwd: input.runtimePolicy.cwd,
              }).pipe(
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
              const sessionScope = yield* Scope.fork(sessionScopes);
              const runtime = yield* adapter
                .openSession({
                  threadId: input.threadId,
                  providerSessionId: input.providerSessionId,
                  ...(input.nativeCreationGuard === undefined
                    ? {}
                    : { nativeCreationGuard: input.nativeCreationGuard }),
                  runtimeLifecycle: makeRuntimeLifecycle(
                    input.providerSessionId,
                    input.modelSelection.instanceId,
                    adapter.driver,
                    input.nativeCreationGuard,
                  ),
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
                })
                .pipe(
                  Effect.provideService(Scope.Scope, sessionScope),
                  // Any failure, including a Stop that interrupts a slow
                  // handshake, stops the provider process this open started.
                  // The session cleanup runs first, and the close is
                  // time-boxed: this runs under the session's open lock, so an
                  // adapter finalizer that never finishes must not hold the
                  // interrupter, the lock, or later opens.
                  Effect.onError(() =>
                    dropReservation.pipe(
                      // Clear only a session this open freshly set up: a reused
                      // one is held by another live provider process and must
                      // survive this open's failure.
                      Effect.andThen(
                        prepared.issued
                          ? clearMcpSession(input.threadId, mcpCredentialId)
                          : Effect.void,
                      ),
                      Effect.ensuring(
                        closeScopeWithin(sessionScope, {
                          providerSessionId: input.providerSessionId,
                          reason: "open_failed",
                        }),
                      ),
                    ),
                  ),
                  Effect.mapError(
                    (cause) =>
                      new ProviderSessionOpenError({
                        instanceId: input.modelSelection.instanceId,
                        providerSessionId: input.providerSessionId,
                        cause,
                      }),
                  ),
                  withMetrics({
                    counter: providerSessionsTotal,
                    attributes: { provider: adapter.driver, operation: "open" },
                  }),
                );
              const eventSubscribers = yield* Ref.make<
                ReadonlyMap<number, Queue.Queue<ProviderSessionEventSignal, Cause.Done>>
              >(new Map());
              const exposedRuntime = decorateRuntime(runtime, eventSubscribers);
              const now = yield* Clock.currentTimeMillis;
              const entry: LiveSessionEntry = {
                attachedThreadIds: new Set([input.threadId]),
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
                scope: sessionScope,
                idleGeneration: 0,
                busyTurns: new Set(),
                lastActivityAtMs: now,
                idleFiber: null,
                pinnedSinceMs: null,
                idleThreadUnloads: new Map(),
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
                // Released on interrupt too: this entry has no event pump or
                // idle timer yet, so nothing else would ever release it.
                Effect.onError((cause) =>
                  releaseEntry(
                    Cause.hasInterruptsOnly(cause)
                      ? {
                          providerSessionId: input.providerSessionId,
                          reason: "manual_shutdown",
                          detail: "The provider session start was interrupted.",
                        }
                      : {
                          providerSessionId: input.providerSessionId,
                          reason: "runtime_error",
                          detail: "Failed to persist the provider-session attachment.",
                        },
                  ).pipe(logReleaseFailure(input.providerSessionId)),
                ),
              );
              yield* startEventPump(entry);
              yield* scheduleIdleRelease(input.providerSessionId);
              return exposedRuntime;
            }),
          ),
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
            let detachedProviderThreads: ReadonlyArray<OrchestrationV2ProviderThread> = [];
            if (currentEntry?.supportsMultipleProviderThreads === true) {
              const projection = yield* Effect.option(
                projectionStore.getThreadRecords(input.threadId, [
                  "providerThreads",
                  "providerTurns",
                ]),
              );
              if (Option.isSome(projection)) {
                const providerThreads = new Map(
                  projection.value.providerThreads
                    .filter((thread) => thread.providerSessionId === input.providerSessionId)
                    .map((thread) => [thread.id, thread] as const),
                );
                detachedProviderThreads = [...providerThreads.values()];
                const activeTurns = projection.value.providerTurns.filter(
                  (turn) => turn.status === "running" && providerThreads.has(turn.providerThreadId),
                );
                yield* Effect.forEach(
                  activeTurns,
                  (turn) =>
                    currentEntry.exposedRuntime
                      .interruptTurn({
                        providerThread: providerThreads.get(turn.providerThreadId)!,
                        providerTurnId: turn.id,
                      })
                      .pipe(
                        Effect.catchCause((cause) =>
                          Effect.logWarning(
                            "orchestration-v2.driver-session.detach-interrupt-failed",
                            {
                              providerSessionId: input.providerSessionId,
                              threadId: input.threadId,
                              providerTurnId: turn.id,
                              cause,
                            },
                          ),
                        ),
                      ),
                  { concurrency: 1, discard: true },
                );
              }
            }
            const detachResult = yield* Ref.modify(sessions, (current) => {
              const entry = current.get(key);
              if (entry === undefined || !entry.attachedThreadIds.has(input.threadId)) {
                return [
                  Option.none<{
                    readonly entry: LiveSessionEntry;
                    readonly idleUnloadFiber: Fiber.Fiber<void, never> | null;
                  }>(),
                  current,
                ] as const;
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
              // The detach unloads the thread itself below.
              const idleThreadUnloads = new Map(entry.idleThreadUnloads);
              idleThreadUnloads.delete(input.threadId);
              const updatedEntry = {
                ...entry,
                attachedThreadIds,
                loadedProviderThreadKeyByThread,
                mcpCredentialIdByThread,
                idleThreadUnloads,
              };
              const updated = new Map(current);
              updated.set(key, updatedEntry);
              return [
                Option.some({
                  entry: updatedEntry,
                  idleUnloadFiber: entry.idleThreadUnloads.get(input.threadId)?.fiber ?? null,
                }),
                updated,
              ] as const;
            });
            if (Option.isSome(detachResult)) {
              yield* cancelIdleFiber(detachResult.value.idleUnloadFiber);
            }
            const detached = Option.map(detachResult, (result) => result.entry);
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
                    (providerThread) =>
                      unloadThread({ providerThread }).pipe(
                        // Bounded so a wedged provider cannot hold up the
                        // thread's next attach.
                        Effect.timeout(UNLOAD_THREAD_TIMEOUT_MS),
                        Effect.catchCause((cause) =>
                          Effect.logWarning(
                            "orchestration-v2.driver-session.detach-unload-failed",
                            {
                              providerSessionId: input.providerSessionId,
                              threadId: input.threadId,
                              providerThreadId: providerThread.id,
                              cause,
                            },
                          ),
                        ),
                      ),
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
