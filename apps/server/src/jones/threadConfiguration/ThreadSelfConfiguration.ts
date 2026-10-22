import {
  THREAD_SELF_CONFIGURATION_APPLICATION,
  CommandId,
  EventId,
  type ThreadId,
  type OrchestrationV2DomainEvent,
  ThreadSelfConfigurationInput,
  type ThreadSelfConfigurationResult,
  type ThreadConfigurationEvidence,
  type ThreadExecutionPrecondition,
} from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Crypto from "effect/Crypto";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Locks from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as Switch from "../../orchestration-v2/ProviderSwitchService.ts";
import * as Registry from "../../provider/ProviderRegistry.ts";
import * as Settings from "../../serverSettings.ts";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import {
  nativeCreationCanonicalJson as canonical,
  nativeCreationSha256 as digest,
} from "../nativeCreation/NativeCreationPreparation.ts";

export class ThreadSelfConfigurationError extends Schema.TaggedError<ThreadSelfConfigurationError>()(
  "ThreadSelfConfigurationError",
  {
    reason: Schema.Literals([
      "caller_unavailable",
      "execution_changed",
      "configuration_changed",
      "request_conflict",
      "provider_unavailable",
      "invalid_selection",
      "continuation_rejected",
      "state_unavailable",
    ]),
  },
) {}

export interface ThreadSelfConfigurationCommitContext {
  readonly revalidate: () => Effect.Effect<
    ReadonlyArray<OrchestrationV2DomainEvent>,
    ThreadSelfConfigurationError
  >;
}

export class ThreadSelfConfiguration extends Context.Service<
  ThreadSelfConfiguration,
  {
    readonly read: (
      threadId: ThreadId,
      scope: McpInvocationScope,
    ) => Effect.Effect<ThreadConfigurationEvidence, ThreadSelfConfigurationError>;
    readonly save: (
      input: ThreadSelfConfigurationInput,
      scope: McpInvocationScope,
    ) => Effect.Effect<ThreadSelfConfigurationResult, ThreadSelfConfigurationError>;
  }
>()("t3/jones/threadConfiguration/ThreadSelfConfiguration") {}

const isConfigurationError = Schema.is(ThreadSelfConfigurationError);
const commandType = "jones.thread.self-configure";
const make = Effect.gen(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const projections = yield* Projections.ProjectionStoreV2;
  const sessions = yield* Sessions.ProviderSessionManagerV2;
  const locks = yield* Locks.ThreadCommandExecutor;
  const registry = yield* Effect.serviceOption(Registry.ProviderRegistry);
  const settings = yield* Settings.ServerSettingsService;
  const transitions = yield* Switch.ProviderSwitchServiceV2;
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const generationNamespace = yield* crypto.randomUUIDv4();
  let nextGeneration = 0;
  const residentGenerations = new WeakMap<object, string>();
  const revision = (threadId: ThreadId) =>
    sql<{ revision: number }>`
    SELECT COALESCE(MAX(sequence), 0) AS revision FROM orchestration_events
    WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${threadId}
      AND event_type IN ('thread.created', 'thread.model-selection-updated', 'thread.provider-switched')
  `.pipe(Effect.map((rows) => rows[0]?.revision ?? 0));
  const snapshot = (threadId: ThreadId, scope: McpInvocationScope) =>
    Effect.gen(function* () {
      const projection = yield* projections.getThreadRecords(threadId, [
        "runs",
        "attempts",
        "providerThreads",
        "providerSessions",
      ]);
      const run = projection.runs.find((run) =>
        ["starting", "running", "waiting"].includes(run.status),
      );
      const provider = projection.providerThreads.find(
        (provider) => provider.id === run?.providerThreadId,
      );
      const caller = scope.thread;
      const attached =
        caller?.threadId === threadId &&
        provider?.providerSessionId != null &&
        provider.providerInstanceId === caller.providerInstanceId &&
        (yield* sessions.isMcpCallerAttached({
          threadId,
          providerSessionId: provider.providerSessionId,
          providerInstanceId: caller.providerInstanceId,
          mcpCredentialId: caller.providerSessionId,
        }));
      const resident =
        attached && provider?.providerSessionId != null
          ? yield* sessions.get(provider.providerSessionId, { touchActivity: false })
          : Option.none();
      // Fence the actual resident object even when an adapter has no native attestation.
      // This nonce is execution authority, never evidence of provider model or options.
      const residentGeneration = Option.isSome(resident)
        ? yield* Effect.sync(() => {
            let generation = residentGenerations.get(resident.value);
            if (generation === undefined) {
              generation = generationNamespace + ":" + ++nextGeneration;
              residentGenerations.set(resident.value, generation);
            }
            return generation;
          })
        : null;
      const precondition: ThreadExecutionPrecondition | null =
        residentGeneration !== null &&
        run?.activeAttemptId != null &&
        provider?.providerSessionId != null
          ? {
              runId: run.id,
              attemptId: run.activeAttemptId,
              providerThreadId: provider.id,
              providerSessionId: provider.providerSessionId,
              runtimeGeneration: `resident:${residentGeneration}:native:${provider.runtimeIdentity?.runtimeGeneration ?? "unavailable"}`,
            }
          : null;
      const evidence: ThreadConfigurationEvidence = {
        configurationRevision: yield* revision(threadId),
        savedSelection: projection.thread.modelSelection,
        executionPrecondition: precondition,
        currentExecution:
          run === undefined
            ? null
            : {
                runId: run.id,
                attemptId: run.activeAttemptId,
                requestedSelection: run.modelSelection,
                providerObservation: provider?.runtimeIdentity ?? null,
                reasoningEffortObservation: "unavailable",
              },
      };
      return { projection, evidence };
    }).pipe(sql.withTransaction);
  const read = (threadId: ThreadId, scope: McpInvocationScope) =>
    snapshot(threadId, scope).pipe(
      Effect.map(({ evidence }) => evidence),
      Effect.mapError(() => new ThreadSelfConfigurationError({ reason: "state_unavailable" })),
    );
  const save = (input: ThreadSelfConfigurationInput, scope: McpInvocationScope) =>
    Effect.gen(function* () {
      const caller = scope.thread;
      if (
        caller === undefined ||
        scope.client !== undefined ||
        !scope.capabilities.has("orchestration")
      )
        return yield* new ThreadSelfConfigurationError({ reason: "caller_unavailable" });
      const threadId = caller.threadId;
      const commandId = CommandId.make(
        `jones:self-configure:${digest(canonical({ version: 1, environmentId: scope.environmentId, threadId, clientRequestId: input.clientRequestId }))}`,
      );
      const eventId = EventId.make(
        `event:${commandId}:${digest(canonical({ version: 1, input }))}`,
      );
      return yield* locks.withLock(
        threadId,
        Effect.gen(function* () {
          const stored = yield* sink.readByCommandId({ commandId }).pipe(Stream.runCollect);
          if (stored.length > 0) {
            const event = stored[0]!.event;
            if (
              stored.length !== 1 ||
              event.id !== eventId ||
              (event.type !== "thread.model-selection-updated" &&
                event.type !== "thread.provider-switched")
            )
              return yield* new ThreadSelfConfigurationError({ reason: "request_conflict" });
            return {
              acceptance: {
                threadId,
                sequence: stored[0]!.sequence,
                committedSelection: event.payload.modelSelection,
                protectedExecution: input.executionPrecondition,
                application: "subsequently_admitted_inheriting_runs" as const,
                applicationMessage: THREAD_SELF_CONFIGURATION_APPLICATION,
              },
              freshObservation: yield* read(threadId, scope),
            } satisfies ThreadSelfConfigurationResult;
          }
          const initial = yield* snapshot(threadId, scope);
          const validate = () =>
            Effect.gen(function* () {
              const current = yield* snapshot(threadId, scope);
              if (
                current.projection.thread.archivedAt !== null ||
                current.projection.thread.deletedAt !== null ||
                current.evidence.executionPrecondition === null ||
                canonical(current.evidence.executionPrecondition) !==
                  canonical(input.executionPrecondition)
              )
                return yield* new ThreadSelfConfigurationError({ reason: "execution_changed" });
              if (current.evidence.configurationRevision !== input.expectedConfigurationRevision)
                return yield* new ThreadSelfConfigurationError({ reason: "configuration_changed" });
            }).pipe(
              Effect.mapError((error) =>
                isConfigurationError(error)
                  ? error
                  : new ThreadSelfConfigurationError({ reason: "state_unavailable" }),
              ),
            );
          yield* validate();
          if (Option.isNone(registry))
            return yield* new ThreadSelfConfigurationError({ reason: "provider_unavailable" });
          const providers = yield* registry.value.getProviders;
          const provider = providers.find(
            (provider) => provider.instanceId === input.modelSelection.instanceId,
          );
          const config = yield* settings.getSettings;
          const projectEnabled =
            config.projectSettingsOverrides[initial.projection.thread.projectId]
              ?.providerInstances?.[input.modelSelection.instanceId]?.enabled;
          if (
            provider === undefined ||
            !provider.enabled ||
            !provider.installed ||
            projectEnabled === false
          )
            return yield* new ThreadSelfConfigurationError({ reason: "provider_unavailable" });
          const model = provider.models.find(
            (model) =>
              model.slug === input.modelSelection.model &&
              (model.isCustom ||
                !config.providerModelPreferences?.[provider.instanceId]?.hiddenModels.includes(
                  model.slug,
                )),
          );
          if (model === undefined)
            return yield* new ThreadSelfConfigurationError({ reason: "invalid_selection" });
          const seen = new Set<string>();
          for (const option of input.modelSelection.options ?? []) {
            const descriptor = model.capabilities?.optionDescriptors?.find(
              (descriptor) => descriptor.id === option.id,
            );
            if (
              seen.has(option.id) ||
              descriptor === undefined ||
              (descriptor.type === "boolean"
                ? typeof option.value !== "boolean"
                : !descriptor.options.some((choice) => choice.id === option.value))
            )
              return yield* new ThreadSelfConfigurationError({ reason: "invalid_selection" });
            seen.add(option.id);
          }
          yield* transitions
            .plan({ projection: initial.projection, targetModelSelection: input.modelSelection })
            .pipe(
              Effect.mapError(
                () => new ThreadSelfConfigurationError({ reason: "continuation_rejected" }),
              ),
            );
          const now = yield* DateTime.now;
          const event: OrchestrationV2DomainEvent = {
            id: eventId,
            type:
              initial.projection.thread.providerInstanceId === input.modelSelection.instanceId
                ? "thread.model-selection-updated"
                : "thread.provider-switched",
            threadId,
            providerInstanceId: input.modelSelection.instanceId,
            occurredAt: now,
            payload: {
              ...initial.projection.thread,
              providerInstanceId: input.modelSelection.instanceId,
              modelSelection: input.modelSelection,
              updatedAt: now,
            },
          };
          let refusal: ThreadSelfConfigurationError | undefined;
          const committed = yield* sink
            .commitCommand({
              commandId,
              threadId,
              commandType,
              acceptedAt: now,
              events: [event],
              effects: [],
              threadSelfConfiguration: {
                revalidate: () =>
                  Effect.gen(function* () {
                    yield* validate();
                    const current = yield* projections.getThread(threadId);
                    if (current === null)
                      return yield* new ThreadSelfConfigurationError({
                        reason: "execution_changed",
                      });
                    return [
                      {
                        ...event,
                        payload: {
                          ...current,
                          providerInstanceId: input.modelSelection.instanceId,
                          modelSelection: input.modelSelection,
                          updatedAt: now,
                        },
                      },
                    ];
                  }).pipe(
                    Effect.mapError((error) =>
                      isConfigurationError(error)
                        ? error
                        : new ThreadSelfConfigurationError({ reason: "state_unavailable" }),
                    ),
                    Effect.tapError((error) =>
                      Effect.sync(() => {
                        refusal = error;
                      }),
                    ),
                  ),
              },
            })
            .pipe(
              Effect.mapError(
                () => refusal ?? new ThreadSelfConfigurationError({ reason: "state_unavailable" }),
              ),
            );
          if (!committed.committed)
            return yield* new ThreadSelfConfigurationError({ reason: "request_conflict" });
          return {
            acceptance: {
              threadId,
              sequence: committed.receipt.resultSequence,
              committedSelection: input.modelSelection,
              protectedExecution: input.executionPrecondition,
              application: "subsequently_admitted_inheriting_runs" as const,
              applicationMessage: THREAD_SELF_CONFIGURATION_APPLICATION,
            },
            freshObservation: yield* read(threadId, scope),
          } satisfies ThreadSelfConfigurationResult;
        }),
      );
    }).pipe(
      Effect.mapError((error) =>
        isConfigurationError(error)
          ? error
          : new ThreadSelfConfigurationError({ reason: "state_unavailable" }),
      ),
    );
  return ThreadSelfConfiguration.of({ read, save });
});
export const layer = Layer.effect(ThreadSelfConfiguration, make).pipe(
  Layer.provide(NodeCrypto.layer),
);
