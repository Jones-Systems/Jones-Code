import { CommandId, RunAttemptId, RunId, ThreadId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as SchemaIssue from "effect/SchemaIssue";

import {
  increment,
  metricAttributes,
  orchestrationEffectClaimsTotal,
  orchestrationEffectQueueWait,
} from "../observability/Metrics.ts";
import * as RunFinalizationService from "./RunFinalizationService.ts";
import { readClaimedQueuedRunStartExecution } from "./RunExecutionService.ts";
import * as RunExecutionService from "./RunExecutionService.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";
import * as CheckpointCaptureService from "./CheckpointCaptureService.ts";
import * as ResourceCleanupService from "./ResourceCleanupService.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import { jsonCause } from "./EventSinkJsonCodec.ts";
import * as CheckpointRollbackService from "./CheckpointRollbackService.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderTurnControlService from "./ProviderTurnControlService.ts";
import * as ProviderTurnStartService from "./ProviderTurnStartService.ts";
import * as RuntimeRequestService from "./RuntimeRequestService.ts";
import * as ThreadTitleRegenerationService from "./ThreadTitleRegenerationService.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import {
  ThreadCommandExecutor,
  layer as threadCommandExecutorLayer,
} from "./ThreadCommandExecutor.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as NativeCreationAuthority from "./NativeCreationAuthority.ts";
import {
  NativeCreationRepository,
  type NativeEffectConfirmationV1,
} from "../persistence/Services/NativeCreationRepository.ts";
import { continueRestartedRun } from "./RestartContinuation.ts";
import {
  nativeEffectEvidenceFromCause,
  ProviderNativeOperationUnknownError,
} from "./ProviderFailure.ts";
import { ProviderNativeEffectEvidence } from "./ProviderAdapter.ts";
import { nativeCreationCanonicalJson } from "./NativeCreationPreparation.ts";
import type {
  ApplicationThreadBirthV2,
  ImportedHistoryStartExecutionPreparationV2,
} from "./Orchestrator.ts";

// Byte-exact JSON.stringify for confirmation comparisons. Key order stays
// significant, and a native stringify exception remains the defect.
const decodeComparisonJson = Schema.decodeEffect(
  Schema.Unknown.pipe(
    Schema.decodeTo(Schema.String, {
      decode: SchemaGetter.onSome<string, unknown>((input, options) => {
        try {
          return Effect.succeed(Option.some(JSON.stringify(input)));
        } catch (cause) {
          return Effect.fail(
            new SchemaIssue.InvalidValue({ nativeJsonCause: cause }, input, options),
          );
        }
      }),
      encode: SchemaGetter.forbiddenEncoding,
    }),
  ),
);
const comparisonJson = (value: unknown) =>
  decodeComparisonJson(value).pipe(Effect.catch((error) => Effect.die(jsonCause(error))));

export const ImportedHistoryStartExecutionPreparation = Context.Reference<
  ImportedHistoryStartExecutionPreparationV2 | undefined
>("t3/orchestration-v2/EffectWorker/ImportedHistoryStartExecutionPreparation", {
  defaultValue: () => undefined,
});

export class OrchestrationEffectExecutionError extends Schema.TaggedError<OrchestrationEffectExecutionError>()(
  "OrchestrationEffectExecutionError",
  {
    effectId: Schema.String,
    effectType: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

class ResourceCleanupUnknownError extends Schema.TaggedError<ResourceCleanupUnknownError>()(
  "ResourceCleanupUnknownError",
  {
    evidence: EffectOutbox.ResourceCleanupUnknownEvidenceV1,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

interface ResourceCleanupHeldV1 {
  readonly status: "cleanup_held";
  readonly evidence: EffectOutbox.ResourceCleanupUnknownEvidenceV1;
}

// Completion proofs repeat only these store reads before the worker settles a claim.
type ClaimRevalidationError =
  | EventSink.OrdinaryCheckoutCommitErrorV1
  | EffectOutbox.EffectOutboxError;

interface DeletionCleanupCompletedV1 {
  readonly status: "cleanup_completed";
  readonly effectId: string;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly workerId: string;
  readonly expectedAttempt: number;
  readonly revalidate: Effect.Effect<boolean, ClaimRevalidationError>;
}

interface AttachmentNamespaceRetainedV1 {
  readonly status: "attachment_namespace_retained";
  readonly effectId: string;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly workerId: string | null;
  readonly expectedAttempt: number;
  readonly revalidate: Effect.Effect<boolean, ClaimRevalidationError>;
}

interface SettledOrdinaryClaimV1 {
  readonly status: "ordinary_claim_settled";
  readonly effectId: string;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly workerId: string;
  readonly expectedAttempt: number;
  readonly revalidate: Effect.Effect<boolean, ClaimRevalidationError>;
}

interface OrdinaryStartRetryV1 {
  readonly status: "ordinary_start_retry";
  readonly effectId: string;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly workerId: string;
  readonly expectedAttempt: number;
  readonly settle: (
    delayMs: number,
  ) => Effect.Effect<void, EventSink.OrdinaryCheckoutCommitErrorV1>;
}

type EffectExecutionResult =
  | void
  | NativeEffectConfirmationV1
  | EffectOutbox.ParkedImportedHistoryDeliveryV1
  | ResourceCleanupHeldV1
  | DeletionCleanupCompletedV1
  | AttachmentNamespaceRetainedV1
  | SettledOrdinaryClaimV1
  | OrdinaryStartRetryV1;

const isOrdinaryStartRetry = (result: EffectExecutionResult): result is OrdinaryStartRetryV1 =>
  result !== undefined && "status" in result && result.status === "ordinary_start_retry";

const isSettledOrdinaryClaim = (result: EffectExecutionResult): result is SettledOrdinaryClaimV1 =>
  result !== undefined && "status" in result && result.status === "ordinary_claim_settled";

const isResourceCleanupHeld = (result: EffectExecutionResult): result is ResourceCleanupHeldV1 =>
  result !== undefined && "status" in result && result.status === "cleanup_held";

const isDeletionCleanupCompleted = (
  result: EffectExecutionResult,
): result is DeletionCleanupCompletedV1 =>
  result !== undefined && "status" in result && result.status === "cleanup_completed";

const isAttachmentNamespaceRetained = (
  result: EffectExecutionResult,
): result is AttachmentNamespaceRetainedV1 =>
  result !== undefined && "status" in result && result.status === "attachment_namespace_retained";

const matchesResourceHoldEvidence = (
  actual: EffectOutbox.UnknownEffectHoldV2["evidence"],
  expected: EffectOutbox.ResourceCleanupUnknownEvidenceV1,
): boolean =>
  "kind" in actual &&
  actual.kind === "resource_cleanup" &&
  Schema.is(EffectOutbox.ResourceCleanupUnknownEvidenceV1)(actual) &&
  actual.version === expected.version &&
  actual.operationId === expected.operationId &&
  actual.threadId === expected.threadId &&
  actual.taskKind === expected.taskKind &&
  actual.bindingSha256 === expected.bindingSha256 &&
  actual.outcome === expected.outcome &&
  (actual.bindingSha256 !== null ||
    (expected.bindingSha256 === null && actual.reason === expected.reason));

const resourceCleanupEvidenceFromCause = (
  cause: unknown,
): EffectOutbox.ResourceCleanupUnknownEvidenceV1 | undefined => {
  const seen = new Set<unknown>();
  for (let depth = 0; depth < 16 && cause != null && !seen.has(cause); depth++) {
    seen.add(cause);
    if (Cause.isCause(cause)) {
      cause = Cause.squash(cause);
      continue;
    }
    if (Schema.is(ResourceCleanupUnknownError)(cause)) return cause.evidence;
    if (!Schema.is(OrchestrationEffectExecutionError)(cause)) return undefined;
    cause = cause.cause;
  }
  return undefined;
};

const matchesResourceClaim = (
  claimed: EffectOutbox.OrchestrationEffectV2,
  current: EffectOutbox.OrchestrationEffectV2,
): boolean => {
  const request = claimed.request;
  if (
    request.type !== "terminal.cleanup" &&
    request.type !== "attachment.cleanup" &&
    request.type !== "worktree.cleanup"
  )
    return false;
  if (
    current.id !== claimed.id ||
    current.commandId !== claimed.commandId ||
    current.threadId !== claimed.threadId ||
    current.status !== "running" ||
    claimed.leaseOwner === null ||
    current.leaseOwner !== claimed.leaseOwner ||
    current.attemptCount !== claimed.attemptCount ||
    current.nativeCreationExecutionReference !== undefined
  )
    return false;
  return request.type === "worktree.cleanup"
    ? current.request.type === "worktree.cleanup"
    : request.type === "terminal.cleanup"
      ? current.request.type === "terminal.cleanup"
      : current.request.type === "attachment.cleanup" &&
        current.request.attachmentIds.length === request.attachmentIds.length &&
        current.request.attachmentIds.every((id, index) => id === request.attachmentIds[index]);
};

const readResourceHold = (
  outbox: EffectOutbox.EffectOutboxV2["Service"],
  claimed: EffectOutbox.OrchestrationEffectV2,
  evidence: EffectOutbox.ResourceCleanupUnknownEvidenceV1,
  requireUnexpired = false,
) =>
  Effect.gen(function* () {
    const current = yield* outbox.get(claimed.id);
    if (
      Option.isNone(current) ||
      !matchesResourceClaim(claimed, current.value) ||
      evidence.operationId !== claimed.id ||
      evidence.threadId !== claimed.threadId ||
      evidence.taskKind !==
        (claimed.request.type === "terminal.cleanup"
          ? "terminal"
          : claimed.request.type === "worktree.cleanup"
            ? "worktree"
            : "attachment")
    )
      return false;
    if (requireUnexpired) {
      const now = DateTime.toEpochMillis(yield* DateTime.now);
      if (
        current.value.leaseExpiresAt === null ||
        !Number.isFinite(Date.parse(current.value.leaseExpiresAt)) ||
        Date.parse(current.value.leaseExpiresAt) <= now
      )
        return false;
    }
    const held = (yield* outbox.listHeldByThreadId(claimed.threadId)).filter(
      (hold) => hold.effectId === claimed.id,
    );
    return (
      held.length === 1 &&
      held[0]!.threadId === claimed.threadId &&
      held[0]!.workerId === claimed.leaseOwner &&
      held[0]!.operationId === claimed.id &&
      held[0]!.expectedAttempt === claimed.attemptCount &&
      matchesResourceHoldEvidence(held[0]!.evidence, evidence)
    );
  });

/**
 * Pure interrupt races with hard process teardown or a dead session produce
 * "not active" protocol errors. Retrying those only delays recovery.
 *
 * Do not apply this to `provider-turn.restart`: that compound effect also runs
 * detach and start. Swallowing a start failure that happens to mention
 * "is not active" would drop the outbox item without ever starting the
 * replacement turn.
 */
export function isNonRetryableProviderTurnControlFailure(
  effectType: string,
  errorText: string,
): boolean {
  if (effectType !== "provider-turn.interrupt") {
    return false;
  }
  return (
    /is not active/i.test(errorText) ||
    /hard teardown is already in progress/i.test(errorText) ||
    /treating as already interrupted/i.test(errorText) ||
    /treating as already stopped/i.test(errorText)
  );
}

export interface OrchestrationEffectExecutorV2Shape {
  readonly claimAttachmentNamespaceCleanupRetry?: (input: {
    readonly effectId: string;
    readonly workerId: string;
    readonly leaseDurationMs: number;
    readonly expectedBindingSha256: string;
    readonly expectedObservationOrdinal: number;
  }) => Effect.Effect<EffectOutbox.OrchestrationEffectV2 | null, OrchestrationEffectExecutionError>;
  /**
   * Runs one claimed effect. `willRetry` reports remaining retry budget;
   * unknown native outcomes remain held independently of that budget.
   */
  readonly execute: (
    effect: EffectOutbox.OrchestrationEffectV2,
    options?: { readonly willRetry: boolean },
  ) => Effect.Effect<EffectExecutionResult, OrchestrationEffectExecutionError>;
}

const isParkedDelivery = (
  result: EffectExecutionResult,
): result is EffectOutbox.ParkedImportedHistoryDeliveryV1 =>
  result !== undefined && "status" in result && result.status === "parked";

const matchesParkedDeliveryState = (
  claimed: EffectOutbox.OrchestrationEffectV2,
  current: EffectOutbox.OrchestrationEffectV2,
): boolean =>
  claimed.request.type === "provider-turn.start" &&
  current.id === claimed.id &&
  current.commandId === claimed.commandId &&
  current.threadId === claimed.threadId &&
  current.request.type === "provider-turn.start" &&
  current.request.runId === claimed.request.runId &&
  current.nativeCreationExecutionReference === undefined &&
  current.status === "pending" &&
  current.leaseOwner === null &&
  current.leaseExpiresAt === null &&
  current.completedAt === null &&
  current.attemptCount === claimed.attemptCount - 1 &&
  current.lastError === "imported-history.waiting-for-head/v1";

const matchesNativeHoldEvidence = (
  actual: EffectOutbox.UnknownEffectHoldV2["evidence"],
  expected: ProviderNativeEffectEvidence,
): boolean =>
  !("kind" in actual && actual.kind === "resource_cleanup") &&
  Schema.is(ProviderNativeEffectEvidence)(actual) &&
  (
    Object.keys(ProviderNativeEffectEvidence.fields) as ReadonlyArray<
      keyof ProviderNativeEffectEvidence
    >
  ).every((key) => actual[key] === expected[key]);

const hasOrdinaryImmediateLineage = (
  facts: EventSink.NativeCommandFactsV2,
  effect: EffectOutbox.OrchestrationEffectV2,
  incarnation: ApplicationThreadBirthV2,
): boolean => {
  if (
    effect.request.type !== "provider-turn.start" ||
    facts.commandId !== effect.commandId ||
    facts.threadId !== effect.threadId ||
    facts.commitSnapshot.commandId !== effect.commandId ||
    facts.commitSnapshot.threadId !== effect.threadId ||
    facts.eventMetadataOverflow !== false ||
    !Array.isArray(facts.events) ||
    facts.events.length > 256 ||
    facts.receipt?.status !== "accepted" ||
    facts.receipt.commandId !== effect.commandId ||
    facts.receipt.threadId !== effect.threadId ||
    (facts.identity !== null &&
      (facts.identity?.kind !== "guarded_message_dispatch" ||
        facts.identity.commandId !== effect.commandId ||
        facts.identity.commandType !== "message.dispatch" ||
        facts.identity.aggregateId !== effect.threadId))
  )
    return false;
  const runId = effect.request.runId;
  const events = facts.events.filter(
    (stored) =>
      stored.commandId === effect.commandId &&
      stored.event.threadId === effect.threadId &&
      stored.sequence > incarnation.sequence,
  );
  const immediate = facts.receipt.commandType === "message.dispatch";
  if (!immediate && facts.receipt.commandType !== "prepared-run.release") return false;
  const runs = events.flatMap((stored) =>
    (stored.event.type === "run.created" || stored.event.type === "run.updated") &&
    stored.event.type === (immediate ? "run.created" : "run.updated") &&
    stored.event.payload.id === runId
      ? [stored.event.payload]
      : [],
  );
  if (runs.length !== 1) return false;
  const run = runs[0]!;
  if (
    run.threadId !== effect.threadId ||
    run.status !== "starting" ||
    run.queuePosition !== null ||
    run.activeAttemptId === null ||
    run.rootNodeId === null ||
    run.providerThreadId === null
  )
    return false;
  const nodes = events.flatMap((stored) =>
    stored.event.type === "node.updated" &&
    stored.event.payload.id === run.rootNodeId &&
    stored.event.payload.runId === run.id &&
    stored.event.payload.providerThreadId === run.providerThreadId
      ? [stored.event.payload]
      : [],
  );
  if (
    nodes.length !== 1 ||
    nodes[0]!.checkpointScopeId === null ||
    !events.some(
      (stored) =>
        stored.event.type === "checkpoint-scope.created" &&
        stored.event.payload.id === nodes[0]!.checkpointScopeId &&
        stored.event.payload.runId === run.id &&
        stored.event.payload.nodeId === run.rootNodeId &&
        stored.event.payload.providerThreadId === run.providerThreadId,
    )
  )
    return false;
  return immediate
    ? events.some(
        (stored) =>
          stored.event.type === "run-attempt.created" &&
          stored.event.payload.id === run.activeAttemptId &&
          stored.event.payload.runId === run.id &&
          stored.event.payload.rootNodeId === run.rootNodeId &&
          stored.event.payload.providerThreadId === run.providerThreadId,
      ) &&
        events.some(
          (stored) =>
            stored.event.type === "message.updated" &&
            stored.event.payload.id === run.userMessageId &&
            stored.event.payload.runId === run.id &&
            stored.event.payload.nodeId === run.rootNodeId &&
            stored.event.payload.role === "user",
        )
    : events.some(
        (stored) =>
          stored.event.type === "turn-item.updated" &&
          stored.event.payload.type === "command_execution" &&
          stored.event.payload.runId === run.id &&
          stored.event.payload.nodeId === run.rootNodeId &&
          stored.event.payload.input === "Preparing workspace" &&
          stored.event.payload.status === "completed" &&
          stored.event.payload.exitCode === 0,
      );
};

const hasOrdinaryArchiveLineage = (
  facts: EventSink.NativeCommandFactsV2,
  effect: EffectOutbox.OrchestrationEffectV2,
  incarnation: ApplicationThreadBirthV2,
): boolean => {
  if (
    effect.request.type !== "terminal.cleanup" ||
    effect.id !== `effect:${effect.commandId}:terminal.cleanup` ||
    facts.commandId !== effect.commandId ||
    facts.threadId !== effect.threadId ||
    facts.eventMetadataOverflow !== false ||
    facts.receipt?.status !== "accepted" ||
    facts.receipt.commandType !== "thread.archive" ||
    facts.receipt.commandId !== effect.commandId ||
    facts.receipt.threadId !== effect.threadId ||
    facts.identity !== null ||
    facts.commitSnapshot.commandId !== effect.commandId ||
    facts.commitSnapshot.threadId !== effect.threadId ||
    !Array.isArray(facts.events) ||
    facts.events.length > 256
  )
    return false;
  const archived = facts.events.filter(
    (stored) =>
      stored.commandId === effect.commandId &&
      stored.event.type === "thread.archived" &&
      stored.event.threadId === effect.threadId &&
      stored.sequence > incarnation.sequence,
  );
  const rows = facts.commitSnapshot.records.threads;
  if (
    archived.length !== 1 ||
    !Array.isArray(rows) ||
    rows.length !== 1 ||
    rows[0]!.thread_id !== effect.threadId ||
    typeof rows[0]!.payload_json !== "string"
  )
    return false;
  const event = archived[0]!.event;
  if (
    event.type !== "thread.archived" ||
    event.payload.id !== effect.threadId ||
    event.payload.archivedAt === null ||
    event.payload.deletedAt !== null
  )
    return false;
  const current = Schema.decodeUnknownOption(
    Schema.fromJsonString(
      Schema.Struct({
        id: ThreadId,
        archivedAt: Schema.NullOr(Schema.String),
        deletedAt: Schema.NullOr(Schema.String),
      }),
    ),
  )(rows[0]!.payload_json);
  return (
    Option.isSome(current) &&
    current.value.id === effect.threadId &&
    current.value.deletedAt === null &&
    current.value.archivedAt === DateTime.formatIso(event.payload.archivedAt)
  );
};

export class OrchestrationEffectExecutorV2 extends Context.Service<
  OrchestrationEffectExecutorV2,
  OrchestrationEffectExecutorV2Shape
>()("t3/orchestration-v2/EffectWorker/OrchestrationEffectExecutorV2") {}

export const executorLayer: Layer.Layer<
  OrchestrationEffectExecutorV2,
  never,
  | ProviderSessionManager.ProviderSessionManagerV2
  | NativeCreationAuthority.NativeCreationAuthority
  | NativeCreationRepository
  | EventSink.EventSinkV2
  | EffectOutbox.EffectOutboxV2
  | RunFinalizationService.RunFinalizationService
  | CheckpointRollbackService.CheckpointRollbackServiceV2
  | ProviderTurnControlService.ProviderTurnControlServiceV2
  | ProviderTurnStartService.ProviderTurnStartServiceV2
  | RuntimeRequestService.RuntimeRequestServiceV2
  | ThreadTitleRegenerationService.ThreadTitleRegenerationService
  | ThreadManagementService.ThreadManagementService
  | ServerSettings.ServerSettingsService
> = Layer.effect(
  OrchestrationEffectExecutorV2,
  Effect.gen(function* () {
    const runFinalization = yield* RunFinalizationService.RunFinalizationService;
    const threadDispatch = yield* ThreadCommandExecutor;
    const resourceCleanup = yield* ResourceCleanupService.ResourceCleanupService;
    const checkpointRollback = yield* CheckpointRollbackService.CheckpointRollbackServiceV2;
    const providerSessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const providerTurnControl = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
    const providerTurnStart = yield* ProviderTurnStartService.ProviderTurnStartServiceV2;
    const runtimeRequests = yield* RuntimeRequestService.RuntimeRequestServiceV2;
    const threadTitleRegeneration =
      yield* ThreadTitleRegenerationService.ThreadTitleRegenerationService;
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const settings = yield* ServerSettings.ServerSettingsService;
    const nativeAuthority = yield* NativeCreationAuthority.NativeCreationAuthority;
    const nativeRepository = yield* NativeCreationRepository;
    const eventSink = yield* EventSink.EventSinkV2;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const importedHistoryPreparation = yield* ImportedHistoryStartExecutionPreparation;
    const ordinaryHandles = new Map<
      string,
      RunExecutionService.OrdinaryManagedRunExecutionHandleV1
    >();
    const prepareOrdinaryClaim = (effect: EffectOutbox.OrchestrationEffectV2) =>
      Effect.gen(function* () {
        const link = yield* eventSink.readOrdinaryCheckoutEffectLink(effect.id);
        if (link === null) return undefined;
        if (
          !("runId" in effect.request) ||
          effect.leaseOwner === null ||
          effect.leaseExpiresAt === null
        )
          return yield* new OrchestrationEffectExecutionError({
            effectId: effect.id,
            effectType: effect.request.type,
            cause: "The admitted checkout effect has no actual running claim and run.",
          });
        const admission = yield* eventSink.readOrdinaryCheckoutAdmissionForRun({
          threadId: effect.threadId,
          runId: effect.request.runId,
        });
        if (
          admission === null ||
          admission.run?.runId !== effect.request.runId ||
          admission.admissionId !== link.admission.admissionId
        )
          return yield* new OrchestrationEffectExecutionError({
            effectId: effect.id,
            effectType: effect.request.type,
            cause: "The real claim lost its original accepted checkout admission.",
          });
        const identity = yield* eventSink.readCommandReceiptIdentity(effect.commandId);
        const system = identity.ordinaryCheckoutSystemLinks?.find(
          (item) => item.effectId === effect.id,
        )?.ordinaryCheckoutExecution;
        const accepted = identity.ordinaryCheckoutCommands.find(
          (item) =>
            item.threadId === effect.threadId &&
            item.admission.admissionId === admission.admissionId,
        );
        const originalUse = system?.originalUse ?? accepted?.joinedUse ?? undefined;
        const source: typeof OrdinaryCheckout.OrdinaryCheckoutOutboxExecutionSourceV1.Type = {
          kind: "outbox",
          link,
          workerId: effect.leaseOwner,
          expectedAttempt: effect.attemptCount,
          leaseExpiresAt: DateTime.makeUnsafe(effect.leaseExpiresAt),
        };
        const retry = yield* eventSink.joinOrdinaryCheckoutRetryClaim(source);
        const ref =
          retry ??
          (originalUse === undefined
            ? yield* Effect.gen(function* () {
                const reserved = yield* threads.beginOrdinaryPreparedCheckoutUse(admission, {
                  operationId: OrdinaryCheckout.ordinaryCheckoutOutboxOperationIdV1(
                    effect.id,
                    effect.attemptCount,
                  ),
                  source,
                });
                return yield* eventSink.bindOrdinaryCheckoutExecution({
                  originalUse: reserved.record.subject.use,
                  executor: { kind: "actual_outbox_claim", source },
                  targetSource: reserved.record.subject.source,
                });
              })
            : yield* eventSink.joinOrdinaryCheckoutClaim({
                originalUse,
                claim: source,
                ...((system ?? accepted?.ordinaryCheckoutExecution) == null
                  ? {}
                  : { predecessorExecution: system ?? accepted!.ordinaryCheckoutExecution! }),
              }));
        const onLoss = Effect.gen(function* () {
          const handle = [...ordinaryHandles.values()].find(
            (item) => item.startExecution.originalUse.operationId === ref.originalUse.operationId,
          );
          if (handle !== undefined)
            yield* handle
              .lose("Checkout ownership renewal or currentness was lost.")
              .pipe(Effect.asVoid);
          const history = yield* eventSink.readOrdinaryCheckoutExecutionAssociations(
            ref.originalUse,
          );
          if (
            history.participants.find((item) => item.ref.associationId === ref.associationId)
              ?.state === "active"
          )
            yield* eventSink
              .recordOrdinaryCheckoutExecutorOutcome({
                ref,
                actualProducerOutcome: {
                  kind: "unknown",
                  reason: "Checkout ownership was lost during the claimed operation.",
                  observedAt: yield* DateTime.now,
                },
              })
              .pipe(Effect.asVoid);
        });
        yield* threads.registerOrdinaryCheckoutExecution(ref, {
          revalidateCaptured: Effect.void,
          onLoss,
        });
        yield* eventSink.revalidateOrdinaryCheckoutExecution(ref);
        return ref;
      });
    const ordinaryStartInput = (
      ref: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1 | undefined,
    ) =>
      ref === undefined
        ? {}
        : {
            ordinaryCheckoutUse: ref.originalUse,
            ordinaryCheckoutExecution: ref,
            onOrdinaryManagedRunStarted: (
              handle: RunExecutionService.OrdinaryManagedRunExecutionHandleV1,
              confirmation?: ProviderTurnStartService.ProviderNativeStartConfirmation,
            ) =>
              Effect.gen(function* () {
                const issued = RunExecutionService.readIssuedOrdinaryManagedRunStartObservation(
                  handle.actualStartObservation,
                );
                if (
                  issued === null ||
                  nativeCreationCanonicalJson(
                    yield* Schema.encodeEffect(OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1)(
                      handle.startExecution,
                    ).pipe(Effect.orDie),
                  ) !==
                    nativeCreationCanonicalJson(
                      yield* Schema.encodeEffect(OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1)(
                        ref,
                      ).pipe(Effect.orDie),
                    )
                )
                  return yield* new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
                    reason: "unknown_use",
                    threadId: ref.originalUse.lease.ownerThreadId,
                    path: ref.originalUse.lease.resourcePath,
                    message: "The provider returned no original issued start observation.",
                  });
                if (confirmation !== undefined && ref.executor.kind === "actual_outbox_claim") {
                  yield* nativeRepository.recordNativeEffectConfirmation({
                    effectId: ref.executor.source.link.effectId,
                    workerId: ref.executor.source.workerId,
                    expectedAttempt: ref.executor.source.expectedAttempt,
                    runId: handle.managedExecutor.run.runId,
                    attemptId: confirmation.attemptId,
                    binding: confirmation.binding,
                    expectedEvidenceRevision: confirmation.evidenceRevision,
                    evidence: confirmation.nativeEffect,
                  });
                }
                const managed = yield* eventSink.activateOrdinaryCheckoutManagedRun({
                  startExecution: ref,
                  managedExecutor: handle.managedExecutor,
                  actualStartObservation: issued.observation,
                  revalidateCaptured: issued.revalidateIssued,
                });
                yield* handle.activate(managed);
                ordinaryHandles.set(managed.associationId, handle);
                yield* threads.registerOrdinaryCheckoutExecution(managed, {
                  revalidateCaptured: handle.revalidateCaptured,
                  onLoss: handle
                    .lose("The managed run lost its original checkout ownership.")
                    .pipe(Effect.asVoid),
                });
              }),
          };
    const startOrdinaryClaim = (
      input: Parameters<ProviderTurnStartService.ProviderTurnStartServiceV2Shape["start"]>[0],
      ordinary: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1 | undefined,
    ) =>
      Effect.gen(function* () {
        let failed: EventSink.StartFailedBeforeOpenObservationV1 | undefined;
        let retry: EventSink.StartRetryBeforeOpenObservationV1 | undefined;
        const result = yield* providerTurnStart.start(input).pipe(
          Effect.provideService(ProviderTurnStartService.StartOutcomeSink, (observation) =>
            Effect.sync(() => {
              failed = observation;
            }),
          ),
          Effect.provideService(ProviderTurnStartService.StartRetryOutcomeSink, (observation) =>
            Effect.sync(() => {
              retry = observation;
            }),
          ),
          Effect.exit,
        );
        if (Exit.isFailure(result)) {
          if (
            retry !== undefined &&
            !Cause.hasInterrupts(result.cause) &&
            ordinary?.executor.kind === "actual_outbox_claim" &&
            input.willRetry === true
          ) {
            const observation = retry;
            const source = ordinary.executor.source;
            const error = Cause.pretty(result.cause);
            return {
              status: "ordinary_start_retry",
              effectId: source.link.effectId,
              commandId: source.link.commandId,
              threadId: source.link.threadId,
              workerId: source.workerId,
              expectedAttempt: source.expectedAttempt,
              settle: (delayMs: number) =>
                eventSink.settleOrdinaryCheckoutStartRetry({ observation, error, delayMs }),
            } satisfies OrdinaryStartRetryV1;
          }
          return yield* Effect.failCause(result.cause);
        }
        if (failed !== undefined && ordinary !== undefined)
          yield* eventSink.settleOrdinaryCheckoutStartFailedBeforeOpen({
            ref: ordinary,
            observation: failed,
          });
        return undefined;
      });
    const settledOrdinaryClaim = (
      effect: EffectOutbox.OrchestrationEffectV2,
      ref: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1,
    ): SettledOrdinaryClaimV1 => ({
      status: "ordinary_claim_settled",
      effectId: effect.id,
      commandId: effect.commandId,
      threadId: effect.threadId,
      workerId: effect.leaseOwner!,
      expectedAttempt: effect.attemptCount,
      revalidate: Effect.gen(function* () {
        if (
          ref.executor.kind !== "actual_outbox_claim" ||
          ref.executor.source.link.effectId !== effect.id ||
          ref.executor.source.workerId !== effect.leaseOwner ||
          ref.executor.source.expectedAttempt !== effect.attemptCount ||
          (effect.request.type !== "provider-turn.start" &&
            effect.request.type !== "checkpoint.capture")
        )
          return false;
        const current = Option.getOrNull(yield* outbox.get(effect.id));
        if (
          current === null ||
          current.status !== "succeeded" ||
          current.completedAt === null ||
          current.commandId !== effect.commandId ||
          current.threadId !== effect.threadId ||
          current.attemptCount !== effect.attemptCount ||
          nativeCreationCanonicalJson(current.request) !==
            nativeCreationCanonicalJson(effect.request)
        )
          return false;
        const history = yield* eventSink.readOrdinaryCheckoutExecutionAssociations(ref.originalUse);
        return (
          history.participants.find((item) => item.ref.associationId === ref.associationId)
            ?.state === "retired" &&
          history.facts.some(
            (fact) =>
              fact.eventKind === "retire" &&
              nativeCreationCanonicalJson(
                Schema.encodeSync(OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1)(fact.ref),
              ) ===
                nativeCreationCanonicalJson(
                  Schema.encodeSync(OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1)(ref),
                ) &&
              fact.evidence.schema === "t3.ordinary-checkout-execution-outcome/v1" &&
              (effect.request.type === "provider-turn.start"
                ? fact.evidence.actualProducerOutcome.kind === "start_activated" ||
                  fact.evidence.actualProducerOutcome.kind === "start_failed_before_open"
                : fact.evidence.actualProducerOutcome.kind === "checkpoint_captured"),
          )
        );
      }),
    });
    const executeAttachmentNamespaceCleanup = (effect: EffectOutbox.OrchestrationEffectV2) =>
      Effect.gen(function* () {
        const same = (left: unknown, right: unknown) =>
          nativeCreationCanonicalJson(left) === nativeCreationCanonicalJson(right);
        const taskMatches = (task: EventSink.AttachmentNamespaceCleanupTaskV1) =>
          effect.request.type === "attachment.cleanup" &&
          effect.attachmentNamespaceCleanup !== undefined &&
          effect.nativeCreationExecutionReference === undefined &&
          task.effectId === effect.id &&
          task.commandId === effect.commandId &&
          task.threadId === effect.threadId &&
          same(task.reference, effect.attachmentNamespaceCleanup);
        const rowMatches = (row: EffectOutbox.OrchestrationEffectV2) =>
          row.id === effect.id &&
          row.commandId === effect.commandId &&
          row.threadId === effect.threadId &&
          row.nativeCreationExecutionReference === undefined &&
          same(row.request, effect.request) &&
          same(row.attachmentNamespaceCleanup, effect.attachmentNamespaceCleanup);
        const retained = (
          revalidate: Effect.Effect<boolean, ClaimRevalidationError> = Effect.succeed(false),
        ): AttachmentNamespaceRetainedV1 => ({
          status: "attachment_namespace_retained",
          effectId: effect.id,
          commandId: effect.commandId,
          threadId: effect.threadId,
          workerId: effect.leaseOwner,
          expectedAttempt: effect.attemptCount,
          revalidate,
        });
        const readState = Effect.gen(function* () {
          const task = yield* eventSink.readAttachmentNamespaceCleanupTask(effect.id);
          const latest = yield* eventSink.readAttachmentNamespaceCleanupObservation(effect.id);
          const current = yield* outbox.get(effect.id);
          if (
            task === null ||
            !taskMatches(task) ||
            Option.isNone(current) ||
            !rowMatches(current.value)
          )
            return null;
          if (
            latest !== null &&
            (!same(latest.task, task) ||
              !same(latest.basis.task, task) ||
              latest.observation.effectId !== effect.id ||
              latest.observation.bindingSha256 !== task.bindingSha256 ||
              latest.observation.workerId !== latest.basis.claim.workerId ||
              latest.observation.expectedAttempt !== latest.basis.claim.expectedAttempt ||
              latest.observation.basisEventSequence !== latest.basis.basisEventSequence)
          )
            return null;
          return { task, latest, current: current.value };
        });
        const fromState = (
          state: Effect.Success<typeof readState>,
        ): DeletionCleanupCompletedV1 | AttachmentNamespaceRetainedV1 | undefined => {
          if (state === null || state.latest === null) return undefined;
          const proof = state.latest;
          if (
            proof.observation.expectedAttempt !== effect.attemptCount ||
            (effect.leaseOwner !== null && proof.observation.workerId !== effect.leaseOwner)
          )
            return undefined;
          const completed =
            proof.status === "completed" &&
            state.current.status === "succeeded" &&
            state.current.completedAt !== null &&
            state.current.attemptCount === proof.observation.expectedAttempt;
          if (!completed && proof.status !== "unknown" && proof.status !== "retryable")
            return undefined;
          const revalidate = Effect.gen(function* () {
            const next = yield* readState;
            return (
              next !== null &&
              next.latest !== null &&
              same(next.task, state.task) &&
              same(next.latest, proof) &&
              (completed
                ? next.current.status === "succeeded" &&
                  next.current.completedAt !== null &&
                  next.current.attemptCount === proof.observation.expectedAttempt
                : next.current.status !== "succeeded")
            );
          });
          return completed
            ? {
                status: "cleanup_completed",
                effectId: effect.id,
                commandId: effect.commandId,
                threadId: effect.threadId,
                workerId: proof.observation.workerId,
                expectedAttempt: effect.attemptCount,
                revalidate,
              }
            : retained(revalidate);
        };
        const before = yield* readState.pipe(Effect.orElseSucceed(() => null));
        const recorded = fromState(before);
        if (recorded !== undefined) return recorded;
        if (
          before === null ||
          effect.leaseOwner === null ||
          resourceCleanup.cleanupAttachmentNamespace === undefined
        )
          return retained();
        if (before.task.reference.mode === "prune_thread") {
          const prepared = yield* Effect.exit(
            threads.ensureApplicationAttachmentInventory({
              threadId: before.task.threadId,
              expectedBirth: before.task.reference.ownerBirth,
            }),
          );
          if (Exit.isFailure(prepared)) return retained();
        }
        const basis = yield* eventSink
          .readAttachmentNamespaceCleanupBasis({
            effectId: effect.id,
            workerId: effect.leaseOwner,
            expectedAttempt: effect.attemptCount,
          })
          .pipe(Effect.orElseSucceed(() => null));
        if (
          basis === null ||
          basis.status === "unavailable" ||
          !same(basis.task, before.task) ||
          basis.claim.workerId !== effect.leaseOwner ||
          basis.claim.expectedAttempt !== effect.attemptCount
        )
          return retained();
        yield* Effect.exit(resourceCleanup.cleanupAttachmentNamespace(basis));
        return fromState(yield* readState.pipe(Effect.orElseSucceed(() => null))) ?? retained();
      }).pipe(
        Effect.mapError(
          (cause) =>
            new OrchestrationEffectExecutionError({
              effectId: effect.id,
              effectType: effect.request.type,
              cause,
            }),
        ),
      );
    const completedCleanup = (
      effect: EffectOutbox.OrchestrationEffectV2,
      cleanup: EventSink.DeletionCleanupTaskBindingV1,
      proof: EventSink.ObservedDeletionCleanupOutcomeV1,
      workerId: string,
    ): DeletionCleanupCompletedV1 => ({
      status: "cleanup_completed",
      effectId: effect.id,
      commandId: effect.commandId,
      threadId: effect.threadId,
      workerId,
      expectedAttempt: effect.attemptCount,
      revalidate: Effect.gen(function* () {
        const persisted = yield* eventSink.readDeletionCleanupTaskOutcome(effect.id);
        const originalTask = yield* eventSink.readDeletionCleanupTask(effect.id);
        const completed = yield* outbox.get(effect.id);
        return (
          persisted !== null &&
          originalTask !== null &&
          originalTask.bindingSha256 === cleanup.bindingSha256 &&
          nativeCreationCanonicalJson(originalTask) === nativeCreationCanonicalJson(cleanup) &&
          persisted.ordinal === proof.ordinal &&
          persisted.correlation.workerId === workerId &&
          persisted.correlation.expectedAttempt === effect.attemptCount &&
          persisted.correlation.bindingSha256 === cleanup.bindingSha256 &&
          nativeCreationCanonicalJson(persisted.outcome) ===
            nativeCreationCanonicalJson(proof.outcome) &&
          nativeCreationCanonicalJson(persisted.correlation.evidence) ===
            nativeCreationCanonicalJson(proof.evidence) &&
          Option.isSome(completed) &&
          completed.value.status === "succeeded" &&
          completed.value.commandId === effect.commandId &&
          completed.value.threadId === effect.threadId &&
          completed.value.attemptCount === effect.attemptCount &&
          nativeCreationCanonicalJson(completed.value.request) ===
            nativeCreationCanonicalJson(effect.request)
        );
      }),
    });
    const executeResourceCleanup = (effect: EffectOutbox.OrchestrationEffectV2) =>
      Effect.gen(function* () {
        const kind = effect.request.type === "terminal.cleanup" ? "terminal" : "attachment";
        const unbound: EffectOutbox.ResourceCleanupUnknownEvidenceV1 = {
          version: 1,
          kind: "resource_cleanup",
          operationId: effect.id,
          threadId: effect.threadId,
          taskKind: kind,
          bindingSha256: null,
          reason: "task_binding_unavailable",
          outcome: "unknown",
        };
        const read = yield* Effect.exit(eventSink.readDeletionCleanupTask(effect.id));
        let candidate = Exit.isSuccess(read) ? read.value : null;
        if (
          Exit.isSuccess(read) &&
          candidate === null &&
          kind === "terminal" &&
          resourceCleanup.captureOwnedTerminalTargets !== undefined
        ) {
          const captureOriginal = resourceCleanup.captureOwnedTerminalTargets;
          const preparation = yield* Effect.exit(
            Effect.gen(function* () {
              const current = yield* outbox.get(effect.id);
              const holds = yield* outbox.listHeldByThreadId(effect.threadId);
              const now = DateTime.toEpochMillis(yield* DateTime.now);
              if (
                holds.some((hold) => hold.effectId === effect.id) ||
                Option.isNone(current) ||
                !matchesResourceClaim(effect, current.value) ||
                current.value.leaseExpiresAt === null ||
                !Number.isFinite(Date.parse(current.value.leaseExpiresAt)) ||
                Date.parse(current.value.leaseExpiresAt) <= now
              )
                return null;
              const original = yield* eventSink.readThreadDeletionCommand(effect.commandId);
              if (
                original === null ||
                original.command.type !== "thread.delete" ||
                original.command.commandId !== effect.commandId ||
                original.command.threadId !== effect.threadId ||
                original.deletion.commandId !== effect.commandId ||
                original.ownerBirth === null ||
                original.ownerBirth.threadId !== effect.threadId ||
                original.inventory.captureStatus !== "captured" ||
                !original.inventory.prerequisiteEffectIds.includes(effect.id) ||
                (original.inventory.leaseInventory.status !== "original" &&
                  original.inventory.leaseInventory.status !== "absent")
              )
                return null;
              const capture = yield* captureOriginal(original.ownerBirth);
              if (
                capture.threadId !== effect.threadId ||
                nativeCreationCanonicalJson(capture.ownerBirth) !==
                  nativeCreationCanonicalJson(original.ownerBirth)
              )
                return null;
              // The store decodes this same task; an unknown capture still prepares nothing.
              const terminalTask = yield* Schema.decodeUnknownEffect(EventSink.LeaseCleanupTaskV2)(
                { kind: "terminal", capture },
                { onExcessProperty: "error" },
              );
              if (terminalTask.kind !== "terminal") return null;
              yield* eventSink.prepareDeletionCleanupTaskBindings({
                commandId: effect.commandId,
                terminalCapture: terminalTask.capture,
              });
              return yield* eventSink.readDeletionCleanupTask(effect.id);
            }),
          );
          if (Exit.isSuccess(preparation)) candidate = preparation.value;
        }
        let task: EventSink.DeletionCleanupTaskBindingV1 | null = null;
        if (candidate !== null && Schema.is(EventSink.DeletionCleanupTaskBindingV1)(candidate)) {
          const { bindingSha256, recordedAt: _recordedAt, ...subject } = candidate;
          if (
            candidate.effectId === effect.id &&
            candidate.threadId === effect.threadId &&
            candidate.deletion.commandId === effect.commandId &&
            candidate.task.kind === kind &&
            EventSink.deletionCleanupTaskBindingDigestV1(subject) === bindingSha256 &&
            (candidate.task.kind !== "attachment" ||
              (effect.request.type === "attachment.cleanup" &&
                candidate.task.attachmentIds.length === effect.request.attachmentIds.length &&
                candidate.task.attachmentIds.every(
                  (id, index) =>
                    effect.request.type === "attachment.cleanup" &&
                    id === effect.request.attachmentIds[index],
                )))
          )
            task = candidate;
        }
        if (Exit.isSuccess(read) && candidate === null && kind === "terminal") {
          const archive = yield* Effect.exit(
            Effect.gen(function* () {
              const facts = yield* eventSink.readNativeCommandFacts({
                threadId: effect.threadId,
                commandId: effect.commandId,
              });
              const incarnation = yield* eventSink.readApplicationBirthRecord(effect.threadId);
              if (incarnation === null || !hasOrdinaryArchiveLineage(facts, effect, incarnation))
                return false;
              const current = yield* outbox.get(effect.id);
              const prior = yield* outbox.listHeldByThreadId(effect.threadId);
              const now = DateTime.toEpochMillis(yield* DateTime.now);
              return (
                prior.every((hold) => hold.effectId !== effect.id) &&
                Option.isSome(current) &&
                matchesResourceClaim(effect, current.value) &&
                current.value.leaseExpiresAt !== null &&
                Number.isFinite(Date.parse(current.value.leaseExpiresAt)) &&
                Date.parse(current.value.leaseExpiresAt) > now
              );
            }),
          );
          if (Exit.isSuccess(archive) && archive.value)
            return yield* resourceCleanup.cleanupTerminals(effect.threadId);
        }
        const evidence: EffectOutbox.ResourceCleanupUnknownEvidenceV1 =
          task === null
            ? unbound
            : {
                version: 1,
                kind: "resource_cleanup",
                operationId: effect.id,
                threadId: effect.threadId,
                taskKind: kind,
                bindingSha256: task.bindingSha256,
                outcome: "unknown",
              };
        const unknown = (cause: unknown) => new ResourceCleanupUnknownError({ evidence, cause });
        if (effect.leaseOwner === null)
          return yield* unknown("Resource cleanup has no owned running claim.");
        const inserted = yield* outbox
          .holdResourceCleanupUnknown({
            effectId: effect.id,
            workerId: effect.leaseOwner,
            expectedAttempt: effect.attemptCount,
            evidence,
          })
          .pipe(Effect.catchCause((cause) => Effect.fail(unknown(cause))));
        const held = yield* readResourceHold(outbox, effect, evidence, inserted).pipe(
          Effect.catchCause((cause) => Effect.fail(unknown(cause))),
        );
        if (!held)
          return yield* unknown(
            "Resource cleanup lacks its exact persisted unknown hold and claim.",
          );
        // Only a newly committed hold permits this invocation's first conditional close.
        // An existing hold is an unknown prior effect and permits reconciliation only.
        if (!inserted || task === null) return { status: "cleanup_held" as const, evidence };
        const pinned = task;
        const prior = yield* eventSink
          .readDeletionCleanupTaskOutcome(effect.id)
          .pipe(Effect.catchCause((cause) => Effect.fail(unknown(cause))));
        if (prior !== null) return { status: "cleanup_held" as const, evidence };
        const owned =
          kind === "terminal"
            ? resourceCleanup.cleanupOwnedTerminals
            : resourceCleanup.cleanupOwnedAttachments;
        const result =
          owned === undefined
            ? null
            : yield* Effect.exit(
                Effect.suspend(() =>
                  owned(pinned, {
                    workerId: effect.leaseOwner!,
                    expectedAttempt: effect.attemptCount,
                  }),
                ),
              );
        if (result !== null && Exit.isSuccess(result) && result.value.observation !== undefined) {
          const observation = result.value.observation;
          const coveredHolds = (yield* outbox.listHeldByThreadId(effect.threadId)).filter(
            (hold) => hold.effectId === effect.id,
          );
          const proof = yield* eventSink
            .recordObservedDeletionCleanupOutcome({
              effectId: effect.id,
              bindingSha256: pinned.bindingSha256,
              expectedLatestOrdinal: -1,
              observation,
              coveredHolds,
            })
            .pipe(Effect.catchCause((cause) => Effect.fail(unknown(cause))));
          if (
            proof.outcome.taskId === effect.id &&
            proof.outcome.result === "succeeded" &&
            (proof.outcome.effect === "confirmed" || proof.outcome.effect === "absent") &&
            proof.bindingSha256 === pinned.bindingSha256 &&
            proof.ordinal === 0 &&
            proof.evidence.producer === "managed_terminal" &&
            nativeCreationCanonicalJson(proof.evidence.observation) ===
              nativeCreationCanonicalJson(observation)
          ) {
            if (
              !(yield* eventSink.completeObservedDeletionCleanup({
                effectId: effect.id,
                bindingSha256: pinned.bindingSha256,
                expectedLatestOrdinal: proof.ordinal,
              }))
            )
              return yield* unknown(
                "Managed terminal cleanup did not complete its exact qualified ordinal.",
              );
            const completion = completedCleanup(effect, pinned, proof, effect.leaseOwner);
            if (!(yield* completion.revalidate))
              return yield* unknown(
                "Managed terminal cleanup failed its durable completion readback.",
              );
            return completion;
          }
          if (!(yield* readResourceHold(outbox, effect, evidence)))
            return yield* unknown("Managed terminal cleanup lost its original unknown hold.");
          return { status: "cleanup_held" as const, evidence };
        }
        const outcome: EventSink.LeaseCleanupTaskOutcomeV2 = {
          taskId: effect.id,
          result: null,
          effect: "unknown",
        };
        const details =
          result !== null &&
          Exit.isSuccess(result) &&
          result.value.outcome.taskId === effect.id &&
          result.value.outcome.result === null &&
          result.value.outcome.effect === "unknown"
            ? result.value.evidence
            : {
                reason:
                  owned === undefined
                    ? "owned_cleanup_unavailable"
                    : "complete_cleanup_outcome_unavailable",
              };
        yield* Effect.exit(
          eventSink.recordLeaseCleanupTaskOutcome({
            effectId: effect.id,
            workerId: effect.leaseOwner,
            expectedAttempt: effect.attemptCount,
            outcome,
            evidence: details,
          }),
        );
        if (
          !(yield* readResourceHold(outbox, effect, evidence).pipe(
            Effect.orElseSucceed(() => false),
          ))
        )
          return yield* unknown(
            "Resource cleanup lost its persisted unknown hold after the conditional close.",
          );
        return { status: "cleanup_held" as const, evidence };
      }).pipe(
        Effect.mapError(
          (cause) =>
            new OrchestrationEffectExecutionError({
              effectId: effect.id,
              effectType: effect.request.type,
              cause,
            }),
        ),
      );
    const executeWorktreeCleanup = (effect: EffectOutbox.OrchestrationEffectV2) =>
      Effect.gen(function* () {
        const read = yield* Effect.exit(eventSink.readDeletionWorktreeTask(effect.id));
        const candidate = Exit.isSuccess(read) ? read.value : null;
        let task: EventSink.DeletionWorktreeTaskBindingV1 | null = null;
        if (candidate !== null && Schema.is(EventSink.DeletionWorktreeTaskBindingV1)(candidate)) {
          const { bindingSha256, recordedAt: _recordedAt, ...subject } = candidate;
          if (
            candidate.effectId === effect.id &&
            candidate.threadId === effect.threadId &&
            candidate.deletion.commandId === effect.commandId &&
            effect.id === EventSink.deletionWorktreeEffectIdV1(effect.commandId, effect.threadId) &&
            EventSink.deletionWorktreeTaskBindingDigestV1(subject) === bindingSha256
          )
            task = candidate;
        }
        const evidence: EffectOutbox.ResourceCleanupUnknownEvidenceV1 =
          task === null
            ? {
                version: 1,
                kind: "resource_cleanup",
                operationId: effect.id,
                threadId: effect.threadId,
                taskKind: "worktree",
                bindingSha256: null,
                reason: "task_binding_unavailable",
                outcome: "unknown",
              }
            : {
                version: 1,
                kind: "resource_cleanup",
                operationId: effect.id,
                threadId: effect.threadId,
                taskKind: "worktree",
                bindingSha256: task.bindingSha256,
                outcome: "unknown",
              };
        const unknown = (cause: unknown) => new ResourceCleanupUnknownError({ evidence, cause });
        return yield* Effect.gen(function* () {
          const retain = () =>
            Effect.gen(function* () {
              if (effect.leaseOwner === null)
                return yield* unknown("Worktree cleanup has no owned running claim.");
              const inserted = yield* outbox.holdResourceCleanupUnknown({
                effectId: effect.id,
                workerId: effect.leaseOwner,
                expectedAttempt: effect.attemptCount,
                evidence,
              });
              if (!(yield* readResourceHold(outbox, effect, evidence, inserted)))
                return yield* unknown(
                  "Worktree cleanup lacks its exact persisted unknown hold and claim.",
                );
              return { status: "cleanup_held" as const, evidence };
            });
          if (task === null || resourceCleanup.cleanupOwnedWorktree === undefined)
            return yield* retain();
          const pinned = task;
          if (effect.leaseOwner === null)
            return yield* unknown("Worktree cleanup has no owned running claim.");
          const workerId = effect.leaseOwner;
          const current = yield* outbox.get(effect.id);
          const now = DateTime.toEpochMillis(yield* DateTime.now);
          if (
            Option.isNone(current) ||
            !matchesResourceClaim(effect, current.value) ||
            current.value.leaseExpiresAt === null ||
            !Number.isFinite(Date.parse(current.value.leaseExpiresAt)) ||
            Date.parse(current.value.leaseExpiresAt) <= now
          )
            return yield* unknown("Worktree cleanup no longer owns its original unexpired claim.");
          const held = (yield* outbox.listHeldByThreadId(effect.threadId)).filter(
            (hold) => hold.effectId === effect.id,
          );
          if (held.length !== 0) {
            if (
              held.length === 1 &&
              "kind" in held[0]!.evidence &&
              held[0]!.evidence.kind === "resource_cleanup" &&
              held[0]!.evidence.bindingSha256 === null &&
              (yield* readResourceHold(outbox, effect, held[0]!.evidence))
            )
              return { status: "cleanup_held" as const, evidence: held[0]!.evidence };
            if (!(yield* readResourceHold(outbox, effect, evidence)))
              return yield* unknown("Worktree cleanup has an unrelated prior hold.");
            const basis = yield* eventSink.readDeletionWorktreeExecutionBasis(effect.id);
            if (
              basis === null ||
              basis.binding.bindingSha256 !== pinned.bindingSha256 ||
              basis.start === null
            )
              return yield* retain();
          }
          const result = yield* Effect.exit(
            resourceCleanup.cleanupOwnedWorktree({
              effectId: effect.id,
              bindingSha256: pinned.bindingSha256,
              workerId,
              expectedAttempt: effect.attemptCount,
            }),
          );
          if (
            Exit.isFailure(result) ||
            result.value.status !== "completed" ||
            result.value.effectId !== effect.id ||
            result.value.bindingSha256 !== pinned.bindingSha256 ||
            result.value.expectedLatestOrdinal === null ||
            !Number.isSafeInteger(result.value.expectedLatestOrdinal) ||
            result.value.expectedLatestOrdinal < 0
          )
            return yield* retain();
          const ordinal = result.value.expectedLatestOrdinal;
          const completion: DeletionCleanupCompletedV1 = {
            status: "cleanup_completed",
            effectId: effect.id,
            commandId: effect.commandId,
            threadId: effect.threadId,
            workerId,
            expectedAttempt: effect.attemptCount,
            revalidate: Effect.gen(function* () {
              const original = yield* eventSink.readDeletionWorktreeTask(effect.id);
              const basis = yield* eventSink.readDeletionWorktreeExecutionBasis(effect.id);
              const latest = yield* eventSink.readDeletionCleanupTaskOutcome(effect.id);
              const completed = yield* outbox.get(effect.id);
              if (
                original === null ||
                basis === null ||
                latest === null ||
                basis.start === null ||
                basis.admission === null ||
                nativeCreationCanonicalJson(original) !== nativeCreationCanonicalJson(pinned) ||
                nativeCreationCanonicalJson(basis.binding) !==
                  nativeCreationCanonicalJson(pinned) ||
                latest.ordinal !== ordinal ||
                latest.correlation.workerId !== workerId ||
                latest.correlation.expectedAttempt !== effect.attemptCount ||
                latest.correlation.bindingSha256 !== pinned.bindingSha256 ||
                latest.outcome.taskId !== effect.id ||
                latest.outcome.result !== "succeeded" ||
                (latest.outcome.effect !== "confirmed" && latest.outcome.effect !== "absent") ||
                latest.correlation.evidence.producer !== "worktree" ||
                !Schema.is(EventSink.DeletionWorktreeRemovalObservationSchemaV1)(
                  latest.correlation.evidence.observation,
                )
              )
                return false;
              const observation = latest.correlation.evidence.observation;
              const retirement = basis.admission.outcome;
              return (
                observation.start.effectId === effect.id &&
                observation.start.bindingSha256 === pinned.bindingSha256 &&
                observation.start.workerId === workerId &&
                observation.start.expectedAttempt === effect.attemptCount &&
                observation.startOrdinal === basis.start.ordinal &&
                nativeCreationCanonicalJson(observation.start) ===
                  nativeCreationCanonicalJson(basis.start.evidence) &&
                basis.latestOutcome?.ordinal === ordinal &&
                nativeCreationCanonicalJson(basis.latestOutcome.outcome) ===
                  nativeCreationCanonicalJson(latest.outcome) &&
                basis.admission.state === "released" &&
                basis.currentLease === "absent" &&
                retirement !== null &&
                retirement.schema === "t3.deletion-worktree-retirement/v1" &&
                retirement.effectId === effect.id &&
                retirement.bindingSha256 === pinned.bindingSha256 &&
                retirement.qualifiedOrdinal === ordinal &&
                nativeCreationCanonicalJson(retirement.originalLeaseInventory) ===
                  nativeCreationCanonicalJson(pinned.leaseInventory) &&
                Option.isSome(completed) &&
                completed.value.status === "succeeded" &&
                completed.value.completedAt !== null &&
                completed.value.commandId === effect.commandId &&
                completed.value.threadId === effect.threadId &&
                completed.value.attemptCount === effect.attemptCount &&
                nativeCreationCanonicalJson(completed.value.request) ===
                  nativeCreationCanonicalJson(effect.request)
              );
            }),
          };
          if (!(yield* completion.revalidate))
            return yield* unknown(
              "Worktree cleanup lacks its exact qualified retirement and effect readback.",
            );
          return completion;
        }).pipe(Effect.catchCause((cause) => Effect.fail(unknown(cause))));
      }).pipe(
        Effect.mapError(
          (cause) =>
            new OrchestrationEffectExecutionError({
              effectId: effect.id,
              effectType: effect.request.type,
              cause,
            }),
        ),
      );
    return OrchestrationEffectExecutorV2.of({
      claimAttachmentNamespaceCleanupRetry: (input) =>
        eventSink.claimAttachmentNamespaceCleanup(input).pipe(
          Effect.mapError(
            (cause) =>
              new OrchestrationEffectExecutionError({
                effectId: input.effectId,
                effectType: "attachment.cleanup",
                cause,
              }),
          ),
        ),
      execute: (effect, options) => {
        const willRetry = options?.willRetry ?? false;
        switch (effect.request.type) {
          case "provider-runtime.continue": {
            const sourceRunId = effect.request.sourceRunId;
            return Effect.gen(function* () {
              const marker = yield* eventSink.readReleasedRestartContinuation({
                effectId: effect.id,
                threadId: effect.threadId,
                sourceRunId,
              });
              if (marker !== null && effect.leaseOwner === null)
                return yield* new ProviderNativeOperationUnknownError({
                  nativeEffect: {
                    operationId: effect.id,
                    operation: "resume_thread",
                    threadId: effect.threadId,
                    outcome: "unknown",
                  },
                  cause: "The captured continuation has no current claimed worker.",
                });
              yield* continueRestartedRun({
                threadId: effect.threadId,
                sourceRunId,
                ...(marker === null
                  ? {}
                  : {
                      capturedContinuation: {
                        effectId: effect.id,
                        marker,
                        workerId: effect.leaseOwner!,
                        expectedAttempt: effect.attemptCount,
                      },
                    }),
              });
            }).pipe(
              Effect.provideService(ThreadManagementService.ThreadManagementService, threads),
              Effect.provideService(ServerSettings.ServerSettingsService, settings),
              Effect.provideService(EventSink.EventSinkV2, eventSink),
              Effect.provideService(EffectOutbox.EffectOutboxV2, outbox),
              Effect.mapError(
                (cause) =>
                  new OrchestrationEffectExecutionError({
                    effectId: effect.id,
                    effectType: effect.request.type,
                    cause,
                  }),
              ),
            );
          }
          case "provider-session.detach": {
            const request = effect.request;
            return Effect.gen(function* () {
              const unknown = (cause: unknown, binding?: EventSink.ProviderBindingExpectationV2) =>
                new ProviderNativeOperationUnknownError({
                  nativeEffect: {
                    operationId: effect.id,
                    operation: "close_session",
                    threadId: effect.threadId,
                    ...(binding === undefined
                      ? {}
                      : {
                          providerSessionId: binding.providerSessionId,
                          providerThreadId: binding.providerThreadId,
                          instanceId: binding.instanceId,
                          ...(binding.runtimeGeneration === null
                            ? {}
                            : { runtimeGeneration: binding.runtimeGeneration }),
                        }),
                    outcome: "unknown",
                  },
                  cause,
                });
              const cleanup = yield* eventSink
                .readDeletionCleanupTask(effect.id)
                .pipe(Effect.catchCause((cause) => Effect.fail(unknown(cause))));
              if (cleanup !== null) {
                const task = cleanup.task;
                if (task.kind !== "provider")
                  return yield* unknown("The cleanup effect has a different pinned resource task.");
                const binding = task.expectedBinding;
                const { bindingSha256, recordedAt: _recordedAt, ...subject } = cleanup;
                if (
                  cleanup.effectId !== effect.id ||
                  cleanup.threadId !== effect.threadId ||
                  cleanup.deletion.commandId !== effect.commandId ||
                  binding.threadId !== effect.threadId ||
                  binding.providerSessionId !== request.providerSessionId ||
                  effect.leaseOwner === null ||
                  !Schema.is(EventSink.DeletionCleanupTaskBindingV1)(cleanup) ||
                  EventSink.deletionCleanupTaskBindingDigestV1(subject) !== bindingSha256
                )
                  return yield* unknown(
                    "The provider cleanup differs from its immutable claimed owner task.",
                    binding,
                  );
                const nativeOperation = {
                  operationId: effect.id,
                  operation: "close_session" as const,
                  threadId: effect.threadId,
                  providerSessionId: binding.providerSessionId,
                  providerThreadId: binding.providerThreadId,
                  instanceId: binding.instanceId,
                  runtimeGeneration: binding.runtimeGeneration,
                };
                const evidence: ProviderNativeEffectEvidence = {
                  ...nativeOperation,
                  outcome: "unknown",
                };
                const claimed = Effect.gen(function* () {
                  const current = yield* outbox.get(effect.id);
                  const now = DateTime.toEpochMillis(yield* DateTime.now);
                  return (
                    Option.isSome(current) &&
                    current.value.id === effect.id &&
                    current.value.commandId === effect.commandId &&
                    current.value.threadId === effect.threadId &&
                    current.value.status === "running" &&
                    current.value.leaseOwner === effect.leaseOwner &&
                    current.value.attemptCount === effect.attemptCount &&
                    current.value.request.type === "provider-session.detach" &&
                    current.value.request.providerSessionId === binding.providerSessionId &&
                    current.value.nativeCreationExecutionReference === undefined &&
                    current.value.leaseExpiresAt !== null &&
                    Number.isFinite(Date.parse(current.value.leaseExpiresAt)) &&
                    Date.parse(current.value.leaseExpiresAt) > now
                  );
                });
                const held = Effect.gen(function* () {
                  const holds = (yield* outbox.listHeldByThreadId(effect.threadId)).filter(
                    (hold) => hold.effectId === effect.id,
                  );
                  return holds.length === 1 &&
                    holds[0]!.threadId === effect.threadId &&
                    holds[0]!.workerId === effect.leaseOwner &&
                    holds[0]!.operationId === effect.id &&
                    holds[0]!.expectedAttempt === effect.attemptCount &&
                    matchesNativeHoldEvidence(holds[0]!.evidence, evidence)
                    ? holds
                    : null;
                });
                if (!(yield* claimed))
                  return yield* unknown(
                    "The provider cleanup has no owned unexpired claim.",
                    binding,
                  );
                const inserted = yield* outbox
                  .holdUnknown({
                    effectId: effect.id,
                    workerId: effect.leaseOwner,
                    operationId: effect.id,
                    expectedAttempt: effect.attemptCount,
                    evidence,
                  })
                  .pipe(Effect.catchCause((cause) => Effect.fail(unknown(cause, binding))));
                if ((yield* held) === null)
                  return yield* unknown(
                    "The provider cleanup lacks its exact pre-effect hold.",
                    binding,
                  );
                if (!inserted)
                  return yield* unknown(
                    "The provider cleanup has an unresolved prior invocation; reconcile its stored outcome.",
                    binding,
                  );
                const recorded = yield* eventSink
                  .recordLeaseCleanupTaskOutcome({
                    effectId: effect.id,
                    workerId: effect.leaseOwner,
                    expectedAttempt: effect.attemptCount,
                    outcome: { taskId: effect.id, result: null, effect: "unknown" },
                    evidence,
                  })
                  .pipe(Effect.catchCause((cause) => Effect.fail(unknown(cause, binding))));
                if (
                  recorded.taskId !== effect.id ||
                  recorded.result !== null ||
                  recorded.effect !== "unknown"
                )
                  return yield* unknown(
                    "The pinned provider cleanup lacks its exact durable unknown outcome.",
                    binding,
                  );
                const started = yield* eventSink.readDeletionCleanupTaskOutcome(effect.id);
                if (
                  started === null ||
                  started.outcome.taskId !== effect.id ||
                  started.outcome.result !== null ||
                  started.outcome.effect !== "unknown" ||
                  started.correlation.bindingSha256 !== bindingSha256 ||
                  started.correlation.workerId !== effect.leaseOwner ||
                  started.correlation.expectedAttempt !== effect.attemptCount ||
                  nativeCreationCanonicalJson(started.correlation.evidence) !==
                    nativeCreationCanonicalJson(evidence) ||
                  !(yield* claimed) ||
                  (yield* held) === null
                )
                  return yield* unknown(
                    "The provider cleanup's durable intent or original claim changed before dispatch.",
                    binding,
                  );
                const stopped = yield* providerSessions
                  .stopPinnedRuntime({
                    operationId: effect.id,
                    binding,
                    expectedEvidenceRevision: task.evidenceRevision,
                    deletionBindingSha256: bindingSha256,
                  })
                  .pipe(
                    Effect.catchCause(() =>
                      Effect.succeed({
                        status: "unknown" as const,
                        reason: "managed_provider_stop_unconfirmed",
                      }),
                    ),
                  );
                const observation: EventSink.ManagedProviderDeletionObservationV1 = {
                  version: 1,
                  kind: "managed_provider",
                  effectId: effect.id,
                  bindingSha256,
                  workerId: effect.leaseOwner,
                  expectedAttempt: effect.attemptCount,
                  binding,
                  evidenceRevision: task.evidenceRevision,
                  nativeOperation,
                  result: stopped,
                  observedAt: DateTime.formatIso(yield* DateTime.now),
                };
                const coveredHolds = yield* held;
                if (coveredHolds === null)
                  return yield* unknown(
                    "The provider cleanup lost its original operation hold after dispatch.",
                    binding,
                  );
                const proof = yield* eventSink
                  .recordObservedDeletionCleanupOutcome({
                    effectId: effect.id,
                    bindingSha256,
                    expectedLatestOrdinal: started.ordinal,
                    observation,
                    coveredHolds,
                  })
                  .pipe(Effect.catchCause((cause) => Effect.fail(unknown(cause, binding))));
                if (
                  proof.outcome.taskId !== effect.id ||
                  proof.outcome.result !== "succeeded" ||
                  proof.outcome.effect !== "confirmed" ||
                  proof.bindingSha256 !== bindingSha256 ||
                  proof.ordinal !== started.ordinal + 1 ||
                  proof.evidence.producer !== "managed_provider" ||
                  nativeCreationCanonicalJson(proof.evidence.observation) !==
                    nativeCreationCanonicalJson(observation)
                )
                  return yield* unknown(
                    stopped.status === "unknown"
                      ? stopped.reason
                      : "The managed stop lacks its qualified stored completion.",
                    binding,
                  );
                if (
                  !(yield* eventSink.completeObservedDeletionCleanup({
                    effectId: effect.id,
                    bindingSha256,
                    expectedLatestOrdinal: proof.ordinal,
                  }))
                )
                  return yield* unknown(
                    "The managed provider cleanup could not complete its exact qualified ordinal.",
                    binding,
                  );
                const completion = completedCleanup(effect, cleanup, proof, effect.leaseOwner);
                if (!(yield* completion.revalidate))
                  return yield* unknown(
                    "The provider cleanup completion did not survive its stored readback.",
                    binding,
                  );
                return completion;
              }
              const intent = yield* eventSink
                .readCurrentThreadRuntimeStopIntent({
                  threadId: effect.threadId,
                  commandId: effect.commandId,
                })
                .pipe(Effect.catchCause((cause) => Effect.fail(unknown(cause))));
              if (intent === null) {
                const facts = yield* eventSink
                  .readNativeCommandFacts({
                    threadId: effect.threadId,
                    commandId: effect.commandId,
                  })
                  .pipe(Effect.catchCause((cause) => Effect.fail(unknown(cause))));
                if (
                  facts.eventMetadataOverflow ||
                  facts.events.some(
                    (stored) =>
                      stored.event.type === "provider-session.detach-requested" ||
                      stored.event.type === "thread.deleted",
                  )
                )
                  return yield* unknown(
                    "The accepted stop or deletion has no immutable pinned target task.",
                  );
                return yield* providerSessions.detach({
                  providerSessionId: request.providerSessionId,
                  threadId: effect.threadId,
                  ...(request.detail === undefined ? {} : { detail: request.detail }),
                  ...(request.revokeMcpCredential === undefined
                    ? {}
                    : { revokeMcpCredential: request.revokeMcpCredential }),
                });
              }
              const binding = intent.targetBinding;
              if (
                intent.commandId !== effect.commandId ||
                intent.threadId !== effect.threadId ||
                binding.threadId !== effect.threadId ||
                binding.providerSessionId !== request.providerSessionId ||
                effect.id !==
                  `effect:${intent.commandId}:provider-session.detach:${binding.providerSessionId}` ||
                effect.leaseOwner === null
              )
                return yield* unknown(
                  "The targeted stop differs from its immutable claimed target.",
                  binding,
                );
              const current = yield* outbox
                .get(effect.id)
                .pipe(Effect.catchCause((cause) => Effect.fail(unknown(cause, binding))));
              const now = DateTime.formatIso(yield* DateTime.now);
              if (
                Option.isNone(current) ||
                current.value.commandId !== effect.commandId ||
                current.value.threadId !== effect.threadId ||
                current.value.request.type !== "provider-session.detach" ||
                current.value.request.providerSessionId !== binding.providerSessionId ||
                current.value.status !== "running" ||
                current.value.leaseOwner !== effect.leaseOwner ||
                current.value.attemptCount !== effect.attemptCount ||
                current.value.leaseExpiresAt === null ||
                !Number.isFinite(Date.parse(current.value.leaseExpiresAt)) ||
                Date.parse(current.value.leaseExpiresAt) <= Date.parse(now)
              )
                return yield* unknown(
                  "The targeted stop no longer owns its running claim barrier.",
                  binding,
                );
              const owner = yield* eventSink
                .readCurrentProviderRuntimeOwner(effect.threadId)
                .pipe(Effect.catchCause((cause) => Effect.fail(unknown(cause, binding))));
              if (
                owner === null ||
                owner.evidenceRevision !== intent.targetEvidenceRevision ||
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
                ).some((key) => owner.binding[key] !== binding[key])
              )
                return yield* unknown(
                  "The targeted stop's original native attachment changed.",
                  binding,
                );
              if (binding.runtimeGeneration === null)
                return yield* unknown(
                  "The pinned target has no actual registered incarnation.",
                  binding,
                );
              const stopped = yield* providerSessions.stopPinnedRuntime({
                operationId: effect.id,
                binding: { ...binding, runtimeGeneration: binding.runtimeGeneration },
                expectedEvidenceRevision: intent.targetEvidenceRevision,
              });
              if (
                stopped.status !== "stopped" ||
                stopped.operationId !== effect.id ||
                stopped.readback.threadAttached !== false ||
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
                ).some((key) => stopped.binding[key] !== binding[key])
              )
                return yield* unknown(
                  stopped.status === "unknown"
                    ? stopped.reason
                    : "The provider stop returned a different target.",
                  binding,
                );
              const completed = yield* outbox.get(effect.id);
              const completedAt = DateTime.toEpochMillis(yield* DateTime.now);
              if (
                Option.isNone(completed) ||
                completed.value.status !== "running" ||
                completed.value.leaseOwner !== effect.leaseOwner ||
                completed.value.attemptCount !== effect.attemptCount ||
                completed.value.commandId !== effect.commandId ||
                completed.value.threadId !== effect.threadId ||
                completed.value.request.type !== "provider-session.detach" ||
                completed.value.request.providerSessionId !== binding.providerSessionId ||
                completed.value.leaseExpiresAt === null ||
                !Number.isFinite(Date.parse(completed.value.leaseExpiresAt)) ||
                Date.parse(completed.value.leaseExpiresAt) <= completedAt
              )
                return yield* unknown(
                  "The provider stop lost its claimed completion barrier.",
                  binding,
                );
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestrationEffectExecutionError({
                    effectId: effect.id,
                    effectType: effect.request.type,
                    cause,
                  }),
              ),
            );
          }
          case "provider-turn.start":
            if (effect.nativeCreationExecutionReference !== undefined) {
              const reference = effect.nativeCreationExecutionReference;
              const requestedRunId = effect.request.runId;
              const unknown = (cause: unknown) =>
                new ProviderNativeOperationUnknownError({
                  nativeEffect: {
                    operationId: reference.effectId,
                    operation: "start_turn",
                    threadId: effect.threadId,
                    outcome: "unknown",
                  },
                  cause,
                });
              return Effect.gen(function* () {
                const context = yield* nativeAuthority.issueExecution({
                  reference,
                  timestamp: DateTime.formatIso(yield* DateTime.now),
                });
                const issuedReference =
                  NativeCreationAuthority.getNativeCreationExecutionReference(context);
                if (
                  issuedReference === null ||
                  issuedReference.effectId !== effect.id ||
                  issuedReference.claimId !== reference.claimId ||
                  issuedReference.stageCommandId !== reference.stageCommandId ||
                  issuedReference.stage !== reference.stage ||
                  issuedReference.version !== reference.version
                ) {
                  return yield* unknown(
                    "The issued context differs from the claimed native effect reference.",
                  );
                }
                const confirmation = yield* providerTurnStart.start({
                  threadId: effect.threadId,
                  runId: requestedRunId,
                  willRetry,
                  nativeCreationExecutionContext: context,
                });
                if (confirmation === undefined || confirmation.status !== "confirmed_start") {
                  return yield* unknown(
                    "The native provider start did not produce a complete current binding confirmation.",
                  );
                }
                if (
                  effect.leaseOwner === null ||
                  confirmation.nativeEffect.operationId !== effect.id
                ) {
                  return yield* unknown(
                    "The native start confirmation differs from its claimed operation.",
                  );
                }
                const proof = yield* nativeRepository.recordNativeEffectConfirmation({
                  effectId: effect.id,
                  workerId: effect.leaseOwner,
                  expectedAttempt: effect.attemptCount,
                  runId: requestedRunId,
                  attemptId: confirmation.attemptId,
                  binding: confirmation.binding,
                  expectedEvidenceRevision: confirmation.evidenceRevision,
                  evidence: confirmation.nativeEffect,
                });
                yield* providerSessions
                  .onNativeEffectConfirmed({ context, confirmation: proof })
                  .pipe(
                    Effect.catchCause(() =>
                      Effect.logWarning(
                        "orchestration-v2.native-confirmation.runtime-classification-retained",
                        {
                          effectId: effect.id,
                          attemptCount: effect.attemptCount,
                        },
                      ),
                    ),
                  );
                return proof;
              }).pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause: unknown(cause),
                    }),
                ),
              );
            }
            {
              const requestedRunId = effect.request.runId;
              const reference = {
                commandId: effect.commandId,
                threadId: effect.threadId,
                runId: requestedRunId,
                effectId: effect.id,
              };
              const unknown = (cause: unknown) =>
                new ProviderNativeOperationUnknownError({
                  nativeEffect: {
                    operationId: effect.id,
                    operation: "start_turn",
                    threadId: effect.threadId,
                    outcome: "unknown",
                  },
                  cause,
                });
              return Effect.gen(function* () {
                const choice = yield* eventSink
                  .readImportedHistoryStartChoice({
                    commandId: effect.commandId,
                    threadId: effect.threadId,
                  })
                  .pipe(Effect.mapError(unknown));
                if (choice === null) {
                  const facts = yield* eventSink
                    .readNativeCommandFacts({
                      threadId: effect.threadId,
                      commandId: effect.commandId,
                    })
                    .pipe(Effect.catchCause((cause) => Effect.fail(unknown(cause))));
                  const reservations = yield* Schema.decodeUnknownEffect(
                    Schema.Array(
                      Schema.Struct({
                        effect_id: Schema.NonEmptyString,
                        command_id: CommandId,
                        thread_id: ThreadId,
                        run_id: RunId,
                      }),
                    ),
                  )(facts.commitSnapshot?.records?.start_reservations).pipe(
                    Effect.mapError(unknown),
                  );
                  const matching = reservations.filter((row) => row.effect_id === effect.id);
                  if (matching.length > 0) {
                    if (
                      matching.length !== 1 ||
                      matching[0]!.command_id !== effect.commandId ||
                      matching[0]!.thread_id !== effect.threadId ||
                      matching[0]!.run_id !== requestedRunId ||
                      effect.leaseOwner === null
                    )
                      return yield* unknown(
                        "The queued reservation differs from this claimed effect.",
                      );
                    const queuedRunStartExecution = {
                      effectId: effect.id,
                      commandId: effect.commandId,
                      threadId: effect.threadId,
                      runId: requestedRunId,
                      workerId: effect.leaseOwner,
                      expectedAttempt: effect.attemptCount,
                    };
                    yield* readClaimedQueuedRunStartExecution(queuedRunStartExecution, eventSink);
                    const ordinary = yield* prepareOrdinaryClaim(effect);
                    const retry = yield* startOrdinaryClaim(
                      {
                        threadId: effect.threadId,
                        runId: requestedRunId,
                        willRetry,
                        ...ordinaryStartInput(ordinary),
                        queuedRunStartExecution,
                      },
                      ordinary,
                    );
                    return (
                      retry ??
                      (ordinary === undefined ? undefined : settledOrdinaryClaim(effect, ordinary))
                    );
                  }
                  const records = facts.commitSnapshot.records;
                  if (
                    ![
                      "effects",
                      "unknown_effect_holds",
                      "stop_intents",
                      "stop_fences",
                      "imported_choices",
                    ].every((key) => Array.isArray(records[key])) ||
                    records.unknown_effect_holds!.length !== 0 ||
                    records.imported_choices!.some((row) => row.command_id === effect.commandId)
                  )
                    return yield* unknown(
                      "The unreserved start has incomplete or contradictory current execution facts.",
                    );
                  const incarnation = yield* eventSink
                    .readApplicationBirthRecord(effect.threadId)
                    .pipe(Effect.mapError(unknown));
                  if (
                    incarnation === null ||
                    !hasOrdinaryImmediateLineage(facts, effect, incarnation)
                  )
                    return yield* unknown(
                      "The unreserved start lacks complete ordinary immediate command lineage.",
                    );
                  const claims = records.effects!.filter((row) => row.effect_id === effect.id);
                  const now = DateTime.formatIso(yield* DateTime.now);
                  const claim = claims[0];
                  if (
                    claims.length !== 1 ||
                    effect.leaseOwner === null ||
                    claim?.command_id !== effect.commandId ||
                    claim.thread_id !== effect.threadId ||
                    claim.effect_type !== "provider-turn.start" ||
                    claim.status !== "running" ||
                    claim.lease_owner !== effect.leaseOwner ||
                    claim.attempt_count !== effect.attemptCount ||
                    typeof claim.payload_json !== "string" ||
                    typeof claim.lease_expires_at !== "string" ||
                    !Number.isFinite(Date.parse(claim.lease_expires_at)) ||
                    Date.parse(claim.lease_expires_at) <= Date.parse(now)
                  )
                    return yield* unknown(
                      "The unreserved start no longer owns its current running claim.",
                    );
                  const payload = yield* EffectOutbox.decodeOrchestrationEffectPayloadV2(
                    claim.payload_json,
                  ).pipe(Effect.mapError(unknown));
                  if (
                    "nativeCreationExecutionReference" in payload ||
                    payload.request.type !== "provider-turn.start" ||
                    payload.request.runId !== requestedRunId
                  )
                    return yield* unknown(
                      "The unreserved claim differs from its actual ordinary start payload.",
                    );
                  if (
                    (yield* eventSink
                      .readQueuedRunRuntimeStopFences({
                        threadId: effect.threadId,
                        runId: requestedRunId,
                        incarnation,
                      })
                      .pipe(Effect.mapError(unknown))).length !== 0
                  )
                    return yield* unknown(
                      "The unreserved start is fenced by its original stopped execution intent.",
                    );
                  for (const row of records.stop_intents!) {
                    const stopCommandId = yield* Schema.decodeUnknownEffect(CommandId)(
                      row.command_id,
                    ).pipe(Effect.mapError(unknown));
                    const intent = yield* eventSink
                      .readCurrentThreadRuntimeStopIntent({
                        threadId: effect.threadId,
                        commandId: stopCommandId,
                      })
                      .pipe(Effect.mapError(unknown));
                    if (intent === null)
                      return yield* unknown("A current stop record has no immutable intent.");
                    if (
                      intent.incarnation.eventId === incarnation.eventId &&
                      intent.incarnation.sequence === incarnation.sequence &&
                      (intent.affectedRunIds.includes(requestedRunId) ||
                        intent.queuedBases.some((basis) => basis.runId === requestedRunId))
                    )
                      return yield* unknown(
                        "The ordinary start belongs to the exact stopped source cohort.",
                      );
                  }
                  const ordinary = yield* prepareOrdinaryClaim(effect);
                  const retry = yield* startOrdinaryClaim(
                    {
                      threadId: effect.threadId,
                      runId: requestedRunId,
                      willRetry,
                      ...ordinaryStartInput(ordinary),
                    },
                    ordinary,
                  );
                  return (
                    retry ??
                    (ordinary === undefined ? undefined : settledOrdinaryClaim(effect, ordinary))
                  );
                }
                if (
                  choice.receipt.status !== "accepted" ||
                  choice.runId !== requestedRunId ||
                  choice.commandId !== effect.commandId ||
                  choice.threadId !== effect.threadId ||
                  (choice.effectId !== null && choice.effectId !== effect.id) ||
                  effect.leaseOwner === null ||
                  importedHistoryPreparation === undefined
                )
                  return yield* unknown(
                    "The accepted imported choice has no current claimed preparation facade.",
                  );
                return yield* Effect.gen(function* () {
                  const preparation = yield* importedHistoryPreparation.prepare({
                    reference,
                    workerId: effect.leaseOwner!,
                    expectedAttempt: effect.attemptCount,
                    prepare: (currentChoice) =>
                      providerTurnStart.prepareImportedHistoryStart({
                        reference,
                        choice: currentChoice,
                      }),
                  });
                  if (
                    preparation.status === "rejected" &&
                    preparation.reason === "queued_delivery_not_first" &&
                    choice.command.delivery.type === "queued_run" &&
                    choice.command.delivery.runId === requestedRunId &&
                    choice.effectId === effect.id
                  ) {
                    const basis = yield* Schema.decodeUnknownEffect(
                      Schema.Struct({
                        records: Schema.Struct({
                          run_attempts: Schema.Array(
                            Schema.Struct({ run_id: RunId, attempt_id: RunAttemptId }),
                          ),
                        }),
                      }),
                    )(choice.basis.snapshot);
                    const originalAttempts = basis.records.run_attempts.filter(
                      (row) => row.run_id === requestedRunId,
                    );
                    if (originalAttempts.length !== 1)
                      return yield* unknown(
                        "The waiting imported choice has no unique original run attempt.",
                      );
                    const parkInput = {
                      effectId: effect.id,
                      commandId: effect.commandId,
                      threadId: effect.threadId,
                      runId: requestedRunId,
                      runAttemptId: originalAttempts[0]!.attempt_id,
                      workerId: effect.leaseOwner!,
                      expectedAttempt: effect.attemptCount,
                    };
                    let parked = yield* Effect.exit(outbox.parkImportedHistoryDelivery(parkInput));
                    if (Exit.isFailure(parked)) {
                      const persisted = yield* outbox.get(effect.id);
                      if (
                        Option.isNone(persisted) ||
                        !matchesParkedDeliveryState(effect, persisted.value)
                      )
                        return yield* Effect.failCause(parked.cause);
                      // A persisted wait permits desired-state validation; it does not permit a new claim or native dispatch.
                      parked = yield* Effect.exit(outbox.parkImportedHistoryDelivery(parkInput));
                    }
                    if (Exit.isFailure(parked)) return yield* Effect.failCause(parked.cause);
                    if (
                      parked.value.status !== "parked" ||
                      parked.value.effectId !== effect.id ||
                      parked.value.commandId !== effect.commandId ||
                      parked.value.threadId !== effect.threadId ||
                      parked.value.runId !== requestedRunId ||
                      parked.value.runAttemptId !== parkInput.runAttemptId ||
                      parked.value.commandDigest !== choice.commandDigest ||
                      parked.value.schedulingAttempt !== effect.attemptCount - 1
                    )
                      return yield* unknown(
                        "The imported delivery wait lacks its exact durable scheduling proof.",
                      );
                    const current = yield* outbox.get(effect.id);
                    if (
                      Option.isNone(current) ||
                      !matchesParkedDeliveryState(effect, current.value)
                    )
                      return yield* unknown(
                        "The imported delivery wait changed after its scheduling commit.",
                      );
                    return parked.value;
                  }
                  if (
                    preparation.status !== "prepared" ||
                    preparation.executionIntent.kind !== "imported_history_choice"
                  )
                    return yield* unknown(
                      preparation.status === "rejected"
                        ? preparation.reason
                        : "An already prepared imported choice is observation-only.",
                    );
                  const confirmation = yield* providerTurnStart.start({
                    threadId: effect.threadId,
                    runId: requestedRunId,
                    willRetry,
                    ...ordinaryStartInput(yield* prepareOrdinaryClaim(effect)),
                    importedHistoryStartExecution: {
                      reference,
                      executionIntent: preparation.executionIntent,
                      workerId: effect.leaseOwner!,
                      expectedAttempt: effect.attemptCount,
                    },
                  });
                  if (
                    confirmation === undefined ||
                    confirmation.status !== "confirmed_start" ||
                    confirmation.nativeEffect.operationId !== effect.id
                  )
                    return yield* unknown(
                      "The imported choice provider start has no complete current ACK confirmation.",
                    );
                  const proof = yield* nativeRepository.recordNativeEffectConfirmation({
                    effectId: effect.id,
                    workerId: effect.leaseOwner!,
                    expectedAttempt: effect.attemptCount,
                    runId: requestedRunId,
                    attemptId: confirmation.attemptId,
                    binding: confirmation.binding,
                    expectedEvidenceRevision: confirmation.evidenceRevision,
                    evidence: confirmation.nativeEffect,
                  });
                  if (proof.nativeExecutionReference !== null)
                    return yield* unknown(
                      "The application choice confirmation unexpectedly contains native authority lineage.",
                    );
                  return proof;
                }).pipe(Effect.mapError(unknown));
              }).pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
            }
          case "provider-turn.interrupt":
            return providerTurnControl
              .interrupt({
                threadId: effect.threadId,
                providerSessionId: effect.request.providerSessionId,
                providerThreadId: effect.request.providerThreadId,
                providerTurnId: effect.request.providerTurnId,
              })
              .pipe(
                // The provider has stopped what it still ran and reported it.
                // Whatever the thread still shows on that provider thread is
                // work no process will report on, so the Stop ends it too.
                Effect.andThen(
                  threads.dispatch({
                    type: "thread.background-work.settle",
                    commandId: CommandId.make(`${effect.commandId}:background-work-settled`),
                    threadId: effect.threadId,
                    providerThreadId: effect.request.providerThreadId,
                    providerTurnId: effect.request.providerTurnId,
                  }),
                ),
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "provider-turn.steer":
            return providerTurnControl
              .steer({
                threadId: effect.threadId,
                providerSessionId: effect.request.providerSessionId,
                providerThreadId: effect.request.providerThreadId,
                providerTurnId: effect.request.providerTurnId,
                messageId: effect.request.messageId,
              })
              .pipe(
                Effect.tap(() =>
                  Effect.gen(function* () {
                    if (effect.request.type !== "provider-turn.steer") return;
                    const messageId = effect.request.messageId;
                    const projection = yield* threads.getThreadRecords(
                      effect.threadId,
                      ["messages", "runs"],
                      { messageIds: [effect.request.messageId] },
                    );
                    const message = projection.messages.find((row) => row.id === messageId);
                    if (message?.delegatedCompletion === undefined) return;
                    yield* threads.dispatch({
                      type: "notification.delivery.accept",
                      commandId: CommandId.make(`command:mailbox-accepted:${effect.id}`),
                      threadId: effect.threadId,
                      messageId: message.id,
                    });
                  }),
                ),
                Effect.catch((error) =>
                  Effect.gen(function* () {
                    if (
                      !("turnCompleted" in error) ||
                      !error.turnCompleted ||
                      effect.request.type !== "provider-turn.steer"
                    ) {
                      return yield* error;
                    }
                    const projection = yield* threads.getThreadRecords(
                      effect.threadId,
                      ["messages", "runs"],
                      { messageIds: [effect.request.messageId] },
                    );
                    const messageId = effect.request.messageId;
                    const message = projection.messages.find((item) => item.id === messageId);
                    const run = projection.runs.find((item) => item.id === message?.runId);
                    if (message === undefined || run === undefined) return yield* error;
                    // Reuse the message identity and a stable command receipt so an outbox
                    // retry cannot append a duplicate message or start a second follow-up.
                    yield* threads.dispatch({
                      type: "message.dispatch",
                      commandId: CommandId.make(`command:steer-follow-up:${effect.id}`),
                      threadId: effect.threadId,
                      messageId: message.id,
                      text: message.text,
                      ...(message.context ? { context: message.context } : {}),
                      attachments: message.attachments,
                      modelSelection: run.modelSelection,
                      dispatchMode: {
                        type:
                          message.delegatedCompletion === undefined
                            ? "start_immediately"
                            : "queue_after_active",
                      },
                      createdBy: message.createdBy,
                      creationSource: message.creationSource,
                      ...(message.delegatedCompletion === undefined
                        ? {}
                        : { delegatedCompletion: message.delegatedCompletion }),
                      ...(message.notification === undefined
                        ? {}
                        : { notification: message.notification }),
                      ...(message.scheduledTaskId === undefined
                        ? {}
                        : { scheduledTaskId: message.scheduledTaskId }),
                      ...(message.senderThreadId === undefined
                        ? {}
                        : { senderThreadId: message.senderThreadId }),
                    });
                  }),
                ),
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "provider-turn.restart":
            return providerTurnControl
              .interruptAndAwaitTerminal({
                threadId: effect.threadId,
                ordinaryCheckoutRestartRequest: effect.request,
                providerSessionId: effect.request.providerSessionId,
                providerThreadId: effect.request.providerThreadId,
                providerTurnId: effect.request.providerTurnId,
                interruptedAttemptId: effect.request.interruptedAttemptId,
                ...(effect.request.sessionTransition?.type === "replace"
                  ? {
                      replacementProviderSessionId:
                        effect.request.sessionTransition.replacementProviderSessionId,
                    }
                  : {}),
              })
              .pipe(
                Effect.andThen(
                  effect.request.sessionTransition?.type === "replace"
                    ? providerSessions.detach({
                        providerSessionId: effect.request.providerSessionId,
                        threadId: effect.threadId,
                        detail: "Selection change requires a provider session restart.",
                      })
                    : effect.request.sessionTransition?.type === "detach"
                      ? providerSessions.detach({
                          providerSessionId: effect.request.providerSessionId,
                          threadId: effect.threadId,
                          detail: "Provider thread handoff replaced this session binding.",
                        })
                      : Effect.void,
                ),
                Effect.andThen(
                  providerTurnStart
                    .start({
                      threadId: effect.threadId,
                      runId: effect.request.runId,
                      willRetry,
                    })
                    .pipe(Effect.asVoid),
                ),
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "runtime-request.respond":
            return runtimeRequests
              .respond({
                threadId: effect.threadId,
                providerSessionId: effect.request.providerSessionId,
                requestId: effect.request.requestId,
                ...(effect.request.decision === undefined
                  ? {}
                  : { decision: effect.request.decision }),
                ...(effect.request.answers === undefined
                  ? {}
                  : { answers: effect.request.answers }),
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "provider-thread.rollback":
            return checkpointRollback
              .execute({
                threadId: effect.threadId,
                providerThreadId: effect.request.providerThreadId,
                checkpointId: effect.request.checkpointId,
                scopeId: effect.request.scopeId,
                sourceEffect: { effectId: effect.id, commandId: effect.commandId },
                ...(effect.request.restoreFiles === undefined
                  ? {}
                  : { restoreFiles: effect.request.restoreFiles }),
              })
              .pipe(
                // The last failed attempt tells waiting clients it failed,
                // instead of leaving them to time out. Clients get a fixed
                // message; the worker logs the full cause for each attempt.
                Effect.tapCause((cause) =>
                  willRetry || Cause.hasInterruptsOnly(cause)
                    ? Effect.void
                    : threads
                        .dispatch({
                          type: "checkpoint.rollback.fail",
                          commandId: CommandId.make(`${effect.commandId}:rollback-failed`),
                          threadId: effect.threadId,
                          requestId: effect.commandId,
                          message: CheckpointRollbackService.ROLLBACK_FAILED_MESSAGE,
                        })
                        .pipe(
                          Effect.catchCause((recordCause) =>
                            Effect.logWarning("Failed to record rollback failure", {
                              effectId: effect.id,
                              cause: recordCause,
                            }),
                          ),
                        ),
                ),
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "checkpoint.capture": {
            const request = effect.request;
            return Effect.gen(function* () {
              const ref = yield* prepareOrdinaryClaim(effect);
              const finalizeAndSettle = Effect.gen(function* () {
                const result = yield* Effect.exit(
                  runFinalization.finalize({
                    threadId: effect.threadId,
                    runId: request.runId,
                    scopeId: request.scopeId,
                    ...(ref === undefined
                      ? {}
                      : { ordinaryCheckoutUse: ref.originalUse, ordinaryCheckoutExecution: ref }),
                  }),
                );
                if (ref === undefined) {
                  if (Exit.isFailure(result)) return yield* Effect.failCause(result.cause);
                  return;
                }
                const observation = Exit.isSuccess(result)
                  ? CheckpointCaptureService.readIssuedCheckpointCaptureObservation(result.value)
                  : CheckpointCaptureService.readIssuedCheckpointCaptureObservationForExecution(
                      ref,
                    );
                if (observation === null) {
                  if (Exit.isFailure(result)) return yield* Effect.failCause(result.cause);
                  return yield* new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
                    reason: "unknown_use",
                    threadId: effect.threadId,
                    path: ref.originalUse.lease.resourcePath,
                    message: "The claimed checkpoint has no original issued physical result.",
                  });
                }
                const revalidateProducer = Effect.suspend(() =>
                  CheckpointCaptureService.readIssuedCheckpointCaptureObservation(observation) ===
                  observation
                    ? Effect.void
                    : Effect.fail(
                        new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
                          reason: "unknown_use",
                          threadId: effect.threadId,
                          path: ref.originalUse.lease.resourcePath,
                          message:
                            "The original physical checkpoint result changed before publication.",
                        }),
                      ),
                );
                yield* eventSink.withTransaction(
                  Effect.gen(function* () {
                    const actualProducerOutcome = {
                      kind: "checkpoint_captured" as const,
                      observation,
                    };
                    yield* eventSink.recordOrdinaryCheckoutExecutorOutcome({
                      ref,
                      actualProducerOutcome,
                      revalidateProducer,
                    });
                    const history = yield* eventSink.readOrdinaryCheckoutExecutionAssociations(
                      ref.originalUse,
                    );
                    for (const participant of history.participants) {
                      if (
                        participant.state !== "active" ||
                        participant.ref.executor.kind !== "captured_managed_run"
                      )
                        continue;
                      yield* eventSink.recordOrdinaryCheckoutExecutorOutcome({
                        ref: participant.ref,
                        actualProducerOutcome,
                        revalidateProducer,
                      });
                    }
                    if (
                      !(yield* outbox.succeed({
                        effectId: effect.id,
                        workerId: effect.leaseOwner!,
                      }))
                    )
                      return yield* new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
                        reason: "claim_mismatch",
                        threadId: effect.threadId,
                        path: ref.originalUse.lease.resourcePath,
                        message:
                          "The checkpoint claim changed before its actual result was settled.",
                      });
                    const completed = yield* eventSink.readOrdinaryCheckoutExecutionAssociations(
                      ref.originalUse,
                    );
                    if (completed.participants.every((item) => item.state === "retired"))
                      yield* eventSink.completeOrdinaryCheckoutUse({
                        originalUse: ref.originalUse,
                        expectedAssociationOrdinal: completed.latestOrdinal,
                        completionEvidence: { ref, actualProducerOutcome },
                      });
                  }),
                );
                return settledOrdinaryClaim(effect, ref);
              });
              // Terminal publication can wake queued admission before checkout retirement.
              // Hold the same thread barrier through the committed retirement result.
              return yield* ref === undefined
                ? finalizeAndSettle
                : threadDispatch.withLock(effect.threadId, finalizeAndSettle);
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestrationEffectExecutionError({
                    effectId: effect.id,
                    effectType: effect.request.type,
                    cause,
                  }),
              ),
            );
          }
          case "terminal.cleanup":
            return executeResourceCleanup(effect);
          case "attachment.cleanup":
            return effect.attachmentNamespaceCleanup === undefined
              ? executeResourceCleanup(effect)
              : executeAttachmentNamespaceCleanup(effect);
          case "worktree.cleanup":
            return executeWorktreeCleanup(effect);
          case "thread-title.generate":
            return threadTitleRegeneration
              .execute({
                threadId: effect.threadId,
                requestId: effect.commandId,
                kind: effect.request.kind,
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
        }
      },
    });
  }),
).pipe(Layer.provide(threadCommandExecutorLayer));

export class OrchestrationEffectWorkerError extends Schema.TaggedError<OrchestrationEffectWorkerError>()(
  "OrchestrationEffectWorkerError",
  {
    operation: Schema.String,
    effectId: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

const isOrchestrationEffectWorkerError = Schema.is(OrchestrationEffectWorkerError);

export interface OrchestrationEffectWorkerV2Shape {
  readonly awaitWork: Effect.Effect<void>;
  readonly runOnce: Effect.Effect<boolean, OrchestrationEffectWorkerError>;
  readonly runRecoveryOnce: Effect.Effect<boolean, OrchestrationEffectWorkerError>;
  readonly nextClaimableAt: Effect.Effect<
    Option.Option<DateTime.Utc>,
    OrchestrationEffectWorkerError
  >;
  readonly drain: (maxEffects?: number) => Effect.Effect<number, OrchestrationEffectWorkerError>;
}

export class OrchestrationEffectWorkerV2 extends Context.Service<
  OrchestrationEffectWorkerV2,
  OrchestrationEffectWorkerV2Shape
>()("t3/orchestration-v2/EffectWorker/OrchestrationEffectWorkerV2") {}

export interface OrchestrationEffectWorkerOptions {
  readonly workerId?: string;
  readonly leaseDurationMs?: number;
  readonly maxAttempts?: number;
}

export const layerWithOptions = (
  options: OrchestrationEffectWorkerOptions = {},
): Layer.Layer<
  OrchestrationEffectWorkerV2,
  never,
  EffectOutbox.EffectOutboxV2 | OrchestrationEffectExecutorV2
> =>
  Layer.effect(
    OrchestrationEffectWorkerV2,
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const executor = yield* OrchestrationEffectExecutorV2;
      const nativeRepository = yield* Effect.serviceOption(NativeCreationRepository);
      const workerId = options.workerId ?? `orchestration-v2:${process.pid}`;
      const leaseDurationMs = Math.max(1, options.leaseDurationMs ?? 30_000);
      const maxAttempts = Math.max(1, options.maxAttempts ?? 5);
      const readCommittedConfirmation = (
        effect: EffectOutbox.OrchestrationEffectV2,
        returned?: NativeEffectConfirmationV1,
      ) =>
        Effect.gen(function* () {
          if (effect.request.type !== "provider-turn.start" || Option.isNone(nativeRepository))
            return false;
          const proof = yield* nativeRepository.value.readNativeEffectConfirmation(effect.id);
          if (
            proof === null ||
            (returned !== undefined &&
              (yield* comparisonJson(returned)) !== (yield* comparisonJson(proof)))
          )
            return false;
          const evidence = proof.evidence;
          const reference = effect.nativeCreationExecutionReference;
          if (
            proof.effectId !== effect.id ||
            proof.commandId !== effect.commandId ||
            proof.threadId !== effect.threadId ||
            proof.workerId !== workerId ||
            proof.expectedAttempt !== effect.attemptCount ||
            proof.runId !== effect.request.runId ||
            proof.binding.threadId !== effect.threadId ||
            evidence.outcome !== "confirmed_success" ||
            evidence.operationId !== effect.id ||
            (evidence.operation !== "start_turn" && evidence.operation !== "compact_thread") ||
            evidence.attemptId !== proof.attemptId ||
            (
              [
                "threadId",
                "providerThreadId",
                "providerSessionId",
                "instanceId",
                "runtimeGeneration",
              ] as const
            ).some((key) => evidence[key] !== proof.binding[key]) ||
            (reference === undefined
              ? proof.nativeExecutionReference !== null
              : proof.nativeExecutionReference === null ||
                (["version", "claimId", "stageCommandId", "effectId", "stage"] as const).some(
                  (key) => reference[key] !== proof.nativeExecutionReference![key],
                ))
          )
            return false;
          const current = yield* outbox.get(effect.id);
          return (
            Option.isSome(current) &&
            current.value.status === "succeeded" &&
            current.value.commandId === effect.commandId &&
            current.value.threadId === effect.threadId &&
            current.value.attemptCount === effect.attemptCount &&
            current.value.request.type === "provider-turn.start" &&
            current.value.request.runId === proof.runId &&
            (reference === undefined
              ? current.value.nativeCreationExecutionReference === undefined
              : current.value.nativeCreationExecutionReference !== undefined &&
                (["version", "claimId", "stageCommandId", "effectId", "stage"] as const).every(
                  (key) => reference[key] === current.value.nativeCreationExecutionReference![key],
                ))
          );
        }).pipe(Effect.orElseSucceed(() => false));
      const wasCancelled = (effectId: string) =>
        outbox.get(effectId).pipe(
          Effect.map(
            Option.match({
              onNone: () => false,
              onSome: (effect) => effect.status === "cancelled",
            }),
          ),
        );
      const requeueClaim = (
        effect: EffectOutbox.OrchestrationEffectV2,
        cause: Cause.Cause<unknown>,
      ) =>
        Cause.hasInterruptsOnly(cause)
          ? Effect.void
          : outbox
              .retry({
                effectId: effect.id,
                workerId,
                error: `Worker failed before settling the claimed effect: ${Cause.pretty(cause)}`,
                delayMs: 0,
              })
              .pipe(
                Effect.flatMap((requeued) =>
                  requeued
                    ? Effect.logWarning("Requeued effect after unexpected worker failure", {
                        effectId: effect.id,
                        effectType: effect.request.type,
                      })
                    : Effect.logWarning("Could not requeue effect after worker lost its lease", {
                        effectId: effect.id,
                        effectType: effect.request.type,
                      }),
                ),
                Effect.catchCause((requeueCause) =>
                  Effect.logError("Failed to requeue effect after unexpected worker failure", {
                    effectId: effect.id,
                    effectType: effect.request.type,
                    error: Cause.pretty(requeueCause),
                  }),
                ),
              );
      const terminalizeClaim = (
        effect: EffectOutbox.OrchestrationEffectV2,
        cause: Cause.Cause<unknown>,
      ) => {
        if (Cause.hasInterruptsOnly(cause)) return Effect.void;
        return outbox
          .fail({
            effectId: effect.id,
            workerId,
            error: `Worker failed to settle a process-bound effect after execution started: ${Cause.pretty(cause)}`,
          })
          .pipe(
            Effect.flatMap((failed) =>
              failed
                ? Effect.logError("Terminalized process-bound effect after settlement failure", {
                    effectId: effect.id,
                    effectType: effect.request.type,
                  })
                : Effect.logWarning(
                    "Could not terminalize process-bound effect after worker lost its lease",
                    {
                      effectId: effect.id,
                      effectType: effect.request.type,
                    },
                  ),
            ),
            Effect.catchCause((failCause) =>
              Effect.logError(
                "Failed to terminalize process-bound effect after settlement failure",
                {
                  effectId: effect.id,
                  effectType: effect.request.type,
                  error: Cause.pretty(failCause),
                },
              ),
            ),
          );
      };
      const recoverPostSuccessSettlement = (
        effect: EffectOutbox.OrchestrationEffectV2,
        cause: Cause.Cause<unknown>,
      ) =>
        EffectOutbox.REPLAY_SAFE_EFFECT_TYPES_AFTER_PROCESS_LOSS.some(
          (effectType) => effectType === effect.request.type,
        )
          ? requeueClaim(effect, cause)
          : terminalizeClaim(effect, cause);

      const runOnce = (excludeRestartContinuations = false) =>
        Effect.gen(function* () {
          const claimExit = yield* Effect.exit(
            Effect.gen(function* () {
              const ordinary = yield* outbox.claimNext({
                workerId,
                leaseDurationMs,
                excludeRestartContinuations,
              });
              if (
                Option.isSome(ordinary) ||
                executor.claimAttachmentNamespaceCleanupRetry === undefined
              )
                return ordinary;
              const candidates = yield* outbox.listAttachmentNamespaceCleanupRetryCandidates({
                limit: 16,
              });
              for (const candidate of candidates.slice(0, 16)) {
                const claimed = yield* executor.claimAttachmentNamespaceCleanupRetry({
                  effectId: candidate.effectId,
                  workerId,
                  leaseDurationMs,
                  expectedBindingSha256: candidate.bindingSha256,
                  expectedObservationOrdinal: candidate.observationOrdinal,
                });
                if (claimed === null) continue;
                const now = DateTime.toEpochMillis(yield* DateTime.now);
                if (
                  claimed.id !== candidate.effectId ||
                  claimed.status !== "running" ||
                  claimed.leaseOwner !== workerId ||
                  claimed.request.type !== "attachment.cleanup" ||
                  claimed.attachmentNamespaceCleanup === undefined ||
                  claimed.nativeCreationExecutionReference !== undefined ||
                  claimed.leaseExpiresAt === null ||
                  !Number.isFinite(Date.parse(claimed.leaseExpiresAt)) ||
                  Date.parse(claimed.leaseExpiresAt) <= now
                )
                  return yield* new OrchestrationEffectWorkerError({
                    operation: "claim-attachment-namespace-retry",
                    effectId: candidate.effectId,
                    cause:
                      "The qualified namespace claim differs from its actual candidate or owned lease.",
                  });
                return Option.some(claimed);
              }
              return Option.none<EffectOutbox.OrchestrationEffectV2>();
            }),
          );
          yield* increment(orchestrationEffectClaimsTotal, {
            result: Exit.isFailure(claimExit)
              ? "error"
              : Option.isNone(claimExit.value)
                ? "empty"
                : "claimed",
          });
          if (Exit.isFailure(claimExit)) return yield* Effect.failCause(claimExit.cause);
          const claimed = claimExit.value;
          if (Option.isNone(claimed)) {
            return false;
          }
          const effect = claimed.value;
          // Arm the process-local cancellation signal before re-reading durable
          // state. A cancellation that commits after the row read has begun can
          // then still win the execution race instead of falling into the gap
          // between the read and signal registration.
          const cancellation = outbox
            .awaitCancellation(effect.id)
            .pipe(Effect.as("cancelled" as const));
          const cancelledBeforeExecution = yield* Effect.gen(function* () {
            const claimedAt = DateTime.toEpochMillis(yield* DateTime.now);
            const eligibleAt = Math.max(
              DateTime.toEpochMillis(DateTime.makeUnsafe(effect.createdAt)),
              DateTime.toEpochMillis(DateTime.makeUnsafe(effect.availableAt)),
            );
            yield* Metric.update(
              Metric.withAttributes(
                orchestrationEffectQueueWait,
                metricAttributes({ effect_type: effect.request.type }),
              ),
              Duration.millis(Math.max(0, claimedAt - eligibleAt)),
            );
            // Cancellation can commit after the durable claim but before the
            // process-local Deferred is registered. Re-read the authoritative row
            // once before starting external work; later cancellations use the
            // Deferred raced below.
            if (yield* wasCancelled(effect.id)) {
              yield* outbox.clearCancellation(effect.id);
              return true;
            }
            return false;
          }).pipe(Effect.onError((cause) => requeueClaim(effect, cause)));
          if (cancelledBeforeExecution) return true;

          const execution = executor
            .execute(effect, { willRetry: effect.attemptCount < maxAttempts })
            .pipe(Effect.map((confirmation) => ({ type: "executed" as const, confirmation })));
          const exit = yield* Effect.exit(Effect.raceFirst(execution, cancellation)).pipe(
            Effect.ensuring(outbox.clearCancellation(effect.id)),
          );
          if (Exit.isSuccess(exit) && exit.value === "cancelled") {
            return true;
          }
          const returned =
            Exit.isSuccess(exit) && exit.value !== "cancelled"
              ? (exit.value.confirmation ?? undefined)
              : undefined;
          if (isOrdinaryStartRetry(returned)) {
            if (
              effect.request.type !== "provider-turn.start" ||
              effect.attemptCount >= maxAttempts ||
              returned.effectId !== effect.id ||
              returned.commandId !== effect.commandId ||
              returned.threadId !== effect.threadId ||
              returned.workerId !== workerId ||
              returned.expectedAttempt !== effect.attemptCount
            )
              return yield* new OrchestrationEffectWorkerError({
                operation: "verify-ordinary-retry",
                effectId: effect.id,
                cause: "Retry does not belong to this original claim and remaining retry budget.",
              });
            yield* returned.settle(
              Math.min(30_000, 100 * 2 ** Math.max(0, effect.attemptCount - 1)),
            );
            return true;
          }
          if (isSettledOrdinaryClaim(returned)) {
            if (
              returned.effectId === effect.id &&
              returned.commandId === effect.commandId &&
              returned.threadId === effect.threadId &&
              returned.workerId === workerId &&
              returned.expectedAttempt === effect.attemptCount &&
              (yield* returned.revalidate.pipe(Effect.orElseSucceed(() => false)))
            )
              return true;
            return yield* new OrchestrationEffectWorkerError({
              operation: "verify-ordinary-claim-settlement",
              effectId: effect.id,
              cause:
                "The worker could not verify its original committed ordinary claim settlement.",
            });
          }
          if (isDeletionCleanupCompleted(returned)) {
            if (
              returned.effectId === effect.id &&
              returned.commandId === effect.commandId &&
              returned.threadId === effect.threadId &&
              returned.workerId === workerId &&
              returned.expectedAttempt === effect.attemptCount &&
              (yield* returned.revalidate.pipe(Effect.orElseSucceed(() => false)))
            )
              return true;
            return yield* new OrchestrationEffectWorkerError({
              operation: "verify-deletion-cleanup-completion",
              effectId: effect.id,
              cause: "The worker could not verify the exact qualified deletion cleanup completion.",
            });
          }
          if (
            isAttachmentNamespaceRetained(returned) ||
            effect.attachmentNamespaceCleanup !== undefined
          ) {
            if (
              effect.request.type === "attachment.cleanup" &&
              effect.attachmentNamespaceCleanup !== undefined &&
              isAttachmentNamespaceRetained(returned) &&
              returned.effectId === effect.id &&
              returned.commandId === effect.commandId &&
              returned.threadId === effect.threadId &&
              effect.leaseOwner === workerId &&
              returned.workerId === workerId &&
              returned.expectedAttempt === effect.attemptCount &&
              (yield* returned.revalidate.pipe(Effect.orElseSucceed(() => false)))
            )
              return true;
            return yield* new OrchestrationEffectWorkerError({
              operation: "verify-attachment-namespace-retention",
              effectId: effect.id,
              cause:
                "The worker could not verify the exact namespace completion or retained observation.",
            });
          }
          const resourceEvidence = isResourceCleanupHeld(returned)
            ? returned.evidence
            : Exit.isFailure(exit)
              ? resourceCleanupEvidenceFromCause(exit.cause)
              : undefined;
          if (resourceEvidence !== undefined) {
            if (
              effect.leaseOwner !== workerId ||
              (effect.request.type !== "terminal.cleanup" &&
                effect.request.type !== "attachment.cleanup" &&
                effect.request.type !== "worktree.cleanup")
            )
              return yield* new OrchestrationEffectWorkerError({
                operation: "hold-resource-cleanup-unknown",
                effectId: effect.id,
                cause: "The resource cleanup result differs from this worker's claimed task.",
              });
            if (!isResourceCleanupHeld(returned)) {
              yield* Effect.exit(
                outbox.holdResourceCleanupUnknown({
                  effectId: effect.id,
                  workerId,
                  expectedAttempt: effect.attemptCount,
                  evidence: resourceEvidence,
                }),
              );
            }
            if (
              yield* readResourceHold(outbox, effect, resourceEvidence).pipe(
                Effect.orElseSucceed(() => false),
              )
            )
              return true;
            return yield* new OrchestrationEffectWorkerError({
              operation: "hold-resource-cleanup-unknown",
              effectId: effect.id,
              cause: "The worker could not verify the exact persisted resource cleanup hold.",
            });
          }
          if (isParkedDelivery(returned)) {
            const current = yield* outbox.get(effect.id);
            if (
              effect.request.type === "provider-turn.start" &&
              returned.effectId === effect.id &&
              returned.commandId === effect.commandId &&
              returned.threadId === effect.threadId &&
              returned.runId === effect.request.runId &&
              returned.schedulingAttempt === effect.attemptCount - 1 &&
              returned.commandDigest.trim().length > 0 &&
              Option.isSome(current) &&
              matchesParkedDeliveryState(effect, current.value)
            )
              return true;
          }
          const requiresConfirmation =
            effect.nativeCreationExecutionReference !== undefined ||
            (Exit.isSuccess(exit) &&
              exit.value !== "cancelled" &&
              exit.value.confirmation !== undefined);
          if (
            (requiresConfirmation || effect.request.type === "provider-turn.start") &&
            (yield* readCommittedConfirmation(
              effect,
              isParkedDelivery(returned) || isResourceCleanupHeld(returned) ? undefined : returned,
            ))
          )
            return true;
          if (Exit.isSuccess(exit) && !requiresConfirmation) {
            return yield* Effect.gen(function* () {
              const completed = yield* outbox.succeed({ effectId: effect.id, workerId });
              if (!completed) {
                if (yield* wasCancelled(effect.id)) return true;
                return yield* new OrchestrationEffectWorkerError({
                  operation: "complete",
                  effectId: effect.id,
                  cause: "The worker no longer owns the effect lease.",
                });
              }
              return true;
            }).pipe(Effect.onError((cause) => recoverPostSuccessSettlement(effect, cause)));
          }

          const nativeEvidence = Exit.isFailure(exit)
            ? nativeEffectEvidenceFromCause(exit.cause)
            : undefined;
          const unresolved = requiresConfirmation
            ? {
                operationId: effect.id,
                operation: "start_turn" as const,
                threadId: effect.threadId,
                outcome: "unknown" as const,
              }
            : undefined;
          const heldEvidence = nativeEvidence?.outcome === "unknown" ? nativeEvidence : unresolved;
          if (heldEvidence !== undefined) {
            const held = yield* outbox.holdUnknown({
              effectId: effect.id,
              workerId,
              operationId: heldEvidence.operationId,
              evidence: heldEvidence,
              expectedAttempt: effect.attemptCount,
            });
            if (!held) {
              const current = yield* outbox.get(effect.id);
              const previous = yield* outbox.listHeldByThreadId(effect.threadId);
              const matching = previous.filter((hold) => hold.effectId === effect.id);
              if (
                Option.isSome(current) &&
                current.value.id === effect.id &&
                current.value.commandId === effect.commandId &&
                current.value.threadId === effect.threadId &&
                current.value.request.type === effect.request.type &&
                current.value.status === "running" &&
                current.value.leaseOwner === workerId &&
                current.value.attemptCount === effect.attemptCount &&
                matching.length === 1 &&
                matching[0]!.threadId === effect.threadId &&
                matching[0]!.workerId === workerId &&
                matching[0]!.operationId === heldEvidence.operationId &&
                matching[0]!.expectedAttempt === effect.attemptCount &&
                matchesNativeHoldEvidence(matching[0]!.evidence, heldEvidence)
              )
                return true;
              if (yield* wasCancelled(effect.id)) return true;
              return yield* new OrchestrationEffectWorkerError({
                operation: "hold-unknown",
                effectId: effect.id,
                cause: "The worker could not durably fence the unknown provider operation.",
              });
            }
            return true;
          }

          if (Exit.isSuccess(exit)) return true;

          const error = Cause.pretty(exit.cause);
          const nonRetryable = isNonRetryableProviderTurnControlFailure(effect.request.type, error);
          yield* Effect.logWarning("Orchestration effect execution failed", {
            effectId: effect.id,
            effectType: effect.request.type,
            attemptCount: effect.attemptCount,
            nonRetryable,
            error,
          });
          // Prefer succeed for terminal interrupt races so the outbox does not
          // keep a failed interrupt around; fail only when we must not retry.
          const updated = nonRetryable
            ? yield* outbox
                .succeed({ effectId: effect.id, workerId })
                .pipe(Effect.onError((cause) => terminalizeClaim(effect, cause)))
            : effect.attemptCount >= maxAttempts
              ? yield* outbox
                  .fail({ effectId: effect.id, workerId, error })
                  .pipe(Effect.onError((cause) => terminalizeClaim(effect, cause)))
              : yield* outbox
                  .retry({
                    effectId: effect.id,
                    workerId,
                    error,
                    delayMs: Math.min(30_000, 100 * 2 ** Math.max(0, effect.attemptCount - 1)),
                  })
                  .pipe(Effect.onError((cause) => requeueClaim(effect, cause)));
          if (!updated) {
            if (yield* wasCancelled(effect.id)) return true;
            return yield* new OrchestrationEffectWorkerError({
              operation: "reschedule",
              effectId: effect.id,
              cause: "The worker no longer owns the effect lease.",
            });
          }
          return true;
        }).pipe(
          Effect.mapError((cause) =>
            isOrchestrationEffectWorkerError(cause)
              ? cause
              : new OrchestrationEffectWorkerError({ operation: "run", cause }),
          ),
        );

      return OrchestrationEffectWorkerV2.of({
        awaitWork: outbox.awaitAvailable,
        runOnce: runOnce(),
        runRecoveryOnce: runOnce(true),
        nextClaimableAt: outbox.nextClaimableAt.pipe(
          Effect.mapError(
            (cause) =>
              new OrchestrationEffectWorkerError({
                operation: "next-claimable",
                cause,
              }),
          ),
        ),
        drain: (maxEffects = Number.MAX_SAFE_INTEGER) =>
          Effect.gen(function* () {
            let completed = 0;
            while (completed < maxEffects && (yield* runOnce())) {
              completed += 1;
            }
            return completed;
          }),
      });
    }),
  );

export const layer = layerWithOptions();

export interface OrchestrationEffectDaemonOptions {
  readonly concurrency?: number;
  readonly livenessPollIntervalMs?: number;
}

const DEFAULT_EFFECT_WORKER_CONCURRENCY = 4;
const DEFAULT_EFFECT_WORKER_LIVENESS_POLL_INTERVAL_MS = 30_000;

export const runDaemonWithOptions = (options: OrchestrationEffectDaemonOptions = {}) =>
  Effect.scoped(
    Effect.gen(function* () {
      const worker = yield* OrchestrationEffectWorkerV2;
      const requestedConcurrency = options.concurrency ?? DEFAULT_EFFECT_WORKER_CONCURRENCY;
      const concurrency = Number.isFinite(requestedConcurrency)
        ? Math.max(1, Math.floor(requestedConcurrency))
        : DEFAULT_EFFECT_WORKER_CONCURRENCY;
      const requestedLivenessPollIntervalMs =
        options.livenessPollIntervalMs ?? DEFAULT_EFFECT_WORKER_LIVENESS_POLL_INTERVAL_MS;
      const livenessPollIntervalMs = Number.isFinite(requestedLivenessPollIntervalMs)
        ? Math.max(1, Math.floor(requestedLivenessPollIntervalMs))
        : DEFAULT_EFFECT_WORKER_LIVENESS_POLL_INTERVAL_MS;
      // Post-commit notifications are the low-latency path. `availableAt` is the
      // durable retry schedule, and the long liveness poll only recovers from a
      // missed in-process notification or work inserted by another process.
      const runWorker = Effect.gen(function* () {
        while (true) {
          const outcome = yield* worker.runOnce.pipe(
            Effect.map((worked) => (worked ? ("worked" as const) : ("idle" as const))),
            Effect.catchCause((cause) =>
              Effect.logWarning("Orchestration effect worker failed", cause).pipe(
                Effect.as("failed" as const),
              ),
            ),
          );
          if (outcome === "worked") {
            yield* Effect.yieldNow;
            continue;
          }
          if (outcome === "failed") {
            // A due row can remain visible when a claim UPDATE fails. Do not
            // feed that past deadline back into the scheduler and retry at the
            // one-millisecond floor; let transient database failures cool off.
            yield* Effect.sleep(Duration.millis(Math.min(1_000, livenessPollIntervalMs)));
            continue;
          }

          const nextClaimableAt = yield* worker.nextClaimableAt.pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning(
                "Failed to read the next orchestration effect deadline",
                cause,
              ).pipe(Effect.as(Option.none<DateTime.Utc>())),
            ),
          );
          const now = DateTime.toEpochMillis(yield* DateTime.now);
          const sleepMs = Option.match(nextClaimableAt, {
            onNone: () => livenessPollIntervalMs,
            onSome: (availableAt) => {
              const untilAvailable = DateTime.toEpochMillis(availableAt) - now;
              return Math.min(livenessPollIntervalMs, untilAvailable > 0 ? untilAvailable : 25);
            },
          });
          yield* Effect.raceFirst(
            worker.awaitWork.pipe(Effect.as("notified" as const)),
            Effect.sleep(Duration.millis(sleepMs)).pipe(Effect.as("scheduled" as const)),
          );
        }
      });

      return yield* Effect.all(
        Array.from({ length: concurrency }, () => runWorker),
        {
          concurrency: "unbounded",
          discard: true,
        },
      );
    }),
  );

export const runDaemon = runDaemonWithOptions();

const daemonLayer: Layer.Layer<never, never, OrchestrationEffectWorkerV2> = Layer.effectDiscard(
  runDaemon.pipe(Effect.forkScoped),
);
