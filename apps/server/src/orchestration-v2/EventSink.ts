export * from "./OrdinaryCheckoutExecution.ts";
import { DispatchGuardRejected } from "./DispatchGuard.ts";
import {
  type RecordedRun as OrchestrationV2Run,
  RecordedLifecycleEvent as OrchestrationV2DomainEvent,
  type RecordedStoredEvent as OrchestrationV2RecordedStoredEvent,
  RecordedStoredLifecycleEvent as OrchestrationV2StoredEvent,
} from "./RecordedTypes.ts";
import {
  canonicalLegacyPayload,
  legacyPayloadHash,
  legacyBootstrapCreateCommandId,
  legacyBootstrapBirth,
  sameLegacyBootstrapPolicy,
} from "./LegacyBootstrap.ts";
import {
  type OrchestrationV2StoredEvent as PublicStoredEvent,
  ChatAttachment,
  UserInputAttachmentAnswerPayload,
  OrchestrationV2AppThreadJson,
  OrchestrationV2ConversationMessageJson,
  OrchestrationV2TurnItemJson,
  OrchestrationV2RunJson,
  CommandId,
  OrchestrationV2Command,
  type OrchestrationV2ProviderThread,
  type ProviderRuntimeEvidenceCapture,
  type RequestedRuntimeIdentity,
  type OrchestrationV2PrivateEvent,
  OrchestrationV2LegacyPreflightBinding,
  QueueDispatchCommand,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  NodeId,
  ProjectId,
  EventId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderDriverKind,
  ProviderTurnId,
  WorktreeCleanupRules,
  OrchestrationV2ThreadDeletionWorktreeRemoval,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as SchemaGetter from "effect/SchemaGetter";
import * as SchemaIssue from "effect/SchemaIssue";
import * as Path from "effect/Path";
import * as NodePathLayer from "@effect/platform-node/NodePath";
import { hasOwnJonesMigration } from "../persistence/JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../persistence/Migrations.ts";
import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
} from "../nativeCreation/NativeCreationPreparation.ts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Semaphore from "effect/Semaphore";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { ProviderNativeOperationContext, identityForRequest } from "./ProviderAdapter.ts";
import { replayAndBufferProjectedLiveEvents } from "./LiveStreamBudget.ts";
import type { UnsequencedProjectEvent } from "../persistence/Services/OrchestrationEventStore.ts";
import { isPublicStoredOrchestrationEvent, projectDomainEventForWire } from "./WireProjection.ts";

import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as TurnItemPositionStore from "./TurnItemPositionStore.ts";
import {
  makeNativeProviderRuntimeEvidence,
  type NativeProviderRuntimeEvidenceShape,
} from "./NativeProviderRuntimeEvidence.ts";
import { makeCommitTransaction } from "./CommitTransaction.ts";
import { ordinaryCheckoutCommandDigestV1 } from "./OrdinaryCheckoutOwnership.ts";
import {
  makeOrdinaryCheckoutStore,
  type OrdinaryCheckoutCaptureInput,
  type OrdinaryCheckoutCommitCapture,
  type OrdinaryCheckoutSystemEffectsV1,
} from "./OrdinaryCheckoutStore.ts";

/**
 * ERRORS
 */
export class EventSinkWriteError extends Schema.TaggedError<EventSinkWriteError>()(
  "EventSinkWriteError",
  {
    eventCount: Schema.Number,
    commandId: Schema.optional(CommandId),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to write ${this.eventCount} orchestration V2 event(s).`;
  }
}

export class EventSinkStreamError extends Schema.TaggedError<EventSinkStreamError>()(
  "EventSinkStreamError",
  {
    threadId: Schema.optional(ThreadId),
    afterSequence: Schema.optional(Schema.Number),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.threadId === undefined
      ? "Failed to stream orchestration V2 events."
      : `Failed to stream orchestration V2 events for thread ${this.threadId}.`;
  }
}

export const EventSinkV2Error = Schema.Union([EventSinkWriteError, EventSinkStreamError]);
export type EventSinkV2Error = typeof EventSinkV2Error.Type;

function runtimeEvidenceMatches(
  current: OrchestrationV2ProviderThread | null,
  capture: ProviderRuntimeEvidenceCapture,
): boolean {
  if (
    current === null ||
    current.appThreadId !== capture.threadId ||
    current.id !== capture.providerThreadId ||
    current.providerSessionId !== capture.providerSessionId ||
    current.providerInstanceId !== capture.providerInstanceId ||
    current.driver !== capture.driver ||
    current.nativeThreadRef?.driver !== capture.driver ||
    current.nativeThreadRef.nativeId !== capture.nativeThreadId ||
    current.runtimeIdentity === undefined
  )
    return false;
  const identity = current.runtimeIdentity;
  if (
    capture.runtimeGeneration !== undefined &&
    identity.runtimeGeneration !== capture.runtimeGeneration
  )
    return false;
  if (capture.evidenceRevision !== undefined) {
    const revision = identity.evidenceRevision;
    if (
      revision === undefined ||
      (capture.runtimeGeneration === undefined
        ? revision !== capture.evidenceRevision
        : revision < capture.evidenceRevision)
    )
      return false;
  }
  return true;
}

/**
 * SERVICE DEFINITION
 */
import * as ImportedAttachments from "./ImportedApplicationAttachmentInventory.ts";
import {
  parseAttachmentIdFromRelativePath,
  parseThreadSegmentFromAttachmentId,
  toSafeThreadAttachmentSegment,
} from "../attachmentStore.ts";
const EventSinkJsonCodec = (() => {
  const JsonValue = Schema.String.pipe(
    Schema.decodeTo(Schema.Unknown, {
      decode: SchemaGetter.onSome<unknown, string>((input, options) => {
        try {
          const value: unknown = JSON.parse(input);
          return Effect.succeed(Option.some(value));
        } catch (cause) {
          return Effect.fail(
            new SchemaIssue.InvalidValue({ nativeJsonCause: cause }, input, options),
          );
        }
      }),
      encode: SchemaGetter.forbiddenEncoding,
    }),
  );

  const BirthTuple = Schema.Tuple([
    Schema.Literal("t3.orchestration-v2.thread-birth/v1"),
    EventId,
    Schema.Int.check(Schema.isGreaterThan(0)),
  ]);

  const BirthTupleJson = BirthTuple.pipe(
    Schema.decodeTo(Schema.String, {
      decode: SchemaGetter.onSome<string, typeof BirthTuple.Type>((input, options) => {
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
  );

  const decodeJson = Schema.decodeEffect(JsonValue);
  const encodeBirthTupleJson = Schema.decodeEffect(BirthTupleJson);

  // Stock JSON getters discard the native exception. Preserve its identity at the
  // caller's existing defect or domain-error boundary through issue metadata.
  function jsonCause(error: Schema.SchemaError): unknown {
    let issue = error.issue;
    while (issue._tag === "Encoding") issue = issue.issue;
    if (
      issue._tag === "InvalidValue" &&
      issue.annotations !== undefined &&
      Object.hasOwn(issue.annotations, "nativeJsonCause")
    ) {
      return issue.annotations["nativeJsonCause"];
    }
    return error;
  }

  const jsonProjection = <S extends Schema.Top>(output: S, read: (text: string) => S["Type"]) =>
    Schema.decodeEffect(
      Schema.String.pipe(
        Schema.decodeTo(output, {
          decode: SchemaGetter.onSome<S["Type"], string>((input, options) => {
            try {
              return Effect.succeed(Option.some(read(input)));
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

  const decodeOwnerBirth = jsonProjection(
    Schema.Unknown,
    (text): unknown => JSON.parse(text).birth,
  );
  const decodeCorrelationEvidence = jsonProjection(
    Schema.Unknown,
    (text): unknown => JSON.parse(text).evidence,
  );
  const decodeBindingSha256 = jsonProjection(
    Schema.Unknown,
    (text): unknown => JSON.parse(text).bindingSha256,
  );
  const decodeTaskKind = jsonProjection(Schema.Unknown, (text): unknown => JSON.parse(text).kind);
  const decodeLeaseStatus = jsonProjection(
    Schema.Unknown,
    (text): unknown => JSON.parse(text).status,
  );
  const decodeTaskWithKind = jsonProjection(
    Schema.Struct({ value: Schema.Unknown, kind: Schema.Unknown }),
    (text) => {
      const value = JSON.parse(text);
      return { value, kind: value.kind };
    },
  );
  const decodeLeaseWithStatus = jsonProjection(
    Schema.Struct({ value: Schema.Unknown, status: Schema.Unknown }),
    (text) => {
      const value = JSON.parse(text);
      return { value, status: value.status };
    },
  );
  const decodeCleanupCorrelation = jsonProjection(
    Schema.Struct({ value: Schema.Unknown, evidenceSchema: Schema.Unknown }),
    (text) => {
      const value = JSON.parse(text);
      return { value, evidenceSchema: value.evidence?.schema };
    },
  );

  const decodeDeletionInventoryMismatch = (
    text: string,
    expected: {
      readonly threadId: string;
      readonly projectId: string;
      readonly branch: string | null;
      readonly projectRoot: string | null;
      readonly path: string | null;
    },
    resolvePath: (root: string, path: string) => string,
  ) =>
    jsonProjection(Schema.Boolean, (text) => {
      const deleted = JSON.parse(text);
      return (
        deleted.id !== expected.threadId ||
        deleted.projectId !== expected.projectId ||
        deleted.branch !== expected.branch ||
        (deleted.worktreePath === null || expected.projectRoot === null
          ? null
          : resolvePath(expected.projectRoot, deleted.worktreePath)) !== expected.path
      );
    })(text);

  const decodeCleanupDeletionMismatch = (
    text: string,
    expected: {
      readonly threadId: string;
      readonly projectId: string;
      readonly branch: string | null;
      readonly projectRoot: string | null;
      readonly path: string | null;
    },
    resolvePath: (root: string, path: string) => string,
  ) =>
    jsonProjection(Schema.Boolean, (text) => {
      const deleted = JSON.parse(text);
      const expectedPath =
        deleted.worktreePath === null || expected.projectRoot === null
          ? null
          : resolvePath(expected.projectRoot, deleted.worktreePath);
      return (
        deleted.id !== expected.threadId ||
        deleted.projectId !== expected.projectId ||
        expectedPath !== expected.path ||
        deleted.branch !== expected.branch
      );
    })(text);

  return {
    decodeJson,
    encodeBirthTupleJson,
    jsonCause,
    decodeOwnerBirth,
    decodeCorrelationEvidence,
    decodeBindingSha256,
    decodeTaskKind,
    decodeLeaseStatus,
    decodeTaskWithKind,
    decodeLeaseWithStatus,
    decodeCleanupCorrelation,
    decodeDeletionInventoryMismatch,
    decodeCleanupDeletionMismatch,
  };
})();
const LowerSha256 = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/));
const CapturedRestartIsoTimestampV1 = Schema.String.check(
  Schema.isPattern(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/),
);
const RegisteredSourceSchemaV2 = Schema.Struct({
  threadId: ThreadId,
  providerThreadId: ProviderThreadId,
  providerSessionId: ProviderSessionId,
  instanceId: ProviderInstanceId,
  driver: ProviderDriverKind,
  nativeThreadId: Schema.NullOr(Schema.String),
  runtimeGeneration: Schema.NonEmptyString,
});
const ApplicationBirthSchemaV2 = Schema.Struct({
  kind: Schema.Literal("application_v2_thread_birth"),
  threadId: ThreadId,
  eventId: EventId,
  sequence: Schema.Int.check(Schema.isGreaterThan(0)),
});
const CleanupLeaseSchemaV2 = Schema.Struct({
  resourcePath: Schema.NonEmptyString,
  leaseId: Schema.NonEmptyString,
  ownerThreadId: ThreadId,
  ownerIncarnation: Schema.NonEmptyString,
  branch: Schema.NullOr(Schema.String),
  acquiredAtMs: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  renewedAtMs: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  expiresAtMs: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});
const CleanupDeletionSchemaV2 = Schema.Struct({
  commandId: CommandId,
  eventId: EventId,
  sequence: Schema.Int.check(Schema.isGreaterThan(0)),
});
const CleanupTerminalTargetSchemaV2 = Schema.Struct({
  threadId: Schema.NonEmptyString,
  terminalId: Schema.NonEmptyString,
  handleId: Schema.NonEmptyString,
  ownerBirth: ApplicationBirthSchemaV2,
});
const CleanupTerminalCaptureSchemaV2 = Schema.Struct({
  managerId: Schema.NonEmptyString,
  threadId: Schema.NonEmptyString,
  ownerBirth: ApplicationBirthSchemaV2,
  status: Schema.Literal("captured"),
  managedTargetsOnly: Schema.Literal(true),
  targets: Schema.Array(CleanupTerminalTargetSchemaV2),
});
const LeaseCleanupTaskV2 = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("provider"),
    expectedBinding: RegisteredSourceSchemaV2,
    evidenceRevision: Schema.Int.check(Schema.isGreaterThan(0)),
  }),
  Schema.Struct({ kind: Schema.Literal("terminal"), capture: CleanupTerminalCaptureSchemaV2 }),
  Schema.Struct({
    kind: Schema.Literal("attachment"),
    attachmentIds: Schema.Array(Schema.NonEmptyString),
  }),
]);
type LeaseCleanupTaskV2 = typeof LeaseCleanupTaskV2.Type;
const LeaseCleanupTaskBindingV2 = Schema.Struct({
  version: Schema.Literal(2),
  effectId: Schema.NonEmptyString,
  threadId: ThreadId,
  lease: CleanupLeaseSchemaV2,
  ownerBirth: ApplicationBirthSchemaV2,
  deletion: CleanupDeletionSchemaV2,
  task: LeaseCleanupTaskV2,
  bindingSha256: LowerSha256,
  recordedAt: CapturedRestartIsoTimestampV1,
});
type LeaseCleanupTaskBindingV2 = typeof LeaseCleanupTaskBindingV2.Type;
const UnleasedDeletionCleanupTaskBindingV1 = Schema.Struct({
  version: Schema.Literal(1),
  effectId: Schema.NonEmptyString,
  threadId: ThreadId,
  leaseInventory: Schema.Struct({
    status: Schema.Literal("absent"),
    resourcePath: Schema.NonEmptyString,
  }),
  ownerBirth: ApplicationBirthSchemaV2,
  deletion: CleanupDeletionSchemaV2,
  task: LeaseCleanupTaskV2,
  bindingSha256: LowerSha256,
  recordedAt: CapturedRestartIsoTimestampV1,
});
type UnleasedDeletionCleanupTaskBindingV1 = typeof UnleasedDeletionCleanupTaskBindingV1.Type;
const DeletionCleanupTaskBindingV1 = Schema.Union([
  LeaseCleanupTaskBindingV2,
  UnleasedDeletionCleanupTaskBindingV1,
]);
type DeletionCleanupTaskBindingV1 = typeof DeletionCleanupTaskBindingV1.Type;
const deletionCleanupTaskBindingDigestV1 = (
  input: Omit<DeletionCleanupTaskBindingV1, "bindingSha256" | "recordedAt">,
) => nativeCreationSha256(nativeCreationCanonicalJson(input));
const LeaseCleanupTaskOutcomeV2 = Schema.Struct({
  taskId: Schema.NonEmptyString,
  result: Schema.NullOr(Schema.Literals(["succeeded", "failed"])),
  effect: Schema.Literals(["confirmed", "absent", "no_effect", "unknown"]),
});
type LeaseCleanupTaskOutcomeV2 = typeof LeaseCleanupTaskOutcomeV2.Type;
const leaseCleanupTaskBindingDigestV2 = (
  input: Omit<LeaseCleanupTaskBindingV2, "bindingSha256" | "recordedAt">,
) => nativeCreationSha256(nativeCreationCanonicalJson(input));
const DeletionWorktreeLeaseInventoryV1 = Schema.Union([
  Schema.Struct({ status: Schema.Literal("original"), lease: CleanupLeaseSchemaV2 }),
  Schema.Struct({ status: Schema.Literal("absent") }),
  Schema.Struct({ status: Schema.Literal("conflict"), leases: Schema.Array(CleanupLeaseSchemaV2) }),
  Schema.Struct({ status: Schema.Literal("unavailable") }),
]);
const DeletionWorktreeCleanupRequestV1 = Schema.Union([
  Schema.Struct({
    origin: Schema.Literal("explicit"),
    consent: OrchestrationV2ThreadDeletionWorktreeRemoval,
  }),
  Schema.Struct({
    origin: Schema.Literal("policy"),
    projectId: ProjectId,
    path: Schema.NonEmptyString,
    branch: Schema.NullOr(Schema.String),
    force: Schema.Literal(false),
    rules: WorktreeCleanupRules,
  }),
]);
type DeletionWorktreeCleanupRequestV1 = typeof DeletionWorktreeCleanupRequestV1.Type;
const DeletionWorktreeTaskBindingV1 = Schema.Struct({
  version: Schema.Literal(1),
  effectId: Schema.NonEmptyString,
  threadId: ThreadId,
  leaseInventory: DeletionWorktreeLeaseInventoryV1,
  ownerBirth: Schema.NullOr(ApplicationBirthSchemaV2),
  deletion: CleanupDeletionSchemaV2,
  task: Schema.Struct({
    kind: Schema.Literal("worktree"),
    canonicalCommand: OrchestrationV2Command,
    commandDigest: LowerSha256,
    consent: Schema.optionalKey(OrchestrationV2ThreadDeletionWorktreeRemoval),
    request: Schema.optionalKey(DeletionWorktreeCleanupRequestV1),
    worktree: Schema.Struct({
      projectId: ProjectId,
      path: Schema.NullOr(Schema.NonEmptyString),
      branch: Schema.NullOr(Schema.String),
    }),
    projectRoot: Schema.NullOr(Schema.NonEmptyString),
    prerequisiteEffectIds: Schema.Array(Schema.NonEmptyString),
    captureStatus: Schema.Literals(["captured", "retained"]),
    reason: Schema.NullOr(Schema.NonEmptyString),
  }),
  bindingSha256: LowerSha256,
  recordedAt: CapturedRestartIsoTimestampV1,
});
type DeletionWorktreeTaskBindingV1 = typeof DeletionWorktreeTaskBindingV1.Type;
const deletionWorktreeCleanupRequestV1 = (
  binding: DeletionWorktreeTaskBindingV1,
): DeletionWorktreeCleanupRequestV1 | null =>
  binding.task.request ??
  (binding.task.consent === undefined
    ? null
    : { origin: "explicit", consent: binding.task.consent });
const deletionWorktreeTaskBindingDigestV1 = (
  input: Omit<DeletionWorktreeTaskBindingV1, "bindingSha256" | "recordedAt">,
) =>
  nativeCreationSha256(
    nativeCreationCanonicalJson(
      Schema.encodeSync(
        Schema.Struct({
          version: DeletionWorktreeTaskBindingV1.fields.version,
          effectId: DeletionWorktreeTaskBindingV1.fields.effectId,
          threadId: DeletionWorktreeTaskBindingV1.fields.threadId,
          leaseInventory: DeletionWorktreeTaskBindingV1.fields.leaseInventory,
          ownerBirth: DeletionWorktreeTaskBindingV1.fields.ownerBirth,
          deletion: DeletionWorktreeTaskBindingV1.fields.deletion,
          task: DeletionWorktreeTaskBindingV1.fields.task,
        }),
      )(input),
    ),
  );
const deletionWorktreeEffectIdV1 = (commandId: CommandId, threadId: ThreadId) =>
  `effect:${commandId}:worktree.cleanup:${threadId}`;
const DeletionWorktreeRemovalTargetV1 = Schema.Struct({
  projectId: ProjectId,
  projectRoot: Schema.NonEmptyString,
  path: Schema.NonEmptyString,
  branch: Schema.NullOr(Schema.String),
  force: Schema.Boolean,
});
type DeletionWorktreeRemovalTargetV1 = typeof DeletionWorktreeRemovalTargetV1.Type;
const DeletionWorktreeRemovalStartV1 = Schema.Struct({
  schema: Schema.Literal("t3.deletion-worktree-removal-start/v1"),
  effectId: Schema.NonEmptyString,
  bindingSha256: LowerSha256,
  workerId: Schema.NonEmptyString,
  expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)),
  target: DeletionWorktreeRemovalTargetV1,
  startedAt: CapturedRestartIsoTimestampV1,
});
type DeletionWorktreeRemovalStartV1 = typeof DeletionWorktreeRemovalStartV1.Type;
const DeletionWorktreeReadbackSchemaV1 = Schema.Struct({
  registration: Schema.Union([
    Schema.Struct({
      status: Schema.Literal("complete"),
      projectRoot: Schema.NonEmptyString,
      gitCommonDirectory: Schema.NonEmptyString,
      entries: Schema.Array(
        Schema.Struct({
          path: Schema.NonEmptyString,
          head: Schema.NullOr(Schema.String),
          branch: Schema.NullOr(Schema.String),
          bare: Schema.Boolean,
        }),
      ),
    }),
    Schema.Struct({ status: Schema.Literal("unavailable"), reason: Schema.NonEmptyString }),
  ]),
  filesystem: Schema.Union([
    Schema.Struct({ status: Schema.Literals(["present", "absent"]), path: Schema.NonEmptyString }),
    Schema.Struct({
      status: Schema.Literal("unavailable"),
      path: Schema.NonEmptyString,
      reason: Schema.NonEmptyString,
    }),
  ]),
});
const DeletionWorktreeRemovalObservationSchemaV1 = Schema.Struct({
  version: Schema.Literal(1),
  start: DeletionWorktreeRemovalStartV1,
  startOrdinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  operation: Schema.Union([
    Schema.Struct({
      kind: Schema.Literal("executed"),
      exitCode: Schema.NullOr(Schema.Int),
      completion: Schema.Literals(["exited", "unknown"]),
    }),
    Schema.Struct({ kind: Schema.Literal("reconciled"), completion: Schema.Literal("unknown") }),
    Schema.Struct({
      kind: Schema.Literal("already_absent"),
      completion: Schema.Literal("not_invoked"),
    }),
  ]),
  before: DeletionWorktreeReadbackSchemaV1,
  after: DeletionWorktreeReadbackSchemaV1,
  observedAt: CapturedRestartIsoTimestampV1,
});
const ManagedTerminalDeletionObservationV1 = Schema.Struct({
  version: Schema.Literal(1),
  kind: Schema.Literal("managed_terminal"),
  effectId: Schema.NonEmptyString,
  bindingSha256: LowerSha256,
  workerId: Schema.NonEmptyString,
  expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)),
  capture: CleanupTerminalCaptureSchemaV2,
  result: Schema.Struct({
    status: Schema.Literals(["closed", "observed_absent", "mismatch", "unknown"]),
    managedTargetsOnly: Schema.Literal(true),
    processExitObserved: Schema.Boolean,
    descendantsQuiescence: Schema.Literal("unavailable"),
    futureWakeClosure: Schema.Literal("unavailable"),
  }),
  observedAt: CapturedRestartIsoTimestampV1,
});
type ManagedTerminalDeletionObservationV1 = typeof ManagedTerminalDeletionObservationV1.Type;
const ManagedProviderDeletionObservationV1 = Schema.Struct({
  version: Schema.Literal(1),
  kind: Schema.Literal("managed_provider"),
  effectId: Schema.NonEmptyString,
  bindingSha256: LowerSha256,
  workerId: Schema.NonEmptyString,
  expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)),
  binding: RegisteredSourceSchemaV2,
  evidenceRevision: Schema.Int.check(Schema.isGreaterThan(0)),
  nativeOperation: ProviderNativeOperationContext,
  result: Schema.Union([
    Schema.Struct({
      status: Schema.Literal("stopped"),
      operationId: Schema.NonEmptyString,
      binding: RegisteredSourceSchemaV2,
      cancelledPendingStart: Schema.Boolean,
      interruptedProviderTurnIds: Schema.Array(ProviderTurnId),
      readback: Schema.Struct({ threadAttached: Schema.Literal(false) }),
    }),
    Schema.Struct({ status: Schema.Literal("unknown"), reason: Schema.String }),
  ]),
  observedAt: CapturedRestartIsoTimestampV1,
});
type ManagedProviderDeletionObservationV1 = typeof ManagedProviderDeletionObservationV1.Type;
const DeletionCleanupObservationV1 = Schema.Union([
  DeletionWorktreeRemovalObservationSchemaV1,
  ManagedTerminalDeletionObservationV1,
  ManagedProviderDeletionObservationV1,
]);
type DeletionCleanupObservationV1 = typeof DeletionCleanupObservationV1.Type;
const deletionWorktreeRemovalTargetV1 = (
  binding: DeletionWorktreeTaskBindingV1,
): DeletionWorktreeRemovalTargetV1 | null =>
  binding.task.worktree.path === null ||
  binding.task.projectRoot === null ||
  deletionWorktreeCleanupRequestV1(binding) === null
    ? null
    : {
        ...binding.task.worktree,
        path: binding.task.worktree.path,
        projectRoot: binding.task.projectRoot,
        force: deletionWorktreeCleanupRequestV1(binding)!.origin === "explicit",
      };
const DeletionWorktreeInventorySchemaV1 = Schema.Struct({
  worktree: DeletionWorktreeTaskBindingV1.fields.task.fields.worktree,
  projectRoot: DeletionWorktreeTaskBindingV1.fields.task.fields.projectRoot,
  leaseInventory: DeletionWorktreeLeaseInventoryV1,
  prerequisiteEffectIds: Schema.Array(Schema.NonEmptyString),
  request: Schema.optionalKey(DeletionWorktreeCleanupRequestV1),
  captureStatus: Schema.Literals(["captured", "retained"]),
  reason: Schema.NullOr(Schema.NonEmptyString),
});
const ThreadDeletionCommandRecordSchemaV1 = Schema.Struct({
  command: OrchestrationV2Command,
  commandDigest: LowerSha256,
  ownerBirth: Schema.NullOr(ApplicationBirthSchemaV2),
  inventory: DeletionWorktreeInventorySchemaV1,
  deletion: CleanupDeletionSchemaV2,
  recordedAt: CapturedRestartIsoTimestampV1,
});
type ThreadDeletionCommandRecordV1 = Omit<
  typeof ThreadDeletionCommandRecordSchemaV1.Type,
  "command"
> & { readonly command: Extract<OrchestrationV2Command, { readonly type: "thread.delete" }> };
const AttachmentNamespaceCleanupTaskV1 = EffectOutbox.AttachmentNamespaceCleanupTaskV1;
type AttachmentNamespaceCleanupTaskV1 = typeof AttachmentNamespaceCleanupTaskV1.Type;
const QualifiedAttachmentNamespaceCleanupBasisCodecV1 =
  EffectOutbox.QualifiedAttachmentNamespaceCleanupBasisV1;
const AttachmentNamespaceCleanupBasisV1 = Schema.Union([
  QualifiedAttachmentNamespaceCleanupBasisCodecV1,
  Schema.Struct({
    status: Schema.Literal("unavailable"),
    effectId: Schema.NonEmptyString,
    reason: Schema.NonEmptyString,
  }),
]);
type AttachmentNamespaceCleanupBasisV1 = typeof AttachmentNamespaceCleanupBasisV1.Type;
type QualifiedAttachmentNamespaceCleanupBasisV1 = Exclude<
  AttachmentNamespaceCleanupBasisV1,
  { readonly status: "unavailable" }
>;
const AttachmentNamespaceCleanupObservationV1 =
  EffectOutbox.AttachmentNamespaceCleanupObservationV1;
type AttachmentNamespaceCleanupObservationV1 = typeof AttachmentNamespaceCleanupObservationV1.Type;
interface AttachmentNamespaceCleanupRecordResultV1 {
  readonly status: "completed" | "retryable" | "unknown" | "stale";
  readonly effectId: string;
  readonly ordinal: number | null;
}
interface AttachmentNamespaceCleanupRecordedObservationV1 {
  readonly ordinal: number;
  readonly task: AttachmentNamespaceCleanupTaskV1;
  readonly basis: QualifiedAttachmentNamespaceCleanupBasisV1;
  readonly observation: AttachmentNamespaceCleanupObservationV1;
  readonly status: "completed" | "retryable" | "unknown";
}
const attachmentTaskDigest = (task: Omit<AttachmentNamespaceCleanupTaskV1, "bindingSha256">) =>
  nativeCreationSha256(nativeCreationCanonicalJson(task));

interface ImportedApplicationAttachmentInventoryInput {
  readonly threadId: ThreadId;
  readonly expectedBirth: ImportedAttachments.ImportedApplicationAttachmentBirthV1;
}
interface ImportedApplicationAttachmentInventoryReadInput extends ImportedApplicationAttachmentInventoryInput {
  readonly inventoryId?: string;
}
interface EventSinkStreamInput {
  readonly threadId?: ThreadId;
  readonly afterSequence?: number;
  /** Filter before queuing so workers retain only the events they handle. */
  readonly eventType?: OrchestrationV2DomainEvent["type"];
  /** Bounded subscribers receive projected public events. Workers retain recorded values. */
  readonly bounded?: boolean;
}

export interface EventSinkV2Shape {
  readonly readThreadRetainedAttachmentPaths?: (
    threadId: ThreadId,
  ) => Effect.Effect<ProjectionStore.ProjectionThreadRetainedAttachmentPaths, EventSinkV2Error>;

  readonly readApplicationBirthRecord?: (
    threadId: ThreadId,
  ) => Effect.Effect<
    ImportedAttachments.ImportedApplicationAttachmentBirthV1 | null,
    EventSinkV2Error
  >;

  readonly prepareImportedApplicationAttachmentInventory?: (
    input: ImportedApplicationAttachmentInventoryInput,
  ) => Effect.Effect<
    ImportedAttachments.ImportedApplicationAttachmentQualificationV1,
    EventSinkV2Error
  >;
  readonly readImportedApplicationAttachmentInventory?: (
    input: ImportedApplicationAttachmentInventoryReadInput,
  ) => Effect.Effect<
    ImportedAttachments.ImportedApplicationAttachmentQualificationV1,
    EventSinkV2Error
  >;

  readonly readUnresolvedDeletionCleanupHolds?: (
    threadId: ThreadId,
  ) => Effect.Effect<ReadonlyArray<EffectOutbox.UnknownEffectHoldV2>, EventSinkV2Error>;
  readonly readAttachmentNamespaceCleanupObservation?: (
    effectId: string,
  ) => Effect.Effect<AttachmentNamespaceCleanupRecordedObservationV1 | null, EventSinkV2Error>;
  readonly readProviderRuntimeEvidence?: NativeProviderRuntimeEvidenceShape["readProviderRuntimeEvidence"];
  readonly readCurrentProviderRuntimeOwner?: NativeProviderRuntimeEvidenceShape["readCurrentProviderRuntimeOwner"];
  readonly registerProviderRuntime?: NativeProviderRuntimeEvidenceShape["registerProviderRuntime"];
  readonly ordinaryCheckoutLifetime?: import("./OrdinaryCheckoutStore.ts").OrdinaryCheckoutLifetime;
  readonly captureOrdinaryCheckout?: (
    input: OrdinaryCheckoutCaptureInput,
  ) => Effect.Effect<OrdinaryCheckoutCommitCapture, EventSinkWriteError>;
  readonly validateOrdinaryCheckoutCommandReplay?: (
    command: OrchestrationV2Command,
    threadId: ThreadId,
  ) => Effect.Effect<void, EventSinkWriteError>;
  readonly validateOrdinaryCheckoutReplay?: (
    capture: OrdinaryCheckoutCommitCapture,
  ) => Effect.Effect<void, EventSinkWriteError>;
  readonly commitLegacyPreflight: (input: {
    readonly commandId: CommandId;
    readonly event: OrchestrationV2PrivateEvent;
  }) => Effect.Effect<
    { readonly receipt: CommandReceiptStore.CommandReceiptV2; readonly committed: boolean },
    EventSinkV2Error
  >;

  readonly write: (input: {
    readonly runtimeIdentityRequest?: RequestedRuntimeIdentity;
    readonly runtimeIdentityPreviousRequest?: RequestedRuntimeIdentity;
    readonly runtimeEvidence?: ProviderRuntimeEvidenceCapture;
    readonly runtimeIdentityObservation?: RequestedRuntimeIdentity;
    readonly runtimeIdentityBoundary?: { readonly expectedGeneration: string | null };
    readonly guardPendingUserInputCancellations?: boolean;
    readonly commandId?: CommandId;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  }) => Effect.Effect<ReadonlyArray<OrchestrationV2StoredEvent>, EventSinkV2Error>;
  readonly writeWithEffects: (input: {
    readonly runtimeIdentityRequest?: RequestedRuntimeIdentity;
    readonly runtimeIdentityPreviousRequest?: RequestedRuntimeIdentity;
    readonly runtimeEvidence?: ProviderRuntimeEvidenceCapture;
    readonly runtimeIdentityObservation?: RequestedRuntimeIdentity;
    readonly runtimeIdentityBoundary?: { readonly expectedGeneration: string | null };
    readonly guardPendingUserInputCancellations?: boolean;
    readonly commandId?: CommandId;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
    readonly effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
    readonly ordinaryCheckoutEffects?: ReadonlyArray<OrdinaryCheckoutSystemEffectsV1>;
  }) => Effect.Effect<ReadonlyArray<OrchestrationV2StoredEvent>, EventSinkV2Error>;
  readonly writeIfRunCurrent: (input: {
    readonly runtimeIdentityRequest?: RequestedRuntimeIdentity;
    readonly runtimeIdentityPreviousRequest?: RequestedRuntimeIdentity;
    readonly runtimeEvidence?: ProviderRuntimeEvidenceCapture;
    readonly runtimeIdentityObservation?: RequestedRuntimeIdentity;
    readonly runtimeIdentityBoundary?: { readonly expectedGeneration: string | null };
    readonly guardPendingUserInputCancellations?: boolean;
    readonly commandId?: CommandId;
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly activeAttemptId: RunAttemptId;
    readonly expectedStatus: OrchestrationV2Run["status"];
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  }) => Effect.Effect<
    {
      readonly committed: boolean;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    },
    EventSinkV2Error
  >;
  /**
   * Atomically commit only when the provider thread is still owned by the
   * expected run attempt and ordinal. Used for late post-terminal
   * provider_thread updates so a completed or superseded attempt cannot clobber
   * a newer attempt that already claimed the thread.
   */
  readonly writeIfProviderThreadOwner: (input: {
    readonly runtimeIdentityRequest?: RequestedRuntimeIdentity;
    readonly runtimeIdentityPreviousRequest?: RequestedRuntimeIdentity;
    readonly runtimeEvidence?: ProviderRuntimeEvidenceCapture;
    readonly runtimeIdentityObservation?: RequestedRuntimeIdentity;
    readonly runtimeIdentityBoundary?: { readonly expectedGeneration: string | null };
    readonly guardPendingUserInputCancellations?: boolean;
    readonly commandId?: CommandId;
    readonly providerThreadId: ProviderThreadId;
    readonly runId: RunId;
    readonly activeAttemptId: RunAttemptId;
    readonly expectedLastRunOrdinal: number;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  }) => Effect.Effect<
    {
      readonly committed: boolean;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    },
    EventSinkV2Error
  >;
  readonly commitCommand: (input: {
    readonly ordinaryCheckout?: OrdinaryCheckoutCommitCapture;
    readonly ordinaryDelegatedCommand?: Extract<
      OrchestrationV2Command,
      { readonly type: "delegated_task.request" }
    >;
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly commandType: string;
    readonly acceptedAt: DateTime.Utc;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
    readonly effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
    readonly cancelUnsettledEffects?: {
      readonly effectTypes: ReadonlyArray<EffectOutbox.OrchestrationEffectRequestV2["type"]>;
      readonly reason: string;
    };
  }) => Effect.Effect<
    {
      readonly receipt: CommandReceiptStore.CommandReceiptV2;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
      readonly committed: boolean;
      readonly cancelledEffectCount: number;
    },
    EventSinkV2Error
  >;
  readonly commitRejectedCommand: (input: {
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly commandType: string;
    readonly rejectedAt: DateTime.Utc;
    readonly error: string;
    readonly legacyGuardRejection?: {
      readonly rejection: DispatchGuardRejected;
      readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
    };
  }) => Effect.Effect<CommandReceiptStore.CommandReceiptV2, EventSinkV2Error>;
  /**
   * Append a project event, fold it into its row and record the receipt in one
   * transaction. A reused command id commits nothing and returns its receipt.
   */
  readonly commitProjectCommand: (input: {
    readonly commandId: CommandId;
    readonly projectId: ProjectId;
    readonly commandType: string;
    readonly acceptedAt: DateTime.Utc;
    readonly event: UnsequencedProjectEvent;
  }) => Effect.Effect<
    { readonly receipt: CommandReceiptStore.ProjectCommandReceiptV2; readonly committed: boolean },
    EventSinkV2Error
  >;
  /** Record a rejected project command, or return the receipt its command id already has. */
  readonly commitRejectedProjectCommand: (input: {
    readonly commandId: CommandId;
    readonly projectId: ProjectId;
    readonly commandType: string;
    readonly rejectedAt: DateTime.Utc;
    readonly error: string;
  }) => Effect.Effect<CommandReceiptStore.ProjectCommandReceiptV2, EventSinkV2Error>;
  readonly stream: {
    (
      input: EventSinkStreamInput & { readonly bounded: true },
    ): Stream.Stream<PublicStoredEvent, EventSinkV2Error>;
    (
      input?: EventSinkStreamInput & { readonly bounded?: false },
    ): Stream.Stream<OrchestrationV2StoredEvent, EventSinkV2Error>;
    (
      input?: EventSinkStreamInput,
    ): Stream.Stream<PublicStoredEvent | OrchestrationV2StoredEvent, EventSinkV2Error>;
  };
  readonly latestSequence: (input?: {
    readonly threadId?: ThreadId;
  }) => Effect.Effect<number, EventSinkV2Error>;
  readonly readByCommandId: (input: {
    readonly commandId: CommandId;
  }) => Stream.Stream<OrchestrationV2StoredEvent, EventSinkV2Error>;
}

export class EventSinkV2 extends Context.Service<EventSinkV2, EventSinkV2Shape>()(
  "t3/orchestration-v2/EventSink/EventSinkV2",
) {}

/**
 * IMPLEMENTATIONS
 */
const isDispatchGuardRejected = (value: unknown): value is DispatchGuardRejected =>
  Schema.is(DispatchGuardRejected)(value);

const baseLayer: Layer.Layer<
  EventSinkV2,
  never,
  | CommandReceiptStore.CommandReceiptStoreV2
  | EffectOutbox.EffectOutboxV2
  | EventStore.EventStoreV2
  | ProjectionStore.ProjectionStoreV2
  | ProjectStore.ProjectStoreV2
  | SqlClient.SqlClient
  | TurnItemPositionStore.TurnItemPositionStoreV2
> = Layer.effect(
  EventSinkV2,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const NodePath = yield* Path.Path.pipe(Effect.provide(NodePathLayer.layer));
    const commandReceipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
    const effectOutbox = yield* EffectOutbox.EffectOutboxV2;
    const eventStore = yield* EventStore.EventStoreV2;
    const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
    const projectStore = yield* ProjectStore.ProjectStoreV2;
    const turnItemPositions = yield* TurnItemPositionStore.TurnItemPositionStoreV2;
    const commitTransaction = yield* makeCommitTransaction();
    const nativeRuntimeEvidence = yield* makeNativeProviderRuntimeEvidence(commitTransaction);
    const readCleanupSnapshot = (effectId: string) =>
      effectOutbox.readQualifiedCleanupSnapshot === undefined
        ? Effect.succeed(Option.none())
        : effectOutbox.readQualifiedCleanupSnapshot(effectId);
    const readThreadDeletionCommandEffect = Effect.fnUntraced(function* (commandId: CommandId) {
      const rows = yield* sql<{
        readonly thread_id: string;
        readonly canonical_command_json: string;
        readonly command_digest: string;
        readonly owner_birth_json: string;
        readonly worktree_inventory_json: string;
        readonly deletion_event_id: string;
        readonly deletion_event_sequence: number;
        readonly recorded_at: string;
      }>`
        SELECT * FROM orchestration_v2_thread_deletion_commands WHERE command_id = ${commandId}`;
      if (rows.length === 0) return null;
      const row = rows[0]!;
      const record = yield* Schema.decodeUnknownEffect(ThreadDeletionCommandRecordSchemaV1)(
        {
          command: yield* EventSinkJsonCodec.decodeJson(row.canonical_command_json).pipe(
            Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
          ),
          commandDigest: row.command_digest,
          ownerBirth: yield* EventSinkJsonCodec.decodeOwnerBirth(row.owner_birth_json).pipe(
            Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
          ),
          inventory: yield* EventSinkJsonCodec.decodeJson(row.worktree_inventory_json).pipe(
            Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
          ),
          deletion: {
            commandId,
            eventId: row.deletion_event_id,
            sequence: row.deletion_event_sequence,
          },
          recordedAt: row.recorded_at,
        },
        { onExcessProperty: "error" },
      );
      const command = record.command;
      const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(commandId));
      const events = yield* sql<{
        readonly payload_json: string;
      }>`SELECT payload_json FROM orchestration_events
        WHERE event_id = ${record.deletion.eventId} AND sequence = ${record.deletion.sequence} AND command_id = ${commandId}
          AND application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${row.thread_id} AND event_type = 'thread.deleted'`;
      if (
        command.type !== "thread.delete" ||
        command.commandId !== commandId ||
        command.threadId !== row.thread_id ||
        record.commandDigest !==
          nativeCreationSha256(
            nativeCreationCanonicalJson(
              yield* Schema.encodeEffect(OrchestrationV2Command)(command).pipe(Effect.orDie),
            ),
          ) ||
        receipt?.status !== "accepted" ||
        receipt.commandType !== command.type ||
        receipt.threadId !== command.threadId ||
        receipt.resultSequence < record.deletion.sequence ||
        events.length !== 1
      )
        return yield* new EventSinkWriteError({
          eventCount: 0,
          commandId,
          cause: "Original deletion command lost its accepted event association",
        });
      if (
        yield* EventSinkJsonCodec.decodeDeletionInventoryMismatch(
          events[0]!.payload_json,
          {
            threadId: command.threadId,
            projectId: record.inventory.worktree.projectId,
            branch: record.inventory.worktree.branch,
            projectRoot: record.inventory.projectRoot,
            path: record.inventory.worktree.path,
          },
          NodePath.resolve,
        ).pipe(Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))))
      )
        return yield* new EventSinkWriteError({
          eventCount: 0,
          commandId,
          cause: "Original deletion inventory differs from its accepted application path",
        });
      return { ...record, command } satisfies ThreadDeletionCommandRecordV1;
    });
    const readDeletionWorktreeTaskEffect = Effect.fnUntraced(function* (effectId: string) {
      const rows = yield* sql<{
        readonly thread_id: string;
        readonly lease_json: string;
        readonly owner_birth_json: string;
        readonly deletion_json: string;
        readonly task_json: string;
        readonly binding_sha256: string;
        readonly recorded_at: string;
      }>`
        SELECT * FROM orchestration_v2_lease_cleanup_task_bindings WHERE effect_id = ${effectId}`;
      if (rows.length === 0) return null;
      const row = rows[0]!;
      const parsedTask = yield* EventSinkJsonCodec.decodeTaskWithKind(row.task_json).pipe(
        Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
      );
      if (parsedTask.kind !== "worktree") return null;
      const task = parsedTask.value;
      const binding = yield* Schema.decodeUnknownEffect(DeletionWorktreeTaskBindingV1)(
        {
          version: 1,
          effectId,
          threadId: row.thread_id,
          leaseInventory: yield* EventSinkJsonCodec.decodeJson(row.lease_json).pipe(
            Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
          ),
          ownerBirth: yield* EventSinkJsonCodec.decodeOwnerBirth(row.owner_birth_json).pipe(
            Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
          ),
          deletion: yield* EventSinkJsonCodec.decodeJson(row.deletion_json).pipe(
            Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
          ),
          task,
          bindingSha256: row.binding_sha256,
          recordedAt: row.recorded_at,
        },
        { onExcessProperty: "error" },
      );
      const { bindingSha256, recordedAt: _recordedAt, ...subject } = binding;
      const command = binding.task.canonicalCommand;
      const original = yield* readThreadDeletionCommandEffect(binding.deletion.commandId);
      const request = deletionWorktreeCleanupRequestV1(binding);
      const originalRequest =
        original?.inventory.request ??
        (original?.command.worktreeRemoval === undefined
          ? null
          : { origin: "explicit", consent: original.command.worktreeRemoval });
      const effect = Option.getOrNull(yield* readCleanupSnapshot(effectId));
      const receipt = Option.getOrNull(
        yield* commandReceipts.getByCommandId(binding.deletion.commandId),
      );
      const events = yield* sql<{
        readonly payload_json: string;
      }>`SELECT payload_json FROM orchestration_events
        WHERE event_id = ${binding.deletion.eventId} AND sequence = ${binding.deletion.sequence}
          AND application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${binding.threadId}
          AND event_type = 'thread.deleted' AND command_id = ${binding.deletion.commandId}`;
      if (
        original === null ||
        nativeCreationCanonicalJson(original.command) !== nativeCreationCanonicalJson(command) ||
        nativeCreationCanonicalJson(original.ownerBirth) !==
          nativeCreationCanonicalJson(binding.ownerBirth) ||
        nativeCreationCanonicalJson(original.inventory.leaseInventory) !==
          nativeCreationCanonicalJson(binding.leaseInventory) ||
        nativeCreationCanonicalJson(original.deletion) !==
          nativeCreationCanonicalJson(binding.deletion) ||
        bindingSha256 !== deletionWorktreeTaskBindingDigestV1(subject) ||
        command.type !== "thread.delete" ||
        command.commandId !== binding.deletion.commandId ||
        command.threadId !== binding.threadId ||
        request === null ||
        nativeCreationCanonicalJson(request) !== nativeCreationCanonicalJson(originalRequest) ||
        (request.origin === "explicit"
          ? command.worktreeRemoval === undefined ||
            nativeCreationCanonicalJson(command.worktreeRemoval) !==
              nativeCreationCanonicalJson(request.consent) ||
            (binding.task.consent !== undefined &&
              nativeCreationCanonicalJson(binding.task.consent) !==
                nativeCreationCanonicalJson(request.consent))
          : command.worktreeRemoval !== undefined || binding.task.consent !== undefined) ||
        nativeCreationSha256(
          nativeCreationCanonicalJson(
            yield* Schema.encodeEffect(OrchestrationV2Command)(command).pipe(Effect.orDie),
          ),
        ) !== binding.task.commandDigest ||
        effect === null ||
        effect.id !== deletionWorktreeEffectIdV1(command.commandId, binding.threadId) ||
        effect.commandId !== command.commandId ||
        effect.threadId !== binding.threadId ||
        effect.request.type !== "worktree.cleanup" ||
        receipt?.status !== "accepted" ||
        receipt.commandType !== "thread.delete" ||
        receipt.threadId !== binding.threadId ||
        receipt.resultSequence < binding.deletion.sequence ||
        events.length !== 1
      )
        return yield* new EventSinkWriteError({
          eventCount: 0,
          cause: "Worktree cleanup lost its original command/deletion/task association",
        });
      if (
        yield* EventSinkJsonCodec.decodeCleanupDeletionMismatch(
          events[0]!.payload_json,
          {
            threadId: binding.threadId,
            projectId: binding.task.worktree.projectId,
            branch: binding.task.worktree.branch,
            projectRoot: binding.task.projectRoot,
            path: binding.task.worktree.path,
          },
          NodePath.resolve,
        ).pipe(Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))))
      )
        return yield* new EventSinkWriteError({
          eventCount: 0,
          cause: "Worktree cleanup differs from its original application path",
        });
      if (binding.ownerBirth !== null) {
        const births =
          yield* sql`SELECT event_id FROM orchestration_events WHERE event_id = ${binding.ownerBirth.eventId}
          AND sequence = ${binding.ownerBirth.sequence} AND application_event_version = 2 AND aggregate_kind = 'thread'
          AND stream_id = ${binding.threadId} AND event_type = 'thread.created' AND sequence < ${binding.deletion.sequence}
          AND json_extract(payload_json, '$.id') = ${binding.threadId}
          AND json_extract(payload_json, '$.projectId') = ${binding.task.worktree.projectId}
          AND NOT EXISTS (SELECT 1 FROM orchestration_events next WHERE next.application_event_version = 2
            AND next.aggregate_kind = 'thread' AND next.stream_id = ${binding.threadId} AND next.event_type = 'thread.created'
            AND next.sequence > ${binding.ownerBirth.sequence} AND next.sequence <= ${binding.deletion.sequence})`;
        if (binding.ownerBirth.threadId !== binding.threadId || births.length !== 1)
          return yield* new EventSinkWriteError({
            eventCount: 0,
            cause: "Worktree cleanup lost its original application birth",
          });
      } else if (binding.task.captureStatus !== "retained")
        return yield* new EventSinkWriteError({
          eventCount: 0,
          cause: "An unavailable application birth cannot authorize worktree cleanup",
        });
      if (
        binding.leaseInventory.status === "original" &&
        (binding.ownerBirth === null ||
          binding.leaseInventory.lease.ownerThreadId !== binding.threadId ||
          binding.leaseInventory.lease.resourcePath !== binding.task.worktree.path ||
          binding.leaseInventory.lease.ownerIncarnation !==
            (yield* EventSinkJsonCodec.encodeBirthTupleJson([
              "t3.orchestration-v2.thread-birth/v1",
              binding.ownerBirth.eventId,
              binding.ownerBirth.sequence,
            ]).pipe(Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))))))
      )
        return yield* new EventSinkWriteError({
          eventCount: 0,
          cause: "Worktree cleanup lost its full original lease",
        });
      return binding;
    });
    const qualifyDeletionCleanupObservation = Effect.fnUntraced(function* (
      binding: DeletionWorktreeTaskBindingV1 | DeletionCleanupTaskBindingV1,
      observation: DeletionCleanupObservationV1,
    ) {
      let result: LeaseCleanupTaskOutcomeV2 = {
        taskId: binding.effectId,
        result: null,
        effect: "unknown",
      };
      if ("start" in observation) {
        if (binding.task.kind !== "worktree")
          return yield* new EventSinkWriteError({
            eventCount: 0,
            cause: "Git observation belongs to a different cleanup task",
          });
        const worktreeBinding = yield* Schema.decodeUnknownEffect(DeletionWorktreeTaskBindingV1)(
          binding,
          { onExcessProperty: "error" },
        );
        const rows = yield* sql<{
          readonly ordinal: number;
          readonly correlation_json: string;
        }>`SELECT ordinal, correlation_json
          FROM orchestration_v2_lease_cleanup_task_outcomes WHERE effect_id = ${binding.effectId}
          AND json_extract(correlation_json, '$.evidence.schema') = 't3.deletion-worktree-removal-start/v1'`;
        const admission = yield* sql<{
          readonly state: string;
          readonly started_at: string | null;
          readonly subject_json: string;
        }>`
          SELECT state, started_at, subject_json FROM orchestration_v2_worktree_path_admissions WHERE operation_id = ${binding.effectId}`;
        const start = observation.start;
        if (
          rows.length !== 1 ||
          rows[0]!.ordinal !== observation.startOrdinal ||
          admission.length !== 1 ||
          !["started", "unknown", "completed", "released"].includes(admission[0]!.state) ||
          admission[0]!.started_at !== start.startedAt ||
          nativeCreationCanonicalJson(
            yield* EventSinkJsonCodec.decodeCorrelationEvidence(rows[0]!.correlation_json).pipe(
              Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
            ),
          ) !== nativeCreationCanonicalJson(start) ||
          (yield* EventSinkJsonCodec.decodeBindingSha256(admission[0]!.subject_json).pipe(
            Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
          )) !== binding.bindingSha256 ||
          start.effectId !== binding.effectId ||
          start.bindingSha256 !== binding.bindingSha256 ||
          nativeCreationCanonicalJson(start.target) !==
            nativeCreationCanonicalJson(deletionWorktreeRemovalTargetV1(worktreeBinding)) ||
          !Number.isFinite(Date.parse(observation.observedAt)) ||
          Date.parse(observation.observedAt) < Date.parse(start.startedAt)
        )
          return yield* new EventSinkWriteError({
            eventCount: 0,
            cause: "Git observation lost its persisted original removal start",
          });
        const target = start.target;
        for (const readback of [observation.before, observation.after]) {
          if (
            readback.filesystem.path !== target.path ||
            (readback.registration.status === "complete" &&
              (readback.registration.projectRoot !== target.projectRoot ||
                !NodePath.isAbsolute(readback.registration.gitCommonDirectory) ||
                readback.registration.entries.some(
                  (entry) =>
                    !NodePath.isAbsolute(entry.path) || NodePath.resolve(entry.path) !== entry.path,
                ) ||
                new Set(readback.registration.entries.map((entry) => entry.path)).size !==
                  readback.registration.entries.length))
          )
            return yield* new EventSinkWriteError({
              eventCount: 0,
              cause: "Git readback differs from the original canonical target",
            });
        }
        const before = observation.before.registration;
        const after = observation.after.registration;
        const absence =
          before.status === "complete" &&
          after.status === "complete" &&
          before.gitCommonDirectory === after.gitCommonDirectory &&
          observation.after.filesystem.status === "absent" &&
          !after.entries.some((entry) => entry.path === target.path);
        const entry =
          before.status === "complete"
            ? before.entries.find((candidate) => candidate.path === target.path)
            : undefined;
        const registeredTarget =
          entry !== undefined &&
          !entry.bare &&
          target.path !== target.projectRoot &&
          (entry.branch === target.branch ||
            entry.branch === (target.branch === null ? null : `refs/heads/${target.branch}`));
        if (
          absence &&
          observation.operation.kind === "executed" &&
          observation.operation.completion === "exited" &&
          observation.operation.exitCode === 0 &&
          registeredTarget &&
          observation.before.filesystem.status === "present"
        )
          result = { taskId: binding.effectId, result: "succeeded", effect: "confirmed" };
        else if (
          absence &&
          (observation.operation.kind === "reconciled" ||
            (observation.operation.kind === "already_absent" &&
              entry === undefined &&
              observation.before.filesystem.status === "absent"))
        )
          result = { taskId: binding.effectId, result: "succeeded", effect: "absent" };
        return {
          outcome: result,
          workerId: start.workerId,
          expectedAttempt: start.expectedAttempt,
          producer: "worktree" as const,
        };
      }
      if (observation.kind === "managed_provider") {
        const operation = observation.nativeOperation;
        if (
          binding.task.kind !== "provider" ||
          observation.effectId !== binding.effectId ||
          observation.bindingSha256 !== binding.bindingSha256 ||
          observation.evidenceRevision !== binding.task.evidenceRevision ||
          nativeCreationCanonicalJson(observation.binding) !==
            nativeCreationCanonicalJson(binding.task.expectedBinding) ||
          operation.operationId !== binding.effectId ||
          operation.operation !== "close_session" ||
          operation.threadId !== binding.threadId ||
          operation.providerThreadId !== observation.binding.providerThreadId ||
          operation.providerSessionId !== observation.binding.providerSessionId ||
          operation.instanceId !== observation.binding.instanceId ||
          operation.runtimeGeneration !== observation.binding.runtimeGeneration ||
          !Number.isFinite(Date.parse(observation.observedAt)) ||
          Date.parse(observation.observedAt) < Date.parse(binding.recordedAt)
        )
          return yield* new EventSinkWriteError({
            eventCount: 0,
            cause:
              "Managed provider observation differs from its original operation and pinned task",
          });
        if (observation.result.status === "stopped") {
          if (
            observation.result.operationId !== binding.effectId ||
            observation.result.readback.threadAttached !== false ||
            nativeCreationCanonicalJson(observation.result.binding) !==
              nativeCreationCanonicalJson(binding.task.expectedBinding) ||
            new Set(observation.result.interruptedProviderTurnIds).size !==
              observation.result.interruptedProviderTurnIds.length
          )
            return yield* new EventSinkWriteError({
              eventCount: 0,
              cause: "Managed provider stop readback belongs to a different captured runtime",
            });
          result = { taskId: binding.effectId, result: "succeeded", effect: "confirmed" };
        }
        return {
          outcome: result,
          workerId: observation.workerId,
          expectedAttempt: observation.expectedAttempt,
          producer: "managed_provider" as const,
        };
      }
      if (
        binding.task.kind !== "terminal" ||
        observation.effectId !== binding.effectId ||
        observation.bindingSha256 !== binding.bindingSha256 ||
        nativeCreationCanonicalJson(observation.capture) !==
          nativeCreationCanonicalJson(binding.task.capture) ||
        !Number.isFinite(Date.parse(observation.observedAt)) ||
        Date.parse(observation.observedAt) < Date.parse(binding.recordedAt)
      )
        return yield* new EventSinkWriteError({
          eventCount: 0,
          cause: "Managed terminal observation differs from its issued owner capture",
        });
      if (observation.result.status === "closed" && observation.result.processExitObserved)
        result = { taskId: binding.effectId, result: "succeeded", effect: "confirmed" };
      else if (observation.result.status === "observed_absent")
        result = { taskId: binding.effectId, result: "succeeded", effect: "absent" };
      return {
        outcome: result,
        workerId: observation.workerId,
        expectedAttempt: observation.expectedAttempt,
        producer: "managed_terminal" as const,
      };
    });
    const cleanupHoldMatchesObservation = (
      binding: DeletionWorktreeTaskBindingV1 | DeletionCleanupTaskBindingV1,
      observation: DeletionCleanupObservationV1,
      hold: EffectOutbox.UnknownEffectHoldV2,
    ) => {
      if (!("start" in observation) && observation.kind === "managed_provider") {
        if (
          binding.task.kind !== "provider" ||
          !("operation" in hold.evidence) ||
          hold.evidence.outcome !== "unknown"
        )
          return false;
        const { outcome: _outcome, ...operation } = hold.evidence;
        return (
          nativeCreationCanonicalJson(operation) ===
          nativeCreationCanonicalJson(
            Schema.encodeSync(ProviderNativeOperationContext)(observation.nativeOperation),
          )
        );
      }
      return (
        "kind" in hold.evidence &&
        hold.evidence.kind === "resource_cleanup" &&
        hold.evidence.bindingSha256 === binding.bindingSha256 &&
        hold.evidence.taskKind === binding.task.kind
      );
    };
    const readLeaseCleanupTaskEffect = Effect.fnUntraced(function* (effectId: string) {
      const rows = yield* sql<{
        readonly thread_id: string;
        readonly lease_json: string;
        readonly owner_birth_json: string;
        readonly deletion_json: string;
        readonly task_json: string;
        readonly binding_sha256: string;
        readonly recorded_at: string;
      }>`
        SELECT * FROM orchestration_v2_lease_cleanup_task_bindings WHERE effect_id = ${effectId}`;
      if (rows.length === 0) return null;
      if (rows.length !== 1)
        return yield* new EventSinkWriteError({
          eventCount: 0,
          cause: "Cleanup task binding is ambiguous",
        });
      const row = rows[0]!;
      if (
        (yield* EventSinkJsonCodec.decodeTaskKind(row.task_json).pipe(
          Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
        )) === "worktree"
      )
        return null;
      if (
        (yield* EventSinkJsonCodec.decodeLeaseStatus(row.lease_json).pipe(
          Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
        )) === "absent"
      )
        return null;
      const binding = yield* Schema.decodeUnknownEffect(LeaseCleanupTaskBindingV2)(
        {
          version: 2,
          effectId,
          threadId: row.thread_id,
          lease: yield* EventSinkJsonCodec.decodeJson(row.lease_json).pipe(
            Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
          ),
          ownerBirth: yield* EventSinkJsonCodec.decodeJson(row.owner_birth_json).pipe(
            Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
          ),
          deletion: yield* EventSinkJsonCodec.decodeJson(row.deletion_json).pipe(
            Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
          ),
          task: yield* EventSinkJsonCodec.decodeJson(row.task_json).pipe(
            Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
          ),
          bindingSha256: row.binding_sha256,
          recordedAt: row.recorded_at,
        },
        { onExcessProperty: "error" },
      );
      const { bindingSha256, recordedAt: _recordedAt, ...subject } = binding;
      const effect = Option.getOrNull(yield* readCleanupSnapshot(effectId));
      const birth =
        yield* sql`SELECT event_id FROM orchestration_events WHERE event_id = ${binding.ownerBirth.eventId}
        AND sequence = ${binding.ownerBirth.sequence} AND application_event_version = 2 AND aggregate_kind = 'thread'
        AND stream_id = ${binding.threadId} AND event_type = 'thread.created' AND json_extract(payload_json, '$.id') = ${binding.threadId}`;
      const deletion =
        yield* sql`SELECT event_id FROM orchestration_events WHERE event_id = ${binding.deletion.eventId}
        AND sequence = ${binding.deletion.sequence} AND command_id = ${binding.deletion.commandId} AND application_event_version = 2
        AND aggregate_kind = 'thread' AND stream_id = ${binding.threadId} AND event_type = 'thread.deleted'
        AND sequence > ${binding.ownerBirth.sequence} AND json_extract(payload_json, '$.id') = ${binding.threadId}`;
      const receipt = Option.getOrNull(
        yield* commandReceipts.getByCommandId(binding.deletion.commandId),
      );
      if (
        bindingSha256 !== leaseCleanupTaskBindingDigestV2(subject) ||
        binding.lease.ownerThreadId !== binding.threadId ||
        binding.ownerBirth.threadId !== binding.threadId ||
        binding.lease.ownerIncarnation !==
          (yield* EventSinkJsonCodec.encodeBirthTupleJson([
            "t3.orchestration-v2.thread-birth/v1",
            binding.ownerBirth.eventId,
            binding.ownerBirth.sequence,
          ]).pipe(Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))))) ||
        birth.length !== 1 ||
        deletion.length !== 1 ||
        receipt?.status !== "accepted" ||
        effect === null ||
        effect.threadId !== binding.threadId ||
        effect.commandId !== binding.deletion.commandId
      )
        return yield* new EventSinkWriteError({
          eventCount: 0,
          cause: "Cleanup task lost its exact historical lease/effect/deletion association",
        });
      const task = binding.task;
      if (
        (task.kind === "provider" &&
          (effect.request.type !== "provider-session.detach" ||
            effect.request.providerSessionId !== task.expectedBinding.providerSessionId ||
            task.expectedBinding.threadId !== binding.threadId)) ||
        (task.kind === "terminal" &&
          (effect.request.type !== "terminal.cleanup" ||
            task.capture.threadId !== binding.threadId ||
            nativeCreationCanonicalJson(task.capture.ownerBirth) !==
              nativeCreationCanonicalJson(binding.ownerBirth) ||
            task.capture.targets.some(
              (target) =>
                target.threadId !== binding.threadId ||
                nativeCreationCanonicalJson(target.ownerBirth) !==
                  nativeCreationCanonicalJson(binding.ownerBirth),
            ))) ||
        (task.kind === "attachment" &&
          (effect.request.type !== "attachment.cleanup" ||
            nativeCreationCanonicalJson(effect.request.attachmentIds) !==
              nativeCreationCanonicalJson(task.attachmentIds)))
      )
        return yield* new EventSinkWriteError({
          eventCount: 0,
          cause: "Cleanup task differs from its pinned target",
        });
      return binding;
    });
    const readDeletionCleanupTaskEffect = Effect.fnUntraced(function* (effectId: string) {
      const rows = yield* sql<{
        readonly thread_id: string;
        readonly lease_json: string;
        readonly owner_birth_json: string;
        readonly deletion_json: string;
        readonly task_json: string;
        readonly binding_sha256: string;
        readonly recorded_at: string;
      }>`
        SELECT * FROM orchestration_v2_lease_cleanup_task_bindings WHERE effect_id = ${effectId}`;
      if (rows.length === 0) return null;
      if (rows.length !== 1)
        return yield* new EventSinkWriteError({
          eventCount: 0,
          cause: "Deletion cleanup task is ambiguous",
        });
      const row = rows[0]!;
      if (
        (yield* EventSinkJsonCodec.decodeTaskKind(row.task_json).pipe(
          Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
        )) === "worktree"
      )
        return null;
      const inventory = yield* EventSinkJsonCodec.decodeLeaseWithStatus(row.lease_json).pipe(
        Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
      );
      if (inventory.status !== "absent") return yield* readLeaseCleanupTaskEffect(effectId);
      const binding = yield* Schema.decodeUnknownEffect(UnleasedDeletionCleanupTaskBindingV1)(
        {
          version: 1,
          effectId,
          threadId: row.thread_id,
          leaseInventory: inventory.value,
          ownerBirth: yield* EventSinkJsonCodec.decodeJson(row.owner_birth_json).pipe(
            Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
          ),
          deletion: yield* EventSinkJsonCodec.decodeJson(row.deletion_json).pipe(
            Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
          ),
          task: yield* EventSinkJsonCodec.decodeJson(row.task_json).pipe(
            Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))),
          ),
          bindingSha256: row.binding_sha256,
          recordedAt: row.recorded_at,
        },
        { onExcessProperty: "error" },
      );
      const { bindingSha256, recordedAt: _recordedAt, ...subject } = binding;
      const original = yield* readThreadDeletionCommandEffect(binding.deletion.commandId);
      const effect = Option.getOrNull(yield* readCleanupSnapshot(effectId));
      if (
        bindingSha256 !== deletionCleanupTaskBindingDigestV1(subject) ||
        original === null ||
        original.inventory.captureStatus !== "captured" ||
        original.inventory.leaseInventory.status !== "absent" ||
        original.inventory.worktree.path !== binding.leaseInventory.resourcePath ||
        original.command.threadId !== binding.threadId ||
        nativeCreationCanonicalJson(original.ownerBirth) !==
          nativeCreationCanonicalJson(binding.ownerBirth) ||
        nativeCreationCanonicalJson(original.deletion) !==
          nativeCreationCanonicalJson(binding.deletion) ||
        effect === null ||
        effect.threadId !== binding.threadId ||
        effect.commandId !== binding.deletion.commandId ||
        !original.inventory.prerequisiteEffectIds.includes(effectId)
      )
        return yield* new EventSinkWriteError({
          eventCount: 0,
          cause: "Cleanup absence was not captured with the original deletion",
        });
      const task = binding.task;
      if (
        (task.kind === "provider" &&
          (effect.request.type !== "provider-session.detach" ||
            effect.request.providerSessionId !== task.expectedBinding.providerSessionId ||
            task.expectedBinding.threadId !== binding.threadId)) ||
        (task.kind === "terminal" &&
          (effect.request.type !== "terminal.cleanup" ||
            task.capture.threadId !== binding.threadId ||
            nativeCreationCanonicalJson(task.capture.ownerBirth) !==
              nativeCreationCanonicalJson(binding.ownerBirth) ||
            task.capture.targets.some(
              (target) =>
                target.threadId !== binding.threadId ||
                nativeCreationCanonicalJson(target.ownerBirth) !==
                  nativeCreationCanonicalJson(binding.ownerBirth),
            ))) ||
        (task.kind === "attachment" &&
          (effect.request.type !== "attachment.cleanup" ||
            nativeCreationCanonicalJson(effect.request.attachmentIds) !==
              nativeCreationCanonicalJson(task.attachmentIds)))
      )
        return yield* new EventSinkWriteError({
          eventCount: 0,
          cause: "Unleased cleanup task differs from its original target",
        });
      return binding;
    });
    const readApplicationBirthRecordEffect = Effect.fnUntraced(function* (threadId: ThreadId) {
      const births = yield* sql<{
        readonly event_id: string;
        readonly sequence: number;
        readonly payload_json: string;
      }>`
        SELECT event_id, sequence, payload_json FROM orchestration_events WHERE application_event_version = 2
          AND aggregate_kind = 'thread' AND stream_id = ${threadId} AND event_type = 'thread.created'
        ORDER BY sequence DESC LIMIT 1`;
      if (births.length === 0) return null;
      const current = yield* sql<{
        readonly payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_projection_threads
        WHERE thread_id = ${threadId} AND json_extract(payload_json, '$.deletedAt') IS NULL`;
      if (current.length !== 1) return null;
      const identity = Schema.fromJsonString(
        Schema.Struct({ id: ThreadId, projectId: ProjectId, createdAt: Schema.String }),
      );
      const birth = yield* Schema.decodeUnknownEffect(identity)(births[0]!.payload_json);
      const projection = yield* Schema.decodeUnknownEffect(identity)(current[0]!.payload_json);
      if (
        birth.id !== threadId ||
        projection.id !== threadId ||
        birth.projectId !== projection.projectId ||
        birth.createdAt !== projection.createdAt
      )
        return null;
      return {
        kind: "application_v2_thread_birth",
        threadId,
        eventId: EventId.make(births[0]!.event_id),
        sequence: births[0]!.sequence,
      } satisfies ImportedAttachments.ImportedApplicationAttachmentBirthV1;
    });
    const importedApplicationInventoryApplicable = Effect.gen(function* () {
      for (const required of [
        [142, "V2NativeAcceptance"],
        [144, "ImportedApplicationAttachments"],
      ] as const)
        if (
          !(yield* hasOwnJonesMigration(jonesMigrationEntries, required).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
            Effect.catch(() => Effect.succeed(false)),
          ))
        )
          return false;
      return true;
    });
    const readImportedApplicationBirthEffect = Effect.fnUntraced(function* (input: {
      readonly threadId: ThreadId;
      readonly expectedBirth: ImportedAttachments.ImportedApplicationAttachmentBirthV1;
    }) {
      if (
        !Schema.is(ImportedAttachments.ImportedApplicationAttachmentBirthV1)(input.expectedBirth) ||
        input.expectedBirth.threadId !== input.threadId
      )
        return null;
      const rows = yield* sql<{
        readonly payload_json: string;
        readonly command_id: string | null;
      }>`
        SELECT payload_json, command_id FROM orchestration_events WHERE event_id = ${input.expectedBirth.eventId}
          AND sequence = ${input.expectedBirth.sequence} AND application_event_version = 2 AND aggregate_kind = 'thread'
          AND stream_id = ${input.threadId} AND event_type = 'thread.created'`;
      if (rows.length !== 1 || rows[0]!.command_id !== null) return null;
      const value = Schema.decodeUnknownOption(Schema.fromJsonString(OrchestrationV2AppThreadJson))(
        rows[0]!.payload_json,
      );
      return Option.isSome(value) &&
        value.value.id === input.threadId &&
        value.value.historyOrigin === "v1_import"
        ? value.value
        : null;
    });
    const readImportedApplicationAttachmentInventoryEffect = Effect.fnUntraced(function* (
      input: ImportedApplicationAttachmentInventoryReadInput,
    ) {
      const unavailable = (
        reason: string,
      ): ImportedAttachments.ImportedApplicationAttachmentQualificationV1 => ({
        status: "unavailable",
        reason,
      });
      if (!(yield* importedApplicationInventoryApplicable))
        return unavailable("imported_application_inventory_schema_unavailable");
      const birth = yield* readImportedApplicationBirthEffect(input);
      if (birth === null) return unavailable("imported_application_birth_unavailable");
      const rows = yield* sql<{
        readonly inventory_id: string;
        readonly thread_id: string;
        readonly project_id: string;
        readonly application_birth_event_id: string;
        readonly application_birth_sequence: number;
        readonly adoption_ordinal: number;
        readonly source_kind: string;
        readonly canonical_header_json: string;
        readonly canonical_source_json: string;
        readonly message_carrier_count: number;
        readonly answer_carrier_count: number;
        readonly attachment_reference_count: number;
        readonly carrier_set_sha256: string;
        readonly recorded_at: string;
      }>`
        SELECT * FROM orchestration_v2_imported_application_attachment_inventories
        WHERE thread_id = ${input.threadId} AND application_birth_event_id = ${input.expectedBirth.eventId}
          AND application_birth_sequence = ${input.expectedBirth.sequence}
          AND (${input.inventoryId ?? null} IS NULL OR inventory_id = ${input.inventoryId ?? null}) ORDER BY adoption_ordinal DESC`;
      if (rows.length === 0) return unavailable("imported_application_inventory_not_found");
      for (const row of rows) {
        const header = Schema.decodeUnknownOption(
          Schema.fromJsonString(ImportedAttachments.ImportedApplicationAttachmentInventoryV1),
        )(row.canonical_header_json);
        if (Option.isNone(header)) return unavailable("inventory_decode_unavailable");
        const value = header.value;
        if (value.source.kind === "native_import_batch")
          return unavailable("native_import_transcript_seal_producer_unavailable");
        const carrierRows = yield* sql<{
          readonly carrier_kind: string;
          readonly carrier_id: string;
          readonly canonical_carrier_json: string;
          readonly source_row_sha256: string;
        }>`SELECT carrier_kind, carrier_id, canonical_carrier_json, source_row_sha256
          FROM orchestration_v2_imported_application_attachment_carriers WHERE inventory_id = ${row.inventory_id} ORDER BY carrier_kind, carrier_id`;
        const carriers: Array<ImportedAttachments.ImportedApplicationAttachmentCarrierV1> = [];
        for (const carrierRow of carrierRows) {
          const carrier = Schema.decodeUnknownOption(
            Schema.fromJsonString(ImportedAttachments.ImportedApplicationAttachmentCarrierV1),
          )(carrierRow.canonical_carrier_json);
          if (
            Option.isNone(carrier) ||
            carrier.value.kind !== carrierRow.carrier_kind ||
            ImportedAttachments.importedApplicationAttachmentCarrierIdV1(carrier.value) !==
              carrierRow.carrier_id ||
            carrier.value.sourceRowSha256 !== carrierRow.source_row_sha256
          )
            return unavailable("inventory_carrier_row_parity_unavailable");
          carriers.push(carrier.value);
        }
        if (
          value.inventoryId !== row.inventory_id ||
          value.projectId !== birth.projectId ||
          value.projectId !== row.project_id ||
          nativeCreationCanonicalJson(value.applicationBirth) !==
            nativeCreationCanonicalJson(input.expectedBirth) ||
          row.source_kind !== value.source.kind ||
          nativeCreationCanonicalJson(value.source) !== row.canonical_source_json ||
          value.messageCarrierCount !== row.message_carrier_count ||
          value.answerCarrierCount !== row.answer_carrier_count ||
          value.attachmentReferenceCount !== row.attachment_reference_count ||
          value.carrierSetSha256 !== row.carrier_set_sha256 ||
          value.recordedAt !== row.recorded_at
        )
          return unavailable("inventory_header_row_parity_unavailable");
        return ImportedAttachments.qualifyImportedApplicationAttachmentSnapshotV1({
          header: value,
          carriers,
        });
      }
      return unavailable("imported_application_inventory_not_found");
    });
    const collectImportedApplicationAttachmentInventoryEffect = Effect.fnUntraced(function* (
      input: ImportedApplicationAttachmentInventoryInput,
    ) {
      const unavailable = (
        reason: string,
      ): ImportedAttachments.ImportedApplicationAttachmentQualificationV1 => ({
        status: "unavailable",
        reason,
      });
      if (!(yield* importedApplicationInventoryApplicable))
        return unavailable("imported_application_inventory_schema_unavailable");
      const birth = yield* readImportedApplicationBirthEffect(input);
      if (
        birth === null ||
        nativeCreationCanonicalJson(yield* readApplicationBirthRecordEffect(input.threadId)) !==
          nativeCreationCanonicalJson(input.expectedBirth)
      )
        return unavailable("imported_application_birth_unavailable");
      if (input.expectedBirth.eventId !== `migration:v1:thread:${input.threadId}:created`)
        return unavailable("native_import_transcript_seal_producer_unavailable");
      const carriers: Array<ImportedAttachments.ImportedApplicationAttachmentCarrierV1> = [];
      let source: ImportedAttachments.ImportedApplicationAttachmentSourceV1;
      {
        const sourceRows = yield* sql<{
          readonly project_id: string;
          readonly created_at: string;
          readonly deleted_at: string | null;
        }>`
          SELECT project_id, created_at, deleted_at FROM projection_threads WHERE thread_id = ${input.threadId}`;
        const legacyBirths = yield* sql<{
          readonly event_id: string;
          readonly sequence: number;
          readonly payload_json: string;
        }>`
          SELECT event_id, sequence, payload_json FROM orchestration_events WHERE application_event_version = 1 AND aggregate_kind = 'thread'
            AND stream_id = ${input.threadId} AND event_type = 'thread.created' ORDER BY sequence DESC LIMIT 1`;
        if (
          sourceRows.length !== 1 ||
          legacyBirths.length !== 1 ||
          sourceRows[0]!.deleted_at !== null ||
          sourceRows[0]!.project_id !== birth.projectId ||
          legacyBirths[0]!.sequence >= input.expectedBirth.sequence ||
          Date.parse(sourceRows[0]!.created_at) !== DateTime.toEpochMillis(birth.createdAt)
        )
          return unavailable("legacy_application_source_birth_unavailable");
        const legacyBirth = Schema.decodeUnknownOption(
          Schema.fromJsonString(
            Schema.Struct({ threadId: ThreadId, projectId: ProjectId, createdAt: Schema.String }),
          ),
        )(legacyBirths[0]!.payload_json);
        if (
          Option.isNone(legacyBirth) ||
          legacyBirth.value.threadId !== input.threadId ||
          legacyBirth.value.projectId !== birth.projectId ||
          legacyBirth.value.createdAt !== sourceRows[0]!.created_at ||
          input.expectedBirth.eventId !== `migration:v1:thread:${input.threadId}:created`
        )
          return unavailable("legacy_application_source_birth_unavailable");
        const boundary = yield* sql<{
          readonly sequence: number;
        }>`SELECT coalesce(max(sequence), 0) AS sequence FROM orchestration_events WHERE application_event_version = 1`;
        const positions = yield* sql<{
          readonly projector: string;
          readonly last_applied_sequence: number;
        }>`SELECT projector, last_applied_sequence FROM projection_state
          WHERE projector IN ('projection.threads', 'projection.thread-messages', 'projection.thread-activities', 'projection.thread-turns')`;
        const position = (name: string) =>
          positions.find((item) => item.projector === name)?.last_applied_sequence;
        const threads = position("projection.threads"),
          messages = position("projection.thread-messages"),
          activities = position("projection.thread-activities"),
          turns = position("projection.thread-turns");
        if (
          positions.length !== 4 ||
          threads === undefined ||
          messages === undefined ||
          activities === undefined ||
          turns === undefined ||
          [threads, messages, activities, turns].some(
            (value) => !Number.isSafeInteger(value) || value < boundary[0]!.sequence,
          )
        )
          return unavailable("legacy_application_source_cut_incomplete");
        const messageRows = yield* sql<{
          readonly message_id: string;
          readonly thread_id: string;
          readonly turn_id: string | null;
          readonly role: string;
          readonly attachments_json: string | null;
          readonly created_at: string;
          readonly updated_at: string;
        }>`
          SELECT * FROM projection_thread_messages WHERE thread_id = ${input.threadId} ORDER BY message_id`;
        const answerRows = yield* sql<{
          readonly activity_id: string;
          readonly thread_id: string;
          readonly turn_id: string | null;
          readonly sequence: number | null;
          readonly payload_json: string;
          readonly created_at: string;
        }>`
          SELECT * FROM projection_thread_activities WHERE thread_id = ${input.threadId} AND kind = 'user-input.answer-submitted' ORDER BY activity_id`;
        for (const row of messageRows) {
          const attachments =
            row.attachments_json === null
              ? Option.some([])
              : Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Array(ChatAttachment)))(
                  row.attachments_json,
                );
          if (Option.isNone(attachments)) return unavailable("carrier_decode_unavailable");
          const value = Schema.decodeUnknownOption(
            ImportedAttachments.ImportedApplicationAttachmentCarrierV1,
          )({
            kind: "legacy_message",
            messageId: row.message_id,
            sourceThreadId: row.thread_id,
            turnId: row.turn_id,
            role: row.role,
            createdAt: row.created_at,
            updatedAt: row.updated_at,
            attachmentsJson: row.attachments_json,
            attachments: attachments.value,
            sourceRowSha256: ImportedAttachments.importedApplicationAttachmentSha256V1(row),
          });
          if (Option.isNone(value)) return unavailable("carrier_decode_unavailable");
          carriers.push(value.value);
        }
        for (const row of answerRows) {
          const answer = Schema.decodeUnknownOption(
            Schema.fromJsonString(UserInputAttachmentAnswerPayload),
          )(row.payload_json);
          if (Option.isNone(answer)) return unavailable("carrier_decode_unavailable");
          const value = Schema.decodeUnknownOption(
            ImportedAttachments.ImportedApplicationAttachmentCarrierV1,
          )({
            kind: "legacy_answer",
            activityId: row.activity_id,
            sourceThreadId: row.thread_id,
            turnId: row.turn_id,
            sequence: row.sequence,
            createdAt: row.created_at,
            payloadJson: row.payload_json,
            answer: answer.value,
            sourceRowSha256: ImportedAttachments.importedApplicationAttachmentSha256V1(row),
          });
          if (Option.isNone(value)) return unavailable("carrier_decode_unavailable");
          carriers.push(value.value);
        }
        source = {
          kind: "legacy_projection",
          legacyBirth: {
            eventId: EventId.make(legacyBirths[0]!.event_id),
            sequence: legacyBirths[0]!.sequence,
          },
          projectId: birth.projectId,
          sourceCreatedAt: sourceRows[0]!.created_at,
          sourceCut: {
            legacyEventSequence: boundary[0]!.sequence,
            projectorPositions: { threads, messages, activities, turns },
            messageRowsSha256:
              ImportedAttachments.importedApplicationAttachmentSha256V1(messageRows),
            answerRowsSha256: ImportedAttachments.importedApplicationAttachmentSha256V1(answerRows),
          },
        };
      }
      const paths = ImportedAttachments.collectImportedApplicationAttachmentPathsV1(carriers);
      if (paths.status !== "complete") return paths;
      const identity = {
        version: 1 as const,
        domain: "jones_materialized_attachment_references/v1" as const,
        applicationBirth: input.expectedBirth,
        projectId: birth.projectId,
        source,
        sourceHistoryCoverage:
          source.kind === "legacy_projection"
            ? ("legacy_materialized_projection" as const)
            : ("native_visible_message_subset" as const),
        completeness: "complete_application_refs" as const,
        messageCarrierCount: paths.messageCarrierCount,
        answerCarrierCount: paths.answerCarrierCount,
        attachmentReferenceCount: paths.attachmentReferenceCount,
        carrierSetSha256: paths.carrierSetSha256,
      };
      const latest = yield* readImportedApplicationAttachmentInventoryEffect(input);
      if (
        latest.status === "complete" &&
        source.kind === "legacy_projection" &&
        latest.inventory.header.source.kind === "legacy_projection"
      ) {
        const original = latest.inventory.header.source;
        if (
          nativeCreationCanonicalJson(original.legacyBirth) ===
            nativeCreationCanonicalJson(source.legacyBirth) &&
          original.projectId === source.projectId &&
          original.sourceCreatedAt === source.sourceCreatedAt &&
          original.sourceCut.messageRowsSha256 === source.sourceCut.messageRowsSha256 &&
          original.sourceCut.answerRowsSha256 === source.sourceCut.answerRowsSha256 &&
          latest.inventory.header.carrierSetSha256 === paths.carrierSetSha256
        )
          return latest;
      }
      if (
        latest.status === "unavailable" &&
        latest.reason !== "imported_application_inventory_not_found"
      )
        return latest;
      const inventoryId =
        ImportedAttachments.makeImportedApplicationAttachmentInventoryIdV1(identity);
      const prior =
        yield* sql`SELECT inventory_id FROM orchestration_v2_imported_application_attachment_inventories WHERE inventory_id = ${inventoryId}`;
      if (prior.length !== 0)
        return yield* readImportedApplicationAttachmentInventoryEffect({ ...input, inventoryId });
      const header = {
        ...identity,
        inventoryId,
        recordedAt: DateTime.formatIso(yield* DateTime.now),
      };
      const qualification = ImportedAttachments.qualifyImportedApplicationAttachmentSnapshotV1({
        header,
        carriers,
      });
      if (qualification.status !== "complete") return qualification;
      return qualification;
    });
    const prepareImportedApplicationAttachmentInventoryEffect = Effect.fnUntraced(function* (
      input: ImportedApplicationAttachmentInventoryInput,
    ) {
      const qualification = yield* collectImportedApplicationAttachmentInventoryEffect(input);
      if (qualification.status !== "complete") return qualification;
      const { header, carriers } = qualification.inventory;
      const existing =
        yield* sql`SELECT inventory_id FROM orchestration_v2_imported_application_attachment_inventories WHERE inventory_id = ${header.inventoryId}`;
      if (existing.length !== 0)
        return yield* readImportedApplicationAttachmentInventoryEffect({
          ...input,
          inventoryId: header.inventoryId,
        });
      const ordinal = yield* sql<{
        readonly ordinal: number;
      }>`SELECT coalesce(max(adoption_ordinal), -1) + 1 AS ordinal
        FROM orchestration_v2_imported_application_attachment_inventories WHERE thread_id = ${input.threadId}
          AND application_birth_event_id = ${input.expectedBirth.eventId} AND application_birth_sequence = ${input.expectedBirth.sequence}`;
      yield* sql`INSERT INTO orchestration_v2_imported_application_attachment_inventories
        (inventory_id, thread_id, project_id, application_birth_event_id, application_birth_sequence, adoption_ordinal, source_kind,
          canonical_header_json, canonical_source_json, message_carrier_count, answer_carrier_count, attachment_reference_count, carrier_set_sha256, recorded_at)
        VALUES (${header.inventoryId}, ${input.threadId}, ${header.projectId}, ${input.expectedBirth.eventId}, ${input.expectedBirth.sequence}, ${ordinal[0]!.ordinal}, ${header.source.kind},
          ${nativeCreationCanonicalJson(header)}, ${nativeCreationCanonicalJson(header.source)}, ${header.messageCarrierCount}, ${header.answerCarrierCount},
          ${header.attachmentReferenceCount}, ${header.carrierSetSha256}, ${header.recordedAt})`;
      for (const carrier of carriers)
        yield* sql`INSERT INTO orchestration_v2_imported_application_attachment_carriers
        (inventory_id, carrier_kind, carrier_id, canonical_carrier_json, source_row_sha256)
        VALUES (${header.inventoryId}, ${carrier.kind}, ${ImportedAttachments.importedApplicationAttachmentCarrierIdV1(carrier)},
          ${nativeCreationCanonicalJson(carrier)}, ${carrier.sourceRowSha256})`;
      return yield* readImportedApplicationAttachmentInventoryEffect({
        ...input,
        inventoryId: header.inventoryId,
      });
    });
    const readImportedBaselineCopiesEffect = Effect.fnUntraced(function* (
      inventory: ImportedAttachments.ImportedApplicationAttachmentSnapshotV1,
    ) {
      type Copies =
        ProjectionStore.ProjectionImportedApplicationAttachmentRetentionInputV1["baselineCopies"];
      const copies: Array<Copies[number]> = [];
      if (inventory.header.source.kind === "native_import_batch") return copies;
      const birth = inventory.header.applicationBirth;
      const rows = yield* sql<{
        readonly event_id: string;
        readonly sequence: number;
        readonly payload_json: string;
      }>`
        SELECT event_id, sequence, payload_json FROM orchestration_events WHERE application_event_version = 2
          AND aggregate_kind = 'thread' AND stream_id = ${birth.threadId} AND event_type = 'message.updated'
          AND command_id IS NULL AND sequence > ${birth.sequence} AND event_id LIKE 'migration:v1:message:%' ORDER BY sequence`;
      const adoptions = yield* sql<{ readonly inventory_id: string }>`SELECT inventory_id
        FROM orchestration_v2_imported_application_attachment_inventories WHERE thread_id = ${birth.threadId}
          AND application_birth_event_id = ${birth.eventId} AND application_birth_sequence = ${birth.sequence} ORDER BY adoption_ordinal`;
      const historical: Array<ImportedAttachments.ImportedApplicationAttachmentSnapshotV1> = [];
      for (const adoption of adoptions) {
        const qualified = yield* readImportedApplicationAttachmentInventoryEffect({
          threadId: birth.threadId,
          expectedBirth: birth,
          inventoryId: adoption.inventory_id,
        });
        if (qualified.status !== "complete") return null;
        historical.push(qualified.inventory);
      }
      for (const row of rows) {
        const decoded = Schema.decodeUnknownOption(
          Schema.fromJsonString(OrchestrationV2ConversationMessageJson),
        )(row.payload_json);
        if (Option.isNone(decoded)) return null;
        const message = decoded.value;
        const itemRows = yield* sql<{
          readonly event_id: string;
          readonly sequence: number;
          readonly payload_json: string;
        }>`
          SELECT event_id, sequence, payload_json FROM orchestration_events WHERE application_event_version = 2
            AND aggregate_kind = 'thread' AND stream_id = ${birth.threadId} AND event_type = 'turn-item.updated'
            AND command_id IS NULL AND event_id = ${`migration:v1:turn-item:${message.id}`} AND sequence > ${row.sequence}`;
        if (itemRows.length !== 1) return null;
        const itemRow = itemRows[0]!;
        const decodedItem = Schema.decodeUnknownOption(
          Schema.fromJsonString(OrchestrationV2TurnItemJson),
        )(itemRow.payload_json);
        if (Option.isNone(decodedItem)) return null;
        const item = decodedItem.value;
        if (
          row.event_id !== `migration:v1:message:${message.id}` ||
          message.threadId !== birth.threadId ||
          message.runId !== null ||
          message.nodeId !== null ||
          message.creationSource !== "server" ||
          message.streaming ||
          !(
            (item.type === "user_message" && message.role === "user") ||
            (item.type === "assistant_message" && message.role === "assistant")
          ) ||
          !(item.type === "user_message" || item.type === "assistant_message") ||
          item.messageId !== message.id ||
          item.id !== `migration:v1:turn-item:${message.id}` ||
          item.threadId !== birth.threadId ||
          item.runId !== null ||
          item.nodeId !== null ||
          item.providerThreadId !== null ||
          item.providerTurnId !== null ||
          item.nativeItemRef !== null ||
          item.parentItemId !== null ||
          item.text !== message.text ||
          item.startedAt === null ||
          DateTime.toEpochMillis(item.startedAt) !== DateTime.toEpochMillis(message.createdAt) ||
          DateTime.toEpochMillis(item.updatedAt) !== DateTime.toEpochMillis(message.updatedAt) ||
          nativeCreationCanonicalJson("context" in item ? (item.context ?? null) : null) !==
            nativeCreationCanonicalJson(message.context ?? null) ||
          (item.type === "user_message" &&
            nativeCreationCanonicalJson(item.attachments) !==
              nativeCreationCanonicalJson(message.attachments))
        )
          return null;
        const carrier = historical
          .flatMap((snapshot) => snapshot.carriers)
          .find(
            (value) =>
              value.kind === "legacy_message" &&
              value.messageId === message.id &&
              value.sourceThreadId === birth.threadId &&
              value.role === message.role &&
              Date.parse(value.createdAt) === DateTime.toEpochMillis(message.createdAt) &&
              Date.parse(value.updatedAt) === DateTime.toEpochMillis(message.updatedAt) &&
              nativeCreationCanonicalJson(value.attachments) ===
                nativeCreationCanonicalJson(message.attachments),
          );
        if (carrier === undefined) return null;
        copies.push({
          applicationBirth: birth,
          messageId: message.id,
          itemId: item.id,
          messageEvent: { eventId: EventId.make(row.event_id), sequence: row.sequence },
          itemEvent: { eventId: EventId.make(itemRow.event_id), sequence: itemRow.sequence },
          messagePayloadSha256: ImportedAttachments.importedApplicationAttachmentSha256V1(
            yield* Schema.encodeEffect(OrchestrationV2ConversationMessageJson)(message).pipe(
              Effect.orDie,
            ),
          ),
          itemPayloadSha256: ImportedAttachments.importedApplicationAttachmentSha256V1(
            yield* Schema.encodeEffect(OrchestrationV2TurnItemJson)(item).pipe(Effect.orDie),
          ),
        });
      }
      return copies;
    });
    const readQualifiedThreadRetainedAttachmentPathsEffect = Effect.fnUntraced(function* (
      threadId: ThreadId,
    ) {
      const unavailable = (
        reason: string,
      ): ProjectionStore.ProjectionThreadRetainedAttachmentPaths => ({
        status: "unavailable",
        reason,
      });
      const readBirth = Effect.fnUntraced(function* (id: ThreadId, beforeSequence?: number) {
        const rows = yield* sql<{
          readonly event_id: string;
          readonly sequence: number;
          readonly payload_json: string;
        }>`
          SELECT event_id, sequence, payload_json FROM orchestration_events WHERE application_event_version = 2
            AND aggregate_kind = 'thread' AND stream_id = ${id} AND event_type = 'thread.created'
            AND (${beforeSequence ?? null} IS NULL OR sequence < ${beforeSequence ?? null}) ORDER BY sequence DESC LIMIT 1`;
        const latest = yield* sql<{
          readonly event_id: string;
          readonly sequence: number;
        }>`SELECT event_id, sequence FROM orchestration_events
          WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${id} AND event_type = 'thread.created' ORDER BY sequence DESC LIMIT 1`;
        const current = yield* sql<{
          readonly payload_json: string;
        }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id = ${id}`;
        if (
          rows.length !== 1 ||
          latest.length !== 1 ||
          current.length !== 1 ||
          rows[0]!.event_id !== latest[0]!.event_id ||
          rows[0]!.sequence !== latest[0]!.sequence
        )
          return null;
        const original = Schema.decodeUnknownOption(
          Schema.fromJsonString(OrchestrationV2AppThreadJson),
        )(rows[0]!.payload_json);
        const projected = Schema.decodeUnknownOption(
          Schema.fromJsonString(OrchestrationV2AppThreadJson),
        )(current[0]!.payload_json);
        if (
          Option.isNone(original) ||
          Option.isNone(projected) ||
          original.value.id !== id ||
          projected.value.id !== id ||
          original.value.projectId !== projected.value.projectId ||
          original.value.historyOrigin !== projected.value.historyOrigin ||
          DateTime.toEpochMillis(original.value.createdAt) !==
            DateTime.toEpochMillis(projected.value.createdAt)
        )
          return null;
        return {
          birth: {
            kind: "application_v2_thread_birth" as const,
            threadId: id,
            eventId: EventId.make(rows[0]!.event_id),
            sequence: rows[0]!.sequence,
          },
          thread: projected.value,
        };
      });
      const target = yield* readBirth(threadId);
      if (target === null) return unavailable("attachment_application_birth_unavailable");
      const visibleImportedBirths: Array<ImportedAttachments.ImportedApplicationAttachmentBirthV1> =
        [];
      const inventories: Array<
        ProjectionStore.ProjectionImportedApplicationAttachmentRetentionInputV1["inventories"][number]
      > = [];
      const baselineCopies: Array<
        ProjectionStore.ProjectionImportedApplicationAttachmentRetentionInputV1["baselineCopies"][number]
      > = [];
      const forkBasis: Array<ImportedAttachments.ImportedApplicationAttachmentForkBasisV1> = [];
      const seen = new Set<ThreadId>();
      let current = target;
      while (true) {
        if (seen.has(current.birth.threadId)) return unavailable("attachment_history_cycle");
        seen.add(current.birth.threadId);
        if (current.thread.historyOrigin === "v1_import") {
          const input = { threadId: current.birth.threadId, expectedBirth: current.birth };
          const persisted = yield* readImportedApplicationAttachmentInventoryEffect(input);
          if (persisted.status !== "complete") return unavailable(persisted.reason);
          const materialized = yield* collectImportedApplicationAttachmentInventoryEffect(input);
          if (materialized.status !== "complete") return unavailable(materialized.reason);
          if (
            materialized.inventory.header.inventoryId !== persisted.inventory.header.inventoryId ||
            nativeCreationCanonicalJson(materialized.inventory.carriers) !==
              nativeCreationCanonicalJson(persisted.inventory.carriers)
          )
            return unavailable("imported_application_inventory_source_changed");
          const copies = yield* readImportedBaselineCopiesEffect(persisted.inventory);
          if (copies === null) return unavailable("imported_baseline_copy_evidence_unavailable");
          visibleImportedBirths.push(current.birth);
          inventories.push({ inventory: persisted.inventory, forkBasis: [...forkBasis] });
          baselineCopies.push(...copies);
        }
        const fork = current.thread.forkedFrom;
        if (fork?.type !== "run") break;
        const runs = yield* sql<{
          readonly event_id: string;
          readonly sequence: number;
          readonly command_id: string | null;
          readonly payload_json: string;
        }>`
          SELECT event_id, sequence, command_id, payload_json FROM orchestration_events WHERE application_event_version = 2
            AND aggregate_kind = 'thread' AND stream_id = ${fork.threadId} AND event_type = 'run.created' AND json_extract(payload_json, '$.id') = ${fork.runId}`;
        if (runs.length !== 1 || runs[0]!.command_id === null)
          return unavailable("fork_source_run_unavailable");
        const runEvent = runs[0]!;
        const decodedRun = Schema.decodeUnknownOption(
          Schema.fromJsonString(OrchestrationV2RunJson),
        )(runEvent.payload_json);
        const runReceipt = Option.getOrNull(
          yield* commandReceipts.getByCommandId(CommandId.make(runEvent.command_id!)),
        );
        const source = yield* readBirth(fork.threadId, runEvent.sequence);
        if (
          source === null ||
          Option.isNone(decodedRun) ||
          decodedRun.value.id !== fork.runId ||
          decodedRun.value.threadId !== fork.threadId ||
          runReceipt?.status !== "accepted" ||
          runReceipt.threadId !== fork.threadId ||
          runReceipt.resultSequence < runEvent.sequence
        )
          return unavailable("fork_source_run_unavailable");
        const forkEvents = yield* sql<{
          readonly event_id: string;
          readonly sequence: number;
          readonly command_id: string | null;
          readonly payload_json: string;
        }>`
          SELECT event_id, sequence, command_id, payload_json FROM orchestration_events WHERE application_event_version = 2
            AND aggregate_kind = 'thread' AND stream_id = ${current.birth.threadId} AND event_type IN ('thread.created', 'thread.metadata-updated')
            AND sequence >= ${current.birth.sequence} AND json_extract(payload_json, '$.forkedFrom.type') = 'run'
            AND json_extract(payload_json, '$.forkedFrom.threadId') = ${fork.threadId} AND json_extract(payload_json, '$.forkedFrom.runId') = ${fork.runId}
            ORDER BY sequence LIMIT 1`;
        const forkEvent = forkEvents[0];
        if (forkEvent === undefined || forkEvent.sequence <= runEvent.sequence)
          return unavailable("fork_application_event_unavailable");
        const decodedFork = Schema.decodeUnknownOption(
          Schema.fromJsonString(OrchestrationV2AppThreadJson),
        )(forkEvent.payload_json);
        if (
          Option.isNone(decodedFork) ||
          decodedFork.value.id !== current.birth.threadId ||
          decodedFork.value.projectId !== current.thread.projectId ||
          nativeCreationCanonicalJson(decodedFork.value.forkedFrom) !==
            nativeCreationCanonicalJson(fork)
        )
          return unavailable("fork_application_event_unavailable");
        if (forkEvent.command_id !== null) {
          const receipt = Option.getOrNull(
            yield* commandReceipts.getByCommandId(CommandId.make(forkEvent.command_id)),
          );
          if (
            receipt?.status !== "accepted" ||
            receipt.threadId !== current.birth.threadId ||
            receipt.resultSequence < forkEvent.sequence
          )
            return unavailable("fork_application_event_unavailable");
        }
        forkBasis.push({
          targetBirth: current.birth,
          sourceBirth: source.birth,
          sourceRunId: fork.runId,
          sourceRunOrdinal: decodedRun.value.ordinal,
          sourceRunEvent: { eventId: EventId.make(runEvent.event_id), sequence: runEvent.sequence },
          forkEvent: { eventId: EventId.make(forkEvent.event_id), sequence: forkEvent.sequence },
        });
        current = source;
      }
      return yield* projectionStore.getThreadRetainedAttachmentPaths(threadId).pipe(
        Effect.provideService(ProjectionStore.ImportedApplicationAttachmentRetentionInputV1, {
          targetBirth: target.birth,
          visibleImportedBirths,
          inventories,
          baselineCopies,
        }),
      );
    });
    const readQualifiedDeletionCleanupOutcomeEffect = Effect.fnUntraced(function* (
      effectId: string,
    ) {
      if (
        !(yield* hasOwnJonesMigration(jonesMigrationEntries, [142, "V2NativeAcceptance"]).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        )) ||
        !(yield* hasOwnJonesMigration(jonesMigrationEntries, [
          139,
          "DeletionWorktreeAdmission",
        ]).pipe(Effect.provideService(SqlClient.SqlClient, sql)))
      )
        return null;
      const rows = yield* sql<{
        readonly ordinal: number;
        readonly outcome_json: string;
        readonly correlation_json: string;
      }>`
        SELECT ordinal, outcome_json, correlation_json FROM orchestration_v2_lease_cleanup_task_outcomes
        WHERE effect_id = ${effectId} ORDER BY ordinal DESC LIMIT 1`;
      if (rows.length === 0) return null;
      const row = rows[0]!;
      const parsedCorrelation = yield* EventSinkJsonCodec.decodeCleanupCorrelation(
        row.correlation_json,
      ).pipe(Effect.catch((error) => Effect.die(EventSinkJsonCodec.jsonCause(error))));
      if (parsedCorrelation.evidenceSchema !== "t3.deletion-cleanup-observation/v1") return null;
      const raw = parsedCorrelation.value;
      const correlation = yield* Schema.decodeUnknownEffect(
        Schema.Struct({
          workerId: Schema.NonEmptyString,
          expectedAttempt: Schema.Int.check(Schema.isGreaterThan(0)),
          bindingSha256: LowerSha256,
          evidence: EffectOutbox.QualifiedDeletionCleanupEvidenceV1,
        }),
      )(raw, { onExcessProperty: "error" });
      const observation = yield* Schema.decodeUnknownEffect(DeletionCleanupObservationV1)(
        correlation.evidence.observation,
        { onExcessProperty: "error" },
      );
      const outcome = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(LeaseCleanupTaskOutcomeV2),
      )(row.outcome_json, { onExcessProperty: "error" });
      const binding =
        (yield* readDeletionCleanupTaskEffect(effectId)) ??
        (yield* readDeletionWorktreeTaskEffect(effectId));
      if (binding === null || correlation.bindingSha256 !== binding.bindingSha256)
        return yield* new EventSinkWriteError({
          eventCount: 0,
          cause: "Qualified cleanup outcome lost its original task",
        });
      const qualified = yield* qualifyDeletionCleanupObservation(binding, observation);
      if (
        nativeCreationCanonicalJson(qualified.outcome) !== nativeCreationCanonicalJson(outcome) ||
        qualified.workerId !== correlation.workerId ||
        qualified.expectedAttempt !== correlation.expectedAttempt ||
        qualified.producer !== correlation.evidence.producer
      )
        return yield* new EventSinkWriteError({
          eventCount: 0,
          cause: "Stored cleanup outcome differs from its finite producer evidence",
        });
      const holds = (yield* effectOutbox.listHeldByThreadId(binding.threadId)).filter(
        (hold) => hold.effectId === effectId,
      );
      if (
        correlation.evidence.coveredHolds.some(
          (hold, index) =>
            hold.effectId !== effectId ||
            hold.threadId !== binding.threadId ||
            hold.workerId !== qualified.workerId ||
            hold.expectedAttempt !== qualified.expectedAttempt ||
            hold.operationId !== effectId ||
            !cleanupHoldMatchesObservation(binding, observation, hold) ||
            !holds.some(
              (stored) => nativeCreationCanonicalJson(stored) === nativeCreationCanonicalJson(hold),
            ) ||
            correlation.evidence.coveredHolds
              .slice(0, index)
              .some((earlier) => earlier.effectId === hold.effectId),
        )
      )
        return yield* new EventSinkWriteError({
          eventCount: 0,
          cause: "Stored cleanup coverage differs from its immutable operation holds",
        });
      const evidence = { ...correlation.evidence, observation };
      return {
        ordinal: row.ordinal,
        outcome,
        bindingSha256: correlation.bindingSha256,
        evidence,
        correlation,
      };
    });
    const readAttachmentNamespaceCleanupTaskEffect = Effect.fnUntraced(function* (
      effectId: string,
    ) {
      const effect = Option.getOrNull(yield* readCleanupSnapshot(effectId));
      if (effect === null) return null;
      const reference = effect.attachmentNamespaceCleanup;
      const fail = () =>
        new EventSinkWriteError({
          eventCount: 0,
          commandId: effect.commandId,
          cause: "Attachment namespace task lost its original birth, receipt or completion trigger",
        });
      if (
        reference === undefined ||
        effect.request.type !== "attachment.cleanup" ||
        effect.nativeCreationExecutionReference !== undefined
      )
        return yield* fail();
      const birth =
        yield* sql`SELECT event_id FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread'
        AND stream_id = ${effect.threadId} AND event_type = 'thread.created' AND event_id = ${reference.ownerBirth.eventId}
        AND sequence = ${reference.ownerBirth.sequence} AND json_extract(payload_json, '$.id') = ${effect.threadId}`;
      const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(effect.commandId));
      const events = yield* eventStore
        .readByCommandId({ commandId: effect.commandId })
        .pipe(Stream.runCollect);
      const triggers = events.filter(
        (stored) =>
          stored.event.id === reference.triggerEventId && stored.event.threadId === effect.threadId,
      );
      const trigger = triggers[0];
      if (
        birth.length !== 1 ||
        reference.ownerBirth.threadId !== effect.threadId ||
        receipt?.status !== "accepted" ||
        receipt.threadId !== effect.threadId ||
        triggers.length !== 1 ||
        trigger === undefined ||
        trigger.sequence <= reference.ownerBirth.sequence
      )
        return yield* fail();
      if (reference.mode === "delete_thread") {
        if (
          receipt.commandType !== "thread.delete" ||
          effectId !== `effect:${effect.commandId}:attachment.cleanup` ||
          trigger.event.type !== "thread.deleted" ||
          trigger.event.payload.id !== effect.threadId ||
          receipt.resultSequence < trigger.sequence
        )
          return yield* fail();
      } else {
        const rollback = Option.getOrNull(yield* readCleanupSnapshot(reference.rollbackEffectId));
        if (
          receipt.commandType !== "checkpoint.rollback" ||
          receipt.resultSequence < reference.ownerBirth.sequence ||
          rollback === null ||
          rollback.commandId !== effect.commandId ||
          rollback.threadId !== effect.threadId ||
          rollback.request.type !== "provider-thread.rollback" ||
          effectId !== `${rollback.id}:attachment.cleanup:prune` ||
          trigger.event.type !== "provider-thread.updated" ||
          trigger.event.payload.id !== rollback.request.providerThreadId ||
          trigger.event.payload.appThreadId !== effect.threadId
        )
          return yield* fail();
      }
      const task = {
        version: 1 as const,
        effectId,
        commandId: effect.commandId,
        threadId: effect.threadId,
        reference,
        triggerSequence: trigger.sequence,
      };
      return yield* Schema.decodeUnknownEffect(AttachmentNamespaceCleanupTaskV1)(
        { ...task, bindingSha256: attachmentTaskDigest(task) },
        { onExcessProperty: "error" },
      );
    });
    const attachmentObservationResult = (
      observation: AttachmentNamespaceCleanupObservationV1,
      ordinal: number,
    ): AttachmentNamespaceCleanupRecordResultV1 => ({
      status:
        observation.outcome.status === "retryable_failure"
          ? "retryable"
          : observation.outcome.status === "unknown"
            ? "unknown"
            : "completed",
      effectId: observation.effectId,
      ordinal,
    });
    const AttachmentNamespaceCorrelationV1 = EffectOutbox.AttachmentNamespaceCleanupCorrelationV1;
    const validateAttachmentNamespaceObservation = Effect.fnUntraced(function* (
      basis: QualifiedAttachmentNamespaceCleanupBasisV1,
      observation: AttachmentNamespaceCleanupObservationV1,
    ) {
      const fail = () =>
        new EventSinkWriteError({
          eventCount: 0,
          cause: "Attachment namespace observation differs from its qualified task, claim or scan",
        });
      const observedAt = yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString)(
        observation.observedAt,
      );
      const segment = toSafeThreadAttachmentSegment(basis.task.threadId);
      if (
        observation.effectId !== basis.task.effectId ||
        observation.bindingSha256 !== basis.task.bindingSha256 ||
        observation.workerId !== basis.claim.workerId ||
        observation.expectedAttempt !== basis.claim.expectedAttempt ||
        observation.basisEventSequence !== basis.basisEventSequence ||
        observation.namespaceSegment !== segment ||
        DateTime.formatIso(observedAt) !== observation.observedAt ||
        observation.configuredRoot.includes("\0") ||
        !NodePath.isAbsolute(observation.configuredRoot) ||
        NodePath.resolve(observation.configuredRoot) !== observation.configuredRoot
      )
        return yield* fail();
      const outcome = observation.outcome;
      if (basis.status === "superseded") {
        if (
          outcome.status !== "superseded" ||
          nativeCreationCanonicalJson(outcome.replacementBirth) !==
            nativeCreationCanonicalJson(basis.replacementBirth)
        )
          return yield* fail();
        return;
      }
      if (basis.status === "unsafe_namespace") {
        if (segment !== null || outcome.status !== "unsafe_namespace") return yield* fail();
        return;
      }
      if (
        segment === null ||
        !["completed", "retryable_failure", "unknown"].includes(outcome.status)
      )
        return yield* fail();
      const actualRetention = yield* readQualifiedThreadRetainedAttachmentPathsEffect(
        basis.task.threadId,
      );
      if (
        actualRetention.status !== "complete" ||
        nativeCreationCanonicalJson(actualRetention.relativePaths) !==
          nativeCreationCanonicalJson(basis.retainedRelativePaths) ||
        nativeCreationCanonicalJson(actualRetention.sourceEvidence) !==
          nativeCreationCanonicalJson(basis.retentionSourceEvidence)
      )
        return yield* fail();
      if (
        basis.retentionSourceEvidence !== undefined &&
        nativeCreationCanonicalJson(
          ImportedAttachments.makeImportedApplicationAttachmentRetentionEvidenceV1({
            ...basis.retentionSourceEvidence,
            relativePaths: basis.retainedRelativePaths,
          }),
        ) !== nativeCreationCanonicalJson(basis.retentionSourceEvidence)
      )
        return yield* fail();
      const validPaths = (paths: ReadonlyArray<string>) =>
        new Set(paths).size === paths.length &&
        paths.every((path) => {
          if (/[\\/\0]/.test(path) || path === "." || path === "..") return false;
          const id = parseAttachmentIdFromRelativePath(path);
          return id !== null && parseThreadSegmentFromAttachmentId(id) === segment;
        });
      if (outcome.status === "completed") {
        const matched = new Set(outcome.matchingPaths);
        const removed = new Set(outcome.removedPaths);
        const retained = new Set(basis.retainedRelativePaths);
        const expectedRetained = outcome.matchingPaths.filter((path) => retained.has(path)).sort();
        if (
          ![outcome.matchingPaths, outcome.removedPaths, outcome.retainedPaths].every(validPaths) ||
          outcome.removedPaths.some((path) => !matched.has(path)) ||
          outcome.retainedPaths.some((path) => !matched.has(path) || removed.has(path)) ||
          outcome.matchingPaths.length !==
            outcome.removedPaths.length + outcome.retainedPaths.length ||
          nativeCreationCanonicalJson([...outcome.retainedPaths].sort()) !==
            nativeCreationCanonicalJson(expectedRetained) ||
          (outcome.rootAbsent && outcome.matchingPaths.length !== 0)
        )
          return yield* fail();
      } else if (outcome.status === "retryable_failure") {
        if (
          !validPaths(outcome.removedPaths) ||
          !validPaths(outcome.remainingPaths) ||
          outcome.remainingPaths.length === 0 ||
          outcome.remainingPaths.some(
            (path) =>
              outcome.removedPaths.includes(path) || basis.retainedRelativePaths.includes(path),
          ) ||
          outcome.removedPaths.some((path) => basis.retainedRelativePaths.includes(path))
        )
          return yield* fail();
      } else if (
        outcome.status === "unknown" &&
        (!validPaths(outcome.removedPaths) ||
          outcome.removedPaths.some((path) => basis.retainedRelativePaths.includes(path)))
      )
        return yield* fail();
    });
    const readAttachmentNamespaceCleanupHistoryEffect = Effect.fnUntraced(function* (
      effectId: string,
    ) {
      if (
        !(yield* hasOwnJonesMigration(jonesMigrationEntries, [143, "AttachmentCleanup"]).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        ))
      )
        return [];
      const rows = yield* sql<{
        readonly ordinal: number;
        readonly binding_sha256: string;
        readonly canonical_task_json: string;
        readonly observation_json: string;
        readonly correlation_json: string;
        readonly recorded_at: string;
      }>`
        SELECT * FROM orchestration_v2_attachment_cleanup_observations WHERE effect_id = ${effectId} ORDER BY ordinal`;
      if (rows.length === 0) return [];
      const task = yield* readAttachmentNamespaceCleanupTaskEffect(effectId);
      if (task === null)
        return yield* new EventSinkWriteError({
          eventCount: 0,
          cause: "Attachment observation history has no original task",
        });
      return yield* Effect.forEach(
        rows,
        (row, ordinal) =>
          Effect.gen(function* () {
            const recordedTask = yield* Schema.decodeUnknownEffect(
              Schema.fromJsonString(AttachmentNamespaceCleanupTaskV1),
            )(row.canonical_task_json, { onExcessProperty: "error" });
            const observation = yield* Schema.decodeUnknownEffect(
              Schema.fromJsonString(AttachmentNamespaceCleanupObservationV1),
            )(row.observation_json, { onExcessProperty: "error" });
            const correlation = yield* Schema.decodeUnknownEffect(
              Schema.fromJsonString(AttachmentNamespaceCorrelationV1),
            )(row.correlation_json, { onExcessProperty: "error" });
            if (
              row.ordinal !== ordinal ||
              row.binding_sha256 !== task.bindingSha256 ||
              row.recorded_at !== observation.observedAt ||
              nativeCreationCanonicalJson(recordedTask) !== nativeCreationCanonicalJson(task) ||
              nativeCreationCanonicalJson(correlation.basis.task) !==
                nativeCreationCanonicalJson(task) ||
              correlation.observationSha256 !==
                nativeCreationSha256(nativeCreationCanonicalJson(observation))
            )
              return yield* new EventSinkWriteError({
                eventCount: 0,
                cause: "Attachment observation history lost its immutable correlation",
              });
            yield* validateAttachmentNamespaceObservation(correlation.basis, observation);
            const status = attachmentObservationResult(observation, ordinal).status;
            if (status === "stale")
              return yield* new EventSinkWriteError({
                eventCount: 0,
                cause: "A stale attachment observation was persisted",
              });
            return {
              ordinal,
              task,
              basis: correlation.basis,
              observation,
              status,
            } satisfies AttachmentNamespaceCleanupRecordedObservationV1;
          }),
        { concurrency: 1 },
      );
    });
    const attachmentNamespaceHoldMatches = (
      task: AttachmentNamespaceCleanupTaskV1,
      history: ReadonlyArray<AttachmentNamespaceCleanupRecordedObservationV1>,
      hold: EffectOutbox.UnknownEffectHoldV2,
    ) =>
      hold.effectId === task.effectId &&
      hold.threadId === task.threadId &&
      hold.operationId === task.effectId &&
      "kind" in hold.evidence &&
      hold.evidence.kind === "resource_cleanup" &&
      hold.evidence.taskKind === "attachment" &&
      hold.evidence.operationId === task.effectId &&
      hold.evidence.threadId === task.threadId &&
      hold.evidence.bindingSha256 === task.bindingSha256 &&
      history.some(
        (entry) =>
          entry.observation.outcome.status === "unknown" &&
          entry.observation.workerId === hold.workerId &&
          entry.observation.expectedAttempt === hold.expectedAttempt,
      );
    const readUnresolvedDeletionCleanupHoldsEffect = Effect.fnUntraced(function* (
      threadId: ThreadId,
    ) {
      if (
        !(yield* hasOwnJonesMigration(jonesMigrationEntries, [142, "V2NativeAcceptance"]).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        ))
      )
        return yield* effectOutbox.listHeldByThreadId(threadId);
      const holds = yield* effectOutbox.listHeldByThreadId(threadId);
      const unresolved: EffectOutbox.UnknownEffectHoldV2[] = [];
      for (const hold of holds) {
        const effect = Option.getOrNull(yield* readCleanupSnapshot(hold.effectId));
        if (effect?.attachmentNamespaceCleanup !== undefined) {
          const history = yield* readAttachmentNamespaceCleanupHistoryEffect(hold.effectId);
          const latest = history[history.length - 1];
          if (
            latest?.status === "completed" &&
            effect.status === "succeeded" &&
            effect.completedAt !== null &&
            attachmentNamespaceHoldMatches(latest.task, history, hold)
          )
            continue;
          unresolved.push(hold);
          continue;
        }
        const latest = yield* readQualifiedDeletionCleanupOutcomeEffect(hold.effectId);
        if (
          latest?.outcome.result !== "succeeded" ||
          !["confirmed", "absent"].includes(latest.outcome.effect) ||
          !latest.evidence.coveredHolds.some(
            (covered) => nativeCreationCanonicalJson(covered) === nativeCreationCanonicalJson(hold),
          )
        )
          unresolved.push(hold);
      }
      return unresolved;
    });

    const checkoutStore = yield* makeOrdinaryCheckoutStore({
      readUnresolvedDeletionCleanupHolds: (threadId) =>
        readUnresolvedDeletionCleanupHoldsEffect(threadId).pipe(
          Effect.mapError(
            (cause) =>
              new EffectOutbox.EffectOutboxError({ operation: "qualified-retirement-read", cause }),
          ),
        ),
    });
    const liveEvents = yield* PubSub.unbounded<OrchestrationV2StoredEvent>();
    const liveEventsByType = new Map<
      OrchestrationV2DomainEvent["type"],
      PubSub.PubSub<OrchestrationV2StoredEvent>
    >();
    const publishLiveEvents = (events: ReadonlyArray<OrchestrationV2StoredEvent>) =>
      Effect.gen(function* () {
        yield* PubSub.publishAll(liveEvents, events);
        for (const [type, pubsub] of liveEventsByType) {
          yield* PubSub.publishAll(
            pubsub,
            events.filter((stored) => stored.event.type === type),
          );
        }
      });
    const publishStoredEvents = (events: ReadonlyArray<OrchestrationV2RecordedStoredEvent>) =>
      eventStore
        .publishCommitted(events)
        .pipe(Effect.andThen(publishLiveEvents(events.filter(isPublicStoredOrchestrationEvent))));

    // Transactions commit one at a time, but each writer publishes after its
    // commit. If a writer is descheduled in between, a later commit reaches
    // subscribers first, and clients drop any event at or below the newest
    // sequence they have applied. So a writer takes this lane as the last step
    // of its transaction and holds it until it has published. Publishing never
    // waits, so a writer that holds the transaction while it waits for the
    // lane is not blocked for long.
    const publishLane = yield* Semaphore.make(1);
    const commitThenPublish = <A, E, R>(
      transaction: Effect.Effect<A, E, R>,
      publish: (committed: A) => Effect.Effect<void>,
    ) =>
      commitTransaction.withTransaction(
        transaction.pipe(
          Effect.tap(() =>
            commitTransaction.retainUntilSettlement(
              publishLane,
              publishLane.take(1),
              publishLane.release(1),
            ),
          ),
          Effect.tap((committed) => commitTransaction.afterCommit(publish(committed))),
        ),
      );

    // A user can answer after terminal normalization reads the pending request.
    // Recheck inside the write transaction so stale cleanup cannot erase answers.
    const guardUserInputCancellations = (events: ReadonlyArray<OrchestrationV2DomainEvent>) =>
      Effect.gen(function* () {
        const staleRequests = new Set<RuntimeRequestId>();
        const staleNodes = new Set<NodeId>();
        for (const event of events) {
          if (
            event.type !== "runtime-request.updated" ||
            event.payload.kind !== "user_input" ||
            event.payload.status !== "cancelled"
          )
            continue;
          const current = yield* projectionStore.getRuntimeRequest(
            event.threadId,
            event.payload.id,
          );
          if (
            current?.status !== "pending" ||
            current.kind !== "user_input" ||
            current.providerTurnId !== event.payload.providerTurnId ||
            current.responseCapability.type === "message"
          ) {
            staleRequests.add(event.payload.id);
            staleNodes.add(event.payload.nodeId);
          }
        }
        return events.filter((event) => {
          switch (event.type) {
            case "runtime-request.updated":
              return event.payload.status !== "cancelled" || !staleRequests.has(event.payload.id);
            case "node.updated":
              return event.payload.status !== "cancelled" || !staleNodes.has(event.payload.id);
            case "turn-item.updated":
              return (
                event.payload.type !== "user_input_request" ||
                event.payload.status !== "cancelled" ||
                !staleRequests.has(event.payload.requestId)
              );
            default:
              return true;
          }
        });
      });

    const guardRuntimeIdentity = (
      input: Pick<
        Parameters<EventSinkV2Shape["write"]>[0],
        | "events"
        | "runtimeEvidence"
        | "runtimeIdentityObservation"
        | "runtimeIdentityBoundary"
        | "runtimeIdentityRequest"
        | "runtimeIdentityPreviousRequest"
      >,
    ) =>
      Effect.gen(function* () {
        const capture = input.runtimeEvidence;
        const update = input.events.find((event) => event.type === "provider-thread.updated");
        if (input.runtimeIdentityBoundary !== undefined) {
          if (update?.type !== "provider-thread.updated") return null;
          const current =
            (yield* projectionStore.getThreadRecords(update.threadId, [
              "providerThreads",
            ])).providerThreads.find((thread) => thread.id === update.payload.id) ?? null;
          if (
            (capture !== undefined && !runtimeEvidenceMatches(current, capture)) ||
            (current?.runtimeIdentity?.runtimeGeneration ?? null) !==
              input.runtimeIdentityBoundary.expectedGeneration ||
            (current !== null &&
              (current.appThreadId !== update.payload.appThreadId ||
                current.providerInstanceId !== update.payload.providerInstanceId ||
                current.driver !== update.payload.driver))
          )
            return null;
          return input.events.map((event) =>
            event.type === "provider-thread.updated" && event.payload.id === update.payload.id
              ? {
                  ...event,
                  payload: {
                    ...event.payload,
                    runtimeIdentity:
                      event.payload.runtimeIdentity === undefined
                        ? undefined
                        : {
                            ...event.payload.runtimeIdentity,
                            evidenceRevision: (current?.runtimeIdentity?.evidenceRevision ?? 0) + 1,
                          },
                  },
                }
              : event,
          );
        }
        if (capture === undefined) return input.events;
        const current =
          (yield* projectionStore.getThreadRecords(capture.threadId, [
            "providerThreads",
          ])).providerThreads.find((thread) => thread.id === capture.providerThreadId) ?? null;
        if (!runtimeEvidenceMatches(current, capture) || current === null) return null;
        const identity = current.runtimeIdentity!;
        if (
          input.runtimeIdentityRequest !== undefined &&
          capture.evidenceRevision !== identity.evidenceRevision
        ) {
          const previous = input.runtimeIdentityPreviousRequest;
          if (
            previous === undefined ||
            identity.requested.providerInstanceId !== previous.providerInstanceId ||
            identity.requested.providerDriver !== previous.providerDriver ||
            identity.requested.model !== previous.model ||
            identity.requested.serviceTier !== previous.serviceTier
          )
            return null;
        }
        if (input.runtimeIdentityObservation !== undefined) {
          const requested = input.runtimeIdentityObservation;
          if (
            identity.requested.providerInstanceId !== requested.providerInstanceId ||
            identity.requested.providerDriver !== requested.providerDriver ||
            identity.requested.model !== requested.model ||
            identity.requested.serviceTier !== requested.serviceTier ||
            identity.evidenceRevision !== capture.evidenceRevision
          )
            return null;
        }
        return input.events.map((event) =>
          event.type === "provider-thread.updated" && event.payload.id === current.id
            ? {
                ...event,
                payload: {
                  ...event.payload,
                  runtimeIdentity:
                    input.runtimeIdentityObservation === undefined
                      ? input.runtimeIdentityRequest === undefined
                        ? identity
                        : {
                            ...identityForRequest(input.runtimeIdentityRequest, identity),
                            evidenceRevision: (identity.evidenceRevision ?? 0) + 1,
                          }
                      : {
                          ...identity,
                          observed: event.payload.runtimeIdentity!.observed,
                          evidenceRevision: (identity.evidenceRevision ?? 0) + 1,
                        },
                },
              }
            : event,
        );
      });

    const normalizeEvents = (events: ReadonlyArray<OrchestrationV2DomainEvent>) => {
      const runOrdinals = new Map(
        events.flatMap((event) =>
          event.type === "run.created" || event.type === "run.updated"
            ? [[event.payload.id, event.payload.ordinal] as const]
            : [],
        ),
      );
      return Effect.forEach(
        events,
        (event): Effect.Effect<OrchestrationV2DomainEvent, unknown> =>
          event.type === "turn-item.updated"
            ? turnItemPositions
                .normalize(
                  event.payload,
                  event.payload.runId === null ? undefined : runOrdinals.get(event.payload.runId),
                )
                .pipe(Effect.map((payload) => ({ ...event, payload })))
            : Effect.succeed(event),
        { concurrency: 1 },
      );
    };

    const applyStoredEvents = (storedEvents: ReadonlyArray<OrchestrationV2RecordedStoredEvent>) =>
      Effect.gen(function* () {
        yield* Effect.forEach(storedEvents, (stored) => projectionStore.apply(stored.event), {
          concurrency: 1,
        });
        const sequence = storedEvents.at(-1)?.sequence;
        if (sequence !== undefined) {
          const now = DateTime.formatIso(yield* DateTime.now);
          yield* sql`
            INSERT INTO orchestration_v2_projection_metadata (
              projection_name,
              schema_version,
              last_sequence,
              updated_at
            )
            VALUES (
              'thread-projections',
              ${ProjectionStore.ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION},
              ${sequence},
              ${now}
            )
            ON CONFLICT(projection_name)
            DO UPDATE SET
              schema_version = excluded.schema_version,
              last_sequence = excluded.last_sequence,
              updated_at = excluded.updated_at
          `;
        }
      });

    const writeEffect = Effect.fn("orchestrationV2.EventSink.write")(function* (
      input: Parameters<EventSinkV2Shape["writeWithEffects"]>[0],
    ) {
      yield* Effect.annotateCurrentSpan({
        "orchestration_v2.command_id": input.commandId ?? null,
        "orchestration_v2.event_count": input.events.length,
        "orchestration_v2.thread_id": input.events[0]?.threadId ?? null,
      });

      return yield* commitThenPublish(
        Effect.gen(function* () {
          const identityEvents = yield* guardRuntimeIdentity(input);
          if (identityEvents === null) return [];
          const normalized = yield* normalizeEvents(
            input.guardPendingUserInputCancellations === true
              ? yield* guardUserInputCancellations(identityEvents)
              : identityEvents,
          );
          const committed = yield* eventStore
            .append({
              ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
              events: normalized,
            })
            .pipe(Effect.map((stored) => stored.filter(isPublicStoredOrchestrationEvent)));
          yield* applyStoredEvents(committed);
          yield* effectOutbox.enqueue(input.effects);
          if (input.ordinaryCheckoutEffects !== undefined)
            yield* checkoutStore.writeSystemEffects(input.effects, input.ordinaryCheckoutEffects);
          return committed;
        }),
        (storedEvents) =>
          Effect.gen(function* () {
            if (input.effects.length > 0) {
              yield* effectOutbox.notifyAvailable(input.effects.length);
            }
            yield* publishStoredEvents(storedEvents);
          }),
      );
    });

    const writeIfRunCurrentEffect = Effect.fn("orchestrationV2.EventSink.writeIfRunCurrent")(
      function* (input: Parameters<EventSinkV2Shape["writeIfRunCurrent"]>[0]) {
        yield* Effect.annotateCurrentSpan({
          "orchestration_v2.command_id": input.commandId ?? null,
          "orchestration_v2.event_count": input.events.length,
          "orchestration_v2.run_id": input.runId,
          "orchestration_v2.thread_id": input.threadId,
        });

        return yield* commitThenPublish(
          Effect.gen(function* () {
            const rows = yield* sql<{
              readonly status: string;
              readonly active_attempt_id: string | null;
            }>`
            SELECT
              status,
              json_extract(payload_json, '$.activeAttemptId') AS active_attempt_id
            FROM orchestration_v2_projection_runs
            WHERE run_id = ${input.runId}
              AND thread_id = ${input.threadId}
            LIMIT 1
          `;
            const current = rows[0];
            if (
              current === undefined ||
              current.status !== input.expectedStatus ||
              current.active_attempt_id !== input.activeAttemptId
            ) {
              return {
                committed: false as const,
                storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
              };
            }

            const identityEvents = yield* guardRuntimeIdentity(input);
            if (identityEvents === null)
              return {
                committed: false as const,
                storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
              };
            const normalized = yield* normalizeEvents(
              input.guardPendingUserInputCancellations === true
                ? yield* guardUserInputCancellations(identityEvents)
                : identityEvents,
            );
            const storedEvents = yield* eventStore
              .append({
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                events: normalized,
              })
              .pipe(Effect.map((stored) => stored.filter(isPublicStoredOrchestrationEvent)));
            yield* applyStoredEvents(storedEvents);
            return { committed: true as const, storedEvents };
          }),
          (result) => (result.committed ? publishStoredEvents(result.storedEvents) : Effect.void),
        );
      },
    );

    const writeIfProviderThreadOwnerEffect = Effect.fn(
      "orchestrationV2.EventSink.writeIfProviderThreadOwner",
    )(function* (input: Parameters<EventSinkV2Shape["writeIfProviderThreadOwner"]>[0]) {
      yield* Effect.annotateCurrentSpan({
        "orchestration_v2.command_id": input.commandId ?? null,
        "orchestration_v2.event_count": input.events.length,
        "orchestration_v2.provider_thread_id": input.providerThreadId,
        "orchestration_v2.run_id": input.runId,
        "orchestration_v2.active_attempt_id": input.activeAttemptId,
        "orchestration_v2.expected_last_run_ordinal": input.expectedLastRunOrdinal,
      });

      return yield* commitThenPublish(
        Effect.gen(function* () {
          const rows = yield* sql<{
            readonly active_attempt_id: string | null;
            readonly last_run_ordinal: number | null;
          }>`
            SELECT
              json_extract(r.payload_json, '$.activeAttemptId') AS active_attempt_id,
              p.last_run_ordinal
            FROM orchestration_v2_projection_provider_threads p
            JOIN orchestration_v2_projection_runs r
              ON r.run_id = ${input.runId}
             AND r.thread_id = p.thread_id
            WHERE p.provider_thread_id = ${input.providerThreadId}
            LIMIT 1
          `;
          const current = rows[0];
          if (
            current === undefined ||
            current.active_attempt_id !== input.activeAttemptId ||
            current.last_run_ordinal !== input.expectedLastRunOrdinal
          ) {
            return {
              committed: false as const,
              storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
            };
          }

          const identityEvents = yield* guardRuntimeIdentity(input);
          if (identityEvents === null)
            return {
              committed: false as const,
              storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
            };
          const normalized = yield* normalizeEvents(
            input.guardPendingUserInputCancellations === true
              ? yield* guardUserInputCancellations(identityEvents)
              : identityEvents,
          );
          const storedEvents = yield* eventStore
            .append({
              ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
              events: normalized,
            })
            .pipe(Effect.map((stored) => stored.filter(isPublicStoredOrchestrationEvent)));
          yield* applyStoredEvents(storedEvents);
          return { committed: true as const, storedEvents };
        }),
        (result) => (result.committed ? publishStoredEvents(result.storedEvents) : Effect.void),
      );
    });

    const existingCommandResult = (commandId: CommandId) =>
      Effect.gen(function* () {
        const existing = yield* commandReceipts.getByCommandId(commandId);
        if (Option.isNone(existing)) {
          return yield* Effect.die(
            new Error(`Command receipt ${commandId} disappeared during its transaction.`),
          );
        }
        const storedEvents = yield* eventStore.readByCommandId({ commandId }).pipe(
          Stream.filter(isPublicStoredOrchestrationEvent),
          Stream.runCollect,
          Effect.map((events): ReadonlyArray<OrchestrationV2StoredEvent> => Array.from(events)),
        );
        return { receipt: existing.value, storedEvents };
      });

    const commitCommandEffect = Effect.fn("orchestrationV2.EventSink.commitCommand")(function* (
      input: Parameters<EventSinkV2Shape["commitCommand"]>[0],
    ) {
      const result = yield* commitThenPublish(
        Effect.gen(function* () {
          const checkoutCapture =
            input.ordinaryCheckout === undefined
              ? undefined
              : yield* checkoutStore.acquireBeforeRead(input.ordinaryCheckout, input.acceptedAt);
          const reserved = yield* commandReceipts.insertIfAbsent({
            commandId: input.commandId,
            threadId: input.threadId,
            commandType: input.commandType,
            acceptedAt: input.acceptedAt,
            resultSequence: 0,
            status: "accepted",
            error: null,
          });
          if (!reserved) {
            const existing = yield* existingCommandResult(input.commandId);
            if (checkoutCapture !== undefined) {
              const binding = (yield* checkoutStore.readCurrentCommands(input.commandId)).find(
                (item) => item.threadId === checkoutCapture.capture.threadId,
              );
              const digest =
                checkoutCapture.command === undefined
                  ? checkoutCapture.capture.commandDigest
                  : ordinaryCheckoutCommandDigestV1(
                      yield* Schema.encodeEffect(OrchestrationV2Command)(checkoutCapture.command),
                    );
              if (binding === undefined || binding.commandDigest !== digest)
                return yield* new EventSinkWriteError({
                  commandId: input.commandId,
                  eventCount: input.events.length,
                  cause: "The replay does not match its permanent checkout admission.",
                });
            }
            return { ...existing, committed: false as const, cancelledEffectIds: [] };
          }

          const normalized = yield* normalizeEvents(input.events);
          const storedEvents = yield* eventStore
            .append({
              commandId: input.commandId,
              events: normalized,
            })
            .pipe(Effect.map((stored) => stored.filter(isPublicStoredOrchestrationEvent)));
          const sequence = storedEvents.at(-1)?.sequence;
          if (sequence === undefined) {
            return yield* Effect.die(
              new Error(`Command ${input.commandId} produced no orchestration events.`),
            );
          }
          yield* applyStoredEvents(storedEvents);
          yield* effectOutbox.enqueue(input.effects);
          const receipt: CommandReceiptStore.CommandReceiptV2 = {
            commandId: input.commandId,
            threadId: input.threadId,
            commandType: input.commandType,
            acceptedAt: input.acceptedAt,
            resultSequence: sequence,
            status: "accepted",
            error: null,
          };
          yield* commandReceipts.upsert(receipt);
          if (checkoutCapture !== undefined)
            yield* checkoutStore.recordAcceptance({
              captured: checkoutCapture,
              receipt,
              events: storedEvents,
              effects: input.effects,
            });
          if (input.ordinaryDelegatedCommand !== undefined) {
            const command = input.ordinaryDelegatedCommand;
            const preparing = input.effects.filter(
              (effect) => effect.request.type === "delegated-workspace.prepare",
            );
            if (
              command.commandId !== input.commandId ||
              command.type !== input.commandType ||
              command.parentThreadId !== input.threadId ||
              preparing.length !== 1
            )
              return yield* new EventSinkWriteError({
                commandId: input.commandId,
                eventCount: storedEvents.length,
                cause:
                  "Delegated acceptance requires its unique preparing effect and original outer command.",
              });
            const effect = preparing[0]!;
            if (effect.request.type !== "delegated-workspace.prepare")
              return yield* Effect.die("Invalid preparation filter");
            const plan = effect.request.plan;
            const child = storedEvents.find(
              (stored) =>
                stored.event.type === "thread.created" &&
                stored.event.threadId === plan.childThreadId,
            );
            const run = storedEvents.findLast(
              (stored) =>
                (stored.event.type === "run.created" || stored.event.type === "run.updated") &&
                stored.event.threadId === plan.childThreadId,
            );
            if (
              effect.commandId !== input.commandId ||
              effect.threadId !== plan.childThreadId ||
              plan.parentThreadId !== command.parentThreadId ||
              child?.event.type !== "thread.created" ||
              child.event.payload.branch !== plan.branch ||
              child.event.payload.worktreePath !== plan.worktreePath ||
              (run?.event.type !== "run.created" && run?.event.type !== "run.updated") ||
              run.event.payload.status !== "preparing" ||
              run.event.payload.id !== effect.request.runId ||
              run.event.payload.workspacePreparation?.type !== "worktree" ||
              run.event.payload.workspacePreparation.baseRef !== plan.parentCommit ||
              run.event.payload.workspacePreparation.branch !== plan.branch ||
              run.event.payload.workspacePreparation.startFromOrigin !== false
            )
              return yield* new EventSinkWriteError({
                commandId: input.commandId,
                eventCount: storedEvents.length,
                cause:
                  "Delegated preparation changed its pinned child, path, branch or local base.",
              });
            const captured = yield* checkoutStore.capture({
              command,
              threadId: plan.childThreadId,
              projectId: child.event.payload.projectId,
              branch: plan.branch,
              canonicalProjectRoot: plan.canonicalProjectRoot,
              canonicalCheckoutPath: plan.worktreePath,
              source: {
                projectWorkspaceRoot: plan.projectWorkspaceRoot,
                worktreePath: plan.worktreePath,
              },
              leaseId: `lease:${input.commandId}:${plan.childThreadId}`,
              origin: { kind: "delegated_child", parentThreadId: command.parentThreadId },
            });
            yield* checkoutStore.recordAcceptance({
              captured: yield* checkoutStore.acquireNewborn(captured, receipt, storedEvents),
              receipt,
              events: storedEvents,
              effects: input.effects,
            });
          }
          const cancelledEffectIds =
            input.cancelUnsettledEffects === undefined
              ? []
              : yield* effectOutbox.cancelUnsettled({
                  threadId: input.threadId,
                  ...input.cancelUnsettledEffects,
                });
          return { receipt, storedEvents, committed: true as const, cancelledEffectIds };
        }),
        (result) =>
          Effect.gen(function* () {
            yield* effectOutbox.signalCancellations(result.cancelledEffectIds);
            if (result.committed && input.effects.length > 0) {
              yield* effectOutbox.notifyAvailable(input.effects.length);
            }
            if (result.committed) yield* publishStoredEvents(result.storedEvents);
          }),
      );
      return {
        receipt: result.receipt,
        storedEvents: result.storedEvents,
        committed: result.committed,
        cancelledEffectCount: result.cancelledEffectIds.length,
      };
    });

    const samePreflightBinding = Schema.toEquivalence(OrchestrationV2LegacyPreflightBinding);
    const readPreflight = (commandId: CommandId) =>
      eventStore.readByCommandId({ commandId }).pipe(
        Stream.runCollect,
        Effect.map((events) => Array.from(events)),
      );
    const commitLegacyPreflight = Effect.fn("orchestrationV2.EventSink.commitLegacyPreflight")(
      function* (input: Parameters<EventSinkV2Shape["commitLegacyPreflight"]>[0]) {
        const event = input.event;
        const binding =
          event.type === "legacy-bootstrap.preflight-intent"
            ? event.payload
            : event.payload.binding;
        const policy = binding.policy;
        const intentId = CommandId.make(`${policy.createCommandId}:preflight-intent`);
        const outcomeId = CommandId.make(`${policy.createCommandId}:preflight-outcome`);
        const reject = (detail: string) => new Error(detail);
        const payload = yield* Schema.decodeUnknownEffect(
          Schema.fromJsonString(QueueDispatchCommand),
        )(binding.canonicalPayload);
        if (
          payload.type !== "thread.turn.start" ||
          payload.bootstrap === undefined ||
          policy.runId !== undefined ||
          policy.createCommandId !==
            legacyBootstrapCreateCommandId(policy.threadId, policy.releaseCommandId) ||
          policy.birthCommandId !== `${policy.createCommandId}:initial-message` ||
          event.threadId !== policy.threadId ||
          binding.fetch.remote !== (binding.fetch.startFromOrigin ? "origin" : null) ||
          canonicalLegacyPayload(policy.dispatchGuard ?? null) !==
            canonicalLegacyPayload(payload.dispatchGuard ?? null) ||
          policy.payloadHash !== legacyPayloadHash(binding.canonicalPayload) ||
          binding.canonicalPayload !== canonicalLegacyPayload(payload) ||
          payload.threadId !== policy.threadId ||
          payload.commandId !== policy.releaseCommandId ||
          payload.message.messageId !== policy.messageId ||
          (payload.bootstrap.createThread !== undefined &&
            payload.bootstrap.createThread.projectId !== policy.projectId) ||
          payload.bootstrap.prepareWorktree?.projectCwd !== binding.fetch.cwd ||
          payload.bootstrap.prepareWorktree.baseBranch !== binding.fetch.baseRef ||
          (payload.bootstrap.prepareWorktree.startFromOrigin === true) !==
            binding.fetch.startFromOrigin ||
          (payload.bootstrap.prepareWorktree.requireWorktree === true) !==
            binding.fetch.requireWorktree ||
          input.commandId !==
            (event.type === "legacy-bootstrap.preflight-intent" ? intentId : outcomeId)
        )
          return yield* Effect.fail(
            reject("Preflight does not match its canonical immutable queue binding."),
          );
        const verify = (
          stored: ReadonlyArray<OrchestrationV2RecordedStoredEvent>,
          type: OrchestrationV2PrivateEvent["type"],
        ) => {
          if (
            stored.length !== 1 ||
            stored[0]?.commandId !== input.commandId ||
            stored[0].event.type !== type ||
            stored[0].event.threadId !== policy.threadId
          )
            return false;
          const recorded = stored[0].event;
          if (recorded.type === "legacy-bootstrap.preflight-intent")
            return samePreflightBinding(recorded.payload, binding);
          if (
            recorded.type !== "legacy-bootstrap.preflight-outcome" ||
            event.type !== recorded.type
          )
            return false;
          return (
            samePreflightBinding(recorded.payload.binding, binding) &&
            canonicalLegacyPayload(recorded.payload) === canonicalLegacyPayload(event.payload)
          );
        };
        const result = yield* commitThenPublish(
          Effect.gen(function* () {
            if (Option.isSome(yield* commandReceipts.getProjectByCommandId(input.commandId)))
              return yield* Effect.fail(
                reject("Preflight command ID belongs to a project command."),
              );
            const existing = yield* commandReceipts.getByCommandId(input.commandId);
            if (Option.isSome(existing)) {
              const stored = yield* readPreflight(input.commandId);
              if (
                existing.value.status !== "accepted" ||
                existing.value.commandType !== event.type ||
                existing.value.threadId !== policy.threadId ||
                !verify(stored, event.type) ||
                stored[0]?.sequence !== existing.value.resultSequence
              )
                return yield* Effect.fail(
                  reject("Preflight command ID belongs to another immutable effect binding."),
                );
              return {
                receipt: existing.value,
                committed: false,
                storedEvents: [] as ReadonlyArray<OrchestrationV2RecordedStoredEvent>,
              };
            }
            if (
              Option.isSome(yield* commandReceipts.getByCommandId(policy.releaseCommandId)) ||
              Option.isSome(
                yield* commandReceipts.getProjectByCommandId(policy.releaseCommandId),
              ) ||
              Option.isSome(yield* commandReceipts.getByCommandId(policy.createCommandId)) ||
              Option.isSome(yield* commandReceipts.getProjectByCommandId(policy.createCommandId))
            )
              return yield* Effect.fail(
                reject("Preflight cannot claim an already born or released bootstrap."),
              );
            if (event.type === "legacy-bootstrap.preflight-intent") {
              const history = Array.from(
                yield* eventStore.read({ threadId: policy.threadId }).pipe(Stream.runCollect),
              );
              for (const previous of history) {
                if (previous.event.type !== "legacy-bootstrap.preflight-intent") continue;
                const previousOutcomes = history.filter(
                  (stored) =>
                    stored.event.type === "legacy-bootstrap.preflight-outcome" &&
                    stored.event.payload.intentCommandId === previous.commandId &&
                    stored.event.payload.intentSequence === previous.sequence,
                );
                if (
                  previousOutcomes.length !== 1 ||
                  previousOutcomes[0]?.event.type !== "legacy-bootstrap.preflight-outcome" ||
                  previousOutcomes[0].event.payload.status === "unknown"
                )
                  return yield* Effect.fail(
                    reject(
                      "Target has an unresolved preflight effect; another command cannot repeat or replace it.",
                    ),
                  );
              }
            }
            if (event.type === "legacy-bootstrap.preflight-outcome") {
              const receipt = yield* commandReceipts.getByCommandId(intentId);
              const intents = yield* readPreflight(intentId);
              const intent = intents[0];
              if (
                Option.isNone(receipt) ||
                receipt.value.status !== "accepted" ||
                receipt.value.commandType !== "legacy-bootstrap.preflight-intent" ||
                receipt.value.threadId !== policy.threadId ||
                intents.length !== 1 ||
                intent?.event.type !== "legacy-bootstrap.preflight-intent" ||
                !samePreflightBinding(intent.event.payload, binding) ||
                event.payload.intentCommandId !== intentId ||
                event.payload.intentSequence !== intent.sequence ||
                receipt.value.resultSequence !== intent.sequence
              )
                return yield* Effect.fail(
                  reject("Preflight outcome has no exact accepted intent."),
                );
            }
            const reserved = yield* commandReceipts.insertIfAbsent({
              commandId: input.commandId,
              threadId: policy.threadId,
              commandType: event.type,
              acceptedAt: event.occurredAt,
              resultSequence: 0,
              status: "accepted",
              error: null,
            });
            if (!reserved)
              return yield* Effect.fail(
                reject("Preflight reservation changed inside its serialized transaction."),
              );
            const storedEvents = yield* eventStore.append({
              commandId: input.commandId,
              events: [event],
            });
            const sequence = storedEvents[0]?.sequence;
            if (sequence === undefined || !verify(storedEvents, event.type))
              return yield* Effect.fail(
                reject("Preflight journal binding was not committed exactly."),
              );
            yield* applyStoredEvents(storedEvents);
            const receipt: CommandReceiptStore.CommandReceiptV2 = {
              commandId: input.commandId,
              threadId: policy.threadId,
              commandType: event.type,
              acceptedAt: event.occurredAt,
              resultSequence: sequence,
              status: "accepted",
              error: null,
            };
            yield* commandReceipts.upsert(receipt);
            return { receipt, committed: true, storedEvents };
          }),
          (result) => (result.committed ? publishStoredEvents(result.storedEvents) : Effect.void),
        );
        const readback = yield* readPreflight(input.commandId);
        if (
          !verify(readback, event.type) ||
          readback[0]?.sequence !== result.receipt.resultSequence
        )
          return yield* Effect.fail(reject("Preflight readback is unknown."));
        return { receipt: result.receipt, committed: result.committed };
      },
      Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 1, cause })),
    );

    const commitRejectedCommandEffect = Effect.fn(
      "orchestrationV2.EventSink.commitRejectedCommand",
    )(function* (input: Parameters<EventSinkV2Shape["commitRejectedCommand"]>[0]) {
      const result = yield* commitThenPublish(
        Effect.gen(function* () {
          const existing = yield* commandReceipts.getByCommandId(input.commandId);
          if (Option.isSome(existing))
            return {
              receipt: existing.value,
              storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
            };
          const legacy = input.legacyGuardRejection;
          let events: ReadonlyArray<OrchestrationV2DomainEvent> = [];
          if (legacy !== undefined) {
            const last = legacy.events.at(-1);
            const decision =
              last?.type === "run.updated" ? last.payload.legacyReleaseDecision : undefined;
            const policy = decision?.policy;
            const invalid = () =>
              new EventSinkWriteError({
                commandId: input.commandId,
                eventCount: legacy.events.length,
                cause: "Legacy guard rejection has no exact authenticated birth and current run.",
              });
            if (
              !isDispatchGuardRejected(legacy.rejection) ||
              legacy.rejection.observed === undefined ||
              input.commandType !== "prepared-run.release" ||
              last?.type !== "run.updated" ||
              decision === undefined ||
              policy === undefined ||
              input.commandId !== policy.releaseCommandId ||
              input.threadId !== policy.threadId ||
              last.threadId !== input.threadId ||
              last.runId !== policy.runId ||
              last.payload.id !== policy.runId ||
              last.payload.userMessageId !== policy.messageId ||
              last.id !== decision.evidenceEventId ||
              decision.reason !== legacy.rejection.reason ||
              canonicalLegacyPayload(decision.observed) !==
                canonicalLegacyPayload(legacy.rejection.observed) ||
              canonicalLegacyPayload(decision.guard) !==
                canonicalLegacyPayload(policy.dispatchGuard)
            )
              return yield* invalid();
            const collect = (commandId: CommandId) =>
              eventStore.readByCommandId({ commandId }).pipe(
                Stream.filter(isPublicStoredOrchestrationEvent),
                Stream.runCollect,
                Effect.map((stored) => Array.from(stored)),
              );
            const proof = legacyBootstrapBirth({
              policy,
              claimEvents: yield* collect(policy.createCommandId),
              birthEvents: yield* collect(policy.birthCommandId),
            });
            const claim = yield* commandReceipts.getByCommandId(policy.createCommandId);
            const birth = yield* commandReceipts.getByCommandId(policy.birthCommandId);
            const projection = yield* projectionStore.getThreadRecords(input.threadId, [
              "runs",
              "attempts",
              "nodes",
              "turnItems",
            ]);
            const current = projection.runs.find((run) => run.id === policy.runId);
            if (
              proof.type !== "valid" ||
              proof.claimEventId !== decision.claimEventId ||
              proof.claimSequence !== decision.claimSequence ||
              proof.birthEventId !== decision.birthEventId ||
              proof.sequence !== decision.birthSequence ||
              Option.isNone(claim) ||
              Option.isNone(birth) ||
              claim.value.status !== "accepted" ||
              birth.value.status !== "accepted" ||
              claim.value.threadId !== policy.threadId ||
              birth.value.threadId !== policy.threadId ||
              claim.value.commandType !==
                (policy.ownsNewThread ? "thread.create" : "thread.metadata.update") ||
              birth.value.commandType !== "message.dispatch" ||
              claim.value.resultSequence !== decision.claimReceiptSequence ||
              birth.value.resultSequence !== decision.birthReceiptSequence ||
              current?.status !== "preparing" ||
              current.legacyBootstrap === undefined ||
              !sameLegacyBootstrapPolicy(current.legacyBootstrap, policy) ||
              canonicalLegacyPayload(current) !==
                canonicalLegacyPayload({
                  ...last.payload,
                  legacyReleaseDecision: current.legacyReleaseDecision,
                  ...(policy.ownsNewThread
                    ? { status: current.status, completedAt: current.completedAt }
                    : {}),
                })
            )
              return yield* invalid();
            if (policy.ownsNewThread) {
              const [attemptEvent, nodeEvent, itemEvent] = legacy.events;
              const attempt = projection.attempts.find(
                (record) => record.id === current.activeAttemptId,
              );
              const node = projection.nodes.find((record) => record.id === current.rootNodeId);
              const item =
                itemEvent?.type === "turn-item.updated"
                  ? projection.turnItems.find(
                      (record) =>
                        record.id === itemEvent.payload.id &&
                        record.type === "command_execution" &&
                        record.input === "Preparing workspace" &&
                        record.runId === current.id,
                    )
                  : undefined;
              if (
                legacy.events.length !== 4 ||
                new Set(legacy.events.map((event) => event.id)).size !== 4 ||
                attemptEvent?.type !== "run-attempt.updated" ||
                nodeEvent?.type !== "node.updated" ||
                itemEvent?.type !== "turn-item.updated" ||
                itemEvent.payload.type !== "command_execution" ||
                attempt === undefined ||
                node === undefined ||
                item === undefined ||
                last.payload.status !== "failed" ||
                canonicalLegacyPayload(last.payload.completedAt) !==
                  canonicalLegacyPayload(input.rejectedAt) ||
                canonicalLegacyPayload(attemptEvent.payload) !==
                  canonicalLegacyPayload({
                    ...attempt,
                    status: "failed",
                    completedAt: input.rejectedAt,
                  }) ||
                canonicalLegacyPayload(nodeEvent.payload) !==
                  canonicalLegacyPayload({
                    ...node,
                    status: "failed",
                    completedAt: input.rejectedAt,
                  }) ||
                canonicalLegacyPayload(itemEvent.payload) !==
                  canonicalLegacyPayload({
                    ...item,
                    status: "failed",
                    title: "Dispatch guard rejected",
                    output: legacy.rejection.reason,
                    exitCode: undefined,
                    completedAt: input.rejectedAt,
                    updatedAt: input.rejectedAt,
                  }) ||
                legacy.events.some(
                  (event) =>
                    event.threadId !== policy.threadId ||
                    event.runId !== policy.runId ||
                    event.providerInstanceId !== current.providerInstanceId ||
                    canonicalLegacyPayload(event.occurredAt) !==
                      canonicalLegacyPayload(input.rejectedAt),
                )
              )
                return yield* invalid();
            } else if (legacy.events.length !== 1) return yield* invalid();
            events = legacy.events;
          }
          const reserved: CommandReceiptStore.CommandReceiptV2 = {
            commandId: input.commandId,
            threadId: input.threadId,
            commandType: input.commandType,
            acceptedAt: input.rejectedAt,
            resultSequence: 0,
            status: "rejected",
            error: input.error,
          };
          if (!(yield* commandReceipts.insertIfAbsent(reserved))) {
            return {
              receipt: (yield* existingCommandResult(input.commandId)).receipt,
              storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
            };
          }
          const storedEvents =
            events.length === 0
              ? []
              : (yield* eventStore.append({
                  commandId: input.commandId,
                  events,
                })).filter(isPublicStoredOrchestrationEvent);
          yield* applyStoredEvents(storedEvents);
          const receipt = {
            ...reserved,
            resultSequence:
              storedEvents.at(-1)?.sequence ??
              (yield* eventStore.latestSequence({ threadId: input.threadId })),
          };
          yield* commandReceipts.upsert(receipt);
          return { receipt, storedEvents };
        }),
        (committed) => publishStoredEvents(committed.storedEvents),
      );
      return result.receipt;
    });

    const existingProjectReceipt = (commandId: CommandId) =>
      commandReceipts.getProjectByCommandId(commandId).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(`Command ${commandId} was already used by a thread command.` as const),
            onSome: Effect.succeed,
          }),
        ),
      );

    const commitProjectCommandEffect = Effect.fn("orchestrationV2.EventSink.commitProjectCommand")(
      function* (input: Parameters<EventSinkV2Shape["commitProjectCommand"]>[0]) {
        const result = yield* commitThenPublish(
          Effect.gen(function* () {
            const reserved: CommandReceiptStore.ProjectCommandReceiptV2 = {
              commandId: input.commandId,
              projectId: input.projectId,
              commandType: input.commandType,
              acceptedAt: input.acceptedAt,
              resultSequence: 0,
              status: "accepted",
              error: null,
            };
            if (!(yield* commandReceipts.insertIfAbsent(reserved))) {
              return { receipt: yield* existingProjectReceipt(input.commandId), event: undefined };
            }
            const event = yield* eventStore.appendProjectEvent(input.event);
            yield* projectStore.apply(event);
            const receipt = { ...reserved, resultSequence: event.sequence };
            yield* commandReceipts.upsert(receipt);
            return { receipt, event };
          }),
          (result) =>
            result.event === undefined ? Effect.void : eventStore.publishCommitted([result.event]),
        );
        return { receipt: result.receipt, committed: result.event !== undefined };
      },
    );

    const commitRejectedProjectCommandEffect = Effect.fn(
      "orchestrationV2.EventSink.commitRejectedProjectCommand",
    )(function* (input: Parameters<EventSinkV2Shape["commitRejectedProjectCommand"]>[0]) {
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const receipt: CommandReceiptStore.ProjectCommandReceiptV2 = {
            commandId: input.commandId,
            projectId: input.projectId,
            commandType: input.commandType,
            acceptedAt: input.rejectedAt,
            resultSequence: yield* eventStore.latestApplicationSequence,
            status: "rejected",
            error: input.error,
          };
          return (yield* commandReceipts.insertIfAbsent(receipt))
            ? receipt
            : yield* existingProjectReceipt(input.commandId);
        }),
      );
    });

    const catchUp = (input: {
      readonly afterSequence: number;
      readonly throughSequence: number;
      readonly threadId?: ThreadId;
      readonly eventType?: OrchestrationV2DomainEvent["type"];
    }): Stream.Stream<OrchestrationV2StoredEvent, unknown> => {
      const pageSize = 256;
      const loop = (afterSequence: number): Stream.Stream<OrchestrationV2StoredEvent, unknown> =>
        Stream.unwrap(
          eventStore
            .read({
              afterSequence,
              throughSequence: input.throughSequence,
              ...(input.threadId === undefined ? {} : { threadId: input.threadId }),
              ...(input.eventType === undefined ? {} : { eventType: input.eventType }),
              limit: pageSize,
            })
            .pipe(
              Stream.runCollect,
              Effect.map((chunk) => Array.from(chunk)),
              Effect.map((events) => {
                if (events.length === 0) {
                  return Stream.empty;
                }
                const current = Stream.fromIterable(
                  events.filter(isPublicStoredOrchestrationEvent),
                );
                const last = events.at(-1)?.sequence ?? input.throughSequence;
                return events.length < pageSize || last >= input.throughSequence
                  ? current
                  : Stream.concat(current, loop(last));
              }),
            ),
        );
      return loop(input.afterSequence);
    };

    const streamEffect = (input?: EventSinkStreamInput) => {
      const afterSequence = input?.afterSequence ?? 0;
      const matches = (stored: OrchestrationV2StoredEvent) =>
        (input?.threadId === undefined || stored.event.threadId === input.threadId) &&
        (input?.eventType === undefined || stored.event.type === input.eventType);
      const replay = (throughSequence: number) =>
        catchUp({
          afterSequence,
          throughSequence,
          ...(input?.threadId === undefined ? {} : { threadId: input.threadId }),
          ...(input?.eventType === undefined ? {} : { eventType: input.eventType }),
        }).pipe(Stream.filter(matches));
      return Stream.unwrap(
        Effect.gen(function* () {
          let pubsub = liveEvents;
          if (input?.eventType !== undefined) {
            const existing = liveEventsByType.get(input.eventType);
            if (existing !== undefined) {
              pubsub = existing;
            } else {
              const created = yield* PubSub.unbounded<OrchestrationV2StoredEvent>();
              pubsub = liveEventsByType.get(input.eventType) ?? created;
              liveEventsByType.set(input.eventType, pubsub);
            }
          }
          if (input?.bounded === true) {
            return replayAndBufferProjectedLiveEvents({
              subscribe: PubSub.subscribe(pubsub),
              latestSequence: eventStore.latestSequence(),
              afterSequence,
              filter: matches,
              replay,
              project: (stored) => ({ ...stored, event: projectDomainEventForWire(stored.event) }),
            });
          }
          const subscription = yield* PubSub.subscribe(pubsub);
          const highWater = yield* eventStore.latestSequence();
          const live = Stream.fromSubscription(subscription).pipe(
            Stream.filter((stored) => stored.sequence > Math.max(highWater, afterSequence)),
            Stream.filter(matches),
          );
          return Stream.concat(replay(highWater), live);
        }),
      );
    };

    function stream(
      input: EventSinkStreamInput & { readonly bounded: true },
    ): Stream.Stream<PublicStoredEvent, EventSinkV2Error>;
    function stream(
      input?: EventSinkStreamInput & { readonly bounded?: false },
    ): Stream.Stream<OrchestrationV2StoredEvent, EventSinkV2Error>;
    function stream(
      input?: EventSinkStreamInput,
    ): Stream.Stream<PublicStoredEvent | OrchestrationV2StoredEvent, EventSinkV2Error>;
    function stream(
      input?: EventSinkStreamInput,
    ): Stream.Stream<PublicStoredEvent | OrchestrationV2StoredEvent, EventSinkV2Error> {
      return streamEffect(input).pipe(
        Stream.mapError(
          (cause) =>
            new EventSinkStreamError({
              ...(input?.threadId === undefined ? {} : { threadId: input.threadId }),
              ...(input?.afterSequence === undefined ? {} : { afterSequence: input.afterSequence }),
              cause,
            }),
        ),
      );
    }

    const encodeOrdinaryCommand = Schema.encodeEffect(OrchestrationV2Command);
    return EventSinkV2.of({
      readThreadRetainedAttachmentPaths: (threadId) =>
        commitTransaction
          .withTransaction(readQualifiedThreadRetainedAttachmentPathsEffect(threadId))
          .pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      readApplicationBirthRecord: (threadId) =>
        commitTransaction
          .withTransaction(readApplicationBirthRecordEffect(threadId))
          .pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      prepareImportedApplicationAttachmentInventory: (input) =>
        commitTransaction
          .withTransaction(prepareImportedApplicationAttachmentInventoryEffect(input))
          .pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      readImportedApplicationAttachmentInventory: (input) =>
        commitTransaction
          .withTransaction(readImportedApplicationAttachmentInventoryEffect(input))
          .pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      readAttachmentNamespaceCleanupObservation: (effectId) =>
        commitTransaction
          .withTransaction(readAttachmentNamespaceCleanupHistoryEffect(effectId))
          .pipe(
            Effect.map((history) => history[history.length - 1] ?? null),
            Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause })),
          ),
      readUnresolvedDeletionCleanupHolds: (threadId) =>
        commitTransaction
          .withTransaction(readUnresolvedDeletionCleanupHoldsEffect(threadId))
          .pipe(Effect.mapError((cause) => new EventSinkWriteError({ eventCount: 0, cause }))),
      ...nativeRuntimeEvidence,
      ordinaryCheckoutLifetime: checkoutStore,
      validateOrdinaryCheckoutCommandReplay: (command, threadId) =>
        Effect.gen(function* () {
          const binding = (yield* checkoutStore.readCurrentCommands(command.commandId)).find(
            (item) => item.threadId === threadId,
          );
          const encoded = yield* encodeOrdinaryCommand(command);
          if (
            binding === undefined ||
            binding.commandDigest !== ordinaryCheckoutCommandDigestV1(encoded)
          )
            return yield* new EventSinkWriteError({
              commandId: command.commandId,
              eventCount: 0,
              cause: "Replay has another permanent original checkout command.",
            });
        }).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({ commandId: command.commandId, eventCount: 0, cause }),
          ),
        ),
      validateOrdinaryCheckoutReplay: (input) =>
        Effect.gen(function* () {
          const admission = yield* checkoutStore.readAdmission(
            input.capture.commandId,
            input.capture.threadId,
          );
          if (admission === null || admission.capture.commandDigest !== input.capture.commandDigest)
            return yield* new EventSinkWriteError({
              commandId: input.capture.commandId,
              eventCount: 0,
              cause: "Replay has no matching permanent checkout command.",
            });
          yield* checkoutStore.current(input);
        }).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                commandId: input.capture.commandId,
                eventCount: 0,
                cause,
              }),
          ),
        ),
      captureOrdinaryCheckout: (input) =>
        checkoutStore.capture(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                commandId: input.command.commandId,
                eventCount: 0,
                cause,
              }),
          ),
        ),
      commitLegacyPreflight,
      write: (input) =>
        writeEffect({ ...input, effects: [] }).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                eventCount: input.events.length,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                cause,
              }),
          ),
        ),
      writeWithEffects: (input) =>
        writeEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                eventCount: input.events.length,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                cause,
              }),
          ),
        ),
      writeIfRunCurrent: (input) =>
        writeIfRunCurrentEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                eventCount: input.events.length,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                cause,
              }),
          ),
        ),
      writeIfProviderThreadOwner: (input) =>
        writeIfProviderThreadOwnerEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                eventCount: input.events.length,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                cause,
              }),
          ),
        ),
      commitCommand: (input) =>
        commitCommandEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                commandId: input.commandId,
                eventCount: input.events.length,
                cause,
              }),
          ),
        ),
      commitRejectedCommand: (input) =>
        commitRejectedCommandEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                commandId: input.commandId,
                eventCount: 0,
                cause,
              }),
          ),
        ),
      commitProjectCommand: (input) =>
        commitProjectCommandEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({ commandId: input.commandId, eventCount: 1, cause }),
          ),
        ),
      commitRejectedProjectCommand: (input) =>
        commitRejectedProjectCommandEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({ commandId: input.commandId, eventCount: 0, cause }),
          ),
        ),
      stream,
      latestSequence: (input) =>
        eventStore.latestSequence(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkStreamError({
                ...(input?.threadId === undefined ? {} : { threadId: input.threadId }),
                cause,
              }),
          ),
        ),
      readByCommandId: (input) =>
        eventStore.readByCommandId(input).pipe(
          Stream.filter(isPublicStoredOrchestrationEvent),
          Stream.mapError(
            (cause) =>
              new EventSinkStreamError({
                cause,
              }),
          ),
        ),
    } satisfies EventSinkV2Shape);
  }),
);

/**
 * Event sink layer for application compositions that already own the
 * persistence services. Keeping the outbox instance shared with the worker is
 * important because enqueue notifications are in-memory wakeups backed by the
 * durable SQL queue.
 */
export const layerFromStores = baseLayer;

export const layer: Layer.Layer<
  EventSinkV2,
  never,
  EventStore.EventStoreV2 | ProjectionStore.ProjectionStoreV2 | SqlClient.SqlClient
> = baseLayer.pipe(
  Layer.provide(
    Layer.mergeAll(
      CommandReceiptStore.layer,
      EffectOutbox.layer,
      ProjectStore.layer,
      TurnItemPositionStore.layer,
    ),
  ),
);
