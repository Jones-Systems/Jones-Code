import {
  type ApplicationProjectEvent,
  CommandId,
  EventId,
  RunId,
  ThreadId,
  OrchestrationDispatchTarget,
  ThreadTurnDispatchGuard,
  type OrchestrationV2ServerCommand,
  NonNegativeInt,
  OrchestrationV2AppThread,
  OrchestrationV2AppThreadJson,
  OrchestrationV2DomainEvent,
  OrchestrationV2DomainEventJson,
  OrchestrationV2LegacyBootstrapPolicy,
  OrchestrationV2PrivateEvent,
  OrchestrationV2Run,
  OrchestrationV2RunJson,
  OrchestrationV2ThreadProjection,
  OrchestrationV2ThreadProjectionJson,
  OrchestrationV2ThreadDetailSnapshot,
  OrchestrationV2ThreadBoundedSnapshot,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as SchemaParser from "effect/SchemaParser";

const legacyOwnedTerminalControlShape = Schema.Struct({
  version: Schema.Literal(1),
  policy: OrchestrationV2LegacyBootstrapPolicy,
  runId: RunId,
  threadId: ThreadId,
  claimEventId: EventId,
  claimSequence: NonNegativeInt,
  claimReceiptSequence: NonNegativeInt,
  birthEventId: EventId,
  birthSequence: NonNegativeInt,
  birthReceiptSequence: NonNegativeInt,
  preparationGeneration: Schema.String,
  terminalId: Schema.String,
  generation: Schema.String,
});
export const LegacyOwnedTerminalControl = Schema.declareConstructor<
  typeof legacyOwnedTerminalControlShape.Type,
  typeof legacyOwnedTerminalControlShape.Encoded
>()(
  [legacyOwnedTerminalControlShape],
  ([codec]) =>
    (input, _ast, options) =>
      SchemaParser.decodeUnknownEffect(codec)(input, { ...options, onExcessProperty: "error" }),
);
export type LegacyOwnedTerminalControl = typeof LegacyOwnedTerminalControl.Type;
const legacyNoTerminalControlShape = Schema.Struct({
  version: Schema.Literal(1),
  type: Schema.Literal("no_control"),
  policy: OrchestrationV2LegacyBootstrapPolicy,
  runId: RunId,
  threadId: ThreadId,
  claimEventId: EventId,
  claimSequence: NonNegativeInt,
  claimReceiptSequence: NonNegativeInt,
  birthEventId: EventId,
  birthSequence: NonNegativeInt,
  birthReceiptSequence: NonNegativeInt,
  preparationGeneration: Schema.String,
  workspacePath: Schema.String,
  projectWorkspaceRoot: Schema.String,
});
export const LegacyNoTerminalControl = Schema.declareConstructor<
  typeof legacyNoTerminalControlShape.Type,
  typeof legacyNoTerminalControlShape.Encoded
>()(
  [legacyNoTerminalControlShape],
  ([codec]) =>
    (input, _ast, options) =>
      SchemaParser.decodeUnknownEffect(codec)(input, { ...options, onExcessProperty: "error" }),
);
export type LegacyNoTerminalControl = typeof LegacyNoTerminalControl.Type;

const recordedThreadFields = {
  legacyBootstrapClaim: Schema.optional(OrchestrationV2LegacyBootstrapPolicy),
};
const legacyDeletionProvenanceFields = {
  version: Schema.Literal(1),
  commandId: CommandId,
  threadId: ThreadId,
  runId: RunId,
  policy: OrchestrationV2LegacyBootstrapPolicy,
  claimEventId: EventId,
  claimSequence: NonNegativeInt,
  claimReceiptSequence: NonNegativeInt,
  birthEventId: EventId,
  birthSequence: NonNegativeInt,
  birthReceiptSequence: NonNegativeInt,
  preparationGeneration: Schema.String,
  workspacePath: Schema.String,
  projectWorkspaceRoot: Schema.String,
  evidenceEventId: EventId,
};
const legacyDeletionProvenanceShape = Schema.Union([
  Schema.Struct({
    ...legacyDeletionProvenanceFields,
    type: Schema.Literal("bound_control"),
    control: LegacyOwnedTerminalControl,
  }),
  Schema.Struct({
    ...legacyDeletionProvenanceFields,
    type: Schema.Literal("no_control"),
    control: LegacyNoTerminalControl,
  }),
]);
export const LegacyDeletionProvenance = Schema.declareConstructor<
  typeof legacyDeletionProvenanceShape.Type,
  typeof legacyDeletionProvenanceShape.Encoded
>()(
  [legacyDeletionProvenanceShape],
  ([codec]) =>
    (input, _ast, options) =>
      SchemaParser.decodeUnknownEffect(codec)(input, { ...options, onExcessProperty: "error" }),
);
export type LegacyDeletionProvenance = typeof LegacyDeletionProvenance.Type;

export const LegacyReleaseDecision = Schema.Struct({
  version: Schema.Literal(1),
  status: Schema.Literal("rejected"),
  policy: OrchestrationV2LegacyBootstrapPolicy,
  claimEventId: EventId,
  claimSequence: NonNegativeInt,
  claimReceiptSequence: NonNegativeInt,
  birthEventId: EventId,
  birthSequence: NonNegativeInt,
  birthReceiptSequence: NonNegativeInt,
  evidenceEventId: EventId,
  deletion: Schema.optional(LegacyDeletionProvenance),
  guard: ThreadTurnDispatchGuard,
  reason: Schema.String,
  observed: Schema.Struct({
    snapshotSequence: NonNegativeInt,
    lastEventSequence: NonNegativeInt,
    target: Schema.NullOr(OrchestrationDispatchTarget),
  }),
});
export type LegacyReleaseDecision = typeof LegacyReleaseDecision.Type;

const legacyWorktreeInput = Schema.Struct({
  cwd: Schema.String,
  args: Schema.Array(Schema.String),
  worktreePath: Schema.String,
  commonDirectory: Schema.String,
  baseCommitOid: Schema.String,
  targetRef: Schema.String,
});
const legacySetupDefinition = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  command: Schema.String,
  async: Schema.Boolean,
  runOnWorktreeCreate: Schema.Boolean,
  definitionHash: Schema.String,
  projectCwd: Schema.String,
  cwd: Schema.String,
  terminalId: Schema.String,
  generation: Schema.String,
  shell: Schema.String,
  shellArgs: Schema.Array(Schema.String),
  commandLine: Schema.String,
  completionToken: Schema.NullOr(Schema.String),
  env: Schema.Struct({
    T3CODE_PROJECT_ROOT: Schema.String,
    T3CODE_WORKTREE_PATH: Schema.optional(Schema.String),
    COLORTERM: Schema.Literal(""),
    NO_COLOR: Schema.Literal("1"),
    FORCE_COLOR: Schema.Literal("0"),
  }),
});
const legacyMaterialClaim = Schema.Struct({
  path: Schema.String,
  realPath: Schema.String,
  device: Schema.String,
  inode: Schema.String,
  parentRealPath: Schema.String,
  gitDirectory: Schema.String,
  commonDirectory: Schema.String,
  registeredPath: Schema.String,
  headRef: Schema.String,
  headOid: Schema.String,
});
const legacyStepInput = Schema.Union([
  Schema.Struct({
    kind: Schema.Literals(["worktree.add", "worktree.submodules", "worktree.base-config"]),
    input: legacyWorktreeInput,
  }),
  Schema.Struct({
    kind: Schema.Literal("branch.rename"),
    input: Schema.Struct({
      claim: legacyMaterialClaim,
      oldRef: Schema.String,
      oldOid: Schema.String,
      targetRef: Schema.String,
      exactName: Schema.Boolean,
      args: Schema.Array(Schema.String),
    }),
  }),
  Schema.Struct({ kind: Schema.Literal("setup.open"), input: legacySetupDefinition }),
  Schema.Struct({
    kind: Schema.Literal("setup.write"),
    input: Schema.Struct({
      terminalId: Schema.String,
      generation: Schema.String,
      definitionHash: Schema.String,
      commandLine: Schema.String,
      completionToken: Schema.NullOr(Schema.String),
    }),
  }),
  Schema.Struct({
    kind: Schema.Literal("setup.completion"),
    input: Schema.Struct({
      terminalId: Schema.String,
      generation: Schema.String,
      definitionHash: Schema.String,
      completionToken: Schema.String,
    }),
  }),
  Schema.Struct({
    kind: Schema.Literal("terminal.close"),
    input: Schema.Struct({
      terminalId: Schema.String,
      generation: Schema.String,
      deleteHistory: Schema.Boolean,
    }),
  }),
  Schema.Struct({
    kind: Schema.Literal("worktree.remove"),
    input: Schema.Struct({
      claim: legacyMaterialClaim,
      cwd: Schema.String,
      force: Schema.Boolean,
      args: Schema.Array(Schema.String),
    }),
  }),
]);
const legacyPreparationStep = Schema.Struct({
  effectId: Schema.String,
  inputHash: Schema.String,
  effect: legacyStepInput,
  intentCommandId: CommandId,
  intentEventId: EventId,
  state: Schema.Literals([
    "intent",
    "known_no_effect_failure",
    "known_succeeded",
    "known_started",
    "known_completed_failure",
    "known_partial",
    "unknown",
    "known_cancelled_cleaned",
  ]),
  outcomeCommandId: Schema.optional(CommandId),
  outcomeEventId: Schema.optional(EventId),
  evidence: Schema.optional(
    Schema.Union([
      Schema.Struct({
        type: Schema.Literal("never_invoked"),
        owner: Schema.Literals(["git", "terminal", "setup"]),
        reason: Schema.Literals(["admission_refused", "input_validation_failed"]),
      }),
      Schema.Struct({ type: Schema.Literal("worktree_claim"), claim: legacyMaterialClaim }),
      Schema.Struct({
        type: Schema.Literal("settled_git"),
        exitCode: Schema.Number,
        claim: Schema.optional(legacyMaterialClaim),
      }),
      Schema.Struct({
        type: Schema.Literal("terminal_generation"),
        terminalId: Schema.String,
        generation: Schema.String,
        shell: Schema.String,
        shellArgs: Schema.Array(Schema.String),
      }),
      Schema.Struct({
        type: Schema.Literal("terminal_write"),
        terminalId: Schema.String,
        generation: Schema.String,
        inputCount: NonNegativeInt,
      }),
      Schema.Struct({
        type: Schema.Literal("setup_completion"),
        terminalId: Schema.String,
        generation: Schema.String,
        exitCode: Schema.NullOr(Schema.Number),
        durationMs: NonNegativeInt,
      }),
      Schema.Struct({
        type: Schema.Literal("control_stopped"),
        terminalId: Schema.String,
        generation: Schema.String,
      }),
      Schema.Struct({
        type: Schema.Literal("unknown"),
        reason: Schema.Literals([
          "outcome_lost",
          "partial_material",
          "ownership_changed",
          "owner_refused",
          "process_result_unavailable",
        ]),
      }),
    ]),
  ),
});
const legacyPreparationShape = Schema.Struct({
  version: Schema.Literal(1),
  policy: OrchestrationV2LegacyBootstrapPolicy,
  claimEventId: EventId,
  claimSequence: NonNegativeInt,
  claimReceiptSequence: NonNegativeInt,
  birthEventId: EventId,
  birthSequence: NonNegativeInt,
  birthReceiptSequence: NonNegativeInt,
  generation: Schema.String,
  projectWorkspaceRoot: Schema.String,
  commonDirectory: Schema.NullOr(Schema.String),
  setup: Schema.Union([
    Schema.Struct({ status: Schema.Literals(["unresolved", "opted_out", "no_script"]) }),
    Schema.Struct({ status: Schema.Literal("resolved"), definition: legacySetupDefinition }),
  ]),
  steps: Schema.Array(legacyPreparationStep),
});
export const LegacyPreparation = Schema.declareConstructor<
  typeof legacyPreparationShape.Type,
  typeof legacyPreparationShape.Encoded
>()(
  [legacyPreparationShape],
  ([codec]) =>
    (input, _ast, options) =>
      SchemaParser.decodeUnknownEffect(codec)(input, { ...options, onExcessProperty: "error" }),
);
export type LegacyPreparation = typeof LegacyPreparation.Type;
const legacyPreparationUpdateShape = Schema.Union([
  Schema.Struct({ type: Schema.Literal("initialize"), preparation: LegacyPreparation }),
  Schema.Struct({ type: Schema.Literals(["intent", "outcome"]), step: legacyPreparationStep }),
  Schema.Struct({
    type: Schema.Literal("setup-policy"),
    setup: legacyPreparationShape.fields.setup,
  }),
]);
export const LegacyPreparationUpdate = Schema.declareConstructor<
  typeof legacyPreparationUpdateShape.Type,
  typeof legacyPreparationUpdateShape.Encoded
>()(
  [legacyPreparationUpdateShape],
  ([codec]) =>
    (input, _ast, options) =>
      SchemaParser.decodeUnknownEffect(codec)(input, { ...options, onExcessProperty: "error" }),
);
export type LegacyPreparationUpdate = typeof LegacyPreparationUpdate.Type;

interface LegacyGuardRejectionDeleteIdentity {
  readonly type: "legacy-bootstrap.guard-rejection-delete";
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly legacyBootstrap: typeof OrchestrationV2LegacyBootstrapPolicy.Type;
}
export type LegacyGuardRejectionDeleteCommand = LegacyGuardRejectionDeleteIdentity &
  (
    | { readonly legacyOwnedControl: LegacyOwnedTerminalControl; readonly legacyNoControl?: never }
    | { readonly legacyNoControl: LegacyNoTerminalControl; readonly legacyOwnedControl?: never }
  );

export type RecordedServerCommand =
  | Exclude<OrchestrationV2ServerCommand, { readonly type: "prepared-run.progress" }>
  | (Extract<OrchestrationV2ServerCommand, { readonly type: "prepared-run.progress" }> & {
      readonly legacyPreparationUpdate?: LegacyPreparationUpdate;
    });

const recordedRunFields = {
  legacyPreparation: Schema.optional(LegacyPreparation),
  legacyReleaseDecision: Schema.optional(LegacyReleaseDecision),
  workspaceRunSetupScript: Schema.optional(Schema.Boolean),
  legacyBootstrap: Schema.optional(OrchestrationV2LegacyBootstrapPolicy),
  legacyPreparationFailureKnown: Schema.optional(Schema.Boolean),
};

export const RecordedAppThread = Schema.Struct({
  ...OrchestrationV2AppThread.fields,
  ...recordedThreadFields,
});
export type RecordedAppThread = typeof RecordedAppThread.Type;
export const RecordedAppThreadJson = Schema.Struct({
  ...OrchestrationV2AppThreadJson.fields,
  ...recordedThreadFields,
});
export type RecordedAppThreadJson = typeof RecordedAppThreadJson.Type;

export const RecordedRun = Schema.Struct({
  ...OrchestrationV2Run.fields,
  ...recordedRunFields,
});
export type RecordedRun = typeof RecordedRun.Type;
export const RecordedRunJson = Schema.Struct({
  ...OrchestrationV2RunJson.fields,
  ...recordedRunFields,
});
export type RecordedRunJson = typeof RecordedRunJson.Type;

const [threadCreated, threadLifecycle, runCreated, runUpdated, ...ordinaryEvents] =
  OrchestrationV2DomainEvent.members;
export const RecordedLifecycleEvent = Schema.Union([
  threadCreated.mapFields((fields) => ({ ...fields, payload: RecordedAppThread })),
  threadLifecycle.mapFields((fields) => ({ ...fields, payload: RecordedAppThread })),
  runCreated.mapFields((fields) => ({ ...fields, payload: RecordedRun })),
  runUpdated.mapFields((fields) => ({ ...fields, payload: RecordedRun })),
  ...ordinaryEvents,
]);
export type RecordedLifecycleEvent = typeof RecordedLifecycleEvent.Type;

const [threadCreatedJson, threadLifecycleJson, runCreatedJson, runUpdatedJson, ...ordinaryJson] =
  OrchestrationV2DomainEventJson.members;
export const RecordedLifecycleEventJson = Schema.Union([
  threadCreatedJson.mapFields((fields) => ({ ...fields, payload: RecordedAppThreadJson })),
  threadLifecycleJson.mapFields((fields) => ({ ...fields, payload: RecordedAppThreadJson })),
  runCreatedJson.mapFields((fields) => ({ ...fields, payload: RecordedRunJson })),
  runUpdatedJson.mapFields((fields) => ({ ...fields, payload: RecordedRunJson })),
  ...ordinaryJson,
]);
export type RecordedLifecycleEventJson = typeof RecordedLifecycleEventJson.Type;

export const RecordedEvent = Schema.Union([RecordedLifecycleEvent, OrchestrationV2PrivateEvent]);
export type RecordedEvent = typeof RecordedEvent.Type;
export const RecordedEventJson = Schema.Union([
  RecordedLifecycleEventJson,
  ...OrchestrationV2PrivateEvent.members.map((event) =>
    event.mapFields((fields) => ({ ...fields, occurredAt: Schema.DateTimeUtcFromString })),
  ),
]);
export type RecordedEventJson = typeof RecordedEventJson.Type;

export const RecordedStoredEvent = Schema.Struct({
  sequence: NonNegativeInt,
  commandId: Schema.NullOr(CommandId),
  event: RecordedEvent,
});
export type RecordedStoredEvent = typeof RecordedStoredEvent.Type;
export const RecordedStoredLifecycleEvent = Schema.Struct({
  ...RecordedStoredEvent.fields,
  event: RecordedLifecycleEvent,
});
export type RecordedStoredLifecycleEvent = typeof RecordedStoredLifecycleEvent.Type;
export const RecordedStoredEventJson = Schema.Struct({
  ...RecordedStoredEvent.fields,
  event: RecordedEventJson,
});
export type RecordedStoredEventJson = typeof RecordedStoredEventJson.Type;

export const RecordedThreadProjection = Schema.Struct({
  ...OrchestrationV2ThreadProjection.fields,
  thread: RecordedAppThread,
  runs: Schema.Array(RecordedRun),
});
export type RecordedThreadProjection = typeof RecordedThreadProjection.Type;
export const RecordedThreadProjectionJson = Schema.Struct({
  ...OrchestrationV2ThreadProjectionJson.fields,
  thread: RecordedAppThreadJson,
  runs: Schema.Array(RecordedRunJson),
});
export type RecordedThreadProjectionJson = typeof RecordedThreadProjectionJson.Type;
export const RecordedThreadSnapshot = Schema.Struct({
  schemaVersion: NonNegativeInt,
  snapshotSequence: NonNegativeInt,
  projection: RecordedThreadProjection,
});
export type RecordedThreadSnapshot = typeof RecordedThreadSnapshot.Type;
export const RecordedThreadDetailSnapshot = Schema.Struct({
  ...OrchestrationV2ThreadDetailSnapshot.fields,
  projection: RecordedThreadProjection,
});
export type RecordedThreadDetailSnapshot = typeof RecordedThreadDetailSnapshot.Type;
export const RecordedThreadBoundedSnapshot = Schema.Struct({
  ...OrchestrationV2ThreadBoundedSnapshot.fields,
  projection: RecordedThreadProjection,
});
export type RecordedThreadBoundedSnapshot = typeof RecordedThreadBoundedSnapshot.Type;
export type ApplicationRecordedEvent = ApplicationProjectEvent | RecordedStoredEvent;
export type ApplicationRecordedLifecycleEvent =
  | ApplicationProjectEvent
  | RecordedStoredLifecycleEvent;
