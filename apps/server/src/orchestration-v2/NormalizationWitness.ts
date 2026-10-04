import {
  CommandId,
  NonNegativeInt,
  OrchestrationV2Command,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { nativeCreationCanonicalJson, nativeCreationSha256 } from "./NativeCreationPreparation.ts";
import { OrdinaryApplicationBirthV1 } from "./OrdinaryCheckoutOwnership.ts";

const sha256 = Schema.String.check(Schema.makeFilter((value) => /^[0-9a-f]{64}$/.test(value)));
export const NormalizationAttachmentV1 = Schema.Struct({
  pendingId: Schema.String,
  contentSha256: sha256,
  sizeBytes: NonNegativeInt,
  finalId: Schema.String,
});
export type NormalizationAttachmentV1 = typeof NormalizationAttachmentV1.Type;
export const NormalizationContextRemapV1 = Schema.Struct({
  sourceId: Schema.String,
  finalId: Schema.String,
});
export type NormalizationContextRemapV1 = typeof NormalizationContextRemapV1.Type;

export const NormalizationWitnessV1 = Schema.Struct({
  commandId: CommandId,
  commandType: Schema.Literals(["message.dispatch", "queued-run.edit", "runtime-request.respond"]),
  witnessVersion: Schema.Literal(1),
  requestDigest: sha256,
  attachments: Schema.Array(NormalizationAttachmentV1),
  contextRemaps: Schema.Array(NormalizationContextRemapV1),
  acceptedCommand: OrchestrationV2Command,
  acceptedCommandDigest: sha256,
  receiptSequence: NonNegativeInt,
  threadId: ThreadId,
  projectId: ProjectId,
  applicationBirth: OrdinaryApplicationBirthV1,
  createdAt: Schema.String,
});
export type NormalizationWitnessV1 = typeof NormalizationWitnessV1.Type;

export const NormalizationWitnessSqlRowV1 = Schema.Struct({
  command_id: CommandId,
  command_type: NormalizationWitnessV1.fields.commandType,
  witness_version: Schema.Literal(1),
  request_digest: sha256,
  attachments_json: Schema.String,
  context_remaps_json: Schema.String,
  accepted_command_json: Schema.String,
  accepted_command_digest: sha256,
  receipt_sequence: NonNegativeInt,
  thread_id: ThreadId,
  project_id: ProjectId,
  application_birth_json: Schema.String,
  created_at: Schema.String,
});
export type NormalizationWitnessSqlRowV1 = typeof NormalizationWitnessSqlRowV1.Type;

export const requestDigest = (rawInput: unknown): string =>
  nativeCreationSha256(nativeCreationCanonicalJson(rawInput));
export const acceptedCommandDigest = (command: OrchestrationV2Command): string =>
  requestDigest(Schema.encodeSync(OrchestrationV2Command)(command));

export function encodeNormalizationWitnessRow(
  row: NormalizationWitnessV1,
): NormalizationWitnessSqlRowV1 {
  return {
    command_id: row.commandId,
    command_type: row.commandType,
    witness_version: row.witnessVersion,
    request_digest: row.requestDigest,
    attachments_json: nativeCreationCanonicalJson(row.attachments),
    context_remaps_json: nativeCreationCanonicalJson(row.contextRemaps),
    accepted_command_json: nativeCreationCanonicalJson(
      Schema.encodeSync(OrchestrationV2Command)(row.acceptedCommand),
    ),
    accepted_command_digest: row.acceptedCommandDigest,
    receipt_sequence: row.receiptSequence,
    thread_id: row.threadId,
    project_id: row.projectId,
    application_birth_json: nativeCreationCanonicalJson(row.applicationBirth),
    created_at: row.createdAt,
  };
}

export const decodeNormalizationWitnessRow = Effect.fnUntraced(function* (input: unknown) {
  const row = yield* Schema.decodeUnknownEffect(NormalizationWitnessSqlRowV1)(input, {
    onExcessProperty: "error",
  });
  return {
    commandId: row.command_id,
    commandType: row.command_type,
    witnessVersion: row.witness_version,
    requestDigest: row.request_digest,
    attachments: yield* Schema.decodeUnknownEffect(
      Schema.fromJsonString(Schema.Array(NormalizationAttachmentV1)),
    )(row.attachments_json),
    contextRemaps: yield* Schema.decodeUnknownEffect(
      Schema.fromJsonString(Schema.Array(NormalizationContextRemapV1)),
    )(row.context_remaps_json),
    acceptedCommand: yield* Schema.decodeUnknownEffect(
      Schema.fromJsonString(OrchestrationV2Command),
    )(row.accepted_command_json),
    acceptedCommandDigest: row.accepted_command_digest,
    receiptSequence: row.receipt_sequence,
    threadId: row.thread_id,
    projectId: row.project_id,
    applicationBirth: yield* Schema.decodeUnknownEffect(
      Schema.fromJsonString(OrdinaryApplicationBirthV1),
    )(row.application_birth_json),
    createdAt: row.created_at,
  } satisfies NormalizationWitnessV1;
});

export interface NormalizationWitnessPreparation {
  readonly commandId: CommandId;
  readonly requestDigest: string;
  readonly attachments: ReadonlyArray<NormalizationAttachmentV1>;
  readonly contextRemaps: ReadonlyArray<NormalizationContextRemapV1>;
  readonly mode: "fresh" | "replay";
}

// Only the exact dispatch supplies this carrier; enclosing launch commands must not inherit it.
export class NormalizationWitnessCarrier extends Context.Reference<
  | (NormalizationWitnessPreparation & { readonly acceptedCommand: OrchestrationV2Command })
  | undefined
>("t3/orchestration-v2/NormalizationWitnessCarrier", { defaultValue: () => undefined }) {}

export class NormalizationWitnessSuperseded extends Schema.TaggedError<NormalizationWitnessSuperseded>()(
  "NormalizationWitnessSuperseded",
  { commandId: CommandId },
) {}
export class NormalizationWitnessConflict extends Schema.TaggedError<NormalizationWitnessConflict>()(
  "NormalizationWitnessConflict",
  { commandId: CommandId, reason: Schema.String },
) {}

export interface NormalizationWitnessReadResult {
  readonly witness: NormalizationWitnessV1 | null;
  readonly receipt: {
    readonly status: "accepted" | "rejected";
    readonly threadId: ThreadId;
    readonly commandType: string;
  } | null;
}
export type ReadNormalizationWitness<E> = (
  commandId: CommandId,
) => Effect.Effect<NormalizationWitnessReadResult, E>;

export const compareForReplay = Effect.fnUntraced(function* <E, R>(
  row: NormalizationWitnessV1,
  input: { readonly commandId: CommandId; readonly threadId: ThreadId; readonly rawInput: unknown },
  pendingProbe: (
    pendingId: string,
  ) => Effect.Effect<{ readonly contentSha256: string; readonly sizeBytes: number } | null, E, R>,
  currentBirth: OrdinaryApplicationBirthV1 | null,
) {
  const conflict = (reason: string) =>
    new NormalizationWitnessConflict({ commandId: input.commandId, reason });
  if (
    row.commandId !== input.commandId ||
    row.threadId !== input.threadId ||
    row.requestDigest !== requestDigest(input.rawInput)
  )
    return yield* conflict("request_changed");
  if (
    currentBirth === null ||
    currentBirth.threadId !== input.threadId ||
    nativeCreationCanonicalJson(currentBirth) !== nativeCreationCanonicalJson(row.applicationBirth)
  )
    return yield* conflict("application_birth_changed");
  const command = row.acceptedCommand;
  if (
    command.commandId !== row.commandId ||
    command.type !== row.commandType ||
    !("threadId" in command) ||
    command.threadId !== row.threadId ||
    acceptedCommandDigest(command) !== row.acceptedCommandDigest
  )
    return yield* conflict("accepted_command_changed");
  for (const attachment of row.attachments) {
    const pending = yield* pendingProbe(attachment.pendingId);
    if (
      pending !== null &&
      (pending.contentSha256 !== attachment.contentSha256 ||
        pending.sizeBytes !== attachment.sizeBytes)
    )
      return yield* conflict("pending_bytes_changed");
  }
  return command;
});
