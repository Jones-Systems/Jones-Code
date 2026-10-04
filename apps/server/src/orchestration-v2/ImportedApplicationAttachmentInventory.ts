import {
  AgentSessionImportSource,
  ChatAttachment,
  EventId,
  IsoDateTime,
  MessageId,
  ProjectId,
  RunId,
  ThreadId,
  TurnId,
  TrimmedNonEmptyString,
  UserInputAttachmentAnswerPayload,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { attachmentRelativePath } from "../attachmentStore.ts";
import { normalizeAttachmentRelativePath } from "../attachmentPaths.ts";
import { nativeCreationCanonicalJson, nativeCreationSha256 } from "./NativeCreationPreparation.ts";

const closed = <Fields extends Schema.Struct.Fields>(fields: Fields) => {
  const schema = Schema.Struct(fields);
  return Schema.flip(
    Schema.flip(schema).check(
      Schema.makeFilter((value) =>
        Reflect.ownKeys(value).every((key) => Object.hasOwn(fields, key)),
      ),
    ),
  );
};
const sha256 = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/));
const nonnegative = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const positive = Schema.Int.check(Schema.isGreaterThan(0));
export const importedApplicationAttachmentCanonicalJsonV1 = nativeCreationCanonicalJson;
export const importedApplicationAttachmentSha256V1 = (value: unknown): string =>
  nativeCreationSha256(nativeCreationCanonicalJson(value));

export const ImportedApplicationAttachmentBirthV1 = closed({
  kind: Schema.Literal("application_v2_thread_birth"),
  threadId: ThreadId,
  eventId: EventId,
  sequence: positive,
});
export type ImportedApplicationAttachmentBirthV1 = typeof ImportedApplicationAttachmentBirthV1.Type;
export const ImportedApplicationAttachmentEventBasisV1 = closed({
  eventId: EventId,
  sequence: positive,
});
export type ImportedApplicationAttachmentEventBasisV1 =
  typeof ImportedApplicationAttachmentEventBasisV1.Type;
export const ImportedApplicationAttachmentSourceV1 = Schema.Union([
  closed({
    kind: Schema.Literal("legacy_projection"),
    legacyBirth: ImportedApplicationAttachmentEventBasisV1,
    projectId: ProjectId,
    sourceCreatedAt: IsoDateTime,
    sourceCut: closed({
      legacyEventSequence: nonnegative,
      projectorPositions: closed({
        threads: nonnegative,
        messages: nonnegative,
        activities: nonnegative,
        turns: nonnegative,
      }),
      messageRowsSha256: sha256,
      answerRowsSha256: sha256,
    }),
  }),
  closed({
    kind: Schema.Literal("native_import_batch"),
    parserPolicy: Schema.Literal("agent_session_visible_messages_v1"),
    source: AgentSessionImportSource,
    birth: ImportedApplicationAttachmentBirthV1,
    eventsSha256: sha256,
    eventBasis: Schema.Array(ImportedApplicationAttachmentEventBasisV1),
    messageCount: nonnegative,
  }),
]);
export type ImportedApplicationAttachmentSourceV1 =
  typeof ImportedApplicationAttachmentSourceV1.Type;
export const ImportedApplicationAttachmentCarrierV1 = Schema.Union([
  closed({
    kind: Schema.Literal("legacy_message"),
    messageId: MessageId,
    sourceThreadId: ThreadId,
    turnId: Schema.NullOr(TurnId),
    role: TrimmedNonEmptyString,
    createdAt: IsoDateTime,
    updatedAt: IsoDateTime,
    attachmentsJson: Schema.NullOr(Schema.String),
    attachments: Schema.Array(ChatAttachment),
    sourceRowSha256: sha256,
  }),
  closed({
    kind: Schema.Literal("legacy_answer"),
    activityId: TrimmedNonEmptyString,
    sourceThreadId: ThreadId,
    turnId: Schema.NullOr(TurnId),
    sequence: Schema.NullOr(nonnegative),
    createdAt: IsoDateTime,
    payloadJson: Schema.String,
    answer: UserInputAttachmentAnswerPayload,
    sourceRowSha256: sha256,
  }),
]);
export type ImportedApplicationAttachmentCarrierV1 =
  typeof ImportedApplicationAttachmentCarrierV1.Type;
export const ImportedApplicationAttachmentInventoryV1 = closed({
  version: Schema.Literal(1),
  domain: Schema.Literal("jones_materialized_attachment_references/v1"),
  inventoryId: sha256,
  applicationBirth: ImportedApplicationAttachmentBirthV1,
  projectId: ProjectId,
  source: ImportedApplicationAttachmentSourceV1,
  sourceHistoryCoverage: Schema.Literals([
    "legacy_materialized_projection",
    "native_visible_message_subset",
  ]),
  completeness: Schema.Literal("complete_application_refs"),
  messageCarrierCount: nonnegative,
  answerCarrierCount: nonnegative,
  attachmentReferenceCount: nonnegative,
  carrierSetSha256: sha256,
  recordedAt: IsoDateTime,
});
export type ImportedApplicationAttachmentInventoryV1 =
  typeof ImportedApplicationAttachmentInventoryV1.Type;
export const ImportedApplicationAttachmentSnapshotV1 = closed({
  header: ImportedApplicationAttachmentInventoryV1,
  carriers: Schema.Array(ImportedApplicationAttachmentCarrierV1),
});
export type ImportedApplicationAttachmentSnapshotV1 =
  typeof ImportedApplicationAttachmentSnapshotV1.Type;
export const ImportedApplicationAttachmentQualificationV1 = Schema.Union([
  closed({
    status: Schema.Literal("complete"),
    inventory: ImportedApplicationAttachmentSnapshotV1,
  }),
  closed({ status: Schema.Literal("unavailable"), reason: TrimmedNonEmptyString }),
]);
export type ImportedApplicationAttachmentQualificationV1 =
  typeof ImportedApplicationAttachmentQualificationV1.Type;
export const ImportedApplicationAttachmentForkBasisV1 = closed({
  targetBirth: ImportedApplicationAttachmentBirthV1,
  sourceBirth: ImportedApplicationAttachmentBirthV1,
  sourceRunId: RunId,
  sourceRunOrdinal: positive,
  sourceRunEvent: ImportedApplicationAttachmentEventBasisV1,
  forkEvent: ImportedApplicationAttachmentEventBasisV1,
});
export type ImportedApplicationAttachmentForkBasisV1 =
  typeof ImportedApplicationAttachmentForkBasisV1.Type;
export const ImportedApplicationAttachmentSegmentV1 = closed({
  inventoryId: sha256,
  applicationBirth: ImportedApplicationAttachmentBirthV1,
  carrierSetSha256: sha256,
  forkBasis: Schema.Array(ImportedApplicationAttachmentForkBasisV1),
});
export type ImportedApplicationAttachmentSegmentV1 =
  typeof ImportedApplicationAttachmentSegmentV1.Type;
export const ImportedApplicationAttachmentRetentionEvidenceV1 = closed({
  version: Schema.Literal(1),
  segments: Schema.Array(ImportedApplicationAttachmentSegmentV1),
  visibleV2CarrierSetSha256: sha256,
  retentionBasisSha256: sha256,
});
export type ImportedApplicationAttachmentRetentionEvidenceV1 =
  typeof ImportedApplicationAttachmentRetentionEvidenceV1.Type;

export function importedApplicationAttachmentCarrierIdV1(
  carrier: ImportedApplicationAttachmentCarrierV1,
): string {
  return carrier.kind === "legacy_message" ? carrier.messageId : carrier.activityId;
}
export function canonicalImportedApplicationAttachmentCarriersV1(
  carriers: ReadonlyArray<ImportedApplicationAttachmentCarrierV1>,
): ReadonlyArray<ImportedApplicationAttachmentCarrierV1> {
  return carriers.toSorted((left, right) => {
    const a = `${left.kind}:${importedApplicationAttachmentCarrierIdV1(left)}`;
    const b = `${right.kind}:${importedApplicationAttachmentCarrierIdV1(right)}`;
    return a < b ? -1 : a > b ? 1 : 0;
  });
}
export function makeImportedApplicationAttachmentInventoryIdV1(
  header: Omit<ImportedApplicationAttachmentInventoryV1, "inventoryId" | "recordedAt">,
): string {
  const {
    inventoryId: _inventoryId,
    recordedAt: _recordedAt,
    ...identity
  } = header as ImportedApplicationAttachmentInventoryV1;
  return importedApplicationAttachmentSha256V1(identity);
}
export type ImportedApplicationAttachmentPathsV1 =
  | {
      readonly status: "complete";
      readonly relativePaths: ReadonlyArray<string>;
      readonly messageCarrierCount: number;
      readonly answerCarrierCount: number;
      readonly attachmentReferenceCount: number;
      readonly carrierSetSha256: string;
    }
  | { readonly status: "unavailable"; readonly reason: string };
export function collectImportedApplicationAttachmentPathsV1(
  carriers: ReadonlyArray<ImportedApplicationAttachmentCarrierV1>,
): ImportedApplicationAttachmentPathsV1 {
  const ordered = canonicalImportedApplicationAttachmentCarriersV1(carriers);
  const seen = new Set<string>();
  const paths = new Set<string>();
  let messageCarrierCount = 0;
  let answerCarrierCount = 0;
  let attachmentReferenceCount = 0;
  for (const carrier of ordered) {
    if (!Schema.is(ImportedApplicationAttachmentCarrierV1)(carrier)) {
      return { status: "unavailable", reason: "carrier_decode_unavailable" };
    }
    const identity = `${carrier.kind}:${importedApplicationAttachmentCarrierIdV1(carrier)}`;
    if (seen.has(identity)) return { status: "unavailable", reason: "carrier_identity_duplicate" };
    seen.add(identity);
    let attachments: ReadonlyArray<ChatAttachment>;
    if (carrier.kind === "legacy_message") {
      messageCarrierCount++;
      const decoded =
        carrier.attachmentsJson === null
          ? Option.some([])
          : Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Array(ChatAttachment)))(
              carrier.attachmentsJson,
            );
      if (
        Option.isNone(decoded) ||
        nativeCreationCanonicalJson(decoded.value) !==
          nativeCreationCanonicalJson(carrier.attachments)
      ) {
        return { status: "unavailable", reason: "carrier_decode_unavailable" };
      }
      attachments = carrier.attachments;
    } else {
      answerCarrierCount++;
      const decoded = Schema.decodeUnknownOption(
        Schema.fromJsonString(UserInputAttachmentAnswerPayload),
      )(carrier.payloadJson);
      if (
        Option.isNone(decoded) ||
        nativeCreationCanonicalJson(decoded.value) !== nativeCreationCanonicalJson(carrier.answer)
      ) {
        return { status: "unavailable", reason: "carrier_decode_unavailable" };
      }
      attachments = Object.values(carrier.answer.attachmentsByQuestionId).flat();
    }
    attachmentReferenceCount += attachments.length;
    for (const attachment of attachments) {
      const path = attachmentRelativePath(attachment);
      if (path === null || normalizeAttachmentRelativePath(path) !== path) {
        return { status: "unavailable", reason: "attachment_path_unavailable" };
      }
      paths.add(path);
    }
  }
  return {
    status: "complete",
    relativePaths: [...paths].sort(),
    messageCarrierCount,
    answerCarrierCount,
    attachmentReferenceCount,
    carrierSetSha256: importedApplicationAttachmentSha256V1(ordered),
  };
}
export function qualifyImportedApplicationAttachmentSnapshotV1(
  input: unknown,
): ImportedApplicationAttachmentQualificationV1 {
  const decoded = Schema.decodeUnknownOption(ImportedApplicationAttachmentSnapshotV1)(input);
  if (Option.isNone(decoded))
    return { status: "unavailable", reason: "inventory_decode_unavailable" };
  const inventory = decoded.value;
  const { header, carriers } = inventory;
  const paths = collectImportedApplicationAttachmentPathsV1(carriers);
  if (paths.status === "unavailable") return paths;
  const { inventoryId, recordedAt: _recordedAt, ...identity } = header;
  const native = header.source.kind === "native_import_batch";
  // A const binding keeps the variant narrowing inside the callbacks below.
  const source = header.source;
  if (
    source.kind === "legacy_projection" &&
    (source.sourceCut.legacyEventSequence < source.legacyBirth.sequence ||
      Object.values(source.sourceCut.projectorPositions).some(
        (position) => position < source.sourceCut.legacyEventSequence,
      ))
  ) {
    return { status: "unavailable", reason: "source_cut_incomplete" };
  }
  if (
    source.kind === "native_import_batch" &&
    (source.eventBasis.length !== source.messageCount * 2 ||
      new Set(source.eventBasis.map((entry) => entry.eventId)).size !== source.eventBasis.length ||
      source.eventBasis.some(
        (entry, index) =>
          entry.sequence <=
          (index === 0 ? header.applicationBirth.sequence : source.eventBasis[index - 1]!.sequence),
      ))
  ) {
    return { status: "unavailable", reason: "native_event_basis_unavailable" };
  }
  if (
    inventoryId !== makeImportedApplicationAttachmentInventoryIdV1(identity) ||
    paths.carrierSetSha256 !== header.carrierSetSha256 ||
    paths.answerCarrierCount !== header.answerCarrierCount ||
    paths.attachmentReferenceCount !== header.attachmentReferenceCount ||
    (!native && paths.messageCarrierCount !== header.messageCarrierCount) ||
    carriers.some((carrier) => carrier.sourceThreadId !== header.applicationBirth.threadId) ||
    (native &&
      (carriers.length !== 0 ||
        header.messageCarrierCount !== header.source.messageCount ||
        nativeCreationCanonicalJson(header.source.birth) !==
          nativeCreationCanonicalJson(header.applicationBirth))) ||
    header.sourceHistoryCoverage !==
      (native ? "native_visible_message_subset" : "legacy_materialized_projection") ||
    (header.source.kind === "legacy_projection" && header.source.projectId !== header.projectId)
  ) {
    return { status: "unavailable", reason: "inventory_parity_unavailable" };
  }
  return {
    status: "complete",
    inventory: { header, carriers: canonicalImportedApplicationAttachmentCarriersV1(carriers) },
  };
}
export function makeImportedApplicationAttachmentRetentionEvidenceV1(input: {
  readonly segments: ImportedApplicationAttachmentRetentionEvidenceV1["segments"];
  readonly visibleV2CarrierSetSha256: string;
  readonly relativePaths: ReadonlyArray<string>;
}): ImportedApplicationAttachmentRetentionEvidenceV1 {
  const evidence = {
    version: 1 as const,
    segments: input.segments,
    visibleV2CarrierSetSha256: input.visibleV2CarrierSetSha256,
  };
  return {
    ...evidence,
    retentionBasisSha256: importedApplicationAttachmentSha256V1({
      ...evidence,
      relativePaths: [...new Set(input.relativePaths)].sort(),
    }),
  };
}

export function visibleImportedApplicationAttachmentSegments(input: {
  readonly targetBirth: ImportedApplicationAttachmentBirthV1;
  readonly visibleImportedBirths: ReadonlyArray<ImportedApplicationAttachmentBirthV1>;
  readonly inventories: ReadonlyArray<{
    readonly inventory: ImportedApplicationAttachmentSnapshotV1;
    readonly forkBasis: ReadonlyArray<ImportedApplicationAttachmentForkBasisV1>;
  }>;
}):
  | {
      readonly status: "complete";
      readonly relativePaths: ReadonlyArray<string>;
      readonly segments: ReadonlyArray<ImportedApplicationAttachmentSegmentV1>;
    }
  | { readonly status: "unavailable"; readonly reason: string } {
  if (!Schema.is(ImportedApplicationAttachmentBirthV1)(input.targetBirth)) {
    return { status: "unavailable", reason: "application_birth_unavailable" };
  }
  const sameBirth = (
    left: ImportedApplicationAttachmentBirthV1,
    right: ImportedApplicationAttachmentBirthV1,
  ) => nativeCreationCanonicalJson(left) === nativeCreationCanonicalJson(right);
  const segments: Array<ImportedApplicationAttachmentSegmentV1> = [];
  const paths = new Set<string>();
  const seen = new Set<string>();
  for (const birth of input.visibleImportedBirths) {
    if (!Schema.is(ImportedApplicationAttachmentBirthV1)(birth)) {
      return { status: "unavailable", reason: "application_birth_unavailable" };
    }
    const identity = nativeCreationCanonicalJson(birth);
    if (seen.has(identity)) continue;
    seen.add(identity);
    const candidates = input.inventories.filter(({ inventory }) =>
      sameBirth(inventory.header.applicationBirth, birth),
    );
    if (candidates.length !== 1)
      return { status: "unavailable", reason: "imported_inventory_unavailable" };
    const candidate = candidates[0]!;
    const qualified = qualifyImportedApplicationAttachmentSnapshotV1(candidate.inventory);
    if (qualified.status !== "complete") return qualified;
    let currentBirth = input.targetBirth;
    const forkBirths = new Set([nativeCreationCanonicalJson(currentBirth)]);
    for (const edge of candidate.forkBasis) {
      if (
        !Schema.is(ImportedApplicationAttachmentForkBasisV1)(edge) ||
        !sameBirth(edge.targetBirth, currentBirth) ||
        edge.sourceRunEvent.sequence <= edge.sourceBirth.sequence ||
        edge.forkEvent.sequence <= edge.sourceRunEvent.sequence ||
        edge.forkEvent.sequence < edge.targetBirth.sequence ||
        forkBirths.has(nativeCreationCanonicalJson(edge.sourceBirth))
      ) {
        return { status: "unavailable", reason: "fork_basis_unavailable" };
      }
      currentBirth = edge.sourceBirth;
      forkBirths.add(nativeCreationCanonicalJson(currentBirth));
    }
    if (!sameBirth(currentBirth, birth))
      return { status: "unavailable", reason: "fork_basis_unavailable" };
    const collected = collectImportedApplicationAttachmentPathsV1(qualified.inventory.carriers);
    if (collected.status !== "complete") return collected;
    for (const path of collected.relativePaths) paths.add(path);
    segments.push({
      inventoryId: qualified.inventory.header.inventoryId,
      applicationBirth: birth,
      carrierSetSha256: qualified.inventory.header.carrierSetSha256,
      forkBasis: candidate.forkBasis,
    });
  }
  return { status: "complete", relativePaths: [...paths].sort(), segments };
}
