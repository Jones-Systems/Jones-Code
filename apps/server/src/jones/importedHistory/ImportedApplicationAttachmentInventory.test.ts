import { describe, expect, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import {
  ImportedApplicationAttachmentBirthV1,
  ImportedApplicationAttachmentSourceV1,
  type ImportedApplicationAttachmentCarrierV1,
  type ImportedApplicationAttachmentInventoryV1,
  canonicalImportedApplicationAttachmentCarriersV1,
  collectImportedApplicationAttachmentPathsV1,
  importedApplicationAttachmentSha256V1,
  makeImportedApplicationAttachmentInventoryIdV1,
  makeImportedApplicationAttachmentRetentionEvidenceV1,
  qualifyImportedApplicationAttachmentSnapshotV1,
  visibleImportedApplicationAttachmentSegments,
} from "./ImportedApplicationAttachmentInventory.ts";

const threadId = ThreadId.make("thread:application-attachments");
const projectId = ProjectId.make("project:application-attachments");
const now = "2026-10-03T00:00:00.000Z";
const birth = {
  kind: "application_v2_thread_birth",
  threadId,
  eventId: EventId.make("event:application-birth"),
  sequence: 5,
} as const;
const file = {
  type: "file",
  id: "application-file",
  name: "report.TXT",
  mimeType: "text/plain",
  sizeBytes: 10,
} as const;
const message = (id: string, role = "system"): ImportedApplicationAttachmentCarrierV1 => ({
  kind: "legacy_message",
  messageId: MessageId.make(id),
  sourceThreadId: threadId,
  turnId: null,
  role,
  createdAt: now,
  updatedAt: now,
  attachmentsJson: null,
  attachments: [],
  sourceRowSha256: importedApplicationAttachmentSha256V1({ id, role }),
});
const answer = (
  id: string,
  files = [file],
): Extract<ImportedApplicationAttachmentCarrierV1, { readonly kind: "legacy_answer" }> => {
  const payload = {
    requestId: "same-request",
    questionTextById: { first: "Preserve this question" },
    answers: { first: "yes" },
    attachmentsByQuestionId: { first: files },
  };
  return {
    kind: "legacy_answer",
    activityId: id,
    sourceThreadId: threadId,
    turnId: null,
    sequence: null,
    createdAt: now,
    payloadJson: JSON.stringify(payload),
    answer: payload,
    sourceRowSha256: importedApplicationAttachmentSha256V1({ id, payload }),
  };
};
const snapshot = (carriers: ReadonlyArray<ImportedApplicationAttachmentCarrierV1>) => {
  const paths = collectImportedApplicationAttachmentPathsV1(carriers);
  if (paths.status !== "complete") throw new Error(paths.reason);
  const identity: Omit<ImportedApplicationAttachmentInventoryV1, "inventoryId" | "recordedAt"> = {
    version: 1,
    domain: "jones_materialized_attachment_references/v1",
    applicationBirth: birth,
    projectId,
    source: {
      kind: "legacy_projection",
      legacyBirth: { eventId: EventId.make("event:legacy-birth"), sequence: 1 },
      projectId,
      sourceCreatedAt: now,
      sourceCut: {
        legacyEventSequence: 4,
        projectorPositions: { threads: 4, messages: 4, activities: 4, turns: 4 },
        messageRowsSha256: importedApplicationAttachmentSha256V1(
          canonicalImportedApplicationAttachmentCarriersV1(
            carriers.filter((row) => row.kind === "legacy_message"),
          ),
        ),
        answerRowsSha256: importedApplicationAttachmentSha256V1(
          canonicalImportedApplicationAttachmentCarriersV1(
            carriers.filter((row) => row.kind === "legacy_answer"),
          ),
        ),
      },
    },
    sourceHistoryCoverage: "legacy_materialized_projection",
    completeness: "complete_application_refs",
    messageCarrierCount: paths.messageCarrierCount,
    answerCarrierCount: paths.answerCarrierCount,
    attachmentReferenceCount: paths.attachmentReferenceCount,
    carrierSetSha256: paths.carrierSetSha256,
  };
  return {
    header: {
      ...identity,
      inventoryId: makeImportedApplicationAttachmentInventoryIdV1(identity),
      recordedAt: now,
    },
    carriers,
  };
};

describe("imported application attachment inventory", () => {
  it("keeps all message roles, null and zero carriers, and distinct answers with duplicate request IDs", () => {
    const carriers = [
      message("system"),
      message("user", "user"),
      message("assistant", "assistant"),
      answer("answer-one"),
      answer("answer-two"),
      answer("answer-zero", []),
    ];
    const result = collectImportedApplicationAttachmentPathsV1(carriers);
    expect(result).toEqual({
      status: "complete",
      relativePaths: ["application-file.txt"],
      messageCarrierCount: 3,
      answerCarrierCount: 3,
      attachmentReferenceCount: 2,
      carrierSetSha256: importedApplicationAttachmentSha256V1(
        canonicalImportedApplicationAttachmentCarriersV1(carriers),
      ),
    });
    const qualified = qualifyImportedApplicationAttachmentSnapshotV1(snapshot(carriers));
    expect(qualified.status).toBe("complete");
    if (qualified.status === "complete") {
      expect(
        qualified.inventory.carriers.filter((row) => row.kind === "legacy_answer")[0]?.answer
          .questionTextById,
      ).toEqual({ first: "Preserve this question" });
    }
  });
  it("canonicalizes row order while preserving raw source and decoded reference identity", () => {
    const carriers = [answer("two"), message("one")];
    expect(collectImportedApplicationAttachmentPathsV1(carriers)).toEqual(
      collectImportedApplicationAttachmentPathsV1([...carriers].reverse()),
    );
    expect(snapshot(carriers).header.inventoryId).toBe(
      snapshot([...carriers].reverse()).header.inventoryId,
    );
    const changed = {
      ...carriers[0]!,
      sourceRowSha256: importedApplicationAttachmentSha256V1("changed source"),
    };
    expect(collectImportedApplicationAttachmentPathsV1([changed])).not.toEqual(
      collectImportedApplicationAttachmentPathsV1([carriers[0]!]),
    );
  });
  it("does not use recordedAt as immutable inventory identity", () => {
    const current = snapshot([]);
    expect(makeImportedApplicationAttachmentInventoryIdV1(current.header)).toBe(
      current.header.inventoryId,
    );
    const reobserved = { ...current.header, recordedAt: "2026-10-04T00:00:00.000Z" };
    expect(makeImportedApplicationAttachmentInventoryIdV1(reobserved)).toBe(
      current.header.inventoryId,
    );
    expect(
      qualifyImportedApplicationAttachmentSnapshotV1({
        ...current,
        header: { ...current.header, recordedAt: "2026-10-04T00:00:00.000Z" },
      }).status,
    ).toBe("complete");
  });
  it.each(["attachmentsJson", "decoded references"])(
    "holds malformed or mismatched %s instead of claiming complete zero",
    (variant) => {
      const carrier = {
        ...message("malformed"),
        ...(variant === "attachmentsJson" ? { attachmentsJson: "{" } : { attachments: [file] }),
      };
      expect(collectImportedApplicationAttachmentPathsV1([carrier])).toEqual({
        status: "unavailable",
        reason: "carrier_decode_unavailable",
      });
    },
  );
  it("holds an undecodable answer activity and preserves row identity independently of requestId", () => {
    expect(
      collectImportedApplicationAttachmentPathsV1([
        { ...answer("bad-answer"), payloadJson: "null" },
      ]),
    ).toEqual({ status: "unavailable", reason: "carrier_decode_unavailable" });
    expect(
      collectImportedApplicationAttachmentPathsV1([
        answer("duplicate-row"),
        answer("duplicate-row"),
      ]),
    ).toEqual({ status: "unavailable", reason: "carrier_identity_duplicate" });
  });
  it("holds future attachment types whose filename cannot be proved", () => {
    const attachment = { ...file, type: "future-audio" };
    const carrier = {
      ...message("future"),
      attachmentsJson: JSON.stringify([attachment]),
      attachments: [attachment],
    };
    expect(collectImportedApplicationAttachmentPathsV1([carrier])).toEqual({
      status: "unavailable",
      reason: "attachment_path_unavailable",
    });
  });
  it.each(["count", "digest", "birth", "omission"])(
    "holds %s parity changes to a complete immutable snapshot",
    (variant) => {
      const current = snapshot([answer("one")]);
      const candidate =
        variant === "omission"
          ? { ...current, carriers: [] }
          : {
              ...current,
              header: {
                ...current.header,
                ...(variant === "count"
                  ? { answerCarrierCount: 0 }
                  : variant === "digest"
                    ? { carrierSetSha256: importedApplicationAttachmentSha256V1([]) }
                    : { applicationBirth: { ...birth, sequence: 6 } }),
              },
            };
      expect(qualifyImportedApplicationAttachmentSnapshotV1(candidate).status).toBe("unavailable");
    },
  );
  it("accepts only the declared native attachment-free application subset counts", () => {
    const current = snapshot([]);
    const source = {
      kind: "native_import_batch",
      parserPolicy: "agent_session_visible_messages_v1",
      birth,
      source: {
        provider: "codex",
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerSessionId: "native-session",
        filePath: "/synthetic/native-history.jsonl",
        size: 100,
        mtimeMs: null,
        device: 1,
        inode: null,
        birthtimeMs: null,
      },
      eventsSha256: importedApplicationAttachmentSha256V1("sealed events"),
      eventBasis: Array.from({ length: 4 }, (_, index) => ({
        eventId: EventId.make(`event:native-message:${index}`),
        sequence: 6 + index,
      })),
      messageCount: 2,
    } as const;
    const { inventoryId: _id, recordedAt, ...original } = current.header;
    const identity = {
      ...original,
      source,
      sourceHistoryCoverage: "native_visible_message_subset" as const,
      messageCarrierCount: 2,
    };
    const native = {
      header: {
        ...identity,
        inventoryId: makeImportedApplicationAttachmentInventoryIdV1(identity),
        recordedAt,
      },
      carriers: [],
    };
    expect(qualifyImportedApplicationAttachmentSnapshotV1(native).status).toBe("complete");
    expect(
      qualifyImportedApplicationAttachmentSnapshotV1({
        ...native,
        carriers: [answer("unexpected")],
      }).status,
    ).toBe("unavailable");
  });
  it("rejects excess source/birth fields and nonpositive application birth sequence", () => {
    expect(
      Option.isNone(
        Schema.decodeUnknownOption(ImportedApplicationAttachmentBirthV1)({
          ...birth,
          nativeGeneration: "unproved",
        }),
      ),
    ).toBe(true);
    expect(
      Option.isNone(
        Schema.decodeUnknownOption(ImportedApplicationAttachmentBirthV1)({ ...birth, sequence: 0 }),
      ),
    ).toBe(true);
    expect(
      Option.isNone(
        Schema.decodeUnknownOption(ImportedApplicationAttachmentSourceV1)({
          ...snapshot([]).header.source,
          arbitraryCompleteness: true,
        }),
      ),
    ).toBe(true);
  });
  it("binds retained paths and source inventory evidence independently of unrelated churn", () => {
    const header = snapshot([answer("one")]).header;
    const inputs = {
      segments: [
        {
          inventoryId: header.inventoryId,
          applicationBirth: birth,
          carrierSetSha256: header.carrierSetSha256,
          forkBasis: [],
        },
      ],
      visibleV2CarrierSetSha256: importedApplicationAttachmentSha256V1([]),
      relativePaths: ["application-file.txt", "application-file.txt"],
    };
    expect(makeImportedApplicationAttachmentRetentionEvidenceV1(inputs)).toEqual(
      makeImportedApplicationAttachmentRetentionEvidenceV1({
        ...inputs,
        relativePaths: ["application-file.txt"],
      }),
    );
    expect(
      makeImportedApplicationAttachmentRetentionEvidenceV1(inputs).retentionBasisSha256,
    ).not.toBe(
      makeImportedApplicationAttachmentRetentionEvidenceV1({ ...inputs, relativePaths: [] })
        .retentionBasisSha256,
    );
    expect(
      makeImportedApplicationAttachmentRetentionEvidenceV1(inputs).retentionBasisSha256,
    ).not.toBe(
      makeImportedApplicationAttachmentRetentionEvidenceV1({
        ...inputs,
        segments: [
          {
            ...inputs.segments[0]!,
            inventoryId: importedApplicationAttachmentSha256V1("next source cut"),
          },
        ],
      }).retentionBasisSha256,
    );
  });

  it("rejects a partial materialized source cut rather than sealing bootstrap rows", () => {
    const current = snapshot([answer("one")]);
    if (current.header.source.kind !== "legacy_projection") throw new Error("wrong fixture source");
    const source = {
      ...current.header.source,
      sourceCut: {
        ...current.header.source.sourceCut,
        projectorPositions: {
          ...current.header.source.sourceCut.projectorPositions,
          activities: 3,
        },
      },
    };
    expect(
      qualifyImportedApplicationAttachmentSnapshotV1({
        ...current,
        header: { ...current.header, source },
      }),
    ).toEqual({ status: "unavailable", reason: "source_cut_incomplete" });
  });
  it("does not accept an incomplete native event pair basis", () => {
    const current = snapshot([]);
    const source = {
      kind: "native_import_batch",
      parserPolicy: "agent_session_visible_messages_v1",
      birth,
      source: {
        provider: "codex",
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerSessionId: "native-session",
        filePath: "/synthetic/native-history.jsonl",
        size: 100,
        mtimeMs: null,
        device: 1,
        inode: null,
        birthtimeMs: null,
      },
      eventsSha256: importedApplicationAttachmentSha256V1("events"),
      eventBasis: [],
      messageCount: 1,
    };
    expect(
      qualifyImportedApplicationAttachmentSnapshotV1({
        ...current,
        header: { ...current.header, source },
      }),
    ).toEqual({ status: "unavailable", reason: "native_event_basis_unavailable" });
  });
  it("retains a qualified local baseline and leaves unrelated inventories out of evidence", () => {
    const inventory = snapshot([answer("one")]);
    const result = visibleImportedApplicationAttachmentSegments({
      targetBirth: birth,
      visibleImportedBirths: [birth],
      inventories: [{ inventory, forkBasis: [] }],
    });
    expect(result).toEqual({
      status: "complete",
      relativePaths: ["application-file.txt"],
      segments: [
        {
          inventoryId: inventory.header.inventoryId,
          applicationBirth: birth,
          carrierSetSha256: inventory.header.carrierSetSha256,
          forkBasis: [],
        },
      ],
    });
    expect(
      visibleImportedApplicationAttachmentSegments({
        targetBirth: birth,
        visibleImportedBirths: [],
        inventories: [{ inventory, forkBasis: [] }],
      }),
    ).toEqual({ status: "complete", relativePaths: [], segments: [] });
  });
  it("keeps nested run-fork segments with each exact source birth and event boundary", () => {
    const middle = {
      ...birth,
      threadId: ThreadId.make("thread:middle-fork"),
      eventId: EventId.make("event:middle-birth"),
      sequence: 16,
    };
    const target = {
      ...birth,
      threadId: ThreadId.make("thread:target-fork"),
      eventId: EventId.make("event:target-birth"),
      sequence: 20,
    };
    const edge = (
      targetBirth: typeof birth | typeof target,
      sourceBirth: typeof birth | typeof middle,
      sequence: number,
    ) => ({
      targetBirth,
      sourceBirth,
      sourceRunId: RunId.make(`run:fork:${sequence}`),
      sourceRunOrdinal: 1,
      sourceRunEvent: { eventId: EventId.make(`event:source-run:${sequence}`), sequence },
      forkEvent: { eventId: targetBirth.eventId, sequence: targetBirth.sequence },
    });
    const forkBasis = [edge(target, middle, 17), edge(middle, birth, 6)];
    const inventory = snapshot([answer("one")]);
    const result = visibleImportedApplicationAttachmentSegments({
      targetBirth: target,
      visibleImportedBirths: [birth],
      inventories: [{ inventory, forkBasis }],
    });
    expect(result.status).toBe("complete");
    if (result.status === "complete") expect(result.segments[0]?.forkBasis).toEqual(forkBasis);
  });
  it("holds an imported segment with missing inventory instead of declaring an empty baseline", () => {
    expect(
      visibleImportedApplicationAttachmentSegments({
        targetBirth: birth,
        visibleImportedBirths: [birth],
        inventories: [],
      }),
    ).toEqual({ status: "unavailable", reason: "imported_inventory_unavailable" });
  });
  it("does not use same-time replacement birth or a node parent as run-fork evidence", () => {
    const inventory = snapshot([answer("one")]);
    const replacement = {
      ...birth,
      eventId: EventId.make("event:replacement-birth"),
      sequence: 21,
    };
    expect(
      visibleImportedApplicationAttachmentSegments({
        targetBirth: replacement,
        visibleImportedBirths: [replacement],
        inventories: [{ inventory, forkBasis: [] }],
      }),
    ).toEqual({ status: "unavailable", reason: "imported_inventory_unavailable" });
    expect(
      visibleImportedApplicationAttachmentSegments({
        targetBirth: replacement,
        visibleImportedBirths: [birth],
        inventories: [{ inventory, forkBasis: [] }],
      }),
    ).toEqual({ status: "unavailable", reason: "fork_basis_unavailable" });
  });
  it("holds cyclic fork witnesses even when an inventory exists", () => {
    const inventory = snapshot([answer("one")]);
    const edge = {
      targetBirth: birth,
      sourceBirth: birth,
      sourceRunId: RunId.make("run:cycle"),
      sourceRunOrdinal: 1,
      sourceRunEvent: { eventId: EventId.make("event:cycle-run"), sequence: 6 },
      forkEvent: { eventId: birth.eventId, sequence: birth.sequence },
    };
    expect(
      visibleImportedApplicationAttachmentSegments({
        targetBirth: birth,
        visibleImportedBirths: [birth],
        inventories: [{ inventory, forkBasis: [edge] }],
      }),
    ).toEqual({ status: "unavailable", reason: "fork_basis_unavailable" });
  });

  it("holds a fork whose source run witness follows the purported fork event", () => {
    const inventory = snapshot([answer("one")]);
    const target = {
      ...birth,
      threadId: ThreadId.make("thread:out-of-order-fork"),
      eventId: EventId.make("event:out-of-order-fork"),
      sequence: 7,
    };
    const edge = {
      targetBirth: target,
      sourceBirth: birth,
      sourceRunId: RunId.make("run:out-of-order"),
      sourceRunOrdinal: 1,
      sourceRunEvent: { eventId: EventId.make("event:out-of-order-run"), sequence: 8 },
      forkEvent: { eventId: target.eventId, sequence: 7 },
    };
    expect(
      visibleImportedApplicationAttachmentSegments({
        targetBirth: target,
        visibleImportedBirths: [birth],
        inventories: [{ inventory, forkBasis: [edge] }],
      }),
    ).toEqual({ status: "unavailable", reason: "fork_basis_unavailable" });
  });
});
