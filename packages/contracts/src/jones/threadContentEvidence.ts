import * as Schema from "effect/Schema";
import { EnvironmentId, NonNegativeInt, ThreadId } from "../baseSchemas.ts";

export const THREAD_CONTENT_EVIDENCE_VERSION = "jones.thread-content-evidence/v1";
export const THREAD_CONTENT_RENDERER_ID = "jones.orchestrator-turn-item-text/v1";

export const ThreadContentEvidenceCapability = Schema.Struct({
  version: Schema.Literal(THREAD_CONTENT_EVIDENCE_VERSION),
  rendererId: Schema.Literal(THREAD_CONTENT_RENDERER_ID),
  offsetUnit: Schema.Literal("utf16-code-unit"),
  digestEncoding: Schema.Literal("utf-16le"),
  hashAlgorithm: Schema.Literal("sha256"),
  maxItemsPerPage: Schema.Literal(100),
  maxRecentRuns: Schema.Literal(50),
  maxCharsPerItem: Schema.Literal(50_000),
  maxRenderedUtf16Length: Schema.Literal(4_000_000),
  maxPageHashUtf16Units: Schema.Literal(8_000_000),
});

export const THREAD_CONTENT_EVIDENCE_CAPABILITY = {
  version: THREAD_CONTENT_EVIDENCE_VERSION,
  rendererId: THREAD_CONTENT_RENDERER_ID,
  offsetUnit: "utf16-code-unit",
  digestEncoding: "utf-16le",
  hashAlgorithm: "sha256",
  maxItemsPerPage: 100,
  maxRecentRuns: 50,
  maxCharsPerItem: 50_000,
  maxRenderedUtf16Length: 4_000_000,
  maxPageHashUtf16Units: 8_000_000,
} as const satisfies typeof ThreadContentEvidenceCapability.Type;

const Digest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}(?![\s\S])/));

export const ThreadContentEvidenceRequest = Schema.Struct({
  version: Schema.Literal(THREAD_CONTENT_EVIDENCE_VERSION),
  expectedRevision: Schema.optional(Digest),
}).annotate({
  description:
    "Opt in only when the responding server advertises threadContentEvidence. Inventory uses version alone; item continuation uses itemId, nextTextOffset and the original expectedRevision. Require returned evidence too: older peers can discard this request. Item consistency only, not an immutable timeline, atomic thread snapshot or no-intermediate-edit guarantee.",
});

export function isThreadContentEvidenceSelection(input: {
  readonly contentEvidence?: typeof ThreadContentEvidenceRequest.Type | undefined;
  readonly itemId?: string | undefined;
  readonly textOffset?: number | undefined;
  readonly afterPosition?: number | undefined;
}): boolean {
  if (input.contentEvidence === undefined) return true;
  if (input.itemId === undefined) {
    return input.contentEvidence.expectedRevision === undefined && input.textOffset === undefined;
  }
  return (
    input.afterPosition === undefined &&
    ((input.textOffset ?? 0) === 0 || input.contentEvidence.expectedRevision !== undefined)
  );
}

const EvidenceIdentity = {
  version: Schema.Literal(THREAD_CONTENT_EVIDENCE_VERSION),
  rendererId: Schema.Literal(THREAD_CONTENT_RENDERER_ID),
  environmentId: EnvironmentId,
  threadId: ThreadId,
};
const AvailableEvidence = Schema.Struct({
  ...EvidenceIdentity,
  availability: Schema.Literal("available"),
  contentRevision: Digest,
  contentSha256: Digest,
  totalUtf16Length: NonNegativeInt.check(Schema.isLessThanOrEqualTo(4_000_000)),
  startOffset: NonNegativeInt,
  endOffset: NonNegativeInt,
}).check(
  Schema.makeFilter(
    (value) =>
      value.startOffset <= value.endOffset &&
      value.endOffset <= value.totalUtf16Length &&
      value.endOffset - value.startOffset <= 50_000,
  ),
);

export const ThreadContentEvidence = Schema.Union([
  AvailableEvidence,
  Schema.Struct({
    ...EvidenceIdentity,
    availability: Schema.Literal("unavailable"),
    reason: Schema.Literals(["source_content_null", "item_hash_limit", "page_hash_budget"]),
  }),
]);
export type ThreadContentEvidence = typeof ThreadContentEvidence.Type;
