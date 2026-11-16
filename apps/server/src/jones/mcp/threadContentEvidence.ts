// @effect-diagnostics-next-line nodeBuiltinImport:off -- Deterministic synchronous UTF-16LE hashing keeps this bounded helper pure, without a runtime service.
import * as NodeCrypto from "node:crypto";
import {
  type EnvironmentId,
  type OrchestratorMcpThreadReadInput,
  type OrchestratorMcpThreadTimelineItem,
  OrchestratorMcpFailure,
  THREAD_CONTENT_EVIDENCE_CAPABILITY,
  ThreadContentEvidenceRequest,
  type ThreadContentEvidence,
  isThreadContentEvidenceSelection,
} from "@t3tools/contracts";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

const capability = THREAD_CONTENT_EVIDENCE_CAPABILITY;
const isEvidenceRequest = Schema.is(ThreadContentEvidenceRequest);

function contentRevision(fields: ReadonlyArray<string | null>): string {
  const hash = NodeCrypto.createHash("sha256").update(`${capability.version}\0`, "ascii");
  // Portable v1 frame: ordered UTF-16LE byte strings, uint32 LE byte lengths;
  // null is 0xffffffff. No BOM, Unicode normalization or JSON serialization.
  for (const field of fields) {
    const bytes = field === null ? null : Buffer.from(field, "utf16le");
    const length = Buffer.alloc(4);
    length.writeUInt32LE(bytes === null ? 0xffffffff : bytes.length);
    hash.update(length);
    if (bytes !== null) hash.update(bytes);
  }
  return hash.digest("hex");
}

/** Input items contain the complete renderer value. Validate the entire page before any ack.
 * Limits bound added text hashing, not upstream payload materialization or rendering allocation.
 */
export function prepareThreadContentEvidence(input: {
  readonly request: OrchestratorMcpThreadReadInput;
  readonly environmentId: EnvironmentId;
  readonly items: ReadonlyArray<OrchestratorMcpThreadTimelineItem>;
  readonly hasMore: boolean;
  readonly maxChars: number;
}): Result.Result<ReadonlyArray<OrchestratorMcpThreadTimelineItem>, OrchestratorMcpFailure> {
  const { request, maxChars } = input;
  const requested = request.contentEvidence;
  const offset = request.itemId === undefined ? 0 : (request.textOffset ?? 0);
  if (
    requested !== undefined &&
    (!isEvidenceRequest(requested) ||
      !isThreadContentEvidenceSelection(request) ||
      !Number.isSafeInteger(offset) ||
      offset < 0 ||
      !Number.isSafeInteger(maxChars) ||
      maxChars < 1 ||
      maxChars > capability.maxCharsPerItem ||
      input.items.length > capability.maxItemsPerPage ||
      (request.itemId !== undefined &&
        (input.items.length !== 1 || input.hasMore || input.items[0]?.itemId !== request.itemId)))
  ) {
    return Result.fail(
      new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "Invalid content evidence selection.",
      }),
    );
  }

  let hashedUnits = 0;
  const items: Array<OrchestratorMcpThreadTimelineItem> = [];
  for (const item of input.items) {
    const text = item.text;
    const end = text === null ? offset : Math.min(text.length, offset + maxChars);
    const textTruncated = text !== null && text.length > offset + maxChars;
    let contentEvidence: ThreadContentEvidence | undefined;
    if (requested !== undefined) {
      const identity = {
        version: capability.version,
        rendererId: capability.rendererId,
        environmentId: input.environmentId,
        threadId: request.threadId,
      };
      const reason =
        text === null
          ? "source_content_null"
          : text.length > capability.maxRenderedUtf16Length
            ? "item_hash_limit"
            : hashedUnits + text.length > capability.maxPageHashUtf16Units
              ? "page_hash_budget"
              : undefined;
      if (reason !== undefined || text === null) {
        if (requested.expectedRevision !== undefined) {
          return Result.fail(
            new OrchestratorMcpFailure({
              code: "content_evidence_unavailable",
              message: "Content evidence is unavailable for this item.",
            }),
          );
        }
        contentEvidence = {
          ...identity,
          availability: "unavailable",
          reason: reason ?? "source_content_null",
        };
      } else {
        hashedUnits += text.length;
        const contentSha256 = NodeCrypto.createHash("sha256")
          .update(Buffer.from(text, "utf16le"))
          .digest("hex");
        const revision = contentRevision([
          identity.rendererId,
          identity.environmentId,
          identity.threadId,
          item.sourceThreadId,
          item.itemId,
          item.runId,
          item.visibility,
          String(item.position),
          item.type,
          item.status,
          item.title,
          item.updatedAt,
          item.messageId,
          String(text.length),
          contentSha256,
        ]);
        if (requested.expectedRevision !== undefined && requested.expectedRevision !== revision) {
          return Result.fail(
            new OrchestratorMcpFailure({
              code: "content_revision_mismatch",
              message: "Rendered item content or identity changed.",
            }),
          );
        }
        if (offset > text.length) {
          return Result.fail(
            new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "Content offset is out of range.",
            }),
          );
        }
        contentEvidence = {
          ...identity,
          availability: "available",
          contentRevision: revision,
          contentSha256,
          totalUtf16Length: text.length,
          startOffset: offset,
          endOffset: end,
        };
      }
    }
    items.push({
      ...item,
      text: text === null ? null : text.slice(offset, offset + maxChars),
      textTruncated,
      nextTextOffset: textTruncated ? offset + maxChars : null,
      ...(contentEvidence === undefined ? {} : { contentEvidence }),
    });
  }
  return Result.succeed(items);
}
