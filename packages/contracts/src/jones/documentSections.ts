import * as Schema from "effect/Schema";
import { ThreadId } from "../baseSchemas.ts";

export const DOCUMENT_SECTIONS_MAX_DOCUMENT_BYTES = 1024 * 1024;
export const DOCUMENT_SECTIONS_MAX_RESPONSE_BYTES = 64 * 1024;
export const DOCUMENT_SECTIONS_MAX_REFERENCE_BYTES = 2048;
export const DOCUMENT_SECTIONS_MAX_SUMMARY_BYTES = 512;

const utf8Bytes = (value: string) => new TextEncoder().encode(value).length;
const exactText = (value: string) => value.isWellFormed() && !value.includes("\0");
export const DocumentRepositoryIdentity = Schema.String.check(
  Schema.isPattern(
    /^github:[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?\/[A-Za-z0-9._-]{1,100}(?![\s\S])/,
  ),
);
export const DocumentCommitOid = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}(?![\s\S])/));
export const DocumentSha256 = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}(?![\s\S])/));
export const DocumentSectionId = Schema.String.check(
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}(?![\s\S])/),
);
export const DocumentMarkdownPath = Schema.String.check(
  Schema.makeFilter(
    (value) =>
      exactText(value) &&
      utf8Bytes(value) <= 2048 &&
      !value.includes("\\") &&
      !/^[A-Za-z]:/.test(value) &&
      value.split("/").every((part) => part !== "" && part !== "." && part !== "..") &&
      /\.(md|markdown)(?![\s\S])/i.test(value),
  ),
);
export const DocumentSectionIds = Schema.Array(DocumentSectionId).check(
  Schema.isMinLength(1),
  Schema.isMaxLength(32),
  Schema.makeFilter((ids) => new Set(ids).size === ids.length),
);
export const DocumentSectionsSelector = Schema.Struct({
  repositoryIdentity: DocumentRepositoryIdentity,
  commitOid: DocumentCommitOid,
  documentPath: DocumentMarkdownPath,
  documentSha256: DocumentSha256,
  sectionIds: DocumentSectionIds,
}).annotate({ parseOptions: { onExcessProperty: "error" } });
export type DocumentSectionsSelector = typeof DocumentSectionsSelector.Type;

export const DocumentSectionsInput = Schema.Struct({
  ...DocumentSectionsSelector.fields,
  threadId: Schema.optional(ThreadId),
}).annotate({ parseOptions: { onExcessProperty: "error" } });
export type DocumentSectionsInput = typeof DocumentSectionsInput.Type;

const positiveLine = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1));
export const DocumentSection = Schema.Struct({
  id: DocumentSectionId,
  level: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1), Schema.isLessThanOrEqualTo(6)),
  title: Schema.String,
  startLine: positiveLine,
  endLine: positiveLine,
  sha256: DocumentSha256,
  text: Schema.String,
});
export type DocumentSection = typeof DocumentSection.Type;
export const DocumentSectionsResult = Schema.Struct({
  schema: Schema.Literal("jones.document-sections/v1"),
  document: Schema.Struct({
    repositoryIdentity: DocumentRepositoryIdentity,
    commitOid: DocumentCommitOid,
    path: DocumentMarkdownPath,
    blobOid: DocumentCommitOid,
    sha256: DocumentSha256,
    sizeBytes: Schema.Int.check(
      Schema.isGreaterThanOrEqualTo(0),
      Schema.isLessThanOrEqualTo(DOCUMENT_SECTIONS_MAX_DOCUMENT_BYTES),
    ),
  }),
  requestedIds: DocumentSectionIds,
  sections: Schema.Array(DocumentSection).check(Schema.isMinLength(1), Schema.isMaxLength(32)),
  authorityEffect: Schema.Literal("none"),
});
export type DocumentSectionsResult = typeof DocumentSectionsResult.Type;

export const DocumentDecisionReference = Schema.Struct({
  schema: Schema.Literal("jones.document-decision-ref/v1"),
  summary: Schema.String.check(
    Schema.makeFilter(
      (value) =>
        exactText(value) &&
        value.trim().length > 0 &&
        utf8Bytes(value) <= DOCUMENT_SECTIONS_MAX_SUMMARY_BYTES,
    ),
  ),
  ...DocumentSectionsSelector.fields,
  authorityEffect: Schema.Literal("none"),
})
  .annotate({ parseOptions: { onExcessProperty: "error" } })
  .check(
    Schema.makeFilter(
      (value) => utf8Bytes(JSON.stringify(value)) <= DOCUMENT_SECTIONS_MAX_REFERENCE_BYTES,
    ),
  );
export type DocumentDecisionReference = typeof DocumentDecisionReference.Type;

export const DocumentSectionErrorCode = Schema.Literals([
  "invalid_selector",
  "invalid_reference",
  "repository_refused",
  "origin_refused",
  "identity_mismatch",
  "unsupported_object_format",
  "commit_refused",
  "path_refused",
  "blob_refused",
  "digest_mismatch",
  "invalid_utf8",
  "missing_section",
  "ambiguous_section",
  "document_too_large",
  "response_too_large",
  "git_unavailable",
  "git_failed",
  "git_timeout",
  "git_output_overflow",
  "git_cleanup_failed",
  "cancelled",
]);
export type DocumentSectionErrorCode = typeof DocumentSectionErrorCode.Type;
