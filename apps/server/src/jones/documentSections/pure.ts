// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off -- Hash original bytes with Node crypto; inspect original JSON key tokens before decoding. Markdown block positions come from the shared parse-only helper.
import { createHash } from "node:crypto";
import { markdownHeadingPositions } from "@t3tools/shared/jones/documentMarkdownAst";
import type {
  DocumentDecisionReference,
  DocumentSection,
  DocumentSectionErrorCode,
  DocumentSectionsResult,
  DocumentSectionsSelector,
} from "../../../../../packages/contracts/src/jones/documentSections.ts";

export const MAX_DOCUMENT_BYTES = 1024 * 1024;
export const MAX_RESPONSE_BYTES = 64 * 1024;
const oidPattern = /^[a-f0-9]{40}(?![\s\S])/;
const digestPattern = /^[a-f0-9]{64}(?![\s\S])/;
const idPattern = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}(?![\s\S])/;
const identityPattern =
  /^github:[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?\/[A-Za-z0-9._-]{1,100}(?![\s\S])/;
const selectorKeys = [
  "repositoryIdentity",
  "commitOid",
  "documentPath",
  "documentSha256",
  "sectionIds",
];

export class DocumentSectionRefusal extends Error {
  readonly code: DocumentSectionErrorCode;
  constructor(code: DocumentSectionErrorCode) {
    super(`Document section extraction refused (${code}).`);
    this.name = "DocumentSectionRefusal";
    this.code = code;
  }
}

export const sha256 = (bytes: Uint8Array): string =>
  createHash("sha256").update(bytes).digest("hex");
export const utf8Length = (value: string): number => Buffer.byteLength(value, "utf8");
export const canonicalJson = (value: unknown): string => {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
      .join(",")}}`;
  }
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new DocumentSectionRefusal("invalid_reference");
  return encoded;
};

const recordOf = (value: unknown): Record<string, unknown> => {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new DocumentSectionRefusal("invalid_selector");
  return value as Record<string, unknown>;
};
const exactKeys = (record: Record<string, unknown>, keys: readonly string[]): boolean =>
  Object.keys(record).length === keys.length && keys.every((key) => Object.hasOwn(record, key));
const exactString = (value: unknown): value is string =>
  typeof value === "string" && value.isWellFormed() && !value.includes("\0");

export function validateSelector(value: unknown): DocumentSectionsSelector {
  const record = recordOf(value);
  const { repositoryIdentity, commitOid, documentPath, documentSha256, sectionIds } = record;
  if (
    !exactKeys(record, selectorKeys) ||
    typeof repositoryIdentity !== "string" ||
    !identityPattern.test(repositoryIdentity) ||
    typeof commitOid !== "string" ||
    !oidPattern.test(commitOid) ||
    typeof documentSha256 !== "string" ||
    !digestPattern.test(documentSha256) ||
    !exactString(documentPath) ||
    utf8Length(documentPath) > 2048 ||
    documentPath.includes("\\") ||
    /^[A-Za-z]:/.test(documentPath) ||
    documentPath.split("/").some((part) => part === "" || part === "." || part === "..") ||
    !/\.(md|markdown)(?![\s\S])/i.test(documentPath) ||
    !Array.isArray(sectionIds) ||
    sectionIds.length < 1 ||
    sectionIds.length > 32 ||
    sectionIds.some((id) => typeof id !== "string" || !idPattern.test(id)) ||
    new Set(sectionIds).size !== sectionIds.length
  )
    throw new DocumentSectionRefusal("invalid_selector");
  return Object.freeze({
    repositoryIdentity,
    commitOid,
    documentPath,
    documentSha256,
    sectionIds: Object.freeze([...sectionIds] as string[]),
  });
}

export function encodeDecisionReference(summary: string, input: DocumentSectionsSelector): string {
  const selector = validateSelector(input);
  if (!exactString(summary) || summary.trim().length === 0 || utf8Length(summary) > 512)
    throw new DocumentSectionRefusal("invalid_reference");
  const encoded = canonicalJson({
    schema: "jones.document-decision-ref/v1",
    summary,
    ...selector,
    authorityEffect: "none",
  });
  if (utf8Length(encoded) > 2048) throw new DocumentSectionRefusal("invalid_reference");
  return encoded;
}

export function decodeDecisionReference(input: string | Uint8Array): DocumentDecisionReference {
  try {
    const text = typeof input === "string" ? input : decodeUtf8(input);
    if (!text.isWellFormed() || utf8Length(text) > 64 * 1024)
      throw new DocumentSectionRefusal("invalid_reference");
    // All reference fields are flat. Inspect original key tokens before JSON.parse
    // can discard duplicate keys, including differently escaped spellings.
    const keys = new Set<string>();
    let depth = 0;
    let keyExpected = false;
    for (const token of text.matchAll(/"(?:\\[\s\S]|[^"\\])*"|[{}\[\]:,]/g)) {
      const part = token[0];
      if (part === "{" || part === "[") {
        depth++;
        if (part === "{" && depth === 1) keyExpected = true;
      } else if (part === "}" || part === "]") depth--;
      else if (part === "," && depth === 1) keyExpected = true;
      else if (part.startsWith('"') && depth === 1 && keyExpected) {
        const key: string = JSON.parse(part);
        if (keys.has(key)) throw new DocumentSectionRefusal("invalid_reference");
        keys.add(key);
        keyExpected = false;
      }
    }
    const record = recordOf(JSON.parse(text));
    if (
      !exactKeys(record, [...selectorKeys, "schema", "summary", "authorityEffect"]) ||
      record.schema !== "jones.document-decision-ref/v1" ||
      record.authorityEffect !== "none" ||
      typeof record.summary !== "string"
    )
      throw new DocumentSectionRefusal("invalid_reference");
    const selector = validateSelector(
      Object.fromEntries(selectorKeys.map((key) => [key, record[key]])),
    );
    return JSON.parse(
      encodeDecisionReference(record.summary, selector),
    ) as DocumentDecisionReference;
  } catch {
    throw new DocumentSectionRefusal("invalid_reference");
  }
}

export function decodeUtf8(bytes: Uint8Array): string {
  try {
    // A BOM is document content and participates in the original-byte digest.
    return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);
  } catch {
    throw new DocumentSectionRefusal("invalid_utf8");
  }
}

interface Heading {
  line: number;
  level: number;
  id: string | undefined;
  title: string;
  end: number;
}

export function extractDocumentSections(
  input: DocumentSectionsSelector,
  blobOid: string,
  bytes: Uint8Array,
): DocumentSectionsResult {
  const selector = validateSelector(input);
  if (!oidPattern.test(blobOid)) throw new DocumentSectionRefusal("blob_refused");
  if (bytes.byteLength > MAX_DOCUMENT_BYTES) throw new DocumentSectionRefusal("document_too_large");
  const digest = sha256(bytes);
  if (digest !== selector.documentSha256) throw new DocumentSectionRefusal("digest_mismatch");
  const text = decodeUtf8(bytes);
  const lines = text.match(/[^\r\n]*(?:\r\n|\r|\n)|[^\r\n]+$/g) ?? [];
  const headings: Heading[] = [];
  const open: Heading[] = [];
  const lineOffsets: number[] = [];
  let offset = 0;
  for (const physical of lines) {
    lineOffsets.push(offset);
    offset += physical.length;
  }
  try {
    for (const parsed of markdownHeadingPositions(text)) {
      const line = parsed.start.line - 1;
      if (!Number.isInteger(line) || line < 0 || line >= lines.length)
        throw new DocumentSectionRefusal("blob_refused");
      const logical = lines[line]!.replace(/(?:\r\n|\r|\n)$/, "");
      const match = /^ {0,3}(#{1,6})(?:[ \t]+(.*?))?[ \t]*$/.exec(logical);
      if (match === null) continue;
      if (
        parsed.depth !== match[1]!.length ||
        parsed.end.line !== parsed.start.line ||
        parsed.start.offset !== lineOffsets[line]! + logical.indexOf("#") ||
        parsed.end.offset !== lineOffsets[line]! + logical.length
      )
        throw new DocumentSectionRefusal("blob_refused");
      const content = (match[2] ?? "")
        .replace(/[ \t]+$/, "")
        .replace(/[ \t]+#+$/, "")
        .replace(/[ \t]+$/, "");
      const stable = /^\[([A-Za-z0-9][A-Za-z0-9._:-]{0,127})\](.*)$/.exec(content);
      const heading: Heading = {
        line,
        level: match[1]!.length,
        id: stable?.[1],
        title: (stable?.[2] ?? "").replace(/^[ \t]+|[ \t]+$/g, ""),
        end: lines.length,
      };
      while (open.length > 0 && open[open.length - 1]!.level >= heading.level)
        open.pop()!.end = line;
      open.push(heading);
      headings.push(heading);
    }
  } catch {
    throw new DocumentSectionRefusal("blob_refused");
  }
  const byId = new Map<string, Heading[]>();
  for (const heading of headings) {
    if (heading.id !== undefined) {
      const matches = byId.get(heading.id) ?? [];
      matches.push(heading);
      byId.set(heading.id, matches);
    }
  }
  const sections: DocumentSection[] = [];
  for (const id of selector.sectionIds) {
    const matches = byId.get(id) ?? [];
    if (matches.length === 0) throw new DocumentSectionRefusal("missing_section");
    if (matches.length !== 1) throw new DocumentSectionRefusal("ambiguous_section");
    const heading = matches[0]!;
    const sectionText = lines.slice(heading.line, heading.end).join("");
    sections.push({
      id,
      level: heading.level,
      title: heading.title,
      startLine: heading.line + 1,
      endLine: heading.end,
      text: sectionText,
      sha256: sha256(Buffer.from(sectionText, "utf8")),
    });
  }
  const result: DocumentSectionsResult = {
    schema: "jones.document-sections/v1",
    document: {
      repositoryIdentity: selector.repositoryIdentity,
      commitOid: selector.commitOid,
      path: selector.documentPath,
      blobOid,
      sha256: digest,
      sizeBytes: bytes.byteLength,
    },
    requestedIds: selector.sectionIds,
    sections,
    authorityEffect: "none",
  };
  // This is the complete JSON result, including metadata and repeated nested sections.
  if (utf8Length(canonicalJson(result)) > MAX_RESPONSE_BYTES)
    throw new DocumentSectionRefusal("response_too_large");
  return result;
}
