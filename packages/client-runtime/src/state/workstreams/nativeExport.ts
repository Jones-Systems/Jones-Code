import * as Schema from "effect/Schema";
import { WorkstreamRegistryCounts } from "@t3tools/contracts";
import { closedWorkstreamsNative as closed } from "@t3tools/contracts/jones/workstreamsNative";
import { WorkstreamsProjectionValue } from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import {
  WorkstreamsNativeReadEnvelope,
  WorkstreamsNativeReadExportTrailer,
} from "@t3tools/contracts/jones/workstreamsNativeRead";
import {
  NativeReadError,
  assertNativeReadSnapshot,
  validateNativeReadEnvelope,
  validateNativeProjectionItems,
  validateNativeReadScope,
  type NativeReadScope,
  type NativeReadSha256,
} from "./nativeRead.ts";
const ExportPage = closed(
  Schema.Struct({
    envelope: WorkstreamsNativeReadEnvelope,
    counts: WorkstreamRegistryCounts,
    offset: Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
    items: WorkstreamsProjectionValue.fields.items,
  }),
);
export type NativeExportPage = typeof ExportPage.Type;
export interface NativeExportOptions {
  readonly scope: NativeReadScope;
  readonly sha256: NativeReadSha256;
  readonly signal?: AbortSignal;
  readonly maxBytes?: number;
  readonly maxPages?: number;
}
export interface NativeCompletedExport {
  readonly complete: true;
  readonly pages: readonly NativeExportPage[];
  readonly trailer: typeof WorkstreamsNativeReadExportTrailer.Type;
  readonly preTrailerBytes: number;
}
const decodePage = Schema.decodeUnknownSync(ExportPage);
const decodeTrailer = Schema.decodeUnknownSync(WorkstreamsNativeReadExportTrailer);
const MAX_BYTES = 16 * 1024 * 1024;
const bad = (message: string): never => {
  throw new NativeReadError("invalid_response", message);
};
/** Publish only after EOF, a coherent complete trailer, and the exact original UTF-8 byte digest. */
export async function validateNativeReadExport(
  stream: AsyncIterable<Uint8Array | string>,
  options: NativeExportOptions,
): Promise<NativeCompletedExport> {
  const scope = validateNativeReadScope(options.scope);
  const maxBytes = options.maxBytes ?? MAX_BYTES,
    maxPages = options.maxPages ?? 10000;
  if (
    !Number.isSafeInteger(maxBytes) ||
    maxBytes < 1 ||
    maxBytes > MAX_BYTES ||
    !Number.isSafeInteger(maxPages) ||
    maxPages < 1 ||
    maxPages > 10000
  )
    throw new NativeReadError("bounds", "Invalid export bound.");
  const fence = () => {
    if (options.signal?.aborted) throw new NativeReadError("cancelled", "Native export cancelled.");
  };
  const encoder = new TextEncoder(),
    decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
  const raw: Uint8Array[] = [],
    pages: NativeExportPage[] = [],
    ids = new Set<string>();
  let pending: Uint8Array = new Uint8Array(0),
    bytes = 0,
    total = 0,
    chunks = 0;
  let trailer: NativeCompletedExport["trailer"] | undefined;
  const accept = async (line: Uint8Array) => {
    fence();
    if (trailer) bad("Trailing bytes after native export trailer.");
    const input: unknown = JSON.parse(decoder.decode(line.subarray(0, line.length - 1)));
    if (input && typeof input === "object" && "complete" in input) {
      trailer = decodeTrailer(input, { onExcessProperty: "error" });
      if (
        !trailer.complete ||
        pages.length === 0 ||
        trailer.registry_version !== scope.binding.registryVersion
      )
        bad("Incomplete or mismatched native export trailer.");
      return;
    }
    const page = decodePage(input, { onExcessProperty: "error" });
    validateNativeReadEnvelope(page.envelope, scope);
    validateNativeProjectionItems(page.items, page.envelope);
    if (
      page.counts.context.owner_id !== scope.binding.ownerId ||
      page.counts.context.server_generation !== scope.binding.serverGeneration ||
      page.counts.context.registry_version !== scope.binding.registryVersion ||
      page.counts.principal_id !== scope.binding.principalId ||
      page.counts.observed_at !== page.envelope.provenance.observed_at
    )
      bad("Native export counts context or principal mismatch.");
    // The actual export format carries no authorization_revision field.
    // Authorization/revocation remains the authenticated stream transport's responsibility.
    if (
      page.envelope.coverage.registry !== "complete" ||
      page.envelope.provenance.stale ||
      page.envelope.provenance.source !== "live"
    )
      bad("Native export is stale or incomplete.");
    const first = pages[0];
    if (first) {
      assertNativeReadSnapshot(first.envelope, page.envelope);
      if (
        page.counts.total !== first.counts.total ||
        page.counts.active !== first.counts.active ||
        page.counts.unknown_lifecycle !== first.counts.unknown_lifecycle ||
        page.counts.principal_id !== first.counts.principal_id ||
        page.counts.observed_at !== first.counts.observed_at
      )
        bad("Native export counts changed.");
    }
    if (
      page.offset !== total ||
      (page.items.length === 0 && (pages.length > 0 || page.counts.total !== 0))
    )
      bad("Native export offset or empty page mismatch.");
    for (const item of page.items) {
      if (ids.has(item.workstream.workstream_id)) bad("Duplicate export workstream.");
      ids.add(item.workstream.workstream_id);
    }
    total += page.items.length;
    if (total > page.counts.total || total > 10000 || pages.length >= maxPages)
      throw new NativeReadError("bounds", "Native export item or page bound exceeded.");
    bytes += line.length;
    if (bytes > maxBytes) throw new NativeReadError("bounds", "Native export byte bound exceeded.");
    raw.push(line);
    pages.push(page);
  };
  fence();
  for await (const chunk of stream) {
    fence();
    if (++chunks > 65536)
      throw new NativeReadError("bounds", "Native export chunk bound exceeded.");
    const part = typeof chunk === "string" ? encoder.encode(chunk) : chunk;
    if (!(part instanceof Uint8Array)) bad("Invalid export chunk.");
    if (trailer && part.length > 0) bad("Trailing bytes after native export trailer.");
    if (part.length + pending.length + bytes > maxBytes + 1024)
      throw new NativeReadError("bounds", "Native export byte bound exceeded.");
    const merged = new Uint8Array(pending.length + part.length);
    merged.set(pending);
    merged.set(part, pending.length);
    let start = 0;
    for (let i = 0; i < merged.length; i++)
      if (merged[i] === 10) {
        await accept(merged.slice(start, i + 1));
        start = i + 1;
      }
    pending = merged.slice(start);
  }
  fence();
  if (pending.length !== 0 || !trailer || pages.length === 0 || total !== pages[0]!.counts.total)
    throw new NativeReadError("incomplete", "Truncated native export.");
  const original = new Uint8Array(bytes);
  let offset = 0;
  for (const part of raw) {
    original.set(part, offset);
    offset += part.length;
  }
  const digest = await options.sha256(original);
  fence();
  if (!/^[a-f0-9]{64}$/.test(digest) || digest !== trailer.sha256)
    bad("Native export digest mismatch.");
  return { complete: true, pages, trailer, preTrailerBytes: bytes };
}
