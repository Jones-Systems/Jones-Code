import * as Schema from "effect/Schema";
import { NonNegativeInt } from "../baseSchemas.ts";

export const THREAD_REFERENCE_EXCERPT_MAX_CHARS = 16_000;
const CapturedAt = Schema.String.check(Schema.isMaxLength(64));
export const ThreadReferenceEarlierHistory = Schema.Literals(["none", "omitted", "unknown"]);
export const CapturedThreadReferenceExcerpt = Schema.Struct({
  status: Schema.Literal("captured"),
  capturedAt: CapturedAt,
  sourceSnapshotSequence: NonNegativeInt,
  earlierHistory: ThreadReferenceEarlierHistory,
  text: Schema.String.check(Schema.isMaxLength(THREAD_REFERENCE_EXCERPT_MAX_CHARS)),
});
export type CapturedThreadReferenceExcerpt = typeof CapturedThreadReferenceExcerpt.Type;

export const UnavailableThreadReferenceExcerpt = Schema.Struct({
  status: Schema.Literal("unavailable"),
  capturedAt: CapturedAt,
  reason: Schema.Literals([
    "source_disconnected",
    "not_found",
    "unsupported_source_server",
    "timeout",
    "invalid_response",
  ]),
});
export type UnavailableThreadReferenceExcerpt = typeof UnavailableThreadReferenceExcerpt.Type;

export const ThreadReferenceExcerpt = Schema.Union([
  CapturedThreadReferenceExcerpt,
  UnavailableThreadReferenceExcerpt,
]);
export type ThreadReferenceExcerpt = typeof ThreadReferenceExcerpt.Type;
