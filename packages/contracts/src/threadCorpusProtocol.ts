import * as Schema from "effect/Schema";

import { QUEUE_DISPATCH_CAPABILITY } from "./queueProtocol.ts";

const threadCorpusCapabilityFields = {
  schemaVersion: Schema.Literal("t3.thread-corpus-capability/v1"),
  shellSnapshot: Schema.Literal("t3.thread-corpus-shell/v1"),
  threadDetailPagination: Schema.Literal("t3.thread-corpus-pagination/v1"),
  placement: Schema.Literal("t3.thread-corpus-placement/v1"),
  authSession: Schema.Literal(QUEUE_DISPATCH_CAPABILITY.authSession),
};

// Validate original wire keys before a struct decoder can strip unknown claims.
export const ThreadCorpusCapability = Schema.flip(
  Schema.flip(Schema.Struct(threadCorpusCapabilityFields)).check(
    Schema.makeFilter((value) =>
      Reflect.ownKeys(value).every((key) => Object.hasOwn(threadCorpusCapabilityFields, key)),
    ),
  ),
);
export type ThreadCorpusCapability = typeof ThreadCorpusCapability.Type;

/**
 * Advertise only after proving the legacy read semantics in
 * docs/internals/connection-runtime.md#legacy-thread-corpus-reads; DTO validation is insufficient.
 */
export const THREAD_CORPUS_CAPABILITY = {
  schemaVersion: "t3.thread-corpus-capability/v1",
  shellSnapshot: "t3.thread-corpus-shell/v1",
  threadDetailPagination: "t3.thread-corpus-pagination/v1",
  placement: "t3.thread-corpus-placement/v1",
  authSession: QUEUE_DISPATCH_CAPABILITY.authSession,
} as const satisfies ThreadCorpusCapability;
