import {
  TOKEN_ACCOUNTING_MAX_GROUPS,
  TOKEN_ACCOUNTING_MAX_INPUT_BYTES,
  TOKEN_ACCOUNTING_MAX_RESPONSE_BYTES,
  TOKEN_ACCOUNTING_MAX_SOURCE_COLLECTION,
  TokenAccountingReportId,
  TokenAccountingUnavailable,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const closed = <Fields extends Schema.Struct.Fields>(schema: Schema.Struct<Fields>) =>
  Schema.flip(
    Schema.flip(schema).check(
      Schema.makeFilter(
        (value) =>
          typeof value === "object" &&
          value !== null &&
          !Array.isArray(value) &&
          Reflect.ownKeys(value).every((key) => Object.hasOwn(schema.fields, key)),
      ),
    ),
  );

export const TokenAccountingReaderBinding = Schema.Union([
  closed(
    Schema.Struct({
      status: Schema.Literal("bound"),
      configuredReportId: TokenAccountingReportId,
    }),
  ),
  closed(
    Schema.Struct({
      status: Schema.Literal("unconfigured"),
      reason: Schema.Literals([
        "reader_unconfigured",
        "report_unconfigured",
        "host_binding_unverified",
      ]),
      configuredReportId: Schema.NullOr(TokenAccountingReportId),
    }),
  ),
]);
export type TokenAccountingReaderBinding = typeof TokenAccountingReaderBinding.Type;

export const TOKEN_ACCOUNTING_READER_LIMITS = Object.freeze({
  maxInputBytes: TOKEN_ACCOUNTING_MAX_INPUT_BYTES,
  maxResponseBytes: TOKEN_ACCOUNTING_MAX_RESPONSE_BYTES,
  maxGroups: TOKEN_ACCOUNTING_MAX_GROUPS,
  maxSourceCollection: TOKEN_ACCOUNTING_MAX_SOURCE_COLLECTION,
} as const);

export class TokenAccountingReaderError extends Schema.TaggedError<TokenAccountingReaderError>()(
  "TokenAccountingReaderError",
  { cause: Schema.Defect() },
) {}

export interface TokenAccountingReaderPort {
  /** The owning adapter verifies enrollment and custody here without accessing the archive. */
  readonly checkBinding: Effect.Effect<TokenAccountingReaderBinding, TokenAccountingReaderError>;
  /**
   * The adapter rechecks custody before reading only the configured report, enforces the
   * input limit before parsing, and runs Python's canonical validator before projecting.
   * This is an injected port, not a launcher or a host-attestation format.
   */
  readonly readSummary: (request: {
    readonly reportId: string;
    readonly limits: typeof TOKEN_ACCOUNTING_READER_LIMITS;
  }) => Effect.Effect<string | TokenAccountingUnavailable, TokenAccountingReaderError>;
}

export const unconfiguredReader: TokenAccountingReaderPort = {
  checkBinding: Effect.succeed({
    status: "unconfigured",
    reason: "reader_unconfigured",
    configuredReportId: null,
  }),
  readSummary: () => Effect.succeed({ status: "unconfigured", reason: "reader_unconfigured" }),
};
