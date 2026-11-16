import {
  TOKEN_ACCOUNTING_MAX_RESPONSE_BYTES,
  TOKEN_ACCOUNTING_MAX_GROUPS,
  TOKEN_ACCOUNTING_MAX_SOURCE_COLLECTION,
  TokenAccountingReadResult,
  TokenAccountingReport,
  TokenAccountingUnavailable,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";

import {
  TOKEN_ACCOUNTING_READER_LIMITS,
  TokenAccountingReaderBinding,
  type TokenAccountingReaderPort,
  unconfiguredReader,
} from "./Reader.ts";

export class TokenAccountingService extends Context.Service<
  TokenAccountingService,
  {
    readonly isAvailable: Effect.Effect<boolean>;
    readonly read: Effect.Effect<TokenAccountingReadResult>;
  }
>()("t3/tokenAccounting/TokenAccountingService") {}

/**
 * The binding check and the archive read have separate budgets, so a slow check cannot
 * starve the read. A server config load asks for availability on every connection; the
 * answer is reused for this long so those loads do not each re-run the check.
 */
const BINDING_CHECK_BUDGET = "5 seconds";
const READ_BUDGET = "5 seconds";
const AVAILABILITY_TTL_MILLIS = 30_000;

type BindingCheck =
  | { readonly kind: "checked"; readonly binding: TokenAccountingReaderBinding }
  | { readonly kind: "unverified" }
  | { readonly kind: "failed" }
  | { readonly kind: "timeout" };

const decodeReport = Schema.decodeUnknownEffect(TokenAccountingReport);
const decodeUnavailable = Schema.decodeUnknownEffect(TokenAccountingUnavailable);
const decodeBinding = Schema.decodeUnknownEffect(TokenAccountingReaderBinding);
const decodeJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));
const encodeResult = Schema.encodeEffect(Schema.fromJsonString(TokenAccountingReadResult));
const utf8Bytes = (value: string) => new TextEncoder().encode(value).byteLength;
const field = (value: unknown, key: string): unknown =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)[key]
    : undefined;
function hasOversizedCollection(value: unknown): boolean {
  return [
    [field(field(value, "input"), "groups"), TOKEN_ACCOUNTING_MAX_GROUPS],
    [field(field(value, "output"), "groups"), TOKEN_ACCOUNTING_MAX_GROUPS],
    [
      field(field(field(value, "provider_coverage"), "latest_scan"), "root_scope_ids"),
      TOKEN_ACCOUNTING_MAX_SOURCE_COLLECTION,
    ],
  ].some(
    ([collection, limit]) => Array.isArray(collection) && collection.length > (limit as number),
  );
}

export const make = Effect.fn("makeTokenAccountingService")(function* (
  reader: TokenAccountingReaderPort = unconfiguredReader,
) {
  const scope = yield* Scope.Scope;
  let pending: Deferred.Deferred<TokenAccountingReadResult> | undefined;
  let availabilityPending: Deferred.Deferred<boolean> | undefined;
  let availabilityCache: { readonly value: boolean; readonly expiresAt: number } | undefined;

  const binding = reader.checkBinding.pipe(Effect.flatMap(decodeBinding));
  const probeAvailability = binding.pipe(
    Effect.map((value) => value.status === "bound"),
    Effect.orElseSucceed(() => false),
    Effect.catchDefect(() => Effect.succeed(false)),
    Effect.timeoutOption(BINDING_CHECK_BUDGET),
    Effect.map((value) => Option.getOrElse(value, () => false)),
  );

  // Concurrent callers share one probe, and its answer is cached until the TTL expires.
  // The server scope owns the probe, so one disconnected caller cannot cancel the others.
  const isAvailable = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    if (availabilityCache !== undefined && now < availabilityCache.expiresAt) {
      return availabilityCache.value;
    }
    const flight = yield* Effect.uninterruptible(
      Effect.gen(function* () {
        if (availabilityPending !== undefined) return availabilityPending;
        const created = Deferred.makeUnsafe<boolean>();
        availabilityPending = created;
        yield* probeAvailability.pipe(
          Effect.tap((value) =>
            Clock.currentTimeMillis.pipe(
              Effect.map((finishedAt) => {
                availabilityCache = { value, expiresAt: finishedAt + AVAILABILITY_TTL_MILLIS };
              }),
            ),
          ),
          Effect.onExit((exit) =>
            Effect.sync(() => {
              availabilityPending = undefined;
            }).pipe(Effect.andThen(Deferred.done(created, exit))),
          ),
          Effect.forkIn(scope),
        );
        return created;
      }),
    );
    return yield* Deferred.await(flight);
  });

  const readOnce = Effect.gen(function* () {
    const readAt = DateTime.formatIso(yield* DateTime.now);
    let configuredReportId: string | null = null;
    const unavailable = (failure: TokenAccountingUnavailable): TokenAccountingReadResult => ({
      state: "unavailable",
      ...failure,
      configuredReportId,
      readAt,
    });

    const checked = yield* binding.pipe(
      Effect.map((value): BindingCheck => ({ kind: "checked", binding: value })),
      Effect.orElseSucceed((): BindingCheck => ({ kind: "unverified" })),
      Effect.catchDefect(() => Effect.succeed<BindingCheck>({ kind: "failed" })),
      Effect.timeoutOption(BINDING_CHECK_BUDGET),
      Effect.map((value) => Option.getOrElse(value, (): BindingCheck => ({ kind: "timeout" }))),
    );
    if (checked.kind === "unverified") {
      return unavailable({ status: "unconfigured", reason: "host_binding_unverified" });
    }
    if (checked.kind !== "checked") {
      return unavailable({
        status: "reader_failed",
        reason: checked.kind === "timeout" ? "reader_timeout" : "reader_failed",
      });
    }
    const selected = checked.binding;
    configuredReportId = selected.configuredReportId;
    if (selected.status !== "bound") {
      return unavailable({ status: selected.status, reason: selected.reason });
    }

    const observe = Effect.gen(function* () {
      const response = yield* reader.readSummary({
        reportId: selected.configuredReportId,
        limits: TOKEN_ACCOUNTING_READER_LIMITS,
      });
      if (typeof response !== "string") {
        const failure = yield* decodeUnavailable(response);
        return unavailable(failure);
      }
      if (utf8Bytes(response) > TOKEN_ACCOUNTING_MAX_RESPONSE_BYTES) {
        return unavailable({ status: "oversized", reason: "projection_too_large" });
      }
      const parsed = yield* decodeJson(response).pipe(Effect.option);
      if (Option.isNone(parsed)) {
        return unavailable({ status: "invalid", reason: "projection_invalid" });
      }
      if (hasOversizedCollection(parsed.value)) {
        return unavailable({ status: "oversized", reason: "collection_limit_exceeded" });
      }
      const decoded = yield* decodeReport(parsed.value).pipe(Effect.option);
      if (Option.isNone(decoded)) {
        return unavailable({ status: "invalid", reason: "projection_invalid" });
      }
      if (decoded.value.report_id !== selected.configuredReportId) {
        return unavailable({ status: "invalid", reason: "configured_report_id_mismatch" });
      }
      const result: TokenAccountingReadResult = {
        state: "ready",
        readAt,
        report: decoded.value,
      };
      if (utf8Bytes(yield* encodeResult(result)) > TOKEN_ACCOUNTING_MAX_RESPONSE_BYTES) {
        return unavailable({ status: "oversized", reason: "projection_too_large" });
      }
      return result;
    }).pipe(
      Effect.orElseSucceed(() => unavailable({ status: "reader_failed", reason: "reader_failed" })),
      Effect.catchDefect(() =>
        Effect.succeed(unavailable({ status: "reader_failed", reason: "reader_failed" })),
      ),
      Effect.timeoutOption(READ_BUDGET),
    );
    return Option.getOrElse(yield* observe, () =>
      unavailable({ status: "reader_failed", reason: "reader_timeout" }),
    );
  });

  const read = Effect.gen(function* () {
    const flight = yield* Effect.uninterruptible(
      Effect.gen(function* () {
        if (pending !== undefined) return pending;
        const created = Deferred.makeUnsafe<TokenAccountingReadResult>();
        pending = created;
        // The server scope owns this read, so one disconnected waiter cannot cancel other clients.
        yield* readOnce.pipe(
          Effect.onExit((exit) =>
            Effect.sync(() => {
              pending = undefined;
            }).pipe(Effect.andThen(Deferred.done(created, exit))),
          ),
          Effect.forkIn(scope),
        );
        return created;
      }),
    );
    return yield* Deferred.await(flight);
  });

  return TokenAccountingService.of({ isAvailable, read });
});

/** Runtime enrollment is owned by the host adapter; this default layer performs no archive reads. */
export const layer = Layer.effect(TokenAccountingService, make());
export const layerWithReader = (reader: TokenAccountingReaderPort) =>
  Layer.effect(TokenAccountingService, make(reader));
