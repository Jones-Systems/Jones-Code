import {
  TOKEN_ACCOUNTING_CAVEATS,
  TOKEN_ACCOUNTING_IDENTITY_ALGORITHM,
  TOKEN_ACCOUNTING_MAX_RESPONSE_BYTES,
  TOKEN_ACCOUNTING_PROJECTION_SCHEMA,
  TOKEN_ACCOUNTING_REPORT_SCHEMA,
  type TokenAccountingMetric,
  type TokenAccountingReport,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";

import {
  TOKEN_ACCOUNTING_READER_LIMITS,
  TokenAccountingReaderError,
  type TokenAccountingReaderPort,
} from "./Reader.ts";
import { make } from "./TokenAccountingService.ts";

const REPORT_ID = "a".repeat(64);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const metric = (requests = 1): TokenAccountingMetric => ({
  known_sum: null,
  total: null,
  known_requests: 0,
  missing_requests: requests,
});
const metrics = (requests = 1) => ({
  input_tokens: metric(requests),
  cached_input_tokens: metric(requests),
  cache_write_input_tokens: metric(requests),
  cache_write_5m_tokens: metric(requests),
  cache_write_1h_tokens: metric(requests),
  output_tokens: metric(requests),
  reasoning_output_tokens: metric(requests),
});
const partition = (requests: number) => ({
  requests,
  metrics: { ...metrics(requests), ordinary_input: metric(requests) },
});
const statusCounts = { primary: 1, legacy_unresolved: 0, conflict: 0, aggregate_delta: 0 };

// Synthetic projection identities exercise transport only; Python owns complete-source identity proof.
function report(): TokenAccountingReport {
  const allocation = {
    allocated: metric(),
    unknown: metric(),
    groups: [],
    unknown_by_reason: { missing_counter: metric() },
    estimated_coverage: null,
  };
  return {
    schema: TOKEN_ACCOUNTING_PROJECTION_SCHEMA,
    source_schema: TOKEN_ACCOUNTING_REPORT_SCHEMA,
    source_identity_algorithm: TOKEN_ACCOUNTING_IDENTITY_ALGORITHM,
    source_identity_verified: true,
    report_id: REPORT_ID,
    selection_id: "b".repeat(64),
    snapshot: { index_snapshot_id: "c".repeat(64), captured_at: "2026-10-02T12:00:00Z" },
    window: { start: "2026-09-01T00:00:00Z", end: "2026-09-02T00:00:00Z", end_exclusive: true },
    provider_coverage: {
      selected_requests: 1,
      accounting_status_counts: statusCounts,
      token_missingness: {
        input_tokens: 1,
        cached_input_tokens: 1,
        cache_write_input_tokens: 1,
        cache_write_5m_tokens: 1,
        cache_write_1h_tokens: 1,
        output_tokens: 1,
        reasoning_output_tokens: 1,
      },
      latest_scan: {
        captured_at: null,
        selected_files: null,
        refreshed_selected_files: null,
        partial_selected_files: null,
        root_scope_ids: [],
        mtime_cutoff: null,
        receipts_status_counts: null,
        validated_files: null,
      },
      cumulative_index: {
        tracked_files: null,
        retained_not_refreshed_files: null,
        missing_tracked_files: null,
      },
      freshness: {
        status: "scan_unavailable",
        requested_end: null,
        last_scan_captured_at: null,
        selected_validation_min: null,
        selected_validation_max: null,
        unrefreshed_files: null,
      },
      historical_window_completeness: "not_proven",
    },
    provider_ledger: {
      requests: 1,
      primary_requests: 1,
      nonadditive_requests: 0,
      accounting_status_counts: statusCounts,
      metrics: { ...metrics(), ordinary_input_tokens: metric(), non_read_input_tokens: metric() },
    },
    input: { ordinary_input: metric(), ...allocation },
    output: { output: metric(), measured_reasoning: metric(), ...allocation },
    partitions: {
      provider: { codex: partition(1), claude: partition(0) },
      role: { root: partition(1), child: partition(0), unknown: partition(0) },
    },
    mechanism_flags: { nonadditive: true, counts: {} },
    estimator_status_counts: { qualified: 0, unavailable: 0, failed: 0 },
    coverage: {
      primary_requests: 1,
      captured_requests: 0,
      unavailable_requests: 1,
      complete_response_requests: 0,
      input_allocated_requests: 0,
      output_allocated_requests: 0,
      reasoning_measured_requests: 0,
      reasoning_missing_requests: 1,
      estimator_qualified_models: 0,
      estimator_unavailable_models: 0,
      diagnostics: {},
    },
    caveats: TOKEN_ACCOUNTING_CAVEATS,
    authority_effect: "none",
  };
}

const reader = (
  readSummary: TokenAccountingReaderPort["readSummary"] = () =>
    Effect.succeed(encodeJson(report())),
): TokenAccountingReaderPort => ({
  checkBinding: Effect.succeed({ status: "bound", configuredReportId: REPORT_ID }),
  readSummary,
});

describe("saved accounting transport", () => {
  it.effect("defaults unavailable without enrolling or reading a report", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const service = yield* make();
        expect(yield* service.isAvailable).toBe(false);
        expect(yield* service.read).toMatchObject({
          state: "unavailable",
          status: "unconfigured",
          reason: "reader_unconfigured",
          configuredReportId: null,
        });
      }),
    ),
  );

  it.effect("does not dispatch an archive read for unconfigured or invalid bindings", () =>
    Effect.scoped(
      Effect.gen(function* () {
        for (const checkBinding of [
          Effect.succeed({
            status: "unconfigured",
            reason: "host_binding_unverified",
            configuredReportId: REPORT_ID,
          } as const),
          Effect.succeed({ status: "bound", configuredReportId: "not-a-sha" } as never),
        ]) {
          let reads = 0;
          const service = yield* make({
            checkBinding,
            readSummary: () =>
              Effect.sync(() => {
                reads += 1;
                return encodeJson(report());
              }),
          });
          expect(yield* service.isAvailable).toBe(false);
          expect(yield* service.read).toMatchObject({
            state: "unavailable",
            status: "unconfigured",
            reason: "host_binding_unverified",
          });
          expect(reads).toBe(0);
        }
      }),
    ),
  );

  it.effect(
    "preserves nulls, identities and window separately from the server observation time",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let request: Parameters<TokenAccountingReaderPort["readSummary"]>[0] | undefined;
          const expected = report();
          const service = yield* make(
            reader((input) =>
              Effect.sync(() => {
                request = input;
                return encodeJson(expected);
              }),
            ),
          );
          const readAt = DateTime.formatIso(yield* DateTime.now);
          expect(yield* service.isAvailable).toBe(true);
          expect(yield* service.read).toEqual({ state: "ready", readAt, report: expected });
          expect(request).toEqual({ reportId: REPORT_ID, limits: TOKEN_ACCOUNTING_READER_LIMITS });
          expect(readAt).not.toBe(expected.snapshot.captured_at);
        }),
      ),
  );

  it.effect("rejects configured identity mismatch and undeclared projection data", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const mismatch = yield* make(
          reader(() => Effect.succeed(encodeJson({ ...report(), report_id: "d".repeat(64) }))),
        );
        expect(yield* mismatch.read).toMatchObject({
          state: "unavailable",
          status: "invalid",
          reason: "configured_report_id_mismatch",
        });
        const extra = yield* make(
          reader(() => Effect.succeed(encodeJson({ ...report(), visible_inventory: {} }))),
        );
        expect(yield* extra.read).toMatchObject({
          state: "unavailable",
          status: "invalid",
          reason: "projection_invalid",
        });
      }),
    ),
  );

  it.effect("rejects excessive groups and root identities without truncating them", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const source = report();
        const cases = [
          {
            ...source,
            input: {
              ...source.input,
              groups: Array.from({ length: 129 }, () => ({
                group: "guidance",
                subtype: "system",
                basis: "measured_component",
                central: 0,
                low: 0,
                high: 0,
              })),
            },
          },
          {
            ...source,
            provider_coverage: {
              ...source.provider_coverage,
              latest_scan: {
                ...source.provider_coverage.latest_scan,
                root_scope_ids: Array.from({ length: 257 }, () => REPORT_ID),
              },
            },
          },
        ];
        for (const value of cases) {
          const service = yield* make(reader(() => Effect.succeed(encodeJson(value))));
          expect(yield* service.read).toMatchObject({
            state: "unavailable",
            status: "oversized",
            reason: "collection_limit_exceeded",
          });
        }
      }),
    ),
  );

  it.effect("rejects estimator status counts that disagree with source coverage", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const service = yield* make(
          reader(() =>
            Effect.succeed(
              encodeJson({
                ...report(),
                estimator_status_counts: { qualified: 0, unavailable: 1, failed: 0 },
              }),
            ),
          ),
        );
        expect(yield* service.read).toMatchObject({
          state: "unavailable",
          status: "invalid",
          reason: "projection_invalid",
        });
      }),
    ),
  );

  it.effect("enforces the response byte cap before parsing and includes the result envelope", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const tooLarge = yield* make(
          reader(() => Effect.succeed("é".repeat(TOKEN_ACCOUNTING_MAX_RESPONSE_BYTES / 2 + 1))),
        );
        expect(yield* tooLarge.read).toMatchObject({
          state: "unavailable",
          status: "oversized",
          reason: "projection_too_large",
        });
        const payload = encodeJson(report());
        const exactInputLimit =
          payload + " ".repeat(TOKEN_ACCOUNTING_MAX_RESPONSE_BYTES - payload.length);
        const envelope = yield* make(reader(() => Effect.succeed(exactInputLimit)));
        expect(yield* envelope.read).toMatchObject({ state: "ready" });
        expect(encodeJson(yield* envelope.read).length).toBeLessThanOrEqual(
          TOKEN_ACCOUNTING_MAX_RESPONSE_BYTES,
        );
      }),
    ),
  );

  it.effect("keeps reader failures local and omits exception details", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const service = yield* make(
          reader(() =>
            Effect.fail(new TokenAccountingReaderError({ cause: "synthetic-private-path" })),
          ),
        );
        const result = yield* service.read;
        expect(result).toMatchObject({
          state: "unavailable",
          status: "reader_failed",
          reason: "reader_failed",
        });
        expect(encodeJson(result)).not.toContain("synthetic-private-path");
        const unavailable = yield* make(
          reader(() => Effect.succeed({ status: "missing", reason: "configured_report_missing" })),
        );
        expect(yield* unavailable.read).toMatchObject({
          state: "unavailable",
          status: "missing",
          reason: "configured_report_missing",
        });
      }),
    ),
  );

  it.effect("shares an overlapping read and starts a new observation after completion", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let reads = 0;
        const service = yield* make(
          reader(() =>
            Effect.gen(function* () {
              reads += 1;
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
              return encodeJson(report());
            }),
          ),
        );
        const first = yield* service.read.pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        const second = yield* service.read.pipe(Effect.forkChild);
        yield* Effect.yieldNow;
        yield* Deferred.succeed(release, undefined);
        expect(yield* Fiber.join(first)).toEqual(yield* Fiber.join(second));
        expect(reads).toBe(1);
        yield* service.read;
        expect(reads).toBe(2);
      }),
    ),
  );

  it.effect("bounds a stalled reader by the five-second deadline", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const service = yield* make(
          reader(() => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))),
        );
        const pending = yield* service.read.pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        yield* TestClock.adjust("5 seconds");
        expect(yield* Fiber.join(pending)).toMatchObject({
          state: "unavailable",
          status: "reader_failed",
          reason: "reader_timeout",
        });
      }),
    ),
  );
});
