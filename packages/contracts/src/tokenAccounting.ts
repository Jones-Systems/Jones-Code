import * as Schema from "effect/Schema";

export const TOKEN_ACCOUNTING_MAX_INPUT_BYTES = 2 * 1024 * 1024;
export const TOKEN_ACCOUNTING_MAX_RESPONSE_BYTES = 256 * 1024;
export const TOKEN_ACCOUNTING_MAX_GROUPS = 128;
export const TOKEN_ACCOUNTING_MAX_SOURCE_COLLECTION = 256;
export const TOKEN_ACCOUNTING_PROJECTION_SCHEMA = "jones-code.token-accounting-projection/v1";
export const TOKEN_ACCOUNTING_REPORT_SCHEMA = "programmatic-token-info.accounting-report/v1";
export const TOKEN_ACCOUNTING_IDENTITY_ALGORITHM = "programmatic-token-info.binary64-tree/v1";

// Input-side checks see undeclared keys that Struct removes from its decoded output.
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
const Count = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }));
const TIMESTAMP_PATTERN = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,6})?Z$/;
function isCalendarUtcTimestamp(value: string): boolean {
  const parts = TIMESTAMP_PATTERN.exec(value);
  if (parts === null) return false;
  const year = Number(parts[1]);
  const month = Number(parts[2]);
  const day = Number(parts[3]);
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return (
    year >= 1 &&
    month >= 1 &&
    month <= 12 &&
    day >= 1 &&
    day <= (daysInMonth[month - 1] ?? 0) &&
    Number(parts[4]) <= 23 &&
    Number(parts[5]) <= 59 &&
    Number(parts[6]) <= 59
  );
}
const Timestamp = Schema.String.check(
  Schema.isMaxLength(32),
  Schema.makeFilter(isCalendarUtcTimestamp),
);
const comparableTimestamp = (value: string): string =>
  value.replace(
    /(?:\.(\d{1,6}))?Z$/,
    (_match: string, fraction: string | undefined) => `.${(fraction ?? "").padEnd(6, "0")}Z`,
  );
export const TokenAccountingReportId = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/));
const boundedGroups = <S extends Schema.Top>(schema: S) =>
  Schema.Array(schema).check(Schema.isMaxLength(TOKEN_ACCOUNTING_MAX_GROUPS));

export const TokenAccountingMetric = closed(
  Schema.Struct({
    known_sum: Schema.NullOr(Count),
    total: Schema.NullOr(Count),
    known_requests: Count,
    missing_requests: Count,
  }),
).check(
  Schema.makeFilter(
    (metric) =>
      (metric.known_requests === 0
        ? metric.known_sum === null && metric.total === null
        : metric.known_sum !== null) &&
      (metric.total === null ||
        (metric.missing_requests === 0 && metric.total === metric.known_sum)),
  ),
);
export type TokenAccountingMetric = typeof TokenAccountingMetric.Type;

const tokenMetricFields = {
  input_tokens: TokenAccountingMetric,
  cached_input_tokens: TokenAccountingMetric,
  cache_write_input_tokens: TokenAccountingMetric,
  cache_write_5m_tokens: TokenAccountingMetric,
  cache_write_1h_tokens: TokenAccountingMetric,
  output_tokens: TokenAccountingMetric,
  reasoning_output_tokens: TokenAccountingMetric,
};
const statusCounts = closed(
  Schema.Struct({
    primary: Count,
    legacy_unresolved: Count,
    conflict: Count,
    aggregate_delta: Count,
  }),
);
const receiptStatusCounts = closed(
  Schema.Struct({
    selected_stat_unchanged: Schema.optionalKey(Count),
    selected_rebuilt: Schema.optionalKey(Count),
    selected_append: Schema.optionalKey(Count),
    retained_missing: Schema.optionalKey(Count),
    retained_not_selected: Schema.optionalKey(Count),
    archive_snapshot: Schema.optionalKey(Count),
  }),
);
const providerCoverage = closed(
  Schema.Struct({
    selected_requests: Count,
    accounting_status_counts: statusCounts,
    token_missingness: closed(
      Schema.Struct({
        input_tokens: Count,
        cached_input_tokens: Count,
        cache_write_input_tokens: Count,
        cache_write_5m_tokens: Count,
        cache_write_1h_tokens: Count,
        output_tokens: Count,
        reasoning_output_tokens: Count,
      }),
    ),
    latest_scan: closed(
      Schema.Struct({
        captured_at: Schema.NullOr(Timestamp),
        selected_files: Schema.NullOr(Count),
        refreshed_selected_files: Schema.NullOr(Count),
        partial_selected_files: Schema.NullOr(Count),
        root_scope_ids: Schema.Array(TokenAccountingReportId).check(
          Schema.isMaxLength(TOKEN_ACCOUNTING_MAX_SOURCE_COLLECTION),
        ),
        mtime_cutoff: Schema.NullOr(Timestamp),
        receipts_status_counts: Schema.NullOr(receiptStatusCounts),
        validated_files: Schema.NullOr(Count),
      }),
    ),
    cumulative_index: closed(
      Schema.Struct({
        tracked_files: Schema.NullOr(Count),
        retained_not_refreshed_files: Schema.NullOr(Count),
        missing_tracked_files: Schema.NullOr(Count),
      }),
    ),
    freshness: closed(
      Schema.Struct({
        status: Schema.Literals([
          "scan_unavailable",
          "archive_snapshot_only",
          "validation_receipts_unavailable",
          "no_selected_source_validated",
          "requested_end_unspecified",
          "requested_end_after_selected_validation",
          "retained_sources_not_refreshed",
          "selected_sources_validated_after_requested_end",
        ]),
        requested_end: Schema.NullOr(Timestamp),
        last_scan_captured_at: Schema.NullOr(Timestamp),
        selected_validation_min: Schema.NullOr(Timestamp),
        selected_validation_max: Schema.NullOr(Timestamp),
        unrefreshed_files: Schema.NullOr(Count),
      }),
    ),
    historical_window_completeness: Schema.Literal("not_proven"),
    history_scope: Schema.optionalKey(Schema.Literal("all_indexed_history")),
    indexed_history_selection: Schema.optionalKey(
      closed(
        Schema.Struct({
          requested_thread_count: Count,
          indexed_thread_count: Count,
          status: Schema.Literals(["no_indexed_requests", "partially_indexed", "indexed"]),
        }),
      ),
    ),
  }),
).check(
  Schema.makeFilter(
    (coverage) =>
      (coverage.history_scope === undefined) === (coverage.indexed_history_selection === undefined),
  ),
);
const window = Schema.Union([
  closed(
    Schema.Struct({ start: Timestamp, end: Timestamp, end_exclusive: Schema.Literal(true) }),
  ).check(
    Schema.makeFilter((value) => comparableTimestamp(value.start) < comparableTimestamp(value.end)),
  ),
  closed(
    Schema.Struct({
      scope: Schema.Literal("all_indexed_history"),
      start: Schema.Null,
      end: Schema.Null,
      end_exclusive: Schema.Literal(false),
    }),
  ),
]);
const snapshot = closed(
  Schema.Struct({
    index_snapshot_id: TokenAccountingReportId,
    source_revision: Schema.optionalKey(TokenAccountingReportId),
    mapping_revision: Schema.optionalKey(TokenAccountingReportId),
    scan_id: Schema.optionalKey(TokenAccountingReportId),
    captured_at: Schema.optionalKey(Timestamp),
    schema_version: Schema.optionalKey(Schema.Literal("programmatic-token-info.index/v1")),
    source_validation: Schema.optionalKey(
      Schema.Literals(["provider_append_only_stat_and_anchor", "archive_snapshot_only"]),
    ),
  }),
);
const allocationBandFields = {
  basis: Schema.Literals(["measured_component", "estimated_calibrated"]),
  central: Count,
  low: Count,
  high: Count,
};
const inputAllocationGroup = Schema.Union([
  closed(
    Schema.Struct({
      group: Schema.Literals([
        "user_role_message",
        "tool_schema_config",
        "client_control",
        "nontext_input",
      ]),
      subtype: Schema.Literal("unknown"),
      ...allocationBandFields,
    }),
  ),
  closed(
    Schema.Struct({
      group: Schema.Literal("guidance"),
      subtype: Schema.Literals([
        "base_instructions",
        "developer",
        "system",
        "session_task",
        "unknown",
      ]),
      ...allocationBandFields,
    }),
  ),
  closed(
    Schema.Struct({
      group: Schema.Literal("tool_result"),
      subtype: Schema.Literals([
        "execution",
        "file_read_code",
        "file_read_document",
        "search",
        "build_test",
        "vcs",
        "web",
        "mcp",
        "agent",
        "patch",
        "other",
        "unknown",
      ]),
      ...allocationBandFields,
    }),
  ),
  closed(
    Schema.Struct({
      group: Schema.Literal("assistant_history"),
      subtype: Schema.Literals(["assistant_text", "tool_call_arguments", "unknown"]),
      ...allocationBandFields,
    }),
  ),
]);
const outputAllocationGroup = Schema.Union([
  closed(
    Schema.Struct({
      group: Schema.Literal("assistant_text"),
      subtype: Schema.Literals(["final", "commentary", "unphased", "unknown"]),
      ...allocationBandFields,
    }),
  ),
  closed(
    Schema.Struct({
      group: Schema.Literal("tool_call_arguments"),
      subtype: Schema.Literals(["file_writing", "agent_launch", "other", "unknown"]),
      ...allocationBandFields,
    }),
  ),
  closed(
    Schema.Struct({
      group: Schema.Literal("other_generated"),
      subtype: Schema.Literal("unknown"),
      ...allocationBandFields,
    }),
  ),
]);
const unknownByReason = closed(
  Schema.Struct({
    missing_counter: Schema.optionalKey(TokenAccountingMetric),
    source_unavailable: Schema.optionalKey(TokenAccountingMetric),
    ambiguous_join: Schema.optionalKey(TokenAccountingMetric),
    estimator_unavailable: Schema.optionalKey(TokenAccountingMetric),
    cache_placement_unsupported: Schema.optionalKey(TokenAccountingMetric),
    envelope_incomplete: Schema.optionalKey(TokenAccountingMetric),
    incompatible_scenario: Schema.optionalKey(TokenAccountingMetric),
    outside_estimator_domain: Schema.optionalKey(TokenAccountingMetric),
    nontext_unqualified: Schema.optionalKey(TokenAccountingMetric),
    framing_or_hidden_unknown: Schema.optionalKey(TokenAccountingMetric),
  }),
);
const allocationFields = {
  allocated: TokenAccountingMetric,
  unknown: TokenAccountingMetric,
  unknown_by_reason: unknownByReason,
  estimated_coverage: Schema.NullOr(
    Schema.Number.check(Schema.isFinite(), Schema.isBetween({ minimum: 0, maximum: 1 })),
  ),
};
const partition = closed(
  Schema.Struct({
    requests: Count,
    metrics: closed(Schema.Struct({ ...tokenMetricFields, ordinary_input: TokenAccountingMetric })),
  }),
);
export const TOKEN_ACCOUNTING_CAVEATS = [
  "primary_requests_only_are_additive",
  "missing_counters_leave_totals_unknown",
  "visible_inventory_is_not_submitted_payload",
  "user_role_does_not_prove_owner_speech",
  "candidate_envelope_is_unproved_submission",
  "cache_placement_unsupported",
  "unknown_residuals_are_not_zero",
  "reasoning_is_an_output_subset",
  "mechanism_flags_are_nonadditive",
  "counter_changes_are_diagnostics_only",
  "bands_use_held_out_group_bias",
  "historical_window_completeness_not_proven",
  "append_only_source_validation_limit",
  "partial_counter_masks_limit_aggregate_conservation",
] as const;

/** This is a bounded projection; Python validates the original report identity and conservation. */
export const TokenAccountingReport = closed(
  Schema.Struct({
    schema: Schema.Literal(TOKEN_ACCOUNTING_PROJECTION_SCHEMA),
    source_schema: Schema.Literal(TOKEN_ACCOUNTING_REPORT_SCHEMA),
    report_id: TokenAccountingReportId,
    selection_id: TokenAccountingReportId,
    source_identity_algorithm: Schema.Literal(TOKEN_ACCOUNTING_IDENTITY_ALGORITHM),
    source_identity_verified: Schema.Literal(true),
    snapshot,
    window,
    provider_coverage: providerCoverage,
    provider_ledger: closed(
      Schema.Struct({
        requests: Count,
        primary_requests: Count,
        nonadditive_requests: Count,
        accounting_status_counts: statusCounts,
        metrics: closed(
          Schema.Struct({
            ...tokenMetricFields,
            ordinary_input_tokens: TokenAccountingMetric,
            non_read_input_tokens: TokenAccountingMetric,
          }),
        ),
      }),
    ),
    input: closed(
      Schema.Struct({
        ordinary_input: TokenAccountingMetric,
        groups: boundedGroups(inputAllocationGroup),
        ...allocationFields,
      }),
    ),
    output: closed(
      Schema.Struct({
        output: TokenAccountingMetric,
        measured_reasoning: TokenAccountingMetric,
        groups: boundedGroups(outputAllocationGroup),
        ...allocationFields,
      }),
    ),
    partitions: closed(
      Schema.Struct({
        provider: closed(Schema.Struct({ codex: partition, claude: partition })),
        role: closed(Schema.Struct({ root: partition, child: partition, unknown: partition })),
      }),
    ),
    mechanism_flags: closed(
      Schema.Struct({
        nonadditive: Schema.Literal(true),
        counts: closed(
          Schema.Struct({
            new_content: Schema.optionalKey(Count),
            repeated_history: Schema.optionalKey(Count),
            changed_guidance_schema_prefix: Schema.optionalKey(Count),
            compaction_restart: Schema.optionalKey(Count),
            first_observation_or_missing_predecessor: Schema.optionalKey(Count),
            cache_miss_unchanged_reconstruction: Schema.optionalKey(Count),
            mixed_unknown: Schema.optionalKey(Count),
          }),
        ),
      }),
    ),
    estimator_status_counts: closed(
      Schema.Struct({ qualified: Count, unavailable: Count, failed: Count }),
    ),
    coverage: closed(
      Schema.Struct({
        primary_requests: Count,
        captured_requests: Count,
        unavailable_requests: Count,
        complete_response_requests: Count,
        input_allocated_requests: Count,
        output_allocated_requests: Count,
        reasoning_measured_requests: Count,
        reasoning_missing_requests: Count,
        estimator_qualified_models: Count,
        estimator_unavailable_models: Count,
        diagnostics: closed(
          Schema.Struct({
            requests_output_without_generated_components: Schema.optionalKey(Count),
          }),
        ),
      }),
    ),
    caveats: Schema.Array(Schema.Literals(TOKEN_ACCOUNTING_CAVEATS)).check(
      Schema.makeFilter(
        (values) =>
          values.length === TOKEN_ACCOUNTING_CAVEATS.length &&
          values.every((value, index) => value === TOKEN_ACCOUNTING_CAVEATS[index]),
      ),
    ),
    authority_effect: Schema.Literal("none"),
  }),
).check(
  Schema.makeFilter(
    (report) =>
      report.estimator_status_counts.qualified === report.coverage.estimator_qualified_models &&
      report.estimator_status_counts.unavailable + report.estimator_status_counts.failed ===
        report.coverage.estimator_unavailable_models,
  ),
);
export type TokenAccountingReport = typeof TokenAccountingReport.Type;

export const TokenAccountingReadInput = closed(Schema.Struct({}));
export type TokenAccountingReadInput = typeof TokenAccountingReadInput.Type;

const unavailableVariants = [
  closed(
    Schema.Struct({
      status: Schema.Literal("unconfigured"),
      reason: Schema.Literals([
        "reader_unconfigured",
        "report_unconfigured",
        "host_binding_unverified",
      ]),
    }),
  ),
  closed(
    Schema.Struct({
      status: Schema.Literal("missing"),
      reason: Schema.Literal("configured_report_missing"),
    }),
  ),
  closed(
    Schema.Struct({
      status: Schema.Literal("invalid"),
      reason: Schema.Literals([
        "report_invalid",
        "report_identity_mismatch",
        "configured_report_id_mismatch",
        "projection_invalid",
      ]),
    }),
  ),
  closed(
    Schema.Struct({
      status: Schema.Literal("unsupported"),
      reason: Schema.Literals(["report_schema_unsupported", "identity_algorithm_unsupported"]),
    }),
  ),
  closed(
    Schema.Struct({
      status: Schema.Literal("oversized"),
      reason: Schema.Literals([
        "input_too_large",
        "projection_too_large",
        "collection_limit_exceeded",
      ]),
    }),
  ),
  closed(
    Schema.Struct({
      status: Schema.Literal("reader_failed"),
      reason: Schema.Literals(["reader_timeout", "reader_failed"]),
    }),
  ),
] as const;
export const TokenAccountingUnavailable = Schema.Union(unavailableVariants);
export type TokenAccountingUnavailable = typeof TokenAccountingUnavailable.Type;

const observationFields = {
  configuredReportId: Schema.NullOr(TokenAccountingReportId),
  readAt: Timestamp,
};
export const TokenAccountingReadResult = Schema.Union([
  closed(
    Schema.Struct({
      state: Schema.Literal("ready"),
      report: TokenAccountingReport,
      readAt: Timestamp,
    }),
  ),
  ...unavailableVariants.map((variant) =>
    closed(
      Schema.Struct({
        state: Schema.Literal("unavailable"),
        ...variant.fields,
        ...observationFields,
      }),
    ),
  ),
]);
export type TokenAccountingReadResult = typeof TokenAccountingReadResult.Type;
