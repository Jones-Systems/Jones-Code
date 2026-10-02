import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  TokenAccountingMetric,
  TokenAccountingReadInput,
  TokenAccountingReadResult,
  TokenAccountingReport,
} from "./tokenAccounting.ts";

describe("saved token accounting projection", () => {
  it("preserves a known sum independently from an unknown complete total", () => {
    const metric = { known_sum: 42, total: null, known_requests: 1, missing_requests: 1 };
    expect(Schema.decodeUnknownSync(TokenAccountingMetric)(metric)).toEqual(metric);
  });

  it("preserves entirely unknown metrics instead of normalizing them to zero", () => {
    const metric = { known_sum: null, total: null, known_requests: 0, missing_requests: 2 };
    expect(Schema.decodeUnknownSync(TokenAccountingMetric)(metric)).toEqual(metric);
  });

  it("rejects unsafe counts and undeclared metric fields", () => {
    const decode = Schema.decodeUnknownSync(TokenAccountingMetric);
    const metric = { known_sum: null, total: null, known_requests: 0, missing_requests: 2 };
    expect(() => decode({ ...metric, known_sum: Number.MAX_SAFE_INTEGER + 1 })).toThrow();
    expect(() => decode({ ...metric, complete: true })).toThrow();
  });

  it("rejects an original report presented as the consumer projection", () => {
    expect(() =>
      Schema.decodeUnknownSync(TokenAccountingReport)({
        schema: "programmatic-token-info.accounting-report/v1",
        report_id: "a".repeat(64),
      }),
    ).toThrow();
  });

  it("rejects caller paths, report selectors and execution flags", () => {
    const decode = Schema.decodeUnknownSync(TokenAccountingReadInput);
    expect(decode({})).toEqual({});
    for (const input of [{ path: "saved.json" }, { reportId: "a".repeat(64) }, { args: [] }]) {
      expect(() => decode(input)).toThrow();
    }
  });

  it("keeps status and reason paired and rejects diagnostic fields", () => {
    const decode = Schema.decodeUnknownSync(TokenAccountingReadResult);
    const result = {
      state: "unavailable",
      status: "missing",
      reason: "configured_report_missing",
      configuredReportId: "a".repeat(64),
      readAt: "2026-10-02T12:00:00Z",
    };
    expect(decode(result)).toEqual(result);
    expect(() => decode({ ...result, status: "unsupported" })).toThrow();
    expect(() => decode({ ...result, stderr: "synthetic diagnostic" })).toThrow();
    expect(() => decode({ ...result, configuredReportId: "A".repeat(64) })).toThrow();
  });

  it("accepts native allocation pairs and rejects impossible pairs in either direction", () => {
    const decodeInput = Schema.decodeUnknownSync(TokenAccountingReport.fields.input.fields.groups);
    const decodeOutput = Schema.decodeUnknownSync(
      TokenAccountingReport.fields.output.fields.groups,
    );
    const group = (name: string, subtype: string) => ({
      group: name,
      subtype,
      basis: "measured_component",
      central: 0,
      low: 0,
      high: 0,
    });
    const input = [
      group("guidance", "system"),
      group("tool_result", "file_read_code"),
      group("assistant_history", "tool_call_arguments"),
      group("tool_schema_config", "unknown"),
    ];
    const output = [
      group("assistant_text", "final"),
      group("tool_call_arguments", "agent_launch"),
      group("other_generated", "unknown"),
    ];
    expect(decodeInput(input)).toEqual(input);
    expect(decodeOutput(output)).toEqual(output);
    for (const invalid of [
      group("guidance", "file_writing"),
      group("tool_result", "system"),
      group("user_role_message", "session_task"),
      group("assistant_text", "commentary"),
      group("reasoning", "unknown"),
      group("unknown", "unknown"),
    ])
      expect(() => decodeInput([invalid])).toThrow();
    for (const invalid of [
      group("guidance", "system"),
      group("assistant_text", "execution"),
      group("tool_call_arguments", "system"),
      group("other_generated", "final"),
      group("reasoning", "unknown"),
      group("unknown", "unknown"),
    ])
      expect(() => decodeOutput([invalid])).toThrow();
  });

  it("requires history scope and indexed selection together while preserving their absence", () => {
    const decode = Schema.decodeUnknownSync(TokenAccountingReport.fields.provider_coverage);
    const coverage = {
      selected_requests: 0,
      accounting_status_counts: {
        primary: 0,
        legacy_unresolved: 0,
        conflict: 0,
        aggregate_delta: 0,
      },
      token_missingness: {
        input_tokens: 0,
        cached_input_tokens: 0,
        cache_write_input_tokens: 0,
        cache_write_5m_tokens: 0,
        cache_write_1h_tokens: 0,
        output_tokens: 0,
        reasoning_output_tokens: 0,
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
    };
    const selection = {
      requested_thread_count: 2,
      indexed_thread_count: 1,
      status: "partially_indexed",
    };
    const scoped = {
      ...coverage,
      history_scope: "all_indexed_history",
      indexed_history_selection: selection,
    };
    expect(decode(coverage)).toEqual(coverage);
    expect(decode(scoped)).toEqual(scoped);
    expect(() => decode({ ...coverage, history_scope: "all_indexed_history" })).toThrow();
    expect(() => decode({ ...coverage, indexed_history_selection: selection })).toThrow();
  });

  it("requires real UTC calendar timestamps including Gregorian leap-year boundaries", () => {
    const decode = Schema.decodeUnknownSync(TokenAccountingReadResult);
    const observation = {
      state: "unavailable",
      status: "unconfigured",
      reason: "reader_unconfigured",
      configuredReportId: null,
    };
    for (const readAt of [
      "2024-02-29T23:59:59.123456Z",
      "2000-02-29T00:00:00Z",
      "2026-10-02T12:00:00.123Z",
    ]) {
      expect(decode({ ...observation, readAt })).toEqual({ ...observation, readAt });
    }
    for (const readAt of [
      "1900-02-29T00:00:00Z",
      "2026-02-29T00:00:00Z",
      "2026-04-31T00:00:00Z",
      "2026-00-01T00:00:00Z",
      "2026-13-01T00:00:00Z",
      "2026-01-00T00:00:00Z",
      "0000-01-01T00:00:00Z",
      "2026-01-01T24:00:00Z",
      "2026-01-01T00:60:00Z",
      "2026-01-01T00:00:60Z",
      "2026-01-01T00:00:00.1234567Z",
      "2026-01-01T00:00:00+00:00",
    ])
      expect(() => decode({ ...observation, readAt })).toThrow();
  });

  it("requires an ordered finite window at microsecond precision and preserves indexed history", () => {
    const decode = Schema.decodeUnknownSync(TokenAccountingReport.fields.window);
    const finite = {
      start: "2026-10-02T12:00:00.100001Z",
      end: "2026-10-02T12:00:00.100002Z",
      end_exclusive: true,
    };
    expect(decode(finite)).toEqual(finite);
    const history = { scope: "all_indexed_history", start: null, end: null, end_exclusive: false };
    expect(decode(history)).toEqual(history);
    for (const invalid of [
      { ...finite, start: finite.end, end: finite.start },
      { ...finite, end: finite.start },
      { ...finite, start: "2026-10-02T12:00:00.1Z", end: "2026-10-02T12:00:00.100000Z" },
      { ...finite, start: "2026-10-02T12:00:00Z", end: "2026-10-02T12:00:00.000000Z" },
      { ...finite, start: null },
      { ...finite, end: "2026-02-30T12:00:00Z" },
    ])
      expect(() => decode(invalid)).toThrow();
  });
});
