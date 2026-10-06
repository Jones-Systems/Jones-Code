// @vitest-environment jsdom
import {
  EnvironmentId,
  TOKEN_ACCOUNTING_CAVEATS,
  TOKEN_ACCOUNTING_IDENTITY_ALGORITHM,
  TOKEN_ACCOUNTING_PROJECTION_SCHEMA,
  TOKEN_ACCOUNTING_REPORT_SCHEMA,
  TokenAccountingReadResult,
  type TokenAccountingMetric,
  type TokenAccountingReport,
} from "@t3tools/contracts";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import {
  AVAILABLE_CONNECTION_STATE,
  type SupervisorConnectionState,
} from "@t3tools/client-runtime/connection";
import * as Schema from "effect/Schema";
import { AsyncResult } from "effect/unstable/reactivity";
import { StrictMode, act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

const testState = vi.hoisted(() => ({
  presentations: new Map(),
  connections: new Map(),
  read: vi.fn(),
}));

vi.mock("@effect/atom-react", () => ({
  useAtomValue: (atom: unknown) =>
    atom === "presentations" ? testState.presentations : testState.connections.get(atom),
}));
vi.mock("../../state/presentation", () => ({
  environmentPresentations: { presentationsAtom: "presentations" },
}));
vi.mock("../../connection/catalog", () => ({
  environmentCatalog: { stateAtom: (environmentId: EnvironmentId) => environmentId },
}));
vi.mock("../../state/server", () => ({
  serverEnvironment: { readTokenAccounting: "read-accounting" },
}));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => testState.read }));

import { SavedTokenAccounting } from "../../jones/usage/SavedTokenAccounting";

const REPORT_ID = "a".repeat(64);
const READ_AT = "2026-10-02T15:00:00Z";
const primaryId = EnvironmentId.make("primary");

function metric(value: number): TokenAccountingMetric {
  return { total: value, known_sum: value, known_requests: 2, missing_requests: 0 };
}

// This synthetic projection proves client behavior, not original source identity.
function report(): TokenAccountingReport {
  const status = { primary: 2, legacy_unresolved: 1, conflict: 1, aggregate_delta: 1 };
  const metrics = {
    input_tokens: metric(100),
    cached_input_tokens: metric(20),
    cache_write_input_tokens: metric(10),
    cache_write_5m_tokens: metric(10),
    cache_write_1h_tokens: metric(0),
    output_tokens: metric(10),
    reasoning_output_tokens: metric(3),
  };
  const partition = { requests: 2, metrics: { ...metrics, ordinary_input: metric(70) } };
  const absent: TokenAccountingMetric = {
    total: null,
    known_sum: null,
    known_requests: 0,
    missing_requests: 0,
  };
  const emptyPartition = {
    requests: 0,
    metrics: {
      input_tokens: absent,
      cached_input_tokens: absent,
      cache_write_input_tokens: absent,
      cache_write_5m_tokens: absent,
      cache_write_1h_tokens: absent,
      output_tokens: absent,
      reasoning_output_tokens: absent,
      ordinary_input: absent,
    },
  };
  return {
    schema: TOKEN_ACCOUNTING_PROJECTION_SCHEMA,
    source_schema: TOKEN_ACCOUNTING_REPORT_SCHEMA,
    source_identity_algorithm: TOKEN_ACCOUNTING_IDENTITY_ALGORITHM,
    source_identity_verified: true,
    report_id: REPORT_ID,
    selection_id: "b".repeat(64),
    snapshot: {
      index_snapshot_id: "c".repeat(64),
      captured_at: "2026-10-01T11:00:00Z",
      source_validation: "archive_snapshot_only",
    },
    window: { start: "2026-09-01T00:00:00Z", end: "2026-10-01T00:00:00Z", end_exclusive: true },
    provider_coverage: {
      selected_requests: 5,
      accounting_status_counts: status,
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
        status: "archive_snapshot_only",
        requested_end: "2026-10-01T00:00:00Z",
        last_scan_captured_at: null,
        selected_validation_min: null,
        selected_validation_max: null,
        unrefreshed_files: null,
      },
      historical_window_completeness: "not_proven",
    },
    provider_ledger: {
      requests: 5,
      primary_requests: 2,
      nonadditive_requests: 3,
      accounting_status_counts: status,
      metrics: { ...metrics, ordinary_input_tokens: metric(70), non_read_input_tokens: metric(80) },
    },
    input: {
      ordinary_input: metric(70),
      allocated: metric(60),
      unknown: metric(10),
      unknown_by_reason: { framing_or_hidden_unknown: metric(10) },
      estimated_coverage: 60 / 70,
      groups: [
        {
          group: "guidance",
          subtype: "developer",
          basis: "estimated_calibrated",
          central: 60,
          low: 50,
          high: 70,
        },
      ],
    },
    output: {
      output: metric(10),
      measured_reasoning: metric(3),
      allocated: metric(7),
      unknown: metric(0),
      unknown_by_reason: {},
      estimated_coverage: 1,
      groups: [
        {
          group: "assistant_text",
          subtype: "final",
          basis: "measured_component",
          central: 7,
          low: 7,
          high: 7,
        },
      ],
    },
    partitions: {
      provider: { codex: partition, claude: emptyPartition },
      role: { root: partition, child: emptyPartition, unknown: emptyPartition },
    },
    mechanism_flags: { nonadditive: true, counts: { new_content: 2, repeated_history: 2 } },
    estimator_status_counts: { qualified: 1, unavailable: 1, failed: 1 },
    coverage: {
      primary_requests: 2,
      captured_requests: 2,
      unavailable_requests: 0,
      complete_response_requests: 2,
      input_allocated_requests: 2,
      output_allocated_requests: 2,
      reasoning_measured_requests: 2,
      reasoning_missing_requests: 0,
      estimator_qualified_models: 1,
      estimator_unavailable_models: 2,
      diagnostics: {},
    },
    caveats: TOKEN_ACCOUNTING_CAVEATS,
    authority_effect: "none",
  };
}

function ready(saved = report()): TokenAccountingReadResult {
  return Schema.decodeUnknownSync(TokenAccountingReadResult)({
    state: "ready",
    report: saved,
    readAt: READ_AT,
  });
}

function environment(
  id: EnvironmentId,
  {
    primary = true,
    capability = true,
    phase = "connected",
  }: {
    readonly primary?: boolean;
    readonly capability?: boolean | "omitted";
    readonly phase?: "connected" | "offline";
  } = {},
) {
  return {
    entry: {
      target: {
        environmentId: id,
        label: id,
        _tag: primary ? "PrimaryConnectionTarget" : "BearerConnectionTarget",
      },
      enabled: true,
    },
    connection: { phase },
    serverConfig: {
      environment: {
        capabilities: capability === "omitted" ? {} : { savedTokenAccounting: capability },
      },
    },
  };
}

function connect(
  id = primaryId,
  generation = 1,
  phase: SupervisorConnectionState["phase"] = "connected",
) {
  testState.connections = new Map(testState.connections).set(
    id,
    AsyncResult.success({ ...AVAILABLE_CONNECTION_STATE, phase, generation }),
  );
  const presentation = testState.presentations.get(id);
  if (presentation !== undefined) {
    // State updates rebuild the real presentation and map. Preserve the supplied
    // projected phase so the lag test can still exercise the connection guard.
    testState.presentations = new Map(testState.presentations).set(id, { ...presentation });
  }
}

function deferredRead() {
  let resolve!: (value: AtomCommandResult<TokenAccountingReadResult, never>) => void;
  const promise = new Promise<AtomCommandResult<TokenAccountingReadResult, never>>((done) => {
    resolve = done;
  });
  testState.read.mockReturnValueOnce(promise);
  return (value = ready()) => resolve(AsyncResult.success(value));
}

describe("saved token accounting panel", () => {
  let renderer: Root;
  let container: HTMLDivElement;
  let mounted: boolean;

  beforeEach(() => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    container = document.createElement("div");
    document.body.append(container);
    renderer = createRoot(container);
    mounted = true;
    testState.read.mockReset().mockResolvedValue(AsyncResult.success(ready()));
    testState.presentations = new Map([[primaryId, environment(primaryId)]]);
    testState.connections = new Map();
    connect();
  });

  afterEach(async () => {
    if (mounted) await act(() => renderer.unmount());
    container.remove();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  async function render() {
    await act(() =>
      renderer.render(
        <StrictMode>
          <SavedTokenAccounting />
        </StrictMode>,
      ),
    );
  }

  async function openPanel() {
    const panel = container.querySelector("details");
    if (panel && !panel.open) await act(() => panel.querySelector("summary")!.click());
  }

  async function click(label: string) {
    await openPanel();
    const button = [...container.querySelectorAll("button")].find(
      (item) => item.textContent?.trim() === label,
    );
    expect(button).toBeDefined();
    await act(() => button!.click());
  }

  function metricText(region: Element, label: string) {
    return [...region.querySelectorAll("dt")].find((term) => term.textContent === label)
      ?.nextElementSibling?.textContent;
  }

  it("reads only on an explicit click, including after disclosure, rerender and reconnect", async () => {
    await render();
    const details = container.querySelector("details")!;
    expect(details.open).toBe(false);
    await act(() => container.querySelector("summary")!.click());
    await render();
    expect(testState.read).not.toHaveBeenCalled();
    await click("Load saved report");
    expect(testState.read).toHaveBeenCalledExactlyOnceWith({ environmentId: primaryId, input: {} });
    expect(container.textContent).toContain(REPORT_ID);
    const observation = [...container.querySelectorAll("dt")].find(
      (term) => term.textContent === "Read observation",
    )?.nextElementSibling;
    expect(observation?.querySelector("time")?.dateTime).toBe(READ_AT);
    expect(observation?.textContent).toContain("EDT");
    expect(container.textContent).toContain("end exclusive");
    expect(container.textContent).toContain("independent of Usage date, provider");
    await render();
    expect(testState.read).toHaveBeenCalledTimes(1);
    connect(primaryId, 1, "offline");
    await render();
    expect(container.textContent).not.toContain(REPORT_ID);
    connect(primaryId, 2);
    await render();
    expect(container.textContent).not.toContain(REPORT_ID);
    expect(testState.read).toHaveBeenCalledTimes(1);
    await click("Read again");
    expect(testState.read).toHaveBeenCalledTimes(2);
    expect(container.textContent).toContain(REPORT_ID);
  });

  it.each(["omitted", false] as const)(
    "does not expose or dispatch a reader when capability is %s",
    async (capability) => {
      testState.presentations = new Map(testState.presentations).set(
        primaryId,
        environment(primaryId, { capability }),
      );
      await render();
      expect(container.textContent).toBe("");
      expect(testState.read).not.toHaveBeenCalled();
    },
  );

  it("never dispatches while disconnected, even when presentation is one update behind", async () => {
    connect(primaryId, 1, "offline");
    await render();
    await openPanel();
    const button = container.querySelector("button")!;
    expect(button.disabled).toBe(true);
    await act(() => button.click());
    expect(testState.read).not.toHaveBeenCalled();
    expect(container.textContent).toContain("disconnected");
    testState.presentations = new Map(testState.presentations).set(
      primaryId,
      environment(primaryId, { phase: "offline" }),
    );
    await render();
    expect(container.textContent).toContain("Connect an environment");
    expect(testState.read).not.toHaveBeenCalled();
  });

  it("discards an in-flight result after a connection generation changes", async () => {
    const finish = deferredRead();
    await render();
    await click("Load saved report");
    expect(container.querySelector("button")?.disabled).toBe(true);
    await act(() => container.querySelector("button")!.click());
    expect(testState.read).toHaveBeenCalledTimes(1);
    connect(primaryId, 2);
    await render();
    await act(() => finish());
    expect(container.textContent).not.toContain(REPORT_ID);
    expect(testState.read).toHaveBeenCalledTimes(1);
    await click("Read again");
    expect(container.textContent).toContain(REPORT_ID);
  });

  it("clears results on a target change and drops a late result from the old environment", async () => {
    await render();
    await click("Load saved report");
    const finish = deferredRead();
    await click("Read again");
    const remote = EnvironmentId.make("remote");
    testState.presentations = new Map([[remote, environment(remote, { primary: false })]]);
    connect(remote);
    await render();
    await act(() => finish());
    expect(container.textContent).not.toContain(REPORT_ID);
    expect(testState.read).toHaveBeenCalledTimes(2);
    await click("Load saved report");
    expect(testState.read).toHaveBeenLastCalledWith({ environmentId: remote, input: {} });
  });

  it("drops a late result after unmount and remount starts without a report", async () => {
    const finish = deferredRead();
    await render();
    await click("Load saved report");
    await act(() => renderer.unmount());
    mounted = false;
    await act(() => finish());
    renderer = createRoot(container);
    mounted = true;
    await render();
    expect(container.textContent).not.toContain(REPORT_ID);
    expect(testState.read).toHaveBeenCalledTimes(1);
  });

  it("clears an in-flight read when the environment stops advertising the capability", async () => {
    const finish = deferredRead();
    await render();
    await click("Load saved report");
    testState.presentations = new Map(testState.presentations).set(
      primaryId,
      environment(primaryId, { capability: "omitted" }),
    );
    await render();
    await act(() => finish());
    expect(container.textContent).toBe("");
    testState.presentations = new Map(testState.presentations).set(
      primaryId,
      environment(primaryId),
    );
    await render();
    expect(container.textContent).not.toContain(REPORT_ID);
    expect(testState.read).toHaveBeenCalledTimes(1);
  });

  it("requires an explicit target with several capable non-primary environments", async () => {
    const first = EnvironmentId.make("first");
    const second = EnvironmentId.make("second");
    testState.presentations = new Map([
      [first, environment(first, { primary: false })],
      [second, environment(second, { primary: false })],
    ]);
    connect(first);
    connect(second);
    await render();
    await openPanel();
    expect(container.textContent).toContain("Choose an environment to load");
    expect(testState.read).not.toHaveBeenCalled();
    const trigger = container.querySelector<HTMLButtonElement>(
      '[aria-label="Saved report environment"]',
    )!;
    await act(() => trigger.click());
    const option = [...document.querySelectorAll<HTMLElement>('[role="option"]')].find(
      (item) => item.textContent?.trim() === "second",
    );
    expect(option).toBeDefined();
    await act(() => option!.click());
    expect(testState.read).not.toHaveBeenCalled();
    await click("Load saved report");
    expect(testState.read).toHaveBeenCalledExactlyOnceWith({ environmentId: second, input: {} });
  });

  it("renders partial sums, wholly unknown metrics and coverage without zero-filling", async () => {
    const saved = report();
    testState.read.mockResolvedValue(
      AsyncResult.success(
        ready({
          ...saved,
          provider_ledger: {
            ...saved.provider_ledger,
            metrics: {
              ...saved.provider_ledger.metrics,
              input_tokens: { total: null, known_sum: 42, known_requests: 1, missing_requests: 1 },
              reasoning_output_tokens: {
                total: null,
                known_sum: null,
                known_requests: 0,
                missing_requests: 2,
              },
            },
          },
        }),
      ),
    );
    await render();
    await click("Load saved report");
    const ledger = container.querySelector('[aria-label="Saved provider ledger"]')!;
    expect(metricText(ledger, "Input")).toBe(
      "Known: 42 · total unknown1 known · 1 missing requests",
    );
    expect(metricText(ledger, "Measured reasoning · included in output")).toBe(
      "Unknown0 known · 2 missing requests",
    );
    expect(container.textContent).toContain("Unknown residuals are not zero");
    expect(metricText(container, "Tracked files")).toBe("Unknown");
  });

  it("keeps excluded statuses, estimator failures and overlapping mechanism counts visible", async () => {
    await render();
    await click("Load saved report");
    expect(metricText(container, "Primary · additive")).toBe("2");
    expect(metricText(container, "Excluded · nonadditive")).toBe("3");
    expect(metricText(container, "Legacy unresolved · excluded")).toBe("1");
    expect(metricText(container, "Conflict · excluded")).toBe("1");
    expect(metricText(container, "Aggregate delta · excluded")).toBe("1");
    expect(metricText(container, "Qualified estimators")).toBe("1");
    expect(metricText(container, "Unavailable estimators")).toBe("1");
    expect(metricText(container, "Failed estimators")).toBe("1");
    expect(metricText(container, "New content")).toBe("2");
    expect(metricText(container, "Repeated history")).toBe("2");
    expect(metricText(container, "Compaction or restart")).toBe("Unknown");
    expect(container.textContent).toContain("counts overlap and must not be summed");
    expect(container.textContent).toContain("Provider and role are separate views");
    expect(container.querySelector('[aria-label="Input allocation"]')?.textContent).toContain(
      "Estimated · calibrated",
    );
    expect(container.querySelector('[aria-label="Output allocation"]')?.textContent).toContain(
      "Measured component",
    );
    expect(container.querySelectorAll("li")).toHaveLength(TOKEN_ACCOUNTING_CAVEATS.length);
  });

  it("preserves an all-indexed-history window and partial indexed selection", async () => {
    const saved = report();
    testState.read.mockResolvedValue(
      AsyncResult.success(
        ready({
          ...saved,
          window: { scope: "all_indexed_history", start: null, end: null, end_exclusive: false },
          provider_coverage: {
            ...saved.provider_coverage,
            history_scope: "all_indexed_history",
            indexed_history_selection: {
              requested_thread_count: 2,
              indexed_thread_count: 1,
              status: "partially_indexed",
            },
          },
        }),
      ),
    );
    await render();
    await click("Load saved report");
    expect(metricText(container, "Saved report window")).toBe("All indexed history");
    expect(metricText(container, "Requested threads")).toBe("2");
    expect(metricText(container, "Indexed threads")).toBe("1");
    expect(metricText(container, "Indexed history selection")).toBe("partially indexed");
    expect(container.textContent).toContain("Historical window completeness is not proven");
  });

  it.each([
    { status: "unconfigured", reason: "reader_unconfigured", text: "has not been configured" },
    { status: "missing", reason: "configured_report_missing", text: "is missing" },
    { status: "invalid", reason: "report_identity_mismatch", text: "identity check" },
    { status: "unsupported", reason: "report_schema_unsupported", text: "format is not supported" },
    { status: "oversized", reason: "projection_too_large", text: "response size limit" },
    { status: "reader_failed", reason: "reader_timeout", text: "did not finish in time" },
  ])(
    "renders the fixed $status unavailable state with read observation",
    async ({ status, reason, text }) => {
      const unavailable = Schema.decodeUnknownSync(TokenAccountingReadResult)({
        state: "unavailable",
        status,
        reason,
        readAt: READ_AT,
        configuredReportId: REPORT_ID,
      });
      testState.read.mockResolvedValue(AsyncResult.success(unavailable));
      await render();
      await click("Load saved report");
      expect(container.querySelector('[role="status"]')?.textContent).toContain(text);
      expect(metricText(container, "Configured report ID")).toBe(REPORT_ID);
      expect(container.querySelector("time")?.dateTime).toBe(READ_AT);
    },
  );

  it("shows a generic transport error without displaying an arbitrary exception", async () => {
    testState.read.mockRejectedValue(new Error("synthetic private exception text"));
    await render();
    await click("Load saved report");
    expect(container.querySelector('[role="alert"]')?.textContent).toBe(
      "The saved report could not be read. Try again when connected.",
    );
    expect(container.textContent).not.toContain("synthetic private exception text");
  });

  it("also shows a generic message for a settled transport failure", async () => {
    testState.read.mockResolvedValue(AsyncResult.fail(new Error("synthetic transport failure")));
    await render();
    await click("Load saved report");
    expect(container.querySelector('[role="alert"]')?.textContent).toBe(
      "The saved report could not be read. Try again when connected.",
    );
    expect(container.textContent).not.toContain("synthetic transport failure");
  });
});
