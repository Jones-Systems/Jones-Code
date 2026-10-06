import { useAtomValue } from "@effect/atom-react";
import type {
  EnvironmentId,
  TokenAccountingMetric,
  TokenAccountingReadResult,
  TokenAccountingReport,
} from "@t3tools/contracts";
import { AsyncResult } from "effect/unstable/reactivity";
import { useState, type ReactNode } from "react";

import { environmentCatalog } from "../../connection/catalog";
import { environmentPresentations } from "../../state/presentation";
import { serverEnvironment } from "../../state/server";
import {
  ACCOUNTING_TRANSPORT_ERROR,
  accountingUnavailableMessage,
  formatAccountingCount,
  formatAccountingMetric,
  selectAccountingEnvironment,
  useSavedTokenAccounting,
  type AccountingEnvironment,
} from "../../state/tokenAccounting";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../../components/ui/button";
import {
  Select,
  SelectItem,
  SelectPopup,
  SelectTrigger,
  SelectValue,
} from "../../components/ui/select";

const TIMESTAMP = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  year: "numeric",
  month: "short",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
  second: "2-digit",
  timeZoneName: "short",
});

const CAVEATS: Record<TokenAccountingReport["caveats"][number], string> = {
  primary_requests_only_are_additive:
    "Only primary requests are additive. Legacy unresolved, conflict and aggregate delta requests are excluded from additive totals.",
  missing_counters_leave_totals_unknown:
    "Missing counters leave complete totals unknown; a known sum covers only requests with a recorded counter.",
  visible_inventory_is_not_submitted_payload:
    "Visible inventory does not establish what was submitted to the provider.",
  user_role_does_not_prove_owner_speech:
    "A user-role message does not prove that the owner spoke it.",
  candidate_envelope_is_unproved_submission:
    "A reconstructed candidate envelope does not prove actual submission.",
  cache_placement_unsupported:
    "Allocation groups do not establish where cache reads or cache writes occurred.",
  unknown_residuals_are_not_zero: "Unknown residuals are not zero.",
  reasoning_is_an_output_subset:
    "Measured reasoning is included in output, rather than added to it.",
  mechanism_flags_are_nonadditive: "Mechanism flags overlap and must not be summed.",
  counter_changes_are_diagnostics_only:
    "Counter changes are diagnostics, not additional requests or token allocations.",
  bands_use_held_out_group_bias:
    "Allocation bands use held-out group bias; estimated allocations are not measured provider counters.",
  historical_window_completeness_not_proven: "Historical window completeness is not proven.",
  append_only_source_validation_limit:
    "Append-only source validation does not establish complete historical coverage.",
  partial_counter_masks_limit_aggregate_conservation:
    "Partial counter coverage limits aggregate conservation claims.",
};

const FRESHNESS: Record<TokenAccountingReport["provider_coverage"]["freshness"]["status"], string> =
  {
    scan_unavailable: "Scan information unavailable",
    archive_snapshot_only: "Archive snapshot only",
    validation_receipts_unavailable: "Source validation receipts unavailable",
    no_selected_source_validated: "No selected source validated",
    requested_end_unspecified: "Requested end unspecified",
    requested_end_after_selected_validation: "Requested end is after selected source validation",
    retained_sources_not_refreshed: "Retained sources were not refreshed",
    selected_sources_validated_after_requested_end:
      "Selected sources validated after requested end",
  };

const MECHANISMS: Record<keyof TokenAccountingReport["mechanism_flags"]["counts"], string> = {
  new_content: "New content",
  repeated_history: "Repeated history",
  changed_guidance_schema_prefix: "Changed guidance or schema prefix",
  compaction_restart: "Compaction or restart",
  first_observation_or_missing_predecessor: "First observation or missing predecessor",
  cache_miss_unchanged_reconstruction: "Cache miss with unchanged reconstruction",
  mixed_unknown: "Mixed or unknown",
};

export function SavedTokenAccounting() {
  const presentations = useAtomValue(environmentPresentations.presentationsAtom);
  const [selected, setSelected] = useState<EnvironmentId | null>(null);
  const environments: AccountingEnvironment[] = [...presentations].map(
    ([environmentId, presentation]) => ({
      environmentId,
      label: presentation.entry.target.label,
      primary: presentation.entry.target._tag === "PrimaryConnectionTarget",
      connected: presentation.connection.phase === "connected",
      supported:
        presentation.entry.enabled !== false &&
        presentation.serverConfig?.environment?.capabilities.savedTokenAccounting === true,
    }),
  );
  const supported = environments.filter((environment) => environment.supported);
  if (supported.length === 0) return null;
  const eligible = supported.filter((environment) => environment.connected);
  const target = selectAccountingEnvironment(environments, selected);

  return (
    <details className="mt-6 min-w-0 rounded-lg border border-border/60 p-4">
      <summary className="cursor-pointer text-sm font-medium text-foreground">
        Saved token accounting
      </summary>
      <div className="mt-4 flex min-w-0 flex-col gap-4">
        <p className="text-xs text-muted-foreground">
          Read one saved report from one environment. Its window is independent of Usage date,
          provider and environment filters and may differ from the activity above. It is not a
          billing total.
        </p>
        {eligible.length > 1 ? (
          <div className="max-w-sm">
            <Select
              value={target?.environmentId ?? null}
              onValueChange={(value) =>
                setSelected(
                  eligible.find((environment) => environment.environmentId === value)
                    ?.environmentId ?? null,
                )
              }
              items={eligible.map((environment) => ({
                value: environment.environmentId,
                label: environment.label,
              }))}
            >
              <SelectTrigger size="sm" aria-label="Saved report environment">
                <SelectValue placeholder="Choose an environment" />
              </SelectTrigger>
              <SelectPopup>
                {eligible.map((environment) => (
                  <SelectItem key={environment.environmentId} value={environment.environmentId}>
                    {environment.label}
                  </SelectItem>
                ))}
              </SelectPopup>
            </Select>
          </div>
        ) : null}
        {target === null ? (
          <p role="status" className="text-sm text-muted-foreground">
            {eligible.length === 0
              ? "Connect an environment with a saved accounting reader to load its report."
              : "Choose an environment to load its saved report."}
          </p>
        ) : (
          <AccountingReader key={target.environmentId} environment={target} />
        )}
      </div>
    </details>
  );
}

function AccountingReader({ environment }: { readonly environment: AccountingEnvironment }) {
  const connection = useAtomValue(environmentCatalog.stateAtom(environment.environmentId));
  const read = useAtomCommand(serverEnvironment.readTokenAccounting, { reportFailure: false });
  const target =
    environment.connected &&
    AsyncResult.isSuccess(connection) &&
    connection.value.phase === "connected"
      ? { environmentId: environment.environmentId, generation: connection.value.generation }
      : null;
  const { state, load } = useSavedTokenAccounting(target, read);
  const [attempted, setAttempted] = useState(false);

  return (
    <div className="flex min-w-0 flex-col gap-4">
      <div className="flex flex-wrap items-center gap-3">
        <span className="text-xs text-muted-foreground">{environment.label}</span>
        <Button
          size="sm"
          variant="outline"
          disabled={target === null || state.phase === "reading"}
          aria-busy={state.phase === "reading"}
          onClick={() => {
            setAttempted(true);
            void load();
          }}
        >
          {state.phase === "reading"
            ? "Reading saved report…"
            : attempted
              ? "Read again"
              : "Load saved report"}
        </Button>
      </div>
      {target === null ? (
        <p role="status" className="text-sm text-muted-foreground">
          The environment is disconnected. Connect again, then load its saved report.
        </p>
      ) : state.phase === "error" ? (
        <p role="alert" className="text-sm text-muted-foreground">
          {ACCOUNTING_TRANSPORT_ERROR}
        </p>
      ) : state.phase === "observed" ? (
        <AccountingObservation result={state.result} />
      ) : null}
    </div>
  );
}

function AccountingObservation({ result }: { readonly result: TokenAccountingReadResult }) {
  if (result.state === "unavailable")
    return (
      <div className="flex min-w-0 flex-col gap-2">
        <p role="status" className="text-sm text-muted-foreground">
          {accountingUnavailableMessage(result)}
        </p>
        <dl className="flex flex-col gap-2 text-xs">
          <TextMetric label="Read observation" value={<Timestamp value={result.readAt} />} />
          {result.configuredReportId === null ? null : (
            <TextMetric label="Configured report ID" value={result.configuredReportId} />
          )}
        </dl>
      </div>
    );
  return <AccountingReport report={result.report} readAt={result.readAt} />;
}

function AccountingReport({
  report,
  readAt,
}: {
  readonly report: TokenAccountingReport;
  readonly readAt: string;
}) {
  const ledger = report.provider_ledger;
  const coverage = report.provider_coverage;
  return (
    <div className="flex min-w-0 flex-col gap-5">
      <dl className="grid min-w-0 gap-x-6 gap-y-3 sm:grid-cols-2">
        <TextMetric label="Report ID" value={report.report_id} />
        <TextMetric label="Read observation" value={<Timestamp value={readAt} />} />
        <TextMetric
          label="Saved report window"
          value={
            report.window.start === null ? (
              "All indexed history"
            ) : (
              <>
                <Timestamp value={report.window.start} /> – <Timestamp value={report.window.end} />{" "}
                (end exclusive)
              </>
            )
          }
        />
        <TextMetric label="Index snapshot ID" value={report.snapshot.index_snapshot_id} />
      </dl>
      <p className="text-xs text-muted-foreground">
        Only primary requests are additive. Unknown residuals are not zero. Historical window
        completeness is not proven.
      </p>
      <section className="flex min-w-0 flex-col gap-3" aria-label="Saved provider ledger">
        <h3 className="text-sm font-medium">Provider ledger</h3>
        <dl className="grid grid-cols-2 gap-x-6 gap-y-3 sm:grid-cols-3">
          <CountMetric label="Recorded requests" value={ledger.requests} />
          <CountMetric label="Primary · additive" value={ledger.primary_requests} />
          <CountMetric label="Excluded · nonadditive" value={ledger.nonadditive_requests} />
          <CountMetric
            label="Legacy unresolved · excluded"
            value={ledger.accounting_status_counts.legacy_unresolved}
          />
          <CountMetric
            label="Conflict · excluded"
            value={ledger.accounting_status_counts.conflict}
          />
          <CountMetric
            label="Aggregate delta · excluded"
            value={ledger.accounting_status_counts.aggregate_delta}
          />
        </dl>
        <dl className="grid grid-cols-2 gap-x-6 gap-y-3 sm:grid-cols-3">
          <Metric label="Input" metric={ledger.metrics.input_tokens} />
          <Metric label="Cached input · cache reads" metric={ledger.metrics.cached_input_tokens} />
          <Metric label="Cache write input" metric={ledger.metrics.cache_write_input_tokens} />
          <Metric label="Cache writes · 5 minutes" metric={ledger.metrics.cache_write_5m_tokens} />
          <Metric label="Cache writes · 1 hour" metric={ledger.metrics.cache_write_1h_tokens} />
          <Metric label="Ordinary input" metric={ledger.metrics.ordinary_input_tokens} />
          <Metric label="Non-read input" metric={ledger.metrics.non_read_input_tokens} />
          <Metric label="Output" metric={ledger.metrics.output_tokens} />
          <Metric
            label="Measured reasoning · included in output"
            metric={ledger.metrics.reasoning_output_tokens}
          />
        </dl>
        <p className="text-xs text-muted-foreground">
          Cache reads and cache writes remain separate recorded counters. Allocation groups do not
          establish cache placement. Reasoning is a subset of output.
        </p>
      </section>
      <div className="grid min-w-0 gap-5 xl:grid-cols-2">
        <Allocation
          title="Input allocation"
          totalLabel="Ordinary input"
          total={report.input.ordinary_input}
          allocation={report.input}
        />
        <Allocation
          title="Output allocation"
          totalLabel="Output"
          total={report.output.output}
          allocation={report.output}
          reasoning={report.output.measured_reasoning}
        />
      </div>
      <section className="flex flex-col gap-3" aria-label="Saved estimator coverage">
        <h3 className="text-sm font-medium">Capture and estimator coverage</h3>
        <dl className="grid grid-cols-2 gap-x-6 gap-y-3 sm:grid-cols-3">
          <CountMetric label="Captured requests" value={report.coverage.captured_requests} />
          <CountMetric label="Unavailable requests" value={report.coverage.unavailable_requests} />
          <CountMetric
            label="Complete responses"
            value={report.coverage.complete_response_requests}
          />
          <CountMetric
            label="Input allocated requests"
            value={report.coverage.input_allocated_requests}
          />
          <CountMetric
            label="Output allocated requests"
            value={report.coverage.output_allocated_requests}
          />
          <CountMetric
            label="Reasoning measured requests"
            value={report.coverage.reasoning_measured_requests}
          />
          <CountMetric
            label="Reasoning missing requests"
            value={report.coverage.reasoning_missing_requests}
          />
          <CountMetric
            label="Qualified estimators"
            value={report.estimator_status_counts.qualified}
          />
          <CountMetric
            label="Unavailable estimators"
            value={report.estimator_status_counts.unavailable}
          />
          <CountMetric label="Failed estimators" value={report.estimator_status_counts.failed} />
        </dl>
      </section>
      <div className="grid min-w-0 gap-5 xl:grid-cols-2">
        <Partitions
          title="By provider"
          partitions={report.partitions.provider}
          labels={{ codex: "Codex", claude: "Claude Code" }}
        />
        <Partitions
          title="By role"
          partitions={report.partitions.role}
          labels={{ root: "Root", child: "Child", unknown: "Unknown role" }}
        />
      </div>
      <p className="text-xs text-muted-foreground">
        Provider and role are separate views of the same primary requests. Do not add these
        partitions together.
      </p>
      <section className="flex flex-col gap-3" aria-label="Saved nonadditive mechanisms">
        <h3 className="text-sm font-medium">Mechanisms · overlapping, nonadditive</h3>
        <dl className="grid grid-cols-2 gap-x-6 gap-y-3 sm:grid-cols-3">
          {Object.entries(MECHANISMS).map(([key, label]) => (
            <TextMetric
              key={key}
              label={label}
              value={nullableCount(report.mechanism_flags.counts[key as keyof typeof MECHANISMS])}
            />
          ))}
        </dl>
        <p className="text-xs text-muted-foreground">
          A request can have several mechanism flags. These counts overlap and must not be summed.
        </p>
      </section>
      <details className="min-w-0 rounded-lg border border-border/60 p-3">
        <summary className="cursor-pointer text-xs font-medium">
          Source coverage, snapshot and caveats
        </summary>
        <div className="mt-3 flex min-w-0 flex-col gap-4">
          <dl className="grid min-w-0 gap-x-6 gap-y-3 sm:grid-cols-2">
            <TextMetric label="Selection ID" value={report.selection_id} />
            <TextMetric label="Source freshness" value={FRESHNESS[coverage.freshness.status]} />
            <TextMetric
              label="Snapshot captured"
              value={
                report.snapshot.captured_at === undefined ? (
                  "Unknown"
                ) : (
                  <Timestamp value={report.snapshot.captured_at} />
                )
              }
            />
            <TextMetric
              label="Source validation"
              value={
                report.snapshot.source_validation === undefined
                  ? "Unknown"
                  : report.snapshot.source_validation === "archive_snapshot_only"
                    ? "Archive snapshot only"
                    : "Append-only stat and anchor validation"
              }
            />
            <TextMetric
              label="Source revision"
              value={report.snapshot.source_revision ?? "Unknown"}
            />
            <TextMetric
              label="Mapping revision"
              value={report.snapshot.mapping_revision ?? "Unknown"}
            />
            <TextMetric label="Scan ID" value={report.snapshot.scan_id ?? "Unknown"} />
            <TextMetric
              label="Last scan captured"
              value={
                coverage.latest_scan.captured_at === null ? (
                  "Unknown"
                ) : (
                  <Timestamp value={coverage.latest_scan.captured_at} />
                )
              }
            />
            <TextMetric
              label="Selected files"
              value={nullableCount(coverage.latest_scan.selected_files)}
            />
            <TextMetric
              label="Refreshed selected files"
              value={nullableCount(coverage.latest_scan.refreshed_selected_files)}
            />
            <TextMetric
              label="Partial selected files"
              value={nullableCount(coverage.latest_scan.partial_selected_files)}
            />
            <TextMetric
              label="Validated files"
              value={nullableCount(coverage.latest_scan.validated_files)}
            />
            <TextMetric
              label="Tracked files"
              value={nullableCount(coverage.cumulative_index.tracked_files)}
            />
            <TextMetric
              label="Retained files not refreshed"
              value={nullableCount(coverage.cumulative_index.retained_not_refreshed_files)}
            />
            <TextMetric
              label="Missing tracked files"
              value={nullableCount(coverage.cumulative_index.missing_tracked_files)}
            />
            <TextMetric
              label="Earliest selected validation"
              value={
                coverage.freshness.selected_validation_min === null ? (
                  "Unknown"
                ) : (
                  <Timestamp value={coverage.freshness.selected_validation_min} />
                )
              }
            />
            <TextMetric
              label="Latest selected validation"
              value={
                coverage.freshness.selected_validation_max === null ? (
                  "Unknown"
                ) : (
                  <Timestamp value={coverage.freshness.selected_validation_max} />
                )
              }
            />
            {coverage.indexed_history_selection === undefined ? null : (
              <>
                <CountMetric
                  label="Requested threads"
                  value={coverage.indexed_history_selection.requested_thread_count}
                />
                <CountMetric
                  label="Indexed threads"
                  value={coverage.indexed_history_selection.indexed_thread_count}
                />
                <TextMetric
                  label="Indexed history selection"
                  value={coverage.indexed_history_selection.status.replaceAll("_", " ")}
                />
              </>
            )}
          </dl>
          <ul className="list-disc space-y-2 pl-4 text-xs text-muted-foreground">
            {report.caveats.map((caveat) => (
              <li key={caveat}>{CAVEATS[caveat]}</li>
            ))}
          </ul>
        </div>
      </details>
    </div>
  );
}

type AllocationData = TokenAccountingReport["input"] | TokenAccountingReport["output"];

function Allocation({
  title,
  totalLabel,
  total,
  allocation,
  reasoning,
}: {
  readonly title: string;
  readonly totalLabel: string;
  readonly total: TokenAccountingMetric;
  readonly allocation: AllocationData;
  readonly reasoning?: TokenAccountingMetric;
}) {
  return (
    <section className="flex min-w-0 flex-col gap-3" aria-label={title}>
      <h3 className="text-sm font-medium">{title}</h3>
      <dl className="grid grid-cols-2 gap-x-6 gap-y-3">
        <Metric label={totalLabel} metric={total} />
        <Metric label="Allocated" metric={allocation.allocated} />
        <Metric label="Unknown residual" metric={allocation.unknown} />
        {reasoning === undefined ? null : (
          <Metric label="Measured reasoning · output subset" metric={reasoning} />
        )}
        <TextMetric
          label="Estimated coverage"
          value={
            allocation.estimated_coverage === null
              ? "Unknown"
              : `${(allocation.estimated_coverage * 100).toFixed(1)}%`
          }
        />
      </dl>
      <div className="min-w-0 overflow-x-auto">
        <table className="w-full text-xs">
          <caption className="sr-only">
            {title} groups with basis and central, low and high bands
          </caption>
          <thead>
            <tr className="border-b border-border text-left text-muted-foreground">
              {["Group", "Basis", "Central", "Low", "High"].map((label) => (
                <th key={label} className="whitespace-nowrap px-2 py-2 font-normal first:pl-0">
                  {label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {allocation.groups.length === 0 ? (
              <tr>
                <td colSpan={5} className="py-3 text-muted-foreground">
                  No allocation groups recorded.
                </td>
              </tr>
            ) : (
              allocation.groups.map((group) => (
                <tr
                  key={`${group.group}:${group.subtype}:${group.basis}`}
                  className="border-b border-border/50"
                >
                  <td className="py-2 pr-2">
                    {readable(group.group)} · {readable(group.subtype)}
                  </td>
                  <td className="whitespace-nowrap px-2 py-2 text-muted-foreground">
                    {group.basis === "measured_component"
                      ? "Measured component"
                      : "Estimated · calibrated"}
                  </td>
                  {[group.central, group.low, group.high].map((value, index) => (
                    <td key={index} className="px-2 py-2 text-right tabular-nums">
                      {formatAccountingCount(value)}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
      <p className="text-xs text-muted-foreground">
        Estimated bands are separate from measured provider counters.
      </p>
      {Object.keys(allocation.unknown_by_reason).length === 0 ? null : (
        <dl className="grid grid-cols-2 gap-x-6 gap-y-3">
          {Object.entries(allocation.unknown_by_reason).map(([reason, metric]) => (
            <Metric key={reason} label={`Unknown · ${readable(reason)}`} metric={metric} />
          ))}
        </dl>
      )}
    </section>
  );
}

function Partitions<Key extends string>({
  title,
  partitions,
  labels,
}: {
  readonly title: string;
  readonly partitions: Record<Key, TokenAccountingReport["partitions"]["provider"]["codex"]>;
  readonly labels: Record<Key, string>;
}) {
  return (
    <section className="flex min-w-0 flex-col gap-3" aria-label={title}>
      <h3 className="text-sm font-medium">{title}</h3>
      <div className="min-w-0 overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-border text-left text-muted-foreground">
              {[title, "Requests", "Input", "Output", "Reasoning · output subset"].map((label) => (
                <th key={label} className="whitespace-nowrap px-2 py-2 font-normal first:pl-0">
                  {label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {(Object.keys(labels) as Key[]).map((key) => (
              <tr key={key} className="border-b border-border/50">
                <th scope="row" className="py-2 pr-2 text-left font-normal">
                  {labels[key]}
                </th>
                <td className="px-2 py-2 text-right tabular-nums">
                  {formatAccountingCount(partitions[key].requests)}
                </td>
                {[
                  partitions[key].metrics.input_tokens,
                  partitions[key].metrics.output_tokens,
                  partitions[key].metrics.reasoning_output_tokens,
                ].map((metric, index) => (
                  <td key={index} className="min-w-28 px-2 py-2 text-right">
                    <MetricValue metric={metric} />
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function MetricValue({ metric }: { readonly metric: TokenAccountingMetric }) {
  return (
    <>
      <span className="tabular-nums">{formatAccountingMetric(metric)}</span>
      <span className="mt-0.5 block text-xs text-muted-foreground">
        {formatAccountingCount(metric.known_requests)} known ·{" "}
        {formatAccountingCount(metric.missing_requests)} missing requests
      </span>
    </>
  );
}

function Metric({
  label,
  metric,
}: {
  readonly label: string;
  readonly metric: TokenAccountingMetric;
}) {
  return <TextMetric label={label} value={<MetricValue metric={metric} />} />;
}

function CountMetric({ label, value }: { readonly label: string; readonly value: number }) {
  return <TextMetric label={label} value={formatAccountingCount(value)} />;
}

function TextMetric({ label, value }: { readonly label: string; readonly value: ReactNode }) {
  return (
    <div className="flex min-w-0 flex-col gap-0.5">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="break-words text-sm text-foreground tabular-nums [overflow-wrap:anywhere]">
        {value}
      </dd>
    </div>
  );
}

function Timestamp({ value }: { readonly value: string }) {
  return <time dateTime={value}>{TIMESTAMP.format(new Date(value))}</time>;
}

function nullableCount(value: number | null | undefined): string {
  return value === null || value === undefined ? "Unknown" : formatAccountingCount(value);
}

function readable(value: string): string {
  return value.replaceAll("_", " ");
}
