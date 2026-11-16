import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as RuntimeCensus from "../../runtime/RuntimeCensus.ts";
import { type ProjectId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as McpToolAccess from "../../../mcp/McpToolAccess.ts";
import { requireMcpCapability } from "../../../mcp/McpInvocationContext.ts";
import { CollectorFailure, DecisionSnapshotCollector, monotonicSeconds } from "./collector.ts";
import { DecisionSnapshot, DecisionSnapshotToolkit } from "./tools.ts";

const encodeNativePacket = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeSnapshotJson = Schema.decodeUnknownEffect(Schema.fromJsonString(DecisionSnapshot));
export class NativeCountReadError extends Schema.TaggedError<NativeCountReadError>()(
  "NativeCountReadError",
  { source: Schema.Literals(["threads", "workstreams"]) },
) {
  override get message(): string {
    return "Native decision snapshot counts are unavailable.";
  }
}

interface OperatingCounts {
  readonly total: number;
  readonly operating: number;
  readonly foregroundWaitingApproval: number;
  readonly foregroundWaitingInput: number;
  readonly foregroundWaitingPlan: number;
  readonly backgroundOperating: number;
  // Application counts also retain unknown external coverage, even when every application thread was sampled.
  readonly backgroundUnknown: number;
  readonly backgroundSampledAt: string;
  readonly foregroundUnknown?: number;
  readonly snapshotSequence?: number;
  readonly countScope?: "application";
  readonly applicationCensusCoverage?: "complete";
  readonly nativeBackgroundCoverage?: "unknown";
}
export class DecisionSnapshotNativeCounts extends Context.Service<
  DecisionSnapshotNativeCounts,
  {
    readonly readOperatingCounts?: (
      projectId?: ProjectId,
    ) => Effect.Effect<OperatingCounts, NativeCountReadError>;
  }
>()("t3/jones/mcp/decisionSnapshot/handlers/DecisionSnapshotNativeCounts") {}
export const DecisionSnapshotNativeCountsLive = Layer.effect(
  DecisionSnapshotNativeCounts,
  Effect.gen(function* () {
    const census = yield* RuntimeCensus.RuntimeCensus;
    return DecisionSnapshotNativeCounts.of({
      readOperatingCounts: Effect.fn("DecisionSnapshotNativeCounts.readOperatingCounts")(
        (projectId) =>
          census
            .readOperatingCounts(projectId)
            .pipe(Effect.mapError(() => new NativeCountReadError({ source: "threads" }))),
      ),
    });
  }),
).pipe(Layer.provide(RuntimeCensus.layer));

interface NativeCountEntry {
  readonly status: "observed" | "partial" | "unavailable" | "timeout";
  readonly observed_at: string | null;
  readonly timestamp_basis: "native_observation" | "unknown";
  readonly scope: Record<string, unknown>;
  readonly values: Record<string, unknown>;
  readonly reason: string | null;
}
const absentEntry = (
  reason: string,
  scope: Record<string, unknown>,
  status: "unavailable" | "timeout" = "unavailable",
) => ({
  status,
  observed_at: null,
  timestamp_basis: "unknown" as const,
  scope,
  values: {},
  reason,
});
const isoNow = DateTime.now.pipe(Effect.map(DateTime.formatIso));
export const DecisionSnapshotToolkitAccess = McpToolAccess.toLayer(
  DecisionSnapshotToolkit,
  Effect.gen(function* () {
    const countsPort = yield* DecisionSnapshotNativeCounts;
    const collector = yield* DecisionSnapshotCollector;
    return {
      decision_snapshot: McpToolAccess.reads(
        Effect.fn("DecisionSnapshotToolkit.collect")(function* ({
          purpose = "display",
          projectId,
        }) {
          const invocation = yield* requireMcpCapability("decision-snapshot");
          const deadlineMonotonic = monotonicSeconds() + 40;
          const threadScope = {
            environment_id: invocation.environmentId,
            project_id: projectId ?? null,
            include_archived: false,
          };
          const threads = Effect.suspend(() =>
            countsPort.readOperatingCounts === undefined
              ? Effect.succeed(absentEntry("native_operating_counts_unavailable", threadScope))
              : countsPort.readOperatingCounts(projectId).pipe(
                  Effect.map((counts) => ({
                    status:
                      counts.backgroundUnknown === 0 &&
                      counts.nativeBackgroundCoverage !== "unknown"
                        ? ("observed" as const)
                        : ("partial" as const),
                    observed_at: counts.backgroundSampledAt,
                    timestamp_basis: "native_observation" as const,
                    scope: {
                      ...threadScope,
                      ...(counts.countScope === undefined
                        ? {}
                        : {
                            count_scope: counts.countScope,
                            application_census_coverage: counts.applicationCensusCoverage,
                            native_background_coverage: counts.nativeBackgroundCoverage,
                            snapshot_sequence: counts.snapshotSequence,
                          }),
                    },
                    values: {
                      operating: counts.operating,
                      total: counts.total,
                      foreground_waiting_approval: counts.foregroundWaitingApproval,
                      foreground_waiting_input: counts.foregroundWaitingInput,
                      foreground_waiting_plan: counts.foregroundWaitingPlan,
                      background_operating: counts.backgroundOperating,
                      ...(counts.countScope === undefined
                        ? {}
                        : {
                            foreground_unknown: counts.foregroundUnknown,
                            background_unknown: counts.backgroundUnknown,
                          }),
                    },
                    reason:
                      counts.backgroundUnknown === 0 &&
                      counts.nativeBackgroundCoverage !== "unknown"
                        ? null
                        : "native_background_coverage_incomplete",
                  })),
                  Effect.orElseSucceed(() =>
                    absentEntry("native_projection_unavailable", threadScope),
                  ),
                  Effect.timeoutOrElse({
                    duration: 5_000,
                    orElse: () =>
                      Effect.succeed(
                        absentEntry("native_projection_timeout", threadScope, "timeout"),
                      ),
                  }),
                ),
          );
          const registryScope = {
            registry_id: null,
            owner_id: null,
            principal_id: null,
            server_generation: null,
            registry_version: null,
            authorization_revision: null,
          };
          // The bound v1 collector accepts only registry identity scope. Sending personal
          // environment scope would invalidate the entire packet, including unrelated sources.
          const registry = Effect.succeed(
            absentEntry("registry_counts_unavailable", registryScope),
          );
          const nativeCounts = yield* Effect.forkChild(
            Effect.all([threads, registry], { concurrency: 2 }),
          );
          const nativePacket = Fiber.join(nativeCounts).pipe(
            Effect.flatMap(([threadEntry, workstreamEntry]) =>
              encodeNativePacket({
                schema: "codex.decision-snapshot-native/v1",
                authority_effect: "none",
                provenance: "native_observed",
                sources: { threads: threadEntry, workstreams: workstreamEntry },
              }),
            ),
            Effect.mapError(() => new CollectorFailure("collector_failed")),
          );
          const output = yield* collector.collect(purpose, nativePacket, deadlineMonotonic).pipe(
            Effect.flatMap((text) =>
              decodeSnapshotJson(text, {
                onExcessProperty: "error",
              }),
            ),
            Effect.filterOrFail(
              (value) => value.purpose === purpose,
              () => new Error("purpose_mismatch"),
            ),
            Effect.map((value) => ({ value, reason: null })),
            Effect.catch((failure) =>
              Effect.succeed({
                value: null,
                reason:
                  failure instanceof CollectorFailure ? failure.reason : "invalid_collector_output",
              }),
            ),
          );
          const [threadEntry, workstreamEntry] = yield* Fiber.join(nativeCounts);
          const collected_at = yield* isoNow;
          const wrap = ({ timestamp_basis, ...entry }: NativeCountEntry, name: string) => {
            return {
              ...entry,
              received_at: collected_at,
              age_seconds:
                entry.observed_at === null
                  ? null
                  : Math.max(0, (Date.parse(collected_at) - Date.parse(entry.observed_at)) / 1_000),
              freshness:
                entry.observed_at === null ||
                Date.parse(entry.observed_at) > Date.parse(collected_at)
                  ? ("unknown" as const)
                  : Date.parse(collected_at) - Date.parse(entry.observed_at) <= 120_000
                    ? ("fresh" as const)
                    : ("stale" as const),
              provenance: {
                kind: "native_observed" as const,
                collector: `jones.${name}`,
                schema: "codex.decision-snapshot-native/v1",
                timestamp_basis,
                release_id: "source_tree",
              },
            };
          };
          if (output.value !== null)
            return {
              ...output.value,
              coverage:
                output.value.coverage === "complete" ? ("partial" as const) : output.value.coverage,
              sources: {
                ...output.value.sources,
                workstreams: wrap(workstreamEntry, "workstreams"),
              },
            };
          const unavailable = (name: string) => ({
            ...wrap(
              absentEntry(
                output.reason ?? "runtime_unavailable",
                name === "host" ? { same_host: true } : {},
              ),
              name,
            ),
            values:
              name === "host"
                ? {
                    cpu_used_percent: null,
                    ram_available_gib: null,
                    one_fully_used_core_percent: null,
                    agent_batch_ceiling: 0,
                    serial_test_process_ceiling: 0,
                    broad_test_allowed: false,
                    start_fenced: true,
                  }
                : output.reason === "cleanup_unknown"
                  ? { start_fenced: true }
                  : {},
          });
          return {
            schema: "codex.decision-snapshot/v1" as const,
            authority_effect: "none" as const,
            coverage:
              threadEntry.status === "observed" || threadEntry.status === "partial"
                ? ("partial" as const)
                : ("unavailable" as const),
            purpose,
            collected_at,
            sources: {
              host: unavailable("host"),
              queue: unavailable("queue"),
              workspaces: unavailable("workspaces"),
              account_malcolm: unavailable("account_malcolm"),
              account_jace: unavailable("account_jace"),
              threads: wrap(threadEntry, "threads"),
              workstreams: wrap(workstreamEntry, "workstreams"),
            },
          };
        }),
      ),
    };
  }),
);

export const DecisionSnapshotToolkitHandlersLive = McpToolAccess.HandlersLayer.layer(
  DecisionSnapshotToolkitAccess,
);
