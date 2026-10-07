import { McpCapabilityUnavailableError, ProjectId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";
import { McpInvocationContext } from "../../../mcp/McpInvocationContext.ts";
import { DecisionSnapshotCollector } from "./collector.ts";

export const SnapshotPurpose = Schema.Literals(["admission", "display"]);
const source = Schema.Struct({
  status: Schema.Literals(["observed", "partial", "stale", "unavailable", "timeout"]),
  observed_at: Schema.NullOr(Schema.String),
  received_at: Schema.String,
  age_seconds: Schema.NullOr(Schema.Number),
  freshness: Schema.Literals(["fresh", "display_only", "stale", "unknown"]),
  provenance: Schema.Struct({
    kind: Schema.Literals(["local_collector", "native_observed", "caller_supplied", "fixture"]),
    collector: Schema.String,
    schema: Schema.NullOr(Schema.String),
    timestamp_basis: Schema.Literals([
      "native_observation",
      "local_receipt",
      "local_receipt_upstream_lag_unknown",
      "unknown",
    ]),
    release_id: Schema.String,
    collection_basis: Schema.optional(Schema.Literals(["sdk", "fresh-cache", "unknown"])),
  }),
  scope: Schema.Record(Schema.String, Schema.Unknown),
  values: Schema.Record(Schema.String, Schema.Unknown),
  reason: Schema.NullOr(Schema.String),
});
export const DecisionSnapshot = Schema.Struct({
  schema: Schema.Literal("codex.decision-snapshot/v1"),
  authority_effect: Schema.Literal("none"),
  coverage: Schema.Literals(["complete", "partial", "unavailable"]),
  purpose: SnapshotPurpose,
  collected_at: Schema.String,
  sources: Schema.Struct({
    host: source,
    queue: source,
    workspaces: source,
    account_malcolm: source,
    account_jace: source,
    threads: source,
    workstreams: source,
  }),
});
export type DecisionSnapshot = typeof DecisionSnapshot.Type;
const DecisionSnapshotTool = Tool.make("decision_snapshot", {
  description:
    "Collect one bounded decision snapshot using the explicitly bound collector release and native thread/registry counts. Partial thread counts are lower bounds when native background coverage is incomplete. Missing runtime or source capabilities remain unavailable; display facts do not grant admission authority.",
  parameters: Schema.Struct({
    purpose: Schema.optional(SnapshotPurpose),
    projectId: Schema.optional(ProjectId),
  }),
  success: DecisionSnapshot,
  failure: McpCapabilityUnavailableError,
  dependencies: [McpInvocationContext, DecisionSnapshotCollector],
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);
export const DecisionSnapshotToolkit = Toolkit.make(DecisionSnapshotTool);
