import * as Schema from "effect/Schema";
import { RunId, ThreadId } from "../baseSchemas.ts";
import { OrchestrationV2RunStatus } from "../orchestrationV2.ts";
import { OrchestratorMcpClientRequestId } from "../orchestratorMcp.ts";

export const ClientLaunchDigest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
export const InitialMessageState = Schema.Literals([
  "not_requested",
  "accepted",
  "rejected",
  "pending",
  "unknown",
]);
export const PreparationState = Schema.Literals([
  "not_applicable",
  "left_preparation",
  "in_progress",
  "unscheduled",
  "unknown_effect",
]);
export const ClientLaunchKey = Schema.Struct({ clientRequestId: OrchestratorMcpClientRequestId });
export const ClientLaunchLookup = Schema.Struct({
  clientRequestId: OrchestratorMcpClientRequestId,
  state: Schema.Literals(["launched", "no_effect", "absent"]),
  basis: Schema.Literals(["receipt", "fence", "rejection", "none"]),
  threadId: Schema.NullOr(ThreadId),
  requestDigest: Schema.NullOr(ClientLaunchDigest),
  claimed: Schema.Boolean,
  threadDeleted: Schema.Boolean,
  initialMessage: InitialMessageState,
  runId: Schema.NullOr(RunId),
  runStatus: Schema.NullOr(OrchestrationV2RunStatus),
  preparation: PreparationState,
  workspace: Schema.NullOr(
    Schema.Struct({
      type: Schema.Literals(["root", "worktree", "existing_worktree"]),
      branch: Schema.NullOr(Schema.String),
      worktreePath: Schema.NullOr(Schema.String),
    }),
  ),
  reason: Schema.NullOr(Schema.String),
});
export type ClientLaunchLookup = typeof ClientLaunchLookup.Type;
export const ClientLaunchFence = Schema.Struct({
  ...ClientLaunchLookup.fields,
  state: Schema.Literals(["launched", "no_effect"]),
});
