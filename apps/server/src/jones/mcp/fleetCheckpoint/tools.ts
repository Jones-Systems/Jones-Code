import { OrchestratorMcpFailure } from "@t3tools/contracts";
import {
  FleetCheckpointInput,
  FleetCheckpointResult,
} from "@t3tools/contracts/jones/fleetCheckpoint";
import { Tool, Toolkit } from "effect/ai";
import { McpInvocationContext } from "../../../mcp/McpInvocationContext.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";
import { Checkpoint } from "../../fleetCheckpoint/Checkpoint.ts";

const make = <const Name extends string>(name: Name, description: string, readOnly: boolean) =>
  Tool.make(name, {
    description,
    parameters: FleetCheckpointInput,
    success: FleetCheckpointResult,
    failure: OrchestratorMcpFailure,
    failureMode: "return",
  })
    .addDependency(McpInvocationContext)
    .addDependency(Checkpoint)
    .addDependency(ThreadManagementService)
    .annotate(Tool.Strict, true)
    .annotate(Tool.Readonly, readOnly)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false);

export const FleetCheckpointToolkit = Toolkit.make(
  make(
    "fleet_checkpoint_place",
    "Place one committed host-tooling.catalog checkpoint through the configured Fleet controller. Reuse originalRequestId and the exact request after uncertainty; get reconciles that original work. No arbitrary commands or targets. timeoutMs is only a caller wait budget and never cancels accepted execution. Verification and resource closure are separate.",
    false,
  ),
  make(
    "fleet_checkpoint_get",
    "Read the original Fleet checkpoint assignment and attributed result for this native thread and originalRequestId. Supply the exact original frozen request. Retained or closed history cannot reconstruct missing verification evidence and never permits replacement work.",
    true,
  ),
  make(
    "fleet_checkpoint_cancel_pending",
    "Cancel the original checkpoint only while Fleet proves dispatch absent. Supply originalRequestId and its unchanged request. Timeout or later execution requires reconciliation of that same identity; no replacement assignment is created.",
    false,
  ),
);
