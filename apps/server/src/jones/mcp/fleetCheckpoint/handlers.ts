import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as McpToolAccess from "../../../mcp/McpToolAccess.ts";
import { requireMcpCapability, requireThreadScope } from "../../../mcp/McpInvocationContext.ts";
import { assertNotLinked } from "../../../mcp/linkOrigin.ts";
import { assertFullAccess, readCaller } from "../../../mcp/threadAccess.ts";
import { Checkpoint, type Action } from "../../fleetCheckpoint/Checkpoint.ts";
import { FleetCheckpointToolkit } from "./tools.ts";

const handle = (action: Action) => (input: unknown) =>
  Effect.gen(function* () {
    const scope = yield* requireMcpCapability("orchestration").pipe(
      Effect.mapError(
        () =>
          new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "Fleet checkpoints require orchestration capability.",
          }),
      ),
    );
    const native = yield* requireThreadScope(scope, "Fleet checkpoints");
    if (action !== "get") {
      const caller = yield* readCaller();
      yield* assertNotLinked(caller, "submit this native thread's Fleet checkpoint");
      yield* assertFullAccess(
        caller,
        "Fleet checkpoint submission requires full-access/default mode.",
      );
    }
    const service = yield* Checkpoint;
    return yield* service
      .execute(
        action,
        { environmentId: scope.environmentId, threadId: native.thread.threadId },
        input,
      )
      .pipe(
        Effect.mapError(
          (error) =>
            new OrchestratorMcpFailure({
              code: "invalid_request",
              message: `Fleet checkpoint: ${error.reason}. Reconcile the original request identity; do not create replacement work.`,
            }),
        ),
      );
  });

export const FleetCheckpointHandlersLive = McpToolAccess.toLayer(FleetCheckpointToolkit, {
  fleet_checkpoint_place: McpToolAccess.actsAsCaller(handle("place")),
  fleet_checkpoint_get: McpToolAccess.readsAsCaller(handle("get")),
  fleet_checkpoint_cancel_pending: McpToolAccess.actsAsCaller(handle("cancel-pending")),
});
