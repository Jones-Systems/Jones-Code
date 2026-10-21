import { OrchestratorMcpFailure, settlementRestoreMcpCommandId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as McpToolAccess from "../../../mcp/McpToolAccess.ts";
import { readCaller } from "../../../mcp/threadAccess.ts";
import * as SettlementRestore from "../../settlementRestore/SettlementRestore.ts";
import { SettlementRestoreToolkit } from "./tools.ts";

const unavailable = () =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message: "The settlement restore could not be completed.",
  });

// Inspect shares restore's write access: one tool cannot declare two access levels.
export const SettlementRestoreHandlersLive = McpToolAccess.toLayer(SettlementRestoreToolkit, {
  t3_thread_settlement_restore: McpToolAccess.writesThreads(
    (input) => [input.threadId],
    (input) =>
      Effect.gen(function* () {
        yield* readCaller();
        const service = yield* SettlementRestore.SettlementRestore;
        if (input.action === "inspect") {
          const { environmentId, items } = yield* service
            .inspect({
              items: [
                {
                  threadId: input.threadId,
                  ...(input.settleCommandId === undefined
                    ? {}
                    : { settleCommandId: input.settleCommandId }),
                },
              ],
            })
            .pipe(Effect.mapError(unavailable));
          return { action: "inspect" as const, environmentId, ...items[0]! };
        }
        if (input.settleCommandId === undefined || input.clientRequestId === undefined)
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "restore requires settleCommandId and clientRequestId.",
          });
        const result = yield* service
          .restore({
            commandId: settlementRestoreMcpCommandId(input.clientRequestId),
            threadId: input.threadId,
            settleCommandId: input.settleCommandId,
          })
          .pipe(
            Effect.mapError((error) =>
              error._tag === "SettlementRestorePreviouslyRejectedError"
                ? new OrchestratorMcpFailure({ code: "invalid_request", message: error.message })
                : unavailable(),
            ),
          );
        return { action: "restore" as const, ...result };
      }),
  ),
});
