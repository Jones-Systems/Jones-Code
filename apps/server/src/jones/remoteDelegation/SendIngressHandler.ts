import * as Effect from "effect/Effect";
import * as McpToolAccess from "../../mcp/McpToolAccess.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import * as Service from "../../mcp/OrchestratorMcpService.ts";
import type { ReceiptLookupInput } from "./SendIngress.ts";

/** Read-only declaration; root registers it with F's shared receipt schema.
 * Namespace and origin authority come only from the authenticated invocation.
 */
export const lookupSendReceiptHandler = McpToolAccess.reads((input: ReceiptLookupInput) => Effect.gen(function* () {
  const scope = yield* Invocation.McpInvocationContext;
  const service = yield* Service.OrchestratorMcpService;
  return yield* service.lookupSendReceipt(scope, input);
}));
