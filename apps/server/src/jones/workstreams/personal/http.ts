import { EnvironmentHttpApi, EnvironmentHttpConflictError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

// Exact legacy endpoints remain declared for old clients; no prefix interception.
export const layer = HttpApiBuilder.group(EnvironmentHttpApi, "workstreams", (handlers) => {
  const unavailable = () =>
    Effect.fail(new EnvironmentHttpConflictError({ message: "workstreams_not_configured" }));
  return handlers
    .handle("list", unavailable)
    .handle("detail", unavailable)
    .handle("references", unavailable)
    .handle("reference", unavailable)
    .handle("memberships", unavailable)
    .handle("declarations", unavailable)
    .handle("edges", unavailable)
    .handle("history", unavailable)
    .handle("command", unavailable)
    .handle("submit", unavailable)
    .handle("threadPlacements", unavailable)
    .handle("nativeRead", unavailable)
    .handle("nativeExport", unavailable)
    .handle("legacyReceiptLookup", unavailable)
    .handle("appearanceRead", unavailable)
    .handle("appearanceSave", unavailable)
    .handle("registrationContext", unavailable)
    .handle("operationSubmit", unavailable)
    .handle("operationRead", unavailable)
    .handle("setupStatus", unavailable);
});
