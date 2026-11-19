import type {
  JonesInstallPlanCliRequest,
  JonesInstallPlanCliResponse,
} from "@t3tools/contracts/jones/installPlan";
import * as Effect from "effect/Effect";

import type { DesktopUpdates } from "../../updates/DesktopUpdatesService.ts";

type Updater = DesktopUpdates["Service"]["installPlan"];

export function installPlanCliFailure(
  request: JonesInstallPlanCliRequest,
  code: Extract<JonesInstallPlanCliResponse, { ok: false }>["code"],
  message: string,
): JonesInstallPlanCliResponse {
  return { version: 1, requestId: request.requestId, ok: false, code, message };
}

export const dispatchInstallPlanCli = Effect.fn("DesktopUpdates.dispatchInstallPlanCli")(function* (
  request: JonesInstallPlanCliRequest,
  installPlan: Updater,
) {
  if (!installPlan) {
    return installPlanCliFailure(
      request,
      "unsupported",
      "This desktop does not support native Jones Code install plans.",
    );
  }
  const read = yield* installPlan({ type: "read" });
  const { action } = request;
  if (action.type === "read") {
    return { version: 1, requestId: request.requestId, ok: true, result: read } as const;
  }
  if (read.state.laptop.environmentId !== action.environmentId) {
    return installPlanCliFailure(
      request,
      "environment-mismatch",
      "The running desktop does not match the requested environment ID.",
    );
  }
  // A late response can be lost. The coordinator still owns all subsequent CAS checks.
  const result =
    read.state.last?.planId === action.planId && read.state.last.cancelled === true
      ? read
      : yield* installPlan({
          type: "cancel-plan",
          planId: action.planId,
          expectedRevision: action.expectedRevision,
        });
  return { version: 1, requestId: request.requestId, ok: true, result } as const;
});
