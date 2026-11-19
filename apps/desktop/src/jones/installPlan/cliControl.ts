import {
  JONES_INSTALL_PLAN_CLI_RESPONSE_BYTES,
  confirmsInstallPlanCancellation,
  findInstallPlanResultPlan,
  type JonesInstallPlanCliRequest,
  type JonesInstallPlanCliResponse,
  type JonesInstallPlanResult,
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

function boundedReply(
  request: JonesInstallPlanCliRequest,
  result: JonesInstallPlanResult,
): JonesInstallPlanCliResponse {
  const { historyPlan, ...rest } = result;
  const response = {
    version: 1,
    requestId: request.requestId,
    ok: true,
    result:
      historyPlan === undefined || historyPlan.planId === result.state.last?.planId ? rest : result,
  } as const;
  const fits = (value: JonesInstallPlanCliResponse) =>
    Buffer.byteLength(JSON.stringify(value)) + 1 <= JONES_INSTALL_PLAN_CLI_RESPONSE_BYTES;
  if (fits(response)) return response;

  const { planId } = request.action;
  const detail = planId === undefined ? undefined : findInstallPlanResultPlan(result, planId);
  if (detail !== undefined) {
    const { schema, laptop, documentRevision, readOnly } = result.state;
    const projected = {
      ...response,
      projection: "plan-detail",
      result: {
        state: {
          schema,
          laptop,
          current: null,
          last: null,
          view: { complete: false },
          ...(documentRevision === undefined ? {} : { documentRevision }),
          ...(readOnly === undefined ? {} : { readOnly }),
        },
        historyPlan: detail,
        ...(result.refused === undefined ? {} : { refused: result.refused }),
      },
    } as const;
    if (fits(projected)) return projected;
  }
  return installPlanCliFailure(
    request,
    "unavailable",
    planId === undefined
      ? "The plan snapshot exceeds the CLI response bound. Read one exact plan ID."
      : "The exact plan exceeds the CLI response bound. Read it in Install status.",
  );
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
  const readExact = (result: JonesInstallPlanResult, planId: string) =>
    findInstallPlanResultPlan(result, planId) !== undefined ||
    result.state.view.recoveryHistory !== 1
      ? Effect.succeed(result)
      : installPlan({ type: "read-history", planId });
  if (action.type === "read") {
    const result = action.planId === undefined ? read : yield* readExact(read, action.planId);
    return boundedReply(request, result);
  }
  const environmentMatches = (result: JonesInstallPlanResult) => {
    const plan = findInstallPlanResultPlan(result, action.planId);
    return (
      result.state.laptop.environmentId === action.environmentId &&
      (plan === undefined || plan.laptop.environmentId === action.environmentId)
    );
  };
  const mismatch = () =>
    installPlanCliFailure(
      request,
      "environment-mismatch",
      "The running desktop does not match the requested environment ID.",
    );
  if (!environmentMatches(read)) return mismatch();
  const exact = yield* readExact(read, action.planId);
  if (!environmentMatches(exact)) return mismatch();
  if (confirmsInstallPlanCancellation(exact, action)) return boundedReply(request, exact);
  if (findInstallPlanResultPlan(exact, action.planId)?.cancelled === true)
    return boundedReply(request, {
      ...exact,
      refused: {
        code: "stale-revision",
        message: "That revision did not cancel this plan.",
      },
    });

  const result = yield* installPlan({
    type: "cancel-plan",
    planId: action.planId,
    expectedRevision: action.expectedRevision,
    environmentId: action.environmentId,
  });
  if (!environmentMatches(result)) return mismatch();
  if (result.refused?.code === "plan-mismatch") {
    // Another window can close and move the exact plan after the first read.
    const replay = yield* readExact(result, action.planId);
    if (!environmentMatches(replay)) return mismatch();
    if (confirmsInstallPlanCancellation(replay, action)) return boundedReply(request, replay);
  }
  return boundedReply(request, result);
});
