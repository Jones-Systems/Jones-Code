import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import type { PlanId, ThreadId } from "@t3tools/contracts";

export function planFollowUpSource(
  interactionMode: "default" | "plan",
  threadId: ThreadId,
  planId: PlanId | undefined,
) {
  return interactionMode === "default" && planId !== undefined
    ? { sourceProposedPlan: { threadId, planId } }
    : {};
}

export async function dispatchPlanFollowUp<A, Settings, ESettings, ETurn>(input: {
  readonly persistSettings: () => Promise<AtomCommandResult<Settings, ESettings>>;
  readonly onSettingsPersisted: () => void;
  readonly startTurn: () => Promise<AtomCommandResult<A, ETurn>>;
}): Promise<AtomCommandResult<Settings | A, ESettings | ETurn>> {
  const settings = await input.persistSettings();
  if (settings._tag === "Failure") return settings;
  input.onSettingsPersisted();
  return input.startTurn();
}
