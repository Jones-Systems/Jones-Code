import type { JonesInstallPlanState } from "@t3tools/contracts/jones/installPlan";
import { isPlanComplete, planCompletion } from "@t3tools/client-runtime/jones/install-plan";
import { installPlanServices, requestInstallPlan } from "./services";

/** Each click reconciles first. A lost response stops this sequence without a resend. */
export async function finishPlan(planId: string) {
  const binding = installPlanServices();
  if (binding === undefined) throw new Error("Install status is not connected.");
  const current = (state: JonesInstallPlanState) => {
    if (installPlanServices() !== binding) throw new Error("Install status connection changed.");
    if (state.readOnly !== undefined) throw new Error(state.readOnly.message);
    if (state.current?.planId !== planId)
      throw new Error("The update plan changed. Read status again.");
    return state.current;
  };
  const read = await requestInstallPlan({ type: "read" }, binding);
  if (read.refused !== undefined) throw new Error(read.refused.message);
  let plan = current(read.state);
  const identity = plan.target;
  const unchanged = () => {
    const native = binding.store.get().native;
    if (native === null) throw new Error("Install status is unavailable.");
    const observed = current(native);
    if (
      observed.revision !== plan.revision ||
      observed.target.repository !== identity.repository ||
      observed.target.sourceSha !== identity.sourceSha ||
      observed.target.sourceTree !== identity.sourceTree
    )
      throw new Error("The update plan changed. Read status before continuing.");
  };
  if (!isPlanComplete(plan)) {
    const observations = () =>
      new Map(
        binding.services.devices().map((device) => [device.environmentId, device.observation]),
      );
    const completion = planCompletion(plan, observations());
    if (!completion.completable) throw new Error("Fresh proof is required to finish this plan.");
    for (const candidate of completion.candidates) {
      unchanged();
      if (
        !planCompletion(plan, observations()).candidates.some(
          (d) => d.environmentId === candidate.environmentId,
        )
      )
        throw new Error("Running source proof changed. Read status before continuing.");
      const result = await requestInstallPlan(
        {
          type: "end",
          planId,
          expectedRevision: plan.revision,
          environmentId: candidate.environmentId,
        },
        binding,
      );
      if (result.refused !== undefined) throw new Error(result.refused.message);
      const next = current(result.state);
      if (
        next.target.repository !== identity.repository ||
        next.target.sourceSha !== identity.sourceSha ||
        next.target.sourceTree !== identity.sourceTree ||
        next.revision <= plan.revision ||
        next.devices.find((d) => d.environmentId === candidate.environmentId)?.ended === undefined
      )
        throw new Error("Finishing this device was not confirmed. Read status before continuing.");
      plan = next;
    }
  }
  unchanged();
  if (!isPlanComplete(plan)) throw new Error("The update plan still has unsettled work.");
  const result = await requestInstallPlan(
    { type: "dismiss", planId, expectedRevision: plan.revision },
    binding,
  );
  if (result.refused !== undefined) throw new Error(result.refused.message);
  if (
    installPlanServices() !== binding ||
    result.state.current !== null ||
    result.state.last?.planId !== planId ||
    result.state.last.dismissedAt === undefined
  )
    throw new Error("Plan dismissal was not confirmed. Read status before continuing.");
  binding.store.update({ open: false, showLast: true, previewSelection: [] });
}
