import type { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesInstallPlanGrant,
  JonesInstallPlanReview,
} from "@t3tools/contracts/jones/installPlan";
import {
  executeJonesAction,
  prepareReviewedInstall,
  resolveDownloadAction,
  type JonesDownloadResolution,
  type JonesExecutorHost,
} from "@t3tools/client-runtime/jones/updates/executor";
import { validateReceipt } from "@t3tools/client-runtime/jones/install-plan";
import { claimantId, routeLaptopDownload, routeLaptopInstall } from "./entrypoints";
import { installPlanServices, requestInstallPlan } from "./services";

export type PanelAction = "download" | "install" | "stop" | "remove" | "end" | "check";
function executorHostFor(environmentId: EnvironmentId): JonesExecutorHost {
  const binding = installPlanServices();
  if (binding === undefined) throw new Error("Install status is not connected.");
  const { services, store } = binding;
  return {
    readLatest: () => {
      const device = services.devices().find((d) => d.environmentId === environmentId);
      return device ?? { observation: { state: null, freshness: "loading" }, canOperate: false };
    },
    legacy: (request) => services.legacy(environmentId, request),
    start: async (input) => {
      const result = await services.operation(environmentId, { action: "operationStart", input });
      if (!("operationId" in result))
        throw new Error("The host did not return an operation receipt.");
      const plan = store.get().native?.current;
      if (input.correlation?.planId === plan?.planId && validateReceipt(input, result))
        await requestInstallPlan(
          {
            type: "report",
            planId: input.correlation!.planId,
            environmentId,
            operationId: input.operationId,
            evidence: { kind: "receipt", receipt: result },
          },
          binding,
        );
      return result;
    },
    confirm: services.confirm,
    preflight: async (input) => {
      const result = await services.operation(environmentId, {
        action: "recoveryPreflight",
        input,
      });
      if (!("binding" in result)) throw new Error("Recovery preflight is unavailable.");
      return result;
    },
    chooseRecovery: services.chooseRecovery,
  };
}

async function sendStopGrant(grant: JonesInstallPlanGrant) {
  const binding = installPlanServices();
  if (binding === undefined) throw new Error("Install status is not connected.");
  const { services } = binding;
  if (
    !Number.isFinite(Date.parse(grant.leaseUntil)) ||
    Date.parse(grant.leaseUntil) <= services.now()
  )
    throw new Error("The native grant expired. Read status before continuing.");
  const request = grant.request;
  if (grant.environmentId !== request.input.environmentId || request.kind !== "operation-stop")
    throw new Error("The native grant does not match this action.");
  const receipt = await services.stop(grant.environmentId, {
    action: "operationStop",
    input: request.input,
  });
  if (receipt.operationId !== grant.operationId || receipt.environmentId !== grant.environmentId)
    throw new Error("The returned Stop receipt does not match this request.");
  const result = await requestInstallPlan(
    {
      type: "report",
      planId: grant.planId,
      environmentId: grant.environmentId,
      operationId: grant.operationId,
      evidence: { kind: "receipt", receipt },
    },
    binding,
  );
  if (result.refused !== undefined) throw new Error(result.refused.message);
}

/** Owner clicks claim once; lost replies keep the native reservation for receipt reconciliation. */
export async function performPanelAction(
  environmentId: EnvironmentId | "laptop",
  action: PanelAction,
) {
  const binding = installPlanServices();
  if (binding === undefined) throw new Error("Install status is not connected.");
  const { services, store } = binding;
  const read = await requestInstallPlan({ type: "read" }, binding);
  const plan = read.state.current;
  if (action === "check") {
    store.update({ desktop: await services.bridge.getUpdateState() });
    if (environmentId !== "laptop" && plan !== null) {
      const observed = await services.observe(environmentId);
      store.update({
        devices: store.get().devices.map((d) => (d.environmentId === environmentId ? observed : d)),
      });
      const operation = plan.devices
        .find((d) => d.environmentId === environmentId)
        ?.operations.at(-1);
      if (operation?.request.kind === "operation-start") {
        const result = await services.operation(environmentId, {
          action: "operationRead",
          input: { environmentId, operationIds: [operation.operationId] },
        });
        if ("receipts" in result) {
          const receipt = result.receipts.find(
            (r) => r.operationId === operation.operationId,
          )?.receipt;
          if (
            receipt != null &&
            validateReceipt(operation.request.input, receipt) &&
            (operation.receipt === undefined || validateReceipt(operation.receipt, receipt))
          )
            await requestInstallPlan(
              {
                type: "report",
                planId: plan.planId,
                environmentId,
                operationId: operation.operationId,
                evidence: { kind: "receipt", receipt },
              },
              binding,
            );
        }
      }
    }
    return;
  }
  if (read.state.readOnly !== undefined) throw new Error(read.state.readOnly.message);
  if (environmentId === "laptop") {
    const desktop = await services.bridge.getUpdateState();
    if (action === "download") {
      await routeLaptopDownload(services.bridge, desktop);
      return;
    }
    if (action === "install") {
      await routeLaptopInstall(services.bridge, desktop);
      return;
    }
    if (action === "stop" && plan !== null) {
      const result = await requestInstallPlan(
        { type: "laptop", planId: plan.planId, action: "stop" },
        binding,
      );
      if (result.refused !== undefined) throw new Error(result.refused.message);
      return;
    }
    throw new Error("That Laptop action is unavailable.");
  }
  if (plan === null) throw new Error("Start with Download on Laptop.");
  if (action === "remove" || action === "end") {
    const result = await requestInstallPlan(
      action === "remove"
        ? {
            type: "edit",
            planId: plan.planId,
            expectedRevision: plan.revision,
            select: [],
            deselect: [environmentId],
          }
        : { type: "end", planId: plan.planId, expectedRevision: plan.revision, environmentId },
      binding,
    );
    if (result.refused !== undefined) throw new Error(result.refused.message);
    const applied = result.applied?.find((d) => d.environmentId === environmentId);
    if (applied?.result === "locked-started")
      throw new Error(
        "This device started before removal, so it stays in. Open it to stop the download.",
      );
    return;
  }
  const observed = await services.observe(environmentId);
  if (
    !observed.canOperate ||
    observed.observation.freshness !== "fresh" ||
    observed.observation.state?.capability.operations !== 1
  )
    throw new Error("Fresh authorized operation capability is required.");
  const device = plan.devices.find((d) => d.environmentId === environmentId);
  if (device === undefined) throw new Error("This device is not in the plan.");
  if (action === "stop") {
    const operation = device.operations.at(-1);
    if (operation === undefined) throw new Error("No download operation is recorded.");
    const result = await requestInstallPlan(
      {
        type: "claim",
        planId: plan.planId,
        environmentId,
        claimant: `window:${claimantId()}`,
        step: "stop",
        explicit: true,
        operationId: operation.operationId,
      },
      binding,
    );
    if (result.grant === undefined)
      throw new Error(result.refused?.message ?? "Stop was not granted.");
    await sendStopGrant(result.grant);
    return;
  }
  const host = executorHostFor(environmentId);
  const proposed: JonesDownloadResolution & { readonly review?: JonesInstallPlanReview } =
    action === "install"
      ? await prepareReviewedInstall({
          environmentId,
          targetSource: plan.target.sourceSha,
          label: device.label,
          host,
          now: services.now,
        })
      : resolveDownloadAction({ ...observed, environmentId, targetSource: plan.target.sourceSha });
  if (proposed.kind !== "ready") throw new Error(proposed.message);
  const latest = host.readLatest();
  const result = await requestInstallPlan(
    {
      type: "claim",
      planId: plan.planId,
      environmentId,
      claimant: `window:${claimantId()}`,
      step: action,
      explicit: true,
      action: proposed.action,
      ...(proposed.review === undefined ? {} : { review: proposed.review }),
      ...(latest.observation.state?.installedSource === undefined
        ? {}
        : { expectedInstalledSource: latest.observation.state.installedSource.sha }),
    },
    binding,
  );
  if (result.grant === undefined)
    throw new Error(result.refused?.message ?? "The action was not granted.");
  const grant = result.grant;
  const executed = await executeJonesAction({
    environmentId,
    targetSource: plan.target.sourceSha,
    operationId: grant.operationId,
    action: proposed.action,
    host: { ...host, planGate: async () => ({ kind: "grant", grant }) },
    now: services.now,
    ...(proposed.review === undefined ? {} : { review: proposed.review }),
  });
  if (executed.kind !== "accepted") throw new Error(executed.message);
}
