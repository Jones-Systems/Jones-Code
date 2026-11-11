import type { EnvironmentId } from "@t3tools/contracts";
import type { JonesOperationReceipt } from "@t3tools/contracts/jones/jonesOperations";
import type { JonesInstallPlan, JonesInstallPlanGrant } from "@t3tools/contracts/jones/installPlan";
import {
  executeJonesAction,
  prepareReviewedInstall,
  resolveDownloadAction,
  type JonesDownloadResolution,
  type JonesExecutorHost,
  type JonesReviewedInstall,
  isNativeJonesInstallReview,
} from "@t3tools/client-runtime/jones/updates/executor";
import {
  validateDirectiveEvidence,
  validateReceipt,
} from "@t3tools/client-runtime/jones/install-plan";
import { claimantId, routeLaptopDownload, routeLaptopInstall } from "./entrypoints";
import { installPlanServices, requestInstallPlan } from "./services";
import type { InstallStatusDevice } from "./panelUi";

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

export type CleanupInspection = {
  readonly status: "eligible" | "unproved";
  readonly recoveryToken?: string;
};

async function readHeldCleanup(
  binding: NonNullable<ReturnType<typeof installPlanServices>>,
  environmentId: EnvironmentId,
  operationId: string,
) {
  const { state } = await requestInstallPlan({ type: "read" }, binding);
  const plan = state.current;
  const device = plan?.devices.find((d) => d.environmentId === environmentId);
  const receipt = device?.operations.at(-1)?.receipt;
  if (
    state.readOnly !== undefined ||
    plan == null ||
    device === undefined ||
    receipt?.operationId !== operationId ||
    receipt.environmentId !== environmentId ||
    receipt.phase !== "held" ||
    receipt.code !== "cleanup-unconfirmed" ||
    receipt.jobId === undefined ||
    receipt.action.kind === "install"
  )
    return undefined;
  const { canOperate, observation } = await binding.services.observe(environmentId);
  if (
    !canOperate ||
    observation.freshness !== "fresh" ||
    observation.state?.environmentId !== environmentId ||
    observation.state.capability.operations !== 1
  )
    return undefined;
  return { planId: plan.planId, label: device.label, receipt, jobId: receipt.jobId };
}

export async function inspectPanelCleanup(
  environmentId: EnvironmentId,
  operationId: string,
): Promise<CleanupInspection> {
  const unproved = { status: "unproved" } as const;
  const binding = installPlanServices();
  if (binding === undefined) return unproved;
  try {
    const held = await readHeldCleanup(binding, environmentId, operationId);
    if (held === undefined) return unproved;
    const result = await binding.services.operation(environmentId, {
      action: "operationRead",
      input: { environmentId, operationIds: [operationId], inspectCleanup: operationId },
    });
    if (!("receipts" in result) || result.cleanupInspection?.operationId !== operationId)
      return unproved;
    const exact = result.receipts[0];
    return result.receipts.length === 1 &&
      exact?.operationId === operationId &&
      exact.receipt !== null &&
      exact.receipt.phase === "held" &&
      exact.receipt.code === "cleanup-unconfirmed" &&
      exact.receipt.jobId === held.jobId &&
      validateReceipt(held.receipt, exact.receipt)
      ? result.cleanupInspection
      : unproved;
  } catch {
    return unproved;
  }
}

export async function recoverPanelCleanup(
  environmentId: EnvironmentId,
  reviewed: JonesOperationReceipt,
  recoveryToken: string,
): Promise<"released" | "released-plan-unconfirmed" | "unproved" | "cancelled"> {
  const binding = installPlanServices();
  if (binding === undefined) return "unproved";
  const { services } = binding;
  const operationId = reviewed.operationId;
  const held = await readHeldCleanup(binding, environmentId, operationId);
  if (
    held === undefined ||
    held.jobId !== reviewed.jobId ||
    !validateReceipt(reviewed, held.receipt)
  )
    return "unproved";
  if (
    !(await services.confirm(
      `Release the cleanup hold on ${held.label} for operation ${operationId}? The host will verify the disk image is absent before releasing the hold. This action will not retry the download or install anything.`,
    ))
  )
    return "cancelled";
  const fresh = await readHeldCleanup(binding, environmentId, operationId);
  if (
    installPlanServices() !== binding ||
    fresh === undefined ||
    fresh.planId !== held.planId ||
    fresh.jobId !== held.jobId ||
    fresh.receipt.updatedAt !== held.receipt.updatedAt ||
    !validateReceipt(held.receipt, fresh.receipt)
  )
    return "unproved";
  const recovered = (receipt: JonesOperationReceipt | null | undefined) =>
    receipt != null &&
    receipt.jobId === held.jobId &&
    receipt.code === "cleanup-recovered" &&
    (receipt.phase === "stopped" || receipt.phase === "interrupted") &&
    validateReceipt(held.receipt, receipt);
  const response = await services
    .operation(environmentId, {
      action: "recoverCleanup",
      input: { environmentId, operationId, jobId: held.jobId, recoveryToken, confirmed: true },
    })
    .catch(() => undefined);
  let receipt =
    response !== undefined && "recoveredCleanup" in response ? response.recoveredCleanup : null;
  if (!recovered(receipt)) {
    const readback = await services
      .operation(environmentId, {
        action: "operationRead",
        input: { environmentId, operationIds: [operationId] },
      })
      .catch(() => undefined);
    receipt =
      readback !== undefined &&
      "receipts" in readback &&
      readback.receipts.length === 1 &&
      readback.receipts[0]?.operationId === operationId
        ? readback.receipts[0].receipt
        : null;
  }
  if (receipt === null || !recovered(receipt)) return "unproved";
  const report = await requestInstallPlan(
    {
      type: "report",
      planId: held.planId,
      environmentId,
      operationId,
      evidence: { kind: "receipt", receipt },
    },
    binding,
  ).catch(() => undefined);
  const recorded =
    report?.state.current?.planId === held.planId
      ? report.state.current.devices
          .find((d) => d.environmentId === environmentId)
          ?.operations.find((o) => o.operationId === operationId)?.receipt
      : undefined;
  return report?.refused === undefined && recovered(recorded)
    ? "released"
    : "released-plan-unconfirmed";
}

/**
 * Read one device's latest plan operation and report evidence that passes the same validation as
 * a manual refresh. It only reads hosts and records validated evidence in the plan; it never starts,
 * stops, fences, claims or directs work. `refreshInstallOutcome` asks the host to re-derive an
 * install outcome and stays limited to an explicit owner refresh.
 */
export async function refreshDeviceEvidence(
  binding: NonNullable<ReturnType<typeof installPlanServices>>,
  environmentId: EnvironmentId,
  plan: JonesInstallPlan,
  observed: InstallStatusDevice,
  options: { readonly refreshInstallOutcome: boolean },
) {
  const { services } = binding;
  const operation = plan.devices.find((d) => d.environmentId === environmentId)?.operations.at(-1);
  if (operation?.request.kind === "plan-directive") {
    const { observation, fleetStatus } = observed;
    if (
      operation.directive === undefined &&
      observed.canOperate &&
      observation.freshness === "fresh" &&
      observation.state?.environmentId === environmentId &&
      observation.state.capability.operations === 1 &&
      fleetStatus != null &&
      validateDirectiveEvidence(operation.request.input, fleetStatus)
    )
      await requestInstallPlan(
        {
          type: "report",
          planId: plan.planId,
          environmentId,
          operationId: operation.operationId,
          evidence: { kind: "directive", status: fleetStatus },
        },
        binding,
      );
  } else if (operation?.request.kind === "operation-start") {
    const result = await services.operation(environmentId, {
      action: "operationRead",
      input: {
        environmentId,
        operationIds: [operation.operationId],
        ...(options.refreshInstallOutcome &&
        operation.request.input.action.kind === "install" &&
        observed.canOperate &&
        observed.observation.freshness === "fresh" &&
        observed.observation.state?.environmentId === environmentId &&
        observed.observation.state.capability.operations === 1
          ? { refreshInstallOutcome: operation.operationId }
          : {}),
      },
    });
    if ("receipts" in result) {
      const receipt = result.receipts.find((r) => r.operationId === operation.operationId)?.receipt;
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
      services.kick?.(environmentId);
      const observed = await services.observe(environmentId);
      store.update({
        devices: store.get().devices.map((d) => (d.environmentId === environmentId ? observed : d)),
      });
      await refreshDeviceEvidence(binding, environmentId, plan, observed, {
        refreshInstallOutcome: true,
      });
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
  const proposed: JonesDownloadResolution & { readonly review?: JonesReviewedInstall } =
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
  if (proposed.review !== undefined && !isNativeJonesInstallReview(proposed.review))
    throw new Error("A native plan requires its full native install review.");
  const nativeReview =
    proposed.review !== undefined && isNativeJonesInstallReview(proposed.review)
      ? proposed.review
      : undefined;
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
      ...(nativeReview === undefined ? {} : { review: nativeReview }),
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
