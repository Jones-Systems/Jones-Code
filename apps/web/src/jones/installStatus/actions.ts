import type { EnvironmentId } from "@t3tools/contracts";
import type { JonesOperationReceipt } from "@t3tools/contracts/jones/jonesOperations";
import type { JonesInstallPlan } from "@t3tools/contracts/jones/installPlan";
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
  canReplayDirective,
  deviceActiveOperations,
  hasUnconfirmedLauncherOutcome,
  laptopGateIssue,
  laptopInstallNeedsReopen,
  operationOutstanding,
  supportsOperationReceiptReads,
  validateDirectiveEvidence,
  validateReceipt,
} from "@t3tools/client-runtime/jones/install-plan";
import { claimantId, routeLaptopDownload, routeLaptopInstall } from "./entrypoints";
import {
  authorizedUpdateStatus,
  currentAuthorizedUpdateStatus,
  installPlanServices,
  requestInstallPlan,
} from "./services";
import type { InstallStatusDevice } from "./panelUi";
import { LAPTOP_REOPEN_TO_CHECK_INSTALL } from "./copy";

export type PanelAction =
  | "download"
  | "install"
  | "remove"
  | "end"
  | "check"
  | "abandon-stage"
  | "check-build";
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

export async function stopPanelDownload(target: {
  readonly kind: "laptop" | "device";
  readonly planId: string;
  readonly environmentId: EnvironmentId;
  readonly operationId: string;
}) {
  const binding = installPlanServices();
  if (binding === undefined) throw new Error("Install status is not connected.");
  const { services } = binding;
  const { kind, planId, environmentId, operationId } = target;
  const missingCapability = "Fresh authorized operation capability is required.";
  const canStop = () =>
    currentAuthorizedUpdateStatus(services, environmentId)?.capability.operations === 1;
  if (kind === "device") {
    const observed = await services.observe(environmentId);
    if (authorizedUpdateStatus(observed, environmentId)?.capability.operations !== 1)
      throw new Error(missingCapability);
  }
  const read = await requestInstallPlan({ type: "read" }, binding);
  const plan = read.state.current;
  if (read.state.readOnly !== undefined) throw new Error(read.state.readOnly.message);
  if (read.refused !== undefined) throw new Error(read.refused.message);
  const changedTarget = "This download is no longer current. Refresh Install status.";
  if (installPlanServices() !== binding || plan?.planId !== planId) throw new Error(changedTarget);
  if (kind === "laptop") {
    const operation = plan.laptop.operations.at(-1);
    if (
      plan.laptop.environmentId !== environmentId ||
      operation?.operationId !== operationId ||
      operation.step !== "download" ||
      (operation.result !== undefined && operation.result.status !== "running")
    )
      throw new Error(changedTarget);
    const result = await requestInstallPlan(
      { type: "laptop", planId, action: "stop", operationId },
      binding,
    );
    if (result.refused !== undefined) throw new Error(result.refused.message);
    return;
  }
  const device = plan.devices.find((candidate) => candidate.environmentId === environmentId);
  const operations = device === undefined ? [] : deviceActiveOperations(device);
  const operation = operations.find(operationOutstanding) ?? operations.at(-1);
  if (
    device?.membership !== "selected" ||
    device.abandoning === true ||
    operation?.operationId !== operationId ||
    operation.step !== "download" ||
    operation.request.kind !== "operation-start" ||
    operation.receipt === undefined ||
    !["accepted", "running", "stop-requested"].includes(operation.receipt.phase)
  )
    throw new Error(changedTarget);
  if (!canStop()) throw new Error(missingCapability);
  const claimed = await requestInstallPlan(
    {
      type: "claim",
      planId,
      environmentId,
      claimant: `window:${claimantId()}`,
      step: "stop",
      explicit: true,
      operationId,
    },
    binding,
  );
  if (claimed.refused !== undefined) throw new Error(claimed.refused.message);
  const grant = claimed.grant;
  const request = grant?.request;
  if (
    grant?.step !== "stop" ||
    grant.planId !== planId ||
    grant.environmentId !== environmentId ||
    grant.operationId !== operationId ||
    request?.kind !== "operation-stop" ||
    request.input.environmentId !== environmentId ||
    request.input.operationId !== operationId
  )
    throw new Error("The native grant does not match this Stop request.");
  if (
    !Number.isFinite(Date.parse(grant.leaseUntil)) ||
    Date.parse(grant.leaseUntil) <= services.now()
  )
    throw new Error("The native grant expired. Read status before continuing.");
  if (installPlanServices() !== binding || !canStop()) throw new Error(missingCapability);
  const receipt = await services.stop(environmentId, {
    action: "operationStop",
    input: request.input,
  });
  if (
    !validateReceipt(operation.request.input, receipt) ||
    !validateReceipt(operation.receipt, receipt)
  )
    throw new Error("The returned Stop receipt does not match this request.");
  const result = await requestInstallPlan(
    {
      type: "report",
      planId,
      environmentId,
      operationId,
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
  const plan = state.current ?? (state.last?.cancelled === true ? state.last : null);
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
  const reportedPlan = report?.state.current ?? report?.state.last;
  const recorded =
    reportedPlan?.planId === held.planId
      ? reportedPlan.devices
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
  const operations = plan.devices.find((d) => d.environmentId === environmentId)?.operations ?? [];
  const operation = operations.find(operationOutstanding) ?? operations.at(-1);
  if (operation?.request.kind === "plan-directive") {
    const { observation, fleetStatus } = observed;
    if (operation.directive !== undefined) return;
    if (
      !observed.canOperate ||
      observation.freshness !== "fresh" ||
      observation.state?.environmentId !== environmentId ||
      observation.state.capability.operations !== 1 ||
      fleetStatus == null
    ) {
      if (options.refreshInstallOutcome)
        return `Could not confirm update setup on ${observed.label}. Check its connection.`;
    } else if (validateDirectiveEvidence(operation.request.input, fleetStatus)) {
      const report = await requestInstallPlan(
        {
          type: "report",
          planId: plan.planId,
          environmentId,
          operationId: operation.operationId,
          evidence: { kind: "directive", status: fleetStatus },
        },
        binding,
      );
      if (options.refreshInstallOutcome && report.refused !== undefined)
        return report.refused.message;
    } else if (options.refreshInstallOutcome)
      return plan.cancelled === true
        ? `${observed.label} has yet to confirm setup from this cancelled plan.`
        : canReplayDirective(operation.request.input, fleetStatus)
          ? `${observed.label} still reports its previous setup. The plan can resend the same request.`
          : `${observed.label} reported a different setup. The original request is still unconfirmed.`;
  } else if (operation?.request.kind === "operation-start") {
    const { observation } = observed;
    const { input } = operation.request;
    if (
      observation.freshness !== "fresh" ||
      observation.state?.environmentId !== environmentId ||
      input.environmentId !== environmentId ||
      input.operationId !== operation.operationId ||
      !supportsOperationReceiptReads(observation.state.capability)
    )
      return options.refreshInstallOutcome
        ? `Could not read the exact update receipt on ${observed.label}. Refresh its status and try again.`
        : undefined;
    const wantsLauncherCheck = options.refreshInstallOutcome && input.action.kind === "install";
    const current = wantsLauncherCheck
      ? services.devices().find((device) => device.environmentId === environmentId)
      : undefined;
    const live = current?.observation;
    const liveStatusAt = live?.unsupportedAt ?? live?.heardAt;
    // A direct status read can succeed while the background stream is stale.
    const latest =
      (live?.freshness === "fresh" || live?.freshness === "unsupported") &&
      liveStatusAt !== undefined &&
      (observation.heardAt === undefined || liveStatusAt > observation.heardAt)
        ? live
        : observation;
    const checkLauncher =
      observed.canOperate &&
      current?.canOperate === true &&
      latest.freshness === "fresh" &&
      latest.state?.environmentId === environmentId &&
      supportsOperationReceiptReads(latest.state.capability);
    const result = await services.operation(environmentId, {
      action: "operationRead",
      input: {
        environmentId,
        operationIds: [operation.operationId],
        ...(checkLauncher ? { refreshInstallOutcome: operation.operationId } : {}),
      },
    });
    if ("receipts" in result) {
      const receipt = result.receipts.find((r) => r.operationId === operation.operationId)?.receipt;
      if (
        receipt != null &&
        validateReceipt(input, receipt) &&
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
    if (wantsLauncherCheck && !checkLauncher)
      return `Requested the retained update receipt from ${observed.label}. Checking its launcher requires fresh status and permission.`;
  }
}

/** Owner clicks claim once; lost replies keep the native reservation for receipt reconciliation. */
export async function performPanelAction(
  environmentId: EnvironmentId | "laptop",
  action: PanelAction,
  checkedPlanId?: string,
) {
  const binding = installPlanServices();
  if (binding === undefined) throw new Error("Install status is not connected.");
  const { services, store } = binding;
  const previouslyShown = store.get().native;
  const read = await requestInstallPlan({ type: "read" }, binding);
  const plan = read.state.current;
  if (action === "check") {
    if (environmentId === "laptop" && read.refused !== undefined)
      throw new Error(read.refused.message);
    store.update({ desktop: await services.bridge.getUpdateState() });
    const checked =
      checkedPlanId === undefined
        ? (plan ?? read.state.last)
        : ([plan, read.state.last].find((candidate) => candidate?.planId === checkedPlanId) ??
          null);
    if (checkedPlanId !== undefined && checked === null)
      throw new Error("This update plan is no longer available. Refresh Install status.");
    if (environmentId !== "laptop") {
      services.kick?.(environmentId);
      const refresh = async () => {
        const observed = await services.observe(environmentId);
        if (services.readStatus === undefined) return observed;
        const state = await services.readStatus(environmentId);
        if (state?.environmentId !== environmentId)
          throw new Error("Could not read current status for this device.");
        return {
          ...observed,
          observation: { state, freshness: "fresh" as const, heardAt: services.now() },
        };
      };
      const update = (observed: InstallStatusDevice) =>
        store.update({
          devices: store
            .get()
            .devices.map((d) => (d.environmentId === environmentId ? observed : d)),
        });
      const observed = await refresh();
      update(observed);
      const planReadError = read.refused?.message ?? read.state.readOnly?.message;
      const message =
        checked === null
          ? undefined
          : await refreshDeviceEvidence(binding, environmentId, checked, observed, {
              refreshInstallOutcome: planReadError === undefined,
            });
      if (checked !== null && services.readStatus !== undefined) {
        const state = await services.readStatus(environmentId);
        if (state?.environmentId === environmentId)
          update({
            ...observed,
            observation: { state, freshness: "fresh", heardAt: services.now() },
          });
      }
      const currentDevice = store
        .get()
        .devices.find((item) => item.environmentId === environmentId);
      const latestState = store.get().native;
      const latestPlan = [latestState?.current, latestState?.last].find(
        (candidate) => checked !== null && candidate?.planId === checked.planId,
      );
      const latestOperation = latestPlan?.devices
        .find((item) => item.environmentId === environmentId)
        ?.operations.find(operationOutstanding);
      const progress = currentDevice?.observation.state?.progress;
      const installUnconfirmed =
        latestOperation?.step === "install" &&
        (hasUnconfirmedLauncherOutcome(latestOperation.receipt) ||
          (progress?.operationId === latestOperation.operationId &&
            progress.error?.code === "outcome-unavailable"));
      store.update({
        error:
          planReadError ??
          message ??
          (currentDevice?.observation.freshness !== "fresh"
            ? `Could not refresh ${observed.label}. Check its connection and try again.`
            : latestOperation === undefined
              ? `Refreshed ${observed.label}.`
              : installUnconfirmed
                ? `Checked ${observed.label}. Its install outcome is still unconfirmed.`
                : `Checked ${observed.label}. Its update outcome is still pending.`),
      });
    } else {
      const latest = store.get().native ?? read.state;
      const checkedLaptop = [latest.current, latest.last].find(
        (candidate) => checked !== null && candidate?.planId === checked.planId,
      );
      const previousPlan = [previouslyShown?.current, previouslyShown?.last].find(
        (candidate) => candidate?.planId === checkedLaptop?.planId,
      );
      const gateIssue = checkedLaptop == null ? undefined : laptopGateIssue(checkedLaptop.laptop);
      const operation = checkedLaptop?.laptop.operations.at(-1);
      let error = "Refreshed Laptop.";
      if (laptopInstallNeedsReopen(checkedLaptop, latest.laptop))
        error = `Checked Laptop. ${LAPTOP_REOPEN_TO_CHECK_INSTALL}`;
      else if (gateIssue !== undefined) error = `Checked Laptop. ${gateIssue.reason}`;
      else if (previousPlan?.laptop.gatePause === true && checkedLaptop?.laptop.gate !== undefined)
        error = "Checked Laptop. Verification is restored. Other devices can continue.";
      else if (operation?.result?.status === "held")
        error = "Checked Laptop. Its update outcome is still unconfirmed.";
      else if (operation?.step === "install" && operation.result?.status === "rolled-back")
        error = "Checked Laptop. The install rolled back.";
      else if (operation?.step === "install" && operation.result?.status === "committed")
        error =
          checkedLaptop?.laptop.gate === undefined
            ? "Checked Laptop. The install committed. Verification is still pending."
            : "Checked Laptop. The selected build is verified.";
      store.update({ error });
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
    if (action === "check-build" && services.bridge.checkForUpdate !== undefined) {
      const checked = await services.bridge.checkForUpdate();
      store.update({ desktop: checked.state });
      return;
    }
    if (action === "abandon-stage" && plan !== null) {
      if (
        !(await services.confirm(
          "Abandon the Laptop's staged update? The downloaded selection will be discarded.",
        ))
      )
        return;
      const result = await requestInstallPlan(
        { type: "laptop", planId: plan.planId, action: "abandon-stage" },
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
  if (action === "abandon-stage" || action === "check-build")
    throw new Error("This action is only available on Laptop.");
  const observed = await services.observe(environmentId);
  if (
    !observed.canOperate ||
    observed.observation.freshness !== "fresh" ||
    observed.observation.state?.capability.operations !== 1
  )
    throw new Error("Fresh authorized operation capability is required.");
  const device = plan.devices.find((d) => d.environmentId === environmentId);
  if (device === undefined) throw new Error("This device is not in the plan.");
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
