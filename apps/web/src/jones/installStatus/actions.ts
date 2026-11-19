import type { EnvironmentId } from "@t3tools/contracts";
import type { JonesOperationReceipt } from "@t3tools/contracts/jones/jonesOperations";
import type {
  JonesInstallPlan,
  JonesInstallPlanDevice,
  JonesInstallPlanHostOperation,
  JonesInstallPlanResult,
} from "@t3tools/contracts/jones/installPlan";
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
import {
  claimantId,
  routeLaptopDownload,
  routeLaptopInstall,
  targetForDesktop,
} from "./entrypoints";
import {
  authorizedUpdateStatus,
  currentAuthorizedUpdateStatus,
  exactOperationRequest,
  exactPlan,
  hasCurrentPlanEnrollment,
  hasPlanEnrollment,
  installPlanServices,
  nativePlanIssue,
  readTargetPlan,
  readWritableTarget,
  requestInstallPlan,
  requireWritablePlan,
  revisionFor,
  type InstallPlanBinding,
  type PlanTarget,
} from "./services";
import type { InstallStatusDevice } from "./panelUi";
import { archivedPlanFrom, selectedHistoryPlan } from "./panelUi";
import { LAPTOP_REOPEN_TO_CHECK_INSTALL } from "./copy";

export type PanelAction =
  | "download"
  | "install"
  | "remove"
  | "end"
  | "check"
  | "abandon-stage"
  | "check-build";
function executorHostFor(
  binding: InstallPlanBinding,
  device: JonesInstallPlanDevice,
  observed: InstallStatusDevice,
  planId: string,
): JonesExecutorHost {
  const { services, store } = binding;
  const { environmentId } = device;
  return {
    readLatest: () => {
      const device = services.devices().find((d) => d.environmentId === environmentId);
      return device ?? { observation: { state: null, freshness: "loading" }, canOperate: false };
    },
    legacy: (request) => services.legacy(environmentId, request),
    start: async (input) => {
      requireWritablePlan(binding);
      if (
        store.get().native?.current?.planId !== planId ||
        input.correlation?.planId !== planId ||
        input.correlation.deviceKey !== environmentId ||
        !hasCurrentPlanEnrollment(services, device, observed)
      )
        throw new Error("This update plan changed before dispatch.");
      const result = await services.operation(environmentId, { action: "operationStart", input });
      if (!("operationId" in result))
        throw new Error("The host did not return an operation receipt.");
      if (installPlanServices() === binding && validateReceipt(input, result))
        await requestInstallPlan(
          {
            type: "report",
            planId,
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

export type PanelOperationTarget = PlanTarget & {
  readonly environmentId: EnvironmentId;
  readonly operationId: string;
};

function latestOperation(device: JonesInstallPlanDevice, archived: boolean) {
  const operations = archived ? device.operations : deviceActiveOperations(device);
  return operations.find(operationOutstanding) ?? operations.at(-1);
}

export function canFenceArchivedOperation(operation: JonesInstallPlanHostOperation, now: number) {
  return (
    operation.request.kind === "operation-start" &&
    operation.receipt === undefined &&
    Date.parse(operation.lease.leaseUntil) <= now &&
    (operation.fence === undefined || Date.parse(operation.fence.lease.leaseUntil) <= now)
  );
}

export function canStopOperation(operation: JonesInstallPlanHostOperation) {
  return (
    operation.step === "download" &&
    operation.request.kind === "operation-start" &&
    operation.receipt !== undefined &&
    ["accepted", "running", "stop-requested"].includes(operation.receipt.phase)
  );
}

const staleDownload = "This download is no longer current. Refresh Install status.";

export async function stopPanelDownload(
  target: PanelOperationTarget & { readonly kind: "laptop" | "device" },
  binding = installPlanServices(),
) {
  if (target.kind === "device") return recoverRemoteOperation(target, "stop", binding);
  if (binding === undefined) throw new Error("Install status is not connected.");
  const { plan, state } = await readWritableTarget(binding, target, staleDownload);
  const operation = plan.laptop.operations.at(-1);
  if (
    (target.archived &&
      (state.view.recoveryHistory !== 1 || target.expectedRevision === undefined)) ||
    state.laptop.environmentId !== target.environmentId ||
    plan.laptop.environmentId !== target.environmentId ||
    operation?.operationId !== target.operationId ||
    operation.step !== "download" ||
    (operation.result !== undefined && operation.result.status !== "running")
  )
    throw new Error(staleDownload);
  const result = await requestInstallPlan(
    {
      type: "laptop",
      planId: target.planId,
      action: "stop",
      operationId: target.operationId,
      ...revisionFor(state, plan),
    },
    binding,
  );
  const issue = nativePlanIssue(result);
  if (issue !== undefined) throw new Error(issue);
  if (target.archived) await readTargetPlan(binding, target);
}

export function fencePanelOperation(
  target: PanelOperationTarget & { readonly archived: true },
  binding = installPlanServices(),
) {
  return recoverRemoteOperation(target, "fence", binding);
}

async function recoverRemoteOperation(
  target: PanelOperationTarget,
  step: "stop" | "fence",
  binding: InstallPlanBinding | undefined,
) {
  if (binding === undefined) throw new Error("Install status is not connected.");
  const { services } = binding;
  const { planId, environmentId, operationId } = target;
  const observed = await services.observe(environmentId);
  const { plan, state } = await readWritableTarget(
    binding,
    target,
    step === "stop" ? staleDownload : undefined,
  );
  const archived = target.archived === true;
  const device = plan.devices.find((candidate) => candidate.environmentId === environmentId);
  const operation = archived
    ? device?.operations.find((candidate) => candidate.operationId === operationId)
    : device === undefined
      ? undefined
      : latestOperation(device, false);
  const original =
    operation === undefined ? undefined : exactOperationRequest(planId, environmentId, operation);
  if (
    device === undefined ||
    operation?.operationId !== operationId ||
    original === undefined ||
    device.membership !== "selected" ||
    device.abandoning === true ||
    (archived && (state.view.recoveryHistory !== 1 || target.expectedRevision === undefined)) ||
    (step === "stop"
      ? !canStopOperation(operation)
      : !archived || !canFenceArchivedOperation(operation, services.now()))
  )
    throw new Error(
      step === "stop"
        ? staleDownload
        : "This operation is no longer eligible. Refresh Install status.",
    );
  const recheckHost = (checkOriginal = true) => {
    const status = currentAuthorizedUpdateStatus(services, environmentId);
    if (
      authorizedUpdateStatus(observed, environmentId)?.capability.operations !== 1 ||
      status?.capability.operations !== 1 ||
      !hasCurrentPlanEnrollment(services, device, observed) ||
      (checkOriginal && !archived && status.operation?.operationId !== operationId)
    )
      throw new Error("Fresh authorized operation capability is required.");
  };
  recheckHost();
  const claimed = await requestInstallPlan(
    {
      type: "claim",
      planId,
      environmentId,
      claimant: `window:${claimantId()}`,
      step,
      explicit: true,
      operationId,
      ...revisionFor(state, plan),
    },
    binding,
  );
  requireWritablePlan(binding, claimed);
  if (!archived && claimed.state.current?.planId !== planId)
    throw new Error("The plan changed before dispatch. Read the exact operation.");
  const grant = claimed.grant;
  const request = grant?.request;
  if (
    grant?.step !== step ||
    grant.planId !== planId ||
    grant.environmentId !== environmentId ||
    grant.operationId !== operationId ||
    request === undefined ||
    !Number.isFinite(Date.parse(grant.leaseUntil)) ||
    Date.parse(grant.leaseUntil) <= services.now()
  )
    throw new Error(
      "The native grant is unavailable, mismatched, or expired. Read the exact operation.",
    );
  recheckHost();
  let receipt: JonesOperationReceipt;
  if (
    step === "stop" &&
    request.kind === "operation-stop" &&
    request.input.environmentId === environmentId &&
    request.input.operationId === operationId
  ) {
    receipt = await services.stop(environmentId, { action: "operationStop", input: request.input });
  } else if (
    step === "fence" &&
    request.kind === "operation-fence" &&
    // A fence uses the entire original operationStart body and can return an already accepted receipt.
    validateReceipt(original, {
      ...request.input,
      protocol: 1,
      phase: "fenced",
      updatedAt: grant.leaseUntil,
    })
  ) {
    const result = await services.operation(environmentId, {
      action: "operationFence",
      input: request.input,
    });
    if (!("operationId" in result)) throw new Error("The fence has no exact operation receipt.");
    receipt = result;
  } else
    throw new Error(
      step === "stop"
        ? "The native grant does not match this Stop request."
        : "The native grant does not match the original operation.",
    );
  if (
    !validateReceipt(original, receipt) ||
    (operation.receipt !== undefined && !validateReceipt(operation.receipt, receipt))
  )
    throw new Error("The host receipt does not match the original operation.");
  const result = await (async () => {
    requireWritablePlan(binding);
    recheckHost(false);
    return requestInstallPlan(
      {
        type: "report",
        planId,
        environmentId,
        operationId,
        evidence: { kind: "receipt", receipt },
      },
      binding,
    );
  })().catch(() => {
    throw new Error(
      "The host returned the exact receipt. Saving it in Install status is unconfirmed.",
    );
  });
  const issue = nativePlanIssue(result);
  if (issue !== undefined)
    throw new Error(`The host returned the exact receipt. Saving it failed: ${issue}`);
  if (archived) {
    const saved = await readTargetPlan(binding, target).catch(() => undefined);
    const recorded = saved?.plan?.devices
      .find((item) => item.environmentId === environmentId)
      ?.operations.find((item) => item.operationId === operationId)?.receipt;
    if (recorded === undefined || !validateReceipt(receipt, recorded))
      throw new Error(
        "The host returned the exact receipt. Readback from recovery history is unconfirmed.",
      );
  }
}

export type CleanupInspection = {
  readonly status: "eligible" | "unproved";
  readonly recoveryToken?: string;
};

type CleanupTarget = PanelOperationTarget & { readonly jobId: string };

async function readHeldCleanup(binding: InstallPlanBinding, target: CleanupTarget) {
  const { environmentId, operationId, jobId } = target;
  const observed = await binding.services.observe(environmentId);
  const { state, plan } = await readWritableTarget(binding, target);
  const device = plan.devices.find((item) => item.environmentId === environmentId);
  const operation = device?.operations.find((item) => item.operationId === operationId);
  const receipt = operation?.receipt;
  const original =
    operation === undefined
      ? undefined
      : exactOperationRequest(plan.planId, environmentId, operation);
  if (
    (target.archived && state.view.recoveryHistory !== 1 && plan.cancelled !== true) ||
    device === undefined ||
    original === undefined ||
    receipt === undefined ||
    receipt.phase !== "held" ||
    receipt.code !== "cleanup-unconfirmed" ||
    receipt.jobId !== jobId ||
    receipt.action.kind === "install" ||
    !validateReceipt(original, receipt) ||
    !hasCurrentPlanEnrollment(binding.services, device, observed) ||
    authorizedUpdateStatus(observed, environmentId)?.capability.operations !== 1 ||
    currentAuthorizedUpdateStatus(binding.services, environmentId)?.capability.operations !== 1
  )
    return undefined;
  return { label: device.label, receipt };
}

function exactReadReceipt(
  result: Awaited<ReturnType<InstallPlanBinding["services"]["operation"]>>,
  operationId: string,
) {
  return "receipts" in result &&
    result.receipts.length === 1 &&
    result.receipts[0]?.operationId === operationId
    ? result.receipts[0].receipt
    : null;
}

export async function inspectPanelCleanup(
  target: CleanupTarget,
  binding = installPlanServices(),
): Promise<CleanupInspection> {
  const unproved = { status: "unproved" } as const;
  if (binding === undefined) return unproved;
  const { environmentId, operationId, jobId } = target;
  try {
    const held = await readHeldCleanup(binding, target);
    if (held === undefined) return unproved;
    const result = await binding.services.operation(environmentId, {
      action: "operationRead",
      input: { environmentId, operationIds: [operationId], inspectCleanup: operationId },
    });
    if (!("receipts" in result) || result.cleanupInspection?.operationId !== operationId)
      return unproved;
    const receipt = exactReadReceipt(result, operationId);
    return receipt !== null &&
      receipt.phase === "held" &&
      receipt.code === "cleanup-unconfirmed" &&
      receipt.jobId === jobId &&
      validateReceipt(held.receipt, receipt)
      ? result.cleanupInspection
      : unproved;
  } catch {
    return unproved;
  }
}

export async function recoverPanelCleanup(
  target: CleanupTarget,
  reviewed: JonesOperationReceipt,
  recoveryToken: string,
  binding = installPlanServices(),
): Promise<"released" | "released-plan-unconfirmed" | "unproved" | "cancelled"> {
  if (binding === undefined) return "unproved";
  const { services } = binding;
  const { planId, environmentId, operationId, jobId } = target;
  const held = await readHeldCleanup(binding, target);
  if (
    held === undefined ||
    reviewed.jobId !== jobId ||
    reviewed.updatedAt !== held.receipt.updatedAt ||
    !validateReceipt(reviewed, held.receipt)
  )
    return "unproved";
  if (
    !(await services.confirm(
      `Release the cleanup hold on ${held.label} for operation ${operationId}? The host will verify the disk image is absent before releasing the hold. This action releases cleanup only.`,
    ))
  )
    return "cancelled";
  const fresh = await readHeldCleanup(binding, target);
  if (
    fresh === undefined ||
    fresh.receipt.updatedAt !== held.receipt.updatedAt ||
    !validateReceipt(held.receipt, fresh.receipt)
  )
    return "unproved";
  const recovered = (receipt: JonesOperationReceipt | null | undefined) =>
    receipt != null &&
    receipt.jobId === jobId &&
    receipt.code === "cleanup-recovered" &&
    (receipt.phase === "stopped" || receipt.phase === "interrupted") &&
    validateReceipt(held.receipt, receipt);
  const response = await services
    .operation(environmentId, {
      action: "recoverCleanup",
      input: { environmentId, operationId, jobId, recoveryToken, confirmed: true },
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
    receipt = readback === undefined ? null : exactReadReceipt(readback, operationId);
  }
  if (receipt === null || !recovered(receipt)) return "unproved";
  const stored = await (async () => {
    requireWritablePlan(binding);
    const report = await requestInstallPlan(
      {
        type: "report",
        planId,
        environmentId,
        operationId,
        evidence: { kind: "receipt", receipt },
      },
      binding,
    );
    if (nativePlanIssue(report) !== undefined) return null;
    const fromResult = [exactPlan(report, planId, false), exactPlan(report, planId, true)].find(
      (plan) => plan !== null,
    );
    if (target.archived || fromResult === undefined)
      return (await readTargetPlan(binding, { planId, archived: true })).plan ?? fromResult;
    return fromResult;
  })().catch(() => null);
  const recorded = stored?.devices
    .find((device) => device.environmentId === environmentId)
    ?.operations.find((operation) => operation.operationId === operationId)?.receipt;
  return recovered(recorded) ? "released" : "released-plan-unconfirmed";
}

/**
 * Read the original operation and optionally save exact evidence. A periodic read never asks the
 * launcher to re-derive an outcome. Older native clients can still read current and last.
 */
export async function refreshDeviceEvidence(
  binding: InstallPlanBinding,
  environmentId: EnvironmentId,
  plan: JonesInstallPlan,
  observed: InstallStatusDevice,
  options: {
    readonly refreshInstallOutcome: boolean;
    readonly allowReport?: boolean;
    readonly archived?: boolean;
    readonly explain?: boolean;
    readonly operationId?: string | null;
  },
) {
  const { services } = binding;
  const { observation, fleetStatus, label } = observed;
  const archived =
    options.archived ?? (plan.dismissedAt !== undefined || plan.supersededAt !== undefined);
  const explain = options.explain ?? options.refreshInstallOutcome;
  const device = plan.devices.find((item) => item.environmentId === environmentId);
  if (device === undefined) return undefined;
  const operation =
    options.operationId === undefined
      ? latestOperation(device, archived)
      : device.operations.find((item) => item.operationId === options.operationId);
  if (operation === undefined) return undefined;
  const canWrite = () =>
    options.allowReport !== false &&
    installPlanServices() === binding &&
    !binding.store.get().native?.readOnly &&
    !binding.store.get().nativeRefusal;
  const authority = () => currentAuthorizedUpdateStatus(services, environmentId, observed);
  const hasScope = () => {
    const status = authority();
    return (
      status !== undefined &&
      supportsOperationReceiptReads(status.capability) &&
      hasCurrentPlanEnrollment(services, device, observed)
    );
  };
  const save = async (
    evidence: Extract<Parameters<typeof requestInstallPlan>[0], { type: "report" }>["evidence"],
  ) => {
    try {
      const report = await requestInstallPlan(
        {
          type: "report",
          planId: plan.planId,
          environmentId,
          operationId: operation.operationId,
          evidence,
        },
        binding,
      );
      const issue = nativePlanIssue(report);
      if (issue !== undefined) return `Saving the exact evidence failed: ${issue}`;
      if (archived) {
        const updated = await readTargetPlan(binding, { planId: plan.planId, archived: true });
        const saved = updated.plan?.devices
          .find((item) => item.environmentId === environmentId)
          ?.operations.find((item) => item.operationId === operation.operationId);
        if (
          nativePlanIssue(updated.result) !== undefined ||
          (evidence.kind === "receipt"
            ? saved?.receipt === undefined || !validateReceipt(evidence.receipt, saved.receipt)
            : saved?.directive?.kind !== "applied" ||
              saved.directive.directive.directiveId !== operation.operationId)
        )
          return "The exact evidence was read. Saving it in recovery history is unconfirmed.";
      }
    } catch {
      return "The exact evidence was read. Saving it in Install status is unconfirmed.";
    }
    return undefined;
  };

  if (operation.request.kind === "plan-directive") {
    if (operation.directive !== undefined) return undefined;
    const input = operation.request.input;
    if (
      observation.freshness !== "fresh" ||
      observation.state?.environmentId !== environmentId ||
      observation.state.capability.operations !== 1 ||
      input.environmentId !== environmentId ||
      input.directiveId !== operation.operationId ||
      input.enrollmentId !== device.enrollmentId ||
      input.targetSource !== plan.target.sourceSha ||
      input.targetTree !== plan.target.sourceTree ||
      fleetStatus == null ||
      !hasPlanEnrollment(device, observed)
    )
      return explain
        ? `Could not confirm update setup on ${label}. Check its connection.`
        : undefined;
    if (!validateDirectiveEvidence(input, fleetStatus)) {
      if (!explain) return undefined;
      return archived
        ? `Setup on ${label} is still unconfirmed for this closed plan.`
        : canReplayDirective(input, fleetStatus)
          ? `${label} still reports its previous setup. The plan can resend the same request.`
          : `${label} reported a different setup. The original request is still unconfirmed.`;
    }
    if (!canWrite() || !hasScope())
      return explain
        ? `Read matching setup on ${label}. Saving it requires plan access and permission.`
        : undefined;
    const problem = await save({ kind: "directive", status: fleetStatus });
    return explain
      ? (problem ?? (archived ? `Saved the exact setup confirmation for ${label}.` : undefined))
      : undefined;
  }

  const input = exactOperationRequest(plan.planId, environmentId, operation);
  if (
    observation.freshness !== "fresh" ||
    observation.state?.environmentId !== environmentId ||
    input === undefined ||
    !supportsOperationReceiptReads(observation.state.capability) ||
    (archived && !hasPlanEnrollment(device, observed))
  )
    return explain
      ? `Could not read the exact update receipt on ${label}. Refresh its status and try again.`
      : undefined;
  const wantsLauncherCheck =
    options.refreshInstallOutcome &&
    input.action.kind === "install" &&
    operationOutstanding(operation);
  const liveStatus = wantsLauncherCheck ? authority() : undefined;
  const checkLauncher =
    wantsLauncherCheck &&
    canWrite() &&
    hasCurrentPlanEnrollment(services, device, observed) &&
    liveStatus !== undefined &&
    supportsOperationReceiptReads(liveStatus.capability);
  let receipt: JonesOperationReceipt | null;
  try {
    const result = await services.operation(environmentId, {
      action: "operationRead",
      input: {
        environmentId,
        operationIds: [operation.operationId],
        ...(checkLauncher ? { refreshInstallOutcome: operation.operationId } : {}),
      },
    });
    receipt = exactReadReceipt(result, operation.operationId);
  } catch (error) {
    if (!explain) throw error;
    return checkLauncher
      ? `Read status on ${label}. The launcher query had no confirmed reply.`
      : `Read status on ${label}. The exact receipt request failed.`;
  }
  if (wantsLauncherCheck && !checkLauncher && !archived && receipt !== null) {
    // Keep the current-plan feedback when the launcher capability changed during a Check.
    if (
      canWrite() &&
      hasScope() &&
      validateReceipt(input, receipt) &&
      (operation.receipt === undefined || validateReceipt(operation.receipt, receipt))
    )
      await save({ kind: "receipt", receipt });
    return `Requested the retained update receipt from ${label}. Checking its launcher requires fresh status and permission.`;
  }
  if (receipt === null)
    return explain
      ? `Read status on ${label}. ${checkLauncher ? "The launcher query returned no exact receipt." : "The host returned no exact receipt for this request."}`
      : undefined;
  if (
    !validateReceipt(input, receipt) ||
    (operation.receipt !== undefined && !validateReceipt(operation.receipt, receipt))
  )
    return explain
      ? `Read status on ${label}. The receipt did not match this operation.`
      : undefined;
  if (!canWrite() || !hasScope())
    return explain
      ? `Read the exact receipt on ${label}. Saving it requires plan access and permission.`
      : undefined;
  const problem = await save({ kind: "receipt", receipt });
  if (!explain) return undefined;
  if (problem !== undefined) return problem;
  if (archived) {
    if (checkLauncher)
      return `Checked the launcher on ${label} and saved the exact receipt. ${hasUnconfirmedLauncherOutcome(receipt) ? "The install outcome is still unconfirmed." : "See the original attempt below."}`;
    return `Saved the exact receipt from ${label}. ${wantsLauncherCheck ? "Checking its launcher requires fresh status and permission." : "See the original attempt below."}`;
  }
  return undefined;
}

export type PanelActionTarget = PlanTarget & {
  readonly operationId?: string | null;
  readonly laptopEnvironmentId?: EnvironmentId;
  readonly stagedHandle?: string;
  readonly preview?: { readonly sourceSha: string; readonly sourceTree: string };
};

async function checkPanel(
  binding: InstallPlanBinding,
  environmentId: EnvironmentId | "laptop",
  target?: PanelActionTarget,
) {
  const { services, store } = binding;
  const previous = store.get();
  const selection = store.historySelection();
  const stillShowing = () =>
    installPlanServices() === binding && store.historySelection() === selection;
  let read: JonesInstallPlanResult | undefined;
  let planReadError: string | undefined;
  try {
    read =
      target === undefined || target.preview !== undefined
        ? await requestInstallPlan({ type: "read" }, binding)
        : (await readTargetPlan(binding, target)).result;
  } catch {
    planReadError = "Install status could not be read from Laptop.";
  }
  if (installPlanServices() !== binding) return;
  const checked =
    read === undefined
      ? null
      : target === undefined
        ? (read.state.current ?? read.state.last)
        : target.preview === undefined
          ? exactPlan(read, target.planId, target.archived === true)
          : null;
  planReadError ??= read === undefined ? undefined : nativePlanIssue(read);
  if (target !== undefined && target.preview === undefined && checked === null)
    planReadError ??= "This saved update plan could not be read. Refresh Install status.";
  const feedback = (error: string) => {
    if (stillShowing()) store.update({ error });
  };

  if (environmentId === "laptop") {
    const desktop = await services.bridge.getUpdateState();
    if (!stillShowing()) return;
    store.update({ desktop });
    if (planReadError !== undefined) return feedback(planReadError);
    const latest = store.get().native ?? read?.state;
    if (latest === undefined)
      return feedback("Refreshed Laptop. Saved install status is unavailable.");
    const selectedHistory = selectedHistoryPlan(store.get());
    const checkedLaptop =
      checked === null
        ? null
        : latest.current?.planId === checked.planId
          ? latest.current
          : (archivedPlanFrom(latest, checked.planId) ??
            (selectedHistory?.planId === checked.planId ? selectedHistory : checked));
    const previousPlan = [
      previous.native?.current,
      previous.native?.last,
      previous.historyPlan,
    ].find((candidate) => candidate?.planId === checkedLaptop?.planId);
    const gateIssue = checkedLaptop === null ? undefined : laptopGateIssue(checkedLaptop.laptop);
    const operation = checkedLaptop?.laptop.operations.at(-1);
    let message = "Refreshed Laptop.";
    if (laptopInstallNeedsReopen(checkedLaptop, latest.laptop))
      message = `Checked Laptop. ${LAPTOP_REOPEN_TO_CHECK_INSTALL}`;
    else if (gateIssue !== undefined && !target?.archived)
      message = `Checked Laptop. ${gateIssue.reason}`;
    else if (
      previousPlan?.laptop.gatePause === true &&
      checkedLaptop?.laptop.gate !== undefined &&
      !target?.archived
    )
      message = "Checked Laptop. Verification is restored. Other devices can continue.";
    else if (
      operation !== undefined &&
      (operation.result === undefined || operation.result.status === "held")
    )
      message = "Checked Laptop. The original update outcome is still unconfirmed.";
    else if (operation?.step === "install" && operation.result?.status === "rolled-back")
      message = "Checked Laptop. The original install receipt records a rollback.";
    else if (operation?.step === "install" && operation.result?.status === "committed")
      message =
        checkedLaptop?.laptop.gate === undefined && !target?.archived
          ? "Checked Laptop. The install committed. Verification is still pending."
          : target?.archived
            ? "Checked Laptop. The original install receipt records a commit."
            : "Checked Laptop. The selected build is verified.";
    feedback(message);
    return;
  }

  services.kick?.(environmentId);
  const update = (observed: InstallStatusDevice) => {
    if (installPlanServices() !== binding) return;
    store.update({
      devices: store.get().devices.map((d) => (d.environmentId === environmentId ? observed : d)),
    });
  };
  const snapshot = await services.observe(environmentId);
  const direct = async () => {
    if (services.readStatus === undefined) return snapshot;
    const state = await services.readStatus(environmentId);
    if (state?.environmentId !== environmentId)
      throw new Error("Could not read current status for this device.");
    return {
      ...snapshot,
      directStatus: true as const,
      observation: { state, freshness: "fresh" as const, heardAt: services.now() },
    };
  };
  let observed;
  try {
    observed = await direct();
  } catch {
    update(snapshot);
    feedback(
      planReadError ?? `Could not refresh ${snapshot.label}. Check its connection and try again.`,
    );
    return;
  }
  update(observed);
  if (!stillShowing()) return;
  let allowReport = planReadError === undefined;
  try {
    requireWritablePlan(binding, read);
  } catch {
    allowReport = false;
  }
  const message =
    checked === null
      ? undefined
      : await refreshDeviceEvidence(binding, environmentId, checked, observed, {
          refreshInstallOutcome: allowReport,
          allowReport,
          explain: true,
          ...(target?.archived ? { archived: true } : {}),
          ...(target?.operationId === undefined ? {} : { operationId: target.operationId }),
        });
  if (checked !== null && services.readStatus !== undefined && stillShowing()) {
    await direct()
      .then(update)
      .catch(() => {});
  }
  const latestState = store.get().native;
  const archivedDetail = store.get().historyPlan;
  const latestPlan =
    checked === null
      ? null
      : latestState?.current?.planId === checked.planId
        ? latestState.current
        : (archivedPlanFrom(latestState ?? null, checked.planId) ??
          (archivedDetail?.planId === checked.planId ? archivedDetail : null));
  const latestOperation = latestPlan?.devices
    .find((item) => item.environmentId === environmentId)
    ?.operations.find(operationOutstanding);
  const progress = store.get().devices.find((d) => d.environmentId === environmentId)?.observation
    .state?.progress;
  const installUnconfirmed =
    latestOperation?.step === "install" &&
    (hasUnconfirmedLauncherOutcome(latestOperation.receipt) ||
      (progress?.operationId === latestOperation.operationId &&
        progress.error?.code === "outcome-unavailable"));
  feedback(
    planReadError ??
      message ??
      (observed.observation.freshness !== "fresh"
        ? `Could not refresh ${observed.label}. Check its connection and try again.`
        : latestOperation === undefined
          ? `Refreshed ${observed.label}.`
          : installUnconfirmed
            ? `Checked ${observed.label}. Its install outcome is still unconfirmed.`
            : `Checked ${observed.label}. Its update outcome is still pending.`),
  );
}

/** One native admission per click, scoped to the plan and row that initiated it. */
export async function performPanelAction(
  environmentId: EnvironmentId | "laptop",
  action: PanelAction,
  target?: PanelActionTarget,
  binding = installPlanServices(),
) {
  if (binding === undefined) throw new Error("Install status is not connected.");
  if (action === "check") return checkPanel(binding, environmentId, target);
  if (target === undefined) throw new Error("Refresh the displayed update plan.");
  const { services, store } = binding;
  if (environmentId === "laptop") {
    if (action === "abandon-stage") return abandonLaptopStage(binding, target);
    const desktop = await services.bridge.getUpdateState();
    const desktopTarget = targetForDesktop(desktop);
    const { result, plan } =
      target.preview === undefined
        ? await readTargetPlan(binding, target)
        : { result: await requestInstallPlan({ type: "read" }, binding), plan: null };
    requireWritablePlan(binding, result);
    const intended = target.preview ?? plan?.target;
    if (
      target.archived ||
      intended === undefined ||
      (target.laptopEnvironmentId !== undefined &&
        result.state.laptop.environmentId !== target.laptopEnvironmentId) ||
      (target.preview !== undefined
        ? result.state.current !== null
        : plan === null || plan.revision !== target.expectedRevision) ||
      (target.operationId !== undefined &&
        (plan?.laptop.operations.at(-1)?.operationId ?? null) !== target.operationId) ||
      (action !== "check-build" &&
        (desktopTarget === undefined ||
          desktopTarget.sourceSha !== intended.sourceSha ||
          desktopTarget.sourceTree !== intended.sourceTree)) ||
      (action === "install" && desktop.jones?.stagedHandle !== target.stagedHandle)
    )
      throw new Error(
        "The Laptop's displayed plan or staged build changed. Refresh Install status.",
      );
    const routeTarget = {
      planId: target.preview === undefined ? target.planId : null,
      environmentId: result.state.laptop.environmentId,
      expectedRevision: plan?.revision,
      binding,
    };
    if (action === "download") {
      await routeLaptopDownload(services.bridge, desktop, undefined, routeTarget);
      return;
    }
    if (action === "install") {
      await routeLaptopInstall(services.bridge, desktop, undefined, undefined, routeTarget);
      return;
    }
    if (action === "check-build" && services.bridge.checkForUpdate !== undefined) {
      const checked = await services.bridge.checkForUpdate();
      if (installPlanServices() === binding) store.update({ desktop: checked.state });
      return;
    }
    throw new Error("That Laptop action is unavailable.");
  }

  if (target.archived || target.preview !== undefined)
    throw new Error("Select the current plan to change the queue.");
  const { plan, state } = await readWritableTarget(binding, target);
  const device = plan.devices.find((d) => d.environmentId === environmentId);
  if (
    device?.membership !== "selected" ||
    device.abandoning ||
    (target.operationId !== undefined &&
      target.operationId !== (latestOperation(device, false)?.operationId ?? null))
  )
    throw new Error("The displayed device or attempt changed. Refresh Install status.");
  if (action === "remove" || action === "end") {
    const result = await requestInstallPlan(
      action === "remove"
        ? {
            type: "edit",
            planId: target.planId,
            expectedRevision: plan.revision,
            select: [],
            deselect: [environmentId],
          }
        : { type: "end", planId: target.planId, expectedRevision: plan.revision, environmentId },
      binding,
    );
    const issue = nativePlanIssue(result);
    if (issue !== undefined) throw new Error(issue);
    const applied = result.applied?.find((d) => d.environmentId === environmentId);
    if (applied?.result === "locked-started")
      throw new Error(
        "This device started before removal, so it stays in. Open it to stop the download.",
      );
    return;
  }
  if (action !== "download" && action !== "install")
    throw new Error("This action is only available on Laptop.");
  const observed = await services.observe(environmentId);
  const host = executorHostFor(binding, device, observed, target.planId);
  if (
    authorizedUpdateStatus(observed, environmentId)?.capability.operations !== 1 ||
    !hasCurrentPlanEnrollment(services, device, observed) ||
    (action === "install" &&
      host.readLatest().observation.state?.stagedHandle !== target.stagedHandle)
  )
    throw new Error("Fresh operation capability and the displayed staged build are required.");
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
  const nativeReview = proposed.review;
  if (
    nativeReview !== undefined &&
    (!isNativeJonesInstallReview(nativeReview) || nativeReview.stagedHandle !== target.stagedHandle)
  )
    throw new Error("The displayed stage changed or its native review is unavailable.");
  await readWritableTarget(binding, { ...target, expectedRevision: plan.revision });
  const latest = currentAuthorizedUpdateStatus(services, environmentId);
  if (
    latest?.capability.operations !== 1 ||
    !hasCurrentPlanEnrollment(services, device, observed) ||
    (action === "install" && latest.stagedHandle !== target.stagedHandle)
  )
    throw new Error(
      "Permission, capability, or the staged build changed. Review the device again.",
    );
  const result = await requestInstallPlan(
    {
      type: "claim",
      planId: target.planId,
      environmentId,
      claimant: `window:${claimantId()}`,
      step: action,
      explicit: true,
      action: proposed.action,
      ...revisionFor(state, plan),
      ...(nativeReview === undefined ? {} : { review: nativeReview }),
      ...(latest.installedSource === undefined
        ? {}
        : { expectedInstalledSource: latest.installedSource.sha }),
    },
    binding,
  );
  requireWritablePlan(binding, result);
  const grant = result.grant;
  if (grant?.planId !== target.planId || grant.step !== action)
    throw new Error("The native grant does not match this attempt.");
  const executed = await executeJonesAction({
    environmentId,
    targetSource: plan.target.sourceSha,
    operationId: grant.operationId,
    action: proposed.action,
    host: { ...host, planGate: async () => ({ kind: "grant", grant }) },
    now: services.now,
    ...(nativeReview === undefined ? {} : { review: nativeReview }),
  });
  if (executed.kind !== "accepted") throw new Error(executed.message);
}

async function abandonLaptopStage(binding: InstallPlanBinding, target: PanelActionTarget) {
  if (target.operationId == null || target.stagedHandle === undefined)
    throw new Error("Review the exact Laptop stage again.");
  const { services } = binding;
  const verify = async () => {
    const desktop = target.archived ? undefined : await services.bridge.getUpdateState();
    const read = await readWritableTarget(binding, target);
    const { plan, state } = read;
    const download = plan.laptop.operations.findLast((o) => o.step === "download");
    const pending = plan.laptop.stageAbandonment;
    if (
      (!target.archived && state.view.deviceRecovery !== 1) ||
      state.laptop.environmentId !== plan.laptop.environmentId ||
      (target.laptopEnvironmentId !== undefined &&
        target.laptopEnvironmentId !== plan.laptop.environmentId) ||
      (target.archived
        ? state.view.recoveryHistory !== 1 ||
          pending === undefined ||
          pending.status === "retired" ||
          pending.operationId !== target.operationId ||
          pending.handle !== target.stagedHandle
        : download === undefined ||
          download.operationId !== target.operationId ||
          download.createdStagedHandle !== target.stagedHandle ||
          desktop?.jones?.stagedHandle !== target.stagedHandle)
    )
      throw new Error("The Laptop staged selection changed. Review the exact stage again.");
    return read;
  };
  await verify();
  if (
    !(await services.confirm(
      target.archived
        ? "Resume the recorded Laptop stage retirement for this plan?"
        : "Abandon the Laptop's staged update? This discards the reviewed downloaded selection.",
    ))
  )
    return;
  const { plan, state } = await verify();
  const result = await requestInstallPlan(
    {
      type: "laptop",
      planId: target.planId,
      action: "abandon-stage",
      ...(state.view.recoveryHistory === 1
        ? { operationId: target.operationId, expectedRevision: plan.revision }
        : {}),
    },
    binding,
  );
  const issue = nativePlanIssue(result);
  if (issue !== undefined) throw new Error(issue);
  if (target.archived) await readTargetPlan(binding, target);
}
