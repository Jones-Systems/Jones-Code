import { msg } from "gt-react";
import { m } from "../../i18n/messages";
import type { EnvironmentId } from "@t3tools/contracts";
import type { JonesOperationReceipt } from "@t3tools/contracts/jones/jonesOperations";
import {
  isJonesOperationNotSentError,
  JonesOperationNotSentError,
} from "@t3tools/client-runtime/jones/updates";
import type {
  JonesInstallPlan,
  JonesInstallPlanCommandReceipt,
  JonesInstallPlanDevice,
  JonesInstallPlanHostOperation,
  JonesInstallPlanRequest,
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
  allowsOperationRecovery,
  canReplayDirective,
  canFenceExpiredOperation,
  deviceActiveOperations,
  hasUnconfirmedLauncherOutcome,
  laptopGateIssue,
  laptopInstallNeedsReopen,
  operationHasUnverifiedUnsupported,
  operationOutstanding,
  orphanReceiptCheck,
  sameDirectiveBinding,
  sameOperationBinding,
  supportsOperationReceiptReads,
  validateDirectiveEvidence,
  validateDirectiveFenceEvidence,
  validateOperationEvidence,
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
  claimAdmission,
  currentAuthorizedUpdateStatus,
  currentPlanHostAuthority,
  exactFleetEnrollment,
  exactOperationRequest,
  exactPlan,
  guardPlanStart,
  hasCurrentPlanEnrollment,
  hasOperationReadScope,
  hasPlanEnrollment,
  installPlanServices,
  issuedDevice,
  nativePlanIssue,
  planEnrollmentFor,
  planStartIssue,
  readTargetPlan,
  readWritableTarget,
  requestInstallPlan,
  requireWritablePlan,
  revisionFor,
  supportsSetupFence,
  type InstallPlanBinding,
  type PlanTarget,
} from "./services";
import type { InstallStatusDevice } from "./panelUi";
import { archivedPlanFrom, selectedHistoryPlan } from "./panelUi";
import { LAPTOP_REOPEN_TO_CHECK_INSTALL, operationAdmissionMessage } from "./copy";
import { reconcileNativeJonesDuty } from "./executionJournal";

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
): JonesExecutorHost {
  const { services } = binding;
  const { environmentId } = device;
  return {
    readLatest: () => {
      const device = services.devices().find((d) => d.environmentId === environmentId);
      return device ?? { observation: { state: null, freshness: "loading" }, canOperate: false };
    },
    legacy: (request) => services.legacy(environmentId, request),
    start: async () => {
      throw new Error("A remote update needs its exact native grant.");
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

export function latestPanelOperation(device: JonesInstallPlanDevice, archived: boolean) {
  const operations = archived ? device.operations : deviceActiveOperations(device);
  return operations.find(operationOutstanding) ?? operations.at(-1);
}

export function canFenceArchivedOperation(operation: JonesInstallPlanHostOperation, now: number) {
  return (
    (operation.request.kind === "plan-directive"
      ? operationOutstanding(operation)
      : canFenceExpiredOperation(operation, now)) &&
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

export function heldCleanupReceipt(operation: JonesInstallPlanHostOperation | undefined) {
  const receipt = operation?.receipt;
  return receipt?.operationId === operation?.operationId &&
    receipt?.phase === "held" &&
    receipt.code === "cleanup-unconfirmed" &&
    receipt.jobId !== undefined &&
    receipt.action.kind !== "install"
    ? { receipt, jobId: receipt.jobId }
    : undefined;
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
  const operation =
    device === undefined
      ? undefined
      : (archived ? device.operations : deviceActiveOperations(device)).find(
          (candidate) => candidate.operationId === operationId,
        );
  const original =
    operation === undefined ? undefined : exactOperationRequest(planId, environmentId, operation);
  const directive =
    archived && step === "fence" && operation?.request.kind === "plan-directive"
      ? operation.request.input
      : undefined;
  if (
    device === undefined ||
    operation?.operationId !== operationId ||
    (original === undefined && directive === undefined) ||
    (directive !== undefined &&
      (state.view.directiveFence !== 1 ||
        device.route !== "follower-operations" ||
        directive.directiveId !== operationId ||
        directive.environmentId !== environmentId ||
        directive.enrollmentId !== device.enrollmentId ||
        directive.repository !== plan.target.repository ||
        directive.targetSource !== plan.target.sourceSha ||
        directive.targetTree !== plan.target.sourceTree ||
        !supportsSetupFence(environmentId, directive.enrollmentId, observed.fleetStatus))) ||
    (device.membership !== "selected" && (!archived || original === undefined)) ||
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
  const recheckHost = (observation = observed) => {
    const authority = currentPlanHostAuthority(binding, device);
    if (step === "fence" && original?.admission !== undefined && authority.boundAdmission !== 1)
      throw new Error(operationAdmissionMessage("admission-binding-required"));
    if (
      authorizedUpdateStatus(observation, environmentId)?.capability.operations !== 1 ||
      !authority.canRecoverOperation ||
      (directive === undefined
        ? !allowsOperationRecovery(planEnrollmentFor(device, observation), step) ||
          !allowsOperationRecovery(authority.enrollment, step)
        : !authority.canFenceSetup ||
          !supportsSetupFence(environmentId, directive.enrollmentId, observation.fleetStatus)) ||
      (!archived && authority.status?.operation?.operationId !== operationId)
    )
      throw new Error("Fresh authorized operation capability is required.");
  };
  recheckHost();
  const claimant = `window:${claimantId()}`;
  const claimed = await requestInstallPlan(
    {
      type: "claim",
      planId,
      environmentId,
      claimant,
      step,
      explicit: true,
      operationId,
      ...(directive === undefined ? {} : { enrollmentId: directive.enrollmentId }),
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
  const leaseLive = () => Date.parse(grant.leaseUntil) > services.now();
  const ownsFence = (item: JonesInstallPlanHostOperation) =>
    item.fence?.lease.claimant === claimant && item.fence.lease.leaseUntil === grant.leaseUntil;
  recheckHost();
  if (request.kind === "plan-directive-fence" && directive !== undefined) {
    if (
      !sameDirectiveBinding(directive, request.input) ||
      claimed.state.recoveryHistory?.find((entry) => entry.planId === planId)?.revision !==
        request.expectedRevision
    )
      throw new Error("The native fence grant does not match this setup request.");
    const readFence = async (expectedRevision?: number) => {
      const latest = await readWritableTarget(binding, {
        planId,
        archived: true,
        ...(expectedRevision === undefined ? {} : { expectedRevision }),
      });
      const member = issuedDevice(latest.plan, plan, device, true);
      const reserved = member?.operations.find((item) => item.operationId === operationId);
      return {
        plan: latest.plan,
        operation:
          latest.state.view.recoveryHistory === 1 &&
          latest.state.view.directiveFence === 1 &&
          reserved?.step === "directive" &&
          reserved.request.kind === "plan-directive" &&
          sameDirectiveBinding(request.input, reserved.request.input)
            ? reserved
            : undefined,
      };
    };
    const status = await services.host(environmentId, {
      action: "fenceDirective",
      input: request.input,
      beforeSend: async () => {
        const fresh = await services.observe(environmentId);
        const { operation: reserved } = await readFence(request.expectedRevision);
        recheckHost(fresh);
        return (
          reserved !== undefined &&
          operationOutstanding(reserved) &&
          ownsFence(reserved) &&
          leaseLive()
        );
      },
    });
    const proof = status.directiveFence;
    const reported = status.follower?.planDirective;
    const returnedEnrollment = exactFleetEnrollment(environmentId, status);
    if (
      !supportsSetupFence(environmentId, directive.enrollmentId, status) ||
      proof === undefined ||
      !validateDirectiveFenceEvidence(directive, proof) ||
      (reported?.directiveId === operationId &&
        (proof.outcome !== "applied" ||
          returnedEnrollment?.enrollmentId !== directive.enrollmentId ||
          returnedEnrollment.enabled !== true ||
          reported.issuedAt !== proof.directive.issuedAt ||
          !validateDirectiveFenceEvidence(directive, { ...proof, directive: reported })))
    )
      throw new Error("The host did not return an exact setup fence proof.");
    const exact = (recorded: JonesInstallPlanHostOperation | undefined) =>
      proof.outcome === "applied"
        ? recorded?.directive?.kind === "applied" &&
          recorded.directive.directive.issuedAt === proof.directive.issuedAt &&
          validateDirectiveFenceEvidence(directive, {
            ...proof,
            directive: recorded.directive.directive,
          })
        : recorded?.directiveFence?.outcome === proof.outcome;
    try {
      const latest = await readFence();
      if (exact(latest.operation)) return;
      if (
        !hasOperationReadScope(services, environmentId) ||
        latest.operation === undefined ||
        !operationOutstanding(latest.operation) ||
        !ownsFence(latest.operation)
      )
        throw new Error("The exact fence reservation or permission changed.");
      const result = await requestInstallPlan(
        {
          type: "report",
          planId,
          environmentId,
          operationId,
          evidence: { kind: "directive-fence", expectedRevision: latest.plan.revision, proof },
        },
        binding,
      );
      const issue = nativePlanIssue(result);
      if (issue !== undefined) throw new Error(issue);
      if (!exact((await readFence()).operation)) throw new Error("The proof was not read back.");
    } catch (error) {
      throw new Error(
        "The host returned exact setup proof. Saving it in recovery history is unconfirmed.",
        { cause: error },
      );
    }
    return;
  }
  if (original === undefined)
    throw new Error("The native grant does not match the original setup fence.");
  const readReservation = async () => {
    const read = await readTargetPlan(binding, target);
    requireWritablePlan(binding, read.result);
    const member = issuedDevice(read.plan, plan, device, archived);
    const reserved = (
      archived || member === undefined ? member?.operations : deviceActiveOperations(member)
    )?.find((item) => item.operationId === operationId);
    const input = reserved && exactOperationRequest(planId, environmentId, reserved);
    if (input === undefined || !sameOperationBinding(original, input)) return undefined;
    return reserved;
  };
  let receipt: JonesOperationReceipt;
  let ledgerRead: true | undefined;
  if (
    step === "stop" &&
    request.kind === "operation-stop" &&
    request.input.environmentId === environmentId &&
    request.input.operationId === operationId
  ) {
    const beforeSend = async () => {
      const reserved = await readReservation();
      recheckHost();
      return (
        reserved !== undefined &&
        canStopOperation(reserved) &&
        reserved.stop?.stopId === request.input.stopId &&
        reserved.stop.lease.claimant === claimant &&
        reserved.stop.lease.leaseUntil === grant.leaseUntil &&
        leaseLive()
      );
    };
    if (!(await beforeSend())) throw new Error(staleDownload);
    receipt = await services.stop(environmentId, {
      action: "operationStop",
      input: request.input,
      beforeSend,
    });
  } else if (
    step === "fence" &&
    request.kind === "operation-fence" &&
    sameOperationBinding(original, request.input)
  ) {
    let result;
    try {
      result = await services.operation(environmentId, {
        action: "operationFence",
        input: request.input,
        beforeSend: async () => {
          const reserved = await readReservation();
          recheckHost();
          return (
            reserved !== undefined &&
            canFenceExpiredOperation(reserved, services.now()) &&
            ownsFence(reserved) &&
            leaseLive()
          );
        },
      });
    } catch (error) {
      if (!isJonesOperationNotSentError(error)) throw error;
      recheckHost();
      throw new Error(
        m(msg("This plan's exact reservation changed or expired. Check the original operation.")),
        { cause: error },
      );
    }
    if (!("operationId" in result)) throw new Error("The fence has no exact operation receipt.");
    receipt = result;
    if (operationHasUnverifiedUnsupported(operation)) {
      if (!hasOperationReadScope(services, environmentId) || !validateReceipt(original, receipt))
        throw new Error("Check the exact operation receipt before settling this request.");
      const read = await services.operation(environmentId, {
        action: "operationRead",
        input: { environmentId, operationIds: [operationId] },
      });
      const saved = exactReadReceipt(read, operationId);
      if (saved === null)
        throw new Error(
          "The host has no confirmed receipt for the fence. Check the original operation.",
        );
      receipt = saved;
      ledgerRead = true;
    }
  } else
    throw new Error(
      step === "stop"
        ? "The native grant does not match this Stop request."
        : "The native grant does not match the original operation.",
    );
  if (
    !validateOperationEvidence(original, receipt, { ledgerRead: ledgerRead === true }) ||
    (operation.receipt !== undefined &&
      !validateReceipt(operation.receipt, receipt, { ledgerRead: ledgerRead === true }))
  )
    throw new Error(
      "The reply did not confirm the original operation. Check its exact receipt in Install status.",
    );
  const result = await (async () => {
    if (!hasOperationReadScope(services, environmentId))
      throw new Error("Read permission is required to record an operation receipt.");
    return requestInstallPlan(
      {
        type: "report",
        planId,
        environmentId,
        operationId,
        evidence: { kind: "receipt", receipt, ...(ledgerRead === true ? { ledgerRead } : {}) },
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

/** A lost V1 plan can supply only the original command identity and an exact retained host read. */
export async function checkOrphanedPlanCommand(
  displayed: JonesInstallPlanCommandReceipt,
  binding = installPlanServices(),
) {
  if (binding === undefined) throw new Error("Install status is not connected.");
  const { services } = binding;
  const commandId = displayed.command.commandId;
  const read = await requestInstallPlan({ type: "read", commandId }, binding);
  requireWritablePlan(binding, read);
  const orphan = read.state.orphanedCommands?.find(
    ({ command }) => command.commandId === commandId,
  );
  const check = orphan === undefined ? undefined : orphanReceiptCheck(orphan);
  if (
    read.state.view.orphanRecovery !== 1 ||
    check === undefined ||
    check.bodyHash !== displayed.bodyHash ||
    check.operationId !== displayed.operationId ||
    check.command.environmentId !== displayed.command.environmentId
  )
    throw new Error("The retained command changed or has no exact host receipt path.");
  const { environmentId } = check.command;
  const observed = await services.observe(environmentId);
  const state = observed.observation.state;
  if (
    observed.environmentId !== environmentId ||
    observed.observation.freshness !== "fresh" ||
    state?.environmentId !== environmentId ||
    !supportsOperationReceiptReads(state.capability) ||
    !hasOperationReadScope(services, environmentId)
  )
    throw new Error("An exact host receipt cannot be read. Refresh this device's connection.");
  requireWritablePlan(binding);
  const found = await services.operation(environmentId, {
    action: "operationRead",
    input: { environmentId, operationIds: [check.operationId] },
  });
  const receipt = exactReadReceipt(found, check.operationId);
  if (receipt === null)
    throw new Error(
      "The host has no retained receipt for this command. Recovery is still on hold.",
    );
  requireWritablePlan(binding);
  const result = await requestInstallPlan(
    { type: "reconcile-orphan", commandId, bodyHash: check.bodyHash, receipt },
    binding,
  );
  const issue = nativePlanIssue(result);
  if (issue !== undefined) throw new Error(issue);
  if (
    result.command?.command.commandId !== commandId ||
    result.command.bodyHash !== check.bodyHash ||
    result.command.operationId !== check.operationId ||
    result.command.outcome !== "terminal" ||
    result.state.orphanedCommands?.some(({ command }) => command.commandId === commandId)
  )
    throw new Error("Laptop could not confirm the retained command was settled.");
  try {
    await reconcileNativeJonesDuty(result);
  } catch {
    throw new Error(
      "Laptop saved the terminal receipt. This browser could not update its request history.",
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
  const held = heldCleanupReceipt(operation);
  const original =
    operation === undefined
      ? undefined
      : exactOperationRequest(plan.planId, environmentId, operation);
  if (
    (target.archived && state.view.recoveryHistory !== 1 && plan.cancelled !== true) ||
    device === undefined ||
    original === undefined ||
    held?.jobId !== jobId ||
    !validateReceipt(original, held.receipt) ||
    !allowsOperationRecovery(planEnrollmentFor(device, observed), "cleanup") ||
    !allowsOperationRecovery(currentPlanHostAuthority(binding, device).enrollment, "cleanup") ||
    authorizedUpdateStatus(observed, environmentId)?.capability.operations !== 1 ||
    currentAuthorizedUpdateStatus(binding.services, environmentId)?.capability.operations !== 1
  )
    return undefined;
  return { label: device.label, receipt: held.receipt };
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
  if (!hasOperationReadScope(binding.services, environmentId)) return unproved;
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
  let ledgerRead: true | undefined;
  if (!recovered(receipt)) {
    if (!hasOperationReadScope(services, environmentId)) return "unproved";
    const readback = await services
      .operation(environmentId, {
        action: "operationRead",
        input: { environmentId, operationIds: [operationId] },
      })
      .catch(() => undefined);
    receipt = readback === undefined ? null : exactReadReceipt(readback, operationId);
    ledgerRead = true;
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
        evidence: { kind: "receipt", receipt, ...(ledgerRead === true ? { ledgerRead } : {}) },
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

type DeviceEvidence = Extract<JonesInstallPlanRequest, { type: "report" }>["evidence"];

function reportDeviceEvidence(
  binding: InstallPlanBinding,
  planId: string,
  environmentId: EnvironmentId,
  operationId: string,
  evidence: DeviceEvidence,
) {
  return requestInstallPlan(
    { type: "report", planId, environmentId, operationId, evidence },
    binding,
  );
}

function canReadDeviceReceipts(
  binding: InstallPlanBinding,
  device: JonesInstallPlanDevice,
  observed: InstallStatusDevice,
) {
  return (
    observed.observation.freshness === "fresh" &&
    observed.observation.state?.environmentId === device.environmentId &&
    supportsOperationReceiptReads(observed.observation.state.capability) &&
    allowsOperationRecovery(planEnrollmentFor(device, observed), "read") &&
    allowsOperationRecovery(currentPlanHostAuthority(binding, device).enrollment, "read")
  );
}

function canReportDeviceEvidence(binding: InstallPlanBinding) {
  const { native, nativeRefusal } = binding.store.get();
  return (
    installPlanServices() === binding && native?.readOnly === undefined && nativeRefusal === null
  );
}

function exactLedgerEvidence(
  input: NonNullable<ReturnType<typeof exactOperationRequest>>,
  operation: JonesInstallPlanHostOperation,
  receipt: JonesOperationReceipt,
) {
  const evidence = { kind: "receipt", receipt, ledgerRead: true } as const;
  return validateOperationEvidence(input, receipt, evidence) &&
    (operation.receipt === undefined || validateReceipt(operation.receipt, receipt, evidence))
    ? evidence
    : undefined;
}

// A device retains at most sixteen operations. They fit in a single exact host receipt read.
const MAX_ARCHIVED_RECEIPTS_PER_POLL = 16;

export async function refreshArchivedDeviceEvidence(
  binding: InstallPlanBinding,
  environmentId: EnvironmentId,
  plan: JonesInstallPlan,
  observed: InstallStatusDevice,
  stillShowing: () => boolean,
) {
  const { services } = binding;
  const device = plan.devices.find((item) => item.environmentId === environmentId);
  if (device === undefined || !stillShowing()) return;
  const pending = device.operations.filter(operationOutstanding);
  const directive = pending.find(
    (item) =>
      item.request.kind === "plan-directive" &&
      item.operationId === observed.fleetStatus?.follower?.planDirective?.directiveId,
  );
  if (directive !== undefined)
    await refreshDeviceEvidence(binding, environmentId, plan, observed, {
      archived: true,
      refreshInstallOutcome: false,
      operationId: directive.operationId,
    });

  if (
    !stillShowing() ||
    !canReadDeviceReceipts(binding, device, observed) ||
    !hasOperationReadScope(services, environmentId)
  )
    return;
  const exact = pending
    .flatMap((operation) => {
      const input = exactOperationRequest(plan.planId, environmentId, operation);
      return input === undefined ? [] : [{ operation, input }];
    })
    .slice(0, MAX_ARCHIVED_RECEIPTS_PER_POLL);
  if (exact.length === 0) return;
  const result = await services.operation(environmentId, {
    action: "operationRead",
    input: { environmentId, operationIds: exact.map(({ input }) => input.operationId) },
  });
  if (!("receipts" in result)) return;
  let saved = false;
  for (const { operation, input } of exact) {
    if (
      !stillShowing() ||
      !canReportDeviceEvidence(binding) ||
      !hasOperationReadScope(services, environmentId)
    )
      break;
    const replies = result.receipts.filter((item) => item.operationId === input.operationId);
    const receipt = replies.length === 1 ? replies[0]?.receipt : undefined;
    const evidence = receipt == null ? undefined : exactLedgerEvidence(input, operation, receipt);
    if (evidence === undefined) continue;
    const report = await reportDeviceEvidence(
      binding,
      plan.planId,
      environmentId,
      input.operationId,
      evidence,
    );
    if (nativePlanIssue(report) === undefined) saved = true;
  }
  if (saved && stillShowing())
    await readTargetPlan(binding, { planId: plan.planId, archived: true });
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
      ? latestPanelOperation(device, archived)
      : device.operations.find((item) => item.operationId === options.operationId);
  if (operation === undefined) return undefined;
  const canWrite = () => options.allowReport !== false && canReportDeviceEvidence(binding);
  const authority = () => currentAuthorizedUpdateStatus(services, environmentId, observed);
  const canRead = () => hasOperationReadScope(services, environmentId);
  const hasReceiptAuthority = () => {
    const status = authority();
    return status !== undefined && supportsOperationReceiptReads(status.capability);
  };
  const save = async (evidence: DeviceEvidence) => {
    try {
      const report = await reportDeviceEvidence(
        binding,
        plan.planId,
        environmentId,
        operation.operationId,
        evidence,
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
    if (!operationOutstanding(operation)) return undefined;
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
    if (
      !canWrite() ||
      !hasReceiptAuthority() ||
      !hasCurrentPlanEnrollment(services, device, observed)
    )
      return explain
        ? `Read matching setup on ${label}. Saving it requires plan access and permission.`
        : undefined;
    const problem = await save({ kind: "directive", status: fleetStatus });
    return explain
      ? (problem ?? (archived ? `Saved the exact setup confirmation for ${label}.` : undefined))
      : undefined;
  }

  const input = exactOperationRequest(plan.planId, environmentId, operation);
  if (input === undefined || !canReadDeviceReceipts(binding, device, observed))
    return explain
      ? `Could not read the exact update receipt on ${label}. Refresh its status and try again.`
      : undefined;
  const wantsLauncherCheck =
    options.refreshInstallOutcome &&
    input.action.kind === "install" &&
    operationOutstanding(operation);
  if (!canRead())
    return explain
      ? m(msg("Read permission is required to check the exact receipt on {label}."), { label })
      : undefined;
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
  if (receipt === null)
    return explain
      ? `Read status on ${label}. ${checkLauncher ? "The launcher query returned no exact receipt." : "The host returned no exact receipt for this request."}`
      : undefined;
  const evidence = exactLedgerEvidence(input, operation, receipt);
  if (evidence === undefined)
    return explain
      ? `Read status on ${label}. The receipt did not match this operation.`
      : undefined;
  if (!canWrite() || !canRead())
    return explain
      ? `Read the exact receipt on ${label}. Saving it requires plan access and permission.`
      : undefined;
  const problem = await save(evidence);
  if (!explain) return undefined;
  if (problem !== undefined) return problem;
  if (archived) {
    if (checkLauncher)
      return `Checked the launcher on ${label} and saved the exact receipt. ${hasUnconfirmedLauncherOutcome(receipt) ? "The install outcome is still unconfirmed." : "See the original attempt below."}`;
    return `Saved the exact receipt from ${label}. ${wantsLauncherCheck ? "Checking its launcher requires fresh status and permission." : "See the original attempt below."}`;
  }
  if (wantsLauncherCheck && !checkLauncher)
    return `Requested the retained update receipt from ${label}. Checking its launcher requires fresh status and permission.`;
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
          refreshInstallOutcome:
            allowReport &&
            (target?.expectedRevision === undefined ||
              target.expectedRevision === checked.revision),
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
  const archivedDetail = selectedHistoryPlan(store.get());
  const last = checked === null ? null : archivedPlanFrom(latestState ?? null, checked.planId);
  const latestPlan =
    checked === null
      ? null
      : latestState?.current?.planId === checked.planId
        ? latestState.current
        : archivedDetail?.planId === checked.planId &&
            archivedDetail.revision >= (last?.revision ?? 0)
          ? archivedDetail
          : last;
  const latestOperation = latestPlan?.devices
    .find((item) => item.environmentId === environmentId)
    ?.operations.find(
      (operation) =>
        operationOutstanding(operation) &&
        (target?.operationId === undefined || operation.operationId === target.operationId),
    );
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
  const { plan } = await readWritableTarget(binding, target);
  const device = plan.devices.find((d) => d.environmentId === environmentId);
  if (
    device?.membership !== "selected" ||
    device.abandoning ||
    (target.operationId !== undefined &&
      target.operationId !== (latestPanelOperation(device, false)?.operationId ?? null))
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
  const host = executorHostFor(binding, device);
  const authority = () => currentPlanHostAuthority(binding, device);
  const initial = authority();
  if (initial.enrollment === "route-changed")
    throw new Error(operationAdmissionMessage("admission-route-changed"));
  if (
    authorizedUpdateStatus(observed, environmentId)?.capability.operations !== 1 ||
    !initial.canDispatch ||
    !hasPlanEnrollment(device, observed) ||
    !allowsOperationRecovery(initial.enrollment, "start") ||
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
  const refreshed = await readWritableTarget(binding, {
    ...target,
    expectedRevision: plan.revision,
  });
  const live = authority();
  const latest = live.status;
  if (live.enrollment === "route-changed")
    throw new Error(operationAdmissionMessage("admission-route-changed"));
  if (
    !live.canDispatch ||
    !allowsOperationRecovery(live.enrollment, "start") ||
    latest?.installedSource === undefined ||
    (action === "install" && latest.stagedHandle !== target.stagedHandle)
  )
    throw new Error(
      "Permission, capability, or the staged build changed. Review the device again.",
    );
  const claim = {
    type: "claim" as const,
    planId: target.planId,
    environmentId,
    claimant: `window:${claimantId()}`,
    step: action,
    explicit: true,
    action: proposed.action,
    ...revisionFor(refreshed.state, plan),
    ...claimAdmission(refreshed.state, live),
    ...(nativeReview === undefined ? {} : { review: nativeReview }),
    expectedInstalledSource: latest.installedSource.sha,
  };
  const result = await requestInstallPlan(claim, binding);
  requireWritablePlan(binding, result);
  const grant = result.grant;
  if (
    grant?.planId !== target.planId ||
    grant.step !== action ||
    grant.request.kind !== "operation-start"
  )
    throw new Error("The native grant does not match this attempt.");
  const grantedInput = grant.request.input;
  const issue = () => planStartIssue(binding, plan, device, grant, claim, grantedInput);
  const initialIssue = issue();
  if (initialIssue !== undefined) throw new Error(initialIssue);
  const executed = await executeJonesAction({
    environmentId,
    targetSource: plan.target.sourceSha,
    operationId: grant.operationId,
    action: proposed.action,
    host: {
      ...host,
      planGate: async () => ({ kind: "grant", grant }),
      start: async (input, beforeSend) => {
        const currentIssue = planStartIssue(binding, plan, device, grant, claim, input);
        if (currentIssue !== undefined) throw new JonesOperationNotSentError({});
        const receipt = await services.operation(environmentId, {
          action: "operationStart",
          input,
          beforeSend: guardPlanStart(binding, issue, beforeSend),
        });
        if (!("operationId" in receipt))
          throw new Error("The host did not return an operation receipt.");
        if (
          hasOperationReadScope(services, environmentId) &&
          validateOperationEvidence(input, receipt)
        )
          await requestInstallPlan(
            {
              type: "report",
              planId: plan.planId,
              environmentId,
              operationId: input.operationId,
              evidence: { kind: "receipt", receipt },
            },
            binding,
          );
        return receipt;
      },
    },
    now: services.now,
    ...(nativeReview === undefined ? {} : { review: nativeReview }),
  });
  if (executed.kind !== "accepted")
    throw new Error(
      operationAdmissionMessage(executed.code) ??
        (executed.code === "not-sent"
          ? (issue() ?? "Nothing was sent. Check the original reservation.")
          : executed.message),
    );
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
