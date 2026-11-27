import { randomUUID } from "../../lib/utils";
import type {
  DesktopBridge,
  DesktopUpdateActionResult,
  DesktopUpdateState,
  EnvironmentId,
} from "@t3tools/contracts";
import type {
  JonesInstallPlanOrigin,
  JonesInstallPlanReview,
  JonesInstallPlanSelection,
  JonesInstallPlanState,
  JonesInstallPlanTarget,
} from "@t3tools/contracts/jones/installPlan";
import type {
  JonesOperationAction,
  JonesOperationStartInput,
} from "@t3tools/contracts/jones/jonesOperations";
import type { JonesUpdateProvenance } from "@t3tools/contracts/jones/jonesUpdates";
import type { JonesOperationBeforeSend } from "@t3tools/client-runtime/jones/updates";
import type {
  JonesExecutorHost,
  JonesPlanGateResult,
  JonesReviewedInstall,
} from "@t3tools/client-runtime/jones/updates/executor";
import {
  prepareReviewedInstall,
  isNativeJonesInstallReview,
} from "@t3tools/client-runtime/jones/updates/executor";
import type { NativeJonesDispatch } from "./executionJournal";
import {
  deviceHasRecoveryDuty,
  planHasLaptopRecoveryDuty,
  selectability,
  supportsInstallPlanMutations,
} from "@t3tools/client-runtime/jones/install-plan";
import {
  installStatusStore,
  type InstallStatusDevice,
  type InstallStatusSnapshot,
} from "./panelUi";
import {
  authorizedUpdateStatus,
  claimAdmission,
  currentPlanHostAuthority,
  guardPlanStart,
  hasPlanEnrollment,
  installPlanServices,
  issuedDevice,
  nativePlanIssue,
  planStartIssue,
  requireWritablePlan,
  requestInstallPlan,
  revisionFor,
  type InstallPlanBinding,
} from "./services";
import { operationAdmissionMessage } from "./copy";

export function openInstallStatus(last = false) {
  (installPlanServices()?.store ?? installStatusStore).open(last);
}
export function targetForDesktop(
  state: DesktopUpdateState | null,
): JonesInstallPlanTarget | undefined {
  const p = state?.jones?.provenance;
  return p === undefined ||
    !/^[a-f0-9]{40}$/.test(p.sourceSha) ||
    !/^[a-f0-9]{40}$/.test(p.sourceTree)
    ? undefined
    : {
        repository: "Jones-Systems/Jones-Code",
        sourceSha: p.sourceSha,
        sourceTree: p.sourceTree,
        version:
          p.version ?? state!.downloadedVersion ?? state!.availableVersion ?? state!.currentVersion,
        ...(p.buildStartedAt === undefined ? {} : { buildStartedAt: p.buildStartedAt }),
      };
}
export function selectionFor(device: InstallStatusDevice): JonesInstallPlanSelection | undefined {
  const route = selectability(device.observation, device.fleetStatus);
  return route.selectable && device.canOperate
    ? {
        environmentId: device.environmentId,
        label: device.label,
        route: route.route,
        ...(route.enrollmentId === undefined ? {} : { enrollmentId: route.enrollmentId }),
      }
    : undefined;
}

export function desktopCanPreviewInstallPlan(desktop: DesktopUpdateState | null) {
  return (
    targetForDesktop(desktop) !== undefined &&
    [
      "available",
      "staged",
      "downloading",
      "verifying",
      "preparing",
      "installing",
      "blocked",
    ].includes(desktop?.jones?.phase ?? "")
  );
}

export function canOpenInstallStatusPill(
  native: InstallStatusSnapshot["native"],
  desktop: DesktopUpdateState | null,
  hasOtherUpdates: boolean,
) {
  if (typeof window === "undefined" || window.desktopBridge?.installPlan === undefined)
    return false;
  return (
    native?.current != null ||
    native?.readOnly !== undefined ||
    (native?.recoveryHistory?.length ?? 0) > 0 ||
    (native?.orphanedCommands?.length ?? 0) > 0 ||
    (!hasOtherUpdates && (native?.last != null || desktopCanPreviewInstallPlan(desktop)))
  );
}

/** This routing read precedes the pill's busy guard; opening status never dispatches work. */
export async function routeInstallStatusPill(
  bridge: Pick<DesktopBridge, "installPlan"> | undefined,
  desktop: DesktopUpdateState | null,
  hasOtherUpdates: boolean,
): Promise<"opened" | "last-and-check" | "legacy"> {
  if (bridge?.installPlan === undefined) return "legacy";
  const { result, store } = await readNativeBridge(bridge, desktop);
  const native = store.get().native ?? result.state;
  if (
    native.current !== null ||
    native.readOnly !== undefined ||
    (native.orphanedCommands?.length ?? 0) > 0 ||
    nativePlanIssue(result) !== undefined
  ) {
    store.open();
    return "opened";
  }
  if (!hasOtherUpdates && desktopCanPreviewInstallPlan(desktop)) {
    const source = targetForDesktop(desktop)!.sourceSha;
    if (store.get().previewTargetSource !== source)
      store.update({ previewTargetSource: source, previewSelection: [] });
    store.open();
    return "opened";
  }
  const history = native.recoveryHistory?.[0];
  if (history !== undefined) {
    store.open(true, history.planId);
    return "opened";
  }
  if (hasOtherUpdates) return "legacy";
  if (native.last !== null) {
    store.open(true);
    return "last-and-check";
  }
  return "legacy";
}

type LaptopRouteTarget = {
  readonly planId: string | null;
  readonly environmentId: EnvironmentId | undefined;
  readonly expectedRevision?: number | undefined;
  readonly binding?: InstallPlanBinding;
};

async function readNativeBridge(
  bridge: Pick<DesktopBridge, "installPlan">,
  desktop: DesktopUpdateState | null,
) {
  if (bridge.installPlan === undefined) throw new Error("Install plans are unsupported.");
  const binding = installPlanServices();
  const store = binding?.store ?? installStatusStore;
  if (binding?.services.bridge.installPlan === bridge.installPlan) {
    const result = await requestInstallPlan({ type: "read" }, binding);
    if (installPlanServices() === binding) store.update({ desktop });
    return { result, store, binding };
  }
  const result = await bridge.installPlan({ type: "read" });
  if (binding === undefined && installPlanServices() === undefined) {
    store.bindDesk(bridge.installPlan);
    store.update({ native: result.state, nativeRefusal: result.refused?.message ?? null, desktop });
  }
  return { result, store, binding: undefined };
}

function assertLaptopRoute(state: JonesInstallPlanState, target: LaptopRouteTarget | undefined) {
  if (target === undefined) return;
  if (
    (target.binding !== undefined && target.binding !== installPlanServices()) ||
    (state.current?.planId ?? null) !== target.planId ||
    state.laptop.environmentId !== target.environmentId ||
    (target.expectedRevision !== undefined && state.current?.revision !== target.expectedRevision)
  )
    throw new Error("The displayed Laptop plan changed. Review it again.");
}

async function routeLaptop(
  bridge: Pick<DesktopBridge, "installPlan">,
  desktop: DesktopUpdateState,
  action: "download" | "install",
  review?: JonesInstallPlanReview,
  origin: JonesInstallPlanOrigin = "settings",
  dispatch?: NativeJonesDispatch,
  expected?: LaptopRouteTarget,
): Promise<void> {
  if (bridge.installPlan === undefined) throw new Error("Install plans are unsupported.");
  const target = targetForDesktop(desktop);
  if (target === undefined) throw new Error("Check for a qualified Laptop build first.");
  const { result: read, store, binding } = await readNativeBridge(bridge, desktop);
  const problem = nativePlanIssue(read);
  if (problem !== undefined) throw new Error(problem);
  if (binding !== undefined) requireWritablePlan(binding, read);
  assertLaptopRoute(read.state, expected);
  const current = read.state.current;
  if (current?.target.sourceSha !== target.sourceSha) {
    const overlap =
      retainedRecoveryIssue(read.state, read.state.laptop.environmentId) ??
      store
        .get()
        .previewSelection.map((device) => retainedRecoveryIssue(read.state, device.environmentId))
        .find((issue) => issue !== undefined);
    if (overlap !== undefined) throw new Error(overlap);
  }
  const request =
    current?.target.sourceSha === target.sourceSha
      ? {
          type: "laptop" as const,
          planId: current.planId,
          action,
          ...revisionFor(read.state, current),
          ...(review === undefined ? {} : { review }),
        }
      : {
          type: "start" as const,
          origin,
          target,
          select: store.get().previewSelection,
          laptop: { action, ...(review === undefined ? {} : { review }) },
        };
  const result = await (dispatch === undefined
    ? binding === undefined
      ? bridge.installPlan(request)
      : requestInstallPlan(request, binding)
    : dispatch(request, read.state));
  store.update({
    native: result.state,
    error: result.refused?.message ?? null,
    showLast: false,
    ...(result.refused === undefined ? { previewSelection: [] } : {}),
  });
  store.open();
  const issue = nativePlanIssue(result);
  if (issue !== undefined) throw new Error(issue);
}
export async function routeLaptopDownload(
  bridge: Pick<DesktopBridge, "installPlan">,
  state: DesktopUpdateState,
  dispatch?: NativeJonesDispatch,
  expected?: LaptopRouteTarget,
) {
  await routeLaptop(bridge, state, "download", undefined, "settings", dispatch, expected);
  return { accepted: true, completed: false, state } satisfies DesktopUpdateActionResult;
}
export async function routeLaptopInstall(
  bridge: Pick<DesktopBridge, "installPlan">,
  state: DesktopUpdateState,
  review?: JonesReviewedInstall,
  dispatch?: NativeJonesDispatch,
  expected?: LaptopRouteTarget,
) {
  let reviewed = review;
  if (reviewed === undefined) {
    const binding = installPlanServices();
    if (binding === undefined || bridge.installPlan === undefined)
      throw new Error("The install review is unavailable. Open Install status.");
    const native = await requestInstallPlan({ type: "read" }, binding);
    requireWritablePlan(binding, native);
    assertLaptopRoute(native.state, expected);
    const environmentId = native.state.laptop.environmentId;
    const target = targetForDesktop(state);
    if (environmentId === undefined || target === undefined)
      throw new Error("Fresh Laptop identity is required for review.");
    expected ??= {
      binding,
      planId: native.state.current?.planId ?? null,
      environmentId,
      expectedRevision: native.state.current?.revision,
    };
    const reviewedStage = state.jones?.stagedHandle;
    let latest = await binding.services.bridge.getUpdateState();
    if (latest.jones?.stagedHandle !== reviewedStage)
      throw new Error("The Laptop staged build changed. Review it again.");
    const host: JonesExecutorHost = {
      readLatest: () => ({
        observation: {
          state:
            latest.jones === undefined
              ? null
              : {
                  ...latest.jones,
                  environmentId,
                  currentVersion: latest.jones.currentVersion ?? latest.currentVersion,
                },
          freshness: "fresh",
        },
        canOperate: true,
      }),
      legacy: async () => {
        throw new Error("Laptop installs use the native plan.");
      },
      start: async () => {
        throw new Error("Laptop installs use the native plan.");
      },
      confirm: async () => {
        const confirmed = await binding.services.confirm(
          `Install update${(latest.downloadedVersion ?? latest.availableVersion) ? ` ${latest.downloadedVersion ?? latest.availableVersion}` : ""} and restart Jones Code?\n\nAny running tasks will be interrupted. Make sure you are ready before continuing.`,
        );
        latest = await binding.services.bridge.getUpdateState();
        return confirmed;
      },
      preflight: async (input) => {
        const read = await requestInstallPlan({ type: "read" }, binding);
        requireWritablePlan(binding, read);
        assertLaptopRoute(read.state, expected);
        const result = await requestInstallPlan({ type: "recovery-preflight", input }, binding);
        if (result.preflight === undefined)
          throw new Error(result.refused?.message ?? "Recovery preflight is unavailable.");
        return result.preflight;
      },
      chooseRecovery: binding.services.chooseRecovery,
    };
    const result = await prepareReviewedInstall({
      environmentId,
      targetSource: target.sourceSha,
      label: "Laptop",
      host,
      now: binding.services.now,
    });
    if (result.kind !== "ready") throw new Error(result.message);
    reviewed = result.review;
  }
  if (!isNativeJonesInstallReview(reviewed))
    throw new Error("A native Laptop install requires its full native review.");
  await routeLaptop(bridge, state, "install", reviewed, "settings", dispatch, expected);
  return { accepted: true, completed: false, state } satisfies DesktopUpdateActionResult;
}

interface JonesHandoffTarget {
  readonly environmentId: EnvironmentId;
  readonly label: string;
}
interface JonesHandoffResult extends JonesHandoffTarget {
  readonly message: string;
  readonly handedOff: boolean;
}
export function partitionUpdateEverything<T extends { readonly environmentId: EnvironmentId }>(
  machines: readonly T[],
  isJones: (machine: T) => boolean,
) {
  return {
    jones: machines.filter(isJones),
    other: machines.filter((machine) => !isJones(machine)),
  };
}
export async function handOffJonesTargets(
  targets: readonly JonesHandoffTarget[],
  dispatch?: NativeJonesDispatch,
  reviewedBuild?: JonesUpdateProvenance,
): Promise<readonly JonesHandoffResult[]> {
  const binding = installPlanServices();
  if (binding?.services.bridge.installPlan === undefined) {
    if (reviewedBuild !== undefined) throw new Error("The native install plan is unavailable.");
    return targets.map((target) => ({
      ...target,
      handedOff: false,
      message: "Update from Connections after Laptop is verified: this client has no native plan.",
    }));
  }
  const read = await requestInstallPlan({ type: "read" }, binding);
  const issue = nativePlanIssue(read);
  if (issue !== undefined) {
    if (reviewedBuild !== undefined) throw new Error(issue);
    return targets.map((target) => ({ ...target, handedOff: false, message: issue }));
  }
  requireWritablePlan(binding, read);
  const desktop = await binding.services.bridge.getUpdateState();
  const target = read.state.current?.target ?? targetForDesktop(desktop);
  if (
    reviewedBuild !== undefined &&
    (read.state.current !== null ||
      target?.sourceSha !== reviewedBuild.sourceSha ||
      target.sourceTree !== reviewedBuild.sourceTree ||
      target.version !== reviewedBuild.version ||
      desktop.jones?.provenance?.artifactId !== reviewedBuild.artifactId ||
      desktop.jones.phase !== "available" ||
      !desktop.jones.capability.download)
  )
    throw new Error("The reviewed build or install plan changed. Open Updates and review again.");
  if (target === undefined)
    return targets.map((t) => ({
      ...t,
      handedOff: false,
      message: "Update from Connections after Laptop is verified: no qualified Laptop target.",
    }));
  const selections: JonesInstallPlanSelection[] = [];
  const results: JonesHandoffResult[] = [];
  const observations = await Promise.allSettled(
    targets.map((entry) => binding.services.observe(entry.environmentId)),
  );
  for (const [index, entry] of targets.entries()) {
    try {
      const overlap = retainedRecoveryIssue(read.state, entry.environmentId);
      if (overlap !== undefined) {
        results.push({ ...entry, handedOff: false, message: overlap });
        continue;
      }
      const observed = observations[index];
      if (observed?.status !== "fulfilled") throw new Error("Fresh host status unavailable.");
      const device = observed.value;
      if (
        device.observation.freshness === "fresh" &&
        device.observation.state?.installedSource?.sha === target.sourceSha
      ) {
        results.push({ ...entry, handedOff: false, message: `Already on ${target.version}` });
        continue;
      }
      const selection = selectionFor(device);
      if (selection === undefined) {
        const route = selectability(device.observation, device.fleetStatus);
        results.push({
          ...entry,
          handedOff: false,
          message: `Update from Connections after Laptop is verified: ${!device.canOperate ? "no permission" : route.selectable ? "unavailable" : route.reason}`,
        });
      } else {
        selections.push(selection);
        results.push({ ...entry, handedOff: true, message: "Handed to Install status" });
      }
    } catch {
      results.push({
        ...entry,
        handedOff: false,
        message: "Update from Connections after Laptop is verified: fresh host status unavailable.",
      });
    }
  }
  const current = read.state.current;
  if (selections.length === 0 && reviewedBuild === undefined) return results;
  requireWritablePlan(binding, read);
  if (current === null) {
    const overlap = retainedRecoveryIssue(read.state, read.state.laptop.environmentId);
    if (overlap !== undefined) throw new Error(overlap);
  }
  const request =
    current !== null
      ? {
          type: "edit" as const,
          planId: current.planId,
          expectedRevision: current.revision,
          select: selections,
          deselect: [],
        }
      : {
          type: "start" as const,
          origin: "update-everything" as const,
          target:
            reviewedBuild === undefined
              ? target
              : { ...target, laptopArtifactId: reviewedBuild.artifactId },
          select: selections,
          ...(desktop.jones?.phase === "available" && desktop.jones.capability.download
            ? { laptop: { action: "download" as const } }
            : {}),
        };
  const result = await (dispatch === undefined
    ? requestInstallPlan(request, binding)
    : dispatch(request, read.state));
  const nativeIssue = nativePlanIssue(result);
  if (reviewedBuild !== undefined && nativeIssue !== undefined) throw new Error(nativeIssue);
  binding.store.open();
  return results.map((entry) => {
    const applied = result.applied?.find((a) => a.environmentId === entry.environmentId);
    const refusal =
      nativeIssue ??
      (applied?.result === "not-selectable"
        ? (applied.reason ?? "Can't join this plan.")
        : undefined);
    return !entry.handedOff || refusal === undefined
      ? entry
      : {
          ...entry,
          handedOff: false,
          message: `Update from Connections after Laptop is verified: ${refusal}`,
        };
  });
}

function retainedRecoveryIssue(
  state: JonesInstallPlanState,
  environmentId: EnvironmentId | undefined,
) {
  if (environmentId === undefined) return undefined;
  const laptop = state.laptop.environmentId === environmentId;
  const older = state.last;
  const pending =
    state.orphanedCommands?.some(({ command }) => command.environmentId === environmentId) ||
    state.recoveryHistory?.some((entry) =>
      laptop
        ? entry.pendingLaptop
        : entry.pendingDevices.some((d) => d.environmentId === environmentId),
    ) ||
    (older !== null &&
      (laptop
        ? older.laptop.environmentId === environmentId && planHasLaptopRecoveryDuty(older)
        : older.devices.some(
            (d) => d.environmentId === environmentId && deviceHasRecoveryDuty(d),
          )));
  return pending
    ? "Review this device's outstanding work in Install status recovery history first."
    : undefined;
}

/** Legacy desktop hosts stay in Connections; the native gate read supplies their prerequisite. */
export type LegacyConnectionsAuthority =
  | { readonly kind: "without-native-plan" }
  | {
      readonly kind: "verified-plan";
      readonly binding: InstallPlanBinding;
      readonly planId: string;
      readonly revision: number;
    };

export async function readLegacyConnectionsAuthority(
  targetSource: string,
  environmentId: EnvironmentId,
): Promise<LegacyConnectionsAuthority | string> {
  const binding = installPlanServices();
  if (typeof window === "undefined" || window.desktopBridge?.installPlan === undefined)
    return { kind: "without-native-plan" };
  if (binding === undefined) return "Open Install status on Laptop first.";
  const result = await requestInstallPlan({ type: "read" }, binding);
  const problem = nativePlanIssue(result);
  if (problem !== undefined) return problem;
  try {
    requireWritablePlan(binding, result);
  } catch (error) {
    return error instanceof Error ? error.message : "Install status is unavailable.";
  }
  const overlap = retainedRecoveryIssue(result.state, environmentId);
  if (overlap !== undefined) return overlap;
  const plan = result.state.current ?? result.state.last;
  return plan?.target.sourceSha === targetSource &&
    plan.laptop.gate !== undefined &&
    plan.laptop.gateClosed === undefined &&
    !result.state.laptop.activationBlocked &&
    result.state.laptop.environmentId === plan.laptop.environmentId &&
    result.state.laptop.activeSource === targetSource &&
    plan.laptop.gate.activeSource === targetSource &&
    result.state.laptop.activeTree === plan.laptop.gate.activeTree &&
    result.state.laptop.activeGeneration === plan.laptop.gate.activeGeneration &&
    (plan.laptop.gate.verificationKind === "ordinary-recovery"
      ? result.state.laptop.committedTransactionId === undefined &&
        result.state.laptop.ordinaryRecovery !== undefined &&
        plan.laptop.gate.ordinaryRecovery?.transactionId === plan.laptop.gate.transactionId &&
        plan.laptop.gate.ordinaryRecovery.activeGeneration === plan.laptop.gate.activeGeneration &&
        result.state.laptop.ordinaryRecovery.transactionId ===
          `manual-${plan.laptop.gate.activeGeneration}` &&
        result.state.laptop.ordinaryRecovery.transactionId === plan.laptop.gate.transactionId &&
        result.state.laptop.ordinaryRecovery.activeGeneration ===
          plan.laptop.gate.activeGeneration &&
        result.state.laptop.ordinaryRecovery.manifestSha256 ===
          plan.laptop.gate.ordinaryRecovery?.manifestSha256 &&
        result.state.laptop.ordinaryRecovery.receiptSha256 ===
          plan.laptop.gate.ordinaryRecovery?.receiptSha256
      : result.state.laptop.ordinaryRecovery === undefined &&
        plan.laptop.gate.ordinaryRecovery === undefined &&
        result.state.laptop.committedTransactionId === plan.laptop.gate.transactionId)
    ? { kind: "verified-plan", binding, planId: plan.planId, revision: plan.revision }
    : "Update from Connections after Laptop is updated and verified for this source.";
}

export async function legacyConnectionsGate(
  targetSource: string,
  environmentId: EnvironmentId,
): Promise<string | null> {
  const authority = await readLegacyConnectionsAuthority(targetSource, environmentId);
  return typeof authority === "string" ? authority : null;
}
export function connectionsPlanDispatch(
  binding: ReturnType<typeof installPlanServices>,
  action: JonesOperationAction,
  review: JonesReviewedInstall | undefined,
  dispatch: (canClaim?: () => boolean) => NativeJonesDispatch,
  canOperate: () => boolean,
) {
  let startIssue: (request: JonesOperationStartInput) => string | undefined = () =>
    "Install was not granted.";
  const planGate: NonNullable<JonesExecutorHost["planGate"]> = async (
    environmentId,
    targetSource,
  ) => {
    if (binding === undefined || installPlanServices() !== binding)
      return { kind: "refused", message: "Open Install status on Laptop first." };
    const active = await requestInstallPlan({ type: "read" }, binding);
    const issue = nativePlanIssue(active) ?? retainedRecoveryIssue(active.state, environmentId);
    if (issue !== undefined) return { kind: "refused", message: issue };
    requireWritablePlan(binding, active);
    const desktopTarget = targetForDesktop(await binding.services.bridge.getUpdateState());
    if ((active.state.current?.target.sourceSha ?? desktopTarget?.sourceSha) !== targetSource)
      return {
        kind: "refused",
        message:
          "This source does not match the current Laptop plan. Finish or review that plan first.",
      };
    if (action.kind !== "install") {
      const device = binding.services.devices().find((d) => d.environmentId === environmentId);
      const results = await handOffJonesTargets(
        [{ environmentId, label: device?.label ?? "Device" }],
        dispatch(),
      );
      return results[0]?.handedOff
        ? { kind: "handled", message: results[0].message }
        : { kind: "refused", message: results[0]?.message ?? "The plan handoff was refused." };
    }
    const current = active.state.current;
    if (current?.target.sourceSha !== targetSource)
      return {
        kind: "refused",
        message: "Open this source's current plan to review installation.",
      };
    const device = current.devices.find((candidate) => candidate.environmentId === environmentId);
    if (
      device?.membership !== "selected" ||
      review === undefined ||
      !isNativeJonesInstallReview(review)
    )
      return {
        kind: "refused",
        message: "Fresh control authority and a native install review are required.",
      };
    const observed = await binding.services.observe(environmentId);
    const directStatus = authorizedUpdateStatus(observed, environmentId);
    const canClaim = () => {
      const plan = binding.store.get().native?.current;
      const latest = currentPlanHostAuthority(binding, device);
      return (
        canOperate() &&
        plan?.revision === current.revision &&
        issuedDevice(plan, current, device) !== undefined &&
        latest.canDispatch &&
        latest.enrollment === "match" &&
        latest.status?.installedSource?.sha === review.expectedInstalledSource &&
        latest.status?.stagedHandle === review.stagedHandle
      );
    };
    requireWritablePlan(binding, active);
    if (
      directStatus === undefined ||
      !supportsInstallPlanMutations(directStatus.capability) ||
      directStatus.installedSource?.sha !== review.expectedInstalledSource ||
      directStatus.stagedHandle !== review.stagedHandle ||
      !hasPlanEnrollment(device, observed) ||
      !canClaim()
    )
      return {
        kind: "refused",
        message:
          (currentPlanHostAuthority(binding, device).enrollment === "route-changed"
            ? operationAdmissionMessage("admission-route-changed")
            : undefined) ??
          "Fresh control authority and the selected device's enrollment are required.",
      };
    const claimant = `window:${claimantId()}`;
    const request = {
      type: "claim" as const,
      planId: current.planId,
      environmentId,
      claimant,
      step: "install" as const,
      explicit: true,
      action,
      review,
      ...revisionFor(active.state, current),
      ...claimAdmission(active.state, currentPlanHostAuthority(binding, device)),
      expectedInstalledSource: review.expectedInstalledSource,
    };
    const result = await dispatch(
      () =>
        canClaim() &&
        (request.boundAdmission !== 1 ||
          (binding.store.get().native?.view.boundAdmission === 1 &&
            currentPlanHostAuthority(binding, device).boundAdmission === 1)),
    )(request, active.state);
    const refusal = nativePlanIssue(result);
    const grant = result.grant;
    if (
      refusal !== undefined ||
      grant?.planId !== current.planId ||
      grant.environmentId !== environmentId ||
      grant.step !== "install" ||
      grant.request.kind !== "operation-start" ||
      grant.request.input.correlation?.planId !== current.planId ||
      grant.request.input.correlation.deviceKey !== environmentId
    )
      return { kind: "refused", message: refusal ?? "Install was not granted." };
    startIssue = (input) =>
      !canOperate()
        ? "This control no longer authorizes the action. Check the original operation."
        : planStartIssue(binding, current, device, grant, request, input);
    return { kind: "grant", grant };
  };
  return {
    planGate,
    startIssue: (request: JonesOperationStartInput) => startIssue(request),
    beforeStart: (request: JonesOperationStartInput, beforeSend?: JonesOperationBeforeSend) =>
      binding === undefined
        ? () => false
        : guardPlanStart(binding, () => startIssue(request), beforeSend),
  };
}
let claimant: string | undefined;
export function claimantId() {
  return (claimant ??= randomUUID());
}

/** A Connections control aimed at the local host uses native Laptop intent as well. */
export async function routeNativeLaptopAction(
  environmentId: EnvironmentId,
  targetSource: string,
  action: JonesOperationAction,
  review?: JonesReviewedInstall,
  dispatch?: NativeJonesDispatch,
  canOperate = () => true,
): Promise<JonesPlanGateResult | undefined> {
  const binding = installPlanServices();
  if (binding?.services.bridge.installPlan === undefined) return undefined;
  const result = await requestInstallPlan({ type: "read" }, binding);
  const issue = nativePlanIssue(result) ?? retainedRecoveryIssue(result.state, environmentId);
  if (issue !== undefined) return { kind: "refused", message: issue };
  requireWritablePlan(binding, result);
  if (result.state.laptop.environmentId !== environmentId) return undefined;
  const desktop = await binding.services.bridge.getUpdateState();
  if (targetForDesktop(desktop)?.sourceSha !== targetSource)
    return {
      kind: "refused",
      message: "The Laptop's native candidate no longer matches this source.",
    };
  if (!canOperate())
    return { kind: "refused", message: "This control no longer authorizes the action." };
  const expected = {
    binding,
    planId: result.state.current?.planId ?? null,
    environmentId,
    expectedRevision: result.state.current?.revision,
  };
  if (action.kind === "install") {
    if (review === undefined || !isNativeJonesInstallReview(review))
      return {
        kind: "refused",
        message: "A native Laptop install requires its full native review.",
      };
    await routeLaptopInstall(binding.services.bridge, desktop, review, dispatch, expected);
  } else await routeLaptopDownload(binding.services.bridge, desktop, dispatch, expected);
  return {
    kind: "handled",
    message: "Handed to Install status on Laptop. Every install requires review.",
  };
}
