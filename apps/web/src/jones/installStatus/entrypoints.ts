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
  JonesInstallPlanTarget,
} from "@t3tools/contracts/jones/installPlan";
import type { JonesOperationAction } from "@t3tools/contracts/jones/jonesOperations";
import type {
  JonesExecutorHost,
  JonesPlanGateResult,
} from "@t3tools/client-runtime/jones/updates/executor";
import { prepareReviewedInstall } from "@t3tools/client-runtime/jones/updates/executor";
import { selectability } from "@t3tools/client-runtime/jones/install-plan";
import {
  installStatusStore,
  type InstallStatusDevice,
  type InstallStatusSnapshot,
} from "./panelUi";
import { installPlanServices, requestInstallPlan } from "./services";

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
    (!hasOtherUpdates &&
      (native?.last != null ||
        (targetForDesktop(desktop) !== undefined &&
          [
            "available",
            "staged",
            "downloading",
            "verifying",
            "preparing",
            "installing",
            "blocked",
          ].includes(desktop?.jones?.phase ?? ""))))
  );
}

/** This routing read precedes the pill's busy guard; opening status never dispatches work. */
export async function routeInstallStatusPill(
  bridge: Pick<DesktopBridge, "installPlan"> | undefined,
  desktop: DesktopUpdateState | null,
  hasOtherUpdates: boolean,
): Promise<"opened" | "last-and-check" | "legacy"> {
  if (bridge?.installPlan === undefined) return "legacy";
  const result = await bridge.installPlan({ type: "read" });
  const store = installPlanServices()?.store ?? installStatusStore;
  store.update({ native: result.state, desktop });
  if (result.state.current !== null || result.state.readOnly !== undefined) {
    store.open();
    return "opened";
  }
  if (
    !hasOtherUpdates &&
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
  ) {
    const source = targetForDesktop(desktop)!.sourceSha;
    if (store.get().previewTargetSource !== source)
      store.update({ previewTargetSource: source, previewSelection: [] });
    store.open();
    return "opened";
  }
  if (hasOtherUpdates) return "legacy";
  if (result.state.last !== null) {
    store.open(true);
    return "last-and-check";
  }
  return "legacy";
}

async function routeLaptop(
  bridge: Pick<DesktopBridge, "installPlan">,
  desktop: DesktopUpdateState,
  action: "download" | "install",
  review?: JonesInstallPlanReview,
  origin: JonesInstallPlanOrigin = "settings",
): Promise<void> {
  if (bridge.installPlan === undefined) throw new Error("Install plans are unsupported.");
  const target = targetForDesktop(desktop);
  if (target === undefined) throw new Error("Check for a qualified Laptop build first.");
  const read = await bridge.installPlan({ type: "read" });
  const store = installPlanServices()?.store ?? installStatusStore;
  store.update({ native: read.state, desktop });
  const current = read.state.current;
  const result = await bridge.installPlan(
    current?.target.sourceSha === target.sourceSha
      ? {
          type: "laptop",
          planId: current.planId,
          action,
          ...(review === undefined ? {} : { review }),
        }
      : {
          type: "start",
          origin,
          target,
          select: store.get().previewSelection,
          laptop: { action, ...(review === undefined ? {} : { review }) },
        },
  );
  store.update({
    native: result.state,
    error: result.refused?.message ?? null,
    showLast: false,
    ...(result.refused === undefined ? { previewSelection: [] } : {}),
  });
  store.open();
  if (result.refused !== undefined) throw new Error(result.refused.message);
}
export async function routeLaptopDownload(
  bridge: Pick<DesktopBridge, "installPlan">,
  state: DesktopUpdateState,
) {
  await routeLaptop(bridge, state, "download");
  return { accepted: true, completed: false, state } satisfies DesktopUpdateActionResult;
}
export async function routeLaptopInstall(
  bridge: Pick<DesktopBridge, "installPlan">,
  state: DesktopUpdateState,
  review?: JonesInstallPlanReview,
) {
  let reviewed = review;
  if (reviewed === undefined) {
    const binding = installPlanServices();
    if (binding === undefined || bridge.installPlan === undefined)
      throw new Error("The install review is unavailable. Open Install status.");
    const native = await requestInstallPlan({ type: "read" }, binding);
    const environmentId = native.state.laptop.environmentId;
    const target = targetForDesktop(state);
    if (environmentId === undefined || target === undefined)
      throw new Error("Fresh Laptop identity is required for review.");
    let latest = await binding.services.bridge.getUpdateState();
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
  await routeLaptop(bridge, state, "install", reviewed);
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
): Promise<readonly JonesHandoffResult[]> {
  const binding = installPlanServices();
  if (binding?.services.bridge.installPlan === undefined)
    return targets.map((target) => ({
      ...target,
      handedOff: false,
      message: "Update from Connections after Laptop is verified: this client has no native plan.",
    }));
  const read = await requestInstallPlan({ type: "read" }, binding);
  const desktop = await binding.services.bridge.getUpdateState();
  const target = read.state.current?.target ?? targetForDesktop(desktop);
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
  const result =
    current !== null
      ? await requestInstallPlan(
          {
            type: "edit",
            planId: current.planId,
            expectedRevision: current.revision,
            select: selections,
            deselect: [],
          },
          binding,
        )
      : await requestInstallPlan(
          {
            type: "start",
            origin: "update-everything",
            target,
            select: selections,
            ...(desktop.jones?.phase === "available" && desktop.jones.capability.download
              ? { laptop: { action: "download" as const } }
              : {}),
          },
          binding,
        );
  binding.store.open();
  return results.map((entry) => {
    const applied = result.applied?.find((a) => a.environmentId === entry.environmentId);
    const refusal =
      result.refused?.message ??
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

/** Legacy desktop hosts stay in Connections; the native gate read supplies their prerequisite. */
export async function legacyConnectionsGate(targetSource: string): Promise<string | null> {
  const binding = installPlanServices();
  if (typeof window === "undefined" || window.desktopBridge?.installPlan === undefined) return null;
  if (binding === undefined) return "Open Install status on Laptop first.";
  const result = await requestInstallPlan({ type: "read" }, binding);
  const plan = result.state.current ?? result.state.last;
  return plan?.target.sourceSha === targetSource &&
    plan.laptop.gate !== undefined &&
    plan.laptop.gateClosed === undefined &&
    !result.state.laptop.activationBlocked &&
    result.state.laptop.activeSource === targetSource &&
    result.state.laptop.committedTransactionId === plan.laptop.gate.transactionId
    ? null
    : "Update from Connections after Laptop is updated and verified for this source.";
}
export function planGateFor(
  action: JonesOperationAction,
  review?: JonesInstallPlanReview,
): NonNullable<JonesExecutorHost["planGate"]> {
  return async (environmentId, targetSource): Promise<JonesPlanGateResult> => {
    const binding = installPlanServices();
    if (binding === undefined)
      return { kind: "refused", message: "Open Install status on Laptop first." };
    const active = await requestInstallPlan({ type: "read" }, binding);
    const desktopTarget = targetForDesktop(await binding.services.bridge.getUpdateState());
    if ((active.state.current?.target.sourceSha ?? desktopTarget?.sourceSha) !== targetSource)
      return {
        kind: "refused",
        message:
          "This source does not match the current Laptop plan. Finish or review that plan first.",
      };
    if (action.kind !== "install") {
      const device = binding.services.devices().find((d) => d.environmentId === environmentId);
      const results = await handOffJonesTargets([
        { environmentId, label: device?.label ?? "Device" },
      ]);
      return results[0]?.handedOff
        ? { kind: "handled", message: results[0].message }
        : { kind: "refused", message: results[0]?.message ?? "The plan handoff was refused." };
    }
    const read = await requestInstallPlan({ type: "read" }, binding);
    if (read.state.current?.target.sourceSha !== targetSource)
      return {
        kind: "refused",
        message: "Open this source's current plan to review installation.",
      };
    const observed = await binding.services.observe(environmentId);
    const result = await requestInstallPlan(
      {
        type: "claim",
        planId: read.state.current.planId,
        environmentId,
        claimant: `window:${claimantId()}`,
        step: "install",
        explicit: true,
        action,
        ...(review === undefined ? {} : { review }),
        ...(observed.observation.state?.installedSource === undefined
          ? {}
          : { expectedInstalledSource: observed.observation.state.installedSource.sha }),
      },
      binding,
    );
    return result.grant === undefined
      ? { kind: "refused", message: result.refused?.message ?? "Install was not granted." }
      : { kind: "grant", grant: result.grant };
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
  review?: JonesInstallPlanReview,
): Promise<JonesPlanGateResult | undefined> {
  const binding = installPlanServices();
  if (binding?.services.bridge.installPlan === undefined) return undefined;
  const result = await requestInstallPlan({ type: "read" }, binding);
  if (result.state.laptop.environmentId !== environmentId) return undefined;
  const desktop = await binding.services.bridge.getUpdateState();
  if (targetForDesktop(desktop)?.sourceSha !== targetSource)
    return {
      kind: "refused",
      message: "The Laptop's native candidate no longer matches this source.",
    };
  if (action.kind === "install") await routeLaptopInstall(binding.services.bridge, desktop, review);
  else await routeLaptopDownload(binding.services.bridge, desktop);
  return {
    kind: "handled",
    message: "Handed to Install status on Laptop. Every install requires review.",
  };
}
