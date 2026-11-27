import type { DesktopBridge, EnvironmentId } from "@t3tools/contracts";
import { AuthOrchestrationOperateScope, sessionGrantsScope } from "@t3tools/contracts";
import { isJonesOperationNotSentError } from "@t3tools/client-runtime/jones/updates";
import { runAtomCommand, squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { environmentSession } from "../../state/session";
import type { EnvironmentPresentation } from "../../state/environments";
import { jonesUpdates } from "../updates/jonesUpdates";
import { fleetHost } from "../fleetUpdates/runtime";
import { requestConfirmDialog } from "../../confirmDialog";
import type { InstallStatusDevice } from "./panelUi";
import type { InstallPlanServices } from "./services";
import { chooseRecoveryPolicy } from "./recoveryReview";

export const runJonesOperation: InstallPlanServices["operation"] = async (environmentId, input) => {
  const result = await runAtomCommand(
    appAtomRegistry,
    jonesUpdates.operation,
    { environmentId, input },
    { reportFailure: false, reportDefect: false },
  );
  if (result._tag === "Success") return result.value;
  const error = squashAtomCommandFailure(result);
  if (isJonesOperationNotSentError(error)) throw error;
  throw new Error("The operation has no acknowledged receipt.");
};

export function productionInstallPlanServices(
  bridge: DesktopBridge,
  environments: () => readonly EnvironmentPresentation[],
): InstallPlanServices {
  const observed = new Map<EnvironmentId, InstallStatusDevice>();
  const device = (environmentId: EnvironmentId): InstallStatusDevice => {
    const environment = environments().find((e) => e.environmentId === environmentId);
    const session = appAtomRegistry.get(environmentSession.sessionStateAtom(environmentId));
    const observation = appAtomRegistry.get(jonesUpdates.observation(environmentId));

    const label = environment?.label ?? "Device";
    const normal = label.trim().toLowerCase();
    return {
      environmentId,
      label:
        normal === "home"
          ? "Laptop"
          : normal === "mac mini" || normal === "mac-mini"
            ? "Mini"
            : normal === "test server"
              ? "Test"
              : label,
      observation,
      canOperate:
        session._tag === "Success" &&
        !session.waiting &&
        session.value.authenticated &&
        sessionGrantsScope(session.value, AuthOrchestrationOperateScope),
      ...(observed.get(environmentId)?.lastObservedAt === undefined
        ? {}
        : { lastObservedAt: observed.get(environmentId)!.lastObservedAt! }),
      ...(observation.freshness !== "fresh" ||
      observed.get(environmentId)?.fleetStatus === undefined
        ? {}
        : { fleetStatus: observed.get(environmentId)!.fleetStatus! }),
    };
  };
  return {
    bridge,
    now: Date.now,
    devices: () => environments().map((e) => device(e.environmentId)),
    readStatus: async (environmentId) => {
      const result = await runAtomCommand(
        appAtomRegistry,
        jonesUpdates.readStatus,
        { environmentId, input: undefined },
        { reportFailure: false, reportDefect: false },
      );
      if (result._tag !== "Success") throw new Error("Current running build is unavailable.");
      return result.value;
    },
    observe: async (id) => {
      const initial = device(id);
      if (initial.observation.freshness !== "fresh") return initial;
      // A failed fleet read stays unknown. It must never become assumed manual enrollment.
      const fleetStatus = await fleetHost(id, { action: "status" }).catch(() => undefined);
      const fresh = {
        ...device(id),
        ...(fleetStatus === undefined
          ? {}
          : { fleetStatus, lastObservedAt: new Date().toISOString() }),
      };
      if (fleetStatus === undefined) {
        const { fleetStatus: _previous, ...withoutEnrollment } = fresh;
        observed.set(id, withoutEnrollment);
        return withoutEnrollment;
      }
      observed.set(id, fresh);
      return fresh;
    },
    legacy: async (environmentId, input) => {
      const result = await runAtomCommand(
        appAtomRegistry,
        jonesUpdates.action,
        { environmentId, input },
        { reportFailure: false, reportDefect: false },
      );
      if (result._tag !== "Success")
        throw new Error("The update request has no acknowledged result.");
      return result.value;
    },
    operation: runJonesOperation,
    stop: async (environmentId, input) => {
      const result = await runAtomCommand(
        appAtomRegistry,
        jonesUpdates.stop,
        { environmentId, input },
        { reportFailure: false, reportDefect: false },
      );
      if (result._tag === "Success") return result.value;
      const error = squashAtomCommandFailure(result);
      if (isJonesOperationNotSentError(error)) throw error;
      throw new Error("Stop is not confirmed. Read the exact operation.");
    },
    host: fleetHost,
    kick: jonesUpdates.kick,
    confirm: async (message) => (await requestConfirmDialog(message)) ?? false,
    chooseRecovery: chooseRecoveryPolicy,
  };
}
