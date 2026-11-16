import {
  createSettlementRestoreController,
  settlementRestoreMessage,
  supportsSettlementRestore,
  type SettlementRestorePorts,
  type SettlementRestoreTarget,
} from "@t3tools/client-runtime/jones/settlement-restore";
import type { EnvironmentId, ExecutionEnvironmentCapabilities } from "@t3tools/contracts";

export interface MobileSettlementRestorePorts extends SettlementRestorePorts {
  readonly capabilities: (
    environmentId: EnvironmentId,
  ) => Pick<ExecutionEnvironmentCapabilities, "settlementRestore"> | undefined;
  readonly canOperate: (environmentId: EnvironmentId) => boolean;
  readonly invalidateTransientUndo: (target: SettlementRestoreTarget) => void;
  readonly alert: (title: string, message: string) => void;
}

export function createMobileSettlementRestoreAction(ports: MobileSettlementRestorePorts) {
  const allowed = (environmentId: EnvironmentId) =>
    ports.canOperate(environmentId) && supportsSettlementRestore(ports.capabilities(environmentId));
  const controller = createSettlementRestoreController(ports);
  const inFlight = new Map<string, Promise<boolean>>();
  return (target: SettlementRestoreTarget): Promise<boolean> => {
    if (!allowed(target.environmentId)) {
      ports.alert(
        "Could not undo settle",
        ports.canOperate(target.environmentId)
          ? settlementRestoreMessage({ type: "unsupported" })
          : "This connection cannot change threads.",
      );
      return Promise.resolve(false);
    }
    const key = JSON.stringify([target.environmentId, target.threadId]);
    const running = inFlight.get(key);
    if (running !== undefined) return running;
    ports.invalidateTransientUndo(target);
    const started = controller
      .restoreCurrent(target)
      .then((outcome) => {
        if (outcome.type === "restored") return true;
        ports.alert("Could not undo settle", settlementRestoreMessage(outcome));
        return false;
      })
      .finally(() => inFlight.delete(key));
    inFlight.set(key, started);
    return started;
  };
}
