import type { EnvironmentId } from "@t3tools/contracts";
import type { JonesAction } from "@t3tools/client-runtime/jones/updates";
import type { JonesExecutorHost } from "@t3tools/client-runtime/jones/updates/executor";
import { runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import { jonesUpdates } from "../updates/jonesUpdates";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { requestConfirmDialog } from "../../confirmDialog";
import { installPlanServices, requestInstallPlan } from "./services";
import { validateReceipt } from "@t3tools/client-runtime/jones/install-plan";
import { chooseRecoveryPolicy } from "./recoveryReview";
import { pendingJonesOperationId, reconcileJonesExecution } from "./execution";

export function connectionExecutorHost(environmentId: EnvironmentId, readLatest: JonesExecutorHost["readLatest"],
  legacy: (input: JonesAction) => ReturnType<JonesExecutorHost["legacy"]>): JonesExecutorHost {
  return {
    readLatest, legacy, confirm: async (message) => (await requestConfirmDialog(message)) ?? false,
    start: async (input) => {
      const result = await runAtomCommand(appAtomRegistry, jonesUpdates.operation, { environmentId, input: { action: "operationStart", input } }, { reportFailure: false, reportDefect: false });
      if (result._tag !== "Success" || !("operationId" in result.value)) throw new Error("Operation acceptance is not confirmed.");
      const receipt = result.value;
      const binding = installPlanServices();
      if (binding !== undefined && input.correlation !== undefined && validateReceipt(input, receipt))
        await requestInstallPlan({ type: "report", planId: input.correlation.planId, environmentId,
          operationId: input.operationId, evidence: { kind: "receipt", receipt } }, binding);
      return receipt;
    },
    preflight: async (input) => {
      const result = await runAtomCommand(appAtomRegistry, jonesUpdates.operation, { environmentId, input: { action: "recoveryPreflight", input } }, { reportFailure: false, reportDefect: false });
      if (result._tag !== "Success" || !("binding" in result.value)) throw new Error("Recovery preflight is unavailable.");
      return result.value;
    },
    chooseRecovery: chooseRecoveryPolicy,
  };
}
export async function readUnresolvedJonesExecution(environmentId: EnvironmentId) {
  const operationId = pendingJonesOperationId(environmentId);
  if (operationId === undefined) return;
  const result = await runAtomCommand(appAtomRegistry, jonesUpdates.operation, { environmentId,
    input: { action: "operationRead", input: { environmentId, operationIds: [operationId] } } }, { reportFailure: false, reportDefect: false });
  if (result._tag === "Success" && "receipts" in result.value) {
    const receipt = result.value.receipts.find((r) => r.operationId === operationId)?.receipt;
    if (receipt != null) reconcileJonesExecution(environmentId, receipt);
  }
}
