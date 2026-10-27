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
import type { WebReviewAuthority } from "./webReviewAuthority";
import { webJonesJournal, reconcileNativeJonesDuty } from "./executionJournal";

export function connectionExecutorHost(
  environmentId: EnvironmentId,
  readLatest: JonesExecutorHost["readLatest"],
  legacy: (input: JonesAction) => ReturnType<JonesExecutorHost["legacy"]>,
  authority?: WebReviewAuthority,
): JonesExecutorHost {
  return {
    readLatest,
    legacy: (input) => {
      if (!readLatest().canOperate)
        throw new Error("This control no longer authorizes the action.");
      return legacy(input);
    },
    confirm: (message) =>
      authority === undefined
        ? Promise.resolve(requestConfirmDialog(message)).then((result) => result ?? false)
        : authority.wait(async () => (await requestConfirmDialog(message)) ?? false, false),
    start: async (input) => {
      if (!readLatest().canOperate)
        throw new Error("This control no longer authorizes the action.");
      const result = await runAtomCommand(
        appAtomRegistry,
        jonesUpdates.operation,
        { environmentId, input: { action: "operationStart", input } },
        { reportFailure: false, reportDefect: false },
      );
      if (result._tag !== "Success" || !("operationId" in result.value))
        throw new Error("Operation acceptance is not confirmed.");
      const receipt = result.value;
      const binding = installPlanServices();
      if (
        binding !== undefined &&
        input.correlation !== undefined &&
        validateReceipt(input, receipt)
      )
        await requestInstallPlan(
          {
            type: "report",
            planId: input.correlation.planId,
            environmentId,
            operationId: input.operationId,
            evidence: { kind: "receipt", receipt },
          },
          binding,
        );
      return receipt;
    },
    preflight: async (input) => {
      const result = await runAtomCommand(
        appAtomRegistry,
        jonesUpdates.operation,
        { environmentId, input: { action: "recoveryPreflight", input } },
        { reportFailure: false, reportDefect: false },
      );
      if (result._tag !== "Success" || !("binding" in result.value))
        throw new Error("Recovery preflight is unavailable.");
      return result.value;
    },
    chooseRecovery: (input) =>
      authority === undefined
        ? chooseRecoveryPolicy(input)
        : authority.wait(() => chooseRecoveryPolicy(input), null),
  };
}
export async function readUnresolvedJonesExecution(environmentId: EnvironmentId) {
  await webJonesJournal.load();
  const native = webJonesJournal
    .getSnapshot()
    .duties.find((d) => d.environmentId === environmentId && d.kind === "native");
  if (native?.native !== undefined) {
    const result = await requestInstallPlan({ type: "read", commandId: native.native.commandId });
    await reconcileNativeJonesDuty(result);
  }
  const operationId = pendingJonesOperationId(environmentId);
  if (operationId === undefined) return;
  const result = await runAtomCommand(
    appAtomRegistry,
    jonesUpdates.operation,
    {
      environmentId,
      input: { action: "operationRead", input: { environmentId, operationIds: [operationId] } },
    },
    { reportFailure: false, reportDefect: false },
  );
  if (result._tag === "Success" && "receipts" in result.value) {
    const receipt = result.value.receipts.find((r) => r.operationId === operationId)?.receipt;
    if (receipt != null) await reconcileJonesExecution(environmentId, receipt);
  }
}
