import { AuthOrchestrationReadScope, type EnvironmentId } from "@t3tools/contracts";
import {
  JonesOperationNotSentError,
  type JonesAction,
} from "@t3tools/client-runtime/jones/updates";
import type { JonesExecutorHost } from "@t3tools/client-runtime/jones/updates/executor";
import { runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import { jonesUpdates } from "../updates/jonesUpdates";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { readEnvironmentScopeAccess } from "../../state/session";
import { requestConfirmDialog } from "../../confirmDialog";
import { hasOperationReadScope, installPlanServices, requestInstallPlan } from "./services";
import { validateOperationEvidence } from "@t3tools/client-runtime/jones/install-plan";
import { chooseRecoveryPolicy } from "./recoveryReview";
import { pendingJonesOperationId, reconcileJonesExecution } from "./execution";
import type { WebReviewAuthority } from "./webReviewAuthority";
import { webJonesJournal, reconcileNativeJonesDuty } from "./executionJournal";
import { runJonesOperation } from "./production";

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
    start: async (input, beforeSend) => {
      if (!readLatest().canOperate) throw new JonesOperationNotSentError({});
      const binding = installPlanServices();
      const receipt = await runJonesOperation(environmentId, {
        action: "operationStart",
        input,
        ...(beforeSend === undefined ? {} : { beforeSend }),
      });
      if (!("operationId" in receipt)) throw new Error("Operation acceptance is not confirmed.");
      if (
        binding !== undefined &&
        hasOperationReadScope(binding.services, environmentId) &&
        input.correlation !== undefined &&
        validateOperationEvidence(input, receipt)
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
  if (
    operationId === undefined ||
    readEnvironmentScopeAccess(environmentId, AuthOrchestrationReadScope).status !== "granted"
  )
    return;
  const result = await runAtomCommand(
    appAtomRegistry,
    jonesUpdates.operation,
    {
      environmentId,
      input: { action: "operationRead", input: { environmentId, operationIds: [operationId] } },
    },
    { reportFailure: false, reportDefect: false },
  );
  if (
    result._tag !== "Success" ||
    !("receipts" in result.value) ||
    result.value.receipts.length !== 1
  )
    return;
  const exact = result.value.receipts[0];
  if (
    exact?.operationId === operationId &&
    exact.receipt !== null &&
    readEnvironmentScopeAccess(environmentId, AuthOrchestrationReadScope).status === "granted"
  )
    await reconcileJonesExecution(environmentId, exact.receipt, { ledgerRead: true });
}
