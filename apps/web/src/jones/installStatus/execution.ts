import type { EnvironmentId } from "@t3tools/contracts";
import type { JonesExecutionResult, JonesExecutorHost } from "@t3tools/client-runtime/jones/updates/executor";
import { executeJonesAction } from "@t3tools/client-runtime/jones/updates/executor";
import { validateReceipt } from "@t3tools/client-runtime/jones/install-plan";
import type { JonesOperationStartInput, JonesOperationReceipt } from "@t3tools/contracts/jones/jonesOperations";
import { legacyConnectionsGate, planGateFor, routeNativeLaptopAction } from "./entrypoints";

// Retain unknown work across notice remounts and target/revision changes. Only its receipt clears it.
const unresolved = new Map<EnvironmentId, { result: JonesExecutionResult; request?: JonesOperationStartInput }>();
export function pendingJonesOperationId(environmentId: EnvironmentId) { const p = unresolved.get(environmentId); return p?.request?.operationId ?? p?.result.receipt?.operationId; }
export function unresolvedJonesExecution(environmentId: EnvironmentId, receipt?: JonesOperationReceipt) {
  if (receipt !== undefined) reconcileJonesExecution(environmentId, receipt);
  return unresolved.get(environmentId)?.result;
}
export function retainUnknownJonesExecution(environmentId: EnvironmentId, result: JonesExecutionResult) {
  if (result.kind === "unknown") unresolved.set(environmentId, { result });
}
export function reconcileJonesExecution(environmentId: EnvironmentId, receipt: JonesOperationReceipt) {
  const pending = unresolved.get(environmentId);
  const original = pending?.request ?? pending?.result.receipt;
  if (original !== undefined && validateReceipt(original, receipt) &&
    (pending?.result.receipt === undefined || validateReceipt(pending.result.receipt, receipt)) &&
    ["completed", "rolled-back", "stopped", "refused", "failed", "blocked", "fenced", "interrupted"].includes(receipt.phase) && receipt.code !== "launcher-unconfirmed") unresolved.delete(environmentId);
}
export async function executeWebJonesAction(input: Parameters<typeof executeJonesAction>[0]) {
  const observation = input.host.readLatest().observation;
  if (observation.state?.operation !== undefined) reconcileJonesExecution(input.environmentId, observation.state.operation);
  const prior = unresolved.get(input.environmentId);
  if (prior !== undefined) return { kind: "unknown" as const, message: prior.result.message, ...("receipt" in prior.result ? { receipt: prior.result.receipt } : {}) };
  let host: JonesExecutorHost = input.host;
  if (typeof window !== "undefined" && window.desktopBridge?.installPlan !== undefined) {
    let local: Awaited<ReturnType<typeof routeNativeLaptopAction>>;
    try { local = await routeNativeLaptopAction(input.environmentId, input.targetSource, input.action, input.review); }
    catch {
      const result: JonesExecutionResult = { kind: "unknown", code: "native-handoff-unknown", message: "The native handoff was not confirmed. Open Install status; nothing is resent automatically." };
      unresolved.set(input.environmentId, { result });
      return result;
    }
    if (local !== undefined) return { kind: local.kind === "handled" ? "accepted" as const : "refused" as const, message: local.kind === "grant" ? "Open Install status." : local.message };
    if (observation.state?.capability.operations === 1) host = { ...host, planGate: planGateFor(input.action, input.review) };
    else {
      const refusal = await legacyConnectionsGate(input.targetSource);
      if (refusal !== null) return { kind: "refused" as const, message: refusal };
    }
  }
  let original: JonesOperationStartInput | undefined;
  const result = await executeJonesAction({ ...input, host: { ...host, start: async (request) => {
    original = request;
    return host.start(request);
  } }, unresolved: false });
  if (result.kind === "unknown") unresolved.set(input.environmentId, { result, ...(original === undefined ? {} : { request: original }) });
  return result;
}
