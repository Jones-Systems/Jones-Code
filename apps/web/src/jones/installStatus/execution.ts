import type { EnvironmentId } from "@t3tools/contracts";
import {
  executeJonesAction,
  type JonesExecutionResult,
  type JonesExecutorHost,
} from "@t3tools/client-runtime/jones/updates/executor";
import type { JonesOperationReceipt } from "@t3tools/contracts/jones/jonesOperations";
import type {
  JonesActionDuty,
  JonesActionJournal,
} from "@t3tools/client-runtime/jones/updates/action-journal";
import { legacyConnectionsGate, planGateFor, routeNativeLaptopAction } from "./entrypoints";
import { nativeJonesDispatch, webJonesJournal } from "./executionJournal";
import { installPlanServices, requestInstallPlan } from "./services";

const unknownMessage =
  "The request may have been accepted. Its effect is unknown. Read its exact outcome; nothing will be resent.";
export function unresolvedJonesExecution(
  environmentId: EnvironmentId,
  _receipt?: JonesOperationReceipt,
  saved = webJonesJournal.getSnapshot(),
) {
  const duty = saved.duties.find((d) => d.environmentId === environmentId);
  if (!saved.ready || saved.fault !== undefined || duty !== undefined)
    return {
      kind: "unknown" as const,
      message:
        saved.fault ??
        (!saved.ready
          ? "Reading update request history…"
          : duty?.kind === "legacy"
            ? "The request may have been accepted. Its effect is unknown. This host cannot provide a bound receipt; nothing will be resent."
            : unknownMessage),
      ...(duty?.receipt === undefined ? {} : { receipt: duty.receipt }),
    };
  return undefined;
}
export function pendingJonesOperationId(environmentId: EnvironmentId) {
  const duties = webJonesJournal.getSnapshot().duties;
  return (
    duties.find((d) => d.environmentId === environmentId && d.kind === "stop") ??
    duties.find((d) => d.environmentId === environmentId)
  )?.operationId;
}
export function reconcileJonesExecution(
  environmentId: EnvironmentId,
  receipt: JonesOperationReceipt,
) {
  return receipt.environmentId === environmentId
    ? webJonesJournal.reconcileReceipt(receipt)
    : Promise.resolve(false);
}

function sameAuthority(
  host: Pick<JonesExecutorHost, "readLatest">,
  before: ReturnType<JonesExecutorHost["readLatest"]>,
  environmentId: EnvironmentId,
) {
  const latest = host.readLatest();
  return (
    latest.canOperate &&
    latest.observation.freshness === "fresh" &&
    latest.observation.state?.environmentId === environmentId &&
    JSON.stringify(latest.observation.state) === JSON.stringify(before.observation.state)
  );
}

export function createWebJonesExecutor(journal: JonesActionJournal) {
  return async (input: Parameters<typeof executeJonesAction>[0]): Promise<JonesExecutionResult> => {
    try {
      await journal.load();
    } catch {
      return {
        kind: "refused",
        message: "Update request history is unavailable. Nothing was sent.",
      };
    }
    const observation = input.host.readLatest().observation;
    if (observation.freshness === "fresh" && observation.state?.operation !== undefined)
      await journal.reconcileReceipt(observation.state.operation);
    if (
      journal.getSnapshot().fault !== undefined ||
      journal.getSnapshot().duties.some((d) => d.environmentId === input.environmentId)
    )
      return { kind: "unknown", message: journal.getSnapshot().fault ?? unknownMessage };
    if (!input.host.readLatest().canOperate)
      return { kind: "refused", message: "This control no longer authorizes the action." };
    let host: JonesExecutorHost = input.host;
    const binding = installPlanServices();
    const canHandOff = () =>
      sameAuthority(input.host, { observation, canOperate: true }, input.environmentId);
    const native = nativeJonesDispatch({
      environmentId: input.environmentId,
      targetSource: input.targetSource,
      action: input.action.kind,
      commandId: input.operationId,
      journal,
      ...(input.action.kind === "install" ? { stagedHandle: input.action.stagedHandle } : {}),
      canOperate: canHandOff,
      send: (request) => requestInstallPlan(request, binding),
    });
    try {
      if (typeof window !== "undefined" && window.desktopBridge?.installPlan !== undefined) {
        const local = await routeNativeLaptopAction(
          input.environmentId,
          input.targetSource,
          input.action,
          input.review,
          native,
          canHandOff,
        );
        if (local !== undefined)
          return {
            kind: local.kind === "handled" ? "accepted" : "refused",
            message: local.kind === "grant" ? "Open Install status." : local.message,
          };
        if (observation.state?.capability.operations === 1)
          host = { ...host, planGate: planGateFor(input.action, input.review, native, canHandOff) };
        else {
          const refusal = await legacyConnectionsGate(input.targetSource);
          if (refusal !== null) return { kind: "refused", message: refusal };
        }
      }
    } catch (cause) {
      return journal.getSnapshot().duties.some((d) => d.environmentId === input.environmentId)
        ? { kind: "unknown", code: "native-handoff-unknown", message: unknownMessage }
        : {
            kind: "refused",
            message:
              cause instanceof Error
                ? cause.message
                : "Native status is unavailable. Nothing was sent.",
          };
    }
    let dispatched = false;
    let localReservation = false;
    const reserve = async (duty: JonesActionDuty) => {
      const before = host.readLatest();
      if (!before.canOperate) throw new Error("This control no longer authorizes the action.");
      await journal.reserve(duty);
      localReservation = true;
      if (!sameAuthority(host, before, input.environmentId)) {
        await journal.notSent(duty.id);
        localReservation = false;
        throw new Error("The selected host changed before dispatch. Nothing was sent.");
      }
    };
    const result = await executeJonesAction({
      ...input,
      unresolved: false,
      host: {
        ...host,
        legacy: async (request) => {
          await reserve({
            id: input.operationId,
            environmentId: input.environmentId,
            kind: "legacy",
            action: input.action.kind,
            targetSource: input.targetSource,
            ...(input.action.kind === "install"
              ? {
                  stagedHandle: input.action.stagedHandle,
                  currentVersion: input.action.currentVersion,
                }
              : {}),
          });
          dispatched = true;
          const reply = await host.legacy(request);
          if (reply !== null) await journal.acknowledge(input.operationId);
          return reply;
        },
        start: async (request) => {
          const nativeDuty = journal
            .getSnapshot()
            .duties.find((d) => d.id === input.operationId && d.kind === "native");
          if (nativeDuty === undefined)
            await reserve({
              id: input.operationId,
              environmentId: input.environmentId,
              kind: "operation",
              action: request.action.kind,
              targetSource: input.targetSource,
              operationId: request.operationId,
              request,
            });
          else if (!host.readLatest().canOperate)
            throw new Error("This control no longer authorizes the action.");
          dispatched = true;
          const receipt = await host.start(request);
          await journal.reconcileReceipt(receipt);
          return receipt;
        },
      },
    });
    const stopped = [...journal.getSnapshot().duties, ...journal.getSnapshot().history].find(
      (d) =>
        d.kind === "stop" &&
        d.environmentId === input.environmentId &&
        d.operationId === (result.receipt?.operationId ?? input.operationId),
    );
    if (stopped !== undefined)
      return {
        kind: journal.getSnapshot().duties.some((d) => d.id === stopped.id)
          ? "unknown"
          : "accepted",
        ...(stopped.receipt === undefined ? {} : { receipt: stopped.receipt }),
        message:
          stopped.receipt?.stop?.result === "stopped"
            ? "Download stopped. Nothing was installed."
            : "Stop is not confirmed. Read its exact operation; nothing will be resent.",
      };
    if (
      result.kind === "unknown" &&
      !dispatched &&
      !localReservation &&
      !journal.getSnapshot().duties.some((d) => d.environmentId === input.environmentId)
    )
      return {
        kind: "refused",
        message: "The action was refused before dispatch. Nothing was sent.",
      };
    return result;
  };
}
export const executeWebJonesAction = createWebJonesExecutor(webJonesJournal);

const stopAdmissions = new Map<EnvironmentId, Promise<string>>();
export function stopWebJonesDownload(
  input: {
    readonly environmentId: EnvironmentId;
    readonly receipt: JonesOperationReceipt;
    readonly uuid: () => string;
    readonly canOperate: () => boolean;
    readonly send: (request: {
      environmentId: EnvironmentId;
      operationId: string;
      stopId: string;
    }) => Promise<JonesOperationReceipt>;
  },
  journal = webJonesJournal,
): Promise<string> {
  const pending = stopAdmissions.get(input.environmentId);
  if (pending !== undefined) return pending;
  const run = (async () => {
    await journal.load();
    const saved = journal
      .getSnapshot()
      .duties.find((d) => d.environmentId === input.environmentId && d.kind === "stop");
    if (saved !== undefined)
      return "Stop is not confirmed. Read its exact operation; nothing will be resent.";
    if (
      journal
        .getSnapshot()
        .history.some(
          (d) =>
            d.kind === "stop" &&
            d.environmentId === input.environmentId &&
            d.operationId === input.receipt.operationId,
        )
    )
      return "The exact Stop result was already recorded. Nothing will be resent.";
    if (
      !input.canOperate() ||
      input.receipt.environmentId !== input.environmentId ||
      input.receipt.stop !== undefined ||
      !["accepted", "running"].includes(input.receipt.phase) ||
      input.receipt.action.kind === "install"
    )
      return "This phase cannot be stopped. Read the current operation status.";
    const stopId = input.uuid();
    await journal.reserve({
      id: stopId,
      environmentId: input.environmentId,
      kind: "stop",
      action: "stop",
      targetSource:
        input.receipt.action.kind === "download"
          ? input.receipt.action.sourceSha
          : input.receipt.action.targetSource,
      operationId: input.receipt.operationId,
      stopId,
      receipt: input.receipt,
    });
    if (!input.canOperate()) {
      await journal.notSent(stopId);
      return "The control changed before Stop was sent.";
    }
    try {
      const receipt = await input.send({
        environmentId: input.environmentId,
        operationId: input.receipt.operationId,
        stopId,
      });
      const settled = await journal.reconcileReceipt(receipt);
      return settled && !journal.getSnapshot().duties.some((d) => d.id === stopId)
        ? receipt.stop?.result === "stopped"
          ? "Download stopped. Nothing was installed."
          : "The exact Stop result was recorded. Review installation separately."
        : "Stop is not confirmed. Read its exact operation; nothing will be resent.";
    } catch {
      return "Stop is not confirmed. Read its exact operation; nothing will be resent.";
    }
  })();
  stopAdmissions.set(input.environmentId, run);
  void run
    .finally(() => {
      if (stopAdmissions.get(input.environmentId) === run)
        stopAdmissions.delete(input.environmentId);
    })
    .catch(() => undefined);
  return run;
}

export async function executeWebJonesReplacement(
  input: {
    readonly environmentId: EnvironmentId;
    readonly operationId: string;
    readonly targetSource: string;
    readonly stagedHandle: string;
    readonly host: Pick<JonesExecutorHost, "readLatest">;
    readonly send: () => Promise<boolean>;
  },
  journal = webJonesJournal,
) {
  await journal.load();
  const before = input.host.readLatest();
  if (!before.canOperate) throw new Error("This control no longer authorizes the action.");
  await journal.reserve({
    id: input.operationId,
    environmentId: input.environmentId,
    kind: "legacy",
    action: "replace-exact",
    targetSource: input.targetSource,
    stagedHandle: input.stagedHandle,
  });
  if (!sameAuthority(input.host, before, input.environmentId)) {
    await journal.notSent(input.operationId);
    throw new Error("This control changed before dispatch. Nothing was sent.");
  }
  try {
    if (!(await input.send())) return false;
    await journal.acknowledge(input.operationId);
    return true;
  } catch {
    return false;
  }
}
