import type { EnvironmentId } from "@t3tools/contracts";
import { msg } from "gt-react";
import { m } from "../../i18n/messages";
import {
  executeJonesAction,
  type JonesExecutionResult,
  type JonesExecutorHost,
} from "@t3tools/client-runtime/jones/updates/executor";
import { isSettledOperationReceipt } from "@t3tools/client-runtime/jones/install-plan";
import {
  isJonesOperationNotSentError,
  JonesOperationNotSentError,
  resolveJonesExactSourceAction,
} from "@t3tools/client-runtime/jones/updates";
import type { JonesOperationReceipt } from "@t3tools/contracts/jones/jonesOperations";
import {
  isProvisionalUnsupportedReceipt,
  validateOperationEvidence,
} from "@t3tools/client-runtime/jones/install-plan";
import type {
  JonesActionDuty,
  JonesActionJournal,
} from "@t3tools/client-runtime/jones/updates/action-journal";
import {
  connectionsPlanDispatch,
  legacyConnectionsGate,
  readLegacyConnectionsAuthority,
  routeNativeLaptopAction,
  type LegacyConnectionsAuthority,
} from "./entrypoints";
import { nativeJonesDispatch, webJonesJournal } from "./executionJournal";
import { installPlanServices, requestInstallPlan } from "./services";
import { operationAdmissionMessage } from "./copy";

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
export function pendingJonesOperationId(
  environmentId: EnvironmentId,
  { duties, history } = webJonesJournal.getSnapshot(),
) {
  const pending =
    duties.find((d) => d.environmentId === environmentId && d.kind === "stop") ??
    duties.find((d) => d.environmentId === environmentId);
  if (pending !== undefined) return pending.operationId;
  return history.findLast(
    (d) =>
      d.environmentId === environmentId &&
      d.kind === "operation" &&
      d.request !== undefined &&
      d.receipt !== undefined &&
      isProvisionalUnsupportedReceipt(d.receipt) &&
      d.unsupportedReceiptVerified !== true,
  )?.operationId;
}
export function reconcileJonesExecution(
  environmentId: EnvironmentId,
  receipt: JonesOperationReceipt,
  options?: { readonly ledgerRead?: true },
) {
  return receipt.environmentId === environmentId
    ? webJonesJournal.reconcileReceipt(receipt, options)
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

/** This prepares an enrolled host through Install status. Its policy still gates all host requests. */
export function canPrepareFollowerMatch(
  environmentId: EnvironmentId,
  targetSource: string,
  host: ReturnType<JonesExecutorHost["readLatest"]>,
) {
  const state = host.observation.state;
  if (
    typeof window === "undefined" ||
    window.desktopBridge?.installPlan === undefined ||
    state?.environmentId !== environmentId ||
    state.installedSource === undefined ||
    state.capability.operations !== 1 ||
    state.capability.exactSource !== true ||
    !state.capability.download ||
    !state.capability.install ||
    (state.operation !== undefined && !isSettledOperationReceipt(state.operation))
  )
    return false;
  const result = resolveJonesExactSourceAction({ environmentId, targetSource, ...host });
  return result.status === "unavailable" && result.reason === "follows-laptop";
}

export function createWebJonesExecutor(journal: JonesActionJournal) {
  return async (
    input: Parameters<typeof executeJonesAction>[0] & {
      readonly matchRunningGeneration?: string;
      readonly prepareFollowerMatch?: true;
    },
  ): Promise<JonesExecutionResult> => {
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
    let nativeLegacy = false;
    const binding = installPlanServices();
    const canHandOff = () =>
      sameAuthority(input.host, { observation, canOperate: true }, input.environmentId);
    const native = (canClaim = canHandOff) =>
      nativeJonesDispatch({
        environmentId: input.environmentId,
        targetSource: input.targetSource,
        action: input.action.kind,
        commandId: input.operationId,
        journal,
        ...(input.action.kind === "install" ? { stagedHandle: input.action.stagedHandle } : {}),
        canOperate: canClaim,
        send: (request) => requestInstallPlan(request, binding),
      });
    const hasNativePlan =
      typeof window !== "undefined" && window.desktopBridge?.installPlan !== undefined;
    if (input.matchRunningGeneration !== undefined && !hasNativePlan)
      return {
        kind: "refused",
        message: m(msg("The Laptop's native verification is unavailable. Check again.")),
      };
    let connections: ReturnType<typeof connectionsPlanDispatch> | undefined;
    try {
      if (input.prepareFollowerMatch) {
        const canPrepare = () =>
          !input.cancelled?.() &&
          canHandOff() &&
          canPrepareFollowerMatch(input.environmentId, input.targetSource, input.host.readLatest());
        if (
          input.matchRunningGeneration === undefined ||
          input.action.kind !== "download-exact" ||
          input.action.targetSource !== input.targetSource ||
          !canPrepare()
        )
          return {
            kind: "refused",
            message: m(msg("The verified Laptop target or host status changed. Check again.")),
          };
        const { planGate } = connectionsPlanDispatch(
          binding,
          input.action,
          undefined,
          native,
          canPrepare,
          input.matchRunningGeneration,
          true,
        );
        const result = await planGate(input.environmentId, input.targetSource);
        return result.kind === "handled"
          ? { kind: "accepted", message: result.message }
          : {
              kind: "refused",
              message: result.kind === "refused" ? result.message : "Open Install status.",
            };
      }
      if (hasNativePlan) {
        const local = await routeNativeLaptopAction(
          input.environmentId,
          input.targetSource,
          input.action,
          input.review,
          native(),
          canHandOff,
        );
        if (local !== undefined)
          return {
            kind: local.kind === "handled" ? "accepted" : "refused",
            message: local.kind === "grant" ? m(msg("Open Install status.")) : local.message,
          };
        if (observation.state?.capability.operations === 1) {
          connections = connectionsPlanDispatch(
            binding,
            input.action,
            input.review,
            native,
            () => !input.cancelled?.() && canHandOff(),
            input.matchRunningGeneration,
          );
          host = { ...host, planGate: connections.planGate };
        } else {
          const refusal = await legacyConnectionsGate(input.targetSource, input.environmentId);
          if (refusal !== null) return { kind: "refused", message: refusal };
          nativeLegacy = true;
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
    let refusedBeforeDispatch: string | undefined;
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
          const before = host.readLatest();
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
          if (nativeLegacy) {
            refusedBeforeDispatch =
              (await legacyConnectionsGate(input.targetSource, input.environmentId).catch(
                () => "Native plan status is unavailable.",
              )) ?? undefined;
            if (!sameAuthority(host, before, input.environmentId))
              refusedBeforeDispatch ??= "This host changed before dispatch.";
            if (refusedBeforeDispatch !== undefined) {
              await journal.notSent(input.operationId);
              localReservation = false;
              throw new Error(refusedBeforeDispatch);
            }
          }
          dispatched = true;
          const reply = await host.legacy(request);
          if (reply !== null) await journal.acknowledge(input.operationId);
          return reply;
        },
        start: async (request, beforeSend) => {
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
          const nativeStart = request.action.kind === "install" ? connections : undefined;
          const issue = nativeStart?.startIssue(request);
          if (issue !== undefined) {
            refusedBeforeDispatch = issue;
            if (localReservation) {
              await journal.notSent(input.operationId);
              localReservation = false;
            }
            throw new JonesOperationNotSentError({});
          }
          const guard = nativeStart?.beforeStart(request, beforeSend) ?? beforeSend;
          dispatched = true;
          let receipt: JonesOperationReceipt;
          try {
            receipt = await host.start(request, guard);
          } catch (error) {
            if (isJonesOperationNotSentError(error)) {
              dispatched = false;
              refusedBeforeDispatch = nativeStart?.startIssue(request) ?? error.message;
              if (localReservation) {
                await journal.notSent(input.operationId);
                localReservation = false;
              }
            }
            throw error;
          }
          if (validateOperationEvidence(request, receipt)) await journal.reconcileReceipt(receipt);
          return receipt;
        },
      },
    });
    if (
      !dispatched &&
      connections !== undefined &&
      input.action.kind === "install" &&
      journal.getSnapshot().duties.some((d) => d.id === input.operationId && d.kind === "native")
    )
      return {
        kind: "unknown",
        code: "native-handoff-unknown",
        message:
          refusedBeforeDispatch ??
          m(
            msg(
              "The native install handoff needs reconciliation. Read the exact attempt in Install status.",
            ),
          ),
      };
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
        message:
          refusedBeforeDispatch ?? "The action was refused before dispatch. Nothing was sent.",
      };
    const admissionMessage = operationAdmissionMessage(result.code);
    return admissionMessage === undefined ? result : { ...result, message: admissionMessage };
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
    readonly send: (
      request: { environmentId: EnvironmentId; operationId: string; stopId: string },
      beforeSend: () => boolean,
    ) => Promise<JonesOperationReceipt>;
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
      return m(msg("The control changed before Stop was sent."));
    }
    try {
      const receipt = await input.send(
        { environmentId: input.environmentId, operationId: input.receipt.operationId, stopId },
        input.canOperate,
      );
      const settled = await journal.reconcileReceipt(receipt);
      return settled && !journal.getSnapshot().duties.some((d) => d.id === stopId)
        ? receipt.stop?.result === "stopped"
          ? "Download stopped. Nothing was installed."
          : "The exact Stop result was recorded. Review installation separately."
        : "Stop is not confirmed. Read its exact operation; nothing will be resent.";
    } catch (error) {
      if (isJonesOperationNotSentError(error)) {
        await journal.notSent(stopId);
        return m(msg("The control changed before Stop was sent."));
      }
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

async function replacementAuthorityIssue(
  targetSource: string,
  environmentId: EnvironmentId,
  reviewed: LegacyConnectionsAuthority,
  matchRunningGeneration?: string,
) {
  const current = await readLegacyConnectionsAuthority(
    targetSource,
    environmentId,
    matchRunningGeneration,
  );
  if (typeof current === "string") return current;
  if (
    current.kind !== reviewed.kind ||
    (current.kind === "verified-plan" &&
      reviewed.kind === "verified-plan" &&
      (current.binding !== reviewed.binding ||
        current.planId !== reviewed.planId ||
        current.revision !== reviewed.revision)) ||
    (current.kind === "verified-running" &&
      reviewed.kind === "verified-running" &&
      (current.binding !== reviewed.binding ||
        JSON.stringify(current.evidence) !== JSON.stringify(reviewed.evidence)))
  )
    return "The verified Laptop plan changed. Review the replacement again.";
  return null;
}

export async function executeWebJonesReplacement(
  input: {
    readonly environmentId: EnvironmentId;
    readonly operationId: string;
    readonly targetSource: string;
    readonly stagedHandle: string;
    readonly authority: LegacyConnectionsAuthority;
    readonly matchRunningGeneration?: string;
    readonly host: Pick<JonesExecutorHost, "readLatest">;
    readonly canDispatch: () => boolean;
    readonly send: () => Promise<boolean>;
  },
  journal = webJonesJournal,
) {
  await journal.load();
  const before = input.host.readLatest();
  const gate = await replacementAuthorityIssue(
    input.targetSource,
    input.environmentId,
    input.authority,
    input.matchRunningGeneration,
  );
  if (gate !== null) throw new Error(gate);
  if (
    !before.canOperate ||
    !input.canDispatch() ||
    !sameAuthority(input.host, before, input.environmentId)
  )
    throw new Error("The displayed replacement changed before dispatch.");
  await journal.reserve({
    id: input.operationId,
    environmentId: input.environmentId,
    kind: "legacy",
    action: "replace-exact",
    targetSource: input.targetSource,
    stagedHandle: input.stagedHandle,
  });
  const stillAllowed = await replacementAuthorityIssue(
    input.targetSource,
    input.environmentId,
    input.authority,
    input.matchRunningGeneration,
  ).catch(() => "Native plan status is unavailable.");
  if (
    stillAllowed !== null ||
    !input.canDispatch() ||
    !sameAuthority(input.host, before, input.environmentId)
  ) {
    await journal.notSent(input.operationId);
    throw new Error(stillAllowed ?? "This control changed before dispatch. Nothing was sent.");
  }
  try {
    if (!(await input.send())) return false;
    await journal.acknowledge(input.operationId);
    return true;
  } catch {
    return false;
  }
}
