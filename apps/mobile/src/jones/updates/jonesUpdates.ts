// @effect-diagnostics globalDate:off - Reviews use the client clock; host receipts remain the authority for operation outcomes.
import {
  createJonesUpdateAtoms,
  type JonesUpdateObservation,
} from "@t3tools/client-runtime/jones/updates";
import {
  executeJonesAction,
  prepareReviewedInstall,
  resolveDownloadAction,
  type JonesDownloadResolution,
  type JonesExecutorHost,
  type JonesReviewedInstall,
} from "@t3tools/client-runtime/jones/updates/executor";
import { jonesTargetStatus } from "@t3tools/client-runtime/jones/updates/target-status";
import { validateReceipt } from "@t3tools/client-runtime/jones/install-plan/model";
import type { EnvironmentId } from "@t3tools/contracts";
import type { JonesActionJournal } from "@t3tools/client-runtime/jones/updates/action-journal";
import { validateJonesStopReceipt } from "@t3tools/client-runtime/jones/updates/action-journal";
import { mobileJonesJournal } from "./actionJournal";
import type {
  JonesOperationReceipt,
  JonesOperationReadResult,
  JonesOperationStartInput,
  JonesOperationStopInput,
  JonesRecoveryPolicy,
  JonesRecoveryPreflight,
} from "@t3tools/contracts/jones/jonesOperations";
import { Alert } from "react-native";
import { connectionAtomRuntime } from "../../connection/runtime";
import { uuidv4 } from "../../lib/uuid";

export const jonesUpdates = createJonesUpdateAtoms(connectionAtomRuntime);

export function confirmJonesHostInstall(message: string): Promise<boolean> {
  return new Promise((resolve) => {
    Alert.alert(
      "Install on host",
      message,
      [
        { text: "Cancel", style: "cancel", onPress: () => resolve(false) },
        { text: "Install", onPress: () => resolve(true) },
      ],
      { cancelable: true, onDismiss: () => resolve(false) },
    );
  });
}

export function chooseJonesHostRecovery(
  preflight: JonesRecoveryPreflight,
): Promise<JonesRecoveryPolicy | null> {
  return new Promise((resolve) => {
    const clone =
      preflight.clone.status === "proved" && preflight.policies.cloneRequired.admissible;
    const copy = preflight.policies.cloneOrCopy.admissible;
    const message = preflight.firstInstall
      ? "This is a first install; no existing state backup is claimed."
      : copy
        ? "The host has room for a recovery copy. Cloning may be used when proved by the host."
        : (preflight.policies.cloneOrCopy.reason ?? "The host cannot make a recovery copy.");
    Alert.alert(
      "Host recovery",
      [
        message,
        preflight.clone.reason,
        !clone && !copy ? "Recovery is unavailable. Stop this install." : undefined,
      ]
        .filter(Boolean)
        .join("\n"),
      [
        { text: "Stop", style: "cancel", onPress: () => resolve(null) },
        ...(copy
          ? [{ text: "Continue", onPress: () => resolve("clone-or-copy" as const) }]
          : clone
            ? [{ text: "Clone", onPress: () => resolve("clone-required" as const) }]
            : []),
      ],
      { cancelable: true, onDismiss: () => resolve(null) },
    );
  });
}

interface MobileJonesSessionState {
  readonly ready: boolean;
  readonly fault?: string;
  readonly busy: boolean;
  readonly reading: boolean;
  readonly stopping: boolean;
  readonly request?: JonesOperationStartInput;
  readonly receipt?: JonesOperationReceipt;
  readonly operationId?: string;
  readonly stopId?: string;
  readonly uncertainty?: "start" | "stop" | "legacy";
  readonly message?: string;
}

const terminal = new Set<JonesOperationReceipt["phase"]>([
  "completed",
  "rolled-back",
  "stopped",
  "refused",
  "failed",
  "blocked",
  "fenced",
  "interrupted",
]);

function createSession(environmentId: EnvironmentId, journal: JonesActionJournal) {
  let state: MobileJonesSessionState = {
    ready: false,
    busy: false,
    reading: false,
    stopping: false,
  };
  const listeners = new Set<() => void>();
  const patch = (next: Partial<MobileJonesSessionState>) => {
    state = { ...state, ...next };
    for (const listener of listeners) listener();
  };
  const sync = () => {
    const saved = journal.getSnapshot();
    const primary = saved.duties.find(
      (d) => d.environmentId === environmentId && d.kind !== "stop",
    );
    const stop = saved.duties.find((d) => d.environmentId === environmentId && d.kind === "stop");
    const settled = [...saved.history]
      .reverse()
      .find((d) => d.environmentId === environmentId && d.receipt !== undefined);
    const retained =
      stop?.receipt ?? primary?.receipt ?? (primary === undefined ? settled?.receipt : undefined);
    patch({
      ready: saved.ready,
      fault: saved.fault,
      uncertainty:
        stop !== undefined
          ? "stop"
          : primary?.kind === "legacy"
            ? "legacy"
            : primary !== undefined
              ? "start"
              : undefined,
      ...(primary === undefined ? {} : { operationId: primary.operationId ?? primary.id }),
      ...(primary?.kind === "legacy"
        ? {
            message:
              "This request is not confirmed. The host cannot provide a bound receipt; nothing will be resent.",
          }
        : {}),
      ...(primary?.request === undefined
        ? {}
        : { request: primary.request, operationId: primary.request.operationId }),
      ...(stop !== undefined
        ? { stopId: stop.stopId }
        : primary === undefined && settled?.kind === "stop"
          ? { stopId: settled.stopId }
          : {}),
      ...(retained === undefined ? {} : { receipt: retained }),
    });
  };
  journal.subscribe(sync);
  void journal.load().then(sync).catch(sync);
  const reconcile = async (receipt: JonesOperationReceipt) => {
    if (state.request === undefined && state.receipt === undefined) return false;
    if (state.request !== undefined && !validateReceipt(state.request, receipt)) return false;
    if (
      state.receipt !== undefined &&
      !(state.stopId === undefined
        ? validateReceipt(state.receipt, receipt)
        : validateJonesStopReceipt(state.receipt, receipt))
    )
      return false;
    if (state.stopId !== undefined && receipt.stop?.stopId !== state.stopId) return false;
    await journal.reconcileReceipt(receipt);
    patch({ receipt });
    return true;
  };
  return {
    getSnapshot: () => state,
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    patch,
    reconcile,
    async observe(receipt: JonesOperationReceipt) {
      if (receipt.environmentId !== environmentId) return;
      if (state.request === undefined && state.receipt === undefined) patch({ receipt });
      else if (
        state.receipt !== undefined &&
        terminal.has(state.receipt.phase) &&
        state.receipt.code !== "launcher-unconfirmed" &&
        state.uncertainty === undefined &&
        state.receipt.operationId !== receipt.operationId
      ) {
        patch({ receipt, request: undefined, stopId: undefined });
      } else await reconcile(receipt);
    },
  };
}

export function createMobileJonesRuntime(journal: JonesActionJournal) {
  const sessions = new Map<EnvironmentId, ReturnType<typeof createSession>>();
  const session = (environmentId: EnvironmentId) => {
    let current = sessions.get(environmentId);
    if (current === undefined) {
      current = createSession(environmentId, journal);
      sessions.set(environmentId, current);
    }
    return current;
  };
  return {
    session,
    actions: (input: MobileJonesActionsInput) =>
      buildMobileJonesActions(input, session(input.environmentId), journal),
  };
}
const mobileRuntime = createMobileJonesRuntime(mobileJonesJournal);
export const mobileJonesUpdateSession = mobileRuntime.session;

export interface MobileJonesTransport {
  readonly legacy: JonesExecutorHost["legacy"];
  readonly start: JonesExecutorHost["start"];
  readonly preflight: NonNullable<JonesExecutorHost["preflight"]>;
  readonly read: (operationId: string) => Promise<JonesOperationReadResult>;
  readonly stop: (input: JonesOperationStopInput) => Promise<JonesOperationReceipt>;
}

export function mobileJonesObservation(
  observation: JonesUpdateObservation,
  session: MobileJonesSessionState,
): JonesUpdateObservation {
  const retained = session.receipt;
  if (observation.state === null || retained === undefined) return observation;
  const operation =
    !terminal.has(retained.phase) ||
    retained.code === "launcher-unconfirmed" ||
    session.uncertainty !== undefined ||
    observation.state.operation?.operationId === retained.operationId
      ? retained
      : (observation.state.operation ?? retained);
  return { ...observation, state: { ...observation.state, operation } };
}

export interface MobileJonesActionsInput {
  readonly environmentId: EnvironmentId;
  readonly readLatest: JonesExecutorHost["readLatest"];
  readonly transport: MobileJonesTransport;
}
export const createMobileJonesActions = (input: MobileJonesActionsInput) =>
  mobileRuntime.actions(input);
function buildMobileJonesActions(
  input: MobileJonesActionsInput,
  session: ReturnType<typeof createSession>,
  journal: JonesActionJournal,
) {
  const { environmentId, transport, readLatest } = input;
  const effective = () => {
    const latest = readLatest();
    return {
      ...latest,
      observation: mobileJonesObservation(latest.observation, session.getSnapshot()),
    };
  };
  const status = () => {
    const latest = effective();
    return jonesTargetStatus(latest.observation, {
      canOperate: latest.canOperate && latest.observation.state?.environmentId === environmentId,
      targetSource: latest.observation.state?.provenance?.sourceSha,
      capability:
        latest.observation.freshness === "fresh"
          ? (latest.observation.state?.capability ?? null)
          : null,
    });
  };
  const host: JonesExecutorHost = {
    readLatest: effective,
    legacy: transport.legacy,
    start: async (request) => {
      session.patch({ request });
      return transport.start(request);
    },
    confirm: confirmJonesHostInstall,
    preflight: transport.preflight,
    chooseRecovery: chooseJonesHostRecovery,
  };
  return {
    async run(kind: "download" | "install") {
      const saved = session.getSnapshot();
      if (
        !saved.ready ||
        saved.fault !== undefined ||
        saved.busy ||
        saved.uncertainty !== undefined ||
        !status().actions[kind]
      )
        return;
      const targetSource = effective().observation.state?.provenance?.sourceSha;
      if (targetSource === undefined) return;
      const operationId = uuidv4();
      session.patch({ busy: true, message: undefined });
      try {
        const prepared: JonesDownloadResolution & { readonly review?: JonesReviewedInstall } =
          kind === "install"
            ? await prepareReviewedInstall({
                environmentId,
                targetSource,
                label: "this host",
                host,
                now: Date.now,
              })
            : resolveDownloadAction({ environmentId, targetSource, ...effective() });
        if (prepared.kind !== "ready") {
          session.patch({ message: prepared.message });
          return;
        }
        session.patch({ operationId, request: undefined, receipt: undefined, stopId: undefined });
        const result = await executeJonesAction({
          environmentId,
          targetSource,
          operationId,
          action: prepared.action,
          host: {
            ...host,
            legacy: async (request) => {
              const before = effective();
              await journal.reserve({
                id: operationId,
                environmentId,
                kind: "legacy",
                action: prepared.action.kind,
                targetSource,
                ...(prepared.action.kind === "install"
                  ? {
                      stagedHandle: prepared.action.stagedHandle,
                      currentVersion: prepared.action.currentVersion,
                    }
                  : {}),
              });
              if (
                !effective().canOperate ||
                JSON.stringify(effective().observation.state) !==
                  JSON.stringify(before.observation.state)
              ) {
                await journal.notSent(operationId);
                throw new Error("The selected host changed before dispatch.");
              }
              const result = await transport.legacy(request);
              if (result !== null) await journal.acknowledge(operationId);
              return result;
            },
            start: async (request) => {
              const before = effective();
              await journal.reserve({
                id: operationId,
                environmentId,
                kind: "operation",
                action: request.action.kind,
                targetSource,
                operationId: request.operationId,
                request,
              });
              if (
                !effective().canOperate ||
                JSON.stringify(effective().observation.state) !==
                  JSON.stringify(before.observation.state)
              ) {
                await journal.notSent(operationId);
                throw new Error("The selected host changed before dispatch.");
              }
              session.patch({ request });
              return transport.start(request);
            },
          },
          ...(prepared.review === undefined ? {} : { review: prepared.review }),
          unresolved: session.getSnapshot().uncertainty !== undefined,
        });
        const current = session.getSnapshot();
        const reconciled =
          result.receipt !== undefined && (await session.reconcile(result.receipt));
        if (
          result.receipt !== undefined &&
          current.request === undefined &&
          current.receipt === undefined
        ) {
          session.patch({ receipt: result.receipt });
        }
        if (current.stopId === undefined) session.patch({ message: result.message });
        if (
          result.kind === "unknown" &&
          !reconciled &&
          session.getSnapshot().receipt === undefined
        ) {
          session.patch({ message: result.message });
        }
      } catch {
        session.patch({ message: "The review could not finish. Nothing new was sent." });
      } finally {
        session.patch({ busy: false });
      }
    },
    async check() {
      if (
        !session.getSnapshot().ready ||
        session.getSnapshot().fault !== undefined ||
        session.getSnapshot().busy ||
        session.getSnapshot().uncertainty !== undefined
      )
        return;
      const latest = effective();
      const receipt = latest.observation.state?.operation;
      if (
        !latest.canOperate ||
        latest.observation.freshness !== "fresh" ||
        latest.observation.state?.environmentId !== environmentId ||
        !latest.observation.state.capability.check ||
        (receipt !== undefined &&
          (!terminal.has(receipt.phase) || receipt.code === "launcher-unconfirmed"))
      )
        return;
      session.patch({ busy: true, message: undefined });
      try {
        const state = await transport.legacy({ action: "check" });
        session.patch({ message: state?.message ?? "Build check finished." });
      } catch {
        session.patch({ message: "The build check failed. Check the host connection." });
      } finally {
        session.patch({ busy: false });
      }
    },
    async read() {
      const saved = session.getSnapshot();
      const latest = effective();
      const operationId =
        saved.request?.operationId ??
        saved.receipt?.operationId ??
        latest.observation.state?.operation?.operationId;
      if (
        saved.reading ||
        operationId === undefined ||
        latest.observation.freshness !== "fresh" ||
        latest.observation.state?.environmentId !== environmentId ||
        latest.observation.state.capability.operations !== 1
      )
        return;
      if (saved.request === undefined && saved.receipt === undefined)
        session.patch({ receipt: latest.observation.state.operation });
      session.patch({ reading: true });
      try {
        const result = await transport.read(operationId);
        const receipt = result.receipts.find((entry) => entry.operationId === operationId)?.receipt;
        if (receipt == null || !(await session.reconcile(receipt))) {
          session.patch({
            message: "The exact operation is still not confirmed. Nothing will be resent.",
          });
        } else
          session.patch({
            message: receipt.message ?? `Operation ${operationId}: ${receipt.phase}.`,
          });
      } catch {
        session.patch({ message: "Operation status is unavailable. Nothing will be resent." });
      } finally {
        session.patch({ reading: false });
      }
    },
    async stop() {
      const saved = session.getSnapshot();
      const latest = effective();
      const receipt = latest.observation.state?.operation;
      if (
        saved.stopping ||
        saved.stopId !== undefined ||
        !saved.ready ||
        saved.fault !== undefined ||
        saved.uncertainty === "stop" ||
        saved.uncertainty === "legacy" ||
        !status().actions.stop ||
        receipt === undefined ||
        receipt.stop !== undefined
      )
        return;
      const stopId = uuidv4();
      session.patch({ stopping: true, stopId, receipt, message: "Stop requested…" });
      try {
        await journal.reserve({
          id: stopId,
          environmentId,
          kind: "stop",
          action: "stop",
          stopId,
          operationId: receipt.operationId,
          receipt,
          targetSource:
            receipt.action.kind === "download"
              ? receipt.action.sourceSha
              : receipt.action.targetSource,
        });
        if (!effective().canOperate) {
          await journal.notSent(stopId);
          session.patch({
            stopId: undefined,
            message: "The control changed before Stop was sent.",
          });
          return;
        }
        const result = await transport.stop({
          environmentId,
          operationId: receipt.operationId,
          stopId,
        });
        if (result.stop?.stopId !== stopId || !(await session.reconcile(result))) {
          session.patch({
            uncertainty: "stop",
            message: "Stop is not confirmed. Read the exact operation before continuing.",
          });
        } else
          session.patch({
            message: result.message ?? `Operation ${result.operationId}: ${result.phase}.`,
          });
      } catch {
        session.patch({
          uncertainty: "stop",
          message: "Stop is not confirmed. Nothing will be resent.",
        });
      } finally {
        session.patch({ stopping: false });
      }
    },
  };
}
