import { useEffect, useSyncExternalStore } from "react";
import type { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesInstallPlanRequest,
  JonesInstallPlanResult,
  JonesInstallPlanState,
  JonesInstallPlanClientCommand,
} from "@t3tools/contracts/jones/installPlan";
import {
  createJonesActionJournal,
  canonicalJonesCommand,
  type JonesActionJournal,
} from "@t3tools/client-runtime/jones/updates/action-journal";

export function createWebJonesJournal(storage: Parameters<typeof createJonesActionJournal>[0]) {
  return createJonesActionJournal(storage);
}

export const webJonesJournal: JonesActionJournal = createWebJonesJournal({
  getItem: (key) => window.localStorage.getItem(key),
  setItem: (key, value) => window.localStorage.setItem(key, value),
  withLock: (work) => {
    if (typeof navigator === "undefined" || navigator.locks === undefined)
      return Promise.reject(new Error("Durable update admission needs browser storage locking."));
    return navigator.locks.request("jones.update-duties.v1", work);
  },
});

export type NativeJonesDispatch = (
  request: Extract<JonesInstallPlanRequest, { type: "start" | "edit" | "laptop" | "claim" }>,
  state: JonesInstallPlanState,
) => Promise<JonesInstallPlanResult>;

export async function reconcileNativeJonesDuty(
  result: JonesInstallPlanResult,
  journal = webJonesJournal,
) {
  await journal.load();
  const proof = result.command;
  if (proof === undefined) return false;
  const duty = journal
    .getSnapshot()
    .duties.find((d) => d.id === proof.command.commandId && d.kind === "native");
  if (
    duty?.native === undefined ||
    duty.environmentId !== proof.command.environmentId ||
    duty.native.bodyHash === undefined ||
    duty.native.bodyHash !== proof.bodyHash ||
    duty.targetSource !== proof.command.targetSource ||
    duty.action !== proof.command.action ||
    duty.stagedHandle !== proof.command.stagedHandle ||
    duty.native.requestType !== proof.requestType ||
    duty.native.planId !== proof.command.planId ||
    duty.native.expectedRevision !== proof.command.expectedRevision
  )
    return false;
  if (!["terminal", "refused", "applied"].includes(proof.outcome)) return false;
  await journal.acknowledge(duty.id);
  return true;
}

export function nativeJonesDispatch(input: {
  readonly environmentId: EnvironmentId;
  readonly targetSource: string;
  readonly action: JonesInstallPlanClientCommand["action"];
  readonly stagedHandle?: string;
  readonly commandId: string;
  readonly canOperate: () => boolean;
  readonly send: (request: JonesInstallPlanRequest) => Promise<JonesInstallPlanResult>;
  readonly journal?: JonesActionJournal;
  readonly digest?: (body: string) => Promise<string>;
}): NativeJonesDispatch {
  const journal = input.journal ?? webJonesJournal;
  return async (request, state) => {
    if (!input.canOperate()) throw new Error("This control no longer authorizes the action.");
    if (state.readOnly !== undefined) throw new Error(state.readOnly.message);
    if (state.clientCommands !== 1)
      throw new Error(
        "This desktop cannot reconcile a lost handoff. Update it before using this control.",
      );
    const expectedRevision =
      request.type === "start"
        ? undefined
        : request.type === "edit"
          ? request.expectedRevision
          : state.current?.revision;
    const command: JonesInstallPlanClientCommand = {
      commandId: input.commandId,
      environmentId: input.environmentId,
      targetSource: input.targetSource,
      action: input.action,
      ...(input.stagedHandle === undefined ? {} : { stagedHandle: input.stagedHandle }),
      ...(request.type === "start"
        ? {}
        : {
            planId: request.planId,
            ...(expectedRevision === undefined ? {} : { expectedRevision }),
          }),
    };
    const body = canonicalJonesCommand({ ...request, command });
    if (input.digest === undefined && globalThis.crypto?.subtle === undefined)
      throw new Error(
        "Native handoff correlation is unavailable in this client. Open Install status on Laptop.",
      );
    const bodyHash = await (input.digest === undefined
      ? globalThis.crypto.subtle
          .digest("SHA-256", new TextEncoder().encode(body))
          .then((bytes) =>
            Array.from(new Uint8Array(bytes), (byte) => byte.toString(16).padStart(2, "0")).join(
              "",
            ),
          )
      : input.digest(body));
    await journal.reserve({
      id: command.commandId,
      environmentId: command.environmentId,
      kind: "native",
      action: command.action,
      targetSource: command.targetSource,
      ...(command.stagedHandle === undefined ? {} : { stagedHandle: command.stagedHandle }),
      native: {
        commandId: command.commandId,
        bodyHash,
        requestType: request.type,
        ...(command.planId === undefined ? {} : { planId: command.planId }),
        ...(command.expectedRevision === undefined
          ? {}
          : { expectedRevision: command.expectedRevision }),
      },
    });
    if (!input.canOperate()) {
      await journal.notSent(command.commandId);
      throw new Error("This control no longer authorizes the action. Nothing was sent.");
    }
    const result = await input.send({ ...request, command });
    await reconcileNativeJonesDuty(result, journal);
    return result;
  };
}

export function useJonesExecutionJournal() {
  const snapshot = useSyncExternalStore(
    webJonesJournal.subscribe,
    webJonesJournal.getSnapshot,
    webJonesJournal.getSnapshot,
  );
  // oxlint-disable-next-line t3code/react-effect-budget -- Controls remain held until the durable uncertainty journal has been restored; storage failure never falls back to memory.
  useEffect(() => {
    void webJonesJournal.load().catch(() => undefined);
  }, []);
  return snapshot;
}
