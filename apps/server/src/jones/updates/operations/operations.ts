// @effect-diagnostics nodeBuiltinImport:off globalDate:off
// Promise adapter owned by JonesUpdates, sharing its existing updater and native operation engine.
import * as NodeCrypto from "node:crypto";
import type {
  JonesOperationStartInput, JonesOperationStopInput, JonesOperationFenceInput,
  JonesOperationReadInput, JonesOperationReadResult, JonesOperationReceipt, JonesRecoveryPreflightInput,
} from "@t3tools/contracts/jones/jonesOperations";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import { JonesUpdater, PreparationStopRequested, type JonesUpdaterInstallInput, type JonesInstallBeforeDispatchRefusal } from "../JonesUpdater.ts";
import { followerTargetRefusal } from "../followerPolicy.ts";
import type { OperationReconciliation } from "../launcherOperation.ts";
import type { PreparationSupervisor } from "../preparation/supervisor.ts";
import { serialized } from "../../fleetUpdates/store.ts";
import { type OperationLedger, operationReceipt, operationTombstone } from "./ledger.ts";

const targetOf = (r: JonesOperationStartInput) => r.action.kind === "download" ? r.action.sourceSha : r.action.targetSource;
const terminal = (r: JonesOperationReceipt) => !["accepted", "running", "stop-requested", "adopting", "handed-off", "held"].includes(r.phase);
const bindingMatches = (r: JonesOperationReceipt, state: JonesUpdateState) => r.action.kind === "install" &&
  state.environmentId === r.environmentId && state.installedSource?.sha === r.expectedInstalledSource &&
  state.currentVersion === r.action.currentVersion && state.stagedHandle === r.action.stagedHandle &&
  state.provenance?.sourceSha === r.action.targetSource;

export function createTargetOperations(options: {
  readonly key: string;
  readonly ledger: OperationLedger;
  readonly updater: JonesUpdater;
  readonly preparation: PreparationSupervisor;
  readonly refreshPolicy: () => Promise<unknown>;
  readonly install: (input: JonesUpdaterInstallInput & { readonly operationId: string; readonly targetSource: string; readonly expectedInstalledSource: string }) => Promise<JonesUpdateState>;
  readonly reconcileNative: (id: string) => Promise<OperationReconciliation>;
  readonly onFailure: () => void;
  readonly recovery?: {
    readonly validate: (recovery: NonNullable<Extract<JonesOperationStartInput["action"], {kind: "install"}>["recovery"]>, binding: JonesRecoveryPreflightInput) => string | undefined;
    readonly observed: (operationId: string) => Promise<"clone" | "copy" | undefined>;
  };
}) {
  const { ledger, updater, preparation } = options;
  const active = new Map<string, AbortController>();
  const lifecycleHolds = new Set<string>();
  let recoveryFailed = false;
  const gate = <A>(f: () => Promise<A>) => serialized(`target-operations:${options.key}`, f);
  const publish = (receipt: JonesOperationReceipt) => { updater.recordOperation(receipt); return receipt; };
  const change = async (id: string, f: (old: JonesOperationReceipt) => JonesOperationReceipt) => publish(await ledger.transition(id, f));
  const patch = (id: string, fields: Partial<JonesOperationReceipt>) => change(id, (old) => ({ ...old, ...fields }));
  const finishStop = (r: JonesOperationReceipt, phase: JonesOperationReceipt["phase"], result: "stopped" | "too-late" | "held") => {
    const { code, message: _message, ...released } = r;
    return { ...(result !== "held" && (code === "unreaped" || code === "cleanup-unconfirmed") ? released : r),
      phase, ...(r.stop === undefined ? {} : { stop: { ...r.stop, result } }) };
  };
  const confirmInstall = (id: string, fields: Partial<JonesOperationReceipt>) => change(id, (old) => {
    const { code, message: _message, ...confirmed } = old;
    return { ...(code === "launcher-unconfirmed" || code === "launcher-unreadable" ? confirmed : old), ...fields };
  });
  const held = (r: JonesOperationReceipt, code: "unreaped" | "cleanup-unconfirmed" = "unreaped") => {
    if (r.jobId !== undefined) updater.setHeldJobs([r.jobId]);
    return change(r.operationId, (old) => ({ ...finishStop(old, "held", "held"), code,
      message: "Preparation exit or native cleanup is unproved. Nothing new starts until release is proved." }));
  };
  const stageReceipt = (r: JonesOperationReceipt, state: JonesUpdateState) => {
    const p = state.provenance;
    if (state.stagedHandle === undefined || p === undefined || p.sourceSha !== targetOf(r) ||
      state.environmentId !== r.environmentId || state.installedSource?.sha !== r.expectedInstalledSource) return undefined;
    return { stagedHandle: state.stagedHandle, sourceSha: p.sourceSha, sourceTree: p.sourceTree,
      artifactId: p.artifactId, ...(p.version === undefined ? {} : { version: p.version }) };
  };
  const reconcileInstall = async (r: JonesOperationReceipt, refusal?: JonesInstallBeforeDispatchRefusal) => {
    const native = await options.reconcileNative(r.operationId);
    if (native.state === "absent") {
      const a = r.action;
      if (a.kind === "install" && refusal?.kind === "before-dispatch" && refusal.operationId === r.operationId &&
        refusal.environmentId === r.environmentId && refusal.expectedInstalledSource === r.expectedInstalledSource &&
        refusal.targetSource === a.targetSource && refusal.stagedHandle === a.stagedHandle && refusal.currentVersion === a.currentVersion) {
        updater.releaseHeldJob(r.operationId);
        return patch(r.operationId, { phase: "refused", code: bindingMatches(r, updater.snapshot()) ? "install-refused" : "stale-binding",
          message: "The installation was refused before native acceptance. The staged selection was retained." });
      }
      updater.setHeldJobs([r.operationId]);
      return patch(r.operationId, { code: "launcher-unconfirmed", message: "Native acceptance is unknown. This operation will not be sent again." });
    }
    const b = native.binding;
    const a = r.action;
    const bound = a.kind === "install" && native.operationId === r.operationId && b !== undefined &&
      b.environmentId === r.environmentId && b.currentVersion === a.currentVersion &&
      b.expectedInstalledSource === r.expectedInstalledSource && b.targetSource === a.targetSource && b.stagedHandle === a.stagedHandle;
    if (!bound || (native.state === "blocked" && native.updateId !== r.operationId)) {
      updater.setHeldJobs([r.operationId]);
      return patch(r.operationId, { code: "launcher-unreadable", message: "The launcher's exact operation binding could not be proved." });
    }
    if (native.state === "pending") {
      updater.setHeldJobs([r.operationId]);
      return confirmInstall(r.operationId, { phase: "handed-off", updateId: r.operationId });
    }
    updater.releaseHeldJob(r.operationId);
    if (native.state === "rejected") return confirmInstall(r.operationId, { phase: "refused", code: "launcher-rejected" });
    if (a.kind === "install" && a.recovery !== undefined && r.recovery === undefined) {
      const observed = await options.recovery?.observed(r.operationId);
      await patch(r.operationId, { recovery: { requested: a.recovery.policy,
        ...(observed === undefined ? {} : { observed }) } });
    }
    const snapshot = updater.snapshot();
    const outcome = snapshot.updateId === r.operationId ? snapshot.outcome : undefined;
    return confirmInstall(r.operationId, {
      phase: native.state === "committed" ? "completed" : native.state,
      updateId: r.operationId,
      ...(outcome === undefined ? {} : { outcome }),
    });
  };
  const execute = async (input: JonesOperationStartInput, controller: AbortController) => {
    const id = input.operationId;
    let receipt = (await ledger.read(id))!;
    if (input.action.kind !== "install" && receipt.stop !== undefined) { await change(id, (r) => finishStop(r, "stopped", "stopped")); return; }
    if (input.action.kind === "install") {
      receipt = await patch(id, { phase: "running" }); // durable dispatch marker, before the native call
      const action = input.action;
      let refusal: JonesInstallBeforeDispatchRefusal | undefined;
      await options.install({ ...action, operationId: id, environmentId: input.environmentId,
        expectedInstalledSource: input.expectedInstalledSource, origin: "manual",
        onBeforeDispatchRefusal: (proof) => { refusal = proof; } });
      await reconcileInstall(receipt, refusal);
      return;
    }
    const jobId = NodeCrypto.randomUUID();
    receipt = await change(id, (r) => r.stop !== undefined ? finishStop(r, "stopped", "stopped") : { ...r, phase: "running", jobId });
    if (receipt.phase === "stopped") return;
    let preparationStarted = false;
    const operation = { operationId: id, jobId, signal: controller.signal,
      onStarting: () => { preparationStarted = true; },
      onAdopting: async () => {
        await change(id, (r) => {
          if (r.stop !== undefined || controller.signal.aborted) throw new PreparationStopRequested(id);
          return { ...r, phase: "adopting" };
        });
      } };
    const state = input.action.kind === "download-exact"
      ? await updater.stageExact(input.action.targetSource, undefined, operation)
      : await updater.download(input.action, operation);
    receipt = (await ledger.read(id))!;
    const join = preparationStarted ? preparation.inspect(jobId) : { jobId, kind: "not-started" as const };
    if (join === "unknown" || join === "running" || join.kind === "unreaped" || join.kind === "cleanup-unconfirmed") {
      await held(receipt, typeof join !== "string" && join.kind === "cleanup-unconfirmed" ? "cleanup-unconfirmed" : "unreaped"); return;
    }
    const staged = stageReceipt(receipt, state);
    if (staged !== undefined) {
      if (receipt.phase !== "adopting") receipt = await patch(id, { phase: "adopting" });
      await change(id, (r) => ({ ...finishStop(r, "completed", "too-late"), staged }));
    } else if (receipt.phase === "adopting") {
      updater.setHeldJobs([jobId]);
      await patch(id, { phase: "held", code: "adoption-unconfirmed", message: "Stage publication needs reconciliation on this host." });
    } else if (receipt.stop !== undefined || controller.signal.aborted)
      await change(id, (r) => finishStop(r, "stopped", "stopped"));
    else await patch(id, { phase: "failed", code: "download-failed", message: "The selected source was not staged. Nothing was installed." });
  };
  const reconcile = () => gate(async () => {
    try {
      if (preparation.unreleasedJobs !== undefined) {
        const unresolved = new Set(await preparation.unreleasedJobs());
        for (const id of lifecycleHolds) if (!unresolved.has(id)) updater.releaseHeldJob(id);
        lifecycleHolds.clear();
        for (const id of unresolved) lifecycleHolds.add(id);
        updater.setHeldJobs([...unresolved]);
      }
      for await (const r of ledger.listUnsettled()) {
        if (active.has(r.operationId)) continue;
        if (r.phase === "accepted" && r.jobId === undefined) { await patch(r.operationId, { phase: "interrupted" }); continue; }
        if (r.action.kind === "install") { await reconcileInstall(r); continue; }
        const staged = stageReceipt(r, updater.snapshot());
        if (r.phase === "adopting" || r.code === "adoption-unconfirmed") {
          if (staged !== undefined) {
            await change(r.operationId, (old) => ({ ...finishStop(old, "completed", "too-late"), staged }));
            if (r.jobId !== undefined) updater.releaseHeldJob(r.jobId);
          } else {
            if (r.jobId !== undefined) updater.setHeldJobs([r.jobId]);
            await patch(r.operationId, { phase: "held", code: "adoption-unconfirmed", message: "Stage publication needs exact reconciliation on this host." });
          }
          continue;
        }
        if (r.jobId === undefined) {
          await change(r.operationId, (old) => finishStop(old, old.stop === undefined ? "interrupted" : "stopped", "stopped"));
          continue;
        }
        const join = preparation.reconcile === undefined ? preparation.inspect(r.jobId) : await preparation.reconcile(r.jobId);
        if (join === "running") continue;
        if (join !== "unknown" && join.kind !== "unreaped" && join.kind !== "cleanup-unconfirmed") {
          updater.releaseHeldJob(r.jobId);
          await change(r.operationId, (old) => finishStop(old, old.stop === undefined ? "interrupted" : "stopped", "stopped"));
        } else await held(r, join !== "unknown" && join.kind === "cleanup-unconfirmed" ? "cleanup-unconfirmed" : "unreaped"); // directory/receipt absence is never worker-exit proof
      }
      recoveryFailed = false;
      updater.releaseHeldJob("ledger-unreadable");
    } catch (cause) {
      recoveryFailed = true;
      updater.setHeldJobs(["ledger-unreadable"]);
      throw cause;
    }
  });
  return {
    start: (input: JonesOperationStartInput) => gate(async () => {
      const old = await ledger.read(input.operationId);
      if (old !== null) {
        updater.recordOperation(old);
        return (await ledger.accept(input)).receipt;
      }
      await options.refreshPolicy();
      const s = updater.snapshot();
      const a = input.action;
      let code: string | undefined;
      if (s.capability.operations !== 1) code = "unsupported";
      else if (input.environmentId !== s.environmentId || input.expectedInstalledSource !== s.installedSource?.sha) code = "stale-binding";
      else if (targetOf(input) === s.installedSource?.sha) code = "already-installed";
      else if (a.kind === "download-exact" && s.capability.exactSource !== true) code = "unsupported";
      else if (a.kind === "install" && !bindingMatches(operationReceipt(input, "accepted"), s)) code = "stale-binding";
      else if (a.kind === "download" && (s.provenance?.artifactId !== a.artifactId || s.provenance.sourceSha !== a.sourceSha)) code = "stale-binding";
      else if (a.kind !== "install" && s.stagedHandle !== undefined && s.provenance?.sourceSha !== targetOf(input)) code = "another-stage";
      else if (followerTargetRefusal(s.policy, targetOf(input), s.provenance?.sourceSha === targetOf(input) ? s.provenance.sourceTree : undefined) !== undefined) code = "follower-policy";
      else if (active.size > 0 || recoveryFailed || updater.admissionHeld() || ["preparing", "installing"].includes(s.phase)) code = "busy";
      else if (a.kind === "install" && a.recovery !== undefined) code =
        options.recovery === undefined || s.capability.recoveryPolicy !== 1 ? "recovery-unsupported" :
        options.recovery.validate(a.recovery, { environmentId: input.environmentId,
          stagedHandle: a.stagedHandle, currentVersion: a.currentVersion, expectedInstalledSource: input.expectedInstalledSource });
      if (code !== undefined) return publish((await ledger.refuse(input, code, "This request did not start; refresh the host's exact update state before choosing another action.")).receipt);
      const accepted = await ledger.accept(input);
      if (accepted.created && accepted.receipt.phase === "accepted") {
        const controller = new AbortController();
        active.set(input.operationId, controller);
        // Acceptance is the response; the durable dispatch continues independently of HTTP lifetime.
        void execute(input, controller).catch(async () => {
          const r = await ledger.read(input.operationId);
          if (r !== null && !terminal(r)) {
            if (r.action.kind === "install") await reconcileInstall(r);
            else await held(r);
          }
        }).catch(options.onFailure).finally(() => active.delete(input.operationId));
      }
      return publish(accepted.receipt);
    }),
    stop: (input: JonesOperationStopInput) => gate(async () => {
      if (input.environmentId !== updater.snapshot().environmentId) throw new Error("Stop environment binding changed.");
      const r = await ledger.read(input.operationId);
      if (r === null) throw new Error("Stop outcome is unknown: this operation has no acceptance receipt.");
      if (r.environmentId !== input.environmentId) throw new Error("Stop environment binding changed.");
      if (r.stop !== undefined) return publish(r);
      const stoppable = r.action.kind !== "install" && ["accepted", "running", "stop-requested"].includes(r.phase);
      const receipt = await change(r.operationId, (old) => ({ ...old,
        ...(stoppable ? { phase: "stop-requested" as const } : {}),
        stop: { stopId: input.stopId, requestedAt: new Date().toISOString(),
          ...(stoppable ? {} : { result: r.phase === "completed" && r.staged !== undefined ? "too-late" as const : "not-stoppable-phase" as const }) } }));
      if (stoppable) {
        active.get(r.operationId)?.abort(new PreparationStopRequested(r.operationId));
        updater.stopPreparation(r.operationId);
      }
      return receipt;
    }),
    fence: (input: JonesOperationFenceInput) => gate(async () => {
      if (input.environmentId !== updater.snapshot().environmentId) throw new Error("Fence environment binding changed.");
      const r = await ledger.fence(input);
      if (r.environmentId !== input.environmentId) throw new Error("Fence environment binding changed.");
      return publish(r);
    }),
    read: async (input: JonesOperationReadInput): Promise<JonesOperationReadResult> => {
      if (input.environmentId !== updater.snapshot().environmentId) throw new Error("Read environment binding changed.");
      return { receipts: await Promise.all(input.operationIds.map(async (operationId) => ({ operationId, receipt: await ledger.read(operationId) }))) };
    },
    reconcile,
    sweep: async () => {
      try {
        await preparation.sweep();
      } finally { await reconcile(); }
    },
  };
}

/** Legacy/desktop hosts never claim durable-operation support or fabricate preflight measurements. */
export const unsupportedTargetOperations = {
  start: async (input: JonesOperationStartInput): Promise<JonesOperationReceipt> => ({ ...operationReceipt(input, "refused"), code: "unsupported", message: "Durable target operations are unavailable on this host." }),
  stop: async (_input: JonesOperationStopInput): Promise<JonesOperationReceipt> => { throw new Error("Durable target operations are unavailable on this host."); },
  fence: async (input: JonesOperationFenceInput): Promise<JonesOperationReceipt> => ({ ...operationTombstone(input), phase: "refused", code: "unsupported" }),
  read: async (input: JonesOperationReadInput): Promise<JonesOperationReadResult> => ({ receipts: input.operationIds.map((operationId) => ({ operationId, receipt: null })) }),
};
