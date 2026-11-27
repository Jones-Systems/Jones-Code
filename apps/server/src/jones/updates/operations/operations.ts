// @effect-diagnostics nodeBuiltinImport:off globalDate:off
// Promise adapter owned by JonesUpdates, sharing its existing updater and native operation engine.
import * as NodeCrypto from "node:crypto";
import type {
  JonesOperationStartInput,
  JonesOperationStopInput,
  JonesOperationFenceInput,
  JonesOperationReadInput,
  JonesOperationReadResult,
  JonesOperationReceipt,
  JonesRecoveryPreflightInput,
  JonesAbandonStageInput,
  JonesAbandonStageResult,
  JonesRecoverCleanupInput,
  JonesRecoverCleanupResult,
} from "@t3tools/contracts/jones/jonesOperations";
import type { FleetEnrollment } from "@t3tools/contracts/jones/fleet-updates";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import {
  JonesUpdater,
  PreparationStopRequested,
  type JonesUpdaterInstallInput,
  type JonesInstallBeforeDispatchRefusal,
} from "../JonesUpdater.ts";
import { followerTargetRefusal } from "../followerPolicy.ts";
import type { OperationReconciliation } from "../launcherOperation.ts";
import { inspectHeldPreparationCleanup } from "../preparation/nativeEffectsRecovery.ts";
import type { PreparationSupervisor } from "../preparation/supervisor.ts";
import {
  type FleetFollowerRecord,
  type FleetHostStore,
  serialized,
} from "../../fleetUpdates/store.ts";
import { type OperationLedger, replayOperationFence, replayOperationReceipt } from "./ledger.ts";

const targetOf = (r: JonesOperationStartInput) =>
  r.action.kind === "download" ? r.action.sourceSha : r.action.targetSource;
const terminal = (r: JonesOperationReceipt) =>
  !["accepted", "running", "stop-requested", "adopting", "handed-off", "held"].includes(r.phase);
const beforeWorker = (r: JonesOperationReceipt) =>
  r.acceptedAt !== undefined &&
  r.jobId === undefined &&
  (r.phase === "accepted" || (r.phase === "stop-requested" && r.action.kind !== "install"));
const bindingMatches = (r: JonesOperationStartInput, state: JonesUpdateState) =>
  r.action.kind === "install" &&
  state.environmentId === r.environmentId &&
  state.installedSource?.sha === r.expectedInstalledSource &&
  state.currentVersion === r.action.currentVersion &&
  state.stagedHandle === r.action.stagedHandle &&
  state.provenance?.sourceSha === r.action.targetSource;

const requiresOperationRoute = (input: JonesOperationStartInput) =>
  input.correlation !== undefined || input.admission !== undefined;

function operationRouteRefusal(
  input: JonesOperationStartInput,
  state: JonesUpdateState,
  enrollment: FleetEnrollment | null,
  follower: FleetFollowerRecord | null,
): string | undefined {
  const { admission } = input;
  if (admission === undefined)
    return enrollment === null ? undefined : "admission-binding-required";
  if (input.correlation === undefined) return "admission-route-changed";
  if (admission.kind === "ordinary")
    return enrollment?.enabled === true ? "admission-route-changed" : undefined;
  const directive = follower?.directive;
  const provenance = state.provenance;
  if (
    input.environmentId !== state.environmentId ||
    enrollment?.environmentId !== input.environmentId ||
    enrollment.enabled !== true ||
    enrollment.enrollmentId !== admission.enrollmentId ||
    directive?.enrollmentId !== admission.enrollmentId ||
    directive.directiveId !== admission.directiveId ||
    directive.targetSource !== admission.targetSource ||
    directive.targetTree !== admission.targetTree ||
    targetOf(input) !== admission.targetSource ||
    (provenance?.sourceSha === admission.targetSource &&
      provenance.sourceTree !== admission.targetTree) ||
    (input.action.kind === "install" && !bindingMatches(input, state))
  )
    return "admission-route-changed";
  return undefined;
}

function operationRouteSupported(input: JonesOperationStartInput, state: JonesUpdateState) {
  return (
    state.capability.operations === 1 &&
    (input.admission === undefined || state.capability.boundAdmission === 1)
  );
}

export function createTargetOperations(options: {
  readonly key: string;
  readonly ledger: OperationLedger;
  readonly fleetStore: FleetHostStore;
  readonly updater: JonesUpdater;
  readonly preparation: PreparationSupervisor;
  readonly refreshPolicy: () => Promise<unknown>;
  readonly install: (
    input: JonesUpdaterInstallInput & {
      readonly operationId: string;
      readonly targetSource: string;
      readonly expectedInstalledSource: string;
    },
  ) => Promise<JonesUpdateState>;
  readonly reconcileNative: (id: string) => Promise<OperationReconciliation>;
  readonly resolveNativeHandoff?: (id: string) => Promise<"accepted" | "rejected" | "unknown">;
  readonly onFailure: () => void;
  readonly recovery?: {
    readonly validate: (
      recovery: NonNullable<
        Extract<JonesOperationStartInput["action"], { kind: "install" }>["recovery"]
      >,
      binding: JonesRecoveryPreflightInput,
    ) => string | undefined;
    readonly observed: (operationId: string) => Promise<"clone" | "copy" | undefined>;
  };
}) {
  const { ledger, updater, preparation, fleetStore } = options;
  const active = new Map<string, AbortController>();
  const lifecycleHolds = new Set<string>();
  const cleanupReviews = new Map<
    string,
    {
      readonly token: string;
      readonly expires: number;
      readonly receipt: JonesOperationReceipt;
      readonly apply: NonNullable<Awaited<ReturnType<typeof inspectHeldPreparationCleanup>>>;
    }
  >();
  let recoveryFailed = false;
  const gate = <A>(f: () => Promise<A>) => serialized(`target-operations:${options.key}`, f);
  const publish = (receipt: JonesOperationReceipt) => {
    if (
      receipt.code !== "operation-conflict" &&
      receipt.environmentId === updater.snapshot().environmentId
    )
      updater.recordOperation(receipt);
    return receipt;
  };
  const inEnvironment = (
    receipt: JonesOperationReceipt | null,
    environmentId: JonesOperationReadInput["environmentId"],
  ) => {
    if (receipt !== null && receipt.environmentId !== environmentId)
      throw new Error("Operation receipt environment binding changed.");
    return receipt;
  };
  const readBoundReceipt = async (
    environmentId: JonesOperationReadInput["environmentId"],
    id: string,
  ) => inEnvironment(await ledger.read(id), environmentId);
  const change = async (id: string, f: (old: JonesOperationReceipt) => JonesOperationReceipt) =>
    publish(await ledger.transition(id, f));
  const patch = (id: string, fields: Partial<JonesOperationReceipt>) =>
    change(id, (old) => ({ ...old, ...fields }));
  const recordRefusal = (input: JonesOperationStartInput, code: string) =>
    ledger.refuse(
      input,
      code,
      "This request did not start; refresh the host's exact update state before choosing another action.",
    );
  const replay = (input: JonesOperationStartInput, retained: JonesOperationReceipt) => {
    if (
      retained.environmentId !== updater.snapshot().environmentId &&
      !(retained.phase === "refused" && retained.acceptedAt === undefined)
    )
      throw new Error("Start environment binding changed.");
    return replayOperationReceipt(publish(retained), input);
  };
  const finishStop = (
    r: JonesOperationReceipt,
    phase: JonesOperationReceipt["phase"],
    result: "stopped" | "too-late" | "held",
  ) => {
    const { code, message: _message, ...released } = r;
    return {
      ...(result !== "held" && (code === "unreaped" || code === "cleanup-unconfirmed")
        ? released
        : r),
      phase,
      ...(r.stop === undefined ? {} : { stop: { ...r.stop, result } }),
    };
  };
  const interruptBeforeWorker = (id: string) =>
    change(id, (r) => {
      if (!beforeWorker(r)) return r;
      if (r.action.kind === "install") return { ...r, phase: "interrupted" };
      return finishStop(r, r.stop === undefined ? "interrupted" : "stopped", "stopped");
    });
  const clearLauncherHold = (receipt: JonesOperationReceipt) => {
    const { code, message: _message, ...confirmed } = receipt;
    return code === "launcher-unconfirmed" || code === "launcher-unreadable" ? confirmed : receipt;
  };
  const confirmInstall = (id: string, fields: Partial<JonesOperationReceipt>) =>
    change(id, (old) => ({ ...clearLauncherHold(old), ...fields }));
  const held = (
    r: JonesOperationReceipt,
    code: "unreaped" | "cleanup-unconfirmed" = "unreaped",
  ) => {
    updater.setHeldJobs([r.jobId ?? r.operationId]);
    return change(r.operationId, (old) => ({
      ...finishStop(old, "held", "held"),
      code,
      message:
        "Preparation exit or native cleanup is unproved. Nothing new starts until release is proved.",
    }));
  };
  const stageReceipt = (r: JonesOperationReceipt, state: JonesUpdateState) => {
    const p = state.provenance;
    if (
      state.stagedHandle === undefined ||
      p === undefined ||
      p.sourceSha !== targetOf(r) ||
      (r.admission?.kind === "follower" && p.sourceTree !== r.admission.targetTree) ||
      (r.action.kind === "download" && p.artifactId !== r.action.artifactId) ||
      state.environmentId !== r.environmentId ||
      state.installedSource?.sha !== r.expectedInstalledSource
    )
      return undefined;
    return {
      stagedHandle: state.stagedHandle,
      sourceSha: p.sourceSha,
      sourceTree: p.sourceTree,
      artifactId: p.artifactId,
      ...(p.version === undefined ? {} : { version: p.version }),
    };
  };
  const nativeInstallMatches = (r: JonesOperationReceipt, native: OperationReconciliation) => {
    const a = r.action;
    const b = native.binding;
    return (
      a.kind === "install" &&
      native.operationId === r.operationId &&
      b !== undefined &&
      b.environmentId === r.environmentId &&
      b.currentVersion === a.currentVersion &&
      b.expectedInstalledSource === r.expectedInstalledSource &&
      b.targetSource === a.targetSource &&
      b.stagedHandle === a.stagedHandle
    );
  };
  const refreshExactInstallOutcome = (
    environmentId: JonesOperationReadInput["environmentId"],
    id: string,
  ) =>
    gate(async () => {
      const r = await readBoundReceipt(environmentId, id);
      if (
        r === null ||
        r.action.kind !== "install" ||
        active.has(id) ||
        (!["running", "handed-off", "held"].includes(r.phase) &&
          !(r.phase === "completed" && r.outcome === undefined))
      )
        return r;
      let native = await options.reconcileNative(id);
      const acceptanceRecorded = r.phase === "handed-off" || r.updateId !== undefined;
      // The launcher queues this query after the server's original handoff. File absence
      // alone cannot settle the attempt or allow a second install.
      let rejected = false;
      if (
        !acceptanceRecorded &&
        !terminal(r) &&
        (native.state === "absent" ||
          (nativeInstallMatches(r, native) &&
            native.state === "blocked" &&
            native.updateId === undefined)) &&
        options.resolveNativeHandoff !== undefined
      ) {
        const resolved = await options.resolveNativeHandoff(id);
        rejected = resolved === "rejected";
        native = await options.reconcileNative(id);
      }
      const matches = nativeInstallMatches(r, native);
      const { outcome } = native;
      const fields: Partial<JonesOperationReceipt> | undefined =
        !acceptanceRecorded &&
        ((matches && native.state === "rejected") || (rejected && native.state === "absent"))
          ? {
              phase: "refused",
              code: "launcher-rejected",
              message:
                "The launcher did not accept this install. Review the current build before retrying.",
            }
          : matches &&
              native.updateId === id &&
              outcome !== undefined &&
              outcome.status === native.state
            ? {
                phase: outcome.status === "committed" ? "completed" : outcome.status,
                updateId: id,
                outcome,
              }
            : undefined;
      if (fields === undefined || (terminal(r) && fields.phase !== r.phase)) return r;
      const requested = r.action.recovery?.policy;
      const recovery =
        requested !== undefined && r.recovery === undefined && fields.phase !== "refused"
          ? { requested, observed: await options.recovery?.observed(id) }
          : undefined;
      const changed = new Error("Install receipt changed during native readback.");
      try {
        const receipt = await change(id, (old) => {
          if (JSON.stringify(old) !== JSON.stringify(r)) throw changed;
          return {
            ...clearLauncherHold(old),
            ...fields,
            ...(recovery === undefined
              ? {}
              : {
                  recovery: {
                    requested: recovery.requested,
                    ...(recovery.observed === undefined ? {} : { observed: recovery.observed }),
                  },
                }),
          };
        });
        updater.releaseHeldJob(id);
        return receipt;
      } catch (error) {
        if (error !== changed) throw error;
        return readBoundReceipt(environmentId, id);
      }
    });
  const reconcileInstall = async (
    r: JonesOperationReceipt,
    refusal?: JonesInstallBeforeDispatchRefusal,
  ) => {
    const native = await options.reconcileNative(r.operationId);
    const acceptanceRecorded = r.phase === "handed-off" || r.updateId !== undefined;
    if (native.state === "absent") {
      const a = r.action;
      if (
        !acceptanceRecorded &&
        a.kind === "install" &&
        refusal?.kind === "before-dispatch" &&
        refusal.operationId === r.operationId &&
        refusal.environmentId === r.environmentId &&
        refusal.expectedInstalledSource === r.expectedInstalledSource &&
        refusal.targetSource === a.targetSource &&
        refusal.stagedHandle === a.stagedHandle &&
        refusal.currentVersion === a.currentVersion
      ) {
        updater.releaseHeldJob(r.operationId);
        return patch(r.operationId, {
          phase: "refused",
          code: bindingMatches(r, updater.snapshot()) ? "install-refused" : "stale-binding",
          message:
            "The installation was refused before native acceptance. The staged selection was retained.",
        });
      }
      updater.setHeldJobs([r.operationId]);
      return patch(r.operationId, {
        code: "launcher-unconfirmed",
        message: "Native acceptance is unknown. This operation will not be sent again.",
      });
    }
    const a = r.action;
    if (
      !nativeInstallMatches(r, native) ||
      (acceptanceRecorded && native.state === "rejected") ||
      (native.state === "blocked" && native.updateId !== r.operationId)
    ) {
      updater.setHeldJobs([r.operationId]);
      return patch(r.operationId, {
        code: "launcher-unreadable",
        message: "The launcher's exact operation binding could not be proved.",
      });
    }
    if (native.state === "pending") {
      updater.setHeldJobs([r.operationId]);
      return confirmInstall(r.operationId, { phase: "handed-off", updateId: r.operationId });
    }
    updater.releaseHeldJob(r.operationId);
    if (native.state === "rejected")
      return confirmInstall(r.operationId, { phase: "refused", code: "launcher-rejected" });
    if (a.kind === "install" && a.recovery !== undefined && r.recovery === undefined) {
      const observed = await options.recovery?.observed(r.operationId);
      await patch(r.operationId, {
        recovery: { requested: a.recovery.policy, ...(observed === undefined ? {} : { observed }) },
      });
    }
    const snapshot = updater.snapshot();
    const outcome =
      native.outcome ?? (snapshot.updateId === r.operationId ? snapshot.outcome : undefined);
    return confirmInstall(r.operationId, {
      phase: native.state === "committed" ? "completed" : native.state,
      updateId: r.operationId,
      ...(outcome === undefined ? {} : { outcome }),
    });
  };
  const execute = async (input: JonesOperationStartInput, controller: AbortController) => {
    const id = input.operationId;
    let receipt = (await ledger.read(id))!;
    if (input.action.kind !== "install" && receipt.stop !== undefined) {
      await change(id, (r) => finishStop(r, "stopped", "stopped"));
      return;
    }
    if (input.action.kind === "install") {
      if (input.admission?.kind === "follower") {
        const state = updater.snapshot();
        if (
          !bindingMatches(input, state) ||
          state.provenance?.sourceTree !== input.admission.targetTree
        ) {
          await patch(id, { phase: "refused", code: "stale-binding" });
          return;
        }
      }
      receipt = await patch(id, { phase: "running" }); // durable dispatch marker, before the native call
      const action = input.action;
      let refusal: JonesInstallBeforeDispatchRefusal | undefined;
      await options.install({
        ...action,
        operationId: id,
        environmentId: input.environmentId,
        expectedInstalledSource: input.expectedInstalledSource,
        origin: "manual",
        onBeforeDispatchRefusal: (proof) => {
          refusal = proof;
        },
      });
      await reconcileInstall(receipt, refusal);
      return;
    }
    // Discovery leaves the receipt accepted. Only the supervisor can attach a job after
    // reserving its durable pre-spawn evidence, and it awaits the receipt before launch.
    const operation = {
      operationId: id,
      signal: controller.signal,
      onReserved: async (jobId: string) => {
        if (input.admission?.kind === "follower") {
          const provenance = updater.snapshot().provenance;
          if (
            provenance?.sourceSha !== input.admission.targetSource ||
            provenance.sourceTree !== input.admission.targetTree
          )
            throw new Error(
              "The discovered artifact does not match this operation's source and tree.",
            );
        }
        await change(id, (r) => {
          if (r.stop !== undefined || controller.signal.aborted)
            throw new PreparationStopRequested(id);
          if (r.phase !== "accepted" || r.jobId !== undefined)
            throw new Error("The preparation reservation no longer matches this operation.");
          return { ...r, phase: "running", jobId };
        });
      },
      onAdopting: async () => {
        await change(id, (r) => {
          if (r.stop !== undefined || controller.signal.aborted)
            throw new PreparationStopRequested(id);
          if (r.phase !== "running" || r.jobId === undefined)
            throw new Error("The staging worker was not bound to this operation.");
          return { ...r, phase: "adopting" };
        });
      },
    };
    const state =
      input.action.kind === "download-exact"
        ? await updater.stageExact(input.action.targetSource, undefined, operation)
        : await updater.download(input.action, operation);
    receipt = (await ledger.read(id))!;
    if (terminal(receipt)) return;
    const join = receipt.jobId === undefined ? undefined : preparation.inspect(receipt.jobId);
    if (
      join === "unknown" ||
      join === "running" ||
      join?.kind === "unreaped" ||
      join?.kind === "cleanup-unconfirmed"
    ) {
      await held(
        receipt,
        typeof join !== "string" && join?.kind === "cleanup-unconfirmed"
          ? "cleanup-unconfirmed"
          : "unreaped",
      );
      return;
    }
    const staged = stageReceipt(receipt, state);
    if (staged !== undefined) {
      await change(id, (r) =>
        r.stop !== undefined && r.phase !== "adopting"
          ? finishStop(r, "stopped", "stopped")
          : {
              ...finishStop(r, "completed", "too-late"),
              staged: {
                ...staged,
                ...(r.phase === "adopting" ? { createdByOperation: true as const } : {}),
              },
            },
      );
    } else if (receipt.phase === "adopting") {
      updater.setHeldJobs([receipt.jobId ?? id]);
      await patch(id, {
        phase: "held",
        code: "adoption-unconfirmed",
        message: "Stage publication needs reconciliation on this host.",
      });
    } else {
      await change(id, (r) =>
        r.stop !== undefined
          ? finishStop(r, "stopped", "stopped")
          : {
              ...r,
              phase: "failed",
              code: "download-failed",
              message: "The selected source was not staged. Nothing was installed.",
            },
      );
    }
  };
  const reconcile = () =>
    gate(async () => {
      try {
        let unresolved: Set<string> | undefined;
        const ledgerHolds = new Set<string>();
        if (preparation.unreleasedJobs !== undefined) {
          unresolved = new Set(await preparation.unreleasedJobs());
          updater.setHeldJobs([...unresolved]);
          for (const id of unresolved) lifecycleHolds.add(id);
        }
        for await (const r of ledger.listUnsettled()) {
          if (active.has(r.operationId)) continue;
          if (beforeWorker(r)) {
            await interruptBeforeWorker(r.operationId);
            continue;
          }
          if (r.action.kind === "install") {
            await reconcileInstall(r);
            continue;
          }
          if (r.jobId === undefined) {
            ledgerHolds.add(r.operationId);
            await held(r);
            continue;
          }
          const staged = stageReceipt(r, updater.snapshot());
          if (r.phase === "adopting" || r.code === "adoption-unconfirmed") {
            if (staged !== undefined) {
              await change(r.operationId, (old) => ({
                ...finishStop(old, "completed", "too-late"),
                staged,
              }));
              updater.releaseHeldJob(r.jobId);
            } else {
              ledgerHolds.add(r.jobId);
              updater.setHeldJobs([r.jobId]);
              await patch(r.operationId, {
                phase: "held",
                code: "adoption-unconfirmed",
                message: "Stage publication needs exact reconciliation on this host.",
              });
            }
            continue;
          }
          const join =
            preparation.reconcile === undefined
              ? preparation.inspect(r.jobId)
              : await preparation.reconcile(r.jobId);
          if (join === "running") {
            ledgerHolds.add(r.jobId);
            continue;
          }
          if (
            join !== "unknown" &&
            join.kind !== "unreaped" &&
            join.kind !== "cleanup-unconfirmed"
          ) {
            updater.releaseHeldJob(r.jobId);
            await change(r.operationId, (old) =>
              finishStop(old, old.stop === undefined ? "interrupted" : "stopped", "stopped"),
            );
          } else {
            ledgerHolds.add(r.jobId);
            await held(
              r,
              join !== "unknown" && join.kind === "cleanup-unconfirmed"
                ? "cleanup-unconfirmed"
                : "unreaped",
            ); // directory/receipt absence is never worker-exit proof
          }
        }
        // Apply release transitions only after the whole read succeeds. A failed reconcile
        // preserves admission holds and cleanup demand for a later maintenance pass.
        if (unresolved !== undefined) {
          for (const id of lifecycleHolds)
            if (!unresolved.has(id)) {
              // Worker release does not prove that its staged result was adopted.
              if (!ledgerHolds.has(id)) updater.releaseHeldJob(id);
              preparation.requestSweep?.();
            }
          updater.setHeldJobs([...unresolved, ...ledgerHolds]);
          lifecycleHolds.clear();
          for (const id of unresolved) lifecycleHolds.add(id);
        }
        recoveryFailed = false;
        updater.releaseHeldJob("ledger-unreadable");
      } catch (cause) {
        recoveryFailed = true;
        updater.setHeldJobs(["ledger-unreadable"]);
        preparation.requestSweep?.();
        throw cause;
      }
    });
  const admissionBusy = (state: JonesUpdateState) =>
    active.size > 0 ||
    recoveryFailed ||
    updater.admissionHeld() ||
    ["preparing", "installing"].includes(state.phase);
  const startRefusal = (input: JonesOperationStartInput, s: JonesUpdateState) => {
    const a = input.action;
    const target = targetOf(input);
    if (!operationRouteSupported(input, s)) return "operation-admission-disabled";
    if (
      input.environmentId !== s.environmentId ||
      input.expectedInstalledSource !== s.installedSource?.sha
    )
      return "stale-binding";
    if (target === s.installedSource?.sha) return "already-installed";
    if (a.kind === "install" ? !s.capability.install : !s.capability.download)
      return "action-unsupported";
    if (a.kind === "download-exact" && (s.capability.exactSource !== true || !s.capability.check))
      return "exact-source-unsupported";
    if (a.kind === "install" && !bindingMatches(input, s)) return "stale-binding";
    if (
      a.kind === "download" &&
      (s.provenance?.artifactId !== a.artifactId || s.provenance.sourceSha !== a.sourceSha)
    )
      return "stale-binding";
    if (a.kind !== "install" && s.stagedHandle !== undefined && s.provenance?.sourceSha !== target)
      return "another-stage";
    if (
      !requiresOperationRoute(input) &&
      followerTargetRefusal(
        s.policy,
        target,
        s.provenance?.sourceSha === target ? s.provenance.sourceTree : undefined,
      ) !== undefined
    )
      return "follower-policy";
    if (admissionBusy(s)) return "busy";
    if (a.kind === "install" && a.recovery !== undefined)
      return options.recovery === undefined || s.capability.recoveryPolicy !== 1
        ? "recovery-unsupported"
        : options.recovery.validate(a.recovery, {
            environmentId: input.environmentId,
            stagedHandle: a.stagedHandle,
            currentVersion: a.currentVersion,
            expectedInstalledSource: input.expectedInstalledSource,
          });
    return undefined;
  };
  const start = async (input: JonesOperationStartInput) => {
    const requiresRoute = requiresOperationRoute(input);
    const admission = await gate(async () => {
      const old = await ledger.read(input.operationId);
      if (old !== null) return { kind: "receipt" as const, receipt: replay(input, old) };
      if (!requiresRoute) {
        const state = updater.snapshot();
        const code = !operationRouteSupported(input, state)
          ? "operation-admission-disabled"
          : admissionBusy(state)
            ? "busy"
            : undefined;
        if (code !== undefined)
          return {
            kind: "receipt" as const,
            receipt: publish((await recordRefusal(input, code)).receipt),
          };
      }
      return { kind: "reservation" as const, reservation: updater.reserveCheckAdmission() };
    });
    if (admission.kind === "receipt") return admission.receipt;
    try {
      // A lost send can still be fenced while this request joins discovery.
      await admission.reservation.ready;
      const result = await gate(async () => {
        const old = await ledger.read(input.operationId);
        if (old !== null) return { receipt: replay(input, old) };
        await options.refreshPolicy();
        const record = (state: JonesUpdateState, routeRefusal?: string) => {
          const code = routeRefusal ?? startRefusal(input, state);
          return code === undefined ? ledger.accept(input) : recordRefusal(input, code);
        };
        // Take the fleet writer lock before the receipt lock to order admission against enrollment
        // and directive edits. Refresh and worker dispatch are outside this critical section.
        const accepted = requiresRoute
          ? await fleetStore.withConfirmedOperationRoute((enrollment, follower) => {
              const state = updater.snapshot();
              return record(state, operationRouteRefusal(input, state, enrollment, follower));
            })
          : await record(updater.snapshot());
        const receipt = publish(accepted.receipt);
        if (!accepted.created || receipt.phase !== "accepted") return { receipt };
        const controller = new AbortController();
        active.set(input.operationId, controller);
        return { receipt, controller };
      });
      if (result.controller !== undefined) {
        // Only the creator dispatches, independently of the HTTP response.
        void execute(input, result.controller)
          .catch(async () => {
            const r = await ledger.read(input.operationId);
            if (r !== null && !terminal(r)) {
              if (r.action.kind === "install") await reconcileInstall(r);
              else if (beforeWorker(r)) await interruptBeforeWorker(r.operationId);
              else await held(r);
            }
          })
          .catch(options.onFailure)
          .finally(() => active.delete(input.operationId));
      }
      return result.receipt;
    } finally {
      admission.reservation.release();
    }
  };
  return {
    start,
    stop: (input: JonesOperationStopInput) =>
      gate(async () => {
        if (input.environmentId !== updater.snapshot().environmentId)
          throw new Error("Stop environment binding changed.");
        const r = await ledger.read(input.operationId);
        if (r === null)
          throw new Error("Stop outcome is unknown: this operation has no acceptance receipt.");
        if (r.environmentId !== input.environmentId)
          throw new Error("Stop environment binding changed.");
        if (r.stop !== undefined) return publish(r);
        const receipt = await change(r.operationId, (old) => {
          if (old.stop !== undefined) return old;
          const stoppable =
            old.action.kind !== "install" &&
            ["accepted", "running", "stop-requested"].includes(old.phase);
          return {
            ...old,
            ...(stoppable ? { phase: "stop-requested" } : {}),
            stop: {
              stopId: input.stopId,
              requestedAt: new Date().toISOString(),
              ...(stoppable
                ? {}
                : {
                    result:
                      old.phase === "completed" && old.staged !== undefined
                        ? "too-late"
                        : "not-stoppable-phase",
                  }),
            },
          };
        });
        if (receipt.phase === "stop-requested") {
          active.get(r.operationId)?.abort(new PreparationStopRequested(r.operationId));
          updater.stopPreparation(r.operationId);
        }
        return receipt;
      }),
    fence: (input: JonesOperationFenceInput) =>
      gate(async () => {
        if (input.environmentId !== updater.snapshot().environmentId)
          throw new Error("Fence environment binding changed.");
        if (!operationRouteSupported(input, updater.snapshot())) {
          const retained = await readBoundReceipt(input.environmentId, input.operationId);
          if (retained === null) throw new Error("Durable target operation admission is disabled.");
          return publish(replayOperationFence(retained, input));
        }
        const r = await ledger.fence(input);
        if (r.environmentId !== input.environmentId)
          throw new Error("Fence environment binding changed.");
        return publish(r);
      }),
    read: async (input: JonesOperationReadInput): Promise<JonesOperationReadResult> => {
      if (input.environmentId !== updater.snapshot().environmentId)
        throw new Error("Read environment binding changed.");
      const refresh = input.refreshInstallOutcome;
      if (refresh !== undefined && input.inspectCleanup !== undefined)
        throw new Error("Choose one exact update readback mode.");
      if (
        refresh !== undefined &&
        input.operationIds.length === 1 &&
        input.operationIds[0] === refresh
      ) {
        return {
          receipts: [
            {
              operationId: refresh,
              receipt: await refreshExactInstallOutcome(input.environmentId, refresh),
            },
          ],
        };
      }
      const receipts = (await ledger.readMany(input.operationIds)).map(
        ({ operationId, receipt }) => ({
          operationId,
          receipt: inEnvironment(receipt, input.environmentId),
        }),
      );
      const operationId = input.inspectCleanup;
      if (operationId === undefined) return { receipts };
      const receipt = receipts[0]?.receipt;
      const apply =
        updater.snapshot().capability.operations === 1 &&
        receipts.length === 1 &&
        receipts[0]?.operationId === operationId &&
        receipt?.operationId === operationId &&
        receipt.environmentId === input.environmentId
          ? await inspectHeldPreparationCleanup(options.key, receipt)
          : undefined;
      const current = apply === undefined ? null : await ledger.read(operationId);
      if (
        receipt == null ||
        apply === undefined ||
        JSON.stringify(current) !== JSON.stringify(receipt)
      )
        return { receipts, cleanupInspection: { operationId, status: "unproved" } };
      const now = performance.now();
      for (const [id, review] of cleanupReviews)
        if (review.expires <= now) cleanupReviews.delete(id);
      const token = NodeCrypto.randomUUID();
      cleanupReviews.delete(operationId);
      if (cleanupReviews.size >= 16) {
        const oldest = cleanupReviews.keys().next().value;
        if (oldest !== undefined) cleanupReviews.delete(oldest);
      }
      cleanupReviews.set(operationId, { token, expires: now + 120_000, receipt, apply });
      return {
        receipts,
        cleanupInspection: { operationId, status: "eligible", recoveryToken: token },
      };
    },
    abandonStage: (input: JonesAbandonStageInput): Promise<JonesAbandonStageResult> =>
      gate(async () => {
        const r = await ledger.read(input.operationId);
        const s = updater.snapshot();
        const refused = (reason: string) => ({ receipt: null, reason });
        if (
          input.confirmed !== true ||
          input.environmentId !== s.environmentId ||
          r?.environmentId !== input.environmentId ||
          r.correlation?.planId !== input.planId ||
          r.action.kind === "install" ||
          r.phase !== "completed" ||
          r.staged?.createdByOperation !== true ||
          r.staged.stagedHandle !== input.stagedHandle
        )
          return refused("This plan does not own the staged selection.");
        if (r.stageRetiredAt !== undefined) return { receipt: r };
        if (
          s.capability.abandonStage !== 1 ||
          active.size > 0 ||
          recoveryFailed ||
          s.currentVersion === undefined ||
          s.installedSource?.sha !== r.expectedInstalledSource ||
          (s.stagedHandle !== undefined &&
            (s.stagedHandle !== r.staged.stagedHandle ||
              s.provenance?.sourceTree !== r.staged.sourceTree))
        )
          return refused(
            "The running build, stage, or update activity changed. Refresh the device.",
          );
        const retired = await updater.retireStagedOperation({
          operationId: r.operationId,
          environmentId: r.environmentId,
          stagedHandle: r.staged.stagedHandle,
          targetSource: r.staged.sourceSha,
          expectedInstalledSource: r.expectedInstalledSource,
          currentVersion: s.currentVersion,
        });
        if (!retired.retired)
          return refused(
            (retired.reason ?? "The native stage could not be retired.").slice(0, 1024),
          );
        return {
          receipt: await patch(r.operationId, { stageRetiredAt: new Date().toISOString() }),
        };
      }),
    recoverCleanup: (input: JonesRecoverCleanupInput): Promise<JonesRecoverCleanupResult> =>
      gate(async () => {
        const refused = { recoveredCleanup: null };
        const { operationId, environmentId, jobId, recoveryToken } = input;
        const review = cleanupReviews.get(operationId);
        const state = updater.snapshot();
        if (
          input.confirmed !== true ||
          state.capability.operations !== 1 ||
          environmentId !== state.environmentId ||
          active.has(operationId) ||
          review === undefined ||
          review.token !== recoveryToken ||
          review.expires <= performance.now() ||
          review.receipt.environmentId !== environmentId ||
          review.receipt.jobId !== jobId
        )
          return refused;
        const unchanged = (receipt: JonesOperationReceipt | null) =>
          JSON.stringify(receipt) === JSON.stringify(review.receipt);
        if (!unchanged(await ledger.read(operationId))) return refused;
        cleanupReviews.delete(operationId);
        if (
          !(await review.apply(() => {
            if (performance.now() >= review.expires) throw new Error("Cleanup review expired.");
          }))
        )
          return refused;
        const recoveredCleanup = await change(operationId, (old) => {
          if (!unchanged(old)) throw new Error("Cleanup operation binding changed.");
          return {
            ...finishStop(old, old.stop === undefined ? "interrupted" : "stopped", "stopped"),
            code: "cleanup-recovered",
            message: "Cleanup was verified and the preparation hold was released.",
          };
        });
        lifecycleHolds.delete(jobId);
        updater.releaseHeldJob(jobId);
        preparation.requestSweep?.();
        return { recoveredCleanup };
      }),
    reconcile,
    sweep: async () => {
      await reconcile();
      try {
        // Real supervisors consume demand; legacy test/adapter implementations retain their API.
        if (preparation.sweepIfNeeded !== undefined) await preparation.sweepIfNeeded();
        else await preparation.sweep();
      } finally {
        await reconcile();
      }
    },
  };
}

/** Legacy/desktop hosts never claim durable-operation support or fabricate preflight measurements. */
const unsupportedTargetOperation = async (_input: unknown): Promise<never> => {
  throw new Error("Durable target operations are unavailable on this host.");
};
export const unsupportedTargetOperations = {
  start: unsupportedTargetOperation,
  stop: unsupportedTargetOperation,
  fence: unsupportedTargetOperation,
  read: unsupportedTargetOperation,
  abandonStage: async (_input: JonesAbandonStageInput): Promise<JonesAbandonStageResult> => ({
    receipt: null,
    reason: "Staged update retirement is unavailable on this host.",
  }),
  recoverCleanup: async (_input: JonesRecoverCleanupInput): Promise<JonesRecoverCleanupResult> => ({
    recoveredCleanup: null,
  }),
};
