// @effect-diagnostics nodeBuiltinImport:off globalDate:off - Native coordinator supplies clocks and validates captured worker exit evidence.
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as NodeCrypto from "node:crypto";
import { canonicalJonesCommand } from "@t3tools/client-runtime/jones/updates/action-journal";
import type { EnvironmentId } from "@t3tools/contracts";
import {
  JonesInstallPlanRequest as RequestSchema,
  type JonesInstallPlanRequest,
  type JonesInstallPlanResult,
  type JonesInstallPlanState,
  type JonesInstallPlan,
  type JonesInstallPlanDevice,
  type JonesInstallPlanLaptopEvidence,
  type JonesInstallPlanLaptopOperation,
  type JonesInstallPlanHostOperation,
  type JonesInstallPlanGrant,
  type JonesInstallPlanReview,
} from "@t3tools/contracts/jones/installPlan";
import type { JonesOperationAdmission } from "@t3tools/contracts/jones/jonesOperations";
import {
  startPlan,
  applyEdit,
  canSupersede,
  isPlanComplete,
  validateReceipt,
  validateOperationEvidence,
  validateDirectiveEvidence,
  validateDirectiveFenceEvidence,
  hasUnconfirmedLauncherOutcome,
  isProvisionalUnsupportedReceipt,
  isSettledOperationReceipt,
  planHasLaptopRecoveryDuty,
  deviceActiveOperations,
  deviceStagedReceipt,
  canRecordRunningTarget,
  operationOutstanding,
} from "@t3tools/client-runtime/jones/install-plan/model";
import { RecoveryPreflightRefused } from "../updates/recoveryPreflight.ts";
import type { JonesDesktopUpdateController } from "../updates/JonesDesktopUpdates.ts";
import {
  PlanStorageError,
  MAX_PLAN_COMMANDS,
  MAX_PLAN_DISPOSITIONS,
  closedPlans,
  retainedPlans,
  type InstallPlanStore,
  type PlanSnapshot,
  type PlanDocument,
  type PlanDispositions,
} from "./store.ts";
import {
  admitPlanWork,
  archiveCurrent,
  commitRevision,
  documentRevision,
  heldByClosedPlan,
  matchesOrphanReceipt,
  orphanedPlanCommands,
  receiptDetailsFit,
  recoveryHistory,
  replaceRetainedPlan,
} from "./journal.ts";

export interface LaptopInstallBinding {
  readonly operationId: string;
  readonly stagedHandle: string;
  readonly targetSource: string;
  readonly targetTree: string;
  readonly transactionId: string;
  readonly fromGeneration: string;
}
export interface LaptopNativeOutcome {
  readonly transactionId: string;
  readonly status: "committed" | "rolled-back";
  readonly activeGeneration: string;
  readonly activeSource: string;
  readonly activeTree: string;
}
export type LaptopStartupPreimage = NonNullable<JonesInstallPlanLaptopOperation["prebinding"]> & {
  readonly environmentId: EnvironmentId;
  readonly currentVersion: string;
  readonly targetSource: string;
  readonly targetTree: string;
};
const localPending = (operation: JonesInstallPlanLaptopOperation) =>
  operation.result === undefined ||
  operation.result.code === "native-unconfirmed" ||
  ["running", "stop-requested", "held"].includes(operation.result.status);
const outstanding = operationOutstanding;
const preservesHeldReason = (
  previous: JonesInstallPlanHostOperation["receipt"],
  next: NonNullable<JonesInstallPlanHostOperation["receipt"]>,
) =>
  previous === undefined ||
  previous.phase !== next.phase ||
  ((!hasUnconfirmedLauncherOutcome(previous) || hasUnconfirmedLauncherOutcome(next)) &&
    (previous.code !== "cleanup-unconfirmed" || next.code === previous.code));
const same = (left: unknown, right: unknown): boolean => {
  if (left === right) return true;
  if (typeof left !== "object" || left === null || typeof right !== "object" || right === null)
    return false;
  const keys = Object.keys(left).sort();
  return (
    sameKeys(keys, Object.keys(right).sort()) &&
    keys.every((key) =>
      same((left as Record<string, unknown>)[key], (right as Record<string, unknown>)[key]),
    )
  );
};
const sameKeys = (a: readonly string[], b: readonly string[]) =>
  a.length === b.length && a.every((v, i) => v === b[i]);
const MAX_OPERATIONS = 16;
const LEASE_MS = 120_000;
const PAUSED_GATE_MESSAGE =
  "Laptop verification is temporarily unavailable. Check the Laptop before continuing.";
const DEVICE_RECOVERY_HOLD = "An earlier plan still needs recovery on this device.";

const ordinaryProof = (evidence: JonesInstallPlanLaptopEvidence) => {
  const proof = evidence.ordinaryRecovery;
  return proof !== undefined &&
    evidence.committedTransactionId === undefined &&
    proof.activeGeneration === evidence.activeGeneration &&
    proof.transactionId === `manual-${proof.activeGeneration}`
    ? proof
    : undefined;
};

const verificationBlocked = (evidence: JonesInstallPlanLaptopEvidence) =>
  evidence.activationBlocked || evidence.reopenToCheckInstall === true;

type RolloutGate = NonNullable<JonesInstallPlan["laptop"]["gate"]>;
function rolloutGateStatus(
  environmentId: EnvironmentId,
  gate: RolloutGate,
  evidence: JonesInstallPlanLaptopEvidence,
): "verified" | "paused" | "changed" {
  const identityKeys = ["activeGeneration", "activeSource", "activeTree"] as const;
  const ordinary = gate.verificationKind === "ordinary-recovery";
  const recorded = gate.ordinaryRecovery;
  if (
    (ordinary
      ? recorded === undefined ||
        recorded.transactionId !== gate.transactionId ||
        recorded.activeGeneration !== gate.activeGeneration
      : recorded !== undefined) ||
    (evidence.environmentId !== undefined && evidence.environmentId !== environmentId) ||
    identityKeys.some((key) => evidence[key] !== undefined && evidence[key] !== gate[key]) ||
    (evidence.committedTransactionId !== undefined &&
      (ordinary || evidence.committedTransactionId !== gate.transactionId)) ||
    (evidence.ordinaryRecovery !== undefined &&
      (!ordinary || !same(recorded, evidence.ordinaryRecovery)))
  )
    return "changed";

  const proofAvailable = ordinary
    ? ordinaryProof(evidence) !== undefined
    : evidence.committedTransactionId !== undefined;
  return !verificationBlocked(evidence) &&
    proofAvailable &&
    evidence.environmentId === environmentId &&
    identityKeys.every((key) => evidence[key] === gate[key])
    ? "verified"
    : "paused";
}

function manualCloseReview(
  plan: JonesInstallPlan,
  evidence: JonesInstallPlanLaptopEvidence,
): JonesInstallPlanState["view"]["manualClose"] {
  // Closing records the bound running build. The publication path separately requires rollout proof.
  const { activeSource, activeTree, activeVersion, activeGeneration } = evidence;
  if (
    evidence.environmentId !== plan.laptop.environmentId ||
    verificationBlocked(evidence) ||
    activeGeneration === undefined ||
    activeSource === undefined ||
    activeTree === undefined ||
    activeVersion === undefined ||
    activeVersion.length > 128
  )
    return undefined;

  const closedGate = plan.laptop.gateClosed;
  const differentBuild =
    activeSource !== plan.target.sourceSha || activeTree !== plan.target.sourceTree;
  const changedFromStart =
    activeVersion !== plan.from.version ||
    (plan.from.sourceSha !== undefined && activeSource !== plan.from.sourceSha);
  if (
    closedGate?.reason !== "changed" &&
    !(
      (plan.laptop.gate === undefined || closedGate !== undefined) &&
      differentBuild &&
      changedFromStart
    )
  )
    return undefined;

  return {
    activeGeneration,
    laptop: { version: activeVersion, sourceSha: activeSource, sourceTree: activeTree },
  };
}

const commandBody = (request: JonesInstallPlanRequest) =>
  NodeCrypto.createHash("sha256").update(canonicalJonesCommand(request)).digest("hex");

function matchesOperationTarget(
  plan: JonesInstallPlan,
  device: JonesInstallPlanDevice,
  operation: JonesInstallPlanHostOperation,
): boolean {
  const request = operation.request;
  if (request.input.environmentId !== device.environmentId) return false;
  if (request.kind === "plan-directive") {
    const input = request.input;
    return (
      operation.step === "directive" &&
      input.directiveId === operation.operationId &&
      input.repository === plan.target.repository &&
      input.targetSource === plan.target.sourceSha &&
      input.targetTree === plan.target.sourceTree
    );
  }
  const { action, admission, correlation, operationId } = request.input;
  return (
    operationId === operation.operationId &&
    operation.step !== "directive" &&
    (operation.step === "install") === (action.kind === "install") &&
    (action.kind === "download" ? action.sourceSha : action.targetSource) ===
      plan.target.sourceSha &&
    (admission?.kind !== "follower" ||
      (admission.targetSource === plan.target.sourceSha &&
        admission.targetTree === plan.target.sourceTree)) &&
    correlation?.planId === plan.planId &&
    correlation.deviceKey === device.environmentId
  );
}

function appliedFollowerAdmission(
  plan: JonesInstallPlan,
  device: JonesInstallPlanDevice,
  operation: JonesInstallPlanHostOperation | undefined,
): Extract<JonesOperationAdmission, { kind: "follower" }> | undefined {
  if (
    operation?.request.kind !== "plan-directive" ||
    operation.directive?.kind !== "applied" ||
    !matchesOperationTarget(plan, device, operation)
  )
    return undefined;
  const { enrollmentId, directiveId, targetSource, targetTree } = operation.request.input;
  const { directive } = operation.directive;
  if (
    enrollmentId !== device.enrollmentId ||
    directive.directiveId !== directiveId ||
    directive.targetSource !== targetSource ||
    directive.targetTree !== targetTree ||
    directive.mode !== "manual-review" ||
    !Number.isFinite(Date.parse(directive.issuedAt))
  )
    return undefined;
  return { kind: "follower", enrollmentId, directiveId, targetSource, targetTree };
}

function refreshCommands(document: PlanDocument): PlanDocument {
  if (document.commands === undefined) return document;
  let changed = false;
  const commands = document.commands.map((entry) => {
    const receipt =
      entry.receipt.bodyHash === undefined
        ? { ...entry.receipt, bodyHash: entry.body }
        : entry.receipt;
    if (receipt !== entry.receipt) changed = true;
    const wasUnsupported = receipt.outcome === "terminal" && receipt.code === "unsupported";
    if (receipt.outcome !== "reserved" && !wasUnsupported)
      return receipt === entry.receipt ? entry : { ...entry, receipt };
    const plan = retainedPlans(document).find((p) => p.planId === receipt.planId);
    const { environmentId, action, targetSource, stagedHandle } = receipt.command;
    if (plan === undefined || plan.target.sourceSha !== targetSource)
      return receipt === entry.receipt ? entry : { ...entry, receipt };
    const step = action === "install" ? "install" : "download";
    const local =
      plan.laptop.environmentId === environmentId && receipt.requestType !== "claim"
        ? plan.laptop.operations.find(
            (op) => op.operationId === receipt.operationId && op.step === step,
          )
        : undefined;
    const device = plan.devices.find((d) => d.environmentId === environmentId);
    const remote =
      receipt.requestType === "claim" && device !== undefined
        ? device.operations.find(
            (op) =>
              op.operationId === receipt.operationId &&
              op.step === step &&
              matchesOperationTarget(plan, device, op),
          )
        : undefined;
    const settled =
      local !== undefined
        ? !localPending(local) && local.review?.stagedHandle === stagedHandle
        : remote?.receipt !== undefined &&
          !outstanding(remote) &&
          remote.review?.stagedHandle === stagedHandle;
    if (
      !settled ||
      (wasUnsupported && (remote?.receipt === undefined || remote.receipt.code === "unsupported"))
    )
      return receipt === entry.receipt ? entry : { ...entry, receipt };
    changed = true;
    const code = local?.result?.code ?? remote?.receipt?.code;
    const { code: _previousCode, ...binding } = receipt;
    return {
      ...entry,
      receipt: {
        ...binding,
        outcome: "terminal" as const,
        ...(code === undefined ? {} : { code }),
      },
    };
  });
  return changed ? { ...document, commands } : document;
}

/** Only ESRCH for both captured identities proves a stopped transfer after restart. No signals are sent. */
function workerAbsent(worker: { readonly pid: number; readonly processGroup: number }): boolean {
  if (![worker.pid, worker.processGroup].every((pid) => Number.isSafeInteger(pid) && pid > 1))
    return false;
  const absent = (pid: number) => {
    try {
      process.kill(pid, 0);
      return false;
    } catch (cause) {
      return (cause as NodeJS.ErrnoException).code === "ESRCH";
    }
  };
  return absent(-worker.processGroup) && absent(worker.pid);
}

const compiledIs1 = Schema.is(RequestSchema);
const decodeExactHostRequest = Schema.decodeUnknownOption(RequestSchema, {
  onExcessProperty: "error",
});

export function createInstallPlanCoordinator(options: {
  readonly store: InstallPlanStore;
  readonly controller: Pick<
    JonesDesktopUpdateController,
    | "state"
    | "busy"
    | "laptopEvidence"
    | "retiredLaptopInstall"
    | "download"
    | "stopDownload"
    | "discard"
    | "laptopStageSelectionName"
    | "isLaptopStageAbsent"
    | "install"
    | "isStaged"
    | "stagedBuild"
    | "recoveryPreflight"
    | "validateRecoveryReview"
  >;
  readonly localEnvironmentId: () => EnvironmentId | undefined;
  readonly track: <A>(work: () => Promise<A>) => Promise<A>;
  readonly broadcast: (state: JonesInstallPlanState) => void;
  readonly now: () => number;
  readonly uuid: () => string;
  readonly workerAbsent?: typeof workerAbsent;
}) {
  const active = new Set<string>();
  let disposed = false;
  let lastState: JonesInstallPlanState | undefined;
  const now = () => new Date(options.now()).toISOString();
  const stateOf = (
    snapshot: PlanSnapshot,
    evidence: JonesInstallPlanLaptopEvidence,
    document = snapshot.document,
  ): JonesInstallPlanState => {
    const current = document.current;
    const editable = snapshot.readOnly === undefined && current !== null;
    const manualClose = editable ? manualCloseReview(current, evidence) : undefined;
    const orphans = snapshot.readOnly === undefined ? orphanedPlanCommands(document) : [];
    return {
      schema: 1,
      current,
      last: document.last,
      clientCommands: 1,
      laptop: evidence,
      ...(snapshot.readOnly === undefined
        ? {
            documentRevision: documentRevision(document),
            recoveryHistory: recoveryHistory(document),
            ...(orphans.length === 0
              ? {}
              : { orphanedCommands: orphans.map(({ receipt }) => receipt) }),
          }
        : {}),
      view: {
        complete: editable && isPlanComplete(current),
        ...(editable ? { cancelPlan: 1 as const, deviceRecovery: 1 as const } : {}),
        ...(snapshot.readOnly === undefined
          ? {
              recoveryHistory: 1 as const,
              directiveFence: 1 as const,
              orphanRecovery: 1 as const,
              boundAdmission: 1 as const,
            }
          : {}),
        ...(manualClose === undefined ? {} : { manualClose }),
      },
      ...(snapshot.readOnly === undefined ? {} : { readOnly: snapshot.readOnly }),
    };
  };
  const gate = (
    plan: JonesInstallPlan,
    evidence: JonesInstallPlanLaptopEvidence,
  ): JonesInstallPlan => {
    const { gate: old, gateClosed, gatePause, ...laptop } = plan.laptop;
    const paused = gatePause === true && gateClosed?.reason === "blocked";
    if (gateClosed !== undefined && (!paused || old === undefined)) return plan;
    if (old !== undefined) {
      const status = rolloutGateStatus(plan.laptop.environmentId, old, evidence);
      if ((status === "verified" && !paused) || (status === "paused" && paused)) return plan;
      return {
        ...plan,
        revision: plan.revision + 1,
        laptop: {
          ...laptop,
          gate: old,
          ...(status === "changed"
            ? { gateClosed: { at: now(), reason: "changed" as const } }
            : status === "paused"
              ? { gatePause: true as const, gateClosed: { at: now(), reason: "blocked" as const } }
              : {}),
        },
      };
    }

    const identity = evidence.environmentId === plan.laptop.environmentId;
    const ordinary = ordinaryProof(evidence);
    const transactionId = evidence.committedTransactionId ?? ordinary?.transactionId;
    if (
      !identity ||
      verificationBlocked(evidence) ||
      (evidence.ordinaryRecovery !== undefined && ordinary === undefined) ||
      transactionId === undefined ||
      evidence.activeGeneration === undefined ||
      evidence.activeSource !== plan.target.sourceSha ||
      evidence.activeTree !== plan.target.sourceTree
    )
      return plan;
    return {
      ...plan,
      revision: plan.revision + 1,
      laptop: {
        ...plan.laptop,
        gate: {
          openedAt: now(),
          basis:
            ordinary === undefined && plan.laptop.binding?.transactionId === transactionId
              ? "plan-install"
              : "prior-verified",
          ...(ordinary === undefined
            ? {}
            : {
                verificationKind: "ordinary-recovery" as const,
                ordinaryRecovery: ordinary,
              }),
          transactionId,
          activeGeneration: evidence.activeGeneration,
          activeSource: evidence.activeSource,
          activeTree: evidence.activeTree,
        },
      },
    };
  };
  const prebindingPreimage = (
    plan: JonesInstallPlan,
    operation: JonesInstallPlanLaptopOperation,
  ): LaptopStartupPreimage | undefined => {
    const { prebinding, review } = operation;
    if (
      prebinding === undefined ||
      review === undefined ||
      prebinding.stagedHandle !== review.stagedHandle ||
      prebinding.fromSource !== review.expectedInstalledSource ||
      review.targetSource !== plan.target.sourceSha
    )
      return undefined;
    return {
      ...prebinding,
      environmentId: plan.laptop.environmentId,
      currentVersion: review.currentVersion,
      targetSource: plan.target.sourceSha,
      targetTree: plan.target.sourceTree,
    };
  };
  const matchesPrebinding = (
    plan: JonesInstallPlan,
    operation: JonesInstallPlanLaptopOperation,
    evidence: JonesInstallPlanLaptopEvidence,
  ): boolean => {
    const preimage = prebindingPreimage(plan, operation);
    return (
      preimage !== undefined &&
      evidence.environmentId === preimage.environmentId &&
      evidence.activeGeneration === preimage.fromGeneration &&
      evidence.activeSource === preimage.fromSource &&
      evidence.activeTree === preimage.fromTree &&
      !verificationBlocked(evidence) &&
      options.controller.isStaged({
        targetSource: preimage.targetSource,
        targetTree: preimage.targetTree,
        stagedHandle: preimage.stagedHandle,
        currentVersion: preimage.currentVersion,
        expectedInstalledSource: preimage.fromSource,
      })
    );
  };
  const startupRecoveryFor = (snapshot: PlanSnapshot) => {
    if (
      disposed ||
      snapshot.readOnly !== undefined ||
      active.size > 0 ||
      options.controller.busy !== null
    )
      return undefined;
    const [candidate, other] = retainedPlans(snapshot.document).flatMap((plan) =>
      plan.laptop.operations.filter(localPending).map((operation) => ({ plan, operation })),
    );
    if (candidate === undefined || other !== undefined) return undefined;
    const { plan, operation } = candidate;
    const { binding, stageAbandonment } = plan.laptop;
    if (
      operation.step !== "install" ||
      operation.worker !== undefined ||
      (stageAbandonment !== undefined && stageAbandonment.status !== "retired")
    )
      return undefined;
    if (binding !== undefined) {
      const previous = plan.laptop.operations.find(
        (entry) => entry.operationId === binding.operationId,
      );
      if (previous?.step !== "install" || previous === operation || localPending(previous))
        return undefined;
    }
    const preimage = prebindingPreimage(plan, operation);
    return preimage === undefined ? undefined : { operation, preimage };
  };
  const reconcileLaptopInstall = async (
    plan: JonesInstallPlan,
    operation: JonesInstallPlanLaptopOperation,
    evidence: JonesInstallPlanLaptopEvidence,
    inspected: JonesInstallPlanLaptopOperation | undefined,
  ): Promise<NonNullable<JonesInstallPlanLaptopOperation["result"]>> => {
    const { binding } = plan.laptop;
    if (operation.prebinding !== undefined) {
      // An unconsumed reservation proves only that the plan fence was never crossed.
      // Native startup must also rule out preparation and confirm the original stage and preimage.
      const interrupted =
        inspected === operation &&
        active.size === 0 &&
        options.controller.busy === null &&
        evidence.startupInspectionComplete === true &&
        matchesPrebinding(plan, operation, evidence);
      return interrupted
        ? { status: "interrupted", code: "install-not-dispatched", at: now() }
        : { status: "held", code: "native-unconfirmed", at: now() };
    }

    const nativeReady =
      options.controller.busy === null &&
      !verificationBlocked(evidence) &&
      evidence.environmentId === plan.laptop.environmentId &&
      evidence.activeGeneration !== undefined;
    const committedGeneration =
      nativeReady && evidence.committedTransactionId === evidence.activeGeneration;
    const manualGeneration = nativeReady && ordinaryProof(evidence) !== undefined;
    const targetVerified =
      (committedGeneration || manualGeneration) &&
      evidence.activeSource === plan.target.sourceSha &&
      evidence.activeTree === plan.target.sourceTree;
    const bound = binding?.operationId === operation.operationId;
    const committed =
      targetVerified &&
      committedGeneration &&
      bound &&
      evidence.activeGeneration === binding.transactionId &&
      evidence.activeGeneration !== binding.fromGeneration;
    if (committed) return { status: "committed", at: now() };
    if (operation.result?.code === "native-binding-changed") return operation.result;

    // A separately verified build settles the active target without crediting this attempt.
    const external =
      (targetVerified &&
        (manualGeneration || !bound || evidence.activeGeneration !== binding.transactionId)) ||
      (manualGeneration &&
        bound &&
        evidence.activeGeneration !== binding.fromGeneration &&
        evidence.activeGeneration !== binding.transactionId);
    const retired =
      !external && nativeReady && bound && (await options.controller.retiredLaptopInstall(binding));
    return external || retired
      ? {
          status: "interrupted",
          code: external && targetVerified ? "target-verified-elsewhere" : "install-not-active",
          at: now(),
        }
      : { status: "held", code: "native-unconfirmed", at: now() };
  };
  const reconcile = async (
    plan: JonesInstallPlan,
    evidence: JonesInstallPlanLaptopEvidence,
    inspected: JonesInstallPlanLaptopOperation | undefined,
  ): Promise<JonesInstallPlan> => {
    const abandonment = plan.laptop.stageAbandonment;
    const retired =
      abandonment !== undefined &&
      abandonment.status !== "retired" &&
      abandonment.selectionName !== undefined &&
      !active.has(abandonment.operationId) &&
      evidence.environmentId === plan.laptop.environmentId &&
      (await options.controller
        .isLaptopStageAbsent(abandonment.selectionName, abandonment.handle)
        .catch(() => false));
    if (retired) plan = withStageAbandonmentResult(plan, abandonment.operationId, true);
    let changed = false;
    const operations = await Promise.all(
      plan.laptop.operations.map(async (operation): Promise<JonesInstallPlanLaptopOperation> => {
        if (
          active.has(operation.operationId) ||
          (operation.of !== undefined && active.has(operation.of)) ||
          !localPending(operation)
        )
          return operation;
        if (operation.step === "install") {
          const result = await reconcileLaptopInstall(plan, operation, evidence, inspected);
          if (operation.result?.status === result.status && operation.result.code === result.code)
            return operation;
          changed = true;
          return { ...operation, result };
        }
        const staged =
          evidence.environmentId === plan.laptop.environmentId &&
          options.controller.isStaged({
            targetSource: plan.target.sourceSha,
            targetTree: plan.target.sourceTree,
          });
        let status: NonNullable<JonesInstallPlanLaptopOperation["result"]>["status"] = "held";
        const original =
          operation.of === undefined
            ? operation
            : (plan.laptop.operations.find((entry) => entry.operationId === operation.of) ??
              operation);
        const worker = operation.worker ?? original.worker;
        const copyAdmitted = [operation.result?.code, original.result?.code].some(
          (code) => code === "copy-admitted" || code === "copy-unconfirmed",
        );
        let code = copyAdmitted ? "copy-unconfirmed" : "preparation-unconfirmed";
        if (staged && operation.step === "download") {
          status = "completed";
          code = "stage-confirmed";
        } else if (staged && operation.step === "stop") {
          status = "completed";
          code = "stage-confirmed";
        } else if (
          !copyAdmitted &&
          worker !== undefined &&
          (options.workerAbsent ?? workerAbsent)(worker)
        ) {
          status =
            operation.step === "stop" ||
            plan.laptop.operations.some((o) => o.of === operation.operationId)
              ? "stopped"
              : "interrupted";
          code = "worker-exited";
        }
        if (operation.result?.status === status && operation.result.code === code) return operation;
        changed = true;
        return {
          ...operation,
          ...(worker === undefined ? {} : { worker }),
          result: { status, code, at: now() },
        };
      }),
    );
    return changed
      ? { ...plan, revision: plan.revision + 1, laptop: { ...plan.laptop, operations } }
      : plan;
  };
  const announce = (state: JonesInstallPlanState) => {
    if (disposed) return;
    lastState = state;
    options.broadcast(state);
  };
  const markLaptopStopped = (
    dispositions: PlanDispositions,
    plan: JonesInstallPlan,
  ): PlanDispositions => {
    const matching = (target: PlanDispositions["targets"][number]) =>
      target.targetSource === plan.target.sourceSha &&
      target.targetTree === plan.target.sourceTree &&
      target.laptopStopped !== true;
    return dispositions.targets.some(matching)
      ? {
          ...dispositions,
          targets: dispositions.targets.map((target) =>
            matching(target) ? { ...target, laptopStopped: true } : target,
          ),
        }
      : dispositions;
  };
  const reconcileJournal = async (
    snapshot: PlanSnapshot,
    evidence: JonesInstallPlanLaptopEvidence,
    inspected: JonesInstallPlanLaptopOperation | undefined,
  ) => {
    let { document, dispositions } = snapshot;
    if (snapshot.readOnly !== undefined) return { document, dispositions };
    for (const plan of retainedPlans(document)) {
      const current = plan === document.current;
      if (!current && !planHasLaptopRecoveryDuty(plan)) continue;
      let next = await reconcile(plan, evidence, inspected);
      if (current) {
        next = gate(next, evidence);
        if (
          next.laptop.operations.some(
            (operation) =>
              operation.result?.status === "held" || operation.result?.status === "interrupted",
          )
        )
          dispositions = markLaptopStopped(dispositions, next);
      }
      if (next !== plan) document = replaceRetainedPlan(document, next);
    }
    return { document: refreshCommands(document), dispositions };
  };
  const transact = async <T>(
    work: (
      snapshot: PlanSnapshot,
      evidence: JonesInstallPlanLaptopEvidence,
    ) => Promise<{
      document: PlanDocument;
      dispositions: PlanDispositions;
      result: T;
    }>,
  ): Promise<T> =>
    options.store.transact(async (snapshot) => {
      const evidence = await options.controller.laptopEvidence();
      return work(snapshot, evidence);
    });
  const nativeChange = async (
    matches: (plan: JonesInstallPlan) => boolean,
    change: (
      plan: JonesInstallPlan,
      evidence: JonesInstallPlanLaptopEvidence,
      archived: boolean,
    ) => JonesInstallPlan,
    selection: {
      readonly required?: boolean;
      readonly currentFallback?: (plan: JonesInstallPlan) => boolean;
    } = {},
  ) => {
    const state = await transact(async (snapshot, evidence) => {
      if (snapshot.readOnly !== undefined) throw new Error("Native plan journal is read-only.");
      const [first, ambiguous] = retainedPlans(snapshot.document).filter(matches);
      const current = snapshot.document.current;
      const plan =
        first ?? (current !== null && selection.currentFallback?.(current) ? current : undefined);
      if (ambiguous !== undefined || (selection.required === true && plan === undefined))
        throw new Error("The native attempt has no unique plan binding.");
      if (plan === undefined) return { ...snapshot, result: stateOf(snapshot, evidence) };
      const next = change(plan, evidence, plan !== snapshot.document.current);
      const document = commitRevision(
        snapshot.document,
        refreshCommands(
          next === plan ? snapshot.document : replaceRetainedPlan(snapshot.document, next),
        ),
      );
      return {
        document,
        dispositions: snapshot.dispositions,
        result: stateOf(snapshot, evidence, document),
      };
    });
    announce(state);
  };
  const finishLaptop = async (
    planId: string,
    operationId: string,
    result: NonNullable<JonesInstallPlanLaptopOperation["result"]>,
    worker?: JonesInstallPlanLaptopOperation["worker"],
    createdStagedHandle?: string,
  ) => {
    await nativeChange(
      (plan) => plan.planId === planId,
      (plan) => {
        const operation = plan.laptop.operations.find((entry) => entry.operationId === operationId);
        if (
          operation === undefined ||
          !localPending(operation) ||
          (operation.step === "install" && disposed)
        )
          return plan;
        return {
          ...plan,
          revision: plan.revision + 1,
          laptop: {
            ...plan.laptop,
            operations: plan.laptop.operations.map((entry) =>
              entry !== operation
                ? entry
                : {
                    ...operation,
                    ...(worker === undefined ? {} : { worker }),
                    ...(createdStagedHandle === undefined ? {} : { createdStagedHandle }),
                    result,
                  },
            ),
          },
        };
      },
    );
  };
  type LaptopEffect = {
    planId: string;
    operationId: string;
    action: "download" | "stop" | "install" | "abandon-stage";
    target: JonesInstallPlan["target"];
    review?: JonesInstallPlanReview;
    of?: string;
    artifactId?: number;
    stagedHandle?: string;
  };
  const withStageAbandonmentResult = (
    plan: JonesInstallPlan,
    operationId: string,
    retired: boolean,
  ): JonesInstallPlan => {
    const stage = plan.laptop.stageAbandonment;
    if (stage?.operationId !== operationId || stage.status === "retired") return plan;
    return {
      ...plan,
      revision: plan.revision + 1,
      laptop: {
        ...plan.laptop,
        stageAbandonment: { ...stage, status: retired ? "retired" : "unconfirmed" },
        operations: retired
          ? plan.laptop.operations.map((operation) =>
              operation.operationId === stage.operationId
                ? {
                    ...operation,
                    result: { status: "stopped", at: now(), code: "stage-retired" },
                  }
                : operation,
            )
          : plan.laptop.operations,
      },
    };
  };
  const recordStageAbandonment = async (effect: LaptopEffect, retired: boolean) => {
    await nativeChange(
      (plan) => plan.planId === effect.planId,
      (plan) => withStageAbandonmentResult(plan, effect.operationId, retired),
    );
  };
  const trackLaptop = <A>(work: () => Promise<A>) =>
    options.track(() => (disposed ? Promise.resolve(undefined) : work()));
  const runLaptop = async (effect: LaptopEffect) => {
    try {
      if (disposed) return;
      if (effect.action === "abandon-stage") {
        const result = await trackLaptop(() => options.controller.discard(effect.stagedHandle!));
        if (result === undefined) return;
        await recordStageAbandonment(effect, result.completed);
      } else if (effect.action === "download") {
        const result = await trackLaptop(() =>
          options.controller.download(
            { artifactId: effect.artifactId!, sourceSha: effect.target.sourceSha },
            { operationId: effect.operationId },
          ),
        );
        if (result === undefined) return;
        await finishLaptop(
          effect.planId,
          effect.operationId,
          {
            status: result.held
              ? "held"
              : result.stopped
                ? "stopped"
                : result.completed
                  ? "completed"
                  : result.accepted
                    ? "failed"
                    : "refused",
            at: now(),
            ...(result.held ? { code: result.heldCode ?? "exit-unproved" } : {}),
          },
          result.held,
          result.createdStagedHandle,
        );
      } else if (effect.action === "stop") {
        const result = await options.controller.stopDownload({
          ...(effect.of === undefined ? {} : { operationId: effect.of }),
          sourceSha: effect.target.sourceSha,
        });
        const state = options.controller.state.jones;
        const status =
          result === "stopped"
            ? "stopped"
            : result === "completed" || result === "not-stoppable-phase"
              ? "completed"
              : result === "exit-unproved"
                ? "held"
                : "refused";
        // The download's promise records its own terminal result. An admission refusal cannot end it.
        await finishLaptop(effect.planId, effect.operationId, { status, code: result, at: now() });
        if (result === "exit-unproved" && effect.of !== undefined && state?.phase === "blocked")
          await finishLaptop(effect.planId, effect.of, {
            status: "held",
            code: "exit-unproved",
            at: now(),
          });
      } else {
        const result = await trackLaptop(() =>
          options.controller.install(effect.review!.stagedHandle, undefined, {
            operationId: effect.operationId,
            ...(effect.review!.recovery === undefined ? {} : { recovery: effect.review!.recovery }),
          }),
        );
        if (result === undefined || disposed) return;
        await nativeChange(
          (plan) => plan.planId === effect.planId,
          (plan, evidence) => {
            if (disposed || result.accepted) return plan;
            const operation = plan.laptop.operations.find(
              (entry) => entry.operationId === effect.operationId,
            );
            if (operation === undefined || !localPending(operation)) return plan;
            const binding = plan.laptop.binding;
            const bound = binding?.operationId === effect.operationId;
            // A refused call alone cannot settle a bound handoff with unknown native effects.
            const failedBeforeActivation =
              result.failed &&
              options.controller.busy === null &&
              !verificationBlocked(evidence) &&
              evidence.environmentId === plan.laptop.environmentId &&
              (!bound || evidence.activeGeneration === binding.fromGeneration);
            const status = failedBeforeActivation ? "failed" : bound ? "held" : "refused";
            return {
              ...plan,
              revision: plan.revision + 1,
              laptop: {
                ...plan.laptop,
                operations: plan.laptop.operations.map((operation) =>
                  operation.operationId === effect.operationId
                    ? {
                        ...operation,
                        result: {
                          status,
                          code: failedBeforeActivation
                            ? "install-failed"
                            : bound
                              ? "native-unconfirmed"
                              : (result.refusal ?? "install-refused"),
                          at: now(),
                        },
                      }
                    : operation,
                ),
              },
            };
          },
        );
      }
    } catch {
      if (effect.action === "abandon-stage") {
        await recordStageAbandonment(effect, false).catch(() => undefined);
        return;
      }
      if (effect.action === "install" && disposed) return;
      // A lost native handoff is not proof of a refused effect.
      await finishLaptop(effect.planId, effect.operationId, {
        status: "held",
        code: "native-unconfirmed",
        at: now(),
      }).catch(() => undefined);
    } finally {
      active.delete(effect.operationId);
    }
  };

  const disposedRefusal = {
    code: "unsupported",
    message: "The native install plan coordinator is closing.",
  } as const;
  const ownsLaptopOperation = (operationId: string) => (plan: JonesInstallPlan) =>
    plan.laptop.operations.some((operation) => operation.operationId === operationId);
  const laptopHasOrphan = (document: PlanDocument, environmentId = options.localEnvironmentId()) =>
    orphanedPlanCommands(document).some(
      ({ receipt }) =>
        receipt.command.environmentId === environmentId ||
        (environmentId === undefined && receipt.requestType !== "claim"),
    );
  const hasLaptopHold = (document: PlanDocument, issuedOperationId?: string) =>
    laptopHasOrphan(document) ||
    retainedPlans(document).some((plan) => {
      if (!planHasLaptopRecoveryDuty(plan)) return false;
      const issued =
        issuedOperationId !== undefined && active.has(issuedOperationId)
          ? plan.laptop.operations.find((operation) => operation.operationId === issuedOperationId)
          : undefined;
      if (
        issued === undefined ||
        !localPending(issued) ||
        issued.step === "stop" ||
        issued.result?.status === "held"
      )
        return true;
      const { stageAbandonment, operations } = plan.laptop;
      return (
        (stageAbandonment !== undefined && stageAbandonment.status !== "retired") ||
        operations.some(
          (operation) =>
            operation !== issued && operation.of !== issued.operationId && localPending(operation),
        )
      );
    });
  const request = async (req: JonesInstallPlanRequest): Promise<JonesInstallPlanResult> => {
    let effect: LaptopEffect | undefined;
    try {
      if (req.type === "recovery-preflight") {
        // Cold staged review has no plan ID, reservation, disposition or install authority.
        return await transact(async (snapshot, evidence) => {
          let extras: Omit<JonesInstallPlanResult, "state">;
          if (disposed) extras = { refused: disposedRefusal };
          else if (!compiledIs1(req))
            extras = {
              refused: { code: "invalid-evidence", message: "Invalid recovery preflight binding." },
            };
          else if (snapshot.readOnly !== undefined)
            extras = {
              refused: {
                code: "storage",
                message: "Native plan storage is held; no recovery review was granted.",
              },
            };
          else if (
            req.input.environmentId !== options.localEnvironmentId() ||
            req.input.environmentId !== evidence.environmentId
          )
            extras = {
              refused: {
                code: "invalid-evidence",
                message: "Recovery preflight must target this native Laptop environment.",
              },
            };
          else {
            try {
              extras = { preflight: await options.controller.recoveryPreflight(req.input) };
            } catch (cause) {
              extras = {
                refused: {
                  code:
                    cause instanceof RecoveryPreflightRefused &&
                    cause.code === "recovery-preflight-stale"
                      ? "invalid-evidence"
                      : "unsupported",
                  message:
                    cause instanceof RecoveryPreflightRefused
                      ? `${cause.code}: ${cause.message}`
                      : "Native recovery inspection could not be proved. Stop; free space and review again.",
                },
              };
            }
          }
          return {
            document: snapshot.document,
            dispositions: snapshot.dispositions,
            result: { state: stateOf(snapshot, evidence), ...extras },
          };
        });
      }
      const result = await options.store.transact<JonesInstallPlanResult>(async (snapshot) => {
        const startup = startupRecoveryFor(snapshot);
        const evidence = await options.controller.laptopEvidence(startup?.preimage);
        const inspected = startup?.operation;
        if (disposed)
          return {
            ...snapshot,
            result: {
              state: stateOf(snapshot, evidence),
              ...(req.type === "read" ? {} : { refused: disposedRefusal }),
            },
          };
        let { document, dispositions } = await reconcileJournal(snapshot, evidence, inspected);
        const beforeAdmission = document;
        const beforeDispositions = dispositions;
        const archived =
          "planId" in req
            ? closedPlans(document).find((entry) => entry.planId === req.planId)
            : undefined;
        const recoverArchived =
          archived !== undefined &&
          (req.type === "report" ||
            (req.type === "claim" && (req.step === "stop" || req.step === "fence")) ||
            (req.type === "laptop" && (req.action === "stop" || req.action === "abandon-stage")) ||
            (req.type === "abandon-device" && req.phase === "finish"));
        let plan = recoverArchived ? archived : document.current;
        const command = "command" in req ? req.command : undefined;
        const validRequest =
          req.type === "claim" || req.type === "report" || req.type === "reconcile-orphan"
            ? Option.isSome(decodeExactHostRequest(req))
            : compiledIs1(req);
        if (validRequest && command !== undefined) {
          const recorded = document.commands?.find(
            (entry) => entry.receipt.command.commandId === command.commandId,
          );
          if (recorded !== undefined) {
            document = commitRevision(snapshot.document, document);
            return {
              document,
              dispositions,
              result:
                recorded.body === commandBody(req)
                  ? { state: stateOf(snapshot, evidence, document), command: recorded.receipt }
                  : {
                      state: stateOf(snapshot, evidence, document),
                      refused: {
                        code: "invalid-evidence" as const,
                        message:
                          "This client command identity is already bound to a different request.",
                      },
                    },
            };
          }
          if ((document.commands?.length ?? 0) >= MAX_PLAN_COMMANDS) {
            document = commitRevision(snapshot.document, document);
            return {
              document,
              dispositions,
              result: {
                state: stateOf(snapshot, evidence, document),
                command: {
                  command,
                  bodyHash: commandBody(req),
                  requestType: req.type as "start" | "edit" | "laptop" | "claim",
                  ...(command.planId === undefined ? {} : { planId: command.planId }),
                  outcome: "refused" as const,
                  code: "command-capacity",
                },
                refused: {
                  code: "bounds" as const,
                  message: "The retained client command history is full; no action was granted.",
                },
              },
            };
          }
        }
        let extras: Omit<JonesInstallPlanResult, "state"> = {};
        const refuse = (
          code: NonNullable<JonesInstallPlanResult["refused"]>["code"],
          message: string,
        ) => {
          extras = { refused: { code, message } };
        };
        const save = (next: JonesInstallPlan) => {
          plan = next;
          document = replaceRetainedPlan(document, next);
        };
        const close = (closed: JonesInstallPlan) => {
          document = archiveCurrent(document, { ...closed, revision: closed.revision + 1 });
          plan = null;
        };
        const replaceOperation = (
          deviceIndex: number,
          index: number,
          operation: JonesInstallPlanHostOperation,
        ) => {
          const current = plan!;
          save({
            ...current,
            revision: current.revision + 1,
            devices: current.devices.map((device, i) =>
              i !== deviceIndex
                ? device
                : {
                    ...device,
                    operations: device.operations.map((old, j) => (j === index ? operation : old)),
                  },
            ),
          });
        };
        const pendingLaptop = () => plan!.laptop.operations.some(localPending);
        const recordLaptop = async (
          action: "download" | "stop" | "install",
          review?: JonesInstallPlanReview,
          stopOperationId?: string,
        ) => {
          const current = plan!;
          if (action !== "stop" && current.laptop.gatePause === true)
            return refuse("gate-closed", PAUSED_GATE_MESSAGE);
          if (
            current.laptop.operations.length >=
            (action === "download" ? MAX_OPERATIONS - 1 : MAX_OPERATIONS)
          )
            return refuse("bounds", "The Laptop operation history is full.");
          if (
            action !== "stop" &&
            (laptopHasOrphan(document, current.laptop.environmentId) ||
              pendingLaptop() ||
              (current.laptop.stageAbandonment !== undefined &&
                current.laptop.stageAbandonment.status !== "retired"))
          )
            return refuse("outstanding-operation", "Laptop work still requires settlement.");
          const native = options.controller.state.jones;
          let of: string | undefined;
          let prebinding: JonesInstallPlanLaptopOperation["prebinding"];
          if (action === "download") {
            if (
              native?.provenance?.sourceSha !== current.target.sourceSha ||
              native.provenance.sourceTree !== current.target.sourceTree ||
              (current.target.laptopArtifactId !== undefined &&
                (native.provenance.artifactId !== current.target.laptopArtifactId ||
                  native.provenance.version !== current.target.version)) ||
              !native.capability.download ||
              native.stagedHandle !== undefined
            )
              return refuse(
                "not-selectable",
                "The native download candidate does not match this plan.",
              );
          } else if (action === "install") {
            if (
              review === undefined ||
              !Number.isFinite(Date.parse(review.reviewedAt)) ||
              options.now() - Date.parse(review.reviewedAt) > LEASE_MS ||
              Date.parse(review.reviewedAt) > options.now() ||
              review.targetSource !== current.target.sourceSha ||
              evidence.environmentId !== current.laptop.environmentId ||
              evidence.activeGeneration === undefined ||
              evidence.activeSource !== review.expectedInstalledSource ||
              evidence.activeTree === undefined ||
              verificationBlocked(evidence) ||
              !options.controller.isStaged({ ...review, targetTree: current.target.sourceTree })
            )
              return refuse(
                "invalid-evidence",
                "Review the current native stage before installing.",
              );
            if (review.recovery !== undefined) {
              try {
                await options.controller.validateRecoveryReview(
                  {
                    environmentId: current.laptop.environmentId,
                    stagedHandle: review.stagedHandle,
                    currentVersion: review.currentVersion,
                    expectedInstalledSource: review.expectedInstalledSource,
                  },
                  review.recovery,
                );
              } catch (cause) {
                return refuse(
                  cause instanceof RecoveryPreflightRefused &&
                    cause.code !== "recovery-preflight-stale"
                    ? "unsupported"
                    : "invalid-evidence",
                  cause instanceof RecoveryPreflightRefused
                    ? `${cause.code}: ${cause.message}`
                    : "Native recovery review could not be proved. Stop; review again.",
                );
              }
            }
            prebinding = {
              fromGeneration: evidence.activeGeneration,
              fromSource: evidence.activeSource,
              fromTree: evidence.activeTree,
              stagedHandle: review.stagedHandle,
            };
          } else {
            if (stopOperationId === undefined)
              return refuse(
                "invalid-evidence",
                "Stop needs the current Laptop download ID. Refresh Update Status and try again.",
              );
            const latest = current.laptop.operations.findLast(
              (operation) => operation.step === "download",
            );
            if (latest?.operationId !== stopOperationId)
              return refuse("invalid-evidence", "The Laptop download changed. Check it again.");
            if (!localPending(latest) || latest.result?.status === "held")
              return refuse("outstanding-operation", "No running Laptop transfer can be stopped.");
            if (current.laptop.operations.some((operation) => operation.of === latest.operationId))
              return refuse("claimed", "Stop was already requested for this download.");
            of = latest.operationId;
          }
          const operationId = options.uuid();
          const operation: JonesInstallPlanLaptopOperation = {
            operationId,
            step: action,
            requestedAt: now(),
            ...(of === undefined ? {} : { of }),
            ...(review === undefined ? {} : { review }),
            ...(prebinding === undefined ? {} : { prebinding }),
            result: { status: action === "stop" ? "stop-requested" : "running", at: now() },
          };
          save({
            ...current,
            revision: current.revision + 1,
            laptop: { ...current.laptop, operations: [...current.laptop.operations, operation] },
          });
          // An explicit retry has its own permit. It must not clear the background suppression
          // before its reservation commits, since dispositions are written ahead of the plan.
          if (action === "stop") dispositions = markLaptopStopped(dispositions, current);
          effect = {
            planId: current.planId,
            operationId,
            action,
            target: current.target,
            ...(review === undefined ? {} : { review }),
            ...(of === undefined ? {} : { of }),
            ...(native?.provenance === undefined
              ? {}
              : { artifactId: native.provenance.artifactId }),
          };
        };
        const commandMatches = () => {
          if (command === undefined) return true;
          if (req.type === "start")
            return (
              command.targetSource === req.target.sourceSha &&
              command.planId === undefined &&
              command.expectedRevision === undefined &&
              (command.environmentId === evidence.environmentId
                ? req.laptop !== undefined &&
                  (command.action === "install") === (req.laptop.action === "install") &&
                  command.stagedHandle === req.laptop.review?.stagedHandle
                : req.select.some((s) => s.environmentId === command.environmentId) &&
                  command.action !== "install")
            );
          if (
            plan === null ||
            command.targetSource !== plan.target.sourceSha ||
            command.planId !== plan.planId ||
            ((req.type === "laptop" || req.type === "claim") &&
              req.expectedRevision !== undefined &&
              command.expectedRevision !== req.expectedRevision)
          )
            return false;
          if (req.type === "edit")
            return (
              command.action !== "install" &&
              command.expectedRevision === req.expectedRevision &&
              req.select.some((s) => s.environmentId === command.environmentId)
            );
          if (req.type === "laptop")
            return (
              command.environmentId === plan.laptop.environmentId &&
              command.expectedRevision === plan.revision &&
              (command.action === "install") === (req.action === "install") &&
              (req.action === "download" || req.action === "install") &&
              command.stagedHandle === req.review?.stagedHandle
            );
          if (req.type === "claim")
            return (
              command.environmentId === req.environmentId &&
              command.expectedRevision === plan.revision &&
              (command.action === "install") === (req.step === "install") &&
              (req.step === "download" || req.step === "install") &&
              command.stagedHandle === req.review?.stagedHandle
            );
          return false;
        };
        const boundCommand = validRequest && commandMatches();
        if (!validRequest) refuse("invalid-evidence", "Invalid plan request.");
        else if (snapshot.readOnly !== undefined) {
          if (req.type !== "read")
            refuse(
              snapshot.readOnly.code === "newer-schema" ? "newer-schema" : "storage",
              snapshot.readOnly.message,
            );
        } else if (!boundCommand)
          refuse(
            "invalid-evidence",
            "The client command does not match the current native action binding.",
          );
        else if (req.type === "read") {
          /* Gate and native reconciliation above are the only changes. */
        } else if (req.type === "read-history") {
          if (archived === undefined) refuse("plan-mismatch", "This closed plan is unavailable.");
          else extras = { historyPlan: archived };
        } else if (req.type === "reconcile-orphan") {
          const entries = document.commands ?? [];
          const entry = entries.find(({ receipt }) => receipt.command.commandId === req.commandId);
          if (entry?.body !== req.bodyHash)
            refuse("invalid-evidence", "This proof does not match the retained client command.");
          else if (
            entry.orphanReceipt !== undefined &&
            validateReceipt(entry.orphanReceipt, req.receipt)
          )
            extras = { command: entry.receipt };
          else if (
            !orphanedPlanCommands(document).includes(entry) ||
            !matchesOrphanReceipt(entry.receipt, req.receipt)
          )
            refuse(
              "invalid-evidence",
              "The retained command needs an exact terminal host receipt.",
            );
          else {
            const receipt = {
              ...entry.receipt,
              outcome: "terminal" as const,
              ...(req.receipt.code === undefined ? {} : { code: req.receipt.code }),
            };
            document = {
              ...document,
              commands: entries.map((old) =>
                old === entry ? { ...old, receipt, orphanReceipt: req.receipt } : old,
              ),
            };
            extras = { command: receipt };
          }
        } else if (req.type === "publication") {
          const code = evidence.activationBlocked
            ? "blocked"
            : evidence.activeSource === undefined || evidence.activeTree === undefined
              ? "unavailable"
              : dispositions.targets.some((target) => target.targetSource === evidence.activeSource)
                ? "plan-managed"
                : evidence.committedTransactionId === undefined &&
                    (req.acceptOrdinaryRecovery !== 1 || ordinaryProof(evidence) === undefined)
                  ? "unproved"
                  : undefined;
          extras = {
            publication:
              code === undefined
                ? {
                    kind:
                      evidence.committedTransactionId === undefined
                        ? "ordinary-verified"
                        : "committed",
                    source: {
                      repository: "Jones-Systems/Jones-Code",
                      sha: evidence.activeSource!,
                      tree: evidence.activeTree!,
                    },
                  }
                : {
                    kind: "withheld",
                    code,
                    message:
                      "Follower publication requires an unmanaged, natively verified active build.",
                  },
          };
        } else if (req.type === "start") {
          const environmentId = options.localEnvironmentId();
          const held = req.select.filter((selection) =>
            heldByClosedPlan(document, selection.environmentId),
          );
          const prior =
            evidence.environmentId === environmentId &&
            (evidence.committedTransactionId !== undefined ||
              ordinaryProof(evidence) !== undefined) &&
            !verificationBlocked(evidence) &&
            evidence.activeSource === req.target.sourceSha &&
            evidence.activeTree === req.target.sourceTree;
          if (plan?.laptop.gatePause === true) refuse("gate-closed", PAUSED_GATE_MESSAGE);
          else if (
            plan !== null &&
            (req.target.laptopArtifactId !== undefined ||
              !canSupersede(plan) ||
              planHasLaptopRecoveryDuty(plan))
          )
            refuse("plan-active", "Finish the current plan before starting another.");
          else if (
            laptopHasOrphan(document, environmentId) ||
            closedPlans(document).some(planHasLaptopRecoveryDuty)
          )
            refuse(
              "outstanding-operation",
              "An earlier plan's Laptop work still needs native verification.",
            );
          else if (
            (req.laptop === undefined && held.length > 0 && held.length === req.select.length) ||
            held.some((selection) => selection.environmentId === command?.environmentId)
          )
            refuse("outstanding-operation", DEVICE_RECOVERY_HOLD);
          else if (environmentId === undefined || evidence.activeVersion === undefined)
            refuse("unsupported", "The native Laptop identity is unavailable.");
          else if (req.laptop === undefined && !prior)
            refuse("laptop-action-required", "Start with a Laptop action.");
          else if (
            new Set(req.select.map((s) => s.environmentId)).size !== req.select.length ||
            req.select.some(
              (s) =>
                s.environmentId === environmentId ||
                (s.route === "follower-operations" && s.enrollmentId === undefined),
            )
          )
            refuse("not-selectable", "The selected device route is invalid.");
          else {
            const next = startPlan({
              planId: options.uuid(),
              origin: req.origin,
              target: req.target,
              from: {
                version: evidence.activeVersion,
                ...(evidence.activeSource === undefined
                  ? {}
                  : { sourceSha: evidence.activeSource }),
              },
              laptopEnvironmentId: environmentId,
              select: req.select.filter((selection) => !held.includes(selection)),
              now: now(),
            });
            const disposition = dispositions.targets.find(
              (target) => target.targetSource === req.target.sourceSha,
            );
            if (disposition !== undefined && disposition.targetTree !== req.target.sourceTree)
              refuse("invalid-evidence", "The retained target tree differs from this plan.");
            else if (
              disposition === undefined &&
              dispositions.targets.length >= MAX_PLAN_DISPOSITIONS
            )
              refuse("bounds", "The retained target disposition history is full.");
            if (extras.refused === undefined) {
              if (disposition === undefined)
                dispositions = {
                  ...dispositions,
                  targets: [
                    ...dispositions.targets,
                    {
                      targetSource: req.target.sourceSha,
                      targetTree: req.target.sourceTree,
                      planId: next.planId,
                      createdAt: now(),
                    },
                  ],
                };
              if (plan !== null) close({ ...plan, supersededAt: now() });
              document = { ...document, current: gate(next, evidence) };
              plan = document.current;
              if (req.laptop !== undefined)
                await recordLaptop(req.laptop.action, req.laptop.review);
              // A refused first action must not leave a newly created plan or disposition behind.
              if (extras.refused !== undefined) {
                document = beforeAdmission;
                dispositions = beforeDispositions;
                plan = document.current;
              } else if (held.length > 0) {
                extras = {
                  applied: held.map(({ environmentId }) => ({
                    environmentId,
                    result: "not-selectable",
                    reason: DEVICE_RECOVERY_HOLD,
                  })),
                };
              }
            }
          }
        } else if (
          plan === null ||
          req.planId !== plan.planId ||
          (!recoverArchived && (plan.dismissedAt !== undefined || plan.supersededAt !== undefined))
        )
          refuse("plan-mismatch", "This is not the current plan.");
        else if (
          (req.type === "claim" || req.type === "laptop") &&
          (recoverArchived || req.expectedRevision !== undefined) &&
          req.expectedRevision !== plan.revision
        )
          refuse(
            "stale-revision",
            "The plan changed. Refresh the exact operation before trying again.",
          );
        else if (req.type === "edit") {
          if (
            req.select.some(
              (s) =>
                s.environmentId === plan!.laptop.environmentId ||
                (s.route === "follower-operations" && s.enrollmentId === undefined),
            )
          )
            refuse("not-selectable", "The selected device route is invalid.");
          else {
            const held = new Set(
              req.select
                .filter((selection) => heldByClosedPlan(document, selection.environmentId))
                .map((selection) => selection.environmentId),
            );
            const edited = applyEdit(
              plan,
              {
                ...req,
                select: req.select.filter((selection) => !held.has(selection.environmentId)),
                deselect: req.deselect.filter((environmentId) => !held.has(environmentId)),
              },
              now(),
            );
            if (edited.refused !== undefined)
              refuse(edited.refused, "The plan changed or reached its device bound.");
            else if (edited.applied.length + held.size > 16)
              refuse("bounds", "The plan reached its device bound.");
            else if (command !== undefined && held.has(command.environmentId))
              refuse("outstanding-operation", DEVICE_RECOVERY_HOLD);
            else {
              if (edited.plan !== plan) save(edited.plan);
              extras = {
                applied: [
                  ...edited.applied,
                  ...Array.from(held, (environmentId) => ({
                    environmentId,
                    result: "not-selectable" as const,
                    reason: DEVICE_RECOVERY_HOLD,
                  })),
                ],
              };
            }
          }
        } else if (req.type === "laptop") {
          if (
            !recoverArchived &&
            req.action !== "stop" &&
            closedPlans(document).some(planHasLaptopRecoveryDuty)
          )
            refuse(
              "outstanding-operation",
              "An earlier plan's Laptop work still needs native verification.",
            );
          else if (req.action === "preflight") {
            const native = options.controller.state.jones;
            const staged = options.controller.stagedBuild;
            if (
              pendingLaptop() ||
              evidence.environmentId !== plan.laptop.environmentId ||
              native?.environmentId !== plan.laptop.environmentId ||
              staged === undefined ||
              !options.controller.isStaged({
                targetSource: plan.target.sourceSha,
                targetTree: plan.target.sourceTree,
              }) ||
              native.installedSource === undefined ||
              native.capability.recoveryPolicy !== 1
            )
              refuse(
                "invalid-evidence",
                "The current staged Laptop identity is not available for recovery review.",
              );
            else {
              try {
                extras = {
                  preflight: await options.controller.recoveryPreflight({
                    environmentId: plan.laptop.environmentId,
                    stagedHandle: staged.handle,
                    currentVersion: options.controller.state.currentVersion,
                    expectedInstalledSource: native.installedSource.sha,
                  }),
                };
              } catch (cause) {
                refuse(
                  cause instanceof RecoveryPreflightRefused &&
                    cause.code === "recovery-preflight-stale"
                    ? "invalid-evidence"
                    : "unsupported",
                  cause instanceof RecoveryPreflightRefused
                    ? `${cause.code}: ${cause.message}`
                    : "Native recovery inspection could not be proved. Stop; free space and review again.",
                );
              }
            }
          } else if (req.action === "abandon-stage") {
            const download = plan.laptop.operations.findLast((op) => op.step === "download");
            const handle = download?.createdStagedHandle;
            const previousSelection = plan.laptop.stageAbandonment;
            if (
              active.size > 0 ||
              pendingLaptop() ||
              plan.laptop.gate !== undefined ||
              evidence.environmentId !== plan.laptop.environmentId ||
              options.controller.busy !== null ||
              download?.result?.status !== "completed" ||
              handle === undefined ||
              handle !== options.controller.stagedBuild?.handle ||
              (req.operationId !== undefined && req.operationId !== download.operationId) ||
              (recoverArchived &&
                (req.operationId !== download.operationId ||
                  previousSelection?.operationId !== download.operationId ||
                  previousSelection.status === "retired" ||
                  previousSelection.handle !== handle))
            )
              refuse(
                "invalid-evidence",
                "Only the current Laptop stage created by this plan can be discarded.",
              );
            else {
              const operationId = download.operationId;
              const selectionName = await options.controller
                .laptopStageSelectionName(handle)
                .catch(() => undefined);
              if (
                selectionName === undefined ||
                (previousSelection !== undefined &&
                  previousSelection.status !== "retired" &&
                  previousSelection.selectionName !== selectionName)
              ) {
                refuse(
                  "invalid-evidence",
                  "The native Laptop selection could not be verified. Refresh it and try again.",
                );
              } else {
                save({
                  ...plan,
                  revision: plan.revision + 1,
                  laptop: {
                    ...plan.laptop,
                    stageAbandonment: { operationId, handle, selectionName, status: "pending" },
                  },
                });
                dispositions = markLaptopStopped(dispositions, plan);
                effect = {
                  planId: plan.planId,
                  operationId,
                  action: "abandon-stage",
                  target: plan.target,
                  stagedHandle: handle,
                };
              }
            }
          } else await recordLaptop(req.action, req.review, req.operationId);
        } else if (req.type === "close-after-manual-install") {
          const review = manualCloseReview(plan, evidence);
          const selected = plan.devices.filter((device) => device.membership === "selected");
          if (req.expectedRevision !== plan.revision) refuse("stale-revision", "The plan changed.");
          else if (
            review === undefined ||
            req.activeGeneration !== review.activeGeneration ||
            !same(req.laptop, review.laptop)
          )
            refuse(
              "invalid-evidence",
              "The running Laptop build changed. Check the devices again.",
            );
          else if (
            active.size > 0 ||
            options.controller.busy !== null ||
            planHasLaptopRecoveryDuty(plan)
          )
            refuse(
              "outstanding-operation",
              "Check the outstanding Laptop work before closing this plan.",
            );
          else if (
            req.devices.length !== selected.length ||
            !selected.every((device) =>
              req.devices.some((observation) => observation.environmentId === device.environmentId),
            )
          )
            refuse("invalid-evidence", "The review must include each selected device.");
          else if (
            req.devices.some((reading) => {
              const age = options.now() - reading.observedAt;
              return age < 0 || age > LEASE_MS;
            })
          )
            refuse("invalid-evidence", "The device readings expired. Check the devices again.");
          else {
            close({
              ...plan,
              dismissedAt: now(),
              manualClosure: { ...review, devices: req.devices },
            });
          }
        } else if (req.type === "cancel-plan") {
          if (
            req.environmentId !== undefined &&
            (req.environmentId !== evidence.environmentId ||
              req.environmentId !== plan.laptop.environmentId)
          )
            refuse(
              "invalid-evidence",
              "The running Laptop does not match the requested environment ID.",
            );
          else if (req.expectedRevision !== plan.revision)
            refuse("stale-revision", "The plan changed.");
          else {
            dispositions = markLaptopStopped(dispositions, plan);
            close({
              ...plan,
              dismissedAt: now(),
              cancelled: true,
              cancelledFromRevision: req.expectedRevision,
            });
          }
        } else if (req.type === "dismiss") {
          if (req.expectedRevision !== plan.revision) refuse("stale-revision", "The plan changed.");
          else if (!isPlanComplete(plan))
            refuse("not-complete", "The chosen plan still has unsettled work.");
          else close({ ...plan, dismissedAt: now() });
        } else {
          const deviceIndex = plan.devices.findIndex(
            (device) => device.environmentId === req.environmentId,
          );
          const device = plan.devices[deviceIndex];
          const operations = device === undefined ? [] : deviceActiveOperations(device);
          const runningTarget = "runningTarget" in req ? req.runningTarget : undefined;
          const validRunningTarget =
            runningTarget !== undefined &&
            runningTarget.observedAt <= options.now() &&
            options.now() - runningTarget.observedAt <= LEASE_MS &&
            runningTarget.build.sourceSha === plan.target.sourceSha &&
            runningTarget.build.sourceTree === plan.target.sourceTree;
          if (device === undefined || (device.membership !== "selected" && req.type !== "report"))
            refuse("not-selectable", "This device is not selected.");
          else if (req.type === "abandon-device") {
            const download = deviceStagedReceipt(operations);
            const directive = operations.findLast(
              (operation) => operation.directive?.kind === "applied",
            );
            const release = req.directiveRelease;
            const replacement = req.stageReplacement;
            const validReplacement =
              replacement !== undefined &&
              download?.staged?.createdByOperation === true &&
              replacement.operationId === download.operationId &&
              replacement.stagedHandle !== download.staged.stagedHandle &&
              Buffer.byteLength(JSON.stringify(replacement.stagedHandle)) <= 1024 &&
              replacement.observedAt <= options.now() &&
              options.now() - replacement.observedAt <= LEASE_MS;
            if (req.expectedRevision !== plan.revision)
              refuse("stale-revision", "The plan changed. Refresh the device before abandoning.");
            else if (
              operations.some(outstanding) ||
              operations.some(
                (operation) =>
                  operation.step === "install" && operation.receipt?.phase === "completed",
              )
            )
              refuse(
                "outstanding-operation",
                "Settle the device's work before abandoning. An installed build requires a separate update.",
              );
            else if (req.phase === "begin") {
              if (heldByClosedPlan(document, device.environmentId))
                refuse("outstanding-operation", DEVICE_RECOVERY_HOLD);
              else if (download !== undefined && download.staged?.createdByOperation !== true)
                refuse(
                  "invalid-evidence",
                  "This plan cannot prove ownership of the staged selection.",
                );
              else if (!device.abandoning)
                save({
                  ...plan,
                  revision: plan.revision + 1,
                  devices: plan.devices.map((d, i) =>
                    i === deviceIndex ? { ...d, abandoning: true } : d,
                  ),
                });
            } else if (
              !device.abandoning ||
              (runningTarget !== undefined && !validRunningTarget) ||
              (replacement !== undefined && !validReplacement) ||
              (download !== undefined && !validRunningTarget && !validReplacement) ||
              (directive !== undefined &&
                (!matchesOperationTarget(plan, device, directive) ||
                  directive.request.kind !== "plan-directive" ||
                  directive.request.input.enrollmentId !== device.enrollmentId ||
                  release?.environmentId !== device.environmentId ||
                  release.enrollment?.environmentId !== device.environmentId ||
                  release.enrollment?.enrollmentId !== device.enrollmentId ||
                  !release.enrollment?.enabled ||
                  release.releasedDirectiveId !== directive.operationId ||
                  release.follower?.planDirective?.directiveId === directive.operationId))
            )
              refuse(
                "invalid-evidence",
                "The staged selection or follower directive still requires recovery.",
              );
            else {
              const {
                abandoning: _abandoning,
                ended: _ended,
                verifiedElsewhere: _verified,
                ...record
              } = device;
              save({
                ...plan,
                revision: plan.revision + 1,
                devices: plan.devices.map((d, i) =>
                  i === deviceIndex
                    ? runningTarget !== undefined
                      ? {
                          ...record,
                          verifiedElsewhere: {
                            version: runningTarget.build.version,
                            observedAt: runningTarget.observedAt,
                          },
                        }
                      : {
                          ...record,
                          membership: "removed",
                          removedAt: now(),
                          archivedOperationCount: device.operations.length,
                          ...(replacement === undefined ? {} : { stageReplacement: replacement }),
                        }
                    : d,
                ),
              });
            }
          } else if (req.type === "end") {
            const downloaded = operations.findLast((operation) => operation.step === "download");
            if (plan.revision !== req.expectedRevision)
              refuse("stale-revision", "The plan changed.");
            else if (req.runningTarget !== undefined) {
              const { build, observedAt } = req.runningTarget;
              if (!validRunningTarget || !canRecordRunningTarget(device))
                refuse(
                  "invalid-evidence",
                  "Check the exact running build and settle pending work first.",
                );
              else
                save({
                  ...plan,
                  revision: plan.revision + 1,
                  devices: plan.devices.map((d, i) =>
                    i === deviceIndex
                      ? { ...d, verifiedElsewhere: { version: build.version, observedAt } }
                      : d,
                  ),
                });
            } else if (
              downloaded?.receipt?.phase !== "completed" ||
              device.operations.some(outstanding)
            )
              refuse(
                "outstanding-operation",
                "Only a fully staged device with no outstanding work can end here.",
              );
            else
              save({
                ...plan,
                revision: plan.revision + 1,
                devices: plan.devices.map((d, i) =>
                  i === deviceIndex ? { ...d, ended: { at: now() } } : d,
                ),
              });
          } else if (req.type === "report") {
            const index = device.operations.findIndex(
              (operation) => operation.operationId === req.operationId,
            );
            const operation = device.operations[index];
            const proof = req.evidence;
            if (operation === undefined || !matchesOperationTarget(plan, device, operation))
              refuse("invalid-evidence", "No matching reservation exists.");
            else if (proof.kind === "receipt") {
              const previous = operation.receipt;
              const validation = { ledgerRead: proof.ledgerRead === true };
              if (
                operation.request.kind !== "operation-start" ||
                !validateOperationEvidence(operation.request.input, proof.receipt, validation) ||
                (previous !== undefined &&
                  (!validateReceipt(previous, proof.receipt, validation) ||
                    (isProvisionalUnsupportedReceipt(previous) &&
                      !isSettledOperationReceipt(proof.receipt)))) ||
                !preservesHeldReason(previous, proof.receipt) ||
                !receiptDetailsFit(previous, proof.receipt) ||
                (proof.receipt.staged !== undefined &&
                  proof.receipt.staged.sourceTree !== plan!.target.sourceTree)
              )
                refuse("invalid-evidence", "The target receipt does not match this operation.");
              // The target owns its Stop receipt. The local Stop lease remains separate.
              else if (!same(previous, proof.receipt))
                replaceOperation(deviceIndex, index, { ...operation, receipt: proof.receipt });
            } else if (operation.request.kind !== "plan-directive")
              refuse("invalid-evidence", "A directive cannot settle an operation.");
            else if (proof.kind === "directive-fence") {
              const { input } = operation.request;
              const result = proof.proof;
              if (!recoverArchived || operation.fence === undefined)
                refuse("invalid-evidence", "This closed plan has no exact directive fence grant.");
              else if (proof.expectedRevision !== plan.revision)
                refuse("stale-revision", "The closed plan changed. Refresh the original setup.");
              else if (
                !outstanding(operation) ||
                device.route !== "follower-operations" ||
                input.enrollmentId !== device.enrollmentId ||
                !validateDirectiveFenceEvidence(input, result) ||
                (result.outcome === "applied" &&
                  Buffer.byteLength(JSON.stringify(result.directive.issuedAt)) > 1024)
              )
                refuse("invalid-evidence", "The fence proof does not match this setup request.");
              else
                replaceOperation(
                  deviceIndex,
                  index,
                  result.outcome === "applied"
                    ? {
                        ...operation,
                        directive: {
                          kind: "applied",
                          observedAt: now(),
                          directive: result.directive,
                        },
                      }
                    : {
                        ...operation,
                        directiveFence: { outcome: result.outcome, observedAt: now() },
                      },
                );
            } else if (proof.kind === "directive-refused")
              refuse("invalid-evidence", "A refusal does not settle every captured setup request.");
            else if (operation.directiveFence !== undefined)
              refuse("invalid-evidence", "The original directive fence is final.");
            else if (operation.directive !== undefined) {
              if (
                operation.directive.kind !== "applied" ||
                proof.status.enrollment?.environmentId !== device.environmentId ||
                !validateDirectiveEvidence(operation.request.input, proof.status)
              )
                refuse("invalid-evidence", "Directive evidence is final.");
            } else {
              if (
                proof.status.enrollment?.environmentId !== device.environmentId ||
                !validateDirectiveEvidence(operation.request.input, proof.status) ||
                !Number.isFinite(Date.parse(proof.status.follower!.planDirective!.issuedAt)) ||
                Buffer.byteLength(JSON.stringify(proof.status.follower!.planDirective!.issuedAt)) >
                  1024
              )
                refuse("invalid-evidence", "The directive binding changed.");
              else
                replaceOperation(deviceIndex, index, {
                  ...operation,
                  directive: {
                    kind: "applied",
                    observedAt: now(),
                    directive: proof.status.follower!.planDirective!,
                  },
                });
            }
          } else if (req.type === "claim") {
            const lease = {
              claimant: req.claimant,
              reservedAt: now(),
              leaseUntil: new Date(options.now() + LEASE_MS).toISOString(),
            };
            const grant = (
              operation: JonesInstallPlanHostOperation,
              step = req.step,
            ): JonesInstallPlanGrant => ({
              planId: plan!.planId,
              environmentId: device.environmentId,
              operationId: operation.operationId,
              step,
              request: operation.request,
              leaseUntil: operation.lease.leaseUntil,
            });
            const gateOpen = plan.laptop.gate !== undefined && plan.laptop.gateClosed === undefined;
            const pending = device.operations.find(outstanding);
            const targeted =
              req.operationId === undefined
                ? undefined
                : device.operations.find((operation) => operation.operationId === req.operationId);
            const previous = pending ?? device.operations.at(-1);
            const forward = req.step !== "stop" && req.step !== "fence";
            if (forward && heldByClosedPlan(document, device.environmentId))
              refuse("outstanding-operation", DEVICE_RECOVERY_HOLD);
            else if (
              forward &&
              req.operationId !== undefined &&
              (targeted === undefined ||
                targeted !== pending ||
                !operations.includes(targeted) ||
                targeted.step !== req.step ||
                !matchesOperationTarget(plan, device, targeted))
            )
              refuse(
                "invalid-evidence",
                "No matching outstanding reservation exists for this reclaim.",
              );
            else if (forward && !gateOpen)
              refuse(
                plan.laptop.gateClosed === undefined || plan.laptop.gatePause === true
                  ? "gate-closed"
                  : "laptop-changed",
                plan.laptop.gatePause === true
                  ? PAUSED_GATE_MESSAGE
                  : "Laptop verification no longer grants this action.",
              );
            else if (req.step === "stop" || req.step === "fence") {
              const directiveFence =
                recoverArchived &&
                req.step === "fence" &&
                targeted?.request.kind === "plan-directive";
              if (
                targeted === undefined ||
                (targeted.request.kind !== "operation-start" && !directiveFence) ||
                !operations.includes(targeted) ||
                !matchesOperationTarget(plan, device, targeted) ||
                (req.enrollmentId !== undefined && req.enrollmentId !== device.enrollmentId) ||
                (directiveFence &&
                  (device.route !== "follower-operations" ||
                    targeted.request.input.enrollmentId !== device.enrollmentId ||
                    req.enrollmentId !== device.enrollmentId)) ||
                (recoverArchived && !req.explicit)
              )
                refuse("invalid-evidence", "No bound target operation exists.");
              else {
                const index = device.operations.indexOf(targeted);
                if (req.step === "stop") {
                  if (
                    !req.explicit ||
                    targeted.step !== "download" ||
                    targeted.receipt === undefined ||
                    !["accepted", "running", "stop-requested"].includes(targeted.receipt.phase)
                  )
                    refuse("outstanding-operation", "Only a running transfer can be stopped.");
                  else if (
                    targeted.stop !== undefined &&
                    targeted.receipt.stop !== undefined &&
                    targeted.stop.stopId !== targeted.receipt.stop.stopId
                  )
                    refuse(
                      "outstanding-operation",
                      "The host already recorded a different Stop. Read its receipt.",
                    );
                  else if (
                    targeted.stop !== undefined &&
                    targeted.stop.lease.claimant !== req.claimant &&
                    Date.parse(targeted.stop.lease.leaseUntil) > options.now()
                  )
                    refuse("claimed", "Another window owns this Stop lease.");
                  else {
                    const stop =
                      targeted.stop === undefined
                        ? { stopId: targeted.receipt.stop?.stopId ?? options.uuid(), lease }
                        : {
                            ...targeted.stop,
                            lease:
                              Date.parse(targeted.stop.lease.leaseUntil) > options.now()
                                ? targeted.stop.lease
                                : lease,
                          };
                    if (!same(targeted.stop, stop))
                      replaceOperation(deviceIndex, index, { ...targeted, stop });
                    extras = {
                      grant: {
                        ...grant(targeted),
                        request: {
                          kind: "operation-stop",
                          input: {
                            operationId: targeted.operationId,
                            environmentId: device.environmentId,
                            stopId: stop.stopId,
                          },
                        },
                        leaseUntil: stop.lease.leaseUntil,
                      },
                    };
                  }
                } else if (
                  directiveFence
                    ? !outstanding(targeted)
                    : targeted.receipt !== undefined ||
                      !Number.isFinite(Date.parse(targeted.lease.leaseUntil)) ||
                      Date.parse(targeted.lease.leaseUntil) > options.now()
                )
                  refuse(
                    "outstanding-operation",
                    directiveFence
                      ? "This setup reservation already settled."
                      : "Only an expired, unobserved reservation can be fenced.",
                  );
                else if (
                  targeted.fence !== undefined &&
                  targeted.fence.lease.claimant !== req.claimant &&
                  Date.parse(targeted.fence.lease.leaseUntil) > options.now()
                )
                  refuse("claimed", "Another window owns the fence lease.");
                else {
                  const fence =
                    targeted.fence !== undefined &&
                    Date.parse(targeted.fence.lease.leaseUntil) > options.now()
                      ? targeted.fence
                      : { lease };
                  if (targeted.fence !== fence)
                    replaceOperation(deviceIndex, index, { ...targeted, fence });
                  // Full original intent is mandatory, including action/source/correlation. No synthetic tombstone binding.
                  extras = {
                    grant: {
                      ...grant(targeted),
                      request:
                        targeted.request.kind === "plan-directive"
                          ? {
                              kind: "plan-directive-fence",
                              input: targeted.request.input,
                              expectedRevision: plan.revision,
                            }
                          : { kind: "operation-fence", input: targeted.request.input },
                      leaseUntil: fence.lease.leaseUntil,
                    },
                  };
                }
              }
            } else if (
              device.ended !== undefined ||
              device.verifiedElsewhere !== undefined ||
              device.abandoning
            )
              refuse(
                "outstanding-operation",
                "This device's update has ended or is being abandoned.",
              );
            else if (pending !== undefined) {
              const identical =
                pending.step === req.step &&
                operations.includes(pending) &&
                matchesOperationTarget(plan, device, pending) &&
                (pending.request.kind === "operation-start"
                  ? same(pending.request.input.action, req.action) &&
                    pending.request.input.expectedInstalledSource === req.expectedInstalledSource &&
                    same(pending.review, req.review) &&
                    (req.enrollmentId === undefined || req.enrollmentId === device.enrollmentId)
                  : pending.request.input.enrollmentId === req.enrollmentId &&
                    pending.request.input.enrollmentId === device.enrollmentId &&
                    pending.request.input.expectedDirectiveId === req.expectedDirectiveId);
              const leaseUntil = Date.parse(pending.lease.leaseUntil);
              const expired = leaseUntil <= options.now();
              if (
                !identical ||
                pending.fence !== undefined ||
                pending.receipt !== undefined ||
                pending.directive !== undefined
              )
                refuse("claimed", "An operation is already reserved; observe or fence it.");
              else if (
                leaseUntil > options.now() &&
                pending.lease.claimant === req.claimant &&
                (req.operationId === undefined || req.operationId === pending.operationId)
              )
                extras = { grant: grant(pending) };
              else if (
                expired &&
                pending.request.kind === "plan-directive" &&
                req.operationId === pending.operationId
              ) {
                const renewed = { ...pending, lease };
                replaceOperation(deviceIndex, device.operations.indexOf(pending), renewed);
                extras = { grant: grant(renewed) };
              } else refuse("claimed", "An operation is already reserved; observe or fence it.");
            } else if (device.operations.length >= MAX_OPERATIONS)
              refuse("bounds", "The device operation history is full.");
            else if (req.step === "directive") {
              if (
                req.operationId !== undefined ||
                device.route !== "follower-operations" ||
                req.enrollmentId === undefined ||
                req.enrollmentId !== device.enrollmentId ||
                req.expectedDirectiveId === undefined ||
                operations.some(
                  (operation) =>
                    operation.step === "directive" && operation.directive?.kind !== "refused",
                ) ||
                (!req.explicit && operations.some((operation) => operation.step === "directive"))
              )
                refuse(
                  "not-selectable",
                  "A new exact enrollment directive cannot be claimed here.",
                );
              else {
                const operationId = options.uuid();
                const operation: JonesInstallPlanHostOperation = {
                  operationId,
                  step: "directive",
                  lease,
                  request: {
                    kind: "plan-directive",
                    input: {
                      environmentId: device.environmentId,
                      enrollmentId: req.enrollmentId,
                      directiveId: operationId,
                      repository: plan.target.repository,
                      expectedDirectiveId: req.expectedDirectiveId,
                      targetSource: plan.target.sourceSha,
                      targetTree: plan.target.sourceTree,
                    },
                  },
                };
                save({
                  ...plan,
                  revision: plan.revision + 1,
                  devices: plan.devices.map((d, i) =>
                    i === deviceIndex ? { ...d, operations: [...d.operations, operation] } : d,
                  ),
                });
                extras = { grant: grant(operation) };
              }
            } else {
              const action = req.action;
              const source = action?.kind === "download" ? action.sourceSha : action?.targetSource;
              const downloaded = operations.findLast((operation) => operation.step === "download");
              const followerAdmission =
                device.route === "follower-operations"
                  ? appliedFollowerAdmission(
                      plan,
                      device,
                      operations.findLast((operation) => operation.step === "directive"),
                    )
                  : undefined;
              const admission =
                req.boundAdmission === 1
                  ? device.route === "operations"
                    ? { kind: "ordinary" as const }
                    : followerAdmission
                  : undefined;
              if (
                action === undefined ||
                req.expectedInstalledSource === undefined ||
                (req.enrollmentId !== undefined && req.enrollmentId !== device.enrollmentId) ||
                (admission?.kind === "ordinary" && device.enrollmentId !== undefined) ||
                source !== plan.target.sourceSha ||
                (req.step === "install") !== (action.kind === "install")
              )
                refuse("invalid-evidence", "The action must match this plan's target.");
              else if (
                device.route === "follower-operations" &&
                (req.boundAdmission === 1 ||
                  req.expectedInstalledSource !== plan.target.sourceSha) &&
                followerAdmission === undefined
              )
                refuse("directive-required", "Apply the manual-review directive first.");
              else if (
                req.step === "download" &&
                downloaded !== undefined &&
                ((!req.explicit && downloaded.receipt?.phase !== "fenced") ||
                  (downloaded.receipt?.phase === "completed" &&
                    downloaded.receipt.stageRetiredAt === undefined &&
                    !(
                      previous?.step === "install" &&
                      ["rolled-back", "refused", "failed", "interrupted"].includes(
                        previous.receipt?.phase ?? "",
                      )
                    )))
              )
                refuse(
                  "outstanding-operation",
                  "Another download requires an explicit retry after a proved terminal result.",
                );
              else if (
                req.step === "install" &&
                (!req.explicit ||
                  req.review === undefined ||
                  action.kind !== "install" ||
                  downloaded?.receipt?.phase !== "completed" ||
                  !matchesOperationTarget(plan, device, downloaded) ||
                  downloaded.receipt.stageRetiredAt !== undefined ||
                  downloaded.receipt.staged?.stagedHandle !== action.stagedHandle ||
                  downloaded.receipt.staged.sourceSha !== plan.target.sourceSha ||
                  downloaded.receipt.staged.sourceTree !== plan.target.sourceTree ||
                  req.review.stagedHandle !== action.stagedHandle ||
                  req.review.targetSource !== action.targetSource ||
                  req.review.currentVersion !== action.currentVersion ||
                  req.review.expectedInstalledSource !== req.expectedInstalledSource ||
                  req.review.continueRunningThreads !== action.continueRunningThreads ||
                  !same(req.review.recovery, action.recovery) ||
                  !Number.isFinite(Date.parse(req.review.reviewedAt)) ||
                  Date.parse(req.review.reviewedAt) > options.now() ||
                  options.now() - Date.parse(req.review.reviewedAt) > LEASE_MS)
              )
                refuse(
                  "invalid-evidence",
                  "Install requires a fresh review of the latest completed stage.",
                );
              else if (previous?.step === "directive" && previous.directive?.kind === "refused")
                refuse("directive-required", "The host refused its manual-review directive.");
              else {
                const operationId = options.uuid();
                const operation: JonesInstallPlanHostOperation = {
                  operationId,
                  step: req.step,
                  lease,
                  ...(req.review === undefined ? {} : { review: req.review }),
                  request: {
                    kind: "operation-start",
                    input: {
                      operationId,
                      environmentId: device.environmentId,
                      expectedInstalledSource: req.expectedInstalledSource,
                      action,
                      correlation: { planId: plan.planId, deviceKey: device.environmentId },
                      ...(admission === undefined ? {} : { admission }),
                    },
                  },
                };
                save({
                  ...plan,
                  revision: plan.revision + 1,
                  devices: plan.devices.map((d, i) =>
                    i === deviceIndex ? { ...d, operations: [...d.operations, operation] } : d,
                  ),
                });
                extras = { grant: grant(operation) };
              }
            }
          }
        }
        if (
          command !== undefined &&
          boundCommand &&
          snapshot.readOnly === undefined &&
          ["start", "edit", "laptop", "claim"].includes(req.type)
        ) {
          const local = plan?.laptop.environmentId === command.environmentId ? effect : undefined;
          const body = commandBody(req);
          const planId =
            req.type === "start"
              ? extras.refused === undefined
                ? plan?.planId
                : undefined
              : command.planId;
          const receipt = {
            command,
            bodyHash: body,
            requestType: req.type as "start" | "edit" | "laptop" | "claim",
            ...(planId === undefined ? {} : { planId }),
            ...(local !== undefined
              ? { operationId: local.operationId }
              : extras.grant === undefined
                ? {}
                : { operationId: extras.grant.operationId }),
            outcome:
              extras.refused !== undefined
                ? ("refused" as const)
                : local !== undefined || extras.grant !== undefined
                  ? ("reserved" as const)
                  : ("applied" as const),
          };
          document = {
            ...document,
            commands: [...(document.commands ?? []), { body, receipt }],
          };
          extras = { ...extras, command: receipt };
        }
        const forwardMutation =
          req.type === "start" ||
          req.type === "edit" ||
          (req.type === "abandon-device" && req.phase === "begin") ||
          (req.type === "claim" && req.step !== "stop" && req.step !== "fence") ||
          (effect !== undefined && !recoverArchived && effect.action !== "stop");
        if (document !== beforeAdmission && (forwardMutation || command !== undefined)) {
          const admitted = admitPlanWork(
            document,
            req.type === "start" && snapshot.document.schema !== 1 && extras.refused === undefined,
          );
          if (admitted !== undefined) document = admitted;
          else {
            document = beforeAdmission;
            dispositions = beforeDispositions;
            effect = undefined;
            if (extras.refused === undefined)
              refuse("bounds", "The journal has no room for new work and its recovery evidence.");
            else extras = { refused: extras.refused };
          }
        }
        document = commitRevision(snapshot.document, refreshCommands(document));
        const queried =
          req.type === "read" && req.commandId !== undefined
            ? document.commands?.find((entry) => entry.receipt.command.commandId === req.commandId)
                ?.receipt
            : undefined;
        const result: JonesInstallPlanResult = {
          state: stateOf(snapshot, evidence, document),
          ...extras,
          ...(queried === undefined ? {} : { command: queried }),
        };
        if (effect !== undefined) active.add(effect.operationId);
        return { document, dispositions, result };
      });
      announce(result.state);
      if (effect !== undefined) void runLaptop(effect);
      return result;
    } catch (cause) {
      if (effect !== undefined) active.delete(effect.operationId);
      const evidence = await options.controller.laptopEvidence();
      const state: JonesInstallPlanState = {
        ...(lastState ?? {
          schema: 1 as const,
          current: null,
          last: null,
          laptop: evidence,
          view: { complete: false },
        }),
        laptop: evidence,
        view: { complete: false },
        readOnly: {
          code: "invalid",
          message: "The plan could not be safely stored. Existing evidence was retained.",
        },
      };
      const code = cause instanceof PlanStorageError ? cause.code : "storage";
      return {
        state,
        refused: {
          code,
          message:
            code === "bounds"
              ? "The plan reached its retained storage bound."
              : "The native plan journal is unavailable; no action was granted.",
        },
      };
    }
  };
  return {
    request,
    dispose: () => {
      disposed = true;
    },
    recordLaptopAdmission: async (operationId: string): Promise<void> => {
      await nativeChange(
        ownsLaptopOperation(operationId),
        (plan) => {
          const operation = plan.laptop.operations.find(
            (entry) => entry.operationId === operationId && entry.step === "download",
          );
          if (operation === undefined || !localPending(operation))
            throw new Error("The copy admission has no matching Laptop transfer.");
          if (operation.result?.code === "copy-admitted") return plan;
          return {
            ...plan,
            revision: plan.revision + 1,
            laptop: {
              ...plan.laptop,
              operations: plan.laptop.operations.map((entry) =>
                entry === operation
                  ? {
                      ...entry,
                      result: { status: "running", code: "copy-admitted", at: now() },
                    }
                  : entry,
              ),
            },
          };
        },
        { required: true },
      );
    },
    recordLaptopWorker: async (input: {
      readonly operationId: string;
      readonly pid: number;
      readonly processGroup: number;
    }): Promise<void> => {
      await nativeChange(
        ownsLaptopOperation(input.operationId),
        (plan) => {
          const operation = plan.laptop.operations.find(
            (entry) => entry.operationId === input.operationId && entry.step === "download",
          );
          if (
            operation === undefined ||
            !localPending(operation) ||
            ![input.pid, input.processGroup].every((pid) => Number.isSafeInteger(pid) && pid > 1)
          )
            throw new Error("The preparation worker does not match its Laptop operation.");
          const worker = { pid: input.pid, processGroup: input.processGroup };
          if (operation.worker !== undefined && !same(operation.worker, worker))
            throw new Error("The captured preparation worker identity changed.");
          return operation.worker !== undefined
            ? plan
            : {
                ...plan,
                revision: plan.revision + 1,
                laptop: {
                  ...plan.laptop,
                  operations: plan.laptop.operations.map((entry) =>
                    entry === operation ? { ...entry, worker } : entry,
                  ),
                },
              };
        },
        { required: true },
      );
    },
    bindLaptopInstall: async (binding: LaptopInstallBinding): Promise<void> => {
      await nativeChange(
        ownsLaptopOperation(binding.operationId),
        (plan, evidence) => {
          const operation = plan.laptop.operations.find(
            (entry) => entry.operationId === binding.operationId && entry.step === "install",
          );
          const previous = plan.laptop.binding;
          if (
            disposed ||
            !active.has(binding.operationId) ||
            operation === undefined ||
            !localPending(operation) ||
            operation.review?.stagedHandle !== binding.stagedHandle ||
            binding.targetSource !== plan.target.sourceSha ||
            binding.targetTree !== plan.target.sourceTree ||
            !/^[a-f0-9]{64}$/.test(binding.transactionId) ||
            !binding.fromGeneration
          )
            throw new Error("The native install does not match its plan review.");
          if (previous?.operationId === binding.operationId) {
            if (
              operation.prebinding !== undefined ||
              !same({ ...binding, boundAt: previous.boundAt }, previous)
            )
              throw new Error("The native install binding changed.");
            return plan;
          }
          const priorOperation = plan.laptop.operations.find(
            (entry) => entry.operationId === previous?.operationId,
          );
          if (
            operation.prebinding?.fromGeneration !== binding.fromGeneration ||
            operation.worker !== undefined ||
            (previous !== undefined &&
              (priorOperation === undefined || localPending(priorOperation))) ||
            !matchesPrebinding(plan, operation, evidence)
          )
            throw new Error("The native install no longer matches its reserved Laptop preimage.");

          const { prebinding: _reservation, ...bound } = operation;
          return {
            ...plan,
            revision: plan.revision + 1,
            laptop: {
              ...plan.laptop,
              operations: plan.laptop.operations.map((entry) =>
                entry === operation ? bound : entry,
              ),
              binding: { ...binding, boundAt: now() },
            },
          };
        },
        { required: true },
      );
      // Shutdown can race the journal write. A durable binding still needs native readback.
      if (disposed) throw new Error("The native install plan coordinator is closing.");
    },
    recordLaptopOutcome: async (outcome: LaptopNativeOutcome): Promise<void> => {
      const operationFor = (plan: JonesInstallPlan) => {
        const binding = plan.laptop.binding;
        return binding?.transactionId === outcome.transactionId
          ? plan.laptop.operations.find(
              (entry) => entry.operationId === binding.operationId && entry.step === "install",
            )
          : undefined;
      };
      await nativeChange(
        (plan) => {
          const operation = operationFor(plan);
          return operation !== undefined && localPending(operation);
        },
        (plan, _evidence, archived) => {
          const binding = plan.laptop.binding!;
          const operation = operationFor(plan)!;
          const committed =
            outcome.status === "committed" &&
            outcome.activeGeneration === binding.transactionId &&
            outcome.activeGeneration !== binding.fromGeneration &&
            outcome.activeSource === binding.targetSource &&
            outcome.activeTree === binding.targetTree;
          const rolledBack =
            outcome.status === "rolled-back" &&
            outcome.activeGeneration === binding.fromGeneration &&
            outcome.activeSource === operation.review?.expectedInstalledSource;
          const status = committed ? "committed" : rolledBack ? "rolled-back" : "held";
          const closeGate =
            !archived &&
            rolledBack &&
            plan.laptop.gate !== undefined &&
            (plan.laptop.gateClosed === undefined || plan.laptop.gatePause === true);
          if (operation.result?.status === status && !closeGate) return plan;
          const next = {
            ...plan,
            revision: plan.revision + 1,
            laptop: {
              ...plan.laptop,
              operations: plan.laptop.operations.map((entry) =>
                entry.operationId === binding.operationId
                  ? {
                      ...entry,
                      result: {
                        status,
                        at: now(),
                        ...(status === "held" ? { code: "native-binding-changed" } : {}),
                      },
                    }
                  : entry,
              ),
            },
          } satisfies JonesInstallPlan;
          if (!closeGate) return next;
          const { gatePause: _pause, ...laptop } = next.laptop;
          return {
            ...next,
            laptop: { ...laptop, gateClosed: { at: now(), reason: "rolled-back" } },
          };
        },
        {
          // A handled failure can share a transaction ID with a later attempt. Keep it as history.
          currentFallback: (plan) =>
            outcome.status === "rolled-back" && operationFor(plan)?.result?.status === "committed",
        },
      );
    },
    withUnmanagedTarget: async <A>(sourceSha: string, work: () => Promise<A>): Promise<A> =>
      transact(async (snapshot) => {
        if (
          disposed ||
          snapshot.readOnly !== undefined ||
          hasLaptopHold(snapshot.document) ||
          snapshot.dispositions.targets.some((target) => target.targetSource === sourceSha)
        )
          throw new Error("This build is managed by the update plan.");
        return { ...snapshot, result: await work() };
      }),
    preparationHeld: async (issuedOperationId?: string): Promise<boolean> => {
      const result = await options.store.transact<{ held: boolean; state?: JonesInstallPlanState }>(
        async (snapshot) => {
          if (disposed || snapshot.readOnly !== undefined)
            return { ...snapshot, result: { held: true } };
          const startup = startupRecoveryFor(snapshot);
          const evidence = await options.controller.laptopEvidence(startup?.preimage);
          const next = await reconcileJournal(snapshot, evidence, startup?.operation);
          const document = commitRevision(snapshot.document, next.document);
          return {
            ...next,
            document,
            result: {
              held: hasLaptopHold(document, issuedOperationId),
              ...(document === snapshot.document
                ? {}
                : { state: stateOf(snapshot, evidence, document) }),
            },
          };
        },
      );
      if (result.state !== undefined) announce(result.state);
      return result.held;
    },
    isPlanManaged: async (sourceSha: string): Promise<boolean> =>
      transact(async (snapshot) => ({
        ...snapshot,
        result:
          disposed ||
          snapshot.readOnly !== undefined ||
          snapshot.dispositions.targets.some((target) => target.targetSource === sourceSha),
      })),
    backgroundSuppressed: async (sourceSha: string): Promise<boolean> =>
      transact(async (snapshot) => ({
        ...snapshot,
        result:
          disposed ||
          snapshot.readOnly !== undefined ||
          hasLaptopHold(snapshot.document) ||
          snapshot.dispositions.targets.some(
            (target) => target.targetSource === sourceSha && target.laptopStopped === true,
          ) ||
          (snapshot.document.current !== null &&
            options.controller.isStaged({
              targetSource: snapshot.document.current.target.sourceSha,
              targetTree: snapshot.document.current.target.sourceTree,
            })),
      })),
  };
}
