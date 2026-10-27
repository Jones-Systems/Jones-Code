// @effect-diagnostics nodeBuiltinImport:off globalDate:off - Native coordinator supplies clocks and validates captured worker exit evidence.
import * as Schema from "effect/Schema";
import type { EnvironmentId } from "@t3tools/contracts";
import {
  JonesInstallPlanRequest as RequestSchema,
  type JonesInstallPlanRequest,
  type JonesInstallPlanResult,
  type JonesInstallPlanState,
  type JonesInstallPlan,
  type JonesInstallPlanLaptopEvidence,
  type JonesInstallPlanLaptopOperation,
  type JonesInstallPlanHostOperation,
  type JonesInstallPlanGrant,
  type JonesInstallPlanReview,
} from "@t3tools/contracts/jones/installPlan";
import {
  startPlan,
  applyEdit,
  canSupersede,
  isPlanComplete,
  validateReceipt,
  validateDirectiveEvidence,
} from "@t3tools/client-runtime/jones/install-plan/model";
import { RecoveryPreflightRefused } from "../updates/recoveryPreflight.ts";
import type { JonesDesktopUpdateController } from "../updates/JonesDesktopUpdates.ts";
import {
  PlanStorageError,
  type InstallPlanStore,
  type PlanSnapshot,
  type PlanDocument,
  type PlanDispositions,
} from "./store.ts";

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
const terminal = new Set([
  "completed",
  "rolled-back",
  "stopped",
  "refused",
  "failed",
  "blocked",
  "fenced",
  "interrupted",
]);
const localPending = (operation: JonesInstallPlanLaptopOperation) =>
  operation.result === undefined ||
  ["running", "stop-requested", "held"].includes(operation.result.status);
const outstanding = (operation: JonesInstallPlanHostOperation) =>
  operation.step === "directive"
    ? operation.directive === undefined
    : operation.receipt === undefined || !terminal.has(operation.receipt.phase);
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

export function createInstallPlanCoordinator(options: {
  readonly store: InstallPlanStore;
  readonly controller: Pick<
    JonesDesktopUpdateController,
    | "state"
    | "busy"
    | "laptopEvidence"
    | "download"
    | "stopDownload"
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
  let lastState: JonesInstallPlanState | undefined;
  const now = () => new Date(options.now()).toISOString();
  const stateOf = (
    snapshot: PlanSnapshot,
    evidence: JonesInstallPlanLaptopEvidence,
    document = snapshot.document,
  ): JonesInstallPlanState => ({
    ...document,
    laptop: evidence,
    view: {
      complete:
        snapshot.readOnly === undefined &&
        document.current !== null &&
        isPlanComplete(document.current),
    },
    ...(snapshot.readOnly === undefined ? {} : { readOnly: snapshot.readOnly }),
  });
  const gate = (
    plan: JonesInstallPlan,
    evidence: JonesInstallPlanLaptopEvidence,
  ): JonesInstallPlan => {
    if (plan.laptop.gateClosed !== undefined) return plan;
    const old = plan.laptop.gate;
    const identity = evidence.environmentId === plan.laptop.environmentId;
    if (old !== undefined) {
      if (
        !identity ||
        evidence.activationBlocked ||
        old.transactionId !== evidence.committedTransactionId ||
        old.activeGeneration !== evidence.activeGeneration ||
        old.activeSource !== evidence.activeSource ||
        old.activeTree !== evidence.activeTree
      )
        return {
          ...plan,
          revision: plan.revision + 1,
          laptop: {
            ...plan.laptop,
            gateClosed: { at: now(), reason: evidence.activationBlocked ? "blocked" : "changed" },
          },
        };
      return plan;
    }
    if (
      !identity ||
      evidence.activationBlocked ||
      evidence.committedTransactionId === undefined ||
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
            plan.laptop.binding?.transactionId === evidence.committedTransactionId
              ? "plan-install"
              : "prior-verified",
          transactionId: evidence.committedTransactionId,
          activeGeneration: evidence.activeGeneration,
          activeSource: evidence.activeSource,
          activeTree: evidence.activeTree,
        },
      },
    };
  };
  const reconcile = (plan: JonesInstallPlan): JonesInstallPlan => {
    let changed = false;
    const operations = plan.laptop.operations.map((operation): JonesInstallPlanLaptopOperation => {
      if (
        active.has(operation.operationId) ||
        (operation.of !== undefined && active.has(operation.of)) ||
        !localPending(operation) ||
        operation.step === "install"
      )
        return operation;
      const staged = options.controller.isStaged({
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
    });
    return changed
      ? { ...plan, revision: plan.revision + 1, laptop: { ...plan.laptop, operations } }
      : plan;
  };
  const announce = (state: JonesInstallPlanState) => {
    lastState = state;
    options.broadcast(state);
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
    change: (plan: JonesInstallPlan) => JonesInstallPlan,
    required = false,
  ) => {
    const state = await transact(async (snapshot, evidence) => {
      if (snapshot.readOnly !== undefined) throw new Error("Native plan journal is read-only.");
      const plan = snapshot.document.current;
      if (required && plan === null) throw new Error("The native install has no current plan.");
      const next = plan === null ? null : change(plan);
      const document = next === plan ? snapshot.document : { ...snapshot.document, current: next };
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
  ) => {
    await nativeChange((plan) => {
      if (plan.planId !== planId) return plan;
      return {
        ...plan,
        revision: plan.revision + 1,
        laptop: {
          ...plan.laptop,
          operations: plan.laptop.operations.map((operation) =>
            operation.operationId !== operationId
              ? operation
              : {
                  ...operation,
                  ...(worker === undefined ? {} : { worker }),
                  result,
                },
          ),
        },
      };
    });
  };
  type LaptopEffect = {
    planId: string;
    operationId: string;
    action: "download" | "stop" | "install";
    target: JonesInstallPlan["target"];
    review?: JonesInstallPlanReview;
    of?: string;
    artifactId?: number;
  };
  const runLaptop = async (effect: LaptopEffect) => {
    active.add(effect.operationId);
    try {
      if (effect.action === "download") {
        const result = await options.track(() =>
          options.controller.download(
            { artifactId: effect.artifactId!, sourceSha: effect.target.sourceSha },
            { operationId: effect.operationId },
          ),
        );
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
        const result = await options.track(() =>
          options.controller.install(effect.review!.stagedHandle, undefined, {
            operationId: effect.operationId,
            ...(effect.review!.recovery === undefined ? {} : { recovery: effect.review!.recovery }),
          }),
        );
        // A bound attempt may already have native effects even if the caller returned a refusal.
        await nativeChange((plan) => {
          if (
            plan.planId !== effect.planId ||
            result.accepted ||
            plan.laptop.binding?.operationId === effect.operationId
          )
            return plan;
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
                        status: "refused",
                        code: result.refusal ?? "install-refused",
                        at: now(),
                      },
                    }
                  : operation,
              ),
            },
          };
        });
      }
    } catch {
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

  const request = async (req: JonesInstallPlanRequest): Promise<JonesInstallPlanResult> => {
    let effect: LaptopEffect | undefined;
    try {
      if (req.type === "recovery-preflight") {
        // Cold staged review has no plan ID, reservation, disposition or install authority.
        return await transact(async (snapshot, evidence) => {
          let extras: Omit<JonesInstallPlanResult, "state">;
          if (!Schema.is(RequestSchema)(req))
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
      const result = await transact(async (snapshot, evidence) => {
        let document = snapshot.document;
        let dispositions = snapshot.dispositions;
        let plan = document.current;
        if (plan !== null && snapshot.readOnly === undefined) {
          const next = gate(reconcile(plan), evidence);
          if (next !== plan) {
            plan = next;
            document = { ...document, current: plan };
          }
          if (
            plan.laptop.operations.some(
              (operation) =>
                operation.result?.status === "held" || operation.result?.status === "interrupted",
            )
          ) {
            if (
              dispositions.targets.some(
                (target) =>
                  target.targetSource === plan!.target.sourceSha && target.laptopStopped !== true,
              )
            )
              dispositions = {
                ...dispositions,
                targets: dispositions.targets.map((target) =>
                  target.targetSource === plan!.target.sourceSha
                    ? { ...target, laptopStopped: true }
                    : target,
                ),
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
          document = { ...document, current: next };
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
        ) => {
          const current = plan!;
          if (current.laptop.operations.length >= MAX_OPERATIONS)
            return refuse("bounds", "The Laptop operation history is full.");
          if (action !== "stop" && pendingLaptop())
            return refuse("outstanding-operation", "Laptop work still requires settlement.");
          const native = options.controller.state.jones;
          let of: string | undefined;
          if (action === "download") {
            if (
              native?.provenance?.sourceSha !== current.target.sourceSha ||
              native.provenance.sourceTree !== current.target.sourceTree ||
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
          } else {
            const latest = current.laptop.operations.findLast(
              (operation) => operation.step === "download",
            );
            if (latest === undefined || !localPending(latest) || latest.result?.status === "held")
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
            result: { status: action === "stop" ? "stop-requested" : "running", at: now() },
          };
          save({
            ...current,
            revision: current.revision + 1,
            laptop: { ...current.laptop, operations: [...current.laptop.operations, operation] },
          });
          if (action === "download" || action === "stop")
            dispositions = {
              ...dispositions,
              targets: dispositions.targets.map((target) =>
                target.targetSource === current.target.sourceSha
                  ? { ...target, laptopStopped: action === "stop" }
                  : target,
              ),
            };
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
        if (!Schema.is(RequestSchema)(req)) refuse("invalid-evidence", "Invalid plan request.");
        else if (snapshot.readOnly !== undefined) {
          if (req.type !== "read")
            refuse(
              snapshot.readOnly.code === "newer-schema" ? "newer-schema" : "storage",
              snapshot.readOnly.message,
            );
        } else if (req.type === "read") {
          /* Gate and native reconciliation above are the only changes. */
        } else if (req.type === "publication") {
          const code = evidence.activationBlocked
            ? "blocked"
            : evidence.activeSource === undefined || evidence.activeTree === undefined
              ? "unavailable"
              : dispositions.targets.some((target) => target.targetSource === evidence.activeSource)
                ? "plan-managed"
                : evidence.committedTransactionId === undefined
                  ? "unproved"
                  : undefined;
          extras = {
            publication:
              code === undefined
                ? {
                    kind: "committed",
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
                      "Follower publication requires an unmanaged, natively committed active build.",
                  },
          };
        } else if (req.type === "start") {
          const environmentId = options.localEnvironmentId();
          const prior =
            evidence.environmentId === environmentId &&
            evidence.committedTransactionId !== undefined &&
            !evidence.activationBlocked &&
            evidence.activeSource === req.target.sourceSha &&
            evidence.activeTree === req.target.sourceTree;
          if (plan !== null && (!canSupersede(plan) || pendingLaptop()))
            refuse("plan-active", "Finish the current plan before starting another.");
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
              select: req.select,
              now: now(),
            });
            let targets = [...dispositions.targets];
            if (!targets.some((target) => target.targetSource === req.target.sourceSha)) {
              if (targets.length >= 64) {
                const protectedSources = [
                  evidence.activeSource,
                  plan?.target.sourceSha,
                  document.last?.target.sourceSha,
                  req.target.sourceSha,
                ];
                const candidate = targets
                  .filter((target) => !protectedSources.includes(target.targetSource))
                  .sort((a, b) => a.createdAt.localeCompare(b.createdAt))[0];
                if (candidate === undefined)
                  refuse("bounds", "No retained target disposition can be evicted.");
                else targets = targets.filter((target) => target !== candidate);
              }
              targets.push({
                targetSource: req.target.sourceSha,
                targetTree: req.target.sourceTree,
                planId: next.planId,
                createdAt: now(),
              });
            }
            if (extras.refused === undefined) {
              dispositions = { ...dispositions, targets };
              document = {
                ...document,
                last: plan === null ? document.last : { ...plan, supersededAt: now() },
                current: gate(next, evidence),
              };
              plan = document.current;
              if (req.laptop !== undefined)
                await recordLaptop(req.laptop.action, req.laptop.review);
              // A refused first action must not leave a newly created plan or disposition behind.
              if (extras.refused !== undefined) {
                document = snapshot.document;
                dispositions = snapshot.dispositions;
                plan = document.current;
              }
            }
          }
        } else if (plan === null || req.planId !== plan.planId || plan.dismissedAt !== undefined)
          refuse("plan-mismatch", "This is not the current plan.");
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
            const edited = applyEdit(plan, req, now());
            if (edited.refused !== undefined)
              refuse(edited.refused, "The plan changed or reached its device bound.");
            else {
              save(edited.plan);
              extras = { applied: edited.applied };
            }
          }
        } else if (req.type === "laptop") {
          if (req.action === "preflight") {
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
          } else await recordLaptop(req.action, req.review);
        } else if (req.type === "dismiss") {
          if (req.expectedRevision !== plan.revision) refuse("stale-revision", "The plan changed.");
          else if (!isPlanComplete(plan))
            refuse("not-complete", "The chosen plan still has unsettled work.");
          else {
            document = {
              ...document,
              current: null,
              last: { ...plan, dismissedAt: now(), revision: plan.revision + 1 },
            };
            plan = null;
          }
        } else {
          const deviceIndex = plan.devices.findIndex(
            (device) => device.environmentId === req.environmentId,
          );
          const device = plan.devices[deviceIndex];
          if (device === undefined || device.membership !== "selected")
            refuse("not-selectable", "This device is not selected.");
          else if (req.type === "end") {
            const downloaded = device.operations.findLast(
              (operation) => operation.step === "download",
            );
            if (plan.revision !== req.expectedRevision)
              refuse("stale-revision", "The plan changed.");
            else if (
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
            if (operation === undefined)
              refuse("invalid-evidence", "No matching reservation exists.");
            else if (proof.kind === "receipt") {
              if (
                operation.request.kind !== "operation-start" ||
                !validateReceipt(operation.request.input, proof.receipt) ||
                (operation.receipt !== undefined &&
                  !validateReceipt(operation.receipt, proof.receipt)) ||
                (proof.receipt.staged !== undefined &&
                  proof.receipt.staged.sourceTree !== plan!.target.sourceTree) ||
                (proof.receipt.stop !== undefined &&
                  proof.receipt.stop.stopId !== operation.stop?.stopId)
              )
                refuse("invalid-evidence", "The target receipt does not match this operation.");
              else if (!same(operation.receipt, proof.receipt))
                replaceOperation(deviceIndex, index, { ...operation, receipt: proof.receipt });
            } else if (operation.request.kind !== "plan-directive")
              refuse("invalid-evidence", "A directive cannot settle an operation.");
            else if (operation.directive !== undefined) {
              if (
                proof.kind !== "directive" ||
                operation.directive.kind !== "applied" ||
                !validateDirectiveEvidence(operation.request.input, proof.status)
              )
                refuse("invalid-evidence", "Directive evidence is final.");
            } else if (proof.kind === "directive") {
              if (!validateDirectiveEvidence(operation.request.input, proof.status))
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
            } else
              replaceOperation(deviceIndex, index, {
                ...operation,
                directive: {
                  kind: "refused",
                  observedAt: now(),
                  reason: proof.reason,
                  message: proof.message,
                },
              });
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
            if (["directive", "download", "install"].includes(req.step) && !gateOpen)
              refuse(
                plan.laptop.gateClosed === undefined ? "gate-closed" : "laptop-changed",
                "Laptop verification no longer grants this action.",
              );
            else if (req.step === "stop" || req.step === "fence") {
              if (targeted === undefined || targeted.request.kind !== "operation-start")
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
                    targeted.stop.lease.claimant !== req.claimant &&
                    Date.parse(targeted.stop.lease.leaseUntil) > options.now()
                  )
                    refuse("claimed", "Another window owns this Stop lease.");
                  else {
                    const stop =
                      targeted.stop === undefined
                        ? { stopId: options.uuid(), lease }
                        : {
                            ...targeted.stop,
                            lease:
                              Date.parse(targeted.stop.lease.leaseUntil) > options.now()
                                ? targeted.stop.lease
                                : lease,
                          };
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
                  targeted.receipt !== undefined ||
                  Date.parse(targeted.lease.leaseUntil) > options.now()
                )
                  refuse(
                    "outstanding-operation",
                    "Only an expired, unobserved reservation can be fenced.",
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
                  replaceOperation(deviceIndex, index, { ...targeted, fence });
                  // Full original intent is mandatory, including action/source/correlation. No synthetic tombstone binding.
                  extras = {
                    grant: {
                      ...grant(targeted),
                      request: { kind: "operation-fence", input: targeted.request.input },
                      leaseUntil: fence.lease.leaseUntil,
                    },
                  };
                }
              }
            } else if (device.ended !== undefined)
              refuse("outstanding-operation", "This device's update has ended.");
            else if (pending !== undefined) {
              const identical =
                pending.step === req.step &&
                pending.lease.claimant === req.claimant &&
                Date.parse(pending.lease.leaseUntil) > options.now() &&
                (pending.request.kind === "operation-start"
                  ? same(pending.request.input.action, req.action) &&
                    pending.request.input.expectedInstalledSource === req.expectedInstalledSource &&
                    same(pending.review, req.review)
                  : pending.request.input.enrollmentId === req.enrollmentId &&
                    pending.request.input.expectedDirectiveId === req.expectedDirectiveId);
              if (!identical || pending.receipt !== undefined || pending.directive !== undefined)
                refuse("claimed", "An operation is already reserved; observe or fence it.");
              else extras = { grant: grant(pending) };
            } else if (device.operations.length >= MAX_OPERATIONS)
              refuse("bounds", "The device operation history is full.");
            else if (req.step === "directive") {
              if (
                device.route !== "follower-operations" ||
                req.enrollmentId === undefined ||
                req.enrollmentId !== device.enrollmentId ||
                req.expectedDirectiveId === undefined ||
                device.operations.some((operation) => operation.step === "directive")
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
              const downloaded = device.operations.findLast(
                (operation) => operation.step === "download",
              );
              if (
                action === undefined ||
                req.expectedInstalledSource === undefined ||
                source !== plan.target.sourceSha ||
                (req.step === "install") !== (action.kind === "install")
              )
                refuse("invalid-evidence", "The action must match this plan's target.");
              else if (
                device.route === "follower-operations" &&
                req.expectedInstalledSource !== plan.target.sourceSha &&
                !device.operations.some((operation) => operation.directive?.kind === "applied")
              )
                refuse("directive-required", "Apply the manual-review directive first.");
              else if (
                req.step === "download" &&
                downloaded !== undefined &&
                (downloaded.receipt?.phase === "completed" ||
                  (!req.explicit && downloaded.receipt?.phase !== "fenced"))
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
        const result: JonesInstallPlanResult = {
          state: stateOf(snapshot, evidence, document),
          ...extras,
        };
        return { document, dispositions, result };
      });
      announce(result.state);
      if (effect !== undefined) void runLaptop(effect);
      return result;
    } catch (cause) {
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
    recordLaptopAdmission: async (operationId: string): Promise<void> => {
      await nativeChange((plan) => {
        const operation = plan.laptop.operations.find(
          (entry) => entry.operationId === operationId && entry.step === "download",
        );
        if (operation === undefined || !localPending(operation))
          throw new Error("The copy admission has no current Laptop transfer.");
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
      }, true);
    },
    recordLaptopWorker: async (input: {
      readonly operationId: string;
      readonly pid: number;
      readonly processGroup: number;
    }): Promise<void> => {
      await nativeChange((plan) => {
        const operation = plan.laptop.operations.find(
          (entry) => entry.operationId === input.operationId && entry.step === "download",
        );
        if (
          operation === undefined ||
          !localPending(operation) ||
          ![input.pid, input.processGroup].every((pid) => Number.isSafeInteger(pid) && pid > 1)
        )
          throw new Error("The preparation worker does not match the current Laptop operation.");
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
      }, true);
    },
    bindLaptopInstall: async (binding: LaptopInstallBinding): Promise<void> => {
      await nativeChange((plan) => {
        const operation = plan.laptop.operations.find(
          (entry) => entry.operationId === binding.operationId && entry.step === "install",
        );
        if (
          operation === undefined ||
          !localPending(operation) ||
          operation.review?.stagedHandle !== binding.stagedHandle ||
          binding.targetSource !== plan.target.sourceSha ||
          binding.targetTree !== plan.target.sourceTree ||
          !/^[a-f0-9]{64}$/.test(binding.transactionId) ||
          !binding.fromGeneration ||
          (plan.laptop.binding !== undefined &&
            plan.laptop.binding.operationId === binding.operationId &&
            !same({ ...binding, boundAt: plan.laptop.binding.boundAt }, plan.laptop.binding)) ||
          (plan.laptop.binding !== undefined &&
            plan.laptop.binding.operationId !== binding.operationId &&
            plan.laptop.operations.some(
              (entry) =>
                entry.operationId === plan.laptop.binding!.operationId && localPending(entry),
            ))
        )
          throw new Error("The native install does not match its plan review.");
        return plan.laptop.binding?.operationId === binding.operationId
          ? plan
          : {
              ...plan,
              revision: plan.revision + 1,
              laptop: { ...plan.laptop, binding: { ...binding, boundAt: now() } },
            };
      }, true);
    },
    recordLaptopOutcome: async (outcome: LaptopNativeOutcome): Promise<void> => {
      await nativeChange((plan) => {
        const binding = plan.laptop.binding;
        if (binding?.transactionId !== outcome.transactionId) return plan;
        const committed =
          outcome.status === "committed" &&
          outcome.activeGeneration === binding.transactionId &&
          outcome.activeGeneration !== binding.fromGeneration &&
          outcome.activeSource === binding.targetSource &&
          outcome.activeTree === binding.targetTree;
        const rolledBack =
          outcome.status === "rolled-back" && outcome.activeGeneration === binding.fromGeneration;
        const status = committed ? "committed" : rolledBack ? "rolled-back" : "held";
        const operation = plan.laptop.operations.find(
          (entry) => entry.operationId === binding.operationId,
        );
        if (operation?.result?.status === status) return plan;
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
        return outcome.status === "rolled-back" &&
          next.laptop.gate !== undefined &&
          next.laptop.gateClosed === undefined
          ? {
              ...next,
              laptop: { ...next.laptop, gateClosed: { at: now(), reason: "rolled-back" } },
            }
          : next;
      });
    },
    withUnmanagedTarget: async <A>(sourceSha: string, work: () => Promise<A>): Promise<A> =>
      transact(async (snapshot) => {
        if (
          snapshot.readOnly !== undefined ||
          snapshot.dispositions.targets.some((target) => target.targetSource === sourceSha)
        )
          throw new Error("This build is managed by the update plan.");
        return { ...snapshot, result: await work() };
      }),
    preparationHeld: async (): Promise<boolean> =>
      transact(async (snapshot) => ({
        ...snapshot,
        result:
          snapshot.readOnly !== undefined ||
          snapshot.document.current?.laptop.operations.some(
            (operation) => operation.result?.status === "held",
          ) === true,
      })),
    isPlanManaged: async (sourceSha: string): Promise<boolean> =>
      transact(async (snapshot) => ({
        ...snapshot,
        result:
          snapshot.readOnly !== undefined ||
          snapshot.dispositions.targets.some((target) => target.targetSource === sourceSha),
      })),
    backgroundSuppressed: async (sourceSha: string): Promise<boolean> =>
      transact(async (snapshot) => ({
        ...snapshot,
        result:
          snapshot.readOnly !== undefined ||
          snapshot.dispositions.targets.some(
            (target) => target.targetSource === sourceSha && target.laptopStopped === true,
          ) ||
          (snapshot.document.current !== null &&
            (snapshot.document.current.laptop.operations.some(localPending) ||
              options.controller.isStaged({
                targetSource: snapshot.document.current.target.sourceSha,
                targetTree: snapshot.document.current.target.sourceTree,
              }))),
      })),
  };
}
