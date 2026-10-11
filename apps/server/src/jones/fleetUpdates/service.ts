import type { JonesUpdateInstallInput, JonesUpdateState } from "@t3tools/contracts";
import {
  FleetHostError,
  type FleetActivateInput, type FleetEnrollment, type FleetEnrollmentInput,
  type FleetHostOperation, type FleetHostStatus, type FleetStageInput,
} from "@t3tools/contracts/jones/fleet-updates";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Semaphore from "effect/Semaphore";
import { ServerEnvironment } from "../../environment/ServerEnvironment.ts";
import { ServerConfig } from "../../config.ts";
import { JonesUpdates } from "../updates/service.ts";
import { createFleetHostStore, type FleetHostStore } from "./store.ts";

type NativeBinding = {
  readonly environmentId: string;
  readonly currentVersion: string;
  readonly expectedInstalledSource: string;
  readonly targetSource: string;
  readonly stagedHandle: string;
};
export interface FleetHostUpdater {
  readonly fleetOperationsSupported: boolean;
  readonly state: () => Effect.Effect<JonesUpdateState | null>;
  readonly stageExact: (input: { readonly targetSource: string; readonly operationId: string }) => Effect.Effect<JonesUpdateState>;
  readonly installForOperation: (input: JonesUpdateInstallInput & {
    readonly operationId: string; readonly targetSource: string; readonly expectedInstalledSource: string;
  }) => Effect.Effect<JonesUpdateState>;
  readonly reconcileOperation: (operationId: string) => Effect.Effect<{
    readonly state: "absent" | "pending" | "committed" | "rolled-back" | "blocked";
    readonly operationId: string;
    readonly binding?: NativeBinding;
    readonly updateId?: string;
    readonly reason?: string;
  }>;
}

export class FleetUpdates extends Context.Service<FleetUpdates, {
  readonly status: (operationId?: string) => Effect.Effect<FleetHostStatus, FleetHostError>;
  readonly enroll: (input: FleetEnrollmentInput) => Effect.Effect<FleetHostStatus, FleetHostError>;
  readonly stage: (input: FleetStageInput) => Effect.Effect<FleetHostStatus, FleetHostError>;
  readonly activate: (input: FleetActivateInput) => Effect.Effect<FleetHostStatus, FleetHostError>;
}>()("t3/jones/fleetUpdates/FleetUpdates") {}

const fail = (reason: FleetHostError["reason"], message: string) => new FleetHostError({ reason, message });
const storage = <T>(f: () => Promise<T>) => Effect.tryPromise({
  try: f,
  catch: (cause) => cause instanceof FleetHostError ? cause : fail("storage", "Fleet receipt storage is unavailable; operation acceptance requires reconciliation."),
});
const sameIntent = (a: FleetStageInput, b: FleetStageInput) =>
  a.operationId === b.operationId && a.enrollmentId === b.enrollmentId &&
  a.environmentId === b.environmentId && a.targetSource === b.targetSource &&
  a.expectedInstalledSource === b.expectedInstalledSource;
const sameEnrollment = (a: FleetEnrollment, b: FleetEnrollment) =>
  a.enrollmentId === b.enrollmentId && a.environmentId === b.environmentId &&
  a.enabled === b.enabled && a.continueRunningThreads === b.continueRunningThreads;
const nativeMatches = (operation: FleetHostOperation, binding: NativeBinding | undefined) =>
  binding !== undefined && binding.environmentId === operation.input.environmentId &&
  binding.currentVersion === operation.currentVersion &&
  binding.expectedInstalledSource === operation.input.expectedInstalledSource &&
  binding.targetSource === operation.input.targetSource && binding.stagedHandle === operation.stagedHandle;

export const makeFleetUpdates = (options: {
  readonly environmentId: FleetHostStatus["environmentId"];
  readonly serviceMode: boolean;
  readonly store: FleetHostStore;
  readonly updates: FleetHostUpdater;
}) => Effect.gen(function* () {
  const { environmentId, store, updates } = options;
  const gate = yield* Semaphore.make(1);
  const requireEnrollment = (input: { environmentId: string; enrollmentId: string }) => Effect.gen(function* () {
    if (input.environmentId !== environmentId) return yield* fail("binding", "The connected environment changed.");
    const enrollment = yield* storage(store.readEnrollment);
    if (enrollment === null || !enrollment.enabled || enrollment.environmentId !== environmentId ||
        enrollment.enrollmentId !== input.enrollmentId) {
      return yield* fail("not-enrolled", "This service has not accepted the selected fleet enrollment.");
    }
    return enrollment;
  });
  const save = (operation: FleetHostOperation) => storage(() => store.editOperation(operation.input.operationId, (old) => {
    if (old !== null && !sameIntent(old.input, operation.input)) throw fail("conflict", "The operation identifier already belongs to another update.");
    if (old !== null && (["current", "committed", "rolled-back", "blocked"].includes(old.phase) ||
        (old.phase === "pending" && ["staging", "stage-blocked", "staged", "dispatching"].includes(operation.phase)) ||
        (old.phase === "dispatching" && ["staging", "stage-blocked", "staged"].includes(operation.phase)))) return old;
    return operation;
  }));
  const reconcile = (operation: FleetHostOperation) => Effect.gen(function* () {
    if (operation.phase !== "dispatching" && operation.phase !== "pending") return operation;
    const native = yield* updates.reconcileOperation(operation.input.operationId);
    if (native.state === "absent" && operation.phase === "dispatching") return operation;
    if (native.operationId !== operation.input.operationId || !nativeMatches(operation, native.binding)) {
      return yield* save({ ...operation, phase: "blocked", reason: "Native operation identity is missing or changed; automatic dispatch is stopped." });
    }
    return yield* save({
      ...operation,
      phase: native.state === "absent" ? "blocked" : native.state,
      ...(native.updateId === undefined ? {} : { updateId: native.updateId }),
      ...(native.reason === undefined ? {} : { reason: native.reason }),
    });
  });
  const status = (operationId?: string) => Effect.gen(function* () {
    const enrollment = yield* storage(store.readEnrollment);
    const operation = operationId === undefined ? null : yield* storage(() => store.readOperation(operationId));
    return {
      operationProtocol: options.serviceMode && updates.fleetOperationsSupported ? 1 as const : null,
      environmentId,
      enrollment,
      operation: operation === null ? null : yield* reconcile(operation),
      update: yield* updates.state(),
    } satisfies FleetHostStatus;
  });
  const enroll = (input: FleetEnrollmentInput) => Effect.gen(function* () {
    if (input.enrollment.environmentId !== environmentId) return yield* fail("binding", "The connected environment changed.");
    const update = yield* updates.state();
    if (input.enrollment.enabled && (!options.serviceMode || !updates.fleetOperationsSupported || !update?.capability.install ||
        !update.capability.download || update.environmentId !== environmentId || update.installedSource === undefined)) {
      return yield* fail("bootstrap-required", "This host needs explicit source-qualified service setup before enrollment.");
    }
    yield* storage(() => store.editEnrollment((old) => {
      if (old !== null && sameEnrollment(old, input.enrollment)) return old;
      if ((old?.enrollmentId ?? null) !== input.expectedEnrollmentId) throw fail("conflict", "Fleet enrollment changed; refresh before changing it.");
      return input.enrollment;
    }));
    return yield* status();
  });
  const stage = (input: FleetStageInput) => Effect.gen(function* () {
    const enrollment = yield* requireEnrollment(input);
    const old = yield* storage(() => store.readOperation(input.operationId));
    if (old !== null && !sameIntent(old.input, input)) return yield* fail("conflict", "The operation identifier already belongs to another update.");
    if (old !== null && old.phase !== "staging" && old.phase !== "stage-blocked") return yield* status(input.operationId);
    const update = yield* updates.state();
    if (!options.serviceMode || !updates.fleetOperationsSupported || update?.environmentId !== environmentId || !update.currentVersion ||
        update.installedSource !== input.expectedInstalledSource) {
      return yield* fail("binding", "Installed source or service identity changed before staging.");
    }
    if (!update.capability.download || !update.capability.install) return yield* fail("bootstrap-required", "The native launcher is not qualified for automatic updates.");
    let operation = yield* save({
      input, currentVersion: update.currentVersion, continueRunningThreads: enrollment.continueRunningThreads,
      phase: input.targetSource === input.expectedInstalledSource ? "current" : "staging",
    });
    if (operation.phase !== "staging") return yield* status(input.operationId);
    const staged = yield* updates.stageExact({ targetSource: input.targetSource, operationId: input.operationId });
    operation = yield* save(staged.phase === "staged" && staged.stagedHandle !== undefined &&
      staged.provenance?.sourceSha === input.targetSource && staged.environmentId === environmentId &&
      staged.currentVersion === operation.currentVersion ? {
        ...operation, phase: "staged", stagedHandle: staged.stagedHandle,
      } : {
        ...operation, phase: "stage-blocked", reason: staged.message ?? "The exact selected source could not be staged.",
      });
    return yield* status(operation.input.operationId);
  });
  const activate = (input: FleetActivateInput) => Effect.gen(function* () {
    const enrollment = yield* requireEnrollment(input);
    let operation = yield* storage(() => store.readOperation(input.operationId));
    if (operation === null || operation.input.enrollmentId !== input.enrollmentId ||
        operation.input.environmentId !== input.environmentId) return yield* fail("binding", "The selected staged operation does not exist in this enrollment.");
    operation = yield* reconcile(operation);
    if (operation.phase !== "staged" && operation.phase !== "dispatching") return yield* status(input.operationId);
    if (enrollment.continueRunningThreads !== operation.continueRunningThreads) return yield* fail("conflict", "Continuation consent changed after staging; stage a new operation.");
    if (operation.stagedHandle === undefined) return yield* fail("binding", "The selected operation has no staged artifact.");
    // This durable marker precedes IPC. Replaying it requires authoritative native absence;
    // the launcher's UUID binding serializes a late or duplicate request.
    operation = yield* save({ ...operation, phase: "dispatching" });
    if (operation.phase !== "dispatching") return yield* status(input.operationId);
    yield* updates.installForOperation({
      operationId: input.operationId, environmentId, currentVersion: operation.currentVersion,
      stagedHandle: operation.stagedHandle!, targetSource: operation.input.targetSource,
      expectedInstalledSource: operation.input.expectedInstalledSource,
      continueRunningThreads: operation.continueRunningThreads,
    });
    return yield* status(input.operationId);
  });
  return FleetUpdates.of({
    status,
    enroll: (input) => gate.withPermits(1)(enroll(input)),
    stage: (input) => gate.withPermits(1)(stage(input)),
    activate: (input) => gate.withPermits(1)(activate(input)),
  });
});

export const layer = Layer.effect(FleetUpdates, Effect.gen(function* () {
  const config = yield* ServerConfig;
  const environment = yield* ServerEnvironment;
  const updates = yield* JonesUpdates;
  return yield* makeFleetUpdates({
    environmentId: yield* environment.getEnvironmentId,
    serviceMode: config.mode !== "desktop", store: createFleetHostStore(config.baseDir), updates,
  });
}));
