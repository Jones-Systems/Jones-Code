// @effect-diagnostics nodeBuiltinImport:off
// A follower mints UUID v4 operation identifiers for its own stages, like the laptop does.
import type {
  JonesUpdateInstallInput,
  JonesUpdatePolicy,
  JonesUpdateState,
} from "@t3tools/contracts";
import {
  FleetHostError,
  type FleetActivateInput,
  type FleetEnrollment,
  type FleetEnrollmentInput,
  type FleetFollowerStatus,
  type FleetHostOperation,
  type FleetHostStatus,
  type FleetLeaderInput,
  type FleetStageInput,
  type FleetRetireInput,
} from "@t3tools/contracts/jones/fleet-updates";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as NodeCrypto from "node:crypto";
import { ServerEnvironment } from "../../environment/ServerEnvironment.ts";
import { ServerConfig } from "../../config.ts";
import { forkParked } from "../../serverActivation.ts";
import { JonesUpdates } from "../updates/service.ts";
import { JonesSourceRelation } from "../updates/sourceRelation.ts";
import {
  QUIET_INPUT_IDLE_MS,
  automaticActivationDecision,
  type AutomaticActivationDecision,
} from "../updates/followerPolicy.ts";
import { makeFleetQuietSignals, type FleetQuietSignals } from "./quietSignals.ts";
import { createFleetHostStore, type FleetFollowerRecord, type FleetHostStore } from "./store.ts";

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
  readonly stageExact: (input: {
    readonly targetSource: string;
    readonly operationId: string;
  }) => Effect.Effect<JonesUpdateState>;
  readonly installForOperation: (
    input: JonesUpdateInstallInput & {
      readonly operationId: string;
      readonly targetSource: string;
      readonly expectedInstalledSource: string;
      readonly admit?: () => Promise<string | undefined>;
    },
  ) => Effect.Effect<JonesUpdateState>;
  readonly retireStagedOperation: (
    input: NativeBinding & { readonly operationId: string },
  ) => Effect.Effect<{ readonly retired: boolean; readonly reason?: string }>;
  readonly reconcileOperation: (operationId: string) => Effect.Effect<{
    readonly state: "absent" | "pending" | "committed" | "rolled-back" | "rejected" | "blocked";
    readonly operationId: string;
    readonly binding?: NativeBinding;
    readonly updateId?: string;
    readonly reason?: string;
  }>;
  readonly refreshPolicy: Effect.Effect<JonesUpdatePolicy | undefined>;
}

export class FleetUpdates extends Context.Service<
  FleetUpdates,
  {
    readonly status: (operationId?: string) => Effect.Effect<FleetHostStatus, FleetHostError>;
    readonly enroll: (
      input: FleetEnrollmentInput,
    ) => Effect.Effect<FleetHostStatus, FleetHostError>;
    readonly stage: (input: FleetStageInput) => Effect.Effect<FleetHostStatus, FleetHostError>;
    readonly retire: (input: FleetRetireInput) => Effect.Effect<FleetHostStatus, FleetHostError>;
    readonly activate: (
      input: FleetActivateInput,
    ) => Effect.Effect<FleetHostStatus, FleetHostError>;
    /** Records the laptop's committed source; a following host prepares and installs it itself. */
    readonly leader: (input: FleetLeaderInput) => Effect.Effect<FleetHostStatus, FleetHostError>;
    /** One pass of the following host's own updater; the layer runs it every minute. */
    readonly follow: Effect.Effect<void>;
  }
>()("t3/jones/fleetUpdates/service/FleetUpdates") {}

/** A failed preparation is retried after this long, unless the laptop's source changes. */
export const FOLLOWER_STAGE_RETRY_MS = 15 * 60_000;
const FOLLOWER_PASS_INTERVAL = "60 seconds";

const fail = (reason: FleetHostError["reason"], message: string) =>
  new FleetHostError({ reason, message });
const storage = <T>(f: () => Promise<T>) =>
  Effect.tryPromise({
    try: f,
    catch: (cause) =>
      Schema.is(FleetHostError)(cause)
        ? cause
        : fail(
            "storage",
            "Fleet receipt storage is unavailable; operation acceptance requires reconciliation.",
          ),
  });
const sameIntent = (a: FleetStageInput, b: FleetStageInput) =>
  a.operationId === b.operationId &&
  a.enrollmentId === b.enrollmentId &&
  a.environmentId === b.environmentId &&
  a.targetSource === b.targetSource &&
  a.expectedInstalledSource === b.expectedInstalledSource;
const sameEnrollment = (a: FleetEnrollment, b: FleetEnrollment) =>
  a.enrollmentId === b.enrollmentId &&
  a.environmentId === b.environmentId &&
  a.enabled === b.enabled &&
  a.continueRunningThreads === b.continueRunningThreads;
const nativeMatches = (operation: FleetHostOperation, binding: NativeBinding | undefined) =>
  binding !== undefined &&
  binding.environmentId === operation.input.environmentId &&
  binding.currentVersion === operation.currentVersion &&
  binding.expectedInstalledSource === operation.input.expectedInstalledSource &&
  binding.targetSource === operation.input.targetSource &&
  binding.stagedHandle === operation.stagedHandle;
const emptyFollower: FleetFollowerRecord = {
  schema: 1,
  leader: null,
  operation: null,
  lastAutomaticActivationAt: null,
};
const epoch = (iso: string | null | undefined) =>
  iso === null || iso === undefined
    ? null
    : Option.match(DateTime.make(iso), {
        onNone: () => null,
        onSome: DateTime.toEpochMillis,
      });
const isoAt = (millis: number) => DateTime.formatIso(DateTime.makeUnsafe(millis));
/** The laptop's report for `enrollment`; one recorded without its tree is not provenance. */
const leaderFor = (record: FleetFollowerRecord | null, enrollment: FleetEnrollment) =>
  record?.leader?.enrollmentId === enrollment.enrollmentId &&
  record.leader.committedTree !== undefined
    ? record.leader
    : null;

export const makeFleetUpdates = Effect.fn("FleetUpdates.makeFleetUpdates")(function* (options: {
  readonly environmentId: FleetHostStatus["environmentId"];
  readonly serviceMode: boolean;
  readonly store: FleetHostStore;
  readonly updates: FleetHostUpdater;
  readonly quiet: FleetQuietSignals;
  /** Ancestry of two commits; without it the staging check alone refuses a downgrade. */
  readonly relation?: JonesSourceRelation["Service"]["relation"];
  readonly operationId?: () => string;
  /** Offered when the follower should run its next pass now. */
  readonly wakeups?: Queue.Queue<void>;
}) {
  const { environmentId, store, updates, quiet } = options;
  const newOperationId = options.operationId ?? (() => NodeCrypto.randomUUID());
  const gate = yield* Semaphore.make(1);
  const wakeups = options.wakeups ?? (yield* Queue.sliding<void>(1));
  const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());
  const activeNative = new Set<string>();
  /** The follower's latest evaluation; status reads never repeat its work. */
  let followerView: FleetFollowerStatus | undefined;
  const stageAttempts = new Map<string, number>();
  /** One retirement identity per foreign stage, so a retry repeats the same launcher binding. */
  const retirements = new Map<string, { readonly operationId: string; readonly at: number }>();
  const requireEnrollment = Effect.fn("FleetUpdates.requireEnrollment")(function* (input: {
    environmentId: string;
    enrollmentId: string;
  }) {
    if (input.environmentId !== environmentId)
      return yield* fail("binding", "The connected environment changed.");
    const enrollment = yield* storage(store.readEnrollment);
    if (
      enrollment === null ||
      !enrollment.enabled ||
      enrollment.environmentId !== environmentId ||
      enrollment.enrollmentId !== input.enrollmentId
    ) {
      return yield* fail(
        "not-enrolled",
        "This service has not accepted the selected fleet enrollment.",
      );
    }
    return enrollment;
  });
  const save = (operation: FleetHostOperation) =>
    storage(() =>
      store.editOperation(operation.input.operationId, (old) => {
        if (old !== null && !sameIntent(old.input, operation.input))
          throw fail("conflict", "The operation identifier already belongs to another update.");
        if (
          old !== null &&
          (["current", "committed", "rolled-back", "blocked", "superseded"].includes(old.phase) ||
            (["pending", "reconciling"].includes(old.phase) &&
              ["staging", "stage-blocked", "staged", "dispatching", "install-blocked"].includes(
                operation.phase,
              )) ||
            (["dispatching", "install-blocked"].includes(old.phase) &&
              ["staging", "stage-blocked", "staged"].includes(operation.phase)))
        )
          return old;
        return operation;
      }),
    );
  const reconcile = Effect.fn("FleetUpdates.reconcile")(function* (operation: FleetHostOperation) {
    if (
      !["dispatching", "install-blocked", "pending", "reconciling"].includes(operation.phase) ||
      activeNative.has(operation.input.operationId)
    )
      return operation;
    const native = yield* updates.reconcileOperation(operation.input.operationId);
    if (native.state === "absent" && ["dispatching", "install-blocked"].includes(operation.phase))
      return operation;
    const bound =
      native.operationId === operation.input.operationId &&
      nativeMatches(operation, native.binding);
    const provenFailure =
      native.state === "blocked" && bound && native.updateId === operation.input.operationId;
    if (!bound || native.state === "absent" || (native.state === "blocked" && !provenFailure)) {
      return yield* save({
        ...operation,
        phase: "reconciling",
        reason:
          native.reason ??
          "Native acceptance is uncertain. Status will be observed without repeating installation.",
      });
    }
    const { reason: _reason, ...previous } = operation;
    // The launcher proved this operation can never start; a new one may be staged.
    if (native.state === "rejected")
      return yield* save({
        ...previous,
        phase: "blocked",
        reason: native.reason ?? "The launcher refused this operation before accepting it.",
      });
    return yield* save({
      ...previous,
      phase: native.state,
      ...(native.updateId === undefined ? {} : { updateId: native.updateId }),
      ...(native.reason === undefined ? {} : { reason: native.reason }),
    });
  });
  /** The current follower view, or a first one built from the durable record alone. */
  const followerStatus = Effect.fn("FleetUpdates.followerStatus")(function* (
    enrollment: FleetEnrollment | null,
  ) {
    if (
      !options.serviceMode ||
      enrollment === null ||
      !enrollment.enabled ||
      enrollment.environmentId !== environmentId
    )
      return undefined;
    if (followerView !== undefined) return followerView;
    const leaderSource =
      leaderFor(yield* storage(store.readFollower), enrollment)?.committedSource ?? null;
    return {
      protocol: 1,
      leaderSource,
      state: leaderSource === null ? "waiting-for-leader" : "preparing",
      reason:
        leaderSource === null
          ? "Waiting for the laptop to report its installed build."
          : "Checking the laptop's installed build.",
    } satisfies FleetFollowerStatus;
  });
  const status = Effect.fn("FleetUpdates.status")(function* (operationId?: string) {
    const enrollment = yield* storage(store.readEnrollment);
    const operation =
      operationId === undefined ? null : yield* storage(() => store.readOperation(operationId));
    const follower = yield* followerStatus(enrollment);
    return {
      operationProtocol:
        options.serviceMode && updates.fleetOperationsSupported ? (1 as const) : null,
      environmentId,
      enrollment,
      operation: operation === null ? null : yield* reconcile(operation),
      update: yield* updates.state(),
      ...(follower === undefined ? {} : { follower }),
    } satisfies FleetHostStatus;
  });
  const enroll = Effect.fn("FleetUpdates.enroll")(function* (input: FleetEnrollmentInput) {
    if (input.enrollment.environmentId !== environmentId)
      return yield* fail("binding", "The connected environment changed.");
    const update = yield* updates.state();
    if (
      input.enrollment.enabled &&
      (!options.serviceMode ||
        !updates.fleetOperationsSupported ||
        !update?.capability.install ||
        !update.capability.download ||
        update.environmentId !== environmentId ||
        update.installedSource === undefined)
    ) {
      return yield* fail(
        "bootstrap-required",
        "This host needs explicit source-qualified service setup before enrollment.",
      );
    }
    const saved = yield* storage(() =>
      store.editEnrollment((old) => {
        if (old !== null && sameEnrollment(old, input.enrollment)) return old;
        if ((old?.enrollmentId ?? null) !== input.expectedEnrollmentId)
          throw fail("conflict", "Fleet enrollment changed; refresh before changing it.");
        return input.enrollment;
      }),
    );
    if (followerView !== undefined && saved.enrollmentId !== input.expectedEnrollmentId)
      followerView = undefined;
    yield* updates.refreshPolicy;
    yield* Queue.offer(wakeups, undefined);
    return yield* status();
  });
  const stage = Effect.fn("FleetUpdates.stage")(function* (input: FleetStageInput) {
    const enrollment = yield* requireEnrollment(input);
    const old = yield* storage(() => store.readOperation(input.operationId));
    if (old !== null && !sameIntent(old.input, input))
      return yield* fail("conflict", "The operation identifier already belongs to another update.");
    if (old !== null && old.phase !== "staging" && old.phase !== "stage-blocked")
      return yield* status(input.operationId);
    const update = yield* updates.state();
    if (
      !options.serviceMode ||
      !updates.fleetOperationsSupported ||
      update?.environmentId !== environmentId ||
      !update.currentVersion ||
      update.installedSource?.sha !== input.expectedInstalledSource
    ) {
      return yield* fail("binding", "Installed source or service identity changed before staging.");
    }
    if (!update.capability.download || !update.capability.install)
      return yield* fail(
        "bootstrap-required",
        "The native launcher is not qualified for automatic updates.",
      );
    let operation = yield* save({
      input,
      currentVersion: update.currentVersion,
      continueRunningThreads: enrollment.continueRunningThreads,
      phase: input.targetSource === input.expectedInstalledSource ? "current" : "staging",
    });
    if (operation.phase !== "staging") return yield* status(input.operationId);
    const staged = yield* updates.stageExact({
      targetSource: input.targetSource,
      operationId: input.operationId,
    });
    operation = yield* save(
      staged.phase === "staged" &&
        staged.stagedHandle !== undefined &&
        staged.provenance?.sourceSha === input.targetSource &&
        staged.environmentId === environmentId &&
        staged.currentVersion === operation.currentVersion
        ? {
            ...operation,
            phase: "staged",
            stagedHandle: staged.stagedHandle,
          }
        : {
            ...operation,
            phase: "stage-blocked",
            reason: staged.message ?? "The exact selected source could not be staged.",
          },
    );
    return yield* status(operation.input.operationId);
  });
  const retire = Effect.fn("FleetUpdates.retire")(function* (input: FleetRetireInput) {
    if (input.environmentId !== environmentId)
      return yield* fail("binding", "The connected environment changed.");
    let operation = yield* storage(() => store.readOperation(input.operationId));
    if (operation !== null && !sameIntent(operation.input, input))
      return yield* fail("conflict", "The retired operation binding changed.");
    if (operation !== null && !["staging", "stage-blocked", "staged"].includes(operation.phase))
      return yield* status(input.operationId);
    const native = yield* updates.reconcileOperation(input.operationId);
    if (native.state !== "absent") {
      if (operation === null)
        return yield* fail(
          "conflict",
          "A native operation exists without its fleet stage receipt; it was preserved.",
        );
      yield* save({
        ...operation,
        phase: "reconciling",
        reason: "Native acceptance must be reconciled before this stage can be retired.",
      });
      return yield* status(input.operationId);
    }
    const update = yield* updates.state();
    if (operation === null) {
      if (!update?.currentVersion)
        return yield* fail("bootstrap-required", "The host cannot bind a retirement receipt.");
      operation = {
        input,
        phase: "staging",
        currentVersion: update.currentVersion,
        continueRunningThreads: false,
      };
    }
    const handle = operation.stagedHandle;
    if (handle !== undefined) {
      const result = yield* updates.retireStagedOperation({
        ...operation.input,
        currentVersion: operation.currentVersion,
        stagedHandle: handle,
      });
      if (!result.retired) {
        yield* save({
          ...operation,
          reason: result.reason ?? "The exact staged selection could not be retired safely.",
        });
        return yield* status(input.operationId);
      }
    } else if (
      update?.stagedHandle !== undefined &&
      update.provenance?.sourceSha === input.targetSource
    ) {
      // A crash after native staging may precede the fleet staged receipt. Recover the
      // exact current slot only when its source/environment/version still match.
      if (
        update.environmentId !== environmentId ||
        update.currentVersion !== operation.currentVersion ||
        update.installedSource?.sha !== input.expectedInstalledSource
      )
        return yield* fail("binding", "The stage identity changed before retirement.");
      const result = yield* updates.retireStagedOperation({
        ...input,
        currentVersion: operation.currentVersion,
        stagedHandle: update.stagedHandle,
      });
      if (!result.retired)
        return yield* fail("conflict", result.reason ?? "The stage could not be retired safely.");
    }
    yield* save({
      ...operation,
      phase: "superseded",
      reason: "The unaccepted stage pointer was retired. Its payload was retained.",
    });
    return yield* status(input.operationId);
  });
  /**
   * Whether an automatic activation may start. The durable work snapshot is read only once the
   * cheap signals allow it. Admission just before the handoff ignores spacing, which the
   * activation itself has just recorded.
   */
  const quietDecision = Effect.fn("FleetUpdates.quietDecision")(function* (
    lastAutomaticActivationAt: number | null,
  ) {
    const base = {
      now: yield* Clock.currentTimeMillis,
      lastInputAt: yield* quiet.lastInputAt,
      runningTerminals: yield* quiet.runningTerminals,
      lastAutomaticActivationAt,
    };
    const cheap = automaticActivationDecision(base);
    if (cheap.ready || cheap.blockers.length > 0 || cheap.notBefore !== undefined) return cheap;
    return automaticActivationDecision({ ...base, work: yield* quiet.work });
  });
  const admit = () =>
    runPromise(
      quietDecision(null).pipe(
        Effect.map((decision) => (decision.ready ? undefined : decision.reason)),
      ),
    );
  /** Holds an automatic activation until a quiet window, or records it before dispatch. */
  const claimAutomaticActivation = Effect.gen(function* () {
    const record = yield* storage(store.readFollower);
    const decision = yield* quietDecision(epoch(record?.lastAutomaticActivationAt));
    if (!decision.ready) return decision;
    const now = isoAt(yield* Clock.currentTimeMillis);
    yield* storage(() =>
      store.editFollower((old) => ({
        ...(old ?? emptyFollower),
        lastAutomaticActivationAt: now,
      })),
    );
    return decision;
  });
  /** Dispatches one staged operation. The caller has already admitted it. */
  const dispatch = Effect.fn("FleetUpdates.dispatch")(function* (
    selectedOperation: FleetHostOperation,
  ) {
    const operationId = selectedOperation.input.operationId;
    let operation = yield* save({ ...selectedOperation, phase: "dispatching" });
    if (operation.phase !== "dispatching") return;
    const selected = operation;
    // Status reads remain available while preparation runs, but cannot turn the
    // reservation-before-state interval of this invocation into a terminal result.
    activeNative.add(operationId);
    const result = yield* updates
      .installForOperation({
        operationId,
        environmentId,
        currentVersion: selected.currentVersion,
        stagedHandle: selected.stagedHandle!,
        targetSource: selected.input.targetSource,
        expectedInstalledSource: selected.input.expectedInstalledSource,
        continueRunningThreads: selected.continueRunningThreads,
        admit,
      })
      .pipe(Effect.ensuring(Effect.sync(() => activeNative.delete(operationId))));
    operation = yield* reconcile(selected);
    // A withdrawn activation leaves the stage ready, like a refusal before acceptance.
    if (
      operation.phase === "dispatching" &&
      ["blocked", "error", "staged"].includes(result.phase)
    ) {
      const current = yield* updates.state();
      const bindingStillValid =
        current?.environmentId === environmentId &&
        current.currentVersion === selected.currentVersion &&
        current.installedSource?.sha === selected.input.expectedInstalledSource &&
        current.stagedHandle === selected.stagedHandle &&
        current.provenance?.sourceSha === selected.input.targetSource;
      yield* save({
        ...operation,
        phase: bindingStillValid ? "install-blocked" : "blocked",
        reason:
          result.message ??
          "The launcher refused preparation before native acceptance. Refresh the selected host before retrying.",
      });
    }
  });
  const activate = Effect.fn("FleetUpdates.activate")(function* (input: FleetActivateInput) {
    const enrollment = yield* requireEnrollment(input);
    let operation = yield* storage(() => store.readOperation(input.operationId));
    if (
      operation === null ||
      operation.input.enrollmentId !== input.enrollmentId ||
      operation.input.environmentId !== input.environmentId
    )
      return yield* fail(
        "binding",
        "The selected staged operation does not exist in this enrollment.",
      );
    operation = yield* reconcile(operation);
    if (!["staged", "dispatching", "install-blocked"].includes(operation.phase))
      return yield* status(input.operationId);
    if (enrollment.continueRunningThreads !== operation.continueRunningThreads)
      return yield* fail(
        "conflict",
        "Continuation consent changed after staging; stage a new operation.",
      );
    if (operation.stagedHandle === undefined)
      return yield* fail("binding", "The selected operation has no staged artifact.");
    // The laptop's campaigns activate without a person at this host, so they wait for the
    // same quiet window as the host's own automatic updates.
    const decision = yield* claimAutomaticActivation;
    if (!decision.ready) {
      yield* save({ ...operation, phase: "install-blocked", reason: decision.reason });
      return yield* status(input.operationId);
    }
    yield* dispatch(operation);
    return yield* status(input.operationId);
  });
  const leader = Effect.fn("FleetUpdates.leader")(function* (input: FleetLeaderInput) {
    yield* requireEnrollment(input);
    if (!options.serviceMode)
      return yield* fail("binding", "Only a service host follows a laptop.");
    const current = yield* storage(store.readFollower);
    if (
      current?.leader?.enrollmentId !== input.enrollmentId ||
      current.leader.committedSource !== input.committedSource ||
      current.leader.committedTree !== input.committedTree
    ) {
      const publishedAt = isoAt(yield* Clock.currentTimeMillis);
      yield* storage(() =>
        store.editFollower((old) => ({
          ...(old ?? emptyFollower),
          leader: {
            enrollmentId: input.enrollmentId,
            committedSource: input.committedSource,
            committedTree: input.committedTree,
            publishedAt,
          },
        })),
      );
      followerView = undefined;
      yield* updates.refreshPolicy;
      yield* Queue.offer(wakeups, undefined);
    }
    return yield* status();
  });

  const quietView = (decision: AutomaticActivationDecision, now: number) => ({
    inputIdleMs: decision.inputIdleMs,
    requiredInputIdleMs: QUIET_INPUT_IDLE_MS,
    blockers: decision.blockers,
    evaluatedAt: isoAt(now),
  });
  /**
   * One pass of a following host: catch up to exactly the laptop's committed source. A different
   * unaccepted stage is retired first (latest wins). Preparation is automatic; activation waits
   * for a quiet window and keeps automatic activations apart.
   */
  const followOnce = Effect.gen(function* () {
    const enrollment = yield* storage(store.readEnrollment);
    if (
      !options.serviceMode ||
      enrollment === null ||
      !enrollment.enabled ||
      enrollment.environmentId !== environmentId
    ) {
      followerView = undefined;
      return;
    }
    yield* updates.refreshPolicy;
    const record = (yield* storage(store.readFollower)) ?? emptyFollower;
    const leaderRecord = leaderFor(record, enrollment);
    const base = {
      protocol: 1 as const,
      leaderSource: leaderRecord?.committedSource ?? null,
      ...(leaderRecord === null ? {} : { leaderPublishedAt: leaderRecord.publishedAt }),
    };
    const show = (view: Omit<FleetFollowerStatus, keyof typeof base>) => {
      followerView = { ...base, ...view };
    };
    if (!updates.fleetOperationsSupported)
      return show({
        state: "unavailable",
        reason: "This host's launcher cannot run automatic updates.",
      });
    if (leaderRecord === null)
      return show({
        state: "waiting-for-leader",
        reason: "Waiting for the laptop to report its installed build.",
      });
    const target = leaderRecord.committedSource;
    const update = yield* updates.state();
    const installed = update?.installedSource?.sha;
    if (
      update === null ||
      update.environmentId !== environmentId ||
      update.currentVersion === undefined ||
      installed === undefined
    )
      return show({ state: "unavailable", reason: "This host's installed build is unknown." });

    const own = record.operation;
    let operation =
      own === null ? null : yield* storage(() => store.readOperation(own.operationId));
    if (operation !== null) operation = yield* reconcile(operation);
    if (operation !== null && ["dispatching", "pending"].includes(operation.phase))
      return show({
        state: "activating",
        operationId: operation.input.operationId,
        reason: "Installing the laptop's build.",
      });
    if (operation?.phase === "reconciling")
      return show({
        state: "reconciling",
        operationId: operation.input.operationId,
        reason: operation.reason ?? "The last automatic update needs reconciliation.",
      });
    if (installed === target) return show({ state: "current" });
    if (update.phase === "preparing" || update.phase === "installing")
      return show({ state: "activating", reason: "An installation is already running." });
    // A host installed ahead of the laptop never goes back. Without a proven relation the
    // staging check refuses a target that does not descend from the installed source.
    if (
      options.relation !== undefined &&
      (yield* options.relation({ base: target, head: installed })).relation === "ahead"
    )
      return show({
        state: "waiting-for-leader",
        reason:
          "This host already runs a newer build than the laptop's and does not go back. It updates once the laptop reports a newer build.",
      });

    const intended =
      operation !== null &&
      operation.input.enrollmentId === enrollment.enrollmentId &&
      operation.input.targetSource === target &&
      operation.input.expectedInstalledSource === installed &&
      operation.continueRunningThreads === enrollment.continueRunningThreads &&
      !["blocked", "superseded"].includes(operation.phase)
        ? operation
        : null;
    if (intended !== null && ["rolled-back", "committed"].includes(intended.phase))
      return show({
        state: "prepare-blocked",
        operationId: intended.input.operationId,
        reason:
          intended.phase === "rolled-back"
            ? "The laptop's build rolled back on this host. It waits for another laptop build or a manual Install."
            : (intended.reason ?? "The last automatic update for this build did not complete."),
      });

    // Latest wins: an unaccepted stage of any other source gives way to the laptop's build.
    const now = yield* Clock.currentTimeMillis;
    const stagedSource = update.provenance?.sourceSha;
    if (update.stagedHandle !== undefined && stagedSource !== target) {
      if (stagedSource === undefined)
        return show({ state: "prepare-blocked", reason: "Another staged build has no source." });
      const previous = retirements.get(update.stagedHandle);
      if (previous !== undefined && now < previous.at + FOLLOWER_STAGE_RETRY_MS)
        return show({
          state: "prepare-blocked",
          reason: "Another staged build could not be retired yet.",
          notBefore: isoAt(previous.at + FOLLOWER_STAGE_RETRY_MS),
        });
      const retirement = { operationId: previous?.operationId ?? newOperationId(), at: now };
      retirements.set(update.stagedHandle, retirement);
      const retired = yield* updates.retireStagedOperation({
        operationId: retirement.operationId,
        environmentId,
        currentVersion: update.currentVersion,
        expectedInstalledSource: installed,
        targetSource: stagedSource,
        stagedHandle: update.stagedHandle,
      });
      if (!retired.retired)
        return show({
          state: "prepare-blocked",
          reason: retired.reason ?? "Another staged build could not be retired.",
          notBefore: isoAt(now + FOLLOWER_STAGE_RETRY_MS),
        });
      retirements.delete(update.stagedHandle);
    }

    const input: FleetStageInput = {
      operationId: intended?.input.operationId ?? newOperationId(),
      enrollmentId: enrollment.enrollmentId,
      environmentId,
      targetSource: target,
      expectedInstalledSource: installed,
    };
    if (intended === null)
      yield* storage(() =>
        store.editFollower((old) => ({
          ...(old ?? emptyFollower),
          operation: {
            operationId: input.operationId,
            targetSource: target,
            expectedInstalledSource: installed,
          },
        })),
      );
    if (intended === null || ["staging", "stage-blocked"].includes(intended.phase)) {
      const attempted = stageAttempts.get(target);
      if (
        intended?.phase === "stage-blocked" &&
        attempted !== undefined &&
        now < attempted + FOLLOWER_STAGE_RETRY_MS
      )
        return show({
          state: "prepare-blocked",
          operationId: input.operationId,
          reason: intended.reason ?? "The laptop's build could not be prepared.",
          notBefore: isoAt(attempted + FOLLOWER_STAGE_RETRY_MS),
        });
      stageAttempts.set(target, now);
      show({ state: "preparing", operationId: input.operationId });
      const staged = yield* stage(input).pipe(Effect.result);
      if (staged._tag === "Failure")
        return show({
          state: "prepare-blocked",
          operationId: input.operationId,
          reason: staged.failure.message,
        });
      operation = staged.success.operation;
    } else {
      operation = intended;
    }
    if (operation === null || !["staged", "install-blocked"].includes(operation.phase))
      return show({
        state: "prepare-blocked",
        operationId: input.operationId,
        reason: operation?.reason ?? "The laptop's build could not be prepared.",
      });

    const decision = yield* claimAutomaticActivation;
    if (!decision.ready)
      return show({
        state: "waiting-for-quiet",
        operationId: input.operationId,
        reason: decision.reason,
        quiet: quietView(decision, now),
        ...(decision.notBefore === undefined ? {} : { notBefore: isoAt(decision.notBefore) }),
      });
    show({
      state: "activating",
      operationId: input.operationId,
      quiet: quietView(decision, now),
    });
    yield* dispatch(operation);
    const settled = yield* storage(() => store.readOperation(input.operationId));
    if (settled?.phase === "install-blocked")
      show({
        state: "waiting-for-quiet",
        operationId: input.operationId,
        reason: settled.reason ?? "The automatic update was withdrawn before it started.",
      });
  });
  const follow = gate
    .withPermits(1)(followOnce)
    .pipe(
      Effect.catch((error) =>
        Effect.sync(() => {
          if (followerView !== undefined)
            followerView = { ...followerView, state: "unavailable", reason: error.message };
        }),
      ),
    );
  return FleetUpdates.of({
    status,
    enroll: (input) => gate.withPermits(1)(enroll(input)),
    stage: (input) => gate.withPermits(1)(stage(input)),
    retire: (input) => gate.withPermits(1)(retire(input)),
    activate: (input) => gate.withPermits(1)(activate(input)),
    leader,
    follow,
  });
});

export const layer = Layer.effect(
  FleetUpdates,
  Effect.gen(function* () {
    const config = yield* ServerConfig;
    const environment = yield* ServerEnvironment;
    const updates = yield* JonesUpdates;
    const sourceRelation = yield* JonesSourceRelation;
    const serviceMode = config.mode !== "desktop";
    const wakeups = yield* Queue.sliding<void>(1);
    const fleet = yield* makeFleetUpdates({
      wakeups,
      environmentId: yield* environment.getEnvironmentId,
      serviceMode,
      store: createFleetHostStore(config.baseDir),
      updates,
      quiet: yield* makeFleetQuietSignals,
      relation: sourceRelation.relation,
    });
    if (serviceMode && updates.fleetOperationsSupported)
      yield* forkParked(
        fleet.follow.pipe(
          Effect.andThen(
            Effect.raceFirst(Effect.sleep(FOLLOWER_PASS_INTERVAL), Queue.take(wakeups)),
          ),
          Effect.forever,
        ),
      );
    return fleet;
  }),
);
