import * as DateTime from "effect/DateTime";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import { OrchestrationV2ThreadLaunchWorkspaceStrategy } from "@t3tools/contracts";
import * as Shared from "../cleanup/sharedAdmission/Protocol.ts";
import * as Checkout from "./OrdinaryCheckout.ts";
import * as Targets from "./CheckoutTarget.ts";
import * as Conflicts from "./CheckoutConflictIntegration.ts";
import {
  canonical,
  CheckoutError,
  type PreparationOrigin,
  type PreparationBinding,
  type Target,
  TargetSnapshot,
} from "./CheckoutProtocol.ts";
import { decodedCommand, planDigests } from "./CheckoutPreparationSchema.ts";
import type { IntentPlan } from "./CheckoutIntent.ts";

const decodeWorkspaceStrategy = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ThreadLaunchWorkspaceStrategy),
);
const isCheckoutError = Schema.is(CheckoutError);

export interface LocalAcceptance {
  readonly live: Shared.LiveAdmission;
  readonly expected: Pick<Shared.LocalCertificate, "operationKey" | "bindingSha256">;
}
export interface PreparationReservation {
  readonly operationId: string;
  readonly generation: number;
  readonly acceptance: LocalAcceptance;
}
// These required ports are owned by the actual materialization/checked-owner
// service. They must validate live shared custody; this module issues no fs permit.
export class CheckoutPreparationCustody extends Context.Service<
  CheckoutPreparationCustody,
  {
    readonly checkOutside: (
      origin: PreparationOrigin,
      reservation: PreparationReservation,
    ) => Effect.Effect<void, CheckoutError>;
    readonly checkReleaseOutside: (
      origin: PreparationOrigin,
      reservation: PreparationReservation,
      binding: PreparationBinding,
    ) => Effect.Effect<void, CheckoutError>;
    readonly checkLocal: (
      origin: PreparationOrigin,
      reservation: PreparationReservation,
    ) => Effect.Effect<void, CheckoutError>;
    readonly checkResult: (
      origin: PreparationOrigin,
      reservation: PreparationReservation,
      result: MaterializedResult,
    ) => Effect.Effect<void, CheckoutError>;
  }
>()("t3/jones/ordinaryCheckout/CheckoutPreparation/CheckoutPreparationCustody") {}
export interface MaterializedResult {
  readonly kind: "materialize" | "rename";
  readonly operationId: string;
  readonly generation: number;
  readonly target: Omit<TargetSnapshot, "threadRevision">;
  readonly resolvedBaseRef: string | null;
  readonly requestedBaseRef: string | null;
  readonly resolvedBranch: string | null;
  readonly registeredPath: string;
  readonly resultIdentity: string;
}
const materializedResultJson = Schema.fromJsonString(
  Schema.Struct({
    kind: Schema.Literals(["materialize", "rename"]),
    operationId: Schema.String,
    generation: Schema.Number,
    target: TargetSnapshot.mapFields(({ threadRevision: _threadRevision, ...fields }) => fields),
    resolvedBaseRef: Schema.NullOr(Schema.String),
    requestedBaseRef: Schema.NullOr(Schema.String),
    resolvedBranch: Schema.NullOr(Schema.String),
    registeredPath: Schema.String,
    resultIdentity: Schema.String,
  }),
);
const decodeMaterializedResult = Schema.decodeEffect(materializedResultJson, {
  onExcessProperty: "error",
});

const handleBrand = Symbol("ordinary preparation");
export interface PreparationHandle {
  readonly [handleBrand]: true;
}
export interface PreparedSwitch {
  readonly kind: "switch";
  readonly handle: PreparationHandle;
  readonly origin: PreparationOrigin;
  readonly predecessor: PreparationBinding | null;
  readonly before: TargetSnapshot;
  readonly target: Target;
  readonly result: MaterializedResult;
  readonly commandDigest: string;
  readonly planDigest: string;
  readonly validateConflicts: Effect.Effect<void, CheckoutError>;
  readonly reservation: PreparationReservation;
}
export interface PreparedRelease {
  readonly kind: "release";
  readonly handle: PreparationHandle;
  readonly origin: PreparationOrigin;
  readonly binding: PreparationBinding;
  readonly before: TargetSnapshot;
  readonly target: Target;
  readonly commandDigest: string;
  readonly planDigest: string;
  readonly expectedOwner: Checkout.Accept["expectedOwner"];
  readonly validateConflicts: Effect.Effect<void, CheckoutError>;
  readonly reservation: PreparationReservation;
}
export class CheckoutPreparation extends Context.Service<
  CheckoutPreparation,
  {
    readonly withPreparation: <A, E, R>(
      input: {
        readonly originRef: string;
        readonly generation: number;
        readonly reservation: PreparationReservation;
      },
      use: (handle: PreparationHandle) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | CheckoutError, R | Scope.Scope>;
    readonly withMaterializedResult: <A, E, R>(
      handle: PreparationHandle,
      input: { readonly command: unknown; readonly result: MaterializedResult },
      dispatch: (handle: PreparationHandle) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | CheckoutError, R>;
    readonly prepareSwitch: (
      handle: PreparationHandle,
      input: { readonly command: unknown; readonly finalPlan: IntentPlan },
    ) => Effect.Effect<PreparedSwitch, CheckoutError>;
    readonly prepareRelease: (
      handle: PreparationHandle,
      input: { readonly command: unknown; readonly finalPlan: IntentPlan },
    ) => Effect.Effect<PreparedRelease, CheckoutError>;
    readonly validate: (
      prepared: PreparedSwitch | PreparedRelease,
      phase: "before" | "after",
    ) => Effect.Effect<void, CheckoutError>;
    readonly consume: <A, E, R>(
      prepared: PreparedSwitch | PreparedRelease,
      write: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | CheckoutError, R>;
  }
>()("t3/jones/ordinaryCheckout/CheckoutPreparation") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const checkout = yield* Checkout.OrdinaryCheckout;
  const targets = yield* Targets.CheckoutTarget;
  const conflicts = yield* Conflicts.CheckoutConflictIntegration;
  const custody = yield* CheckoutPreparationCustody;
  interface Live {
    readonly fiber: number;
    readonly origin: PreparationOrigin;
    readonly reservation: PreparationReservation;
    active: boolean;
    result: { readonly commandDigest: string; readonly value: MaterializedResult } | null;
  }
  const handles = new WeakMap<PreparationHandle, Live>();
  const preparedObjects = new WeakMap<
    PreparedSwitch | PreparedRelease,
    { readonly fiber: number; readonly identity: string; spent: boolean }
  >();
  const preparedIdentity = (value: PreparedSwitch | PreparedRelease) =>
    canonical({
      ...value,
      handle: null,
      validateConflicts: null,
      reservation: { ...value.reservation, acceptance: null },
    });
  const current = Effect.fn("CheckoutPreparation.current")(function* (handle: PreparationHandle) {
    const live = handles.get(handle);
    if (!live || !live.active || live.fiber !== (yield* Effect.fiberId))
      return yield* new CheckoutError({ reason: "unavailable" });
    return live;
  });
  const outside = Effect.gen(function* () {
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* new CheckoutError({ reason: "unavailable" });
  });
  const withPreparation: CheckoutPreparation["Service"]["withPreparation"] = Effect.fn(
    "CheckoutPreparation.withPreparation",
  )(function* <A, E, R>(
    input: {
      readonly originRef: string;
      readonly generation: number;
      readonly reservation: PreparationReservation;
    },
    use: (handle: PreparationHandle) => Effect.Effect<A, E, R>,
  ): Effect.fn.Return<A, E | CheckoutError, R | Scope.Scope> {
    yield* outside;
    if (
      input.generation !== input.reservation.generation ||
      !Number.isSafeInteger(input.generation) ||
      input.generation < 0
    )
      return yield* new CheckoutError({ reason: "identity" });
    const origin = yield* checkout.readPreparationOrigin(input.originRef);
    if (!origin) return yield* new CheckoutError({ reason: "unavailable" });
    yield* checkout.validatePreparationOrigin(origin, "preparing");
    yield* custody.checkOutside(origin, input.reservation);
    const handle: PreparationHandle = Object.freeze<PreparationHandle>({ [handleBrand]: true });
    const live: Live = {
      fiber: yield* Effect.fiberId,
      origin,
      reservation: Object.freeze({
        ...input.reservation,
        acceptance: Object.freeze({
          ...input.reservation.acceptance,
          expected: Object.freeze({ ...input.reservation.acceptance.expected }),
        }),
      }),
      active: true,
      result: null,
    };
    handles.set(handle, live);
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        live.active = false;
        live.result = null;
      }),
    );
    return yield* use(handle).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          live.active = false;
          live.result = null;
        }),
      ),
    );
  });
  const withMaterializedResult: CheckoutPreparation["Service"]["withMaterializedResult"] =
    Effect.fn("CheckoutPreparation.withMaterializedResult")(function* <A, E, R>(
      handle: PreparationHandle,
      input: { readonly command: unknown; readonly result: MaterializedResult },
      dispatch: (handle: PreparationHandle) => Effect.Effect<A, E, R>,
    ): Effect.fn.Return<A, E | CheckoutError, R> {
      yield* outside;
      const live = yield* current(handle);
      if (live.result !== null) return yield* new CheckoutError({ reason: "conflict" });
      const command = yield* decodedCommand(input.command);
      // Plain, retained service output; never retain mutable caller references.
      const result = yield* decodeMaterializedResult(canonical(input.result)).pipe(
        Effect.mapError((cause) => new CheckoutError({ reason: "identity", cause })),
      );
      if (
        result.operationId !== live.reservation.operationId ||
        result.generation !== live.reservation.generation
      )
        return yield* new CheckoutError({ reason: "identity" });
      yield* custody.checkResult(live.origin, live.reservation, result);
      live.result = { commandDigest: command.digest, value: result };
      return yield* dispatch(handle).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            live.result = null;
          }),
        ),
      );
    });
  const prepareSwitch = Effect.fn("CheckoutPreparation.prepareSwitch")(
    function* (
      handle: PreparationHandle,
      input: { readonly command: unknown; readonly finalPlan: IntentPlan },
    ) {
      yield* outside;
      const live = yield* current(handle);
      const command = yield* decodedCommand(input.command);
      if (
        !live.result ||
        live.result.commandDigest !== command.digest ||
        command.command.type !== "thread.metadata.update" ||
        command.command.threadId !== live.origin.subjectThreadId
      )
        return yield* new CheckoutError({ reason: "identity" });
      yield* checkout.validatePreparationOrigin(live.origin, "preparing");
      yield* custody.checkOutside(live.origin, live.reservation);
      const before = yield* checkout.readTargetSnapshot(live.origin.subjectThreadId);
      const metadata = input.finalPlan.events.filter(
        (event) =>
          event.threadId === live.origin.subjectThreadId &&
          event.type === "thread.metadata-updated",
      );
      if (
        input.finalPlan.events.some(
          (event) =>
            event.threadId !== live.origin.subjectThreadId ||
            event.type !== "thread.metadata-updated",
        ) ||
        input.finalPlan.effects.length !== 0 ||
        metadata.length !== 1 ||
        metadata[0]?.type !== "thread.metadata-updated"
      )
        return yield* new CheckoutError({ reason: "identity" });
      const next: TargetSnapshot = {
        ...before,
        projectId: metadata[0].payload.projectId,
        worktreePath: metadata[0].payload.worktreePath,
        threadBranch: metadata[0].payload.branch,
        threadRevision: DateTime.formatIso(metadata[0].payload.updatedAt),
      };
      const result = live.result.value;
      const { threadRevision: _revision, ...resultTarget } = next;
      if (
        canonical(resultTarget) !== canonical(result.target) ||
        next.projectId !== live.origin.initial.projectId ||
        next.projectRoot !== live.origin.initial.projectRoot ||
        next.projectRevision !== live.origin.initial.projectRevision
      )
        return yield* new CheckoutError({ reason: "identity" });
      const predecessor = yield* checkout.readPreparationBinding(live.origin.originId);
      const previous = predecessor?.target ?? live.origin.initial;
      if (
        before.projectId !== previous.projectId ||
        before.projectRoot !== previous.projectRoot ||
        before.projectRevision !== previous.projectRevision ||
        before.worktreePath !== previous.worktreePath ||
        before.threadBranch !== previous.threadBranch
      )
        return yield* new CheckoutError({ reason: "stale" });
      const target = yield* targets.capture(next);
      const strategy = yield* decodeWorkspaceStrategy(live.origin.strategyJson).pipe(
        Effect.mapError((cause) => new CheckoutError({ reason: "identity", cause })),
      );
      if (predecessor === null) {
        if (
          result.kind !== "materialize" ||
          result.registeredPath !== target.checkoutKey ||
          result.resolvedBranch !== target.threadBranch ||
          (strategy.type === "root" && target.cwd !== target.canonicalProjectRoot) ||
          (strategy.type === "existing_worktree" &&
            target.worktreePath !== strategy.worktreePath) ||
          (strategy.type === "worktree" &&
            (target.gitCommonDirectory === null ||
              target.worktreePath === null ||
              result.resolvedBaseRef === null ||
              result.requestedBaseRef !== (strategy.baseRef ?? null))) ||
          (strategy.branch !== undefined && strategy.branch !== target.threadBranch)
        )
          return yield* new CheckoutError({ reason: "identity" });
      } else if (
        result.kind !== "rename" ||
        target.checkoutKey !== predecessor.target.checkoutKey ||
        canonical(target.directoryIdentity) !== canonical(predecessor.target.directoryIdentity) ||
        target.gitDirectory !== predecessor.target.gitDirectory ||
        canonical(target.gitDirectoryIdentity) !==
          canonical(predecessor.target.gitDirectoryIdentity) ||
        canonical(target.gitCommonIdentity) !== canonical(predecessor.target.gitCommonIdentity) ||
        target.worktreePath !== predecessor.target.worktreePath
      )
        return yield* new CheckoutError({ reason: "identity" });
      const prepared: PreparedSwitch = Object.freeze({
        kind: "switch",
        handle,
        origin: live.origin,
        predecessor,
        before,
        target,
        result,
        commandDigest: command.digest,
        planDigest: planDigests(input.finalPlan).authority,
        validateConflicts: yield* conflicts.prepareTargetValidation([target.checkoutKey]),
        reservation: live.reservation,
      });
      preparedObjects.set(prepared, {
        fiber: yield* Effect.fiberId,
        identity: preparedIdentity(prepared),
        spent: false,
      });
      return prepared;
    },
    Effect.mapError((cause) =>
      isCheckoutError(cause) ? cause : new CheckoutError({ reason: "identity", cause }),
    ),
  );
  const prepareRelease = Effect.fn("CheckoutPreparation.prepareRelease")(function* (
    handle: PreparationHandle,
    input: { readonly command: unknown; readonly finalPlan: IntentPlan },
  ) {
    yield* outside;
    const live = yield* current(handle);
    if (
      input.finalPlan.events.some(
        (event) =>
          event.type === "thread.metadata-updated" ||
          event.threadId !== live.origin.subjectThreadId,
      )
    )
      return yield* new CheckoutError({ reason: "identity" });
    const command = yield* decodedCommand(input.command);
    if (
      command.command.type !== "prepared-run.release" ||
      command.command.threadId !== live.origin.subjectThreadId ||
      command.command.runId !== live.origin.runId
    )
      return yield* new CheckoutError({ reason: "identity" });
    yield* checkout.validatePreparationOrigin(live.origin, "preparing");
    yield* custody.checkOutside(live.origin, live.reservation);
    const binding = yield* checkout.readPreparationBinding(live.origin.originId);
    if (!binding || (yield* checkout.readPreparationRelease(live.origin.originId)))
      return yield* new CheckoutError({ reason: "unavailable" });
    yield* custody.checkReleaseOutside(live.origin, live.reservation, binding);
    const before = yield* checkout.readTargetSnapshot(live.origin.subjectThreadId);
    const target = yield* targets.capture(before);
    if (canonical(target) !== canonical(binding.target))
      return yield* new CheckoutError({ reason: "stale" });
    const owner = yield* checkout.readOwner(target.checkoutKey);
    const prepared: PreparedRelease = Object.freeze({
      kind: "release",
      handle,
      origin: live.origin,
      binding,
      before,
      target,
      commandDigest: command.digest,
      planDigest: planDigests(input.finalPlan).authority,
      expectedOwner: owner === null ? null : { epoch: owner.owner_epoch, revision: owner.revision },
      validateConflicts: yield* conflicts.prepareTargetValidation([target.checkoutKey]),
      reservation: live.reservation,
    });
    preparedObjects.set(prepared, {
      fiber: yield* Effect.fiberId,
      identity: preparedIdentity(prepared),
      spent: false,
    });
    return prepared;
  });
  const validate = Effect.fn("CheckoutPreparation.validate")(function* (
    prepared: PreparedSwitch | PreparedRelease,
    phase: "before" | "after",
  ) {
    const state = preparedObjects.get(prepared);
    const live = yield* current(prepared.handle);
    if (
      !state ||
      state.spent ||
      state.identity !== preparedIdentity(prepared) ||
      state.fiber !== (yield* Effect.fiberId) ||
      Option.getOrUndefined(yield* Effect.serviceOption(SqlClient.SqlClient)) !== sql ||
      Option.isNone(yield* Effect.serviceOption(sql.transactionService))
    )
      return yield* new CheckoutError({ reason: "unavailable" });
    yield* custody.checkLocal(live.origin, live.reservation);
    yield* checkout.validatePreparationOrigin(
      live.origin,
      phase === "after" && prepared.kind === "release" ? "released" : "preparing",
    );
    if (
      canonical(yield* checkout.readPreparationBinding(live.origin.originId)) !==
      canonical(prepared.kind === "switch" ? prepared.predecessor : prepared.binding)
    )
      return yield* new CheckoutError({ reason: "stale" });
    const snapshot = yield* checkout.readTargetSnapshot(live.origin.subjectThreadId);
    const expected =
      phase === "before"
        ? prepared.before
        : {
            projectId: prepared.target.projectId,
            threadId: prepared.target.threadId,
            projectRoot: prepared.target.projectRoot,
            worktreePath: prepared.target.worktreePath,
            threadBranch: prepared.target.threadBranch,
            projectRevision: prepared.target.projectRevision,
            threadRevision: prepared.target.threadRevision,
          };
    if (canonical(snapshot) !== canonical(expected))
      return yield* new CheckoutError({ reason: "stale" });
    yield* prepared.validateConflicts;
  });
  const consume: CheckoutPreparation["Service"]["consume"] = Effect.fn(
    "CheckoutPreparation.consume",
  )(function <A, E, R>(
    prepared: PreparedSwitch | PreparedRelease,
    write: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | CheckoutError, R> {
    return Effect.gen(function* () {
      yield* validate(prepared, "after");
      const state = preparedObjects.get(prepared)!;
      // An attempted commit cannot be reused after rollback. Reprepare under fresh
      // service custody after reconciling the already performed physical effect.
      state.spent = true;
      return yield* write;
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          const state = preparedObjects.get(prepared);
          if (state) state.spent = true;
        }),
      ),
    );
  });
  return CheckoutPreparation.of({
    withPreparation,
    withMaterializedResult,
    prepareSwitch,
    prepareRelease,
    validate,
    consume,
  });
});
export const layer = Layer.effect(CheckoutPreparation, make);
