import * as Setup from "../../project/ProjectSetupScriptRunner.ts";
import {
  CommandId,
  type OrchestrationV2ServerCommand,
  RunId,
  ThreadId,
  VcsCreateWorktreeResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import { OrchestrationV2ThreadLaunchWorkspaceStrategy } from "@t3tools/contracts";
import * as Git from "../../git/GitWorkflowService.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Shared from "../cleanup/sharedAdmission/Protocol.ts";
import * as Checkout from "./OrdinaryCheckout.ts";
import * as Preparation from "./CheckoutPreparation.ts";
import * as Intent from "./CheckoutIntent.ts";
import * as Targets from "./CheckoutTarget.ts";
import type { PreparationContext } from "./CheckoutCommit.ts";
import {
  canonical,
  CheckoutError,
  digest,
  type PreparationBinding,
  PreparationOrigin,
  type TargetSnapshot,
} from "./CheckoutProtocol.ts";

const isCheckoutError = Schema.is(CheckoutError);
const isAdmissionError = Schema.is(Shared.AdmissionError);
const decodeAcceptedStrategy = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ThreadLaunchWorkspaceStrategy),
);

const decodePreparationOrigin = Schema.decodeEffect(Schema.fromJsonString(PreparationOrigin), {
  onExcessProperty: "error",
});
// Detached copies preserve the encoded result/strategy strings, including their digest inputs.
const decodeWorktreeResult = Schema.decodeEffect(
  Schema.fromJsonString(Schema.toEncoded(VcsCreateWorktreeResult)),
  { onExcessProperty: "error" },
);
const launchStrategy = Schema.Union([
  OrchestrationV2ThreadLaunchWorkspaceStrategy.members[1],
  OrchestrationV2ThreadLaunchWorkspaceStrategy.members[2],
  OrchestrationV2ThreadLaunchWorkspaceStrategy.members[3].mapFields((fields) => ({
    ...fields,
    baseRef: Schema.required(fields.baseRef),
  })),
]);
const decodeLaunchStrategy = Schema.decodeEffect(
  Schema.fromJsonString(Schema.toEncoded(launchStrategy)),
  { onExcessProperty: "error" },
);

// The Git allocation owner must reserve the exact path its createWorktree will
// use. No layer guesses a path from userdata, branch text or a caller payload.
export class LaunchPreparationAllocation extends Context.Service<
  LaunchPreparationAllocation,
  {
    readonly allocate: (origin: PreparationOrigin) => Effect.Effect<string, CheckoutError>;
  }
>()("t3/jones/ordinaryCheckout/LaunchPreparation/LaunchPreparationAllocation") {}

const operationBrand = Symbol("launch preparation operation");
export interface LaunchOperation {
  readonly [operationBrand]: true;
}
type Dispatch<A, E, R> = (
  command: OrchestrationV2ServerCommand,
  context?: PreparationContext,
) => Effect.Effect<A, E, R>;
type CreateInput = Parameters<Git.GitWorkflowService["Service"]["createWorktree"]>[0];
type CreateOptions = Parameters<Git.GitWorkflowService["Service"]["createWorktree"]>[1];
type RenameInput = Parameters<Git.GitWorkflowService["Service"]["renameBranch"]>[0];
type AcceptedStrategy = OrchestrationV2ThreadLaunchWorkspaceStrategy;
type Strategy =
  | Exclude<AcceptedStrategy, { readonly type: "auto" | "worktree" }>
  | (Extract<AcceptedStrategy, { readonly type: "worktree" }> & { readonly baseRef: string });
interface Live {
  readonly fiber: number;
  readonly origin: PreparationOrigin;
  readonly reservation: Preparation.PreparationReservation;
  readonly path: string | null;
  readonly scopes: readonly [Shared.PhysicalScope, ...Shared.PhysicalScope[]];
  readonly strategy: Strategy;
  active: boolean;
  entered: boolean;
  ready: boolean;
  setupSettled: boolean;
  result: Preparation.MaterializedResult | null;
}

// Build this layer once, provide its custody to CheckoutPreparation, and its
// retained owner to LaunchPreparation. Rebuilding either side breaks identity.
class LaunchPreparationOwner extends Context.Service<
  LaunchPreparationOwner,
  {
    readonly open: (
      threadId: ThreadId,
      runId: RunId,
      renameOf?: LaunchOperation,
    ) => Effect.Effect<Live | null, CheckoutError, Scope.Scope>;
    readonly current: (operation: LaunchOperation) => Effect.Effect<Live, CheckoutError>;
    readonly retain: (live: Live) => LaunchOperation;
    readonly renameParent: (operation: LaunchOperation) => Effect.Effect<Live, CheckoutError>;
    readonly reserve: (
      command: OrchestrationV2ServerCommand,
      scopes: Live["scopes"],
      kind?: "acceptance" | "mutation",
    ) => Effect.Effect<Preparation.LocalAcceptance, CheckoutError, Scope.Scope>;
    readonly captureScopes: (
      paths: readonly [string, ...string[]],
    ) => Effect.Effect<Live["scopes"], CheckoutError>;
    readonly checkOutside: Effect.Effect<void, CheckoutError>;
  }
>()("t3/jones/ordinaryCheckout/LaunchPreparation/LaunchPreparationOwner") {}

const makeOwner = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const checkout = yield* Checkout.OrdinaryCheckout;
  const participant = yield* Shared.Participant;
  const journal = yield* Shared.CommonJournal;
  const bridge = yield* Shared.LocalBridge;
  const observations = yield* Shared.ScopeObservation;
  const allocation = yield* LaunchPreparationAllocation;
  const shared = Context.make(Shared.Participant, participant).pipe(
    Context.add(Shared.CommonJournal, journal),
    Context.add(Shared.LocalBridge, bridge),
    Context.add(Shared.ScopeObservation, observations),
  );
  const mapError = (cause: unknown) =>
    isCheckoutError(cause) ? cause : new CheckoutError({ reason: "unavailable", cause });
  const lives = new WeakMap<LaunchOperation, Live>();
  const reservations = new WeakMap<Preparation.PreparationReservation, Live>();
  const outside = Effect.gen(function* () {
    if (
      participant.sql !== sql ||
      Option.isSome(yield* Effect.serviceOption(sql.transactionService))
    )
      return yield* new CheckoutError({ reason: "unavailable" });
    yield* participant.checkOutside.pipe(Effect.mapError(mapError));
  });
  const check = Effect.fn("LaunchPreparationOwner.check")(function* (live: Live) {
    if (!active.has(live) || !live.active || live.fiber !== (yield* Effect.fiberId))
      return yield* new CheckoutError({ reason: "unavailable" });
  });
  const lookup = Effect.fn("LaunchPreparationOwner.lookup")(function* (
    origin: PreparationOrigin,
    reservation: Preparation.PreparationReservation,
  ) {
    const live = reservations.get(reservation);
    // CheckoutPreparation freezes a copy of the reservation; match its retained
    // token and full operation identity, never a structurally similar token.
    const retained =
      live ??
      [...active].find(
        (candidate) =>
          candidate.reservation.acceptance.live === reservation.acceptance.live &&
          canonical(candidate.reservation.acceptance.expected) ===
            canonical(reservation.acceptance.expected) &&
          candidate.reservation.operationId === reservation.operationId &&
          candidate.reservation.generation === reservation.generation,
      );
    if (!retained || retained.origin.originId !== origin.originId || !retained.entered)
      return yield* new CheckoutError({ reason: "unavailable" });
    yield* check(retained);
    return retained;
  });
  const active = new Set<Live>();
  const captureScopes = Effect.fn("LaunchPreparationOwner.captureScopes")(function* (
    paths: readonly [string, ...string[]],
  ) {
    yield* outside;
    const scopes = yield* Effect.forEach([...new Set(paths)], (path) =>
      Effect.gen(function* () {
        const scope = yield* observations.capture(path);
        // An absent destination occupies its existing parent for the whole
        // operation, including after creation. The exact allocated path is retained
        // separately and must equal Git's completed result.
        return scope.unresolved.length === 0
          ? scope
          : yield* observations.capture(scope.ancestors[0].path);
      }),
    );
    return scopes as [Shared.PhysicalScope, ...Shared.PhysicalScope[]];
  }, Effect.mapError(mapError));
  const reserve: LaunchPreparationOwner["Service"]["reserve"] = Effect.fn(
    "LaunchPreparationOwner.reserve",
  )(function* (
    command: OrchestrationV2ServerCommand,
    scopes: Live["scopes"],
    kind: "acceptance" | "mutation" = "acceptance",
  ) {
    yield* outside;
    const binding = {
      operation: {
        storeId: participant.identity.storeId,
        originalOperationId: command.commandId,
        role: `ordinary-launch-${kind}`,
        generation: 0,
      },
      scopes,
      kind,
      localReference: command.commandId,
      owner: null,
      slot: null,
    };
    const live = yield* Shared.reserveScopes(binding).pipe(Effect.provide(shared));
    return {
      live,
      expected: {
        operationKey: Shared.operationKey(binding.operation),
        bindingSha256: digest({ ...binding, participant: participant.identity }),
      },
    };
  }, Effect.mapError(mapError));
  const owner = LaunchPreparationOwner.of({
    checkOutside: outside,
    captureScopes,
    reserve,
    renameParent: Effect.fn("LaunchPreparationOwner.renameParent")(function* (
      operation: LaunchOperation,
    ) {
      const live = lives.get(operation);
      if (!live?.active || !live.entered)
        return yield* new CheckoutError({ reason: "unavailable" });
      return live;
    }),
    current: Effect.fn("LaunchPreparationOwner.current")(function* (operation: LaunchOperation) {
      const live = lives.get(operation);
      if (!live) return yield* new CheckoutError({ reason: "unavailable" });
      yield* check(live);
      return live;
    }),
    retain: (live) => {
      const operation: LaunchOperation = Object.freeze<LaunchOperation>({ [operationBrand]: true });
      lives.set(operation, live);
      return operation;
    },
    open: Effect.fn("LaunchPreparationOwner.open")(function* (
      threadId: ThreadId,
      runId: RunId,
      renameOf?: LaunchOperation,
    ) {
      yield* outside;
      const origin = yield* checkout.readPreparationForRun(threadId, runId);
      if (!origin) return yield* new CheckoutError({ reason: "unavailable" });
      if (yield* checkout.readPreparationRelease(origin.originId)) return null;
      yield* checkout.validatePreparationOrigin(origin, "preparing");
      const acceptedStrategy = yield* decodeAcceptedStrategy(origin.strategyJson);
      if (
        acceptedStrategy.type === "auto" ||
        (acceptedStrategy.type === "worktree" && acceptedStrategy.baseRef === undefined)
      )
        return yield* new CheckoutError({ reason: "unavailable" });
      const strategy: Strategy =
        acceptedStrategy.type === "worktree"
          ? { ...acceptedStrategy, baseRef: acceptedStrategy.baseRef! }
          : acceptedStrategy;
      const predecessor = yield* checkout.readPreparationBinding(origin.originId);
      const parent = renameOf === undefined ? undefined : lives.get(renameOf);
      if (
        renameOf !== undefined &&
        (!parent?.active ||
          !parent.entered ||
          parent.origin.originId !== origin.originId ||
          !predecessor)
      )
        return yield* new CheckoutError({ reason: "unavailable" });
      // A retained physical outcome needs reconciliation, not another create or
      // setup invocation. No retry generation is inferred from failed status.
      if (renameOf === undefined && predecessor !== null)
        return yield* new CheckoutError({ reason: "unavailable" });
      const progressId = CommandId.make(
        `launch-preparation:${origin.originId}:${parent ? "rename" : "materialize"}:enter`,
      );
      const retained = yield* journal.read(
        Shared.operationKey({
          storeId: participant.identity.storeId,
          originalOperationId: progressId,
          role: "ordinary-launch-mutation",
          generation: 0,
        }),
      );
      if (retained !== null) return yield* new CheckoutError({ reason: "unavailable" });
      const path = parent
        ? predecessor!.target.worktreePath
        : strategy.type === "worktree"
          ? yield* allocation.allocate(
              yield* decodePreparationOrigin(canonical(origin)).pipe(Effect.mapError(mapError)),
            )
          : strategy.type === "existing_worktree"
            ? strategy.worktreePath
            : null;
      if (strategy.type === "worktree" && path === null)
        return yield* new CheckoutError({ reason: "identity" });
      const scopes = yield* captureScopes(
        path === null ? [origin.initial.projectRoot] : [origin.initial.projectRoot, path],
      );
      const progress = {
        type: "prepared-run.progress" as const,
        commandId: progressId,
        threadId,
        runId,
        phase: "worktree" as const,
      };
      const acceptance = yield* reserve(progress, scopes, "mutation");
      const reservation = Object.freeze({
        operationId: progress.commandId,
        generation: 0,
        acceptance,
      });
      const live: Live = {
        origin,
        reservation,
        scopes,
        strategy,
        path,
        fiber: yield* Effect.fiberId,
        active: true,
        entered: false,
        ready: false,
        setupSettled: path === null,
        result: null,
      };
      reservations.set(reservation, live);
      active.add(live);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          live.active = false;
          active.delete(live);
        }),
      );
      return live;
    }, Effect.mapError(mapError)),
  });
  const custody = Preparation.CheckoutPreparationCustody.of({
    checkOutside: Effect.fn("LaunchPreparationCustody.checkOutside")(function* (
      origin: PreparationOrigin,
      reservation: Preparation.PreparationReservation,
    ) {
      yield* outside;
      const live = yield* lookup(origin, reservation);
      for (const scope of live.scopes)
        yield* observations.revalidate(scope).pipe(Effect.mapError(mapError));
    }),
    checkLocal: Effect.fn("LaunchPreparationCustody.checkLocal")(function* (
      origin: PreparationOrigin,
      reservation: Preparation.PreparationReservation,
    ) {
      yield* lookup(origin, reservation);
      if (
        Option.getOrUndefined(yield* Effect.serviceOption(SqlClient.SqlClient)) !== sql ||
        Option.isNone(yield* Effect.serviceOption(sql.transactionService))
      )
        return yield* new CheckoutError({ reason: "unavailable" });
      yield* participant.validateLocal.pipe(Effect.mapError(mapError));
    }),
    checkResult: Effect.fn("LaunchPreparationCustody.checkResult")(function* (
      origin: PreparationOrigin,
      reservation: Preparation.PreparationReservation,
      result: Preparation.MaterializedResult,
    ) {
      yield* outside;
      const live = yield* lookup(origin, reservation);
      if (!live.result || canonical(live.result) !== canonical(result))
        return yield* new CheckoutError({ reason: "identity" });
    }),
    checkReleaseOutside: Effect.fn("LaunchPreparationCustody.checkReleaseOutside")(function* (
      origin: PreparationOrigin,
      reservation: Preparation.PreparationReservation,
      binding: PreparationBinding,
    ) {
      yield* outside;
      const live = yield* lookup(origin, reservation);
      if (!live.ready || !live.result || binding.resultDigest !== digest(live.result))
        return yield* new CheckoutError({ reason: "unavailable" });
    }),
  });
  return Context.make(LaunchPreparationOwner, owner).pipe(
    Context.add(Preparation.CheckoutPreparationCustody, custody),
  );
});
export const layerCustody = Layer.effectContext(makeOwner);

export class LaunchPreparation extends Context.Service<
  LaunchPreparation,
  {
    readonly initialTarget: (
      commandId: CommandId,
    ) => Effect.Effect<TargetSnapshot | null, CheckoutError>;
    readonly accept: <A, E, R>(
      command: OrchestrationV2ServerCommand,
      paths: readonly [string, ...string[]],
      dispatch: Dispatch<A, E, R>,
    ) => Effect.Effect<A, E | CheckoutError, R>;
    readonly withRun: <A, DispatchError, DispatchRequirements, UseError, UseRequirements>(
      threadId: ThreadId,
      runId: RunId,
      dispatch: Dispatch<unknown, DispatchError, DispatchRequirements>,
      use: (
        operation: LaunchOperation,
        strategy: Strategy,
      ) => Effect.Effect<A, UseError, UseRequirements>,
    ) => Effect.Effect<
      A | undefined,
      DispatchError | UseError | CheckoutError,
      DispatchRequirements | UseRequirements
    >;
    readonly createWorktree: (
      operation: LaunchOperation,
      input: CreateInput,
      options?: CreateOptions,
    ) => Effect.Effect<
      Effect.Success<ReturnType<Git.GitWorkflowService["Service"]["createWorktree"]>>,
      Effect.Error<ReturnType<Git.GitWorkflowService["Service"]["createWorktree"]>> | CheckoutError
    >;
    readonly workspace: <A, E, R>(
      operation: LaunchOperation,
      command: Extract<OrchestrationV2ServerCommand, { type: "thread.metadata.update" }>,
      dispatch: Dispatch<A, E, R>,
    ) => Effect.Effect<A, E | CheckoutError, R>;
    readonly rename: <A, E, R>(
      parent: LaunchOperation,
      input: RenameInput,
      command: (
        branch: string,
      ) => Extract<OrchestrationV2ServerCommand, { type: "thread.metadata.update" }>,
      dispatch: Dispatch<A, E, R>,
    ) => Effect.Effect<
      A,
      | E
      | CheckoutError
      | Effect.Error<ReturnType<Git.GitWorkflowService["Service"]["renameBranch"]>>,
      R
    >;
    readonly runSetup: (
      operation: LaunchOperation,
      input: Setup.ProjectSetupScriptRunnerInput,
    ) => Effect.Effect<
      Setup.ProjectSetupScriptRunnerResult,
      Setup.ProjectSetupScriptRunnerError | CheckoutError
    >;
    readonly ready: (operation: LaunchOperation) => Effect.Effect<void, CheckoutError>;
    readonly release: <A, E, R>(
      operation: LaunchOperation,
      command: Extract<OrchestrationV2ServerCommand, { type: "prepared-run.release" }>,
      dispatch: Dispatch<A, E, R>,
    ) => Effect.Effect<A, E | CheckoutError, R>;
  }
>()("t3/jones/ordinaryCheckout/LaunchPreparation") {}

const make = Effect.gen(function* () {
  const owner = yield* LaunchPreparationOwner;
  const preparation = yield* Preparation.CheckoutPreparation;
  const intent = yield* Intent.CheckoutIntent;
  const checkout = yield* Checkout.OrdinaryCheckout;
  const targets = yield* Targets.CheckoutTarget;
  const git = yield* Git.GitWorkflowService;
  const setupScripts = yield* Setup.ProjectSetupScriptRunner;
  const targetObservation = yield* Targets.CheckoutTargetObservation;
  const receipts = yield* Receipts.CommandReceiptStoreV2;
  const handles = new WeakMap<LaunchOperation, Preparation.PreparationHandle>();
  const mapError = (cause: unknown) =>
    isCheckoutError(cause) ? cause : new CheckoutError({ reason: "unavailable", cause });
  const replay = Effect.fn("LaunchPreparation.replay")(function* (
    command: OrchestrationV2ServerCommand,
  ) {
    const receipt = yield* receipts
      .getByCommandId(command.commandId)
      .pipe(Effect.mapError(mapError));
    if (Option.isNone(receipt)) return false;
    yield* intent.replay(command, receipt.value.threadId);
    return true;
  });
  const accept: LaunchPreparation["Service"]["accept"] = (command, paths, dispatch) =>
    Effect.scoped(
      Effect.gen(function* () {
        if (yield* replay(command)) return yield* dispatch(command);
        const localAcceptance = yield* owner.reserve(command, yield* owner.captureScopes(paths));
        return yield* dispatch(command, { localAcceptance });
      }),
    );
  const withLive = Effect.fn("LaunchPreparation.withLive")(function* <
    A,
    DispatchError,
    DispatchRequirements,
    UseError,
    UseRequirements,
  >(
    live: Live,
    dispatch: Dispatch<unknown, DispatchError, DispatchRequirements>,
    use: (operation: LaunchOperation) => Effect.Effect<A, UseError, UseRequirements>,
  ): Effect.fn.Return<
    A,
    DispatchError | UseError | CheckoutError,
    DispatchRequirements | UseRequirements | Scope.Scope
  > {
    const operation = owner.retain(live);
    const progress = {
      type: "prepared-run.progress" as const,
      commandId: CommandId.make(live.reservation.operationId),
      threadId: ThreadId.make(live.origin.subjectThreadId),
      runId: RunId.make(live.origin.runId),
      phase: "worktree" as const,
    };
    yield* dispatch(progress, { localAcceptance: live.reservation.acceptance });
    return yield* Shared.enterOnce(
      live.reservation.acceptance.live,
      Effect.gen(function* () {
        live.entered = true;
        return yield* preparation.withPreparation(
          {
            originRef: live.origin.originId,
            generation: live.reservation.generation,
            reservation: live.reservation,
          },
          (handle) => {
            handles.set(operation, handle);
            return use(operation);
          },
        );
      }),
    ).pipe(Effect.mapError((cause) => (isAdmissionError(cause) ? mapError(cause) : cause)));
  });
  const createAttempts = new WeakSet<LaunchOperation>();
  const setupAttempts = new WeakSet<LaunchOperation>();
  const results = new WeakMap<
    LaunchOperation,
    {
      readonly value: unknown;
      readonly path: string;
      readonly branch: string;
      readonly requestedBase: string | null;
      readonly resolvedBase: string | null;
      readonly kind: "materialize" | "rename";
    }
  >();
  const workspace: LaunchPreparation["Service"]["workspace"] = (operation, command, dispatch) =>
    Effect.scoped(
      Effect.gen(function* () {
        if (yield* replay(command)) return yield* dispatch(command);
        const live = yield* owner.current(operation);
        const handle = handles.get(operation);
        if (!handle) return yield* new CheckoutError({ reason: "unavailable" });
        const actual = results.get(operation);
        if (live.strategy.type === "worktree" && !actual)
          return yield* new CheckoutError({ reason: "unavailable" });
        const before = yield* checkout.readTargetSnapshot(live.origin.subjectThreadId);
        const target = yield* targets.capture({
          ...before,
          worktreePath: command.worktreePath ?? null,
          threadBranch: command.branch ?? null,
        });
        if (
          target.worktreePath !== live.path ||
          (actual && (actual.path !== target.worktreePath || actual.branch !== target.threadBranch))
        )
          return yield* new CheckoutError({ reason: "identity" });
        const { threadRevision: _revision, ...snapshot } = {
          projectId: target.projectId,
          threadId: target.threadId,
          projectRoot: target.projectRoot,
          projectRevision: target.projectRevision,
          worktreePath: target.worktreePath,
          threadBranch: target.threadBranch,
          threadRevision: target.threadRevision,
        };
        const result: Preparation.MaterializedResult = {
          kind: actual?.kind ?? "materialize",
          operationId: live.reservation.operationId,
          generation: live.reservation.generation,
          target: snapshot,
          requestedBaseRef: actual?.requestedBase ?? null,
          resolvedBaseRef: actual?.resolvedBase ?? null,
          resolvedBranch: target.threadBranch,
          registeredPath: target.checkoutKey,
          resultIdentity: digest({ actual: actual ?? null, target }),
        };
        live.result = result;
        const localAcceptance = yield* owner.reserve(command, live.scopes);
        return yield* preparation.withMaterializedResult(handle, { command, result }, () =>
          dispatch(command, { preparation: handle, localAcceptance }),
        );
      }),
    );
  return LaunchPreparation.of({
    initialTarget: Effect.fn("LaunchPreparation.initialTarget")(function* (commandId: CommandId) {
      const origins = yield* checkout.readPreparationOrigins(commandId);
      if (origins.length > 1) return yield* new CheckoutError({ reason: "identity" });
      return origins[0]?.initial ?? null;
    }),
    accept,
    withRun: Effect.fn("LaunchPreparation.withRun")(function <
      A,
      DispatchError,
      DispatchRequirements,
      UseError,
      UseRequirements,
    >(
      threadId: ThreadId,
      runId: RunId,
      dispatch: Dispatch<unknown, DispatchError, DispatchRequirements>,
      use: (
        operation: LaunchOperation,
        strategy: Strategy,
      ) => Effect.Effect<A, UseError, UseRequirements>,
    ): Effect.Effect<
      A | undefined,
      DispatchError | UseError | CheckoutError,
      DispatchRequirements | UseRequirements
    > {
      return Effect.scoped(
        Effect.gen(function* () {
          const live = yield* owner.open(threadId, runId);
          if (live === null) return undefined;
          return yield* withLive(live, dispatch, (operation) =>
            decodeLaunchStrategy(canonical(live.strategy)).pipe(
              Effect.mapError(mapError),
              Effect.flatMap((strategy) => use(operation, strategy)),
            ),
          ).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                live.active = false;
              }),
            ),
          );
        }),
      );
    }),
    createWorktree: Effect.fn("LaunchPreparation.createWorktree")(function* (
      operation: LaunchOperation,
      input: CreateInput,
      options?: CreateOptions,
    ) {
      const live = yield* owner.current(operation);
      if (
        live.strategy.type !== "worktree" ||
        live.path === null ||
        createAttempts.has(operation) ||
        input.cwd !== live.origin.initial.projectRoot
      )
        return yield* new CheckoutError({ reason: "identity" });
      createAttempts.add(operation);
      // Git receives the very destination fenced before physical entry.
      const result = yield* git.createWorktree({ ...input, path: live.path }, options);
      if (result.worktree.path !== live.path)
        return yield* new CheckoutError({ reason: "identity" });
      const observed = yield* targetObservation.git(result.worktree.path);
      if (!observed?.head) return yield* new CheckoutError({ reason: "unavailable" });
      results.set(operation, {
        value: {
          completed: yield* decodeWorktreeResult(canonical(result)).pipe(Effect.mapError(mapError)),
          usedRef: input.refName,
          baseRefName: input.baseRefName ?? null,
          observed,
        },
        path: result.worktree.path,
        branch: result.worktree.refName,
        requestedBase: live.strategy.baseRef ?? null,
        resolvedBase: observed.head,
        kind: "materialize",
      });
      return result;
    }),
    workspace,
    rename: Effect.fn("LaunchPreparation.rename")(function <A, E, R>(
      parent: LaunchOperation,
      input: RenameInput,
      commandFor: (
        branch: string,
      ) => Extract<OrchestrationV2ServerCommand, { type: "thread.metadata.update" }>,
      dispatch: Dispatch<A, E, R>,
    ) {
      return Effect.scoped(
        Effect.gen(function* () {
          // Issuing a child operation is deliberate; a fork inherits no handle.
          const parentState = yield* owner.renameParent(parent);
          const parentLive = yield* owner.open(
            ThreadId.make(parentState.origin.subjectThreadId),
            RunId.make(parentState.origin.runId),
            parent,
          );
          if (!parentLive) return yield* new CheckoutError({ reason: "unavailable" });
          return yield* withLive(parentLive, dispatch, (operation) =>
            Effect.gen(function* () {
              const renamed = yield* git.renameBranch(input);
              const command = commandFor(renamed.branch);
              results.set(operation, {
                value: renamed,
                path: input.cwd,
                branch: renamed.branch,
                requestedBase: null,
                resolvedBase: null,
                kind: "rename",
              });
              const result = yield* workspace(operation, command, dispatch);
              yield* owner.renameParent(parent);
              parentState.result = parentLive.result;
              return result;
            }),
          ).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                parentLive.active = false;
              }),
            ),
          );
        }),
      );
    }),
    runSetup: Effect.fn("LaunchPreparation.runSetup")(function* (
      operation: LaunchOperation,
      input: Setup.ProjectSetupScriptRunnerInput,
    ) {
      const live = yield* owner.current(operation);
      if (
        setupAttempts.has(operation) ||
        input.threadId !== live.origin.subjectThreadId ||
        input.worktreePath !== live.path ||
        !live.result
      )
        return yield* new CheckoutError({ reason: "identity" });
      setupAttempts.add(operation);
      const setup = yield* setupScripts.runForThread(input);
      if (setup.status !== "started") {
        live.setupSettled = true;
        return setup;
      }
      // A spawned terminal is not a successfully written setup command.
      yield* setup.writeCompletion;
      if (setup.async) live.setupSettled = true;
      if (!setup.completion) {
        if (!setup.async) return yield* new CheckoutError({ reason: "unavailable" });
        return setup;
      }
      return {
        ...setup,
        completion: setup.completion.pipe(
          Effect.tap((completed) =>
            Effect.sync(() => {
              if (completed.exitCode === 0) live.setupSettled = true;
            }),
          ),
        ),
      };
    }),
    ready: Effect.fn("LaunchPreparation.ready")(function* (operation: LaunchOperation) {
      const live = yield* owner.current(operation);
      if (!live.setupSettled || !live.result)
        return yield* new CheckoutError({ reason: "unavailable" });
      live.ready = true;
    }),
    release: Effect.fn("LaunchPreparation.release")(function <A, E, R>(
      operation: LaunchOperation,
      command: Extract<OrchestrationV2ServerCommand, { type: "prepared-run.release" }>,
      dispatch: Dispatch<A, E, R>,
    ) {
      return Effect.scoped(
        Effect.gen(function* () {
          if (yield* replay(command)) return yield* dispatch(command);
          const live = yield* owner.current(operation);
          const handle = handles.get(operation);
          if (!handle || !live.ready) return yield* new CheckoutError({ reason: "unavailable" });
          const localAcceptance = yield* owner.reserve(command, live.scopes);
          return yield* dispatch(command, { preparation: handle, localAcceptance });
        }),
      );
    }),
  });
});
export const layer = Layer.effect(LaunchPreparation, make);
