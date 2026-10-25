import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Deletion from "../cleanup/DeletionAdmission.ts";
import * as Native from "../nativeCreation/NativeCreationRepository.ts";
import * as NativeAuthority from "../nativeCreation/NativeCreationAuthority.ts";
import * as Physical from "../nativeCreation/NativeWorkspacePhysicalPorts.ts";
import { canonical, CheckoutError, type Admission, type Birth } from "./CheckoutProtocol.ts";
import * as Target from "./CheckoutTarget.ts";
import * as Checkout from "./OrdinaryCheckout.ts";

type AcceptanceBound =
  | "birth"
  | "leaseId"
  | "receiptSequence"
  | "basisEventId"
  | "basisSequence"
  | "now"
  | "expiresAt";
export interface OrdinaryPreflight extends Omit<Checkout.Accept, AcceptanceBound> {
  readonly birth: Birth | null;
  readonly birthEventId: string;
}
// The owning receipt transaction supplies bound fields and acceptance authority.
export type CommitOrdinary = (
  final: readonly Checkout.Accept[],
) => Effect.Effect<readonly Admission[], CheckoutError, Checkout.CheckoutAcceptanceAuthority>;

const invariant = (input: OrdinaryPreflight | Checkout.Accept) => {
  const fields: Record<string, unknown> = { ...input };
  // Omit only transaction-bound fields: future invariant fields compare by default.
  for (const key of [
    "birth",
    "leaseId",
    "receiptSequence",
    "basisEventId",
    "basisSequence",
    "now",
    "expiresAt",
    "birthEventId",
  ])
    delete fields[key];
  return canonical(fields);
};

// Inactive composition only. All three receiving owners must use these adapters together.
// No layer here installs an executor or provides ordinary acceptance/retirement authority.
export class CheckoutConflictIntegration extends Context.Service<
  CheckoutConflictIntegration,
  {
    readonly prepareOrdinary: (
      preflight: readonly OrdinaryPreflight[],
    ) => Effect.Effect<CommitOrdinary, CheckoutError>;
    readonly prepareHandoff: (
      input: Parameters<Checkout.OrdinaryCheckout["Service"]["handoff"]>[0],
    ) => Effect.Effect<ReturnType<Checkout.OrdinaryCheckout["Service"]["handoff"]>, CheckoutError>;
    readonly native: Native.NativeCreationRepository["Service"];
    readonly deletion: Effect.Success<typeof Deletion.makeDeletionAdmission>;
  }
>()("t3/jones/ordinaryCheckout/CheckoutConflictIntegration") {}

const isCheckoutError = Schema.is(CheckoutError);
const isNativeError = Schema.is(Native.NativeCreationRepositoryError);
const isNativeAuthorityError = Schema.is(NativeAuthority.NativeCreationAuthorityError);
const isDeletionError = Schema.is(Deletion.DeletionAdmissionError);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const path = yield* Path.Path;
  const observation = yield* Target.CheckoutTargetObservation;
  const files = yield* Physical.NativeWorkspaceFileObservation;
  const checkout = yield* Checkout.OrdinaryCheckout;
  const native = yield* Native.NativeCreationRepository;
  const deletion = yield* Deletion.makeDeletionAdmission;
  const checkoutError = (cause: unknown) =>
    isCheckoutError(cause) ? cause : new CheckoutError({ reason: "unavailable", cause });
  const nativeError = (cause: unknown) =>
    isNativeError(cause)
      ? cause
      : new Native.NativeCreationRepositoryError({
          code: "conflict",
          message: "Ordinary checkout exclusion could not be established",
        });
  const deletionError = (cause: unknown) =>
    isDeletionError(cause)
      ? cause
      : new Deletion.DeletionAdmissionError({
          reason: "ordinary_checkout_original_owner_handoff_required",
          cause,
        });

  const canonicalPath = Effect.fn("CheckoutConflictIntegration.canonicalPath")(function* (
    value: string,
    absentAllowed: boolean,
  ) {
    if (!path.isAbsolute(value) || path.normalize(value) !== value)
      return yield* new CheckoutError({ reason: "identity" });
    const observed = yield* files.inspect(value);
    if (observed === null) {
      if (!absentAllowed) return yield* new CheckoutError({ reason: "unavailable" });
      const parent = yield* observation.directory(path.dirname(value));
      return path.join(parent.realPath, path.basename(value));
    }
    if (observed.kind !== "directory") return yield* new CheckoutError({ reason: "identity" });
    const directory = yield* observation.directory(value);
    const git = yield* observation.git(directory.realPath);
    return git === null ? directory.realPath : git.root;
  }, Effect.mapError(checkoutError));

  // Resolve physical paths before SQL. Re-read the exact retained rows inside the
  // write transaction; a newly inserted native/deletion claim invalidates this snapshot.
  const foreignRows = () => sql<{
    readonly kind: string;
    readonly id: string;
    readonly target: string;
    readonly project: string | null;
  }>`SELECT 'native' AS kind,claim_id AS id,worktree_path AS target,project_cwd AS project
      FROM native_creation_intents WHERE worktree_path IS NOT NULL
    UNION ALL SELECT 'workspace',claim_id,worktree_path,NULL FROM jones_native_workspace_admissions
    UNION ALL SELECT 'deletion',effect_id,worktree_path,NULL FROM jones_deletion_worktree_admissions WHERE state!='completed'
    UNION ALL SELECT 'legacy',lease_id,resource_path,NULL FROM worktree_ownership_leases
    ORDER BY kind,id,target`;
  const prepareForeign = Effect.gen(function* () {
    const rows = yield* foreignRows();
    const keys: string[] = [];
    for (const row of rows) {
      keys.push(yield* canonicalPath(row.target, true));
      if (row.project !== null) keys.push(yield* canonicalPath(row.project, false));
    }
    return { rows: canonical(rows), keys };
  });
  const assertForeign = Effect.fn("CheckoutConflictIntegration.assertForeign")(function* (
    snapshot: Effect.Success<typeof prepareForeign>,
    keys: readonly string[],
  ) {
    if (canonical(yield* foreignRows()) !== snapshot.rows)
      return yield* new CheckoutError({ reason: "stale" });
    if (keys.some((key) => snapshot.keys.some((other) => Target.overlaps(key, other, path.sep))))
      return yield* new CheckoutError({ reason: "conflict" });
  });
  const newKeys = Effect.fn("CheckoutConflictIntegration.newKeys")(function* (
    inputs: readonly (Checkout.CommandIdentity & { readonly target: Checkout.Accept["target"] })[],
  ) {
    const keys: string[] = [];
    for (const input of inputs) {
      const replay = yield* checkout.replay(input);
      if (
        replay.kind !== "accepted" ||
        !replay.admissions.some((row) => row.checkout_key === input.target.checkoutKey)
      )
        keys.push(input.target.checkoutKey);
    }
    return keys;
  });
  // Freeze outside SQL; the returned closure grants no acceptance authority and
  // must run inside the transaction that owns the final accepted receipt.
  const prepareOrdinary = Effect.fn("CheckoutConflictIntegration.prepareOrdinary")(function* (
    inputs: readonly OrdinaryPreflight[],
  ) {
    const expected = new Map<
      string,
      { invariant: string; birth: string | null; birthEventId: string }
    >();
    for (const input of inputs) {
      if (
        expected.has(input.admissionId) ||
        (input.birth !== null && input.birth.eventId !== input.birthEventId)
      )
        return yield* new CheckoutError({ reason: "identity" });
      expected.set(input.admissionId, {
        invariant: invariant(input),
        birth: input.birth === null ? null : canonical(input.birth),
        birthEventId: input.birthEventId,
      });
    }
    const keys = yield* newKeys(inputs);
    const snapshot = keys.length === 0 ? null : yield* prepareForeign;
    const commit: CommitOrdinary = Effect.fn("CheckoutConflictIntegration.commit")(function* (
      final,
    ) {
      if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
        return yield* new CheckoutError({ reason: "unavailable" });
      const seen = new Set<string>();
      for (const input of final) {
        const frozen = expected.get(input.admissionId);
        if (
          seen.has(input.admissionId) ||
          frozen === undefined ||
          invariant(input) !== frozen.invariant ||
          (frozen.birth === null
            ? input.birth.eventId !== frozen.birthEventId
            : canonical(input.birth) !== frozen.birth)
        )
          return yield* new CheckoutError({ reason: "identity" });
        seen.add(input.admissionId);
      }
      if (seen.size !== expected.size) return yield* new CheckoutError({ reason: "identity" });
      if (snapshot !== null) yield* assertForeign(snapshot, keys);
      return yield* checkout.acceptMany(final);
    }, Effect.mapError(checkoutError));
    return commit;
  }, Effect.mapError(checkoutError));

  const assertNoOrdinary = Effect.fn("CheckoutConflictIntegration.assertNoOrdinary")(function* (
    keys: readonly string[],
  ) {
    for (const key of keys) {
      if ((yield* checkout.readConflict(key)).some((conflict) => conflict.kind === "ordinary"))
        return yield* new CheckoutError({ reason: "conflict" });
    }
  });
  const nativeKeys = Effect.fn("CheckoutConflictIntegration.nativeKeys")(function* (
    project: string,
    worktree: string,
  ) {
    const projectKey = yield* canonicalPath(project, false);
    const worktreeKey = yield* canonicalPath(worktree, true);
    // Never rewrite the immutable native authority binding to a different physical target.
    if (worktreeKey !== worktree || (yield* observation.directory(project)).realPath !== project)
      return yield* new CheckoutError({ reason: "identity" });
    return [projectKey, worktreeKey];
  });
  const claim: Native.NativeCreationRepository["Service"]["claim"] = Effect.fn(
    "CheckoutConflictIntegration.claim",
  )(
    function* (input, authorize) {
      const keys = yield* nativeKeys(input.resources.projectCwd, input.resources.worktreePath);
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* assertNoOrdinary(keys);
          return yield* native.claim(input, authorize);
        }),
      );
    },
    Effect.mapError((cause) =>
      // The existing authority owner retains its typed denial and all 100/101 fences.
      isNativeError(cause) || isNativeAuthorityError(cause) ? cause : nativeError(cause),
    ),
  );
  const admitWorkspace: NonNullable<Native.NativeCreationRepository["Service"]["admitWorkspace"]> =
    Effect.fn("CheckoutConflictIntegration.admitWorkspace")(function* (claimId, basis) {
      if (!native.admitWorkspace)
        return yield* nativeError(new CheckoutError({ reason: "unavailable" }));
      const keys = yield* nativeKeys(basis.projectCwd, basis.worktreePath);
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* assertNoOrdinary(keys);
          return yield* native.admitWorkspace!(claimId, basis);
        }),
      );
    }, Effect.mapError(nativeError));
  const deletionKey = Effect.fn("CheckoutConflictIntegration.deletionKey")(function* (target: {
    readonly path: string;
  }) {
    const key = yield* canonicalPath(target.path, true);
    if (key !== target.path) return yield* new CheckoutError({ reason: "identity" });
    return key;
  });
  return CheckoutConflictIntegration.of({
    prepareOrdinary,
    prepareHandoff: Effect.fn("CheckoutConflictIntegration.prepareHandoff")(function* (input) {
      const snapshot = yield* prepareForeign;
      const expected = canonical(input);
      return sql
        .withTransaction(
          Effect.gen(function* () {
            if (canonical(input) !== expected)
              return yield* new CheckoutError({ reason: "identity" });
            yield* assertForeign(snapshot, [input.next.target.checkoutKey]);
            return yield* checkout.handoff(input);
          }),
        )
        .pipe(Effect.mapError(checkoutError));
    }, Effect.mapError(checkoutError)),
    native: { ...native, claim, admitWorkspace },
    deletion: {
      ...deletion,
      start: Effect.fn("CheckoutConflictIntegration.start")(function* (input) {
        const key = yield* deletionKey(input.target);
        return yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* assertNoOrdinary([key]);
            return yield* deletion.start(input);
          }),
        );
      }, Effect.mapError(deletionError)),
      revalidate: Effect.fn("CheckoutConflictIntegration.revalidate")(function* (
        captured,
        ordinal,
        rules,
        live,
      ) {
        const key = yield* deletionKey(captured.target);
        return yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* assertNoOrdinary([key]);
            return yield* deletion.revalidate(captured, ordinal, rules, live);
          }),
        );
      }, Effect.mapError(deletionError)),
    },
  });
});

export const layer = Layer.effect(CheckoutConflictIntegration, make);
