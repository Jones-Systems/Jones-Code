import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Deletion from "../cleanup/DeletionAdmission.ts";
import * as Native from "../nativeCreation/NativeCreationRepository.ts";
import * as NativeAuthority from "../nativeCreation/NativeCreationAuthority.ts";
import * as Physical from "../nativeCreation/NativeWorkspacePhysicalPorts.ts";
import { canonical, CheckoutError } from "./CheckoutProtocol.ts";
import * as Target from "./CheckoutTarget.ts";
import * as Checkout from "./OrdinaryCheckout.ts";

// Inactive composition only. All three receiving owners must use these adapters together.
// No layer here installs an executor or provides ordinary acceptance/retirement authority.
export class CheckoutConflictIntegration extends Context.Service<
  CheckoutConflictIntegration,
  {
    readonly prepareOrdinary: (
      inputs: readonly Checkout.Accept[],
    ) => Effect.Effect<ReturnType<Checkout.OrdinaryCheckout["Service"]["acceptMany"]>, CheckoutError>;
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

  const canonicalPath = (value: string, absentAllowed: boolean) =>
    Effect.gen(function* () {
      if (!path.isAbsolute(value) || path.normalize(value) !== value)
        return yield* new CheckoutError({ reason: "identity" });
      const observed = yield* files.inspect(value);
      if (observed === null) {
        if (!absentAllowed) return yield* new CheckoutError({ reason: "unavailable" });
        const parent = yield* observation.directory(path.dirname(value));
        return path.join(parent.realPath, path.basename(value));
      }
      if (observed.kind !== "directory")
        return yield* new CheckoutError({ reason: "identity" });
      const directory = yield* observation.directory(value);
      const git = yield* observation.git(directory.realPath);
      return git === null ? directory.realPath : git.root;
    }).pipe(Effect.mapError(checkoutError));

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
  const assertForeign = (snapshot: Effect.Success<typeof prepareForeign>, keys: readonly string[]) =>
    Effect.gen(function* () {
      if (canonical(yield* foreignRows()) !== snapshot.rows)
        return yield* new CheckoutError({ reason: "stale" });
      if (keys.some((key) => snapshot.keys.some((other) => Target.overlaps(key, other, path.sep))))
        return yield* new CheckoutError({ reason: "conflict" });
    });
  const newKeys = (inputs: readonly Checkout.Accept[]) =>
    Effect.gen(function* () {
      const keys: string[] = [];
      for (const input of inputs) {
        const replay = yield* checkout.replay(input);
        if (
          replay.kind !== "accepted" ||
          !replay.admissions.some((row) => row.checkout_key === input.target.checkoutKey)
        ) keys.push(input.target.checkoutKey);
      }
      return keys;
    });
  // The caller prepares outside EventSink, then runs the returned effect in the
  // accepted receipt transaction. The prepared effect grants no acceptance authority.
  const prepareOrdinary = (inputs: readonly Checkout.Accept[]) =>
    Effect.gen(function* () {
      const expected = canonical(inputs);
      const keys = yield* newKeys(inputs);
      const snapshot = keys.length === 0 ? null : yield* prepareForeign;
      return sql.withTransaction(Effect.gen(function* () {
        if (canonical(inputs) !== expected)
          return yield* new CheckoutError({ reason: "identity" });
        if (snapshot !== null) yield* assertForeign(snapshot, keys);
        return yield* checkout.acceptMany(inputs);
      })).pipe(Effect.mapError(checkoutError));
    }).pipe(Effect.mapError(checkoutError));

  const assertNoOrdinary = (keys: readonly string[]) =>
    Effect.gen(function* () {
      for (const key of keys) {
        if ((yield* checkout.readConflict(key)).some((conflict) => conflict.kind === "ordinary"))
          return yield* new CheckoutError({ reason: "conflict" });
      }
    });
  const nativeKeys = (project: string, worktree: string) =>
    Effect.gen(function* () {
      const projectKey = yield* canonicalPath(project, false);
      const worktreeKey = yield* canonicalPath(worktree, true);
      // Never rewrite the immutable native authority binding to a different physical target.
      if (worktreeKey !== worktree || (yield* observation.directory(project)).realPath !== project)
        return yield* new CheckoutError({ reason: "identity" });
      return [projectKey, worktreeKey];
    });
  const claim: Native.NativeCreationRepository["Service"]["claim"] = (input, authorize) =>
    Effect.gen(function* () {
      const keys = yield* nativeKeys(input.resources.projectCwd, input.resources.worktreePath);
      return yield* sql.withTransaction(Effect.gen(function* () {
        yield* assertNoOrdinary(keys);
        return yield* native.claim(input, authorize);
      }));
    }).pipe(Effect.mapError((cause) =>
      // The existing authority owner retains its typed denial and all 100/101 fences.
      isNativeError(cause) || isNativeAuthorityError(cause)
        ? cause
        : nativeError(cause),
    ));
  const admitWorkspace: NonNullable<Native.NativeCreationRepository["Service"]["admitWorkspace"]> =
    (claimId, basis) => Effect.gen(function* () {
      if (!native.admitWorkspace)
        return yield* nativeError(new CheckoutError({ reason: "unavailable" }));
      const keys = yield* nativeKeys(basis.projectCwd, basis.worktreePath);
      return yield* sql.withTransaction(Effect.gen(function* () {
        yield* assertNoOrdinary(keys);
        return yield* native.admitWorkspace!(claimId, basis);
      }));
    }).pipe(Effect.mapError(nativeError));
  const deletionKey = (target: { readonly path: string }) =>
    Effect.gen(function* () {
      const key = yield* canonicalPath(target.path, true);
      if (key !== target.path) return yield* new CheckoutError({ reason: "identity" });
      return key;
    });
  return CheckoutConflictIntegration.of({
    prepareOrdinary,
    prepareHandoff: (input) => Effect.gen(function* () {
      const snapshot = yield* prepareForeign;
      const expected = canonical(input);
      return sql.withTransaction(Effect.gen(function* () {
        if (canonical(input) !== expected)
          return yield* new CheckoutError({ reason: "identity" });
        yield* assertForeign(snapshot, [input.next.target.checkoutKey]);
        return yield* checkout.handoff(input);
      })).pipe(Effect.mapError(checkoutError));
    }).pipe(Effect.mapError(checkoutError)),
    native: { ...native, claim, admitWorkspace },
    deletion: {
      ...deletion,
      start: (input) => Effect.gen(function* () {
        const key = yield* deletionKey(input.target);
        return yield* sql.withTransaction(Effect.gen(function* () {
          yield* assertNoOrdinary([key]);
          return yield* deletion.start(input);
        }));
      }).pipe(Effect.mapError(deletionError)),
      revalidate: (captured, ordinal, rules, live) => Effect.gen(function* () {
        const key = yield* deletionKey(captured.target);
        return yield* sql.withTransaction(Effect.gen(function* () {
          yield* assertNoOrdinary([key]);
          return yield* deletion.revalidate(captured, ordinal, rules, live);
        }));
      }).pipe(Effect.mapError(deletionError)),
    },
  });
});

export const layer = Layer.effect(CheckoutConflictIntegration, make);
