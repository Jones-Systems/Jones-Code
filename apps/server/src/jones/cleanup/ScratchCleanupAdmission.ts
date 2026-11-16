import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Checkout from "../ordinaryCheckout/OrdinaryCheckout.ts";
import * as Target from "../ordinaryCheckout/CheckoutTarget.ts";
import { canonical, DirectoryIdentity, type Owner } from "../ordinaryCheckout/CheckoutProtocol.ts";
import { inventoryRead } from "./InventoryRead.ts";

export interface ScratchTarget {
  readonly path: string;
  readonly threadId: string;
  readonly projectId: string;
  readonly projectRoot: string;
  readonly storedPath: string;
}
export interface OrdinaryScratchEvidence {
  readonly target: ScratchTarget;
  readonly directory: Target.Directory;
  readonly owner: Owner;
  readonly reservationSnapshot: string;
}
export type ScratchClassification =
  | { readonly status: "ordinary"; readonly evidence: OrdinaryScratchEvidence }
  | { readonly status: "held"; readonly reason: string; readonly cause?: unknown };
export class ScratchCleanupError extends Schema.TaggedError<ScratchCleanupError>()("ScratchCleanupError", {
  reason: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}

/** Only the owning writer/native-start integration may implement this port.
 * It must exclude native-start and lease creation for this physical identity
 * through the removal effect, revalidate the exact evidence, and retain unknown
 * outcomes. A veto or a read-only reservation snapshot cannot implement it.
 * There is deliberately no default or permissive layer. */
export class ScratchCleanupExclusion extends Context.Service<ScratchCleanupExclusion, {
  readonly removeOrdinary: (input: {
    readonly evidence: OrdinaryScratchEvidence;
    readonly revalidate: Effect.Effect<void, ScratchCleanupError>;
    readonly remove: Effect.Effect<void, ScratchCleanupError>;
  }) => Effect.Effect<{ readonly status: "removed" } | { readonly status: "held"; readonly reason: string }, ScratchCleanupError>;
}>()("t3/jones/cleanup/ScratchCleanupExclusion") {}

const decodeWitness = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Struct({
  version: Schema.Literal(1), directory: DirectoryIdentity,
  gitDirectory: Schema.Null, gitDirectoryIdentity: Schema.Null, gitCommonIdentity: Schema.Null,
})), { onExcessProperty: "error" });

/** Preparation resolves physical aliases outside the writer transaction. The
 * final classifier reads and compares current authority on its owning writer;
 * neither inventory priority nor reader-pool routing applies to this guard. */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const path = yield* Path.Path;
  const checkout = yield* Effect.serviceOption(Checkout.OrdinaryCheckout);
  const observation = yield* Effect.serviceOption(Target.CheckoutTargetObservation);
  const rows = () => sql<{ kind: string; target: string }>`
    SELECT 'native' AS kind,worktree_path AS target FROM native_creation_intents WHERE worktree_path IS NOT NULL
    UNION ALL SELECT 'native',worktree_path FROM jones_native_workspace_admissions
    UNION ALL SELECT 'legacy',resource_path FROM worktree_ownership_leases
    UNION ALL SELECT 'deletion',worktree_path FROM jones_deletion_worktree_admissions WHERE state!='completed'
    UNION ALL SELECT 'lifecycle',worktree FROM jones_workspace_lifecycle_bindings
    UNION ALL SELECT 'lifecycle',worktree FROM jones_workspace_lifecycle_transitions
    UNION ALL SELECT 'runtime',canonical_path FROM jones_workspace_runtime_uses
    UNION ALL SELECT 'retirement',worktree FROM jones_workspace_retirement_state
    ORDER BY kind,target LIMIT 10001`;
  const held = (reason: string, cause?: unknown): ScratchClassification => ({ status: "held", reason, cause });
  const classify = (target: ScratchTarget): Effect.Effect<ScratchClassification> => Effect.gen(function* () {
    if (Option.isNone(checkout) || Option.isNone(observation))
      return held("scratch cleanup ownership classification unavailable");
    if (!path.isAbsolute(target.path) || path.normalize(target.path) !== target.path)
      return held("scratch cleanup path identity is unknown");
    const directory = yield* observation.value.directory(target.path);
    if (directory.realPath !== target.path) return held("scratch cleanup path identity changed");
    const project = yield* observation.value.directory(target.projectRoot);
    if ((yield* observation.value.git(directory.realPath)) !== null)
      return held("scratch cleanup target is a Git checkout");
    const prepared = yield* rows().pipe(inventoryRead);
    if (prepared.length > 10000) return held("scratch cleanup reservation inventory exceeds the limit");
    const physical: { kind: string; target: string }[] = [];
    for (const row of prepared) {
      if (typeof row.target !== "string" || !path.isAbsolute(row.target))
        return held("scratch cleanup reservation identity is unknown");
      // A native claim can precede its directory. Resolve the existing parent;
      // an unavailable/symlinked parent remains unknown instead of ordinary.
      const resolved = yield* observation.value.directory(row.target).pipe(
        Effect.map((observed) => observed.realPath),
        Effect.catchIf((error) => typeof error.cause === "object" && error.cause !== null &&
          "code" in error.cause && error.cause.code === "ENOENT", () => observation.value.directory(path.dirname(row.target)).pipe(
          Effect.map((parent) => path.join(parent.realPath, path.basename(row.target))),
        )),
      );
      physical.push({ kind: row.kind, target: resolved });
    }
    const snapshot = canonical(prepared);
    return yield* sql.withTransaction(Effect.gen(function* () {
      if (canonical(yield* rows()) !== snapshot)
        return held("scratch cleanup reservations changed during classification");
      const conflict = physical.find((row) => Target.overlaps(target.path, row.target, path.sep));
      if (conflict) return held(`scratch cleanup overlaps ${conflict.kind} ownership`);
      const conflicts = yield* checkout.value.readConflict(target.path);
      const foreign = conflicts.find((entry) => entry.kind !== "ordinary" || entry.checkoutKey !== target.path);
      if (foreign) return held(`scratch cleanup overlaps ${foreign.kind} ownership`);
      const currentThreads = yield* sql<{ projectId: string; worktreePath: string | null }>`
        SELECT project_id AS "projectId",json_extract(payload_json,'$.worktreePath') AS "worktreePath"
        FROM orchestration_v2_projection_threads WHERE thread_id=${target.threadId}`;
      if (currentThreads.length !== 1 || currentThreads[0]!.projectId !== target.projectId ||
          currentThreads[0]!.worktreePath !== target.storedPath)
        return held("scratch cleanup thread identity changed");
      const owner = yield* checkout.value.readOwner(target.path);
      // Empty metadata, branch=null and absence of turns are never ordinary proof.
      if (owner === null || owner.state !== "owned" || owner.checkout_key !== target.path ||
          owner.owner_thread_id !== target.threadId || owner.project_root !== project.realPath ||
          owner.git_common_directory !== null || owner.branch !== null)
        return held("scratch cleanup lacks positive ordinary ownership");
      const witness = yield* decodeWitness(owner.directory_identity_json);
      if (canonical(witness.directory) !== canonical(directory.identity))
        return held("scratch cleanup directory incarnation changed");
      return { status: "ordinary" as const, evidence: { target, directory, owner, reservationSnapshot: snapshot } };
    }));
  }).pipe(Effect.catch((cause) => Effect.succeed(held("scratch cleanup ownership is unknown", cause))));
  const remove = (input: {
    readonly target: ScratchTarget;
    readonly revalidate: Effect.Effect<void, ScratchCleanupError>;
    readonly remove: Effect.Effect<void, ScratchCleanupError>;
  }): Effect.Effect<string | null, ScratchCleanupError> => Effect.gen(function* () {
    const initial = yield* classify(input.target);
    if (initial.status === "held") return initial.reason;
    const exclusion = yield* Effect.serviceOption(ScratchCleanupExclusion);
    if (Option.isNone(exclusion)) return "scratch cleanup native-start and lease exclusion unavailable";
    const revalidate = Effect.gen(function* () {
      yield* input.revalidate;
      const current = yield* classify(input.target);
      if (current.status !== "ordinary" || canonical(current.evidence) !== canonical(initial.evidence))
        return yield* new ScratchCleanupError({ reason: current.status === "held" ? current.reason : "scratch cleanup ownership changed" });
    });
    const result = yield* exclusion.value.removeOrdinary({ evidence: initial.evidence, revalidate, remove: input.remove });
    return result.status === "held" ? result.reason : null;
  });
  return { classify, remove };
});
