// @effect-diagnostics nodeBuiltinImport:off -- Original creation coverage observes no-follow physical metadata outside application transactions.
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { NativeCreationStoredIntent } from "../nativeCreation/NativeCreationRepository.ts";
import type {
  NativeWorkspaceBasis,
  NativeWorkspaceProof,
  NativeWorkspaceVerified,
} from "../nativeCreation/NativeCreationWorkspaceTypes.ts";
import {
  HeldRuntimeOwner,
  assertCheckedDatabaseOwner,
  bindHeldDatabaseOwner,
  type CheckedDatabaseOwnerToken,
  type HeldRuntimeOwnerToken,
} from "./CheckedRuntimeOwner.ts";
import {
  RuntimeUseCreationAdmission,
  type RuntimeUseCreationMaterial,
} from "./RuntimeUseCreationProtocol.ts";
import { readRuntimeUseInstallation } from "./RuntimeUseInstallation.ts";
import { canonical, digest, RuntimeUseError, runtimeUseError } from "./RuntimeUseProtocol.ts";
import { captureRuntimeUseWorkspace } from "./RuntimeUseWorkspace.ts";
import { hasWorkspaceLeaseProof } from "./WorkspaceLeaseProof.ts";

interface OriginalScope {
  readonly fiberId: number;
  readonly active: () => boolean;
}
class CreationScope extends Context.Service<CreationScope, OriginalScope>()(
  "t3/jones/cleanup/RuntimeUseCreationCapture/CreationScope",
) {}
interface FreshCapture {
  readonly scope: OriginalScope;
  readonly sql: SqlClient.SqlClient;
  readonly owner: CheckedDatabaseOwnerToken;
  readonly fact: RuntimeUseCreationAdmission;
  readonly factSha256: string;
  created: CreatedMaterial | null;
  consumed: boolean;
}
type CreatedMaterial = Pick<
  RuntimeUseCreationMaterial,
  "target" | "dotGit" | "gitDirectory" | "gitCommon"
>;
type PreparedMaterial = Omit<
  RuntimeUseCreationMaterial,
  "worktreeStartSha256" | "worktreeCompletionSha256"
>;
const material = new WeakMap<
  NativeWorkspaceVerified,
  {
    readonly fresh: FreshCapture;
    readonly fact: PreparedMaterial;
    readonly factSha256: string;
    readonly verifiedSha256: string;
    consumed: boolean;
  }
>();
const fresh = new WeakMap<NativeWorkspaceBasis, FreshCapture>();
const fail = (reason: string) => Effect.fail(new RuntimeUseError({ reason }));
const parseLegacyIdentity = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      device: Schema.NonEmptyString,
      inode: Schema.NonEmptyString,
      birthtime: Schema.Number.check(Schema.isFinite()),
    }),
  ),
  { onExcessProperty: "error" },
);
const decodeAdmission = Schema.decodeUnknownEffect(RuntimeUseCreationAdmission, {
  onExcessProperty: "error",
});

/** Lifetime only; no creation grant, process retirement or replay permission. */
export const withRuntimeUseCreationScope = Effect.fn("RuntimeUseCreationCapture.withScope")(
  function* <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.fn.Return<A, E, R> {
    const fiberId = yield* Effect.fiberId;
    let active = true;
    return yield* effect.pipe(
      Effect.provideService(CreationScope, { fiberId, active: () => active }),
      Effect.ensuring(
        Effect.sync(() => {
          active = false;
        }),
      ),
    );
  },
);

// Keep released native receipt bytes unchanged. Bracket their numeric-stat
// identity with fresh bigint observations; unsupported/lossy identities refuse.
const observeOriginalDirectory = Effect.fn("RuntimeUseCreationCapture.directory")(function* (
  path: string,
  original: string,
) {
  const legacy = yield* parseLegacyIdentity(original);
  const before = yield* captureRuntimeUseWorkspace(path);
  const observed = yield* Effect.tryPromise({
    try: async () => {
      const stat = await NodeFSP.lstat(path);
      if (!stat.isDirectory() || stat.isSymbolicLink())
        throw new RuntimeUseError({ reason: "runtime_creation_directory_kind" });
      return { device: String(stat.dev), inode: String(stat.ino), birthtime: stat.birthtimeMs };
    },
    catch: runtimeUseError,
  });
  if (
    canonical(legacy) !== original ||
    canonical(observed) !== original ||
    before.directoryIdentity.device !== observed.device ||
    before.directoryIdentity.inode !== observed.inode ||
    !/^[1-9][0-9]*$/.test(before.directoryIdentity.birth)
  )
    return yield* fail("runtime_creation_original_directory_changed");
  yield* before.revalidate;
  return { canonicalPath: before.canonicalPath, identity: before.directoryIdentity };
});

const parseMaterialBirth = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      target: Schema.NonEmptyString,
      dotGit: Schema.NonEmptyString,
      gitDirectory: Schema.NonEmptyString,
    }),
  ),
  { onExcessProperty: "error" },
);
const observeOriginalGitFile = Effect.fn("RuntimeUseCreationCapture.gitFile")(function* (
  path: string,
  original: string,
) {
  const legacy = yield* parseLegacyIdentity(original);
  if (canonical(legacy) !== original) return yield* fail("runtime_creation_git_file_encoding");
  return yield* Effect.tryPromise({
    try: async () => {
      if (
        !NodePath.isAbsolute(path) ||
        NodePath.normalize(path) !== path ||
        (await NodeFSP.realpath(path)) !== path
      )
        throw new RuntimeUseError({ reason: "runtime_creation_git_file_alias" });
      const before = await NodeFSP.lstat(path, { bigint: true });
      if (
        !before.isFile() ||
        before.isSymbolicLink() ||
        before.nlink !== 1n ||
        before.size > 4096n ||
        before.ino === 0n ||
        before.birthtimeNs <= 0n
      )
        throw new RuntimeUseError({ reason: "runtime_creation_git_file_kind" });
      const handle = await NodeFSP.open(
        path,
        NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
      );
      let pointer: string;
      try {
        const opened = await handle.stat({ bigint: true });
        if (
          !opened.isFile() ||
          opened.dev !== before.dev ||
          opened.ino !== before.ino ||
          opened.birthtimeNs !== before.birthtimeNs ||
          opened.size > 4096n ||
          opened.nlink !== 1n
        )
          throw new RuntimeUseError({ reason: "runtime_creation_git_file_handle_changed" });
        pointer = (await handle.readFile("utf8")).trim();
        const after = await handle.stat({ bigint: true });
        if (
          after.dev !== before.dev ||
          after.ino !== before.ino ||
          after.birthtimeNs !== before.birthtimeNs ||
          after.size > 4096n ||
          after.nlink !== 1n
        )
          throw new RuntimeUseError({ reason: "runtime_creation_git_file_changed_during_read" });
      } finally {
        await handle.close();
      }
      const numeric = await NodeFSP.lstat(path);
      const last = await NodeFSP.lstat(path, { bigint: true });
      if (
        (await NodeFSP.realpath(path)) !== path ||
        !last.isFile() ||
        last.nlink !== 1n ||
        before.dev !== last.dev ||
        before.ino !== last.ino ||
        before.birthtimeNs !== last.birthtimeNs ||
        String(numeric.dev) !== String(before.dev) ||
        String(numeric.ino) !== String(before.ino) ||
        canonical({
          device: String(numeric.dev),
          inode: String(numeric.ino),
          birthtime: numeric.birthtimeMs,
        }) !== original ||
        !/^gitdir: [^\r\n\0]+$/.test(pointer)
      )
        throw new RuntimeUseError({ reason: "runtime_creation_original_git_file_changed" });
      return {
        entry: {
          canonicalPath: path,
          identity: {
            version: 1 as const,
            device: String(before.dev),
            inode: String(before.ino),
            birth: String(before.birthtimeNs),
          },
        },
        pointer: pointer.slice(8),
      };
    },
    catch: runtimeUseError,
  });
});

const observeCreatedMaterial = Effect.fn("RuntimeUseCreationCapture.material")(function* (
  basis: NativeWorkspaceBasis,
  proof: NativeWorkspaceProof,
) {
  if (
    proof.worktreePath !== basis.worktreePath ||
    proof.gitCommonDirectory !== basis.gitCommonDirectory ||
    proof.physicalGitIdentity !== basis.physicalGitIdentity
  )
    return yield* fail("runtime_creation_material_basis_changed");
  const birth = yield* parseMaterialBirth(proof.pathBirth);
  const target = yield* observeOriginalDirectory(proof.worktreePath, birth.target);
  const dotGit = yield* observeOriginalGitFile(
    NodePath.join(proof.worktreePath, ".git"),
    birth.dotGit,
  );
  const gitPath = NodePath.resolve(proof.worktreePath, dotGit.pointer);
  const relative = NodePath.relative(NodePath.join(basis.gitCommonDirectory, "worktrees"), gitPath);
  if (!relative || relative.startsWith("..") || NodePath.isAbsolute(relative))
    return yield* fail("runtime_creation_git_directory_not_original_namespace");
  return {
    target,
    dotGit: dotGit.entry,
    gitDirectory: yield* observeOriginalDirectory(gitPath, birth.gitDirectory),
    gitCommon: yield* observeOriginalDirectory(basis.gitCommonDirectory, basis.physicalGitIdentity),
  } satisfies CreatedMaterial;
});

const validatePhysicalCapture = Effect.fn("RuntimeUseCreationCapture.validatePhysical")(function* (
  captured: FreshCapture,
  basis: NativeWorkspaceBasis,
) {
  const scope = yield* Effect.serviceOption(CreationScope);
  if (
    !captured.consumed ||
    !captured.scope.active() ||
    Option.isNone(scope) ||
    scope.value !== captured.scope ||
    captured.scope.fiberId !== (yield* Effect.fiberId) ||
    digest(captured.fact) !== captured.factSha256 ||
    digest(basis) !== captured.fact.workspaceBasisSha256
  )
    return yield* fail("runtime_creation_original_material_capture_changed");
  if (Option.isSome(yield* Effect.serviceOption(captured.sql.transactionService)))
    return yield* fail("runtime_creation_material_observation_inside_transaction");
  if (!(yield* hasWorkspaceLeaseProof(basis.worktreePath)))
    return yield* fail("runtime_creation_material_exclusion_lost");
  yield* Effect.try({
    try: () => assertCheckedDatabaseOwner(captured.owner),
    catch: runtimeUseError,
  });
});

/** Actual Git afterEffect(worktree.add), before config/submodule/setup writers.
 * Retain the FIRST material identities; later observations cannot replace them. */
export const observeRuntimeUseCreatedMaterial = Effect.fn(
  "RuntimeUseCreationCapture.firstMaterial",
)(function* (basis: NativeWorkspaceBasis, proof: NativeWorkspaceProof) {
  const captured = fresh.get(basis);
  if (captured === undefined || !captured.consumed) return;
  yield* validatePhysicalCapture(captured, basis);
  if (captured.created !== null) return yield* fail("runtime_creation_material_already_observed");
  const observed = yield* observeCreatedMaterial(basis, proof);
  if (canonical(observed.gitCommon) !== canonical(captured.fact.gitCommon))
    return yield* fail("runtime_creation_original_common_directory_changed");
  yield* validatePhysicalCapture(captured, basis);
  captured.created = observed;
}, Effect.mapError(runtimeUseError));

/** Called after the real original preparation verifies every configured stage.
 * It observes physical metadata only and cannot manufacture a native receipt. */
export const captureRuntimeUseCreationMaterial = Effect.fn(
  "RuntimeUseCreationCapture.verifiedMaterial",
)(function* (verified: NativeWorkspaceVerified) {
  const captured = fresh.get(verified.basis);
  if (captured === undefined || !captured.consumed) return;
  yield* validatePhysicalCapture(captured, verified.basis);
  if (verified.claimId !== captured.fact.claimId || captured.created === null)
    return yield* fail("runtime_creation_first_material_missing");
  const current = yield* observeCreatedMaterial(verified.basis, verified.proof);
  if (canonical(current) !== canonical(captured.created))
    return yield* fail("runtime_creation_first_material_replaced");
  yield* validatePhysicalCapture(captured, verified.basis);
  const fact: PreparedMaterial = {
    schema: "jones.workspace-runtime-creation-material/v1",
    claimId: verified.claimId,
    admissionSha256: captured.factSha256,
    workspaceVerifiedSha256: digest(verified),
    ...current,
  };
  material.set(verified, {
    fresh: captured,
    fact,
    factSha256: digest(fact),
    verifiedSha256: digest(verified),
    consumed: false,
  });
}, Effect.mapError(runtimeUseError));

/** Called solely after the actual native physical port has independently
 * established absent target/registration/branch material for this exact basis.
 * Unsupported contexts retain the original feature's unavailable cleanup path. */
const observeFreshCreation = Effect.fn("RuntimeUseCreationCapture.observeFresh")(function* (
  intent: NativeCreationStoredIntent,
  basis: NativeWorkspaceBasis,
  binding: { readonly sql: SqlClient.SqlClient; readonly owner: HeldRuntimeOwnerToken },
) {
  const scope = yield* Effect.serviceOption(CreationScope);
  if (Option.isNone(scope)) return;
  if (!scope.value.active() || scope.value.fiberId !== (yield* Effect.fiberId))
    return yield* fail("runtime_creation_scope_changed");
  const sql = binding.sql;
  if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
    return yield* fail("runtime_creation_physical_observation_inside_transaction");
  if (!(yield* hasWorkspaceLeaseProof(basis.worktreePath))) return;
  const installation = yield* readRuntimeUseInstallation().pipe(
    Effect.provideService(SqlClient.SqlClient, sql),
  );
  if (installation === null) return;
  const owner = yield* bindHeldDatabaseOwner(binding.owner).pipe(
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.mapError(runtimeUseError),
  );
  yield* Effect.try({ try: () => assertCheckedDatabaseOwner(owner), catch: runtimeUseError });
  if (
    basis.worktreePath !== intent.resources.worktreePath ||
    basis.projectCwd !== intent.resources.projectCwd ||
    basis.bootId !== intent.claimedBootId
  )
    return yield* fail("runtime_creation_original_basis_changed");
  const absent = yield* Effect.tryPromise({
    try: async () => {
      if (
        !NodePath.isAbsolute(basis.worktreePath) ||
        NodePath.normalize(basis.worktreePath) !== basis.worktreePath
      )
        throw new RuntimeUseError({ reason: "runtime_creation_target_not_canonical" });
      try {
        await NodeFSP.lstat(basis.worktreePath);
        return false;
      } catch (cause) {
        if (
          typeof cause === "object" &&
          cause !== null &&
          "code" in cause &&
          cause.code === "ENOENT"
        )
          return true;
        throw cause;
      }
    },
    catch: runtimeUseError,
  });
  if (!absent) return yield* fail("runtime_creation_target_was_not_absent");
  const fact = yield* decodeAdmission({
    schema: "jones.workspace-runtime-creation-admission/v1",
    claimId: intent.claimId,
    commandId: intent.commandId,
    rootThreadId: intent.threadId,
    canonicalPath: basis.worktreePath,
    installationSha256: digest(installation),
    nativeIntentSha256: digest(intent),
    workspaceBasisSha256: digest(basis),
    worktreeEffectId: digest({ claimId: intent.claimId, stage: "worktree", basis }),
    project: yield* observeOriginalDirectory(basis.projectCwd, basis.projectBirth),
    parent: yield* observeOriginalDirectory(
      NodePath.dirname(basis.worktreePath),
      basis.parentBirth,
    ),
    gitCommon: yield* observeOriginalDirectory(basis.gitCommonDirectory, basis.physicalGitIdentity),
  });
  yield* Effect.try({ try: () => assertCheckedDatabaseOwner(owner), catch: runtimeUseError });
  if (!(yield* hasWorkspaceLeaseProof(basis.worktreePath)))
    return yield* fail("runtime_creation_workspace_exclusion_lost");
  fresh.set(basis, {
    scope: scope.value,
    sql,
    owner,
    fact,
    factSha256: digest(fact),
    created: null,
    consumed: false,
  });
}, Effect.mapError(runtimeUseError));

// Capture the actual service constructor's original owner/client, as the other
// runtime-use guards do. A later caller cannot substitute its own current DB or
// renewal/replacement owner when invoking the returned physical port.
export const makeRuntimeUseCreationObserver = Effect.gen(function* () {
  const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
  const owner = yield* Effect.serviceOption(HeldRuntimeOwner);
  const observe: (
    intent: NativeCreationStoredIntent,
    basis: NativeWorkspaceBasis,
  ) => Effect.Effect<void, RuntimeUseError> =
    Option.isNone(sql) || Option.isNone(owner)
      ? () => Effect.void
      : (intent, basis) =>
          observeFreshCreation(intent, basis, { sql: sql.value, owner: owner.value.token });
  return observe;
});

/** The actual native repository calls this only in the transaction inserting a
 * genuinely NEW admission. No observation is issued or refreshed inside SQL. */
export const withCapturedRuntimeUseFreshCreation = Effect.fn(
  "RuntimeUseCreationCapture.consumeFresh",
)(function* <E, R>(
  intent: NativeCreationStoredIntent,
  basis: NativeWorkspaceBasis,
  write: (fact: RuntimeUseCreationAdmission) => Effect.Effect<void, E, R>,
): Effect.fn.Return<boolean, E | RuntimeUseError, R | SqlClient.SqlClient> {
  const captured = fresh.get(basis);
  if (captured === undefined) return false;
  const sql = yield* SqlClient.SqlClient;
  const scope = yield* Effect.serviceOption(CreationScope);
  if (
    captured.consumed ||
    !captured.scope.active() ||
    Option.isNone(scope) ||
    scope.value !== captured.scope ||
    captured.scope.fiberId !== (yield* Effect.fiberId) ||
    sql !== captured.sql ||
    digest(captured.fact) !== captured.factSha256 ||
    Option.isNone(yield* Effect.serviceOption(sql.transactionService)) ||
    digest(intent) !== captured.fact.nativeIntentSha256 ||
    digest(basis) !== captured.fact.workspaceBasisSha256
  )
    return yield* fail("runtime_creation_original_capture_changed");
  const installation = yield* readRuntimeUseInstallation();
  if (installation === null || digest(installation) !== captured.fact.installationSha256)
    return yield* fail("runtime_creation_original_installation_changed");
  yield* write(captured.fact);
  if (digest(captured.fact) !== captured.factSha256)
    return yield* fail("runtime_creation_capture_mutated_during_publication");
  captured.consumed = true;
  return true;
});

export const withCapturedRuntimeUseCreationMaterial = Effect.fn(
  "RuntimeUseCreationCapture.consumeMaterial",
)(function* <E, R>(
  verified: NativeWorkspaceVerified,
  write: (fact: PreparedMaterial) => Effect.Effect<void, E, R>,
): Effect.fn.Return<boolean, E | RuntimeUseError, R | SqlClient.SqlClient> {
  const captured = material.get(verified);
  if (captured === undefined) return false;
  const original = captured.fresh;
  const sql = yield* SqlClient.SqlClient;
  const scope = yield* Effect.serviceOption(CreationScope);
  if (
    captured.consumed ||
    !original.consumed ||
    !original.scope.active() ||
    Option.isNone(scope) ||
    scope.value !== original.scope ||
    sql !== original.sql ||
    original.scope.fiberId !== (yield* Effect.fiberId) ||
    Option.isNone(yield* Effect.serviceOption(sql.transactionService)) ||
    fresh.get(verified.basis) !== original ||
    original.created === null ||
    digest(verified) !== captured.verifiedSha256 ||
    digest(captured.fact) !== captured.factSha256 ||
    captured.fact.admissionSha256 !== original.factSha256 ||
    digest(original.fact) !== original.factSha256
  )
    return yield* fail("runtime_creation_original_material_publication_changed");
  const installation = yield* readRuntimeUseInstallation();
  if (installation === null || digest(installation) !== original.fact.installationSha256)
    return yield* fail("runtime_creation_material_installation_changed");
  yield* write(captured.fact);
  if (digest(verified) !== captured.verifiedSha256 || digest(captured.fact) !== captured.factSha256)
    return yield* fail("runtime_creation_material_mutated_during_publication");
  captured.consumed = true;
  return true;
});
