import type * as SetupCustody from "./NativeWorkspaceSetupCustody.ts";
// @effect-diagnostics nodeBuiltinImport:off - physical ownership needs lstat, which does not follow symlinks.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { ProjectId, type ProjectScript } from "@t3tools/contracts";
import { resolveProjectScripts, setupProjectScript } from "@t3tools/shared/projectScripts";
import {
  resolveProjectSettings,
  resolveProjectFileBackedSetting,
} from "@t3tools/shared/projectSettings";
import { parseT3ProjectFile } from "@t3tools/shared/t3ProjectFile";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Git from "../../vcs/GitVcsDriver.ts";
import * as Projects from "../../orchestration-v2/ProjectStore.ts";
import * as Settings from "../../serverSettings.ts";
import type * as Authority from "./NativeCreationAuthority.ts";
import * as Repository from "./NativeCreationRepository.ts";
import * as Workspace from "./NativeCreationWorkspacePreparation.ts";
import type { NativeWorkspaceBasis, NativeWorkspaceProof } from "./NativeCreationWorkspaceTypes.ts";
import {
  nativeCreationCanonicalJson as canonical,
  nativeCreationSha256 as digest,
} from "./NativeCreationPreparation.ts";

const deny = (code: Workspace.NativeWorkspaceError["code"], message: string) =>
  new Workspace.NativeWorkspaceError({ code, message });
const conflict = () => deny("conflict", "Native physical workspace identity changed");
const unavailable = () =>
  deny("unavailable", "Native physical workspace original owner is unavailable");

export interface NativeWorkspacePathObservation {
  readonly realPath: string;
  readonly birth: string;
  readonly kind: "directory" | "file";
}
export class NativeWorkspaceFileObservation extends Context.Service<
  NativeWorkspaceFileObservation,
  {
    readonly inspect: (
      path: string,
    ) => Effect.Effect<NativeWorkspacePathObservation | null, Workspace.NativeWorkspaceError>;
    readonly readFile: (path: string) => Effect.Effect<string, Workspace.NativeWorkspaceError>;
  }
>()("t3/jones/nativeCreation/NativeWorkspacePhysicalPorts/NativeWorkspaceFileObservation") {}

const nodeFileObservation = NativeWorkspaceFileObservation.of({
  inspect: (path) =>
    Effect.tryPromise({
      try: async () => {
        let stat;
        try {
          stat = await NodeFSP.lstat(path);
        } catch (cause) {
          if (
            typeof cause === "object" &&
            cause !== null &&
            "code" in cause &&
            cause.code === "ENOENT"
          )
            return null;
          throw cause;
        }
        if (stat.isSymbolicLink() || (!stat.isDirectory() && !stat.isFile())) throw conflict();
        return {
          realPath: await NodeFSP.realpath(path),
          birth: canonical({
            device: String(stat.dev),
            inode: String(stat.ino),
            birthtime: stat.birthtimeMs,
          }),
          kind: stat.isDirectory() ? ("directory" as const) : ("file" as const),
        };
      },
      catch: conflict,
    }),
  readFile: (path) =>
    Effect.tryPromise({
      try: async () => {
        const stat = await NodeFSP.lstat(path);
        if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 4096) throw conflict();
        return await NodeFSP.readFile(path, "utf8");
      },
      catch: conflict,
    }),
});

// This original-owner input is not a grant issuer. No positive owner is installed by this module.
export class NativeWorkspacePhysicalOwner extends Context.Service<
  NativeWorkspacePhysicalOwner,
  {
    readonly assertAvailable: (
      input: Authority.NativeCreationAuthorityInput,
    ) => Effect.Effect<void, Workspace.NativeWorkspaceError>;
    readonly readCurrent: (claim: Repository.NativeCreationStoredIntent) => Effect.Effect<
      {
        readonly bootId: string;
        readonly producerId: string;
      },
      Workspace.NativeWorkspaceError
    >;
    readonly assertSetupAvailable?: Effect.Effect<void, Workspace.NativeWorkspaceError>;
    readonly setup?: (input: {
      readonly custody?: SetupCustody.NativeWorkspaceSetupCustody;
      readonly claim: Repository.NativeCreationStoredIntent;
      readonly basis: NativeWorkspaceBasis;
      readonly proof: NativeWorkspaceProof;
      readonly script: ProjectScript;
      readonly revalidate: Effect.Effect<void, Workspace.NativeWorkspaceError>;
    }) => Effect.Effect<
      {
        readonly terminalId: string;
        readonly completion: Effect.Effect<number, Workspace.NativeWorkspaceError>;
      },
      Workspace.NativeWorkspaceError
    >;
    readonly rollback?: (input: {
      readonly claim: Repository.NativeCreationStoredIntent;
      readonly basis: NativeWorkspaceBasis;
      readonly proof: NativeWorkspaceProof;
      readonly revalidate: Effect.Effect<void, Workspace.NativeWorkspaceError>;
    }) => Effect.Effect<void, Workspace.NativeWorkspaceError>;
  }
>()("t3/jones/nativeCreation/NativeWorkspacePhysicalPorts/NativeWorkspacePhysicalOwner") {}

const make = Effect.gen(function* () {
  const observation = yield* Effect.serviceOption(NativeWorkspaceFileObservation);
  const fs = Option.getOrElse(observation, () => nodeFileObservation);
  const git = yield* Git.GitVcsDriver;
  const projects = yield* Projects.ProjectStoreV2;
  const settings = yield* Settings.ServerSettingsService;
  const repository = yield* Repository.NativeCreationRepository;
  const optionalOwner = yield* Effect.serviceOption(NativeWorkspacePhysicalOwner);
  const owner = () =>
    Option.isSome(optionalOwner) ? Effect.succeed(optionalOwner.value) : Effect.fail(unavailable());
  const directory = (path: string) =>
    Effect.gen(function* () {
      if (!NodePath.isAbsolute(path) || NodePath.normalize(path) !== path) return yield* conflict();
      const value = yield* fs.inspect(path);
      if (value === null || value.kind !== "directory" || value.realPath !== path)
        return yield* conflict();
      return value;
    });
  const readGit = (cwd: string, args: readonly string[]) =>
    git
      .execute({
        operation: "NativeWorkspacePhysicalPorts.observe",
        cwd,
        args,
        env: { GIT_OPTIONAL_LOCKS: "0" },
        maxOutputBytes: 256 * 1024,
      })
      .pipe(
        Effect.flatMap((value) =>
          value.exitCode === 0 && !value.stdoutTruncated && !value.stderrTruncated
            ? Effect.succeed(value.stdout)
            : Effect.fail(conflict()),
        ),
        Effect.mapError(conflict),
      );
  const common = (cwd: string) =>
    Effect.gen(function* () {
      const path = (yield* readGit(cwd, [
        "rev-parse",
        "--path-format=absolute",
        "--git-common-dir",
      ])).trim();
      const physical = yield* directory(path);
      return { path, identity: physical.birth };
    });
  const claimFor = (basis: NativeWorkspaceBasis) =>
    Effect.gen(function* () {
      if (!repository.readWorkspaceClaim || !repository.readWorkspaceAdmission)
        return yield* unavailable();
      const decoded = yield* Schema.decodeEffect(
        Schema.fromJsonString(
          Schema.Struct({ claimId: Schema.NonEmptyString, ownerId: Schema.NonEmptyString }),
        ),
      )(basis.producerId).pipe(Effect.mapError(conflict));
      const history = yield* repository
        .readWorkspaceClaim(decoded.claimId)
        .pipe(Effect.mapError(conflict));
      const admission = yield* repository
        .readWorkspaceAdmission(decoded.claimId)
        .pipe(Effect.mapError(conflict));
      if (canonical(admission) !== canonical(basis)) return yield* conflict();
      const current = yield* (yield* owner()).readCurrent(history.intent);
      if (current.bootId !== basis.bootId || current.producerId !== decoded.ownerId)
        return yield* conflict();
      return history.intent;
    });
  const definitions = (claim: Repository.NativeCreationStoredIntent) =>
    Effect.gen(function* () {
      const project = yield* projects
        .get(ProjectId.make(claim.binding.projectId))
        .pipe(Effect.mapError(conflict));
      if (Option.isNone(project) || project.value.workspaceRoot !== claim.resources.projectCwd)
        return yield* conflict();
      const currentSettings = yield* settings.getSettings.pipe(Effect.mapError(conflict));
      const script = setupProjectScript(
        resolveProjectScripts(currentSettings, {
          id: project.value.projectId,
          scripts: project.value.scripts,
        }),
      );
      const revision = claim.binding.startFromOrigin
        ? `refs/remotes/origin/${claim.binding.baseBranch.replace(/^origin\//u, "")}`
        : claim.binding.baseBranch;
      const oid = (yield* git
        .resolveCommit({ cwd: claim.resources.projectCwd, revision })
        .pipe(Effect.mapError(conflict))).commitSha;
      const readTreeFile = (name: string) =>
        Effect.gen(function* () {
          const entry = yield* readGit(claim.resources.projectCwd, [
            "ls-tree",
            "-z",
            oid,
            "--",
            name,
          ]);
          if (entry === "") return null;
          if (!entry.startsWith("100644 ") && !entry.startsWith("100755 "))
            return yield* conflict();
          return yield* readGit(claim.resources.projectCwd, ["show", `${oid}:${name}`]);
        });
      const modules = yield* readTreeFile(".gitmodules");
      const projectFile = yield* readTreeFile("t3.json");
      const parsed = projectFile === null ? null : parseT3ProjectFile(projectFile);
      if (projectFile !== null && parsed === null) return yield* conflict();
      const override = resolveProjectSettings(currentSettings, project.value.projectId).settings
        .worktreeSubmodules;
      const mode =
        modules === null
          ? "none"
          : resolveProjectFileBackedSetting("worktreeSubmodules", override, parsed).value;
      const remotes = (yield* readGit(claim.resources.projectCwd, ["remote"]))
        .trim()
        .split("\n")
        .filter(Boolean)
        .sort((a, b) => b.length - a.length);
      const prefix = remotes.find((remote) => claim.binding.baseBranch.startsWith(`${remote}/`));
      const baseBranch =
        prefix === undefined
          ? claim.binding.baseBranch
          : claim.binding.baseBranch.slice(prefix.length + 1);
      const baseConfiguration = {
        key: `branch.${claim.resources.branch}.gh-merge-base`,
        value: baseBranch,
      };
      return {
        script,
        revision,
        oid,
        mode,
        submodules: canonical({ mode, modules, projectFile }),
        baseConfiguration,
        base: canonical(baseConfiguration),
      };
    });
  const materialBirth = (worktreePath: string) =>
    Effect.gen(function* () {
      const target = yield* directory(worktreePath);
      const dotGit = yield* fs.inspect(NodePath.join(worktreePath, ".git"));
      if (dotGit === null || dotGit.kind !== "file") return yield* conflict();
      const gitDirectory = (yield* readGit(worktreePath, [
        "rev-parse",
        "--absolute-git-dir",
      ])).trim();
      const physicalDirectory = yield* directory(gitDirectory);
      return canonical({
        target: target.birth,
        dotGit: dotGit.birth,
        gitDirectory: physicalDirectory.birth,
      });
    });
  // Transient evidence is usable only within the current create call, never for retry or recovery.
  const inFlight = new Map<string, NativeWorkspaceProof>();
  const verify = (basis: NativeWorkspaceBasis, proof: NativeWorkspaceProof) =>
    Effect.gen(function* () {
      const targetBirth = yield* materialBirth(proof.worktreePath);
      const parent = yield* directory(NodePath.dirname(proof.worktreePath));
      const gitIdentity = yield* common(proof.worktreePath);
      const dotGit = yield* fs.inspect(NodePath.join(proof.worktreePath, ".git"));
      if (dotGit === null || dotGit.kind !== "file") return yield* conflict();
      const link = (yield* fs.readFile(NodePath.join(proof.worktreePath, ".git"))).trim();
      if (!link.startsWith("gitdir: ")) return yield* conflict();
      const gitDirectory = (yield* readGit(proof.worktreePath, [
        "rev-parse",
        "--absolute-git-dir",
      ])).trim();
      yield* directory(gitDirectory);
      const relative = NodePath.relative(
        NodePath.join(basis.gitCommonDirectory, "worktrees"),
        gitDirectory,
      );
      if (
        !relative ||
        relative.startsWith("..") ||
        NodePath.isAbsolute(relative) ||
        NodePath.resolve(proof.worktreePath, link.slice(8)) !== gitDirectory
      )
        return yield* conflict();
      const branch = (yield* readGit(proof.worktreePath, ["symbolic-ref", "HEAD"])).trim();
      const oid = (yield* readGit(proof.worktreePath, [
        "rev-parse",
        "--verify",
        "HEAD^{commit}",
      ])).trim();
      const records = (yield* readGit(basis.projectCwd, ["worktree", "list", "--porcelain", "-z"]))
        .split("\0\0")
        .map((record) => record.split("\0"))
        .filter((record) => record.includes(`worktree ${proof.worktreePath}`));
      if (
        targetBirth !== proof.pathBirth ||
        parent.birth !== basis.parentBirth ||
        proof.worktreePath !== basis.worktreePath ||
        gitIdentity.path !== basis.gitCommonDirectory ||
        gitIdentity.identity !== basis.physicalGitIdentity ||
        proof.gitCommonDirectory !== basis.gitCommonDirectory ||
        proof.physicalGitIdentity !== basis.physicalGitIdentity ||
        branch !== `refs/heads/${proof.branch}` ||
        records.length !== 1 ||
        !records[0]!.includes(`branch ${branch}`) ||
        !records[0]!.includes(`HEAD ${oid}`) ||
        records[0]!.some((field) => field.startsWith("locked") || field.startsWith("prunable"))
      )
        return yield* conflict();
      const claim = yield* claimFor(basis);
      const definition = yield* definitions(claim);
      if (
        proof.branch !== claim.resources.branch ||
        definition.submodules !== basis.configuredSubmodulesDefinition ||
        definition.base !== basis.baseConfigurationDefinition
      )
        return yield* conflict();
      if (
        proof.baseRef !== basis.baseRef ||
        proof.configuredSubmodulesDigest !== digest(basis.configuredSubmodulesDefinition) ||
        proof.baseConfigurationDigest !== digest(basis.baseConfigurationDefinition)
      )
        return yield* conflict();
      const config = yield* git
        .readConfigValue(basis.projectCwd, definition.baseConfiguration.key)
        .pipe(Effect.mapError(conflict));
      if (config !== definition.baseConfiguration.value) return yield* conflict();
    });
  const inspect: Workspace.NativeWorkspacePorts["Service"]["inspect"] = (claim, expected) =>
    Effect.gen(function* () {
      const currentOwner = yield* owner();
      const identity = yield* currentOwner.readCurrent(claim);
      if (
        identity.bootId !== claim.claimedBootId ||
        !identity.producerId ||
        claim.binding.projectCwd !== claim.resources.projectCwd ||
        claim.binding.requestedBranch !== claim.resources.branch
      )
        return yield* conflict();
      if (claim.binding.runSetupScript) {
        if (!currentOwner.setup || !currentOwner.assertSetupAvailable) return yield* unavailable();
        yield* currentOwner.assertSetupAvailable;
      }
      const project = yield* directory(claim.resources.projectCwd);
      const parent = yield* directory(NodePath.dirname(claim.resources.worktreePath));
      const gitIdentity = yield* common(claim.resources.projectCwd);
      const definition = yield* definitions(claim);
      if (claim.binding.runSetupScript && definition.script === null) return yield* unavailable();
      const basis: NativeWorkspaceBasis = {
        bootId: identity.bootId,
        projectId: claim.binding.projectId,
        projectCwd: claim.resources.projectCwd,
        projectBirth: project.birth,
        gitCommonDirectory: gitIdentity.path,
        physicalGitIdentity: gitIdentity.identity,
        worktreePath: claim.resources.worktreePath,
        parentBirth: parent.birth,
        producerId: canonical({ claimId: claim.claimId, ownerId: identity.producerId }),
        baseRef: claim.binding.baseBranch,
        setupDefinition: definition.script === null ? null : canonical(definition.script),
        configuredSubmodulesDefinition: definition.submodules,
        baseConfigurationDefinition: definition.base,
      };
      const proof = expected ?? inFlight.get(claim.claimId);
      if (proof !== undefined) {
        // Intermediate callbacks prove material; base configuration is checked only after its owner completes.
        const currentBirth = yield* materialBirth(proof.worktreePath);
        if (
          currentBirth !== proof.pathBirth ||
          proof.gitCommonDirectory !== basis.gitCommonDirectory ||
          proof.physicalGitIdentity !== basis.physicalGitIdentity
        )
          return yield* conflict();
      } else {
        if ((yield* fs.inspect(claim.resources.worktreePath)) !== null) return yield* conflict();
        const registration = yield* readGit(basis.projectCwd, [
          "worktree",
          "list",
          "--porcelain",
          "-z",
        ]);
        const refs = yield* readGit(basis.projectCwd, [
          "for-each-ref",
          "--format=%(refname)",
          `refs/heads/${claim.resources.branch}`,
        ]);
        const config = yield* git
          .readConfigValue(basis.projectCwd, `branch.${claim.resources.branch}.gh-merge-base`)
          .pipe(Effect.mapError(conflict));
        if (
          registration.split("\0").includes(`worktree ${basis.worktreePath}`) ||
          refs.trim() !== "" ||
          config !== null
        )
          return yield* conflict();
      }
      return basis;
    });
  const createWorktree: Workspace.NativeWorkspacePorts["Service"]["createWorktree"] = (
    basis,
    branch,
    revalidate,
  ) =>
    Effect.gen(function* () {
      const claim = yield* claimFor(basis);
      if (branch !== claim.resources.branch) return yield* conflict();
      const definition = yield* definitions(claim);
      let proof: NativeWorkspaceProof | undefined;
      let pending: Git.LegacyWorktreePreparationStep["kind"] | undefined;
      const completed = new Set<Git.LegacyWorktreePreparationStep["kind"]>();
      yield* git
        .createWorktree(
          {
            cwd: basis.projectCwd,
            refName: definition.oid,
            newRefName: branch,
            baseRefName: basis.baseRef,
            path: basis.worktreePath,
          },
          {
            submodules: definition.mode,
            legacyPreparation: {
              beforeEffect: (step) =>
                Effect.gen(function* () {
                  yield* revalidate;
                  if (
                    pending !== undefined ||
                    completed.has(step.kind) ||
                    step.worktreePath !== basis.worktreePath ||
                    step.commonDirectory !== basis.gitCommonDirectory ||
                    step.targetRef !== `refs/heads/${branch}` ||
                    step.baseCommitOid !== definition.oid
                  )
                    return yield* conflict();
                  const expectedArgs =
                    step.kind === "worktree.add"
                      ? ["worktree", "add", "-b", branch, basis.worktreePath, definition.oid]
                      : step.kind === "worktree.submodules"
                        ? [
                            "submodule",
                            "update",
                            "--init",
                            ...(definition.mode === "recursive" ? ["--recursive"] : []),
                          ]
                        : [
                            "config",
                            `branch.${branch}.gh-merge-base`,
                            definition.baseConfiguration.value,
                          ];
                  const actualArgs = step.kind === "worktree.add" ? step.args.slice(-6) : step.args;
                  if (
                    canonical(actualArgs) !== canonical(expectedArgs) ||
                    step.cwd !==
                      (step.kind === "worktree.submodules"
                        ? basis.worktreePath
                        : basis.projectCwd) ||
                    (step.kind !== "worktree.add" && !completed.has("worktree.add"))
                  )
                    return yield* conflict();
                  pending = step.kind;
                }),
              afterEffect: (step, result, material) =>
                Effect.gen(function* () {
                  if (
                    pending !== step.kind ||
                    result !== "settled_success" ||
                    material === undefined ||
                    material.path !== basis.worktreePath ||
                    material.commonDirectory !== basis.gitCommonDirectory
                  )
                    return yield* deny(
                      "unknown",
                      "Physical worktree owner did not prove completion",
                    );
                  const path = yield* directory(material.path);
                  const birth = yield* Schema.decodeEffect(
                    Schema.fromJsonString(
                      Schema.Struct({
                        device: Schema.NonEmptyString,
                        inode: Schema.NonEmptyString,
                        birthtime: Schema.Number,
                      }),
                    ),
                  )(path.birth).pipe(Effect.mapError(conflict));
                  if (birth.device !== material.device || birth.inode !== material.inode)
                    return yield* conflict();
                  proof = {
                    worktreePath: material.path,
                    pathBirth: yield* materialBirth(material.path),
                    gitCommonDirectory: basis.gitCommonDirectory,
                    physicalGitIdentity: basis.physicalGitIdentity,
                    branch,
                    baseRef: basis.baseRef,
                    configuredSubmodulesDigest: digest(basis.configuredSubmodulesDefinition),
                    baseConfigurationDigest: digest(basis.baseConfigurationDefinition),
                  };
                  inFlight.set(claim.claimId, proof);
                  completed.add(step.kind);
                  pending = undefined;
                }),
            },
          },
        )
        .pipe(
          Effect.mapError(() =>
            deny("unknown", "Physical worktree creation requires original-owner observation"),
          ),
          Effect.ensuring(Effect.sync(() => inFlight.delete(claim.claimId))),
        );
      if (
        proof === undefined ||
        pending !== undefined ||
        !completed.has("worktree.add") ||
        !completed.has("worktree.base-config") ||
        (definition.mode !== "none" && !completed.has("worktree.submodules"))
      )
        return yield* deny(
          "unknown",
          "Physical worktree owner did not prove every configured step",
        );
      yield* verify(basis, proof);
      return proof;
    });
  const ports: Workspace.NativeWorkspacePorts["Service"] & {
    readonly assertAvailable: NativeWorkspacePhysicalOwner["Service"]["assertAvailable"];
  } = {
    assertAvailable: (input) =>
      Effect.gen(function* () {
        const physicalOwner = yield* owner();
        if (input.preparation.binding.run_setup_script) {
          if (!physicalOwner.setup || !physicalOwner.assertSetupAvailable)
            return yield* unavailable();
          yield* physicalOwner.assertSetupAvailable;
        }
        yield* physicalOwner.assertAvailable(input);
      }),
    inspect,
    verify,
    createWorktree,
    fetch: (basis, revalidate) =>
      Effect.gen(function* () {
        const claim = yield* claimFor(basis);
        if (!claim.binding.startFromOrigin) return yield* conflict();
        yield* revalidate;
        yield* git
          .fetchRemote({ cwd: basis.projectCwd, remoteName: "origin", refName: basis.baseRef })
          .pipe(
            Effect.mapError(() =>
              deny("unknown", "Physical fetch completion requires original-owner observation"),
            ),
          );
      }),
    setup: (basis, proof, revalidate, custody) =>
      Effect.gen(function* () {
        const claim = yield* claimFor(basis);
        const physicalOwner = yield* owner();
        const definition = yield* definitions(claim);
        if (
          custody === undefined ||
          !physicalOwner.setup ||
          !physicalOwner.assertSetupAvailable ||
          !claim.binding.runSetupScript ||
          definition.script === null ||
          canonical(definition.script) !== basis.setupDefinition
        )
          return yield* unavailable();
        yield* physicalOwner.assertSetupAvailable;
        yield* revalidate;
        yield* verify(basis, proof);
        const result = yield* physicalOwner.setup({
          custody,
          claim,
          basis,
          proof,
          script: definition.script,
          revalidate,
        });
        if (!result.terminalId)
          return yield* deny("unknown", "Setup original owner did not retain its terminal");
        return result;
      }),
    cleanup: (basis, proof, revalidate) =>
      Effect.gen(function* () {
        const claim = yield* claimFor(basis);
        const physicalOwner = yield* owner();
        if (!physicalOwner.rollback) return yield* unavailable();
        yield* revalidate;
        yield* verify(basis, proof);
        yield* physicalOwner.rollback({ claim, basis, proof, revalidate });
      }),
  };
  return Workspace.NativeWorkspacePorts.of(ports);
});

export const layer = Layer.effect(Workspace.NativeWorkspacePorts, make);
