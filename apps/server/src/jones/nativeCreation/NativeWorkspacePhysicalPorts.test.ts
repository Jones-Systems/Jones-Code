import { assert, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  NativeCreationHistoricalBinding,
  ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Git from "../../vcs/GitVcsDriver.ts";
import * as Projects from "../../orchestration-v2/ProjectStore.ts";
import * as Settings from "../../serverSettings.ts";
import * as Repository from "./NativeCreationRepository.ts";
import * as Workspace from "./NativeCreationWorkspacePreparation.ts";
import * as Physical from "./NativeWorkspacePhysicalPorts.ts";
import type { NativeWorkspaceBasis } from "./NativeCreationWorkspaceTypes.ts";
import { nativeCreationCanonicalJson as canonical } from "./NativeCreationPreparation.ts";

const fixtureClaim = (setup = false): Repository.NativeCreationStoredIntent => ({
  claimId: "fixture-claim",
  claimedBootId: "fixture-boot",
  claimedAt: "2026-10-07T12:00:00Z",
  actorSessionId: "fixture-actor",
  grantId: "fixture-grant",
  grantRevision: 1,
  preparationId: "fixture-preparation",
  operationId: "fixture-operation",
  preparationSha256: "fixture-hash",
  bindingDigest: "fixture-binding",
  promptDigest: "fixture-prompt",
  commandDigest: "fixture-command",
  commandId: "fixture-command",
  threadId: "fixture-thread",
  messageId: "fixture-message",
  canonicalPreparation: "{}",
  binding: Schema.decodeUnknownSync(NativeCreationHistoricalBinding)({
    backendInstance: "fixture-backend",
    environmentId: "fixture-environment",
    projectId: "fixture-project",
    projectCwd: "/fixture/project",
    accountRef: "fixture-account",
    accountBindingId: "fixture-qualified-account",
    accountBindingRevision: 1,
    providerModelSelection: { instanceId: "codex", model: "fixture-model" },
    runtimeMode: "full-access",
    interactionMode: "default",
    baseBranch: "main",
    startFromOrigin: false,
    runSetupScript: setup,
    requestedBranch: "fixture-branch",
  }),
  resources: {
    projectCwd: "/fixture/project",
    worktreePath: "/fixture/worktrees/new",
    branch: "fixture-branch",
  },
});
const scenario = (
  options: {
    owner?: boolean;
    setup?: boolean;
    revoked?: boolean;
    badMaterial?: boolean;
    failConfig?: boolean;
    failSubmodules?: boolean;
  } = {},
) => {
  const claim = fixtureClaim(options.setup);
  const calls: string[] = [];
  let created = false;
  let configured = false;
  let admitted: NativeWorkspaceBasis | undefined;
  const birth = (name: string) =>
    canonical({ device: "fixture-device", inode: name, birthtime: 1 });
  const paths = new Map<string, Physical.NativeWorkspacePathObservation>([
    [
      "/fixture/project",
      { realPath: "/fixture/project", kind: "directory", birth: birth("project") },
    ],
    [
      "/fixture/worktrees",
      { realPath: "/fixture/worktrees", kind: "directory", birth: birth("parent") },
    ],
    [
      "/fixture/project/.git",
      { realPath: "/fixture/project/.git", kind: "directory", birth: birth("common") },
    ],
  ]);
  const git = {
    execute: ({ args, cwd }: Git.ExecuteGitInput) =>
      Effect.sync(() => {
        calls.push(`read:${args[0]}`);
        let stdout = "";
        if (args.includes("--git-common-dir")) stdout = "/fixture/project/.git\n";
        else if (args.includes("--absolute-git-dir"))
          stdout = "/fixture/project/.git/worktrees/new\n";
        else if (args[0] === "symbolic-ref") stdout = "refs/heads/fixture-branch\n";
        else if (args[0] === "rev-parse") stdout = `${"a".repeat(40)}\n`;
        else if (args[0] === "worktree")
          stdout = created
            ? `worktree /fixture/worktrees/new\0HEAD ${"a".repeat(40)}\0branch refs/heads/fixture-branch\0\0`
            : "";
        else if (args[0] === "remote") stdout = "origin\n";
        else if (args[0] === "ls-tree" && args.at(-1) === ".gitmodules" && options.failSubmodules)
          stdout = `100644 blob ${"b".repeat(40)}\t.gitmodules\0`;
        else if (args[0] === "show")
          stdout = '[submodule "fixture"]\npath = fixture\nurl = https://example.invalid/fixture\n';
        assert.isTrue(cwd.startsWith("/fixture/"));
        return {
          exitCode: 0,
          stdout,
          stderr: "",
          stdoutTruncated: false,
          stderrTruncated: false,
        } as Git.ExecuteGitResult;
      }),
    resolveCommit: () => Effect.succeed({ commitSha: "a".repeat(40) }),
    readConfigValue: () => Effect.succeed(configured ? "main" : null),
    fetchRemote: () =>
      Effect.sync(() => {
        calls.push("fetch");
      }),
    createWorktree: (_input: unknown, options?: Git.CreateWorktreeOptions) =>
      Effect.gen(function* () {
        assert.isDefined(options?.legacyPreparation);
        const hooks = options!.legacyPreparation!;
        const step = {
          cwd: "/fixture/project",
          args: [
            "-c",
            "checkout.workers=0",
            "worktree",
            "add",
            "-b",
            "fixture-branch",
            claim.resources.worktreePath,
            "a".repeat(40),
          ],
          worktreePath: claim.resources.worktreePath,
          commonDirectory: "/fixture/project/.git",
          baseCommitOid: "a".repeat(40),
          targetRef: "refs/heads/fixture-branch",
        };
        yield* hooks.beforeEffect({ ...step, kind: "worktree.add" });
        calls.push("add");
        created = true;
        paths.set(claim.resources.worktreePath, {
          realPath: claim.resources.worktreePath,
          kind: "directory",
          birth: birth("target"),
        });
        paths.set(`${claim.resources.worktreePath}/.git`, {
          realPath: `${claim.resources.worktreePath}/.git`,
          kind: "file",
          birth: birth("dotgit"),
        });
        paths.set("/fixture/project/.git/worktrees/new", {
          realPath: "/fixture/project/.git/worktrees/new",
          kind: "directory",
          birth: birth("gitdir"),
        });
        const material: Git.LegacyWorktreeMaterialClaim = {
          path: claim.resources.worktreePath,
          realPath: claim.resources.worktreePath,
          device: "fixture-device",
          inode: scenarioOptions.badMaterial ? "foreign" : "target",
          parentRealPath: "/fixture/worktrees",
          gitDirectory: "/fixture/project/.git/worktrees/new",
          commonDirectory: "/fixture/project/.git",
          registeredPath: claim.resources.worktreePath,
          headRef: "refs/heads/fixture-branch",
          headOid: "a".repeat(40),
        };
        yield* hooks.afterEffect({ ...step, kind: "worktree.add" }, "settled_success", material);
        if (scenarioOptions.failSubmodules) {
          yield* hooks.beforeEffect({
            ...step,
            cwd: claim.resources.worktreePath,
            args: ["submodule", "update", "--init", "--recursive"],
            kind: "worktree.submodules",
          });
          calls.push("submodules");
          yield* hooks.afterEffect(
            {
              ...step,
              cwd: claim.resources.worktreePath,
              args: ["submodule", "update", "--init", "--recursive"],
              kind: "worktree.submodules",
            },
            "failed_or_unknown",
            material,
          );
        }
        yield* hooks.beforeEffect({
          ...step,
          args: ["config", "branch.fixture-branch.gh-merge-base", "main"],
          kind: "worktree.base-config",
        });
        calls.push("config");
        if (scenarioOptions.failConfig) {
          yield* hooks.afterEffect(
            {
              ...step,
              args: ["config", "branch.fixture-branch.gh-merge-base", "main"],
              kind: "worktree.base-config",
            },
            "failed_or_unknown",
            material,
          );
        }
        configured = true;
        yield* hooks.afterEffect(
          {
            ...step,
            args: ["config", "branch.fixture-branch.gh-merge-base", "main"],
            kind: "worktree.base-config",
          },
          "settled_success",
          material,
        );
        return {
          worktree: { path: claim.resources.worktreePath, refName: claim.resources.branch },
        };
      }),
  };
  const scenarioOptions = options;
  const owners = Layer.mergeAll(
    Layer.succeed(Physical.NativeWorkspaceFileObservation, {
      inspect: (path) =>
        Effect.sync(() => {
          calls.push("read:lstat");
          return paths.get(path) ?? null;
        }),
      readFile: () =>
        Effect.sync(() => {
          calls.push("read:gitlink");
          return "gitdir: /fixture/project/.git/worktrees/new\n";
        }),
    }),
    Layer.succeed(Git.GitVcsDriver, git as unknown as Git.GitVcsDriver["Service"]),
    Layer.succeed(Projects.ProjectStoreV2, {
      get: () =>
        Effect.succeed(
          Option.some({
            projectId: ProjectId.make("fixture-project"),
            workspaceRoot: claim.resources.projectCwd,
            scripts: [],
            deletedAt: null,
          }),
        ),
    } as unknown as Projects.ProjectStoreV2["Service"]),
    Layer.mock(Settings.ServerSettingsService)({
      getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
    }),
    Layer.succeed(Repository.NativeCreationRepository, {
      readWorkspaceClaim: () =>
        Effect.succeed({ intent: claim, normalizedCommandDigest: null, effects: [] }),
      readWorkspaceAdmission: () =>
        admitted === undefined ? Effect.die("fixture missing admission") : Effect.succeed(admitted),
    } as unknown as Repository.NativeCreationRepository["Service"]),
  );
  const physicalOwner = Layer.succeed(Physical.NativeWorkspacePhysicalOwner, {
    assertAvailable: () => Effect.void,
    readCurrent: () => Effect.succeed({ bootId: "fixture-boot", producerId: "fixture-producer" }),
  });
  const layer = Physical.layer.pipe(
    Layer.provide(options.owner === false ? owners : Layer.merge(owners, physicalOwner)),
  );
  const prepare = Effect.gen(function* () {
    const ports = yield* Workspace.NativeWorkspacePorts;
    const basis = yield* ports.inspect(claim);
    admitted = basis;
    let checks = 0;
    const revalidate = Effect.gen(function* () {
      checks += 1;
      if (options.revoked && checks > 1)
        return yield* new Workspace.NativeWorkspaceError({
          code: "conflict",
          message: "fixture authority revoked",
        });
      assert.deepEqual(yield* ports.inspect(claim), basis);
    });
    const proof = yield* ports.createWorktree(basis, claim.resources.branch, revalidate);
    yield* ports.verify(basis, proof);
    return { ports, basis, proof };
  });
  return { calls, claim, prepare, layer };
};

it.effect("missing physical owner refuses before any filesystem or Git operation", () => {
  const value = scenario({ owner: false });
  return Effect.gen(function* () {
    const result = yield* value.prepare.pipe(Effect.result);
    assert.isTrue(result._tag === "Failure");
    assert.deepEqual(value.calls, []);
  }).pipe(Effect.provide(value.layer));
});
it.effect("missing retained setup executor refuses before physical allocation", () => {
  const value = scenario({ setup: true });
  return Effect.gen(function* () {
    const result = yield* value.prepare.pipe(Effect.result);
    assert.isTrue(result._tag === "Failure");
    assert.deepEqual(value.calls, []);
  }).pipe(Effect.provide(value.layer));
});
it.effect(
  "complete Git owner callbacks and physical registration prove the admitted workspace",
  () => {
    const value = scenario();
    return Effect.gen(function* () {
      const result = yield* value.prepare;
      assert.equal(result.proof.worktreePath, value.claim.resources.worktreePath);
      assert.deepEqual(
        value.calls.filter((call) => !call.startsWith("read:")),
        ["add", "config"],
      );
      const duplicate = yield* result.ports.inspect(value.claim).pipe(Effect.result);
      assert.isTrue(duplicate._tag === "Failure");
      const rollback = yield* result.ports
        .cleanup(result.basis, result.proof, Effect.void)
        .pipe(Effect.result);
      assert.isTrue(rollback._tag === "Failure");
      assert.deepEqual(
        value.calls.filter((call) => !call.startsWith("read:")),
        ["add", "config"],
      );
    }).pipe(Effect.provide(value.layer));
  },
);
it.effect("revocation after worktree add prevents base configuration and does not clean up", () => {
  const value = scenario({ revoked: true });
  return Effect.gen(function* () {
    const result = yield* value.prepare.pipe(Effect.result);
    assert.isTrue(result._tag === "Failure");
    assert.deepEqual(
      value.calls.filter((call) => !call.startsWith("read:")),
      ["add"],
    );
  }).pipe(Effect.provide(value.layer));
});
it.effect("foreign material cannot supply a workspace proof", () => {
  const value = scenario({ badMaterial: true });
  return Effect.gen(function* () {
    const result = yield* value.prepare.pipe(Effect.result);
    assert.isTrue(result._tag === "Failure");
    assert.deepEqual(
      value.calls.filter((call) => !call.startsWith("read:")),
      ["add"],
    );
  }).pipe(Effect.provide(value.layer));
});
it.effect(
  "failed base configuration retains an unknown workspace instead of reporting success",
  () => {
    const value = scenario({ failConfig: true });
    return Effect.gen(function* () {
      const result = yield* value.prepare.pipe(Effect.result);
      assert.isTrue(result._tag === "Failure");
      assert.deepEqual(
        value.calls.filter((call) => !call.startsWith("read:")),
        ["add", "config"],
      );
    }).pipe(Effect.provide(value.layer));
  },
);

it.effect("failed submodule completion prevents base configuration and workspace success", () => {
  const value = scenario({ failSubmodules: true });
  return Effect.gen(function* () {
    const result = yield* value.prepare.pipe(Effect.result);
    assert.isTrue(result._tag === "Failure");
    assert.deepEqual(
      value.calls.filter((call) => !call.startsWith("read:")),
      ["add", "submodules"],
    );
  }).pipe(Effect.provide(value.layer));
});
