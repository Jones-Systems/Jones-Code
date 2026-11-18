// @effect-diagnostics nodeBuiltinImport:off -- replacement cases retain the original physical directory and compare inode witnesses.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { CheckpointRef } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as ServerConfig from "../../../config.ts";
import * as Git from "../../../vcs/GitVcsDriver.ts";
import * as CheckoutCloner from "../../../vcs/CheckoutCloner.ts";
import * as VcsProcess from "../../../vcs/VcsProcess.ts";
import { makeGitVcsDriverCore } from "../../../vcs/GitVcsDriverCore.ts";
import { makeObservedBranchRename } from "./ObservedBranchRename.ts";

class SyntheticRenameFailure extends Schema.TaggedError<SyntheticRenameFailure>()(
  "SyntheticRenameFailure",
  { message: Schema.String },
) {}

const layerTest = ServerConfig.layerTest(process.cwd(), {
  prefix: "jones-observed-rename-",
}).pipe(Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)));

const fixture = Effect.fn("observedRenameFixture")(function* (failRename = false) {
  const fs = yield* FileSystem.FileSystem;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "jones-rename-fixture-" });
  const realRoot = yield* fs.realPath(root);
  const primary = NodePath.join(realRoot, "primary");
  const cwd = NodePath.join(realRoot, "linked");
  const timeline: string[] = [];
  const renames: ReadonlyArray<string>[] = [];
  const delegate = yield* ChildProcessSpawner.ChildProcessSpawner;
  const spawner = ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      if (!ChildProcess.isStandardCommand(command)) return yield* delegate.spawn(command);
      const args = command.args.map(String);
      const rename = args[0] === "branch" && args[1] === "-m";
      if (rename) {
        renames.push(args);
        timeline.push("git");
      }
      const handle = yield* delegate.spawn(command);
      if (!rename || !failRename) return handle;
      // Git really completes; only its reported completion fails, leaving ambiguous ownership.
      return ChildProcessSpawner.makeHandle({
        ...handle,
        exitCode: handle.exitCode.pipe(Effect.as(ChildProcessSpawner.ExitCode(1))),
      });
    }),
  );
  const driver = yield* makeGitVcsDriverCore().pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    Effect.provideService(CheckoutCloner.CheckoutCloner, () =>
      Effect.die("Observed rename must not clone"),
    ),
  );
  const git = (at: string, args: ReadonlyArray<string>) =>
    driver
      .execute({
        operation: "observedRenameFixture.git",
        cwd: at,
        args,
      })
      .pipe(Effect.map((result) => result.stdout.trim()));
  yield* fs.makeDirectory(primary);
  yield* git(primary, ["init", "-q", "-b", "main"]);
  yield* git(primary, ["config", "user.name", "Synthetic Fixture"]);
  yield* git(primary, ["config", "user.email", "fixture@example.invalid"]);
  yield* git(primary, ["config", "commit.gpgsign", "false"]);
  yield* git(primary, ["commit", "--allow-empty", "-qm", "synthetic base"]);
  let before: Git.LegacyWorktreeBeforeObservation | undefined;
  let claim: Git.LegacyWorktreeMaterialClaim | undefined;
  yield* driver.createWorktree(
    {
      cwd: primary,
      path: cwd,
      refName: "main",
      newRefName: "legacy/pending",
    },
    {
      submodules: "none",
      legacyPreparation: {
        beforeEffect: (step) =>
          Effect.sync(() => {
            before = step.before;
          }),
        afterEffect: (_step, _outcome, material) =>
          Effect.sync(() => {
            claim = material;
          }),
      },
    },
  );
  if (before === undefined || claim === undefined)
    return yield* Effect.die("Missing fixture claim");
  const intents: Git.LegacyBranchRenameStep[] = [];
  const outcomes: Array<{
    outcome: string;
    claim: Git.LegacyWorktreeMaterialClaim | undefined;
  }> = [];
  const hooks: Git.LegacyBranchRenameHooks = {
    before,
    claim,
    beforeEffect: (step) =>
      Effect.sync(() => {
        intents.push(step);
        timeline.push("intent");
      }),
    afterEffect: (_step, outcome, material) =>
      Effect.sync(() => {
        outcomes.push({ outcome, claim: material });
        timeline.push(`outcome:${outcome}`);
      }),
  };
  const rename = (overrides: Partial<Git.GitRenameBranchInput> = {}) =>
    driver.renameBranch({
      cwd,
      oldBranch: "legacy/pending",
      newBranch: "chosen",
      legacyPreparation: hooks,
      ...overrides,
    });
  return {
    fs,
    realRoot,
    primary,
    cwd,
    driver,
    git,
    hooks,
    intents,
    outcomes,
    timeline,
    renames,
    rename,
  };
});

const isolated = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.scoped,
    Effect.provideService(HostProcess.Environment, {
      ...process.env,
      GIT_CONFIG_GLOBAL: "/dev/null",
      GIT_CONFIG_NOSYSTEM: "1",
      GIT_ALLOW_PROTOCOL: "file",
    }),
    Effect.provide(layerTest),
  );

describe("observed branch rename through the shared-config owner", () => {
  it.effect(
    "journals the actual collision-resolved name before Git and settles the same material",
    () =>
      isolated(
        Effect.gen(function* () {
          const f = yield* fixture();
          yield* f.git(f.primary, ["branch", "chosen"]);
          const renamed = yield* f.rename();
          assert.equal(renamed.branch, "chosen-1");
          assert.equal(f.intents.length, 1);
          assert.deepEqual(f.intents[0], {
            claim: f.hooks.claim,
            oldRef: "refs/heads/legacy/pending",
            oldOid: f.hooks.claim.headOid,
            targetRef: "refs/heads/chosen-1",
            exactName: false,
            args: ["branch", "-m", "--", "legacy/pending", "chosen-1"],
          });
          assert.deepEqual(f.renames, [f.intents[0]!.args]);
          assert.deepEqual(f.timeline, ["intent", "git", "outcome:settled_success"]);
          assert.deepEqual(f.outcomes, [
            {
              outcome: "settled_success",
              claim: { ...f.hooks.claim, headRef: "refs/heads/chosen-1" },
            },
          ]);
          assert.equal(yield* f.git(f.cwd, ["symbolic-ref", "HEAD"]), "refs/heads/chosen-1");
          // Renaming does not weaken the independent primary-checkout capture fence.
          const vcs = yield* Git.makeVcsDriverShape().pipe(Effect.provide(VcsProcess.layer));
          const refused = yield* vcs.checkpoints
            .captureCheckpoint({
              cwd: f.primary,
              checkpointRef: CheckpointRef.make("refs/checkpoints/rename-primary"),
            })
            .pipe(Effect.result);
          if (Result.isSuccess(refused)) return assert.fail("Expected primary checkout fence");
          assert.equal(refused.failure._tag, "VcsPrimaryCheckoutCheckpointError");
        }),
      ),
  );

  it.effect("a before-hook refusal runs no rename and leaves the original branch", () =>
    isolated(
      Effect.gen(function* () {
        const f = yield* fixture();
        const refused = yield* f
          .rename({
            legacyPreparation: {
              ...f.hooks,
              beforeEffect: () =>
                Effect.fail(
                  new SyntheticRenameFailure({ message: "synthetic persistence refusal" }),
                ),
            },
          })
          .pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(refused));
        assert.deepEqual(f.renames, []);
        assert.deepEqual(f.outcomes, []);
        assert.equal(yield* f.git(f.cwd, ["symbolic-ref", "HEAD"]), f.hooks.claim.headRef);
      }),
    ),
  );

  it.effect(
    "failed invocation is unknown even when recapture finds the renamed material; stale replay refuses",
    () =>
      isolated(
        Effect.gen(function* () {
          const f = yield* fixture(true);
          assert.isTrue(Exit.isFailure(yield* f.rename().pipe(Effect.exit)));
          assert.deepEqual(f.outcomes, [
            {
              outcome: "failed_or_unknown",
              claim: { ...f.hooks.claim, headRef: "refs/heads/chosen" },
            },
          ]);
          assert.isTrue(Exit.isFailure(yield* f.rename().pipe(Effect.exit)));
          assert.equal(f.renames.length, 1);
          assert.equal(f.intents.length, 1);
        }),
      ),
  );

  it.effect("interruption after invocation records unknown before propagating interruption", () =>
    isolated(
      Effect.gen(function* () {
        const f = yield* fixture();
        const observed = yield* makeObservedBranchRename({
          execute: f.driver.execute,
          rename: (cwd, args) =>
            f.driver
              .execute({
                operation: "test.interruptedRename",
                cwd,
                args,
                timeoutMs: 10_000,
              })
              .pipe(Effect.andThen(Effect.interrupt)),
        });
        const result = yield* observed({
          cwd: f.cwd,
          oldBranch: "legacy/pending",
          targetBranch: "chosen",
          exactName: true,
          hooks: f.hooks,
        }).pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(result));
        assert.deepEqual(f.timeline, ["intent", "git", "outcome:failed_or_unknown"]);
        assert.equal(f.outcomes[0]?.claim?.headRef, "refs/heads/chosen");
        assert.equal(f.renames.length, 1);
      }),
    ),
  );

  it.effect(
    "post-effect persistence failure cannot become success or authorize a second invocation",
    () =>
      isolated(
        Effect.gen(function* () {
          const f = yield* fixture();
          const result = yield* f
            .rename({
              legacyPreparation: {
                ...f.hooks,
                afterEffect: (step, outcome, claim) =>
                  f.hooks
                    .afterEffect(step, outcome, claim)
                    .pipe(
                      Effect.andThen(
                        Effect.fail(
                          new SyntheticRenameFailure({ message: "synthetic lost completion" }),
                        ),
                      ),
                    ),
              },
            })
            .pipe(Effect.exit);
          assert.isTrue(Exit.isFailure(result));
          assert.equal(f.outcomes[0]?.outcome, "settled_success");
          assert.isTrue(Exit.isFailure(yield* f.rename().pipe(Effect.exit)));
          assert.equal(f.renames.length, 1);
          assert.equal(f.intents.length, 1);
        }),
      ),
  );

  it.effect(
    "replaced material after intent refuses Git and records unknown without a forged claim",
    () =>
      isolated(
        Effect.gen(function* () {
          const f = yield* fixture();
          const result = yield* f
            .rename({
              legacyPreparation: {
                ...f.hooks,
                beforeEffect: (step) =>
                  f.hooks.beforeEffect(step).pipe(
                    Effect.andThen(
                      Effect.promise(async () => {
                        await NodeFSP.rename(f.cwd, NodePath.join(f.realRoot, "retained-original"));
                        await NodeFSP.mkdir(f.cwd);
                      }),
                    ),
                  ),
              },
            })
            .pipe(Effect.exit);
          assert.isTrue(Exit.isFailure(result));
          assert.deepEqual(f.renames, []);
          assert.deepEqual(f.outcomes, [{ outcome: "failed_or_unknown", claim: undefined }]);
          assert.deepEqual(f.timeline, ["intent", "outcome:failed_or_unknown"]);
        }),
      ),
  );

  it.effect(
    "same-name no-op has no journal, while exact-name collision and primary material refuse before intent",
    () =>
      isolated(
        Effect.gen(function* () {
          const f = yield* fixture();
          assert.deepEqual(yield* f.rename({ newBranch: "legacy/pending" }), {
            branch: "legacy/pending",
          });
          yield* f.git(f.primary, ["branch", "chosen"]);
          assert.isTrue(Exit.isFailure(yield* f.rename({ exactName: true }).pipe(Effect.exit)));
          assert.isTrue(
            Exit.isFailure(
              yield* f
                .rename({
                  cwd: f.primary,
                  oldBranch: "main",
                  newBranch: "other",
                  legacyPreparation: {
                    ...f.hooks,
                    claim: {
                      ...f.hooks.claim,
                      path: f.primary,
                      realPath: f.primary,
                      registeredPath: f.primary,
                      headRef: "refs/heads/main",
                    },
                  },
                })
                .pipe(Effect.exit),
            ),
          );
          assert.deepEqual(f.intents, []);
          assert.deepEqual(f.outcomes, []);
          assert.deepEqual(f.renames, []);
          assert.equal(yield* f.git(f.primary, ["symbolic-ref", "HEAD"]), "refs/heads/main");
        }),
      ),
  );
});
