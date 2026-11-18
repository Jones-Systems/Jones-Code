import { assert, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as GitWorkflow from "../../git/GitWorkflowService.ts";
import * as ServerConfig from "../../config.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import { git } from "../workspaceRetirement/Filesystem.ts";
import { acquireNativeFixture, releaseNativeFixture } from "./Native.testkit.ts";
import * as Recovery from "./Recovery.ts";

const fixtureGit = (cwd: string, ...args: string[]) =>
  Effect.promise(async () => (await git(cwd, args)).toString().trim());

it.effect.each(["behind", "remote-only", "diverged", "new-default"] as const)(
  "prepares a recovery worktree against fresh remote evidence (%s)",
  (state) =>
    Effect.gen(function* () {
      const f = yield* Effect.acquireRelease(Effect.promise(acquireNativeFixture), (f) =>
        Effect.promise(() => releaseNativeFixture(f)),
      );
      const remote = `${f.root}/remote.git`;
      const publisher = `${f.root}/publisher`;
      const fromDefault = state === "new-default";
      const remoteBranch = fromDefault ? "main" : "task";
      const trackingRef = `origin/${remoteBranch}`;
      yield* fixtureGit(f.repo, "clone", "--bare", "--quiet", ".", remote);
      yield* fixtureGit(f.repo, "remote", "add", "origin", remote);
      yield* fixtureGit(f.repo, "fetch", "--quiet", "origin");
      yield* fixtureGit(f.repo, "clone", "--quiet", "--branch", remoteBranch, remote, publisher);
      const commit = (cwd: string, message: string) =>
        fixtureGit(
          cwd,
          "-c",
          "user.name=Fixture",
          "-c",
          "user.email=fixture@example.invalid",
          "commit",
          "--allow-empty",
          "-qm",
          message,
        );
      yield* commit(publisher, "new remote tip");
      yield* fixtureGit(publisher, "push", "--quiet", "origin", remoteBranch);
      const remoteTip = yield* fixtureGit(publisher, "rev-parse", "HEAD");
      if (state === "diverged") yield* commit(f.checkout, "unpublished local tip");
      const localTip = yield* fixtureGit(f.checkout, "rev-parse", "HEAD");
      yield* fixtureGit(f.repo, "worktree", "remove", f.checkout);
      if (state === "remote-only") yield* fixtureGit(f.repo, "branch", "-d", "task");
      assert.equal(yield* fixtureGit(f.repo, "rev-parse", trackingRef), f.source.head);

      const workflow = Layer.effect(
        GitWorkflow.GitWorkflowService,
        Effect.map(GitVcsDriver.GitVcsDriver, (native) =>
          GitWorkflow.GitWorkflowService.of({
            hasCommit: ({ cwd, refName }: { cwd: string; refName: string }) =>
              native
                .execute({
                  cwd,
                  operation: "fixture.hasCommit",
                  args: ["rev-parse", "--verify", `${refName}^{commit}`],
                  allowNonZeroExit: true,
                })
                .pipe(Effect.map(({ exitCode }) => exitCode === 0)),
            resolvePrimaryRemoteName: native.resolvePrimaryRemoteName,
            resolveDefaultBranchName: native.resolveDefaultBranchName,
            resolveWorktreeBaseRemote: native.resolveWorktreeBaseRemote,
            fetchWorktreeBaseCommit: native.fetchWorktreeBaseCommit,
            createWorktree: native.createWorktree,
          } as GitWorkflow.GitWorkflowService["Service"]),
        ),
      ).pipe(
        Layer.provide(
          GitVcsDriver.layer.pipe(Layer.provide(ServerConfig.layerTest(f.repo, `${f.root}/state`))),
        ),
      );
      const result = yield* Effect.gen(function* () {
        const recovery = yield* Recovery.make;
        return yield* recovery.attach({
          cwd: f.repo,
          worktreePath: f.checkout,
          runId: "restore-test",
          branch: fromDefault ? null : "task",
          previousHead: f.source.head,
          choice: fromDefault ? "default" : "branch",
          hold: () => Effect.void,
        });
      }).pipe(Effect.provide(workflow), Effect.result);

      assert.equal(yield* fixtureGit(f.repo, "rev-parse", trackingRef), remoteTip);
      if (result._tag === "Failure") {
        assert.equal(state, "diverged");
        assert.match(Recovery.failureDetail(result.failure), /have diverged/);
        assert.equal(yield* fixtureGit(f.repo, "rev-parse", "task"), localTip);
      } else {
        assert.notEqual(state, "diverged");
        if (fromDefault) {
          assert.match(result.success.branch, /^t3\/recovered-/);
          assert.equal(yield* fixtureGit(f.repo, "rev-parse", "main"), f.source.head);
        } else {
          assert.equal(result.success.branch, "task");
        }
        assert.equal(yield* fixtureGit(result.success.path, "rev-parse", "HEAD"), remoteTip);
        assert.equal(
          yield* fixtureGit(result.success.path, "symbolic-ref", "HEAD"),
          `refs/heads/${result.success.branch}`,
        );
      }
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);
