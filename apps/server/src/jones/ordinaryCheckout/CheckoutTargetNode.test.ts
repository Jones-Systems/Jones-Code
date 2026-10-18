import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as ServerConfig from "../../config.ts";
import * as Git from "../../vcs/GitVcsDriver.ts";
import * as Target from "./CheckoutTarget.ts";
import * as Observation from "./CheckoutTargetNode.ts";

const scratch = process.env.JONES_CHECKOUT_TEST_SCRATCH;
it.effect.skipIf(!scratch)(
  "real filesystem aliases and linked worktrees preserve physical target identity",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({
          directory: scratch!,
          prefix: "checkout-target-",
        });
        const repo = path.join(root, "repo");
        const alias = path.join(root, "alias");
        const linked = path.join(root, "linked");
        const plain = path.join(root, "plain");
        const templates = path.join(root, "templates");
        // A readable empty ancestor marker is not a repository; it must not hide a plain workspace.
        yield* fs.makeDirectory(path.join(root, ".git"));
        yield* fs.makeDirectory(repo);
        yield* fs.makeDirectory(plain);
        yield* fs.makeDirectory(templates);
        yield* fs.symlink(repo, alias);
        const gitLayer = Git.layer.pipe(
          Layer.provide(ServerConfig.layerTest(root, path.join(root, "state"))),
          Layer.provideMerge(NodeServices.layer),
        );
        const testLayer = Target.layer.pipe(
          Layer.provideMerge(Observation.layer),
          Layer.provideMerge(gitLayer),
        );
        yield* Effect.gen(function* () {
          const git = yield* Git.GitVcsDriver;
          const targets = yield* Target.CheckoutTarget;
          const run = (args: readonly string[]) =>
            git.execute({
              operation: "CheckoutTarget.test",
              cwd: repo,
              args: [
                "-c",
                "user.name=Synthetic",
                "-c",
                "user.email=synthetic@example.invalid",
                "-c",
                `core.hooksPath=${templates}`,
                ...args,
              ],
              env: { GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null" },
            });
          yield* run(["init", "--template", templates, "-b", "work"]);
          yield* run(["commit", "--allow-empty", "-m", "synthetic"]);
          yield* run(["worktree", "add", "-b", "linked", linked]);
          yield* fs.makeDirectory(path.join(repo, "sub"));
          const input: Target.TargetInput = {
            projectId: "project",
            threadId: "thread",
            projectRoot: repo,
            worktreePath: null,
            threadBranch: null,
            projectRevision: "1",
            threadRevision: "1",
          };
          const original = yield* targets.capture(input);
          const throughAlias = yield* targets.capture({ ...input, projectRoot: alias });
          const nested = yield* targets.capture({ ...input, projectRoot: path.join(repo, "sub") });
          const other = yield* targets.capture({ ...input, worktreePath: linked });
          assert.strictEqual(original.checkoutKey, throughAlias.checkoutKey);
          assert.deepEqual(original.directoryIdentity, throughAlias.directoryIdentity);
          assert.strictEqual(nested.checkoutKey, original.checkoutKey);
          assert.notEqual(other.checkoutKey, original.checkoutKey);
          assert.strictEqual(other.gitCommonDirectory, original.gitCommonDirectory);
          const plainInput = { ...input, projectRoot: plain };
          assert.strictEqual((yield* targets.capture(plainInput)).branch, null);
          const plainMarker = path.join(plain, ".git");
          yield* fs.makeDirectory(plainMarker);
          yield* fs.writeFileString(path.join(plainMarker, "HEAD"), "invalid repository identity");
          assert.isTrue(Exit.isFailure(yield* Effect.exit(targets.capture(plainInput))));
          yield* fs.remove(plainMarker, { recursive: true });
          yield* fs.writeFileString(plainMarker, "gitdir: inaccessible", { mode: 0o000 });
          assert.isTrue(Exit.isFailure(yield* Effect.exit(targets.capture(plainInput))));
          yield* fs.remove(plainMarker);
          yield* fs.symlink(path.join(root, "missing-git-directory"), plainMarker);
          assert.isTrue(Exit.isFailure(yield* Effect.exit(targets.capture(plainInput))));
          yield* fs.remove(plainMarker);
          assert.strictEqual((yield* targets.capture(plainInput)).branch, null);
          yield* run(["symbolic-ref", "HEAD", "refs/heads/changed"]);
          assert.isTrue(Exit.isFailure(yield* Effect.exit(targets.revalidate(original))));
          yield* fs.remove(alias);
          yield* fs.symlink(plain, alias);
          assert.isTrue(Exit.isFailure(yield* Effect.exit(targets.revalidate(throughAlias))));
        }).pipe(Effect.provide(testLayer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
