import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { GitCommandError, ProjectId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";

import type { DeletionWorktreeRemovalStartV1 } from "../orchestration-v2/EventSink.ts";
import * as ServerConfig from "../config.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import {
  DeletionWorktreeRemovalPreconditionError,
  makeDeletionWorktreeRemoval,
} from "./DeletionWorktreeRemoval.ts";

const testLayer = GitVcsDriver.layer.pipe(
  Layer.provideMerge(
    ServerConfig.layerTest(process.cwd(), { prefix: "t3-deletion-worktree-config-" }),
  ),
  Layer.provideMerge(NodeServices.layer),
);

// The scoped allocation registers exact-root cleanup before the first Git command.
// Every repository and linked worktree is below that root, including failed runs.
const makeFixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const git = yield* GitVcsDriver.GitVcsDriver;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-deletion-worktree-" });
  const projectRoot = path.join(root, "repository");
  const targetPath = path.join(root, "target [worktree]");
  const run = (args: ReadonlyArray<string>) =>
    git.execute({
      operation: "DeletionWorktreeRemoval.test.fixture",
      cwd: root,
      args,
      timeoutMs: 10_000,
    });
  yield* run(["-c", "init.templateDir=", "init", "--initial-branch=main", projectRoot]);
  yield* run([
    "-C",
    projectRoot,
    "-c",
    "user.name=Deletion Fixture",
    "-c",
    "user.email=fixture@example.test",
    "-c",
    "commit.gpgsign=false",
    "commit",
    "--allow-empty",
    "-m",
    "fixture",
  ]);
  yield* run(["-C", projectRoot, "worktree", "add", "-b", "deletion-target", targetPath]);
  const start: DeletionWorktreeRemovalStartV1 = {
    schema: "t3.deletion-worktree-removal-start/v1",
    effectId: "fixture-removal-effect",
    bindingSha256: "a".repeat(64),
    workerId: "fixture-worker",
    expectedAttempt: 2,
    target: {
      projectId: ProjectId.make("fixture-project"),
      projectRoot,
      path: targetPath,
      branch: "deletion-target",
      force: false,
    },
    startedAt: "2026-10-03T00:00:00.000Z",
  };
  return { root, projectRoot, targetPath, fs, git, run, start };
});

it.layer(testLayer)("Deletion worktree removal producer", (it) => {
  it.effect(
    "removes an exact started worktree and observes both registration and filesystem absence",
    () =>
      Effect.gen(function* () {
        const fixture = yield* makeFixture;
        const producer = yield* makeDeletionWorktreeRemoval;
        const sequence: string[] = [];
        const observedGit = {
          ...fixture.git,
          execute: (input: GitVcsDriver.ExecuteGitInput) => {
            if (input.args[0] === "worktree" && input.args[1] === "remove") sequence.push("remove");
            return fixture.git.execute(input);
          },
        };
        const observedProducer = yield* makeDeletionWorktreeRemoval.pipe(
          Effect.provideService(GitVcsDriver.GitVcsDriver, observedGit),
        );
        const result = yield* observedProducer.executeStarted(fixture.start, 0, (start, ordinal) =>
          Effect.sync(() => {
            assert.deepEqual(start, fixture.start);
            assert.strictEqual(ordinal, 0);
            sequence.push("revalidate");
          }),
        );
        assert.deepEqual(sequence, ["revalidate", "remove"]);
        assert.deepEqual(result.operation, { kind: "executed", completion: "exited", exitCode: 0 });
        assert.strictEqual(result.before.filesystem.status, "present");
        assert.deepEqual(result.after.filesystem, { status: "absent", path: fixture.targetPath });
        assert.strictEqual(result.before.registration.status, "complete");
        assert.strictEqual(result.after.registration.status, "complete");
        if (
          result.before.registration.status === "complete" &&
          result.after.registration.status === "complete"
        ) {
          assert.strictEqual(
            result.before.registration.gitCommonDirectory,
            result.after.registration.gitCommonDirectory,
          );
          assert.isTrue(
            result.before.registration.entries.some(
              (entry) =>
                entry.path === fixture.targetPath && entry.branch === "refs/heads/deletion-target",
            ),
          );
          assert.isFalse(
            result.after.registration.entries.some((entry) => entry.path === fixture.targetPath),
          );
        }
        assert.isTrue(Object.isFrozen(result));
        assert.isTrue(Object.isFrozen(result.start.target));
        assert.isTrue(yield* fixture.fs.exists(fixture.projectRoot));
        assert.strictEqual(
          (yield* producer.inspectTarget(fixture.start.target)).filesystem.status,
          "absent",
        );
      }).pipe(Effect.scoped),
  );

  it.effect("keeps policy force=false on a dirty worktree instead of escalating to force", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      const path = yield* Path.Path;
      yield* fixture.fs.writeFileString(
        path.join(fixture.targetPath, "untracked.txt"),
        "preserve\n",
      );
      const producer = yield* makeDeletionWorktreeRemoval;
      const result = yield* producer.executeStarted(fixture.start, 0, () => Effect.void);
      assert.strictEqual(result.operation.kind, "executed");
      if (result.operation.kind === "executed") {
        assert.strictEqual(result.operation.completion, "exited");
        assert.notStrictEqual(result.operation.exitCode, 0);
      }
      assert.strictEqual(result.after.filesystem.status, "present");
      assert.strictEqual(
        yield* fixture.fs.readFileString(path.join(fixture.targetPath, "untracked.txt")),
        "preserve\n",
      );
    }).pipe(Effect.scoped),
  );

  it.effect("uses the exact recorded force=true consent for a dirty worktree", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      const path = yield* Path.Path;
      yield* fixture.fs.writeFileString(
        path.join(fixture.targetPath, "untracked.txt"),
        "fixture-only\n",
      );
      const producer = yield* makeDeletionWorktreeRemoval;
      const start = { ...fixture.start, target: { ...fixture.start.target, force: true } };
      const result = yield* producer.executeStarted(start, 0, () => Effect.void);
      assert.deepEqual(result.operation, { kind: "executed", completion: "exited", exitCode: 0 });
      assert.strictEqual(result.start.target.force, true);
      assert.deepEqual(result.after.filesystem, { status: "absent", path: fixture.targetPath });
    }).pipe(Effect.scoped),
  );

  it.effect("does not remove after recorded-start revalidation fails", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      const producer = yield* makeDeletionWorktreeRemoval;
      const result = yield* producer
        .executeStarted(fixture.start, 0, () => Effect.fail("fixture-stale-attempt"))
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
      const readback = yield* producer.inspectTarget(fixture.start.target);
      assert.strictEqual(readback.filesystem.status, "present");
      assert.strictEqual(readback.registration.status, "complete");
      if (readback.registration.status === "complete")
        assert.isTrue(
          readback.registration.entries.some((entry) => entry.path === fixture.targetPath),
        );
    }).pipe(Effect.scoped),
  );

  it.effect("rejects a different registered branch before revalidating or removing", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      const producer = yield* makeDeletionWorktreeRemoval;
      const wrong = {
        ...fixture.start,
        target: { ...fixture.start.target, branch: "replacement-branch" },
      };
      const error = yield* producer
        .executeStarted(wrong, 0, () => Effect.die("mismatched target must not reach revalidation"))
        .pipe(Effect.flip);
      assert.instanceOf(error, DeletionWorktreeRemovalPreconditionError);
      assert.strictEqual(error.reason, "registered_target_mismatch");
      assert.strictEqual(
        (yield* producer.inspectTarget(fixture.start.target)).filesystem.status,
        "present",
      );
    }).pipe(Effect.scoped),
  );

  it.effect(
    "observes recovered starts without replaying removal, including an already vanished target",
    () =>
      Effect.gen(function* () {
        const fixture = yield* makeFixture;
        let removalCalls = 0;
        const observedGit = {
          ...fixture.git,
          execute: (input: GitVcsDriver.ExecuteGitInput) => {
            if (input.args[0] === "worktree" && input.args[1] === "remove") {
              removalCalls += 1;
              return Effect.die("recovery may only observe");
            }
            return fixture.git.execute(input);
          },
        };
        const producer = yield* makeDeletionWorktreeRemoval.pipe(
          Effect.provideService(GitVcsDriver.GitVcsDriver, observedGit),
        );
        const present = yield* producer.observeStarted(fixture.start, 0);
        assert.deepEqual(present.operation, { kind: "reconciled", completion: "unknown" });
        assert.strictEqual(present.after.filesystem.status, "present");
        yield* fixture.run([
          "-C",
          fixture.projectRoot,
          "worktree",
          "remove",
          "--",
          fixture.targetPath,
        ]);
        const absent = yield* producer.observeStarted(fixture.start, 0);
        assert.deepEqual(absent.operation, { kind: "reconciled", completion: "unknown" });
        assert.strictEqual(absent.after.filesystem.status, "absent");
        assert.strictEqual(removalCalls, 0);
      }).pipe(Effect.scoped),
  );

  it.effect("revalidates an originally absent target without invoking removal", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      yield* fixture.run([
        "-C",
        fixture.projectRoot,
        "worktree",
        "remove",
        "--",
        fixture.targetPath,
      ]);
      const producer = yield* makeDeletionWorktreeRemoval;
      let validated = false;
      const result = yield* producer.executeStarted(fixture.start, 0, () =>
        Effect.sync(() => {
          validated = true;
        }),
      );
      assert.isTrue(validated);
      assert.deepEqual(result.operation, { kind: "already_absent", completion: "not_invoked" });
      assert.strictEqual(result.before.filesystem.status, "absent");
      assert.strictEqual(result.after.filesystem.status, "absent");
    }).pipe(Effect.scoped),
  );

  it.effect(
    "keeps a lost removal response unknown and recovers through readback without another removal",
    () =>
      Effect.gen(function* () {
        const fixture = yield* makeFixture;
        let removalCalls = 0;
        const observedGit = {
          ...fixture.git,
          execute: (input: GitVcsDriver.ExecuteGitInput) => {
            if (input.args[0] !== "worktree" || input.args[1] !== "remove")
              return fixture.git.execute(input);
            removalCalls += 1;
            return fixture.git.execute(input).pipe(
              Effect.andThen(
                Effect.fail(
                  new GitCommandError({
                    operation: input.operation,
                    command: "git",
                    cwd: input.cwd,
                    argumentCount: input.args.length,
                    detail: "Fixture lost the response after removal.",
                  }),
                ),
              ),
            );
          },
        };
        const producer = yield* makeDeletionWorktreeRemoval.pipe(
          Effect.provideService(GitVcsDriver.GitVcsDriver, observedGit),
        );
        const uncertain = yield* producer.executeStarted(fixture.start, 0, () => Effect.void);
        assert.deepEqual(uncertain.operation, {
          kind: "executed",
          completion: "unknown",
          exitCode: null,
        });
        assert.strictEqual(uncertain.after.filesystem.status, "absent");
        const recovered = yield* producer.observeStarted(fixture.start, 0);
        assert.deepEqual(recovered.operation, { kind: "reconciled", completion: "unknown" });
        assert.strictEqual(recovered.after.filesystem.status, "absent");
        assert.strictEqual(removalCalls, 1);
      }).pipe(Effect.scoped),
  );

  it.effect(
    "reports remaining target state when an invocation returns exit zero without removing it",
    () =>
      Effect.gen(function* () {
        const fixture = yield* makeFixture;
        const observedGit = {
          ...fixture.git,
          execute: (input: GitVcsDriver.ExecuteGitInput) => {
            if (input.args[0] === "worktree" && input.args[1] === "remove")
              return fixture.git.execute({
                ...input,
                args: ["rev-parse", "HEAD"],
              });
            return fixture.git.execute(input);
          },
        };
        const producer = yield* makeDeletionWorktreeRemoval.pipe(
          Effect.provideService(GitVcsDriver.GitVcsDriver, observedGit),
        );
        const result = yield* producer.executeStarted(fixture.start, 0, () => Effect.void);
        assert.deepEqual(result.operation, { kind: "executed", completion: "exited", exitCode: 0 });
        assert.strictEqual(result.after.filesystem.status, "present");
        assert.strictEqual(result.after.registration.status, "complete");
        if (result.after.registration.status === "complete")
          assert.isTrue(
            result.after.registration.entries.some((entry) => entry.path === fixture.targetPath),
          );
      }).pipe(Effect.scoped),
  );

  for (const defect of ["truncated", "malformed", "stderr"] as const) {
    it.effect(`holds ${defect} registration readback instead of treating it as absence`, () =>
      Effect.gen(function* () {
        const fixture = yield* makeFixture;
        const observedGit = {
          ...fixture.git,
          execute: (input: GitVcsDriver.ExecuteGitInput) =>
            fixture.git.execute(input).pipe(
              Effect.map((result) =>
                input.args[0] === "worktree" && input.args[1] === "list"
                  ? {
                      ...result,
                      ...(defect === "truncated"
                        ? { stdoutTruncated: true }
                        : defect === "malformed"
                          ? { stdout: result.stdout.slice(0, -1) }
                          : { stderr: "fixture-read-warning" }),
                    }
                  : result,
              ),
            ),
        };
        const producer = yield* makeDeletionWorktreeRemoval.pipe(
          Effect.provideService(GitVcsDriver.GitVcsDriver, observedGit),
        );
        const readback = yield* producer.inspectTarget(fixture.start.target);
        assert.strictEqual(readback.registration.status, "unavailable");
        assert.strictEqual(readback.filesystem.status, "present");
        const error = yield* producer
          .executeStarted(fixture.start, 0, () =>
            Effect.die("incomplete registration must not reach removal"),
          )
          .pipe(Effect.flip);
        assert.strictEqual(error.reason, "target_readback_unavailable");
      }).pipe(Effect.scoped),
    );
  }

  it.effect("distinguishes a failed lstat from direct ENOENT absence", () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      const producer = yield* makeDeletionWorktreeRemoval;
      const invalid = { ...fixture.start.target, path: `${fixture.targetPath}\0invalid` };
      const failed = yield* producer.inspectTarget(invalid);
      assert.deepEqual(failed.filesystem, {
        status: "unavailable",
        path: invalid.path,
        reason: "lstat_failed",
      });
      const missingPath = `${fixture.targetPath}-missing`;
      const missing = yield* producer.inspectTarget({ ...fixture.start.target, path: missingPath });
      assert.deepEqual(missing.filesystem, { status: "absent", path: missingPath });
    }).pipe(Effect.scoped),
  );

  it.effect("cleans the exact task-owned fixture after success, failure and cancellation", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      let successfulRoot = "";
      yield* Effect.gen(function* () {
        successfulRoot = (yield* makeFixture).root;
      }).pipe(Effect.scoped);
      assert.isFalse(yield* fs.exists(successfulRoot));
      let failedRoot = "";
      const failure = yield* Effect.gen(function* () {
        failedRoot = (yield* makeFixture).root;
        return yield* Effect.fail("fixture-intended-failure");
      }).pipe(Effect.scoped, Effect.exit);
      assert.isTrue(Exit.isFailure(failure));
      assert.isFalse(yield* fs.exists(failedRoot));
      const ready = yield* Deferred.make<string>();
      const fiber = yield* Effect.gen(function* () {
        const fixture = yield* makeFixture;
        yield* Deferred.succeed(ready, fixture.root);
        return yield* Effect.never;
      }).pipe(Effect.scoped, Effect.forkChild);
      const cancelledRoot = yield* Deferred.await(ready);
      yield* Fiber.interrupt(fiber);
      assert.isFalse(yield* fs.exists(cancelledRoot));
    }),
  );
});
