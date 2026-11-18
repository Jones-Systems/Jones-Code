// @effect-diagnostics nodeBuiltinImport:off -- these tests compare the driver's physical device/inode claim with lstat.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as ServerConfig from "../../../config.ts";
import * as CheckoutCloner from "../../../vcs/CheckoutCloner.ts";
import * as Git from "../../../vcs/GitVcsDriver.ts";
import { makeGitVcsDriverCore } from "../../../vcs/GitVcsDriverCore.ts";

const layerTest = ServerConfig.layerTest(process.cwd(), {
  prefix: "jones-observed-worktree-",
}).pipe(Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)));

class SyntheticJournalFailure extends Schema.TaggedError<SyntheticJournalFailure>()(
  "SyntheticJournalFailure",
  { message: Schema.String },
) {}

type Command = { cwd: string; args: ReadonlyArray<string> };
const materialKind = (args: ReadonlyArray<string>) => {
  if (args.includes("-h")) return null;
  if (args.includes("worktree") && args.includes("add")) return "worktree.add";
  if (args[0] === "submodule") return "worktree.submodules";
  if (args[0] === "config" && args.length === 3 && !args[1]!.startsWith("-"))
    return "worktree.base-config";
  if (args[0] === "branch" || args[0] === "update-index" || args[0] === "read-tree")
    return "unobserved";
  return null;
};

const fixture = Effect.fn("observedWorktreeFixture")(function* (failCommand?: string) {
  const fs = yield* FileSystem.FileSystem;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "jones-observed-worktree-fixture-" });
  const realRoot = yield* fs.realPath(root);
  const cwd = NodePath.join(realRoot, "project");
  const worktreePath = NodePath.join(realRoot, "created");
  yield* fs.makeDirectory(cwd);
  const commands: Command[] = [];
  const timeline: string[] = [];
  const delegate = yield* ChildProcessSpawner.ChildProcessSpawner;
  let recording = false;
  const spawner = ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      if (!ChildProcess.isStandardCommand(command)) return yield* delegate.spawn(command);
      const args = command.args.map(String);
      const kind = materialKind(args);
      if (recording) {
        commands.push({ cwd: String(command.options.cwd), args });
        if (kind !== null) timeline.push(`command:${kind}`);
      }
      const handle = yield* delegate.spawn(command);
      // Force the cache's unsupported-Git fallback without depending on the host version
      // or launching asynchronous cache builds in this focused driver route test.
      if (args[0] === "version")
        return ChildProcessSpawner.makeHandle({
          ...handle,
          stdout: Stream.encodeText(Stream.make("git version 2.30.0\n")),
        });
      if (!recording || kind !== failCommand) return handle;
      // The command really ran; a failed completion signal must not trigger a second effect.
      return ChildProcessSpawner.makeHandle({
        ...handle,
        exitCode: handle.exitCode.pipe(Effect.as(ChildProcessSpawner.ExitCode(1))),
      });
    }),
  );
  let cloneCalls = 0;
  const driver = yield* makeGitVcsDriverCore().pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    Effect.provideService(CheckoutCloner.CheckoutCloner, () =>
      Effect.sync(() => {
        cloneCalls++;
      }),
    ),
  );
  const git = (at: string, args: ReadonlyArray<string>) =>
    driver
      .execute({ operation: "observedFixture.git", cwd: at, args })
      .pipe(Effect.map((result) => result.stdout.trim()));
  const init = Effect.fn("observedFixture.init")(function* (at: string) {
    yield* fs.makeDirectory(at, { recursive: true });
    yield* git(at, ["init", "-q", "-b", "main"]);
    yield* git(at, ["config", "user.name", "Synthetic Fixture"]);
    yield* git(at, ["config", "user.email", "fixture@example.invalid"]);
    yield* git(at, ["config", "commit.gpgsign", "false"]);
    yield* fs.writeFileString(NodePath.join(at, "README.md"), "synthetic material\n");
    yield* git(at, ["add", "."]);
    yield* git(at, ["commit", "-qm", "synthetic base"]);
  });
  yield* init(cwd);
  const child = NodePath.join(realRoot, "submodule-source");
  yield* init(child);
  yield* git(cwd, ["-c", "protocol.file.allow=always", "submodule", "add", "-q", child, "child"]);
  yield* fs.writeFileString(NodePath.join(cwd, "t3.json"), '{"worktreeSubmodules":"recursive"}\n');
  yield* git(cwd, ["add", "."]);
  yield* git(cwd, ["commit", "-qm", "submodule and settings"]);
  yield* git(cwd, ["config", "checkout.workers", "1"]);
  yield* git(cwd, ["remote", "add", "origin", child]);
  yield* git(cwd, ["update-ref", "refs/remotes/origin/main", "HEAD"]);
  const oid = yield* git(cwd, ["rev-parse", "HEAD"]);
  const commonDirectory = yield* git(cwd, [
    "rev-parse",
    "--path-format=absolute",
    "--git-common-dir",
  ]);
  const before: Git.LegacyWorktreePreparationStep[] = [];
  const after: Array<{
    step: Git.LegacyWorktreePreparationStep;
    outcome: string;
    material: Git.LegacyWorktreeMaterialClaim | undefined;
  }> = [];
  const hooks: Git.LegacyWorktreePreparationHooks = {
    beforeEffect: (step) =>
      Effect.sync(() => {
        before.push(step);
        timeline.push(`before:${step.kind}`);
      }),
    afterEffect: (step, outcome, material) =>
      Effect.sync(() => {
        after.push({ step, outcome, material });
        timeline.push(`after:${step.kind}:${outcome}`);
      }),
  };
  const input = {
    cwd,
    refName: oid,
    newRefName: "native/observed",
    baseRefName: "origin/main",
    path: worktreePath,
  };
  const create = (options: Git.CreateWorktreeOptions = {}) => {
    recording = true;
    return driver.createWorktree(input, {
      fastCheckout: true,
      legacyPreparation: hooks,
      ...options,
    });
  };
  const createOrdinary = () => {
    recording = true;
    return driver.createWorktree(input, { fastCheckout: true, submodules: "none" });
  };
  return {
    fs,
    cwd,
    worktreePath,
    git,
    input,
    create,
    createOrdinary,
    hooks,
    before,
    after,
    commands,
    timeline,
    oid,
    commonDirectory,
    cloneCalls: () => cloneCalls,
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

describe("original-owner observation through GitVcsDriverCore", () => {
  it.effect.each(["recursive", "top-level"] as const)(
    "awaits exact add, %s submodules and both remote base-config witnesses",
    (submodules) =>
      isolated(
        Effect.gen(function* () {
          const f = yield* fixture();
          const created = yield* f.create({ submodules });
          assert.equal(created.worktree.path, f.worktreePath);
          const expected: Array<Pick<(typeof f.before)[number], "kind" | "cwd" | "args">> = [
            {
              kind: "worktree.add",
              cwd: f.cwd,
              args: [
                "-c",
                "checkout.workers=1",
                "worktree",
                "add",
                "-b",
                f.input.newRefName,
                "--",
                f.worktreePath,
                f.oid,
              ],
            },
            {
              kind: "worktree.submodules",
              cwd: f.worktreePath,
              args: [
                "submodule",
                "update",
                "--init",
                ...(submodules === "recursive" ? ["--recursive"] : []),
              ],
            },
            {
              kind: "worktree.base-config",
              cwd: f.cwd,
              args: ["config", `branch.${f.input.newRefName}.t3-merge-base`, "main"],
            },
            {
              kind: "worktree.base-config",
              cwd: f.cwd,
              args: [
                "config",
                `branch.${f.input.newRefName}.t3-merge-base-ref`,
                "refs/remotes/origin/main",
              ],
            },
          ];
          assert.deepEqual(
            f.before.map(({ kind, cwd, args }) => ({ kind, cwd, args })),
            expected,
          );
          assert.deepEqual(
            f.commands.filter(({ args }) => materialKind(args) !== null),
            expected.map(({ cwd, args }) => ({ cwd, args })),
          );
          assert.deepEqual(
            f.timeline,
            expected.flatMap(({ kind }) => [
              `before:${kind}`,
              `command:${kind}`,
              `after:${kind}:settled_success`,
            ]),
          );
          const stat = yield* Effect.promise(() => NodeFSP.lstat(f.worktreePath));
          for (const [index, receipt] of f.after.entries()) {
            assert.strictEqual(receipt.step, f.before[index]);
            assert.equal(receipt.step.commonDirectory, f.commonDirectory);
            assert.equal(receipt.step.baseCommitOid, f.oid);
            assert.equal(receipt.step.targetRef, `refs/heads/${f.input.newRefName}`);
            assert.isTrue(receipt.step.before?.targetRefAbsent);
            assert.isTrue(receipt.step.before?.registrationAbsent);
            assert.equal(receipt.material?.device, String(stat.dev));
            assert.equal(receipt.material?.inode, String(stat.ino));
            assert.equal(receipt.material?.commonDirectory, f.commonDirectory);
            assert.equal(receipt.material?.registeredPath, f.worktreePath);
            assert.equal(receipt.material?.headOid, f.oid);
            assert.equal(receipt.material?.headRef, `refs/heads/${f.input.newRefName}`);
          }
          assert.equal(
            yield* f.fs.readFileString(NodePath.join(f.worktreePath, "child", "README.md")),
            "synthetic material\n",
          );
          assert.equal(
            yield* f.git(f.cwd, [
              "config",
              "--get",
              `branch.${f.input.newRefName}.t3-merge-base-ref`,
            ]),
            "refs/remotes/origin/main",
          );
          assert.equal(f.cloneCalls(), 0);
          assert.isFalse(
            f.commands.some(({ args }) => args.includes("--no-checkout") || args[0] === "version"),
          );
        }),
      ),
  );

  it.effect.each(["worktree.add", "worktree.submodules", "worktree.base-config"] as const)(
    "refuses %s before its command without cleanup, retry or cache fallback",
    (refused) =>
      isolated(
        Effect.gen(function* () {
          const f = yield* fixture();
          const result = yield* f
            .create({
              legacyPreparation: {
                ...f.hooks,
                beforeEffect: (step) =>
                  f.hooks
                    .beforeEffect(step)
                    .pipe(
                      Effect.andThen(
                        step.kind === refused
                          ? Effect.fail(new SyntheticJournalFailure({ message: "owner refused" }))
                          : Effect.void,
                      ),
                    ),
              },
            })
            .pipe(Effect.result);
          assert.isTrue(Result.isFailure(result));
          assert.isFalse(f.commands.some(({ args }) => materialKind(args) === refused));
          assert.isFalse(f.commands.some(({ args }) => materialKind(args) === "unobserved"));
          assert.isFalse(
            f.commands.some(({ args }) => args.includes("remove") || args.includes("unlock")),
          );
          assert.equal(f.cloneCalls(), 0);
          assert.equal(f.before.filter((step) => step.kind === refused).length, 1);
          assert.isFalse(f.after.some(({ step }) => step.kind === refused));
          assert.equal(yield* f.fs.exists(f.worktreePath), refused !== "worktree.add");
        }),
      ),
  );

  it.effect.each(["worktree.add", "worktree.submodules", "worktree.base-config"] as const)(
    "stops on %s post-effect journal failure, retaining material with no replay",
    (failed) =>
      isolated(
        Effect.gen(function* () {
          const f = yield* fixture();
          let claimed = false;
          const result = yield* f
            .create({
              progress: {
                onWorktreeClaimed: () =>
                  Effect.sync(() => {
                    claimed = true;
                  }),
              },
              legacyPreparation: {
                ...f.hooks,
                afterEffect: (step, outcome, material) =>
                  f.hooks
                    .afterEffect(step, outcome, material)
                    .pipe(
                      Effect.andThen(
                        step.kind === failed
                          ? Effect.fail(
                              new SyntheticJournalFailure({ message: "journal did not settle" }),
                            )
                          : Effect.void,
                      ),
                    ),
              },
            })
            .pipe(Effect.result);
          assert.isTrue(Result.isFailure(result));
          assert.isTrue(yield* f.fs.exists(f.worktreePath));
          assert.equal(f.commands.filter(({ args }) => materialKind(args) === failed).length, 1);
          assert.equal(f.after.at(-1)?.step.kind, failed);
          assert.equal(f.after.at(-1)?.outcome, "settled_success");
          assert.equal(f.after.at(-1)?.material?.path, f.worktreePath);
          assert.equal(claimed, failed !== "worktree.add");
          assert.isFalse(f.commands.some(({ args }) => materialKind(args) === "unobserved"));
          assert.equal(f.cloneCalls(), 0);
          // A new attempt may inspect, but cannot invoke add again over retained material.
          const previousCommands = f.commands.filter(
            ({ args }) => materialKind(args) !== null,
          ).length;
          const previousHooks = f.before.length;
          assert.isTrue(Result.isFailure(yield* f.create().pipe(Effect.result)));
          assert.equal(
            f.commands.filter(({ args }) => materialKind(args) !== null).length,
            previousCommands,
          );
          assert.equal(f.before.length, previousHooks);
        }),
      ),
  );

  it.effect.each(["worktree.add", "worktree.submodules", "worktree.base-config"] as const)(
    "a failed %s completion reports uncertainty without retry or later mutations",
    (failed) =>
      isolated(
        Effect.gen(function* () {
          const f = yield* fixture(failed);
          assert.isTrue(Result.isFailure(yield* f.create().pipe(Effect.result)));
          const expected = ["worktree.add", "worktree.submodules", "worktree.base-config"];
          assert.deepEqual(
            f.commands.map(({ args }) => materialKind(args)).filter((kind) => kind !== null),
            expected.slice(0, expected.indexOf(failed) + 1),
          );
          assert.equal(f.after.at(-1)?.outcome, "failed_or_unknown");
          assert.equal(f.after.at(-1)?.material?.path, f.worktreePath);
          assert.isTrue(yield* f.fs.exists(f.worktreePath));
          assert.equal(f.cloneCalls(), 0);
        }),
      ),
  );

  it.effect("rejects replacement material before another observed command can run", () =>
    isolated(
      Effect.gen(function* () {
        const f = yield* fixture();
        const retained = `${f.worktreePath}-retained`;
        const result = yield* f
          .create({
            submodules: "none",
            legacyPreparation: {
              ...f.hooks,
              afterEffect: (step, outcome, material) =>
                Effect.gen(function* () {
                  yield* f.hooks.afterEffect(step, outcome, material);
                  if (step.kind === "worktree.add") {
                    const pointer = yield* f.fs.readFileString(
                      NodePath.join(f.worktreePath, ".git"),
                    );
                    yield* f.fs.rename(f.worktreePath, retained);
                    yield* f.fs.makeDirectory(f.worktreePath);
                    yield* f.fs.writeFileString(NodePath.join(f.worktreePath, ".git"), pointer);
                  }
                }),
            },
          })
          .pipe(Effect.result);
        assert.isTrue(Result.isFailure(result));
        assert.deepEqual(
          f.before.map((step) => step.kind),
          ["worktree.add"],
        );
        assert.deepEqual(
          f.commands.map(({ args }) => materialKind(args)).filter((kind) => kind !== null),
          ["worktree.add"],
        );
        assert.isTrue(yield* f.fs.exists(retained));
        assert.isTrue(yield* f.fs.exists(f.worktreePath));
      }),
    ),
  );

  it.effect("observes created material before any progress callback or follow-up mutation", () =>
    isolated(
      Effect.gen(function* () {
        const f = yield* fixture();
        let journalSettled = false;
        yield* f.create({
          submodules: "none",
          legacyPreparation: {
            ...f.hooks,
            afterEffect: (step, outcome, material) =>
              Effect.gen(function* () {
                yield* f.hooks.afterEffect(step, outcome, material);
                if (step.kind === "worktree.add") {
                  assert.isTrue(yield* f.fs.exists(f.worktreePath));
                  assert.equal(
                    f.commands.filter(({ args }) => materialKind(args) !== null).length,
                    1,
                  );
                  journalSettled = true;
                }
              }),
          },
          progress: { onWorktreeClaimed: () => Effect.sync(() => assert.isTrue(journalSettled)) },
        });
        assert.deepEqual(
          f.before.map((step) => step.kind),
          ["worktree.add", "worktree.base-config", "worktree.base-config"],
        );
      }),
    ),
  );

  it.effect("ordinary fast-checkout calls still enter the cache and upstream branch owner", () =>
    isolated(
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.createOrdinary();
        assert.isEmpty(f.before);
        assert.isEmpty(f.after);
        assert.isTrue(f.commands.some(({ args }) => args[0] === "version"));
        assert.isTrue(
          f.commands.some(({ args }) => args[0] === "branch" && args.includes(f.input.newRefName)),
        );
        const add = f.commands.find(({ args }) => materialKind(args) === "worktree.add");
        assert.isDefined(add);
        assert.notInclude(add!.args, "-b");
        assert.isTrue(yield* f.fs.exists(f.worktreePath));
      }),
    ),
  );
});
