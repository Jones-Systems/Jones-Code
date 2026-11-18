import * as NodeNet from "node:net";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Exit from "effect/Exit";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as Metric from "effect/Metric";
import { assert, it } from "@effect/vitest";

import { CheckpointRef, GitCommandError, VcsProcessExitError } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { symlinksSupported } from "@t3tools/shared/testing/symlinks";
import * as ServerConfig from "../config.ts";
import { gitCommandDuration, gitCommandsTotal } from "../observability/Metrics.ts";
import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as GitVcsDriver from "./GitVcsDriver.ts";
import type * as VcsDriver from "./VcsDriver.ts";
import * as VcsDriverRegistry from "./VcsDriverRegistry.ts";
import * as VcsProcess from "./VcsProcess.ts";
import * as CheckoutCloner from "./CheckoutCloner.ts";
import { runVcsDriverContractSuite } from "./testing/VcsDriverContractHarness.ts";

const layerServerConfig = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-git-vcs-contract-",
});
const layerGitContract = Layer.mergeAll(GitVcsDriver.layerVcs, GitVcsDriver.layer).pipe(
  Layer.provide(layerServerConfig),
  Layer.provideMerge(VcsProcess.layer),
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);
const layerGitCaptureContract = Layer.merge(
  layerGitContract,
  ProcessRunner.layer.pipe(Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

const runGit = (cwd: string, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const driver = yield* GitVcsDriver.GitVcsDriver;
    yield* driver.execute({
      operation: "GitVcsDriver.contract.git",
      cwd,
      args,
      timeoutMs: 10_000,
    });
  });

it.effect(
  "remote tracking commit lookup reads the exact local ref, distinguishes absence, and never fetches",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-remote-tracking-lookup-" });
      const driver = yield* GitVcsDriver.GitVcsDriver;
      for (const args of [
        ["init", "-q", "-b", "main"],
        [
          "-c",
          "user.name=Synthetic",
          "-c",
          "user.email=fixture@example.invalid",
          "-c",
          "commit.gpgsign=false",
          "commit",
          "--allow-empty",
          "-qm",
          "base",
        ],
        ["remote", "add", "team/origin", "https://fixture.invalid/never-fetch.git"],
        ["update-ref", "refs/remotes/team/origin/topic/nested", "HEAD"],
      ])
        yield* driver.execute({ operation: "test.trackingRefSetup", cwd, args });
      const oid = (yield* driver.execute({
        operation: "test.trackingRefHead",
        cwd,
        args: ["rev-parse", "HEAD"],
      })).stdout.trim();
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const commands: ReadonlyArray<string>[] = [];
      const observed = yield* GitVcsDriver.make.pipe(
        Effect.provide(Layer.merge(layerServerConfig, CheckoutCloner.layer)),
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make((command) => {
            if (ChildProcess.isStandardCommand(command)) commands.push(command.args.map(String));
            return spawner.spawn(command);
          }),
        ),
      );
      assert.deepEqual(
        yield* observed.resolveRemoteTrackingCommitIfExists({
          cwd,
          remoteName: "team/origin",
          branchName: "topic/nested",
        }),
        { commitSha: oid, remoteRefName: "team/origin/topic/nested" },
      );
      assert.isNull(
        yield* observed.resolveRemoteTrackingCommitIfExists({
          cwd,
          remoteName: "team/origin",
          branchName: "topic",
        }),
      );
      assert.isFalse(commands.some((args) => args.includes("fetch")));
      const invalid = yield* observed
        .resolveRemoteTrackingCommitIfExists({
          cwd,
          remoteName: "team/origin",
          branchName: "topic/*",
        })
        .pipe(Effect.result);
      assert.equal(invalid._tag, "Failure");
      assert.isTrue(
        commands.some((args) => args.includes("refs/remotes/team/origin/topic/nested^{commit}")),
      );
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect(
  "remote tracking lookup preserves repository errors instead of reporting a missing ref",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-remote-tracking-non-repo-" });
      const driver = yield* GitVcsDriver.GitVcsDriver;
      const result = yield* driver
        .resolveRemoteTrackingCommitIfExists({
          cwd,
          remoteName: "origin",
          branchName: "main",
        })
        .pipe(Effect.result);
      if (result._tag === "Success") return assert.fail("Expected typed repository error");
      assert.equal(result.failure._tag, "GitCommandError");
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

const makeCaptureStore = Effect.fn("test.makeCaptureStore")(function* (
  driver: VcsDriver.VcsDriver["Service"],
  cwd: string,
) {
  const repository = yield* driver.detectRepository(cwd);
  if (repository === null) return yield* Effect.die("Expected a test Git repository");
  const handle = { kind: repository.kind, repository, driver };
  return yield* CheckpointStore.make.pipe(
    Effect.provideService(VcsDriverRegistry.VcsDriverRegistry, {
      get: () => Effect.succeed(driver),
      detect: () => Effect.succeed(handle),
      resolve: () => Effect.succeed(handle),
    }),
  );
});

type GitContractError = GitCommandError | PlatformError.PlatformError;

runVcsDriverContractSuite<GitVcsDriver.GitVcsDriver, GitContractError>({
  name: "Git",
  kind: "git",
  layer: layerGitContract,
  fixture: {
    createRepo: (cwd) =>
      Effect.gen(function* () {
        yield* runGit(cwd, ["init"]);
        yield* runGit(cwd, ["config", "user.email", "test@test.com"]);
        yield* runGit(cwd, ["config", "user.name", "Test"]);
      }),
    writeFile: (cwd, relativePath, contents) =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const absolutePath = path.join(cwd, relativePath);
        yield* fileSystem.makeDirectory(path.dirname(absolutePath), { recursive: true });
        yield* fileSystem.writeFileString(absolutePath, contents);
      }),
    trackFile: (cwd, relativePath) => runGit(cwd, ["add", relativePath]),
    commit: (cwd, message) => runGit(cwd, ["commit", "-m", message]),
    ignorePath: (cwd, pattern) =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        yield* fileSystem.writeFileString(path.join(cwd, ".gitignore"), `${pattern}\n`);
      }),
  },
});

it.effect.each([
  {
    fetchUrl: "https://github.com/pingdotgg/t3code.git",
    pushUrl: "git@github.com:octocat/t3code.git",
    lineEnding: "\n",
  },
  { fetchUrl: "../team repos/shared.git", pushUrl: "../team repos/push.git", lineEnding: "\n" },
  {
    fetchUrl: " ../team repos/shared (fetch) [blob:none].git ",
    pushUrl: "../team repos/push (push).git\t",
    lineEnding: "\r\n",
  },
])(
  "lists the fetch and push URLs of a partial clone remote (%j)",
  ({ fetchUrl, pushUrl, lineEnding }) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const cwd = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-partial-clone-remotes-",
      });
      const process = yield* VcsProcess.VcsProcess;
      const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provideService(VcsProcess.VcsProcess, {
          run: (input) =>
            process.run(input).pipe(
              Effect.map((result) => ({
                ...result,
                stdout: result.stdout.replace(/\r?\n/g, lineEnding),
              })),
            ),
        }),
      );
      yield* runGit(cwd, ["init"]);
      yield* runGit(cwd, ["remote", "add", "origin", fetchUrl]);
      yield* runGit(cwd, ["remote", "set-url", "--push", "origin", pushUrl]);
      yield* runGit(cwd, ["config", "remote.origin.promisor", "true"]);
      yield* runGit(cwd, ["config", "remote.origin.partialclonefilter", "blob:none"]);

      const result = yield* driver.listRemotes(cwd);
      assert.deepEqual(result.remotes, [
        {
          name: "origin",
          url: fetchUrl,
          pushUrl: Option.some(pushUrl),
          isPrimary: true,
        },
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

interface UntrackedBlobChange {
  readonly git: Effect.Success<ReturnType<typeof makeCheckpointFixture>>["git"];
  readonly writeGitPath: (
    gitPath: string,
    contents: string,
  ) => Effect.Effect<
    void,
    Effect.Error<ReturnType<UntrackedBlobChange["git"]>> | PlatformError.PlatformError
  >;
  readonly write: (
    relativePath: string,
    contents: string,
  ) => Effect.Effect<void, PlatformError.PlatformError>;
  readonly checkpointRef: CheckpointRef;
}

const makeCheckpointFixture = Effect.fn("makeCheckpointFixture")(function* (
  driver: Effect.Success<ReturnType<typeof GitVcsDriver.makeVcsDriverShape>>,
  root: string,
  initArgs: ReadonlyArray<string> = [],
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const repository = path.join(root, "repository");
  const cwd = path.join(root, "worktree");
  yield* fileSystem.makeDirectory(repository);
  const primaryGit = (args: ReadonlyArray<string>) =>
    driver.execute({ operation: "checkpoint-test", cwd: repository, args });
  yield* primaryGit(["init", ...initArgs]);
  yield* primaryGit(["config", "user.name", "Test"]);
  yield* primaryGit(["config", "user.email", "test@test.com"]);
  yield* fileSystem.writeFileString(path.join(repository, "file.txt"), "initial\n");
  yield* primaryGit(["add", "."]);
  yield* primaryGit(["commit", "-m", "initial"]);
  yield* primaryGit(["worktree", "add", "-b", "checkpoint-test", cwd]);
  const git = (args: ReadonlyArray<string>) =>
    driver.execute({ operation: "checkpoint-test", cwd, args });
  const gitDir = (yield* git(["rev-parse", "--absolute-git-dir"])).stdout.trim();
  const checkpointRef = CheckpointRef.make("refs/t3/checkpoints/test");
  yield* fileSystem.writeFileString(path.join(cwd, "file.txt"), "staged\n");
  yield* git(["add", "."]);
  yield* fileSystem.writeFileString(path.join(cwd, "file.txt"), "unstaged\n");
  const gitCommonDir = (yield* git([
    "rev-parse",
    "--path-format=absolute",
    "--git-common-dir",
  ])).stdout.slice(0, -1);
  return { cwd, git, gitDir, gitCommonDir, repository, checkpointRef };
});

it.effect.each(["interrupt", "timeout", "success"] as const)(
  "checkpoint bulk staging cleans operation-owned packs after $0 and can retry",
  (stop) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const liveProcess = yield* VcsProcess.VcsProcess;
      const sandbox = yield* fs.makeTempDirectoryScoped({ prefix: "t3-bulk-checkpoint-" });
      const fixtureRoot = path.join(sandbox, "repo");
      yield* fs.makeDirectory(fixtureRoot);
      const { cwd, git, gitDir, gitCommonDir, checkpointRef } = yield* makeCheckpointFixture(
        driver,
        fixtureRoot,
      );
      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      const originalCheckpoint = (yield* git(["rev-parse", checkpointRef])).stdout;
      const originalIndex = yield* fs.readFile(path.join(gitDir, "index"));
      const originalHead = (yield* git(["rev-parse", "HEAD"])).stdout;
      yield* git(["config", "core.bigFileThreshold", "1m"]);
      const crypto = yield* SecureRandom.SecureRandom;
      const payload = yield* crypto.randomBytes(2 * 1024 * 1024);
      yield* fs.writeFile(path.join(cwd, "a-media.bin"), payload);

      // Git keeps bulk-checkin's pack open while the next path's clean filter runs.
      // The socket is a barrier, not a race against the speed of compression.
      const connected = Promise.withResolvers<NodeNet.Socket>();
      const sockets = new Set<NodeNet.Socket>();
      let released = false;
      const server = yield* Effect.acquireRelease(
        Effect.sync(() =>
          NodeNet.createServer((socket) => {
            sockets.add(socket);
            connected.resolve(socket);
            // Git can run the filter again while refreshing the staged entry.
            if (released) socket.end();
          }),
        ),
        (server) =>
          Effect.sync(() => {
            for (const socket of sockets) socket.destroy();
            server.close();
          }),
      );
      yield* Effect.promise(
        () =>
          new Promise<void>((resolve, reject) => {
            server.once("error", reject);
            server.listen(0, "127.0.0.1", resolve);
          }),
      );
      const address = server.address();
      assert.isTrue(address !== null && typeof address !== "string");
      if (address === null || typeof address === "string") return;
      const filter = path.join(sandbox, "filter.cjs");
      yield* fs.writeFileString(
        filter,
        `const s = require('node:net').connect(${address.port}, '127.0.0.1'); s.resume(); process.stdin.resume(); s.on('end', () => process.exit(0));`,
      );
      yield* git(["config", "filter.barrier.clean", `"${process.execPath}" "${filter}"`]);
      yield* git(["config", "filter.barrier.required", "true"]);
      yield* fs.writeFileString(path.join(cwd, ".gitattributes"), "z-gate filter=barrier\n");
      yield* fs.writeFileString(path.join(cwd, "z-gate"), "gate\n");
      const sharedPack = path.join(gitCommonDir, "objects/pack/tmp_pack_other_operation");
      yield* fs.writeFileString(sharedPack, "owned by another Git operation");
      let objectDirectory = path.join(gitCommonDir, "objects");
      const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provideService(VcsProcess.VcsProcess, {
          run: (input) => {
            if (input.args.includes("add") && input.env?.GIT_OBJECT_DIRECTORY)
              objectDirectory = input.env.GIT_OBJECT_DIRECTORY;
            return liveProcess.run(input);
          },
        }),
      );
      const fiber = yield* captureDriver.checkpoints
        .captureCheckpoint({ cwd, checkpointRef })
        .pipe(Effect.exit, Effect.forkScoped);
      const socket = yield* Effect.promise(() => connected.promise).pipe(
        Effect.raceFirst(
          Fiber.join(fiber).pipe(
            Effect.flatMap((result) =>
              Exit.isFailure(result)
                ? Effect.failCause(result.cause)
                : Effect.die("capture ended before the filter connected"),
            ),
          ),
        ),
      );
      // Batch fsync can put the open pack in a nested tmp_objdir before migration.
      // Traverse only this capture's private store while the filter holds it open.
      const readOwnedPacks = Effect.fn(function* (
        directory: string,
      ): Effect.fn.Return<string[], PlatformError.PlatformError> {
        const packs: string[] = [];
        for (const name of yield* fs.readDirectory(directory)) {
          const entry = path.join(directory, name);
          if (name === "pack") {
            for (const pack of yield* fs.readDirectory(entry)) {
              if (pack.startsWith("tmp_pack_")) packs.push(path.join(entry, pack));
            }
          } else if (name.startsWith("tmp_objdir-")) {
            packs.push(...(yield* readOwnedPacks(entry)));
          }
        }
        return packs;
      });
      assert.notStrictEqual(objectDirectory, path.join(gitCommonDir, "objects"));
      assert.isTrue(yield* fs.exists(objectDirectory));
      const packs = yield* readOwnedPacks(objectDirectory);
      assert.strictEqual(packs.length, 1);
      assert.isAbove(Number((yield* fs.stat(packs[0]!)).size), 1024 * 1024);
      // An overlapping capture publishes the same large blob while the first
      // still owns an unfinished pack. Its objects/ref must survive either exit.
      const concurrentRef = CheckpointRef.make("refs/t3/checkpoints/concurrent");
      const concurrentDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provideService(VcsProcess.VcsProcess, {
          run: (input) =>
            liveProcess.run({ ...input, args: ["-c", "filter.barrier.clean=cat", ...input.args] }),
        }),
      );
      yield* concurrentDriver.checkpoints.captureCheckpoint({ cwd, checkpointRef: concurrentRef });
      const concurrentOid = (yield* git(["rev-parse", concurrentRef])).stdout;
      const sharedPackNames = (yield* fs.readDirectory(
        path.join(gitCommonDir, "objects/pack"),
      )).sort();
      const packName = sharedPackNames.find((name) => name.endsWith(".pack"));
      assert.isDefined(packName);
      const sharedPackBytes = yield* fs.readFile(
        path.join(gitCommonDir, "objects/pack", packName!),
      );
      if (stop === "interrupt") yield* Fiber.interrupt(fiber);
      else if (stop === "timeout") yield* TestClock.adjust("30 seconds");
      released = true;
      socket.end();
      const result = yield* Fiber.await(fiber);
      if (stop === "interrupt")
        assert.isTrue(Exit.isFailure(result) && Cause.hasInterruptsOnly(result.cause));
      else if (stop === "timeout") {
        assert.isTrue(Exit.isSuccess(result));
        if (Exit.isSuccess(result)) {
          assert.isTrue(Exit.isFailure(result.value));
          if (Exit.isFailure(result.value)) {
            const error = Cause.findErrorOption(result.value.cause);
            assert.isTrue(Option.isSome(error) && error.value._tag === "VcsProcessTimeoutError");
          }
        }
      } else assert.isTrue(Exit.isSuccess(result) && Exit.isSuccess(result.value));
      assert.deepEqual(
        (yield* fs.readDirectory(path.join(gitCommonDir, "objects/pack"))).sort(),
        sharedPackNames,
      );
      assert.deepEqual(
        yield* fs.readFile(path.join(gitCommonDir, "objects/pack", packName!)),
        sharedPackBytes,
      );
      assert.strictEqual((yield* git(["rev-parse", concurrentRef])).stdout, concurrentOid);
      assert.isFalse(yield* fs.exists(packs[0]!));
      assert.isFalse(yield* fs.exists(objectDirectory));
      assert.strictEqual(yield* fs.readFileString(sharedPack), "owned by another Git operation");
      if (stop !== "success")
        assert.strictEqual((yield* git(["rev-parse", checkpointRef])).stdout, originalCheckpoint);
      assert.deepEqual(yield* fs.readFile(path.join(gitDir, "index")), originalIndex);
      assert.strictEqual((yield* git(["rev-parse", "HEAD"])).stdout, originalHead);
      assert.deepEqual(yield* fs.readFile(path.join(cwd, "a-media.bin")), payload);

      yield* git(["config", "filter.barrier.clean", "cat"]);
      yield* captureDriver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      assert.isFalse(yield* fs.exists(objectDirectory));
      const blob = (yield* git(["hash-object", "a-media.bin"])).stdout.trim();
      assert.strictEqual(
        (yield* git(["rev-parse", `${checkpointRef}:a-media.bin`])).stdout.trim(),
        blob,
      );
      yield* fs.remove(sharedPack);
      yield* git(["fsck", "--full"]);
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each(["failure", "interrupt"] as const)(
  "checkpoint publication $0 preserves existing refs and permits retry",
  (stop) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const fixtureRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-publish-" });
      const { cwd, git, gitCommonDir, checkpointRef } = yield* makeCheckpointFixture(
        driver,
        fixtureRoot,
      );
      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      const previous = (yield* git(["show-ref"])).stdout;
      yield* fs.writeFileString(path.join(cwd, "new.txt"), "new checkpoint content\n");
      const published = yield* Deferred.make<void>();
      const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provideService(FileSystem.FileSystem, {
          ...fs,
          link: (source, destination) =>
            fs.link(source, destination).pipe(
              Effect.andThen(Deferred.succeed(published, undefined)),
              Effect.andThen(
                stop === "interrupt"
                  ? Effect.never
                  : Effect.fail(
                      PlatformError.systemError({
                        _tag: "BadResource",
                        module: "FileSystem",
                        method: "link",
                      }),
                    ),
              ),
            ),
        }),
      );
      const fiber = yield* captureDriver.checkpoints
        .captureCheckpoint({ cwd, checkpointRef })
        .pipe(Effect.exit, Effect.forkScoped);
      yield* Deferred.await(published);
      if (stop === "interrupt") yield* Fiber.interrupt(fiber);
      const result = yield* Fiber.await(fiber);
      if (stop === "interrupt")
        assert.isTrue(Exit.isFailure(result) && Cause.hasInterruptsOnly(result.cause));
      else {
        assert.isTrue(Exit.isSuccess(result) && Exit.isFailure(result.value));
        if (Exit.isSuccess(result) && Exit.isFailure(result.value)) {
          const error = Cause.findErrorOption(result.value.cause);
          assert.isTrue(
            Option.isSome(error) &&
              error.value._tag === "VcsCheckpointStorageError" &&
              error.value.stage === "publish",
          );
        }
      }
      assert.strictEqual((yield* git(["show-ref"])).stdout, previous);
      assert.deepEqual(
        (yield* fs.readDirectory(path.join(gitCommonDir, "objects"))).filter((name) =>
          name.startsWith("tmp_objdir-t3-checkpoint-"),
        ),
        [],
      );
      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      assert.strictEqual(
        (yield* git(["show", `${checkpointRef}:new.txt`])).stdout,
        "new checkpoint content\n",
      );
      yield* git(["fsck", "--full"]);
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each([
  { stop: "before git restore spawns", gate: "restore", index: "staged", file: "later\n" },
  { stop: "after git restore ran", gate: "clean", index: "head", file: "unstaged\n" },
] as const)(
  "checkpoint restore interrupted $stop leaves the $index index",
  ({ gate, index, file }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const liveProcess = yield* VcsProcess.VcsProcess;
      const fixtureRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-restore-interrupt-" });
      const { cwd, git, checkpointRef } = yield* makeCheckpointFixture(driver, fixtureRoot);
      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      yield* fs.writeFileString(path.join(cwd, "file.txt"), "later\n");
      yield* git(["add", "file.txt"]);
      const stagedTree = (yield* git(["write-tree"])).stdout;
      const entered = yield* Deferred.make<void>();
      const gatedDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provideService(VcsProcess.VcsProcess, {
          run: (input) =>
            input.operation === "GitVcsDriver.checkpoints.restoreCheckpoint" &&
            input.args.includes(gate)
              ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))
              : liveProcess.run(input),
        }),
      );
      const fiber = yield* gatedDriver.checkpoints
        .restoreCheckpoint({ cwd, checkpointRef })
        .pipe(Effect.exit, Effect.forkScoped);
      yield* Deferred.await(entered);
      yield* Fiber.interrupt(fiber);
      const result = yield* Fiber.await(fiber);
      assert.isTrue(Exit.isFailure(result) && Cause.hasInterruptsOnly(result.cause));

      assert.strictEqual(
        (yield* git(["write-tree"])).stdout,
        index === "head" ? (yield* git(["rev-parse", "HEAD^{tree}"])).stdout : stagedTree,
      );
      assert.strictEqual(yield* fs.readFileString(path.join(cwd, "file.txt")), file);
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint publication falls back to rename where hardlinks are unavailable", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const crypto = yield* SecureRandom.SecureRandom;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const fixtureRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-publish-no-links-" });
    const { cwd, git, gitDir, gitCommonDir, repository, checkpointRef } =
      yield* makeCheckpointFixture(driver, fixtureRoot);
    const countBefore = yield* git(["count-objects", "-v"]);
    const commonCountBefore = yield* driver.execute({
      operation: "checkpoint-test",
      cwd: repository,
      args: ["count-objects", "-v"],
    });
    const refsBefore = (yield* fs.exists(path.join(gitDir, "refs")))
      ? yield* fs.readDirectory(path.join(gitDir, "refs"))
      : null;
    yield* git(["config", "core.bigFileThreshold", "1m"]);
    yield* fs.writeFile(path.join(cwd, "media.bin"), yield* crypto.randomBytes(2 * 1024 * 1024));
    const failedLinks: Array<readonly [string, string]> = [];
    const renamed: Array<readonly [string, string]> = [];
    const failedCommands: Array<{
      args: ReadonlyArray<string>;
      stdout: string;
      stderr: string;
      before?: ReadonlyArray<string> | undefined;
      after?: ReadonlyArray<string> | undefined;
    }> = [];
    const runner = yield* ProcessRunner.make();
    // Object listings are diagnostic evidence only; a failed listing is a test defect,
    // never a substitute process result.
    const listObjects = (directory: string) => fs.readDirectory(directory).pipe(Effect.orDie);
    const observedProcess = yield* VcsProcess.make.pipe(
      Effect.provideService(ProcessRunner.ProcessRunner, {
        run: Effect.fn("test.observeNativeCheckpointCommand")(function* (
          input: ProcessRunner.ProcessRunInput,
        ) {
          const objects = input.env?.GIT_OBJECT_DIRECTORY;
          const before = objects ? yield* listObjects(objects) : undefined;
          const result = yield* runner.run(input);
          const after =
            objects && (yield* fs.exists(objects).pipe(Effect.orDie))
              ? yield* listObjects(objects)
              : undefined;
          if (result.code !== 0 && failedCommands.length < 20)
            failedCommands.push({
              args: input.args,
              stdout: result.stdout,
              stderr: result.stderr,
              before,
              after,
            });
          return result;
        }),
      }),
    );
    const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, observedProcess),
      Effect.provideService(FileSystem.FileSystem, {
        ...fs,
        link: (source, destination) =>
          Effect.sync(() => failedLinks.push([source, destination])).pipe(
            Effect.andThen(
              Effect.fail(
                PlatformError.systemError({
                  _tag: "PermissionDenied",
                  module: "FileSystem",
                  method: "link",
                }),
              ),
            ),
          ),
        rename: (source, destination) =>
          fs
            .rename(source, destination)
            .pipe(Effect.tap(() => Effect.sync(() => renamed.push([source, destination])))),
      }),
    );
    const captured = yield* captureDriver.checkpoints
      .captureCheckpoint({ cwd, checkpointRef })
      .pipe(Effect.exit);
    assert.isTrue(Exit.isSuccess(captured), JSON.stringify(failedCommands));
    assert.isAbove(failedLinks.length, 0);
    assert.isAbove(renamed.length, 0);
    for (const renamedObject of renamed) assert.deepInclude(failedLinks, renamedObject);
    for (const extension of [".pack", ".idx"]) {
      assert.isTrue(renamed.some(([, destination]) => destination.endsWith(extension)));
    }
    const files = yield* fs.readDirectory(path.join(gitCommonDir, "objects/pack"));
    assert.isTrue(files.some((name) => name.endsWith(".pack")));
    assert.isTrue(yield* driver.checkpoints.hasCheckpointRef({ cwd, checkpointRef }));
    const linkedCount = yield* git(["count-objects", "-v"]);
    // Count the actual common object store. Native Git already reports the
    // fresh linked worktree's empty administrative refs directory as garbage.
    const count = yield* driver.execute({
      operation: "checkpoint-test",
      cwd: repository,
      args: ["count-objects", "-v"],
    });
    const refsAfter = (yield* fs.exists(path.join(gitDir, "refs")))
      ? yield* fs.readDirectory(path.join(gitDir, "refs"))
      : null;
    const fsck = yield* driver.execute({
      operation: "checkpoint-test",
      cwd,
      args: ["fsck", "--full"],
      allowNonZeroExit: true,
    });
    const diagnostics = JSON.stringify({
      countBefore,
      commonCountBefore,
      refsBefore,
      refsAfter,
      linkedCount,
      count,
      fsck,
      packNames: files.sort(),
      objectNames: (yield* fs.readDirectory(path.join(gitCommonDir, "objects"))).sort(),
      failedLinks,
      renamed,
    });
    assert.match(commonCountBefore.stdout, /^garbage: 0$/m, diagnostics);
    assert.strictEqual(linkedCount.stderr, countBefore.stderr, diagnostics);
    for (const field of ["garbage", "size-garbage"]) {
      const pattern = new RegExp(`^${field}: .*`, "m");
      assert.strictEqual(
        linkedCount.stdout.match(pattern)?.[0],
        countBefore.stdout.match(pattern)?.[0],
        diagnostics,
      );
    }
    assert.deepEqual(refsAfter, refsBefore, diagnostics);
    assert.match(count.stdout, /^garbage: 0$/m, diagnostics);
    assert.strictEqual(fsck.exitCode, 0, diagnostics);
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "checkpoint publication preserves Git's shared object directory permissions",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const fixtureRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-publish-shared-" });
      const { cwd, git, gitCommonDir } = yield* makeCheckpointFixture(driver, fixtureRoot, [
        "--shared=0660",
      ]);
      const checkpointRef = CheckpointRef.make("refs/t3/checkpoints/shared");
      yield* fs.writeFileString(path.join(cwd, "shared.txt"), "new shared object\n");
      const blob = (yield* git(["hash-object", "-w", "shared.txt"])).stdout.trim();
      const directory = path.join(gitCommonDir, "objects", blob.slice(0, 2));
      const expectedMode = (yield* fs.stat(directory)).mode & 0o7777;
      yield* fs.remove(directory, { recursive: true });
      assert.isFalse(yield* fs.exists(directory));
      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      assert.strictEqual((yield* fs.stat(directory)).mode & 0o7777, expectedMode);
      yield* git(["fsck", "--full"]);
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint capture ignores an inherited object directory", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const sandbox = yield* fs.makeTempDirectoryScoped({ prefix: "t3-inherited-objects-" });
    const fixtureRoot = path.join(sandbox, "repo");
    yield* fs.makeDirectory(fixtureRoot);
    const { cwd, git, checkpointRef } = yield* makeCheckpointFixture(driver, fixtureRoot);
    const unrelated = path.join(sandbox, "unrelated");
    yield* git(["init", "--bare", unrelated]);
    const unrelatedObjects = path.join(unrelated, "objects");
    // VcsProcess clears inherited selectors from git children; only the driver sees this one.
    yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef }).pipe(
      Effect.provideService(HostProcess.Environment, {
        ...(yield* HostProcess.Environment),
        GIT_OBJECT_DIRECTORY: unrelatedObjects,
      }),
    );
    assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "unstaged\n");
    assert.deepEqual(
      (yield* fs.readDirectory(unrelatedObjects)).filter(
        (name) => name !== "info" && name !== "pack",
      ),
      [],
    );
    yield* git(["fsck", "--full"]);
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each([
  { name: "nothing changed", reuses: true, change: () => Effect.void },
  {
    name: "its conversion attributes changed while it was being captured",
    reuses: false,
    change: () => Effect.void,
    during: ({ write }: UntrackedBlobChange) =>
      Effect.orDie(write(".gitattributes", "*.txt text eol=lf\n")),
  },
  {
    name: "its conversion config changed while it was being captured",
    reuses: false,
    change: () => Effect.void,
    during: ({ git }: UntrackedBlobChange) =>
      Effect.orDie(Effect.asVoid(git(["config", "core.autocrlf", "true"]))),
  },
  {
    name: "its conversion attributes changed",
    reuses: false,
    change: ({ write }: UntrackedBlobChange) => write(".gitattributes", "*.txt text eol=lf\n"),
  },
  {
    name: "a clean filter named like an unset attribute now applies to it",
    reuses: false,
    change: ({ git, writeGitPath }: UntrackedBlobChange) =>
      writeGitPath("info/attributes", "*.txt filter=unspecified\n").pipe(
        Effect.andThen(git(["config", "filter.unspecified.clean", "tr a-z A-Z"])),
      ),
  },
  {
    name: "its conversion config changed",
    reuses: false,
    change: ({ git }: UntrackedBlobChange) => git(["config", "core.autocrlf", "true"]),
  },
  {
    name: "it was rewritten at the same size",
    reuses: false,
    change: ({ write }: UntrackedBlobChange) =>
      write("nested/large.txt", "LINE\r\n".repeat(3 * 1024 * 1024)),
  },
  {
    name: "it became ignored",
    reuses: false,
    change: ({ write }: UntrackedBlobChange) => write(".gitignore", "nested/\n"),
  },
  {
    name: "HEAD now tracks it",
    reuses: false,
    change: ({ git }: UntrackedBlobChange) =>
      git(["add", "nested/large.txt"]).pipe(Effect.andThen(git(["commit", "-m", "track"]))),
  },
  {
    name: "its directory became a repository",
    reuses: false,
    change: ({ git }: UntrackedBlobChange) =>
      git(["init", "nested"]).pipe(
        Effect.andThen(
          git(
            ["-C", "nested", "-c", "user.name=Test", "-c", "user.email=test@test.com"].concat([
              "commit",
              "--allow-empty",
              "-m",
              "nested",
            ]),
          ),
        ),
      ),
  },
  {
    name: "its blob was pruned",
    reuses: false,
    change: ({ git, checkpointRef }: UntrackedBlobChange) =>
      git(["update-ref", "-d", checkpointRef]).pipe(Effect.andThen(git(["prune", "--expire=now"]))),
  },
])(
  "checkpoint capture skips hashing an untracked file only while a fresh hash would match: $name",
  ({ reuses, change, during }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const liveProcess = yield* VcsProcess.VcsProcess;
      let reused = 0;
      let afterPublish: Effect.Effect<void> = Effect.void;
      const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provideService(VcsProcess.VcsProcess, {
          run: (input) => {
            if (input.args.includes("core.ignorestat=true")) reused++;
            // Only a capture whose staging took real time looks for files to remember.
            if (input.args.includes("add"))
              return liveProcess.run(input).pipe(Effect.tap(() => TestClock.adjust("1 second")));
            // The files are hashed and published by now, but not yet remembered.
            return input.args.includes("update-ref")
              ? liveProcess.run(input).pipe(Effect.tap(() => afterPublish))
              : liveProcess.run(input);
          },
        }),
      );
      // A driver that has never captured this workspace hashes every file, as a first capture does.
      const fresh = yield* GitVcsDriver.makeVcsDriverShape();
      const fixtureRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-untracked-blobs-" });
      const { cwd, git, checkpointRef } = yield* makeCheckpointFixture(driver, fixtureRoot);
      const write = (relativePath: string, contents: string) =>
        fs.writeFileString(path.join(cwd, relativePath), contents);
      const writeGitPath = Effect.fn(function* (gitPath: string, contents: string) {
        const resolved = (yield* git([
          "rev-parse",
          "--path-format=absolute",
          "--git-path",
          gitPath,
        ])).stdout.slice(0, -1);
        yield* fs.makeDirectory(path.dirname(resolved), { recursive: true });
        yield* fs.writeFileString(resolved, contents);
      });
      yield* fs.makeDirectory(path.join(cwd, "nested"));
      yield* write("nested/large.txt", "line\r\n".repeat(3 * 1024 * 1024));
      // Only files left alone for a few seconds before a capture are remembered.
      yield* TestClock.adjust("36500 days");
      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      // A file is remembered by the first capture that read its conversion rules before hashing it.
      if (during !== undefined) afterPublish = during({ git, write, writeGitPath, checkpointRef });
      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      afterPublish = Effect.void;

      yield* change({ git, write, writeGitPath, checkpointRef });
      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      const freshRef = CheckpointRef.make("refs/t3/checkpoints/fresh");
      yield* fresh.checkpoints.captureCheckpoint({ cwd, checkpointRef: freshRef });

      assert.strictEqual(reused, reuses ? 1 : 0);
      assert.strictEqual(
        (yield* git(["rev-parse", `${checkpointRef}^{tree}`])).stdout,
        (yield* git(["rev-parse", `${freshRef}^{tree}`])).stdout,
      );
      yield* git(["fsck", "--connectivity-only"]);
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each(["sha1", "sha256"] as const)(
  "checkpoint object isolation supports linked worktrees with $0 objects",
  (format) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const sandbox = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-worktree-" });
      // A separator must remain part of the alternate's path, not split the list.
      const platform = yield* HostProcess.Platform;
      const fixtureRoot = path.join(
        sandbox,
        platform === "win32" ? "repo;space" : 'repo:space"\u0001',
      );
      yield* fs.makeDirectory(fixtureRoot);
      const { git, gitDir, gitCommonDir, checkpointRef } = yield* makeCheckpointFixture(
        driver,
        fixtureRoot,
        [`--object-format=${format}`],
      );
      const worktree = path.join(sandbox, "linked");
      yield* git(["worktree", "add", "-b", "linked", worktree]);
      yield* fs.writeFileString(path.join(worktree, "untracked.txt"), "linked worktree\n");
      const originalIndex = yield* fs.readFile(path.join(gitDir, "index"));
      const primaryIndex = yield* fs.readFile(path.join(gitCommonDir, "index"));
      assert.strictEqual((yield* git(["rev-parse", "--show-object-format"])).stdout.trim(), format);
      const capturedGitDir = (yield* driver.execute({
        operation: "test.index-custody",
        cwd: worktree,
        args: ["rev-parse", "--absolute-git-dir"],
      })).stdout.trim();
      const capturedIndex = yield* fs.readFile(path.join(capturedGitDir, "index"));
      yield* driver.checkpoints.captureCheckpoint({ cwd: worktree, checkpointRef });
      assert.deepEqual(yield* fs.readFile(path.join(capturedGitDir, "index")), capturedIndex);
      assert.strictEqual(
        (yield* git(["show", `${checkpointRef}:untracked.txt`])).stdout,
        "linked worktree\n",
      );
      assert.deepEqual(yield* fs.readFile(path.join(gitDir, "index")), originalIndex);
      assert.deepEqual(yield* fs.readFile(path.join(gitCommonDir, "index")), primaryIndex);
      yield* git(["fsck", "--full"]);
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint capture skips untracked nested repositories without a commit", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-unborn-" });
    const { cwd, git, gitDir, checkpointRef } = yield* makeCheckpointFixture(driver, root);
    const nested = "scratch/empty [repo]";
    yield* git(["init", nested]);
    yield* git(["init", "another empty"]);
    yield* fileSystem.writeFileString(path.join(cwd, nested, "private.txt"), "nested\n");
    yield* git(["init", "committed"]);
    yield* git([
      "-C",
      "committed",
      "-c",
      "user.name=Test",
      "-c",
      "user.email=test@test.com",
      "commit",
      "--allow-empty",
      "-m",
      "initial",
    ]);
    const nestedHead = (yield* git(["-C", "committed", "rev-parse", "HEAD"])).stdout.trim();
    yield* fileSystem.writeFileString(path.join(cwd, "untracked.txt"), "new\n");
    const originalIndex = yield* fileSystem.readFile(path.join(gitDir, "index"));

    yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });

    assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "unstaged\n");
    assert.strictEqual((yield* git(["show", `${checkpointRef}:untracked.txt`])).stdout, "new\n");
    assert.strictEqual((yield* git(["ls-tree", "-r", checkpointRef, "--", nested])).stdout, "");
    assert.strictEqual((yield* git(["ls-tree", checkpointRef, "--", "another empty"])).stdout, "");
    assert.strictEqual(
      (yield* git(["ls-tree", checkpointRef, "--", "committed"])).stdout,
      `160000 commit ${nestedHead}\tcommitted\n`,
    );
    assert.deepEqual(yield* fileSystem.readFile(path.join(gitDir, "index")), originalIndex);
    assert.isTrue(yield* driver.checkpoints.restoreCheckpoint({ cwd, checkpointRef }));
    assert.strictEqual(
      yield* fileSystem.readFileString(path.join(cwd, nested, "private.txt")),
      "nested\n",
    );
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.skipIf(!symlinksSupported)(
  "checkpoint capture writes its index into the real git dir when cwd is a symlinked subdirectory",
  () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const liveRunner = yield* ProcessRunner.ProcessRunner;
      const indexFiles: string[] = [];
      const captureProcess = yield* VcsProcess.make.pipe(
        Effect.provideService(ProcessRunner.ProcessRunner, {
          run: (input) => {
            const indexFile = input.env?.GIT_INDEX_FILE;
            if (indexFile !== undefined) indexFiles.push(indexFile);
            return liveRunner.run(input);
          },
        }),
      );
      const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provideService(VcsProcess.VcsProcess, captureProcess),
      );
      const sandbox = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-checkpoint-symlink-",
      });
      const { cwd, git, checkpointRef } = yield* makeCheckpointFixture(driver, sandbox);
      const subdir = path.join(cwd, "sub", "dir");
      const link = path.join(sandbox, "nest", "link");
      yield* fileSystem.makeDirectory(subdir, { recursive: true });
      yield* fileSystem.makeDirectory(path.dirname(link), { recursive: true });
      yield* fileSystem.symlink(subdir, link);
      // A decoy repository at the link's ancestor must not receive the checkpoint index.
      yield* git(["-C", sandbox, "init"]);
      yield* fileSystem.writeFileString(path.join(subdir, "nested.txt"), "from-link\n");
      const realCommonDir = yield* fileSystem.realPath(path.join(sandbox, "repository", ".git"));

      yield* driver.checkpoints.captureCheckpoint({ cwd: link, checkpointRef });

      assert.isTrue(indexFiles.length > 0);
      for (const indexFile of indexFiles) {
        assert.strictEqual(yield* fileSystem.realPath(path.dirname(indexFile)), realCommonDir);
      }
      assert.strictEqual(
        (yield* git(["show", `${checkpointRef}:sub/dir/nested.txt`])).stdout,
        "from-link\n",
      );
    }).pipe(Effect.scoped, Effect.provide(layerGitCaptureContract)),
);

it.effect("checkpoint capture keeps the shared index a split index still uses", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-split-" });
    const { cwd, git, checkpointRef } = yield* makeCheckpointFixture(driver, root);
    yield* git(["config", "core.splitIndex", "true"]);
    yield* git(["config", "splitIndex.sharedIndexExpire", "now"]);
    yield* git(["update-index", "--split-index"]);
    yield* fileSystem.writeFileString(path.join(cwd, "untracked.txt"), "new\n");

    yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });

    assert.strictEqual((yield* git(["show", `${checkpointRef}:untracked.txt`])).stdout, "new\n");
    assert.strictEqual((yield* git(["ls-files", "--cached"])).stdout, "file.txt\n");
    assert.strictEqual((yield* git(["diff", "--cached", "--name-only"])).stdout, "file.txt\n");
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint recovery discovers nested HEAD independently of inherited GIT_DIR", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-git-dir-" });
    const { cwd, git, gitDir, checkpointRef } = yield* makeCheckpointFixture(driver, root);
    yield* git(["init", "empty"]);
    const originalIndex = yield* fs.readFile(path.join(gitDir, "index"));
    yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef }).pipe(
      Effect.provideService(HostProcess.Environment, {
        ...(yield* HostProcess.Environment),
        GIT_DIR: gitDir,
      }),
    );
    assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "unstaged\n");
    assert.strictEqual((yield* git(["ls-tree", "-r", checkpointRef, "--", "empty"])).stdout, "");
    assert.deepEqual(yield* fs.readFile(path.join(gitDir, "index")), originalIndex);
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint capture still fails when a clean filter rejects a file", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const root = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-checkpoint-filter-failure-",
    });
    const { cwd, git, gitDir, checkpointRef } = yield* makeCheckpointFixture(driver, root);
    yield* fileSystem.writeFileString(path.join(cwd, ".gitattributes"), "file.txt filter=reject\n");
    yield* git(["config", "filter.reject.clean", "false"]);
    yield* git(["config", "filter.reject.required", "true"]);
    const originalIndex = yield* fileSystem.readFile(path.join(gitDir, "index"));

    const result = yield* Effect.result(
      driver.checkpoints.captureCheckpoint({ cwd, checkpointRef }),
    );

    assert.strictEqual(result._tag, "Failure");
    if (result._tag === "Failure") {
      assert.strictEqual(result.failure._tag, "VcsProcessExitError");
    }
    assert.deepEqual(yield* fileSystem.readFile(path.join(gitDir, "index")), originalIndex);
    assert.isFalse(yield* driver.checkpoints.hasCheckpointRef({ cwd, checkpointRef }));
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint capture keeps LF files that core.safecrlf would refuse to commit", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-safecrlf-" });
    const { cwd, git, gitDir, checkpointRef } = yield* makeCheckpointFixture(driver, root);
    yield* fileSystem.writeFileString(path.join(cwd, ".gitattributes"), "*.txt text eol=crlf\n");
    yield* git(["config", "core.safecrlf", "true"]);
    yield* fileSystem.writeFileString(path.join(cwd, "untracked.txt"), "new\n");
    const originalIndex = yield* fileSystem.readFile(path.join(gitDir, "index"));

    yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });

    assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "unstaged\n");
    assert.strictEqual((yield* git(["show", `${checkpointRef}:untracked.txt`])).stdout, "new\n");
    assert.deepEqual(yield* fileSystem.readFile(path.join(gitDir, "index")), originalIndex);
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint capture refuses a truncated nested repository listing", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const liveProcess = yield* VcsProcess.VcsProcess;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-truncated-" });
    const { cwd, git, checkpointRef } = yield* makeCheckpointFixture(driver, root);
    yield* git(["init", "empty"]);
    const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, {
        run: (input) =>
          liveProcess
            .run(input)
            .pipe(
              Effect.map((result) =>
                input.args.includes("--others") ? { ...result, stdoutTruncated: true } : result,
              ),
            ),
      }),
    );

    const result = yield* Effect.result(
      captureDriver.checkpoints.captureCheckpoint({ cwd, checkpointRef }),
    );

    assert.strictEqual(result._tag, "Failure");
    assert.isFalse(yield* driver.checkpoints.hasCheckpointRef({ cwd, checkpointRef }));
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint capture skips an ignored project while keeping its allowed sibling", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-ignored-" });
    const { cwd: parent, git, gitDir } = yield* makeCheckpointFixture(driver, root);
    yield* fs.writeFileString(
      path.join(parent, ".gitignore"),
      "*\n!.gitignore\n!file.txt\n!keep/\n!keep/**\n",
    );
    const ignoredCwd = path.join(parent, "ignored");
    const keepCwd = path.join(parent, "keep");
    yield* fs.makeDirectory(ignoredCwd);
    yield* fs.makeDirectory(keepCwd);
    yield* fs.writeFileString(path.join(ignoredCwd, "secret.txt"), "private\n");
    yield* fs.writeFileString(path.join(keepCwd, "new.txt"), "new\n");
    const originalIndex = yield* fs.readFile(path.join(gitDir, "index"));
    const ignoredRef = CheckpointRef.make("refs/t3/checkpoints/ignored");
    const keepRef = CheckpointRef.make("refs/t3/checkpoints/keep");

    const ignored = yield* Effect.result(
      driver.checkpoints.captureCheckpoint({ cwd: ignoredCwd, checkpointRef: ignoredRef }),
    );
    yield* driver.checkpoints.captureCheckpoint({ cwd: keepCwd, checkpointRef: keepRef });

    assert.strictEqual((yield* git(["show", `${keepRef}:keep/new.txt`])).stdout, "new\n");
    assert.strictEqual((yield* git(["ls-tree", "-r", keepRef, "--", "ignored"])).stdout, "");
    assert.isFalse(
      yield* driver.checkpoints.hasCheckpointRef({ cwd: parent, checkpointRef: ignoredRef }),
    );
    assert.deepEqual(yield* fs.readFile(path.join(gitDir, "index")), originalIndex);
    assert.strictEqual(ignored._tag, "Failure");
    if (ignored._tag === "Failure") {
      assert.strictEqual(ignored.failure._tag, "VcsUnsupportedOperationError");
    }
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each([
  { workspace: "the repository root", subdirectory: "" },
  { workspace: "a subdirectory", subdirectory: "sub" },
])(
  "checkpoint capture keeps new files in $workspace when the root .gitignore has a blank CRLF line",
  ({ subdirectory }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const fixtureRoot = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-checkpoint-crlf-ignore-",
      });
      const { cwd: parent, git, checkpointRef } = yield* makeCheckpointFixture(driver, fixtureRoot);
      yield* fs.writeFileString(path.join(parent, ".gitignore"), "a.txt\r\n\r\nb.txt\r\n");
      const cwd = path.join(parent, subdirectory);
      yield* fs.makeDirectory(cwd, { recursive: true });
      yield* fs.writeFileString(path.join(cwd, "notes.md"), "notes\n");
      const notesPath = subdirectory === "" ? "notes.md" : `${subdirectory}/notes.md`;

      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });

      assert.include(
        (yield* git(["ls-tree", "-r", "--name-only", checkpointRef])).stdout.split("\n"),
        notesPath,
      );
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each([
  { tracking: "committed-only", committed: ["committed.txt"], staged: [] },
  { tracking: "staged-only", committed: [], staged: ["staged.txt"] },
  { tracking: "mixed", committed: ["committed.txt"], staged: ["staged.txt"] },
])(
  "checkpoint capture keeps $tracking tracked edits but excludes new files in a project ignored later",
  ({ committed, staged }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-ignored-later-" });
      const {
        cwd: parent,
        git,
        gitDir,
        checkpointRef,
      } = yield* makeCheckpointFixture(driver, root);
      const cwd = path.join(parent, "sub");
      yield* fs.makeDirectory(cwd);
      for (const file of committed) {
        yield* fs.writeFileString(path.join(cwd, file), "committed\n");
        yield* git(["add", `sub/${file}`]);
      }
      if (committed.length > 0) {
        yield* git(["commit", "-m", "track project"]);
      }
      for (const file of staged) {
        yield* fs.writeFileString(path.join(cwd, file), "staged\n");
        yield* git(["add", `sub/${file}`]);
      }
      const tracked = [...committed, ...staged];
      yield* fs.writeFileString(path.join(parent, ".gitignore"), "sub/\n");
      for (const file of tracked) {
        yield* fs.writeFileString(path.join(cwd, file), `${file} working\n`);
      }
      yield* fs.writeFileString(path.join(cwd, "secret.txt"), "private\n");
      const originalIndex = yield* fs.readFile(path.join(gitDir, "index"));

      const captured = yield* Effect.result(
        driver.checkpoints.captureCheckpoint({ cwd, checkpointRef }),
      );

      assert.deepEqual(yield* fs.readFile(path.join(gitDir, "index")), originalIndex);
      if (captured._tag === "Failure") return yield* captured.failure;
      assert.strictEqual(
        (yield* git(["ls-tree", "-r", "--name-only", checkpointRef, "--", "sub"])).stdout,
        tracked.map((file) => `sub/${file}\n`).join(""),
      );
      for (const file of tracked) {
        assert.strictEqual(
          (yield* git(["show", `${checkpointRef}:sub/${file}`])).stdout,
          `${file} working\n`,
        );
      }
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each([
  { phase: "add", nestedRecovery: false, expireRecovery: false },
  { phase: "update-ref", nestedRecovery: false, expireRecovery: false },
  { phase: "update-ref", nestedRecovery: true, expireRecovery: false },
  { phase: "add", nestedRecovery: true, expireRecovery: false },
  { phase: "add", nestedRecovery: true, expireRecovery: true },
])(
  "checkpoint handles a $phase lock with nestedRecovery=$nestedRecovery, expireRecovery=$expireRecovery",
  ({ phase, nestedRecovery, expireRecovery }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const liveRunner = yield* ProcessRunner.ProcessRunner;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-ref-race-" });
      const { cwd, git, gitDir, checkpointRef } = yield* makeCheckpointFixture(driver, root);
      if (nestedRecovery) yield* git(["init", "empty"]);
      const originalIndex = yield* fs.readFile(path.join(gitDir, "index"));
      const gitCommonDir = (yield* git(["rev-parse", "--git-common-dir"])).stdout.trim();
      const refLockPath = path.join(gitCommonDir, `${checkpointRef}.lock`);
      const failed = yield* Deferred.make<void>();
      const retryReached = yield* Deferred.make<void>();
      const allowRetry = yield* Deferred.make<void>();
      const clock = yield* Clock.Clock;
      const privateIndexes = new Set<string>();
      let racedAttempts = 0;
      let discoveries = 0;
      let stageError: VcsProcessExitError | undefined;
      const captureProcess = yield* VcsProcess.make.pipe(
        Effect.provideService(ProcessRunner.ProcessRunner, {
          run: (input) => {
            if (input.args.includes("--others")) discoveries += 1;
            if (input.env?.GIT_INDEX_FILE) privateIndexes.add(input.env.GIT_INDEX_FILE);
            const initialStage =
              phase === "add" &&
              nestedRecovery &&
              !input.args.some((arg) => arg.startsWith(":(exclude,literal)"));
            if (!input.args.includes(phase) || initialStage || ++racedAttempts !== 1) {
              return liveRunner.run(input);
            }
            const lockPath = phase === "add" ? `${input.env!.GIT_INDEX_FILE!}.lock` : refLockPath;
            return Effect.gen(function* () {
              yield* fs
                .makeDirectory(path.dirname(lockPath), { recursive: true })
                .pipe(Effect.orDie);
              yield* fs.writeFileString(lockPath, "concurrent ref writer").pipe(Effect.orDie);
              return yield* liveRunner.run(input).pipe(
                Effect.ensuring(fs.remove(lockPath).pipe(Effect.orDie)),
                Effect.tap(() => Deferred.succeed(failed, undefined)),
              );
            });
          },
        }),
      );
      const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provideService(VcsProcess.VcsProcess, {
          run: (input) =>
            captureProcess.run(input).pipe(
              Effect.tapError((error) => {
                if (input.args.includes("add") && error._tag === "VcsProcessExitError")
                  stageError = error;
                return Effect.void;
              }),
            ),
        }),
      );
      const captureStore = yield* makeCaptureStore(captureDriver, cwd);
      const fiber = yield* captureStore.captureCheckpoint({ cwd, checkpointRef }).pipe(
        Effect.provideService(Clock.Clock, {
          ...clock,
          sleep: (duration) =>
            Duration.toMillis(duration) === 75
              ? Deferred.succeed(retryReached, undefined).pipe(
                  Effect.andThen(Deferred.await(allowRetry)),
                )
              : clock.sleep(duration),
        }),
        Effect.exit,
        Effect.forkScoped,
      );
      yield* Deferred.await(failed);
      yield* Deferred.await(retryReached);
      if (expireRecovery) yield* TestClock.adjust("5 seconds");
      else yield* Deferred.succeed(allowRetry, undefined);
      const result = yield* Fiber.join(fiber);
      if (expireRecovery) {
        if (Exit.isSuccess(result))
          return yield* Effect.die("Expected the recovery deadline to expire");
        const error = Cause.findErrorOption(result.cause);
        assert.isTrue(error._tag === "Some");
        if (error._tag === "Some") assert.strictEqual(error.value, stageError);
      } else assert.isTrue(Exit.isSuccess(result));
      assert.strictEqual(racedAttempts, expireRecovery ? 1 : 2);
      assert.strictEqual(discoveries, nestedRecovery ? 1 : 0);
      assert.strictEqual(privateIndexes.size, 1);
      assert.strictEqual(
        yield* driver.checkpoints.hasCheckpointRef({ cwd, checkpointRef }),
        !expireRecovery,
      );
      for (const index of privateIndexes) {
        assert.isFalse(yield* fs.exists(index));
        assert.isFalse(yield* fs.exists(`${index}.lock`));
      }
      assert.deepEqual(yield* fs.readFile(path.join(gitDir, "index")), originalIndex);
    }).pipe(Effect.scoped, Effect.provide(layerGitCaptureContract)),
);

it.effect.each(["discovery", "probe", "retry"] as const)(
  "checkpoint recovery has one deadline including %s",
  (blockedPhase) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const liveProcess = yield* VcsProcess.VcsProcess;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-recovery-timeout-" });
      const { cwd, git, gitDir, checkpointRef } = yield* makeCheckpointFixture(driver, root);
      yield* git(["init", "empty"]);
      const originalIndex = yield* fs.readFile(path.join(gitDir, "index"));
      const entered = yield* Deferred.make<void>();
      const discovered = yield* Deferred.make<void>();
      let stageError: VcsProcessExitError | undefined;
      let privateIndex: string | undefined;
      let interrupted = false;
      let stagingAttempts = 0;
      const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provideService(VcsProcess.VcsProcess, {
          run: (input) => {
            const staging = input.args.includes("add") && input.args.includes("-A");
            if (staging) {
              privateIndex = input.env?.GIT_INDEX_FILE;
              stagingAttempts += 1;
            }
            const block =
              (blockedPhase === "discovery" && input.args.includes("--others")) ||
              (blockedPhase === "probe" && input.cwd !== cwd && input.args.includes("rev-parse")) ||
              (blockedPhase === "retry" &&
                staging &&
                input.args.some((arg) => arg.startsWith(":(exclude,literal)")));
            if (block)
              return (
                blockedPhase === "retry"
                  ? fs
                      .writeFileString(
                        `${input.env!.GIT_INDEX_FILE!}.lock`,
                        "interrupted index write",
                      )
                      .pipe(Effect.orDie)
                  : Effect.void
              ).pipe(
                Effect.andThen(Deferred.succeed(entered, undefined)),
                Effect.andThen(Effect.never),
                Effect.onInterrupt(() =>
                  Effect.sync(() => {
                    interrupted = true;
                  }),
                ),
              );
            return liveProcess.run(input).pipe(
              Effect.tap(() =>
                blockedPhase === "probe" && input.args.includes("--others")
                  ? Deferred.succeed(discovered, undefined).pipe(
                      Effect.andThen(Effect.sleep("3 seconds")),
                    )
                  : Effect.void,
              ),
              Effect.tapError((error) => {
                if (staging && error._tag === "VcsProcessExitError") stageError = error;
                return Effect.void;
              }),
            );
          },
        }),
      );
      const captureStore = yield* makeCaptureStore(captureDriver, cwd);
      const fiber = yield* captureStore
        .captureCheckpoint({ cwd, checkpointRef })
        .pipe(Effect.flip, Effect.forkScoped);
      if (blockedPhase === "probe") {
        yield* Deferred.await(discovered);
        yield* TestClock.adjust("3 seconds");
      }
      yield* Deferred.await(entered);
      yield* TestClock.adjust(blockedPhase === "probe" ? "2 seconds" : "5 seconds");
      const error = yield* Fiber.join(fiber);
      assert.strictEqual(error, stageError);
      assert.strictEqual(stagingAttempts, blockedPhase === "retry" ? 2 : 1);
      assert.isTrue(interrupted);
      assert.isDefined(privateIndex);
      assert.isFalse(yield* fs.exists(privateIndex!));
      assert.isFalse(yield* fs.exists(`${privateIndex!}.lock`));
      assert.isFalse(yield* driver.checkpoints.hasCheckpointRef({ cwd, checkpointRef }));
      assert.deepEqual(yield* fs.readFile(path.join(gitDir, "index")), originalIndex);
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint recovery preserves interruption and removes the private index", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const liveProcess = yield* VcsProcess.VcsProcess;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-recovery-interrupt-" });
    const { cwd, git, checkpointRef } = yield* makeCheckpointFixture(driver, root);
    yield* git(["init", "empty"]);
    const entered = yield* Deferred.make<void>();
    let privateIndex: string | undefined;
    const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, {
        run: (input) => {
          if (input.args.includes("add") && input.args.includes("-A"))
            privateIndex = input.env?.GIT_INDEX_FILE;
          return input.args.includes("--others")
            ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))
            : liveProcess.run(input);
        },
      }),
    );
    const fiber = yield* captureDriver.checkpoints
      .captureCheckpoint({ cwd, checkpointRef })
      .pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(fiber);
    const exit = yield* Fiber.await(fiber);
    assert.isTrue(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause));
    assert.isDefined(privateIndex);
    assert.isFalse(yield* fs.exists(privateIndex!));
    assert.isFalse(yield* driver.checkpoints.hasCheckpointRef({ cwd, checkpointRef }));
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint capture does not rerun clean filters for unchanged indexed files", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-cache-" });
    const { cwd, git, gitDir, checkpointRef } = yield* makeCheckpointFixture(driver, root);
    yield* fileSystem.writeFileString(
      path.join(cwd, ".gitattributes"),
      "stable.txt filter=probe\n",
    );
    yield* fileSystem.writeFileString(path.join(cwd, "stable.txt"), "unchanged\n");
    yield* fileSystem.writeFileString(
      path.join(gitDir, "filter.cjs"),
      'require("node:fs").appendFileSync(require("node:path").join(__dirname, "filter-runs"), "read\\n"); process.stdin.pipe(process.stdout);',
    );
    yield* git(["config", "filter.probe.clean", `node "${path.join(gitDir, "filter.cjs")}"`]);
    yield* fileSystem.utimes(path.join(cwd, "stable.txt"), 1_700_000_000, 1_700_000_000);
    yield* git(["add", "."]);
    yield* git(["commit", "-m", "record stable file"]);
    yield* fileSystem.writeFileString(path.join(gitDir, "filter-runs"), "");
    yield* fileSystem.writeFileString(path.join(cwd, "file.txt"), "changed\n");
    const originalIndex = yield* fileSystem.readFile(path.join(gitDir, "index"));

    yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });

    assert.strictEqual(yield* fileSystem.readFileString(path.join(gitDir, "filter-runs")), "");
    assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "changed\n");
    assert.strictEqual((yield* git(["show", `${checkpointRef}:stable.txt`])).stdout, "unchanged\n");
    assert.deepEqual(yield* fileSystem.readFile(path.join(gitDir, "index")), originalIndex);
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each(
  [false, true].flatMap((nested) =>
    (["sparse", "flags", "manual-skip", "missing", "non-cone-missing"] as const).map(
      (indexState) => ({ nested, indexState }),
    ),
  ),
)(
  "sparse checkpoint preserves two captures (nested=$nested, index=$indexState)",
  ({ nested, indexState }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const liveProcess = yield* VcsProcess.VcsProcess;
      let freshReadTrees = 0;
      const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provideService(VcsProcess.VcsProcess, {
          run: (input) => {
            if (input.args.includes("read-tree") && input.args.includes("index.sparse=true"))
              freshReadTrees++;
            return liveProcess.run(input);
          },
        }),
      );
      const fixtureRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-sparse-" });
      const { cwd, git, gitDir } = yield* makeCheckpointFixture(driver, fixtureRoot);
      const write = Effect.fn(function* (name: string, contents: string) {
        yield* fs.makeDirectory(path.dirname(path.join(cwd, name)), { recursive: true });
        yield* fs.writeFileString(path.join(cwd, name), contents);
      });
      for (const name of [
        "scope/in/edit",
        "scope/in/delete",
        "scope/out/deep/absent",
        "scope/out/present",
        "elsewhere/file",
      ]) {
        yield* write(name, "original\n");
      }
      yield* git(["add", "."]);
      yield* git(["commit", "-m", "sparse fixture"]);
      yield* git(["sparse-checkout", "set", "--cone", "--sparse-index", "scope/in", "elsewhere"]);
      if (indexState === "non-cone-missing")
        yield* git(["sparse-checkout", "set", "--no-cone", "/scope/in/", "/elsewhere/"]);
      yield* write("scope/in/edit", "staged\n");
      yield* write("elsewhere/file", "staged outside\n");
      yield* git(["add", "."]);
      if (indexState === "flags")
        yield* git(["update-index", "--assume-unchanged", "scope/in/delete"]);
      if (indexState === "manual-skip")
        yield* git(["update-index", "--skip-worktree", "scope/in/delete"]);
      yield* git(["config", "sparse.expectFilesOutsideOfPatterns", "true"]);
      yield* write("scope/in/edit", "working\n");
      yield* write("scope/out/present", "modified skipped\n");
      yield* write("scope/out/new file", "new outside cone\n");
      yield* write("elsewhere/file", "working outside\n");
      yield* fs.remove(path.join(cwd, "scope/in/delete"));
      const indexPath = path.join(gitDir, "index");
      if (indexState.endsWith("missing")) yield* fs.remove(indexPath);
      const originalIndex = yield* fs.readFile(indexPath).pipe(Effect.orElseSucceed(() => null));
      const captureCwd = nested ? path.join(cwd, "scope") : cwd;
      for (const turn of [1, 2]) {
        const ref = CheckpointRef.make(`refs/t3/checkpoints/sparse/${turn}`);
        if (turn === 2) {
          yield* write("scope/in/edit", "second\n");
          yield* fs.remove(path.join(cwd, "scope/out/new file"));
          yield* write("scope/out/second", "second addition\n");
        }
        const capture = driver.checkpoints.captureCheckpoint({
          cwd: captureCwd,
          checkpointRef: ref,
        });
        if (indexState === "non-cone-missing") {
          assert.strictEqual((yield* capture.pipe(Effect.flip))._tag, "VcsProcessExitError");
          assert.isFalse(
            yield* driver.checkpoints.hasCheckpointRef({ cwd: captureCwd, checkpointRef: ref }),
          );
          assert.isFalse(yield* fs.exists(indexPath));
          break;
        }
        yield* capture;
        for (const [name, content] of [
          ["scope/out/deep/absent", "original\n"],
          ["scope/out/present", "modified skipped\n"],
          ["scope/in/edit", turn === 1 ? "working\n" : "second\n"],
          ["elsewhere/file", nested ? "original\n" : "working outside\n"],
          [
            turn === 1 ? "scope/out/new file" : "scope/out/second",
            turn === 1 ? "new outside cone\n" : "second addition\n",
          ],
        ]) {
          assert.strictEqual((yield* git(["show", `${ref}:${name}`])).stdout, content);
        }
        const files = (yield* git(["ls-tree", "-rz", "--name-only", ref])).stdout.split("\0");
        assert.notInclude(files, "scope/in/delete");
        if (turn === 2) assert.notInclude(files, "scope/out/new file");
        assert.deepEqual(
          yield* fs.readFile(indexPath).pipe(Effect.orElseSucceed(() => null)),
          originalIndex,
        );
        assert.isFalse(yield* fs.exists(path.join(cwd, "scope/out/deep/absent")));
        assert.strictEqual(
          yield* fs.readFileString(path.join(cwd, "elsewhere/file")),
          "working outside\n",
        );
      }
      assert.strictEqual(freshReadTrees, indexState === "missing" ? 2 : 0);
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each(
  (["normal", "skip-worktree", "assume-unchanged", "sparse"] as const).flatMap((indexMode) =>
    [1, 3, 4096].map((chunkSize) => ({ indexMode, chunkSize })),
  ),
)(
  "checkpoint index inspection handles entries beyond the output cap (index=$indexMode, chunk=$chunkSize)",
  ({ indexMode, chunkSize }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const liveProcess = yield* VcsProcess.VcsProcess;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-inspection-" });
      const { cwd, git, gitDir, checkpointRef } = yield* makeCheckpointFixture(driver, root);
      yield* fs.writeFileString(path.join(cwd, ".gitattributes"), "stable filter=probe\n");
      yield* fs.writeFileString(path.join(cwd, "stable"), "unchanged\n");
      yield* fs.writeFileString(path.join(cwd, "z-skipped"), "original\n");
      yield* fs.writeFileString(path.join(cwd, "z-deleted"), "original\n");
      yield* fs.makeDirectory(path.join(cwd, "excluded"));
      yield* fs.writeFileString(path.join(cwd, "excluded/file"), "absent\n");
      yield* fs.writeFileString(
        path.join(gitDir, "filter.cjs"),
        'require("node:fs").appendFileSync(require("node:path").join(__dirname, "reads"), "read\\n"); process.stdin.pipe(process.stdout);',
      );
      yield* git(["config", "filter.probe.clean", `node "${path.join(gitDir, "filter.cjs")}"`]);
      yield* git(["config", "core.trustctime", "false"]);
      yield* fs.utimes(path.join(cwd, "stable"), 1_700_000_000, 1_700_000_000);
      yield* fs.utimes(path.join(cwd, "z-skipped"), 1_700_000_000, 1_700_000_000);
      yield* git(["add", "."]);
      yield* git(["commit", "-m", "inspection fixture"]);
      const flagged = indexMode === "skip-worktree" || indexMode === "assume-unchanged";
      if (flagged) {
        yield* git(["update-index", `--${indexMode}`, "z-skipped", "z-deleted"]);
        yield* git(["config", "core.ignorestat", "true"]);
        yield* fs.remove(path.join(cwd, "z-deleted"));
      }
      if (indexMode === "sparse")
        yield* git(["sparse-checkout", "set", "--cone", "--sparse-index", "included"]);
      yield* fs.writeFileString(path.join(cwd, "z-skipped"), "modified\n");
      // A flagged edit with the indexed size and mtime is invisible to Git's stat check.
      if (flagged) yield* fs.utimes(path.join(cwd, "z-skipped"), 1_700_000_000, 1_700_000_000);
      yield* fs.writeFileString(path.join(gitDir, "reads"), "");
      const originalIndex = yield* fs.readFile(path.join(gitDir, "index"));
      let freshReadTrees = 0;
      const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provideService(VcsProcess.VcsProcess, {
          run: (input) => {
            if (input.args.includes("read-tree") && !input.args.includes("--reset"))
              freshReadTrees++;
            return liveProcess.run(
              input.args.includes("ls-files")
                ? {
                    ...input,
                    maxOutputBytes: 8,
                    onStdoutChunk: (chunk) => {
                      for (let i = 0; i < chunk.length; i += chunkSize)
                        input.onStdoutChunk?.(chunk.subarray(i, i + chunkSize));
                    },
                  }
                : input,
            );
          },
        }),
      );
      yield* captureDriver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      assert.strictEqual((yield* git(["show", `${checkpointRef}:z-skipped`])).stdout, "modified\n");
      const files = (yield* git(["ls-tree", "-z", "--name-only", checkpointRef])).stdout.split(
        "\0",
      );
      if (flagged) assert.notInclude(files, "z-deleted");
      else assert.include(files, "z-deleted");
      assert.strictEqual(freshReadTrees, 0);
      assert.strictEqual(yield* fs.readFileString(path.join(gitDir, "reads")), "");
      assert.deepEqual(yield* fs.readFile(path.join(gitDir, "index")), originalIndex);
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each([1_700_000_000, 1_700_000_000.9999])(
  "checkpoint capture preserves same-size edits with racy index timestamps (%s)",
  (timestamp) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-racy-" });
      const { cwd, git, gitDir, checkpointRef } = yield* makeCheckpointFixture(driver, root);
      const filePath = path.join(cwd, "file.txt");
      const indexPath = path.join(gitDir, "index");
      yield* git(["config", "core.trustctime", "false"]);
      yield* fileSystem.writeFileString(filePath, "before\n");
      yield* fileSystem.utimes(filePath, timestamp, timestamp);
      yield* git(["add", "file.txt"]);
      yield* git(["commit", "-m", "record racy file"]);
      yield* fileSystem.utimes(indexPath, timestamp, timestamp);
      const originalIndex = yield* fileSystem.readFile(indexPath);
      const originalIndexMtime = (yield* fileSystem.stat(indexPath)).mtime;
      yield* fileSystem.writeFileString(filePath, "after!\n");
      yield* fileSystem.utimes(filePath, timestamp, timestamp);

      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });

      assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "after!\n");
      assert.deepEqual(yield* fileSystem.readFile(indexPath), originalIndex);
      assert.deepEqual((yield* fileSystem.stat(indexPath)).mtime, originalIndexMtime);
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);
it.effect.each(
  [false, true].flatMap((nested) =>
    (["sparse", "flags", "manual-skip", "missing", "non-cone-missing"] as const).map(
      (indexState) => ({ nested, indexState }),
    ),
  ),
)(
  "sparse checkpoint preserves two captures (nested=$nested, index=$indexState)",
  ({ nested, indexState }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-sparse-" });
      const { cwd, gitDir, git } = yield* makeCheckpointFixture(driver, root);
      const write = Effect.fn(function* (name: string, contents: string) {
        yield* fs.makeDirectory(path.dirname(path.join(cwd, name)), { recursive: true });
        yield* fs.writeFileString(path.join(cwd, name), contents);
      });
      for (const name of [
        "scope/in/edit",
        "scope/in/delete",
        "scope/out/deep/absent",
        "scope/out/present",
        "elsewhere/file",
      ]) {
        yield* write(name, "original\n");
      }
      yield* git(["add", "."]);
      yield* git(["commit", "-m", "sparse fixture"]);
      yield* git(["sparse-checkout", "set", "--cone", "--sparse-index", "scope/in", "elsewhere"]);
      if (indexState === "non-cone-missing")
        yield* git(["sparse-checkout", "set", "--no-cone", "/scope/in/", "/elsewhere/"]);
      yield* write("scope/in/edit", "staged\n");
      yield* write("elsewhere/file", "staged outside\n");
      yield* git(["add", "."]);
      if (indexState === "flags")
        yield* git(["update-index", "--assume-unchanged", "scope/in/delete"]);
      if (indexState === "manual-skip")
        yield* git(["update-index", "--skip-worktree", "scope/in/delete"]);
      yield* git(["config", "sparse.expectFilesOutsideOfPatterns", "true"]);
      yield* write("scope/in/edit", "working\n");
      yield* write("scope/out/present", "modified skipped\n");
      yield* write("scope/out/new file", "new outside cone\n");
      yield* write("elsewhere/file", "working outside\n");
      yield* fs.remove(path.join(cwd, "scope/in/delete"));
      const indexPath = path.join(gitDir, "index");
      if (indexState.endsWith("missing")) yield* fs.remove(indexPath);
      const originalIndex = yield* fs.readFile(indexPath).pipe(Effect.orElseSucceed(() => null));
      const captureCwd = nested ? path.join(cwd, "scope") : cwd;
      for (const turn of [1, 2]) {
        const ref = CheckpointRef.make(`refs/t3/checkpoints/sparse/${turn}`);
        if (turn === 2) {
          yield* write("scope/in/edit", "second\n");
          yield* fs.remove(path.join(cwd, "scope/out/new file"));
          yield* write("scope/out/second", "second addition\n");
        }
        const capture = driver.checkpoints.captureCheckpoint({
          cwd: captureCwd,
          checkpointRef: ref,
        });
        if (indexState === "non-cone-missing") {
          assert.strictEqual((yield* capture.pipe(Effect.flip))._tag, "VcsProcessExitError");
          assert.isFalse(
            yield* driver.checkpoints.hasCheckpointRef({ cwd: captureCwd, checkpointRef: ref }),
          );
          assert.isFalse(yield* fs.exists(indexPath));
          break;
        }
        yield* capture;
        for (const [name, content] of [
          ["scope/out/deep/absent", "original\n"],
          ["scope/out/present", "modified skipped\n"],
          ["scope/in/edit", turn === 1 ? "working\n" : "second\n"],
          ["elsewhere/file", nested ? "original\n" : "working outside\n"],
          [
            turn === 1 ? "scope/out/new file" : "scope/out/second",
            turn === 1 ? "new outside cone\n" : "second addition\n",
          ],
        ]) {
          assert.strictEqual((yield* git(["show", `${ref}:${name}`])).stdout, content);
        }
        const files = (yield* git(["ls-tree", "-rz", "--name-only", ref])).stdout.split("\0");
        assert.notInclude(files, "scope/in/delete");
        if (turn === 2) assert.notInclude(files, "scope/out/new file");
        assert.deepEqual(
          yield* fs.readFile(indexPath).pipe(Effect.orElseSucceed(() => null)),
          originalIndex,
        );
        assert.isFalse(yield* fs.exists(path.join(cwd, "scope/out/deep/absent")));
        assert.strictEqual(
          yield* fs.readFileString(path.join(cwd, "elsewhere/file")),
          "working outside\n",
        );
      }
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("a branch switch landing after the snapshot belongs to the next checkpoint", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const liveProcess = yield* VcsProcess.VcsProcess;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const fixtureRoot = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-checkpoint-switch-race-",
    });
    const { cwd, git } = yield* makeCheckpointFixture(driver, fixtureRoot);
    yield* fileSystem.writeFileString(path.join(cwd, "f.txt"), "A\n");
    yield* git(["add", "."]);
    yield* git(["commit", "-m", "a"]);
    yield* git(["checkout", "-b", "b"]);
    yield* fileSystem.writeFileString(path.join(cwd, "f.txt"), "B\n");
    yield* git(["commit", "-am", "b"]);
    const branchBHead = (yield* git(["rev-parse", "HEAD"])).stdout.trim();
    yield* git(["checkout", "-"]);
    const branchAHead = (yield* git(["rev-parse", "HEAD"])).stdout.trim();
    assert.notStrictEqual(branchAHead, branchBHead);
    const [first, second, third] = ["1", "2", "3"].map((turn) =>
      CheckpointRef.make(`refs/t3/checkpoints/switch-race/${turn}`),
    ) as [CheckpointRef, CheckpointRef, CheckpointRef];
    let switchesAfterSnapshot = 0;
    const racedDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, {
        run: Effect.fn(function* (input: VcsProcess.VcsProcessInput) {
          const result = yield* liveProcess.run(input);
          if (input.args.includes("write-tree")) {
            assert.strictEqual((yield* git(["rev-parse", "HEAD"])).stdout.trim(), branchAHead);
            yield* git(["checkout", "b"]).pipe(Effect.orDie);
            switchesAfterSnapshot++;
          }
          return result;
        }),
      }),
    );
    const changedSince = (fromCheckpointRef: CheckpointRef, toCheckpointRef: CheckpointRef) =>
      Effect.gen(function* () {
        const refs = { cwd, fromCheckpointRef, toCheckpointRef };
        return {
          patch: yield* driver.checkpoints.diffCheckpoints({ ...refs, ignoreWhitespace: false }),
          authored: yield* driver.checkpoints.listAuthoredPaths(refs),
        };
      });

    yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef: first });
    yield* racedDriver.checkpoints.captureCheckpoint({
      cwd,
      checkpointRef: second,
      previousCheckpointRef: first,
    });
    yield* driver.checkpoints.captureCheckpoint({
      cwd,
      checkpointRef: third,
      previousCheckpointRef: second,
    });

    assert.strictEqual(switchesAfterSnapshot, 1);
    assert.strictEqual((yield* git(["rev-parse", "HEAD"])).stdout.trim(), branchBHead);
    for (const [ref, head, contents] of [
      [first, branchAHead, "A\n"],
      [second, branchAHead, "A\n"],
      [third, branchBHead, "B\n"],
    ]) {
      assert.strictEqual((yield* git(["rev-parse", `${ref}^1`])).stdout.trim(), head);
      assert.strictEqual((yield* git(["show", `${ref}:f.txt`])).stdout, contents);
    }
    assert.deepEqual(yield* changedSince(first, second), { patch: "", authored: null });
    assert.deepEqual(yield* changedSince(second, third), { patch: "", authored: null });
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint capture preserves racy edits made after resetting the index", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const liveProcess = yield* VcsProcess.VcsProcess;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-racy-reset-" });
    const { cwd, git, gitDir, checkpointRef } = yield* makeCheckpointFixture(driver, root);
    const racyPath = path.join(cwd, "racy.txt");
    const indexPath = path.join(gitDir, "index");
    const timestamp = 1_700_000_000;
    yield* git(["config", "core.trustctime", "false"]);
    yield* fileSystem.writeFileString(racyPath, "before\n");
    yield* fileSystem.utimes(racyPath, timestamp, timestamp);
    yield* git(["add", "."]);
    yield* git(["commit", "-m", "record racy file"]);
    yield* fileSystem.writeFileString(path.join(cwd, "file.txt"), "staged\n");
    yield* git(["add", "file.txt"]);
    yield* fileSystem.utimes(indexPath, timestamp, timestamp);
    const originalIndex = yield* fileSystem.readFile(indexPath);
    const originalIndexMtime = (yield* fileSystem.stat(indexPath)).mtime;
    const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, {
        run: Effect.fn(function* (input: VcsProcess.VcsProcessInput) {
          const result = yield* liveProcess.run(input);
          if (input.args.includes("read-tree") && input.args.includes("--reset")) {
            yield* fileSystem.writeFileString(racyPath, "after!\n").pipe(Effect.orDie);
            yield* fileSystem.utimes(racyPath, timestamp, timestamp).pipe(Effect.orDie);
          }
          return result;
        }),
      }),
    );

    yield* captureDriver.checkpoints.captureCheckpoint({ cwd, checkpointRef });

    assert.strictEqual((yield* git(["show", `${checkpointRef}:racy.txt`])).stdout, "after!\n");
    assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "staged\n");
    assert.deepEqual(yield* fileSystem.readFile(indexPath), originalIndex);
    assert.deepEqual((yield* fileSystem.stat(indexPath)).mtime, originalIndexMtime);
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each(
  [false, true].flatMap((nested) =>
    (["normal", "flags", "split"] as const).map((indexMode) => ({ nested, indexMode })),
  ),
)(
  "checkpoint index reuse preserves two turns (nested=$nested, index=$indexMode)",
  ({ nested, indexMode }) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-turns-" });
      const { cwd, gitDir, git } = yield* makeCheckpointFixture(driver, root);
      const write = (name: string, contents: string) =>
        fileSystem.writeFileString(path.join(cwd, name), contents);
      yield* fileSystem.makeDirectory(path.join(cwd, "scope"));
      for (const name of [
        "scope/staged",
        "scope/deleted",
        "scope/assumed",
        "scope/skipped",
        "outside",
      ]) {
        yield* write(name, "original\n");
      }
      yield* git(["add", "."]);
      yield* git(["commit", "-m", "initial scoped files"]);
      yield* write("scope/staged", "staged\n");
      yield* write("scope/new-deleted", "staged then deleted\n");
      yield* write("outside", "staged outside\n");
      yield* git(["add", "."]);
      if (indexMode === "flags") {
        yield* git(["update-index", "--assume-unchanged", "scope/assumed"]);
        yield* git(["update-index", "--skip-worktree", "scope/skipped"]);
      }
      if (indexMode === "split") {
        yield* git(["update-index", "--split-index"]);
      }
      const originalIndex = yield* fileSystem.readFile(path.join(gitDir, "index"));
      for (const name of ["scope/staged", "scope/assumed", "scope/skipped", "outside"]) {
        yield* write(name, "working\n");
      }
      yield* write("scope/new", "first\n");
      yield* fileSystem.remove(path.join(cwd, "scope/deleted"));
      yield* fileSystem.remove(path.join(cwd, "scope/new-deleted"));
      const captureCwd = nested ? path.join(cwd, "scope") : cwd;
      const first = CheckpointRef.make("refs/t3/checkpoints/turns/1");
      const second = CheckpointRef.make("refs/t3/checkpoints/turns/2");
      yield* driver.checkpoints.captureCheckpoint({ cwd: captureCwd, checkpointRef: first });
      for (const name of ["scope/staged", "scope/assumed", "scope/skipped"]) {
        assert.strictEqual((yield* git(["show", `${first}:${name}`])).stdout, "working\n");
      }
      assert.strictEqual(
        (yield* git(["show", `${first}:outside`])).stdout,
        nested ? "original\n" : "working\n",
      );
      const files = (yield* git(["ls-tree", "-r", "--name-only", first])).stdout.split("\n");
      assert.notInclude(files, "scope/deleted");
      assert.notInclude(files, "scope/new-deleted");
      assert.include(files, "scope/new");

      yield* write("scope/staged", "second\n");
      yield* fileSystem.remove(path.join(cwd, "scope/new"));
      yield* write("scope/second", "added in second turn\n");
      yield* driver.checkpoints.captureCheckpoint({ cwd: captureCwd, checkpointRef: second });
      assert.strictEqual(
        (yield* git(["diff", "--name-only", first, second])).stdout,
        "scope/new\nscope/second\nscope/staged\n",
      );
      assert.strictEqual((yield* git(["show", `${second}:scope/staged`])).stdout, "second\n");
      assert.strictEqual(
        (yield* git(["show", `${second}:scope/second`])).stdout,
        "added in second turn\n",
      );
      assert.deepEqual(yield* fileSystem.readFile(path.join(gitDir, "index")), originalIndex);
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each(["missing", "invalid"] as const)(
  "checkpoint capture falls back when the user index is %s",
  (indexState) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-index-" });
      const { cwd, git, gitDir, checkpointRef } = yield* makeCheckpointFixture(driver, root);
      const indexPath = path.join(gitDir, "index");
      if (indexState === "missing") {
        yield* fileSystem.remove(indexPath);
      } else {
        yield* fileSystem.writeFileString(indexPath, "invalid index");
      }

      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });

      assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "unstaged\n");
      if (indexState === "missing") {
        assert.isFalse(yield* fileSystem.exists(indexPath));
      } else {
        assert.strictEqual(yield* fileSystem.readFileString(indexPath), "invalid index");
      }
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

const captureRecordingGitCommands = Effect.fn("test.captureRecordingGitCommands")(function* (
  cwd: string,
  checkpointRef: CheckpointRef,
  onCommand?: (args: ReadonlyArray<string>) => void,
) {
  const liveProcess = yield* VcsProcess.VcsProcess;
  const commands: Array<string | undefined> = [];
  const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
    Effect.provideService(VcsProcess.VcsProcess, {
      run: (input) => {
        onCommand?.(input.args);
        let index = 0;
        while (input.args[index] === "-C" || input.args[index] === "-c") index += 2;
        commands.push(input.args[index]);
        return liveProcess.run(input);
      },
    }),
  );
  yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
  return commands;
});

it.effect.each(["committed", "unborn"] as const)(
  "checkpoint capture reads the repository with one rev-parse (HEAD=%s)",
  (head) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const fixtureRoot = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-checkpoint-processes-",
      });
      const { cwd, git } = yield* makeCheckpointFixture(driver, fixtureRoot);
      const checkpointRef = CheckpointRef.make("refs/t3/checkpoints/test");
      if (head === "unborn") {
        yield* git(["restore", "--source=HEAD", "--staged", "--worktree", "--", "."]);
        yield* git(["switch", "--orphan", "checkpoint-unborn"]);
        yield* fileSystem.writeFileString(path.join(cwd, "file.txt"), "unstaged\n");
      }

      const commands = yield* captureRecordingGitCommands(cwd, checkpointRef);

      assert.deepStrictEqual(commands, [
        "rev-parse",
        ...(head === "committed" ? ["log"] : []),
        "config",
        ...(head === "committed" ? ["read-tree", "ls-files"] : []),
        "add",
        "write-tree",
        "commit-tree",
        "update-ref",
      ]);
      assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "unstaged\n");
      const [, ...parents] = (yield* git([
        "rev-list",
        "--parents",
        "-n",
        "1",
        checkpointRef,
      ])).stdout
        .trim()
        .split(" ");
      assert.deepStrictEqual(
        parents,
        head === "committed" ? [(yield* git(["rev-parse", "HEAD"])).stdout.trim()] : [],
      );
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect(
  "checkpoint capture composes the primary fence with batched subdirectory prefix isolation",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const liveProcess = yield* VcsProcess.VcsProcess;
      const fixtureRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-prefix-" });
      const { cwd, git, gitDir, gitCommonDir, repository, checkpointRef } =
        yield* makeCheckpointFixture(driver, fixtureRoot);
      const primaryIndex = yield* fs.readFile(path.join(gitCommonDir, "index"));
      yield* driver.execute({
        operation: "test.primary-task-branch",
        cwd: repository,
        args: ["branch", "-m", "primary-task"],
      });
      yield* fs.writeFileString(
        path.join(repository, "private.txt"),
        "primary-only dirty content\n",
      );
      const commands: ReadonlyArray<string>[] = [];
      const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provideService(VcsProcess.VcsProcess, {
          run: (input) => {
            commands.push(input.args);
            return liveProcess.run(input);
          },
        }),
      );
      const refusal = yield* captureDriver.checkpoints
        .captureCheckpoint({ cwd: repository, checkpointRef })
        .pipe(Effect.result);
      assert.strictEqual(refusal._tag, "Failure");
      if (refusal._tag !== "Failure")
        return assert.fail("Expected actual primary checkout refusal");
      assert.strictEqual(refusal.failure._tag, "VcsPrimaryCheckoutCheckpointError");
      assert.strictEqual(commands.length, 1);
      assert.include(commands[0]!, "--git-dir");
      assert.deepEqual(yield* fs.readFile(path.join(gitCommonDir, "index")), primaryIndex);
      assert.isFalse(yield* driver.checkpoints.hasCheckpointRef({ cwd, checkpointRef }));

      const nested = " scope name ";
      const captureCwd = path.join(cwd, nested);
      yield* fs.makeDirectory(captureCwd);
      yield* fs.writeFileString(path.join(cwd, ".gitignore"), "ignored.txt\r\n\r\n");
      yield* fs.writeFileString(path.join(captureCwd, "notes.md"), "nested content\n");
      // The fixture already has a staged and a dirty outside file; neither belongs in this workspace.
      const originalIndex = yield* fs.readFile(path.join(gitDir, "index"));
      commands.length = 0;
      yield* captureDriver.checkpoints.captureCheckpoint({ cwd: captureCwd, checkpointRef });
      assert.strictEqual(commands.filter((args) => args.includes("rev-parse")).length, 1);
      assert.include(commands[0]!, "--git-dir");
      assert.include(commands[0]!, "--show-toplevel");
      assert.include(commands[0]!, "--show-prefix");
      assert.isTrue(commands.some((args) => args.includes("--walk-reflogs")));
      assert.strictEqual(
        (yield* git(["show", `${checkpointRef}:${nested}/notes.md`])).stdout,
        "nested content\n",
      );
      assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "initial\n");
      assert.notInclude(
        (yield* git(["ls-tree", "-r", "--name-only", checkpointRef])).stdout,
        "private.txt",
      );
      assert.deepEqual(yield* fs.readFile(path.join(gitDir, "index")), originalIndex);
      assert.deepEqual(yield* fs.readFile(path.join(gitCommonDir, "index")), primaryIndex);
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint capture outside a repository fails with the common dir lookup error", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-no-repo-" });

    const error = yield* captureRecordingGitCommands(
      cwd,
      CheckpointRef.make("refs/t3/checkpoints/test"),
    ).pipe(Effect.flip);

    assert.instanceOf(error, VcsProcessExitError);
    assert.strictEqual(error.operation, "GitVcsDriver.checkpoints.resolveGitCommonDir");
    assert.strictEqual(error.exitCode, 128);
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "checkpoint capture handles a repository path containing a newline",
  () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const fixtureRoot = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-checkpoint-new\nline-",
      });
      const { cwd, git, gitDir, checkpointRef } = yield* makeCheckpointFixture(driver, fixtureRoot);
      const commonOutput = (yield* git(["rev-parse", "--git-common-dir"])).stdout;
      assert.isTrue(commonOutput.endsWith("\n"));
      const gitCommonDir = path.resolve(cwd, commonOutput.slice(0, -1));
      const originalIndex = yield* fileSystem.readFile(path.join(gitDir, "index"));
      const metadataArgs: ReadonlyArray<string>[] = [];
      const commands = yield* captureRecordingGitCommands(cwd, checkpointRef, (args) =>
        metadataArgs.push(args),
      );

      assert.deepStrictEqual(commands, [
        "rev-parse",
        "rev-parse",
        "rev-parse",
        "rev-parse",
        "config",
        "rev-parse",
        "read-tree",
        "ls-files",
        "add",
        "write-tree",
        "commit-tree",
        "update-ref",
        // Without a parsed top level, capture cannot rule out submodules.
        "submodule",
      ]);
      assert.deepStrictEqual(metadataArgs.slice(0, 3), [
        [
          "-C",
          cwd,
          "rev-parse",
          "--path-format=absolute",
          "--git-common-dir",
          "--git-dir",
          "--git-path",
          "index",
          "--show-toplevel",
          "--show-prefix",
          "--verify",
          "--quiet",
          "HEAD",
        ],
        ["-C", cwd, "rev-parse", "--git-common-dir"],
        ["-C", cwd, "rev-parse", "--git-dir"],
      ]);
      assert.deepStrictEqual(metadataArgs[3], ["-C", cwd, "rev-parse", "--show-prefix"]);
      assert.deepStrictEqual(metadataArgs[5], [
        "-C",
        cwd,
        "rev-parse",
        "--path-format=absolute",
        "--git-path",
        "index",
      ]);
      assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "unstaged\n");
      assert.deepEqual(yield* fileSystem.readFile(path.join(gitDir, "index")), originalIndex);
      assert.deepStrictEqual(
        (yield* fileSystem.readDirectory(gitCommonDir)).filter((name) =>
          name.startsWith("t3-checkpoint-index-"),
        ),
        [],
      );
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("restores empty checkpoints without changing paths outside the workspace", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    for (const nested of [false, true]) {
      const temporaryRoot = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-empty-checkpoint-",
      });
      const repository = path.join(temporaryRoot, "repository");
      const root = path.join(temporaryRoot, "worktree");
      yield* fileSystem.makeDirectory(repository);
      yield* runGit(repository, ["init"]);
      yield* runGit(repository, ["config", "user.email", "test@test.com"]);
      yield* runGit(repository, ["config", "user.name", "Test"]);
      if (nested) {
        yield* fileSystem.writeFileString(path.join(repository, "outside.txt"), "original\n");
        yield* runGit(repository, ["add", "."]);
      }
      yield* runGit(repository, ["commit", "--allow-empty", "-m", "initial"]);
      yield* runGit(repository, ["worktree", "add", "-b", "checkpoint-test", root]);
      const cwd = nested ? path.join(root, "nested") : root;
      yield* fileSystem.makeDirectory(cwd, { recursive: true });
      const checkpointRef = CheckpointRef.make("refs/t3/checkpoints/empty");
      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      if (nested) {
        yield* fileSystem.writeFileString(path.join(root, "outside.txt"), "changed\n");
        yield* runGit(root, ["add", "outside.txt"]);
      }
      for (const staged of [false, true]) {
        const addedPath = path.join(cwd, "added.txt");
        yield* fileSystem.writeFileString(addedPath, "new\n");
        if (staged) yield* runGit(cwd, ["add", "added.txt"]);
        assert.isTrue(
          yield* driver.checkpoints.restoreCheckpoint({
            cwd,
            checkpointRef,
            fallbackToHead: false,
          }),
        );
        assert.isTrue(yield* fileSystem.exists(cwd));
        assert.isFalse(yield* fileSystem.exists(addedPath));
      }
      yield* fileSystem.writeFileString(
        path.join(repository, ".git", "info", "exclude"),
        "ignored.txt\n",
      );
      yield* fileSystem.writeFileString(path.join(cwd, "ignored.txt"), "keep\n");
      yield* fileSystem.makeDirectory(path.join(cwd, "untracked"));
      yield* fileSystem.writeFileString(path.join(cwd, "untracked", "file.txt"), "remove\n");
      yield* fileSystem.writeFileString(path.join(cwd, "untracked", ".gitignore"), "hidden.txt\n");
      yield* fileSystem.writeFileString(path.join(cwd, "untracked", "hidden.txt"), "remove\n");
      assert.isTrue(
        yield* driver.checkpoints.restoreCheckpoint({ cwd, checkpointRef, fallbackToHead: false }),
      );
      assert.strictEqual(yield* fileSystem.readFileString(path.join(cwd, "ignored.txt")), "keep\n");
      assert.isFalse(yield* fileSystem.exists(path.join(cwd, "untracked")));
      if (nested) {
        assert.strictEqual(
          yield* fileSystem.readFileString(path.join(root, "outside.txt")),
          "changed\n",
        );
        const staged = yield* driver.execute({
          operation: "test",
          cwd: root,
          args: ["diff", "--cached", "--name-only"],
        });
        assert.strictEqual(staged.stdout.trim(), "outside.txt");
      }
      yield* fileSystem.writeFileString(path.join(cwd, "checkpoint.txt"), "checkpoint\n");
      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      yield* fileSystem.writeFileString(path.join(cwd, "checkpoint.txt"), "later\n");
      let deepest = cwd;
      for (let depth = 0; depth < 4; depth++) {
        yield* fileSystem.writeFileString(path.join(deepest, ".gitignore"), "d/.gitignore\n");
        deepest = path.join(deepest, "d");
        yield* fileSystem.makeDirectory(deepest);
      }
      yield* fileSystem.writeFileString(path.join(deepest, ".gitignore"), "hidden.txt\n");
      yield* fileSystem.writeFileString(path.join(deepest, "hidden.txt"), "remove\n");
      const incomplete = yield* driver.checkpoints
        .restoreCheckpoint({ cwd, checkpointRef, fallbackToHead: false })
        .pipe(Effect.flip);
      assert.strictEqual(incomplete._tag, "VcsProcessExitError");
      assert.strictEqual(
        yield* fileSystem.readFileString(path.join(cwd, "checkpoint.txt")),
        "checkpoint\n",
      );
      assert.strictEqual(
        (yield* driver.execute({
          operation: "test",
          cwd,
          args: ["diff", "--cached", "--name-only", "--", "."],
        })).stdout,
        "",
      );
      if (nested) {
        assert.strictEqual(
          (yield* driver.execute({
            operation: "test",
            cwd: root,
            args: ["diff", "--cached", "--name-only"],
          })).stdout.trim(),
          "outside.txt",
        );
      }
    }
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("restores a checkpoint before the first commit without staging its files", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    for (const nested of [false, true]) {
      const temporaryRoot = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-unborn-restore-",
      });
      // Checkpoints refuse Git's primary checkout; an orphan linked worktree is still unborn.
      const repository = path.join(temporaryRoot, "repository");
      const root = path.join(temporaryRoot, "worktree");
      yield* fileSystem.makeDirectory(repository);
      yield* runGit(repository, ["init"]);
      yield* runGit(repository, ["config", "user.email", "test@test.com"]);
      yield* runGit(repository, ["config", "user.name", "Test"]);
      yield* runGit(repository, ["worktree", "add", "--orphan", "-b", "unborn-test", root]);
      if (nested) {
        yield* fileSystem.writeFileString(path.join(root, "outside.txt"), "staged\n");
        yield* runGit(root, ["add", "outside.txt"]);
      }
      const cwd = nested ? path.join(root, "nested") : root;
      yield* fileSystem.makeDirectory(cwd, { recursive: true });
      yield* fileSystem.writeFileString(path.join(cwd, "file.txt"), "original\n");
      const checkpointRef = CheckpointRef.make("refs/t3/checkpoints/unborn");
      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      yield* fileSystem.writeFileString(path.join(cwd, "file.txt"), "changed\n");
      yield* fileSystem.writeFileString(path.join(cwd, "added.txt"), "new\n");

      assert.isTrue(
        yield* driver.checkpoints.restoreCheckpoint({ cwd, checkpointRef, fallbackToHead: false }),
      );

      assert.strictEqual(
        yield* fileSystem.readFileString(path.join(cwd, "file.txt")),
        "original\n",
      );
      assert.isFalse(yield* fileSystem.exists(path.join(cwd, "added.txt")));
      const staged = yield* driver.execute({
        operation: "test",
        cwd: root,
        args: ["diff", "--cached", "--name-only"],
      });
      assert.strictEqual(staged.stdout.trim(), nested ? "outside.txt" : "");
    }
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("restores edits to skip-worktree files whose checkpoint content matches HEAD", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const fixtureRoot = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-skip-worktree-restore-",
    });
    const { cwd, git, checkpointRef } = yield* makeCheckpointFixture(driver, fixtureRoot);
    const skippedPath = path.join(cwd, "skipped.txt");
    yield* fileSystem.writeFileString(skippedPath, "original\n");
    yield* git(["add", "skipped.txt"]);
    yield* git(["commit", "-m", "add skipped file"]);
    yield* git(["update-index", "--skip-worktree", "skipped.txt"]);
    yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
    yield* fileSystem.writeFileString(skippedPath, "later\n");

    assert.isTrue(
      yield* driver.checkpoints.restoreCheckpoint({ cwd, checkpointRef, fallbackToHead: false }),
    );

    assert.strictEqual(yield* fileSystem.readFileString(skippedPath), "original\n");
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("sparse restore rewrites manual skip-worktree files and keeps exclusions absent", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const liveProcess = yield* VcsProcess.VcsProcess;
    // One byte per chunk, so the index listing splits inside every record.
    const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, {
        run: (input) => {
          const onStdoutChunk = input.onStdoutChunk;
          return liveProcess.run(
            onStdoutChunk === undefined
              ? input
              : {
                  ...input,
                  onStdoutChunk: (chunk) => {
                    for (let index = 0; index < chunk.length; index++)
                      onStdoutChunk(chunk.subarray(index, index + 1));
                  },
                },
          );
        },
      }),
    );
    const fixtureRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sparse-restore-" });
    const { cwd, git, checkpointRef } = yield* makeCheckpointFixture(driver, fixtureRoot);
    for (const name of ["in/normal", "in/skipped", "in/s space", "out/deep/absent", "out/stray"]) {
      yield* fs.makeDirectory(path.dirname(path.join(cwd, name)), { recursive: true });
      yield* fs.writeFileString(path.join(cwd, name), "original\n");
    }
    yield* git(["add", "."]);
    yield* git(["commit", "-m", "sparse fixture"]);
    yield* git(["sparse-checkout", "set", "--cone", "--sparse-index", "in"]);
    // Keeps Git from clearing the bit on files that are present.
    yield* git(["config", "sparse.expectFilesOutsideOfPatterns", "true"]);
    yield* git(["update-index", "--skip-worktree", "in/skipped", "in/s space"]);
    const captureCwd = path.join(cwd, "in");
    yield* driver.checkpoints.captureCheckpoint({ cwd: captureCwd, checkpointRef });
    yield* fs.writeFileString(path.join(cwd, "in/skipped"), "later\n");
    yield* fs.writeFileString(path.join(cwd, "in/s space"), "later\n");
    yield* fs.writeFileString(path.join(cwd, "in/normal"), "later\n");

    for (const restoreCwd of [captureCwd, cwd]) {
      assert.isTrue(
        yield* driver.checkpoints.restoreCheckpoint({
          cwd: restoreCwd,
          checkpointRef,
          fallbackToHead: false,
        }),
      );
      assert.strictEqual(yield* fs.readFileString(path.join(cwd, "in/skipped")), "original\n");
      assert.strictEqual(yield* fs.readFileString(path.join(cwd, "in/s space")), "original\n");
      assert.strictEqual(yield* fs.readFileString(path.join(cwd, "in/normal")), "original\n");
      assert.isFalse(yield* fs.exists(path.join(cwd, "out")));
    }
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("recursive checkpoint refusal cannot delete an unrelated primary repository ref", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const liveProcess = yield* VcsProcess.VcsProcess;
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-submodule-refusal-" });
    const { cwd, git, checkpointRef } = yield* makeCheckpointFixture(driver, root);
    const outsider = path.join(cwd, "unregistered");
    yield* fs.makeDirectory(outsider);
    const outsiderGit = (args: ReadonlyArray<string>) =>
      driver.execute({ operation: "checkpoint-test", cwd: outsider, args });
    yield* outsiderGit(["init"]);
    yield* outsiderGit([
      "-c",
      "user.name=Test",
      "-c",
      "user.email=test@test.com",
      "commit",
      "--allow-empty",
      "-m",
      "initial",
    ]);
    yield* outsiderGit(["update-ref", checkpointRef, "HEAD"]);
    const original = (yield* outsiderGit(["rev-parse", checkpointRef])).stdout;
    // Force discovery of a spurious child. Even an existing valid checkpoint
    // there must survive refusal, including the recursive failure-cleanup path.
    yield* fs.writeFileString(path.join(cwd, ".gitmodules"), "");
    const childCommands: ReadonlyArray<string>[] = [];
    const observed = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, {
        run: (input) => {
          if (input.cwd === outsider) childCommands.push(input.args);
          if (input.args.includes("foreach"))
            return Effect.succeed({
              exitCode: ChildProcessSpawner.ExitCode(0),
              stdout: "unregistered\0",
              stderr: "",
              stdoutTruncated: false,
              stderrTruncated: false,
            });
          return liveProcess.run(input);
        },
      }),
    );
    yield* observed.checkpoints.captureCheckpoint({ cwd, checkpointRef });
    assert.strictEqual((yield* outsiderGit(["rev-parse", checkpointRef])).stdout, original);
    assert.isAbove(childCommands.length, 0);
    assert.isFalse(
      childCommands.some((args) =>
        ["add", "write-tree", "commit-tree", "update-ref"].some((command) =>
          args.includes(command),
        ),
      ),
    );
    assert.isTrue(yield* driver.checkpoints.hasCheckpointRef({ cwd, checkpointRef }));
    yield* git(["fsck", "--full"]);
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("restores and deletes checkpoints inside populated submodules", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const git = (cwd: string, args: ReadonlyArray<string>) =>
      driver.execute({ operation: "test", cwd, args });
    const makeRepository = Effect.fn("makeRepository")(function* (prefix: string) {
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix });
      yield* git(cwd, ["init"]);
      yield* git(cwd, ["config", "user.email", "test@test.com"]);
      yield* git(cwd, ["config", "user.name", "Test"]);
      yield* fileSystem.writeFileString(path.join(cwd, "file.txt"), "original\n");
      yield* git(cwd, ["add", "."]);
      yield* git(cwd, ["commit", "-m", "initial"]);
      return cwd;
    });
    const nested = yield* makeRepository("t3-submodule-nested-");
    const library = yield* makeRepository("t3-submodule-library-");
    const addSubmodule = (cwd: string, url: string, name: string) =>
      git(cwd, ["-c", "protocol.file.allow=always", "submodule", "add", url, name]).pipe(
        Effect.andThen(git(cwd, ["commit", "-m", `add ${name}`])),
      );
    yield* addSubmodule(library, nested, "nested");
    const repository = yield* makeRepository("t3-submodule-root-");
    yield* addSubmodule(repository, library, "library");
    const fixtureRoot = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-submodule-worktree-",
    });
    const root = path.join(fixtureRoot, "worktree");
    yield* git(repository, ["worktree", "add", "-b", "checkpoint-test", root]);
    yield* git(root, [
      "-c",
      "protocol.file.allow=always",
      "submodule",
      "update",
      "--init",
      "--recursive",
    ]);
    const files = ["file.txt", "library/file.txt", "library/nested/file.txt"].map((file) =>
      path.join(root, file),
    );
    const checkpointRef = CheckpointRef.make("refs/t3/checkpoints/submodules");
    // A populated submodule is primary in its own repository. Only the
    // successfully authorized parent's private path may capture it.
    const libraryCwd = path.join(root, "library");
    const directCapture = yield* driver.checkpoints
      .captureCheckpoint({
        cwd: libraryCwd,
        checkpointRef,
      })
      .pipe(Effect.result);
    assert.strictEqual(directCapture._tag, "Failure");
    if (directCapture._tag === "Failure")
      assert.strictEqual(directCapture.failure._tag, "VcsPrimaryCheckoutCheckpointError");
    assert.isFalse(yield* driver.checkpoints.hasCheckpointRef({ cwd: libraryCwd, checkpointRef }));
    yield* fileSystem.writeFileString(files[1]!, "captured\n");
    yield* driver.checkpoints.captureCheckpoint({ cwd: root, checkpointRef });
    for (const cwd of [root, path.join(root, "library"), path.join(root, "library", "nested")]) {
      assert.isTrue(yield* driver.checkpoints.hasCheckpointRef({ cwd, checkpointRef }));
    }

    for (const file of files) {
      yield* fileSystem.writeFileString(file, "changed\n");
      yield* fileSystem.writeFileString(`${file}.added`, "new\n");
    }
    assert.isTrue(
      yield* driver.checkpoints.restoreCheckpoint({
        cwd: root,
        checkpointRef,
        fallbackToHead: false,
      }),
    );

    assert.deepStrictEqual(
      yield* Effect.forEach(files, (file) => fileSystem.readFileString(file)),
      ["original\n", "captured\n", "original\n"],
    );
    for (const file of files) assert.isFalse(yield* fileSystem.exists(`${file}.added`));

    yield* driver.checkpoints.deleteCheckpointRefs({ cwd: root, checkpointRefs: [checkpointRef] });
    for (const cwd of [root, path.join(root, "library"), path.join(root, "library", "nested")]) {
      const ref = yield* git(cwd, ["for-each-ref", "refs/t3"]);
      assert.strictEqual(ref.stdout, "");
    }
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

// The real VCS process, spawning `command` wherever the driver runs git.
const vcsProcessSpawning = (command: string) =>
  Layer.effect(
    VcsProcess.VcsProcess,
    Effect.map(VcsProcess.make, (vcsProcess) =>
      VcsProcess.VcsProcess.of({ run: (input) => vcsProcess.run({ ...input, command }) }),
    ),
  ).pipe(
    Layer.provide(ProcessRunner.layer),
    Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
  );

it.effect("GitVcsDriver treats a missing git executable as outside any repository", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-missing-git-" });
    const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provide(vcsProcessSpawning(path.join(cwd, "git"))),
    );

    assert.strictEqual(yield* driver.isInsideWorkTree(cwd), false);
    assert.strictEqual(yield* driver.detectRepository(cwd), null);
  }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "GitVcsDriver detects a repository whose path contains a newline",
  () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const parent = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-newline-repo-" });
      const cwd = path.join(parent, "multi\nline");
      yield* fileSystem.makeDirectory(cwd);
      yield* runGit(cwd, ["init"]);
      const driver = yield* GitVcsDriver.makeVcsDriverShape();

      const repository = yield* driver.detectRepository(cwd);
      assert.strictEqual(repository?.rootPath, yield* fileSystem.realPath(cwd));
      assert.strictEqual(repository?.metadataPath, ".git");
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "GitVcsDriver still fails detection when git exists but cannot be spawned",
  () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      // A file without the execute bit fails to spawn with a permission error.
      const notExecutable = yield* fileSystem.makeTempFileScoped({ prefix: "t3-git-" });
      const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provide(vcsProcessSpawning(notExecutable)),
      );

      const error = yield* driver.isInsideWorkTree(process.cwd()).pipe(Effect.flip);
      assert.strictEqual(error._tag, "VcsProcessSpawnError");
      const detectionError = yield* driver.detectRepository(process.cwd()).pipe(Effect.flip);
      assert.strictEqual(detectionError._tag, "VcsProcessSpawnError");
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

it.effect("GitVcsDriver fails repository probes when git cannot read the repository config", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-bad-config-" });
    // The included file's path appears in Git's message and must not read as "no repository".
    const includeDir = path.join(root, "not a git repository");
    yield* fileSystem.makeDirectory(includeDir);
    yield* fileSystem.writeFileString(path.join(includeDir, "included"), "invalid config\n");
    const configs = {
      direct: "invalid config\n",
      included: `[include]\n\tpath = ${path.join(includeDir, "included")}\n`,
    };
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const checkpointRef = CheckpointRef.make("refs/t3/checkpoints/bad-config/0");

    for (const [name, config] of Object.entries(configs)) {
      const cwd = path.join(root, name);
      yield* fileSystem.makeDirectory(cwd);
      yield* runGit(cwd, ["init"]);
      yield* fileSystem.writeFileString(path.join(cwd, ".git", "config"), config);
      const outcomes = {
        isInsideWorkTree: Exit.isFailure(yield* Effect.exit(driver.isInsideWorkTree(cwd))),
        detectRepository: Exit.isFailure(yield* Effect.exit(driver.detectRepository(cwd))),
        hasCheckpointRef: Exit.isFailure(
          yield* Effect.exit(driver.checkpoints.hasCheckpointRef({ cwd, checkpointRef })),
        ),
      };
      assert.deepStrictEqual(
        outcomes,
        { isInsideWorkTree: true, detectRepository: true, hasCheckpointRef: true },
        `a bad ${name} config is an operational failure, not an absent repository or ref`,
      );
    }
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("GitVcsDriver reports no repository for a deleted cwd or a bare repository", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-no-work-tree-" });
    const bare = path.join(root, "bare.git");
    yield* fileSystem.makeDirectory(bare);
    yield* runGit(bare, ["init", "--bare"]);
    const driver = yield* GitVcsDriver.makeVcsDriverShape();

    for (const cwd of [path.join(root, "deleted"), bare]) {
      assert.equal(yield* driver.isInsideWorkTree(cwd), false, cwd);
      assert.equal(yield* driver.detectRepository(cwd), null, cwd);
    }
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

// Git outputs CI's Git version cannot produce with a real repository.
const makeStubbedDriver = Effect.fn("test.makeStubbedDriver")(function* (output: {
  readonly code: number;
  readonly stdout: string;
  readonly stderr: string;
}) {
  const process = yield* VcsProcess.make.pipe(
    Effect.provideService(ProcessRunner.ProcessRunner, {
      run: () =>
        Effect.succeed({
          ...output,
          code: ChildProcessSpawner.ExitCode(output.code),
          timedOut: false,
          stdoutTruncated: false,
          stderrTruncated: false,
          stdoutInvalidUtf8: false,
          stderrInvalidUtf8: false,
        }),
    }),
  );
  return yield* GitVcsDriver.makeVcsDriverShape().pipe(
    Effect.provideService(VcsProcess.VcsProcess, process),
  );
});

it.effect("GitVcsDriver reports no repository when old Git answers false before failing", () =>
  Effect.gen(function* () {
    // Git before 2.38 ignores safe.bareRepository, so a bare repo prints "false" and exits 128.
    const driver = yield* makeStubbedDriver({
      code: 128,
      stdout: "false\n",
      stderr: "fatal: this operation must be run in a work tree\n",
    });

    assert.equal(yield* driver.detectRepository(globalThis.process.cwd()), null);
  }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

it.effect("GitVcsDriver fails a checkpoint ref lookup when git cannot read its object", () =>
  Effect.gen(function* () {
    const driver = yield* makeStubbedDriver({
      code: 1,
      stdout: "",
      stderr: "error: unable to open loose object 0733271782d6: Permission denied\n",
    });
    const checkpointRef = CheckpointRef.make("refs/t3/checkpoints/unreadable/0");

    const exit = yield* Effect.exit(
      driver.checkpoints.hasCheckpointRef({ cwd: globalThis.process.cwd(), checkpointRef }),
    );
    assert.isTrue(Exit.isFailure(exit), "an unreadable checkpoint object is not an absent ref");
  }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

it.effect("GitVcsDriver reads an absent checkpoint ref as absent despite a Git warning", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-git-warning-" });
    yield* runGit(cwd, ["init"]);
    // Valid legacy config that makes every Git command print a deprecation warning.
    yield* runGit(cwd, ["config", "core.fsyncObjectFiles", "true"]);
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const checkpointRef = CheckpointRef.make("refs/t3/checkpoints/absent/0");

    assert.isFalse(yield* driver.checkpoints.hasCheckpointRef({ cwd, checkpointRef }));
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

const gitCommandCount = (operation: string, outcome: string) =>
  Metric.value(
    Metric.withAttributes(gitCommandsTotal, [
      ["operation", operation],
      ["outcome", outcome],
    ]),
  ).pipe(Effect.map((state) => state.count));

const gitCommandTiming = (operation: string) =>
  Metric.value(Metric.withAttributes(gitCommandDuration, [["operation", operation]]));

// Keep VcsProcess's exit classification; replace only the subprocess and its clock.
const makeMeteredDriver = Effect.fn("test.makeMeteredDriver")(function* (exitCode: number) {
  const process = yield* VcsProcess.make.pipe(
    Effect.provideService(ProcessRunner.ProcessRunner, {
      run: () =>
        TestClock.adjust("250 millis").pipe(
          Effect.as({
            code: ChildProcessSpawner.ExitCode(exitCode),
            stdout: "git output",
            stderr: exitCode === 0 ? "" : "fatal: test failure",
            timedOut: false,
            stdoutTruncated: false,
            stderrTruncated: false,
            stdoutInvalidUtf8: false,
            stderrInvalidUtf8: false,
          }),
        ),
    }),
  );
  return yield* GitVcsDriver.makeVcsDriverShape().pipe(
    Effect.provideService(VcsProcess.VcsProcess, process),
  );
});

it.effect("GitVcsDriver meters each git command once under its operation", () =>
  Effect.gen(function* () {
    const operation = "GitVcsDriver.checkpoints.resolveHeadCommit";
    const driver = yield* makeMeteredDriver(0);

    const result = yield* driver.execute({ operation, cwd: "/repo", args: ["rev-parse", "HEAD"] });
    yield* driver.execute({ operation, cwd: "/another-repo", args: ["rev-parse", "other-ref"] });

    assert.strictEqual(result.stdout, "git output");
    assert.strictEqual(yield* gitCommandCount(operation, "success"), 2);
    assert.strictEqual(yield* gitCommandCount(operation, "failure"), 0);
    const timing = yield* gitCommandTiming(operation);
    assert.strictEqual(timing.count, 2);
    assert.strictEqual(timing.sum, 500);
  }).pipe(
    Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
    Effect.provideService(Metric.MetricRegistry, new Map()),
  ),
);

it.effect("GitVcsDriver counts an allowed non-zero exit as a success", () =>
  Effect.gen(function* () {
    const operation = "GitVcsDriver.checkpoints.hasHeadCommit";
    const driver = yield* makeMeteredDriver(1);

    const result = yield* driver.execute({
      operation,
      cwd: "/repo",
      args: ["rev-parse", "--verify", "HEAD"],
      allowNonZeroExit: true,
    });

    assert.strictEqual(result.exitCode, 1);
    assert.strictEqual(yield* gitCommandCount(operation, "success"), 1);
    assert.strictEqual(yield* gitCommandCount(operation, "failure"), 0);
    const timing = yield* gitCommandTiming(operation);
    assert.strictEqual(timing.count, 1);
    assert.strictEqual(timing.sum, 250);
  }).pipe(
    Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
    Effect.provideService(Metric.MetricRegistry, new Map()),
  ),
);

it.effect("GitVcsDriver counts a failed git command as a failure", () =>
  Effect.gen(function* () {
    const operation = "GitVcsDriver.checkpoints.resolveHeadCommit";
    const driver = yield* makeMeteredDriver(128);

    const error = yield* driver
      .execute({ operation, cwd: "/repo", args: ["rev-parse", "HEAD"] })
      .pipe(Effect.flip);

    assert.strictEqual(error._tag, "VcsProcessExitError");
    assert.strictEqual(yield* gitCommandCount(operation, "failure"), 1);
    assert.strictEqual(yield* gitCommandCount(operation, "success"), 0);
    const timing = yield* gitCommandTiming(operation);
    assert.strictEqual(timing.count, 1);
    assert.strictEqual(timing.sum, 250);
  }).pipe(
    Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
    Effect.provideService(Metric.MetricRegistry, new Map()),
  ),
);

it.effect("checkpoint capture meters every git process it starts, including streamed ones", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const liveProcess = yield* VcsProcess.VcsProcess;
    const operation = VcsProcess.CHECKPOINT_CAPTURE_OPERATION;
    let started = 0;
    let streamed = 0;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, {
        run: (input) => {
          if (input.operation === operation) {
            started++;
            if (input.onStdoutChunk !== undefined) streamed++;
          }
          return liveProcess.run(input);
        },
      }),
    );
    const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-metrics-" });
    const { cwd, checkpointRef } = yield* makeCheckpointFixture(driver, root);

    yield* captureDriver.checkpoints.captureCheckpoint({ cwd, checkpointRef });

    assert.isAbove(streamed, 0);
    assert.strictEqual(yield* gitCommandCount(operation, "success"), started);
    assert.strictEqual(yield* gitCommandCount(operation, "failure"), 0);
    assert.strictEqual((yield* gitCommandTiming(operation)).count, started);
  }).pipe(
    Effect.scoped,
    Effect.provide(layerGitContract),
    Effect.provideService(Metric.MetricRegistry, new Map()),
  ),
);

it.effect("git metrics finalize cancellation without exposing arbitrary operation labels", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const process = yield* VcsProcess.make.pipe(
      Effect.provideService(ProcessRunner.ProcessRunner, {
        run: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
      }),
    );
    const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, process),
    );
    const operation = "arbitrary operation /private/path";
    const fiber = yield* driver
      .execute({ operation, cwd: "/repo", args: ["rev-parse", "HEAD"] })
      .pipe(Effect.forkChild);
    yield* Deferred.await(started);
    yield* TestClock.adjust("250 millis");
    yield* Fiber.interrupt(fiber);

    assert.strictEqual(yield* gitCommandCount("other", "interrupt"), 1);
    assert.strictEqual(yield* gitCommandCount(operation, "interrupt"), 0);
    const timing = yield* gitCommandTiming("other");
    assert.strictEqual(timing.count, 1);
    assert.strictEqual(timing.sum, 250);
  }).pipe(
    Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
    Effect.provideService(Metric.MetricRegistry, new Map()),
  ),
);

it.effect("git metrics count fsync fallback and transient retry subprocesses separately", () =>
  Effect.gen(function* () {
    const retryReady = yield* Deferred.make<void>();
    const operation = VcsProcess.CHECKPOINT_CAPTURE_OPERATION;
    let attempts = 0;
    const process = yield* VcsProcess.make.pipe(
      Effect.provideService(HostProcess.Platform, "darwin"),
      Effect.provideService(ProcessRunner.ProcessRunner, {
        run: (input) =>
          Effect.gen(function* () {
            attempts++;
            yield* TestClock.adjust("250 millis");
            if (attempts === 2) {
              assert.include(input.args, "core.fsyncMethod=writeout-only");
              yield* Deferred.succeed(retryReady, undefined);
            }
            return {
              code: ChildProcessSpawner.ExitCode(attempts < 3 ? 128 : 0),
              stdout: "",
              stderr:
                attempts === 1
                  ? "fatal: fsync error on '.git/objects/test': Operation not supported"
                  : attempts === 2
                    ? "fatal: Unable to create '/repo/.git/index.lock': File exists."
                    : "",
              timedOut: false,
              stdoutTruncated: false,
              stderrTruncated: false,
              stdoutInvalidUtf8: false,
              stderrInvalidUtf8: false,
            };
          }),
      }),
    );
    const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, process),
    );
    const fiber = yield* driver
      .execute({ operation, cwd: "/repo", args: ["-c", "core.fsyncMethod=batch", "add"] })
      .pipe(Effect.forkChild);
    yield* Deferred.await(retryReady);
    yield* TestClock.adjust("75 millis");
    yield* Fiber.join(fiber);

    assert.strictEqual(attempts, 3);
    assert.strictEqual(yield* gitCommandCount(operation, "failure"), 2);
    assert.strictEqual(yield* gitCommandCount(operation, "success"), 1);
    const timing = yield* gitCommandTiming(operation);
    assert.strictEqual(timing.count, 3);
    assert.strictEqual(timing.sum, 750);
  }).pipe(
    Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
    Effect.provideService(Metric.MetricRegistry, new Map()),
  ),
);

it.effect(
  "checkpoint capture still fails when a clean filter rejects a file [Jones compatibility]",
  () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const root = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-checkpoint-filter-failure-",
      });
      const { cwd, git, gitDir, checkpointRef } = yield* makeCheckpointFixture(driver, root);
      yield* fileSystem.writeFileString(
        path.join(cwd, ".gitattributes"),
        "file.txt filter=reject\n",
      );
      yield* git(["config", "filter.reject.clean", "false"]);
      yield* git(["config", "filter.reject.required", "true"]);
      const originalIndex = yield* fileSystem.readFile(path.join(gitDir, "index"));

      const result = yield* Effect.result(
        driver.checkpoints.captureCheckpoint({ cwd, checkpointRef }),
      );

      assert.strictEqual(result._tag, "Failure");
      assert.deepEqual(yield* fileSystem.readFile(path.join(gitDir, "index")), originalIndex);
      assert.isFalse(yield* driver.checkpoints.hasCheckpointRef({ cwd, checkpointRef }));
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint recovery refuses excessive candidates before probing", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const liveProcess = yield* VcsProcess.VcsProcess;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-recovery-cap-" });
    const { cwd, git, gitDir, checkpointRef } = yield* makeCheckpointFixture(driver, root);
    yield* git(["init", "empty0"]);
    for (let i = 1; i < 65; i++)
      yield* fs.copy(path.join(cwd, "empty0"), path.join(cwd, `empty${i}`));
    const originalIndex = yield* fs.readFile(path.join(gitDir, "index"));
    let stageError: VcsProcessExitError | undefined;
    let nestedProbes = 0;
    let stageAttempts = 0;
    const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, {
        run: (input) => {
          if (input.cwd !== cwd && input.args.includes("rev-parse")) nestedProbes++;
          if (input.args.includes("add") && input.args.includes("-A")) stageAttempts++;
          return liveProcess.run(input).pipe(
            Effect.tapError((error) => {
              if (error._tag === "VcsProcessExitError") stageError = error;
              return Effect.void;
            }),
          );
        },
      }),
    );
    const result = yield* Effect.result(
      captureDriver.checkpoints.captureCheckpoint({ cwd, checkpointRef }),
    );
    assert.strictEqual(nestedProbes, 0);
    assert.strictEqual(stageAttempts, 1);
    assert.strictEqual(result._tag, "Failure");
    if (result._tag === "Failure") assert.strictEqual(result.failure, stageError);
    assert.isFalse(yield* driver.checkpoints.hasCheckpointRef({ cwd, checkpointRef }));
    assert.deepEqual(yield* fs.readFile(path.join(gitDir, "index")), originalIndex);
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each(["linux", "darwin", "win32"] as const)(
  "GitVcsDriver flushes checkpoint objects and refs to disk before publishing them on %s",
  (platform) => {
    const observedArgs: ReadonlyArray<string>[] = [];

    return Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-durability-" });
      const cwd = path.join(root, "worktree");
      const common = path.join(root, "repository", ".git");
      const gitDir = path.join(common, "worktrees", "checkpoint-test");
      yield* fs.makeDirectory(path.join(common, "objects"), { recursive: true });
      const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provideService(VcsProcess.VcsProcess, {
          run: (input) =>
            Effect.sync(() => {
              observedArgs.push(input.args);
              const metadata =
                input.args.includes("--git-common-dir") && input.args.includes("--git-path");
              return {
                exitCode: ChildProcessSpawner.ExitCode(metadata ? 1 : 0),
                stdout: metadata
                  ? `${common}\n${gitDir}\n${path.join(gitDir, "index")}\n${cwd}\n\n`
                  : input.args.includes("write-tree")
                    ? "tree0000\n"
                    : input.args.includes("commit-tree")
                      ? "commit0000\n"
                      : "",
                stderr: "",
                stdoutTruncated: false,
                stderrTruncated: false,
              };
            }),
        }),
      );

      yield* driver.checkpoints.captureCheckpoint({
        cwd,
        checkpointRef: CheckpointRef.make("refs/t3/checkpoints/thread/turn/1"),
      });

      const writeCommands = ["add", "write-tree", "commit-tree", "update-ref"];
      const writes = observedArgs.filter((args) =>
        writeCommands.some((command) => args.includes(command)),
      );
      assert.strictEqual(writes.length, 4);
      for (const args of writes) {
        const command = args.findIndex((arg) => writeCommands.includes(arg));
        for (const setting of ["core.fsync=objects,reference", "core.fsyncMethod=fsync"]) {
          const index = args.indexOf(setting);
          assert.strictEqual(args[index - 1], "-c", args.join(" "));
          assert.isBelow(index, command);
        }
        assert.strictEqual(args.includes("core.fsyncMethod=batch"), platform === "win32");
      }
      assert.deepStrictEqual(observedArgs.at(-1), [
        "-C",
        cwd,
        "-c",
        "core.fsync=objects,reference",
        "-c",
        "core.fsyncMethod=fsync",
        ...(platform === "win32" ? ["-c", "core.fsyncMethod=batch"] : []),
        "update-ref",
        "refs/t3/checkpoints/thread/turn/1",
        "commit0000",
      ]);
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
      Effect.provideService(HostProcess.Platform, platform),
    );
  },
);

it.effect("refuses primary checkout capture before running index, object, or ref writers", () => {
  const commands: Array<ReadonlyArray<string>> = [];
  return Effect.gen(function* () {
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const result = yield* driver.checkpoints
      .captureCheckpoint({
        cwd: "/synthetic-primary",
        checkpointRef: CheckpointRef.make("refs/t3/checkpoints/primary-refusal"),
      })
      .pipe(Effect.result);
    assert.equal(result._tag, "Failure");
    if (result._tag !== "Failure") return assert.fail("Expected primary checkout refusal");
    assert.equal(result.failure._tag, "VcsPrimaryCheckoutCheckpointError");
    assert.deepEqual(commands, [
      [
        "-C",
        "/synthetic-primary",
        "rev-parse",
        "--path-format=absolute",
        "--git-common-dir",
        "--git-dir",
        "--git-path",
        "index",
        "--show-toplevel",
        "--show-prefix",
        "--verify",
        "--quiet",
        "HEAD",
      ],
    ]);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        NodeServices.layer,
        NodeCrypto.layer,
        Layer.mock(VcsProcess.VcsProcess)({
          run: (input) =>
            Effect.sync(() => {
              commands.push(input.args);
              return {
                exitCode: ChildProcessSpawner.ExitCode(0),
                stdout:
                  "/synthetic-primary/.git\n/synthetic-primary/.git\n/synthetic-primary/.git/index\n/synthetic-primary\n\n0123456789012345678901234567890123456789\n",
                stderr: "",
                stdoutTruncated: false,
                stderrTruncated: false,
              };
            }),
        }),
      ),
    ),
  );
});

const malformedCaptureMetadata = [
  {
    label: "empty directory",
    stdout:
      "\n/synthetic/linked\n/synthetic/index\n/synthetic/worktree\n\n0123456789012345678901234567890123456789\n",
    exitCode: 0,
    headProbe: false,
  },
  {
    label: "relative directory",
    stdout:
      ".git\n/synthetic/linked\n/synthetic/index\n/synthetic/worktree\n\n0123456789012345678901234567890123456789\n",
    exitCode: 0,
    headProbe: false,
  },
  {
    label: "missing directory",
    stdout:
      "/synthetic/common\n/synthetic/index\n/synthetic/worktree\n\n0123456789012345678901234567890123456789\n",
    exitCode: 0,
    headProbe: false,
  },
  {
    label: "extra field",
    stdout:
      "/synthetic/common\n/synthetic/linked\n/synthetic/index\n/synthetic/worktree\n\nextra\n0123456789012345678901234567890123456789\n",
    exitCode: 0,
    headProbe: false,
  },
  {
    label: "invalid object ID",
    stdout:
      "/synthetic/common\n/synthetic/linked\n/synthetic/index\n/synthetic/worktree\n\nnot-an-object\n",
    exitCode: 0,
    headProbe: true,
  },
  {
    label: "zero object ID",
    stdout:
      "/synthetic/common\n/synthetic/linked\n/synthetic/index\n/synthetic/worktree\n\n0000000000000000000000000000000000000000\n",
    exitCode: 0,
    headProbe: true,
  },
  {
    label: "missing object ID",
    stdout: "/synthetic/common\n/synthetic/linked\n/synthetic/index\n/synthetic/worktree\n\n",
    exitCode: 0,
    headProbe: true,
  },
  {
    label: "contradictory unborn exit",
    stdout:
      "/synthetic/common\n/synthetic/linked\n/synthetic/index\n/synthetic/worktree\n\n0123456789012345678901234567890123456789\n",
    exitCode: 1,
    headProbe: true,
  },
  {
    label: "missing final terminator",
    stdout:
      "/synthetic/common\n/synthetic/linked\n/synthetic/index\n/synthetic/worktree\n\n0123456789012345678901234567890123456789",
    exitCode: 0,
    headProbe: true,
  },
  {
    label: "truncated output",
    stdout:
      "/synthetic/common\n/synthetic/linked\n/synthetic/index\n/synthetic/worktree\n\n0123456789012345678901234567890123456789\n",
    exitCode: 0,
    headProbe: true,
    stdoutTruncated: true,
  },
  {
    label: "invalid UTF-8",
    stdout:
      "/synthetic/common\n/synthetic/linked\n/synthetic/index\n/synthetic/worktree\n\n0123456789012345678901234567890123456789\n",
    exitCode: 0,
    headProbe: true,
    stdoutInvalidUtf8: true,
  },
] as const;

it.effect.each(malformedCaptureMetadata)(
  "capture metadata rejects $label before a primary writer",
  (sample) => {
    const commands: ReadonlyArray<string>[] = [];
    return Effect.gen(function* () {
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const result = yield* Effect.result(
        driver.checkpoints.captureCheckpoint({
          cwd: "/synthetic-primary",
          checkpointRef: CheckpointRef.make("refs/t3/checkpoints/invalid-metadata"),
        }),
      );
      assert.equal(result._tag, "Failure");
      if (result._tag !== "Failure") return assert.fail("Expected capture refusal");
      assert.equal(result.failure._tag, "VcsPrimaryCheckoutCheckpointError");
      assert.deepEqual(commands, [
        [
          "-C",
          "/synthetic-primary",
          "rev-parse",
          "--path-format=absolute",
          "--git-common-dir",
          "--git-dir",
          "--git-path",
          "index",
          "--show-toplevel",
          "--show-prefix",
          "--verify",
          "--quiet",
          "HEAD",
        ],
        ["-C", "/synthetic-primary", "rev-parse", "--git-common-dir"],
        ["-C", "/synthetic-primary", "rev-parse", "--git-dir"],
        ...(sample.headProbe
          ? [["-C", "/synthetic-primary", "rev-parse", "--verify", "--quiet", "HEAD"]]
          : []),
      ]);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          NodeServices.layer,
          NodeCrypto.layer,
          Layer.mock(VcsProcess.VcsProcess)({
            run: (input) =>
              Effect.gen(function* () {
                commands.push(input.args);
                const args = input.args.slice(2);
                if (commands.length === 1)
                  return {
                    exitCode: ChildProcessSpawner.ExitCode(sample.exitCode),
                    stdout: sample.stdout,
                    stderr: "",
                    stdoutTruncated: "stdoutTruncated" in sample && sample.stdoutTruncated,
                    stderrTruncated: false,
                    stdoutInvalidUtf8: "stdoutInvalidUtf8" in sample && sample.stdoutInvalidUtf8,
                  };
                const stdout =
                  args.join(" ") === "rev-parse --git-common-dir" ||
                  args.join(" ") === "rev-parse --git-dir"
                    ? "/synthetic-primary/.git\n"
                    : args.join(" ") === "rev-parse --verify --quiet HEAD"
                      ? "0123456789012345678901234567890123456789\n"
                      : undefined;
                if (stdout === undefined)
                  return yield* Effect.die(
                    new Error(`Unexpected capture command: ${input.args.join(" ")}`),
                  );
                return {
                  exitCode: ChildProcessSpawner.ExitCode(0),
                  stdout,
                  stderr: "",
                  stdoutTruncated: false,
                  stderrTruncated: false,
                };
              }),
          }),
        ),
      ),
    );
  },
);

it.effect("capture metadata preserves SHA-256 HEAD identity and the primary fence", () => {
  const commands: ReadonlyArray<string>[] = [];
  return Effect.gen(function* () {
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const result = yield* Effect.result(
      driver.checkpoints.captureCheckpoint({
        cwd: "/synthetic-primary",
        checkpointRef: CheckpointRef.make("refs/t3/checkpoints/sha256-primary"),
      }),
    );
    assert.equal(result._tag, "Failure");
    if (result._tag !== "Failure") return assert.fail("Expected primary refusal");
    assert.equal(result.failure._tag, "VcsPrimaryCheckoutCheckpointError");
    assert.deepEqual(commands, [
      [
        "-C",
        "/synthetic-primary",
        "rev-parse",
        "--path-format=absolute",
        "--git-common-dir",
        "--git-dir",
        "--git-path",
        "index",
        "--show-toplevel",
        "--show-prefix",
        "--verify",
        "--quiet",
        "HEAD",
      ],
    ]);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        NodeServices.layer,
        NodeCrypto.layer,
        Layer.mock(VcsProcess.VcsProcess)({
          run: (input) =>
            Effect.sync(() => {
              commands.push(input.args);
              return {
                exitCode: ChildProcessSpawner.ExitCode(0),
                stdout:
                  "/synthetic-primary/.git\n/synthetic-primary/.git\n/synthetic-primary/.git/index\n/synthetic-primary\n\n0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n",
                stderr: "",
                stdoutTruncated: false,
                stderrTruncated: false,
              };
            }),
        }),
      ),
    ),
  );
});

it.effect("capture metadata propagates an unknown HEAD probe without treating it as unborn", () => {
  const commands: ReadonlyArray<string>[] = [];
  return Effect.gen(function* () {
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const result = yield* Effect.result(
      driver.checkpoints.captureCheckpoint({
        cwd: "/synthetic-linked",
        checkpointRef: CheckpointRef.make("refs/t3/checkpoints/unknown-head"),
      }),
    );
    assert.equal(result._tag, "Failure");
    if (result._tag !== "Failure" || result.failure._tag !== "VcsProcessExitError")
      return assert.fail("Expected the actual HEAD probe failure");
    assert.equal(result.failure.operation, "GitVcsDriver.checkpoints.hasHeadCommit");
    assert.equal(result.failure.exitCode, 128);
    assert.deepEqual(commands, [
      [
        "-C",
        "/synthetic-linked",
        "rev-parse",
        "--path-format=absolute",
        "--git-common-dir",
        "--git-dir",
        "--git-path",
        "index",
        "--show-toplevel",
        "--show-prefix",
        "--verify",
        "--quiet",
        "HEAD",
      ],
      ["-C", "/synthetic-linked", "rev-parse", "--git-common-dir"],
      ["-C", "/synthetic-linked", "rev-parse", "--git-dir"],
      ["-C", "/synthetic-linked", "rev-parse", "--verify", "--quiet", "HEAD"],
    ]);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        NodeServices.layer,
        NodeCrypto.layer,
        Layer.mock(VcsProcess.VcsProcess)({
          run: (input) =>
            Effect.gen(function* () {
              commands.push(input.args);
              const args = input.args.slice(2).join(" ");
              if (commands.length === 1)
                return {
                  exitCode: ChildProcessSpawner.ExitCode(0),
                  stdout: "--path-format=absolute\n",
                  stderr: "",
                  stdoutTruncated: false,
                  stderrTruncated: false,
                };
              if (args === "rev-parse --verify --quiet HEAD")
                return {
                  exitCode: ChildProcessSpawner.ExitCode(128),
                  stdout: "",
                  stderr: "fixture HEAD lookup failure",
                  stdoutTruncated: false,
                  stderrTruncated: false,
                };
              const stdout =
                args === "rev-parse --git-common-dir"
                  ? "/synthetic/.git\n"
                  : args === "rev-parse --git-dir"
                    ? "/synthetic/.git/worktrees/linked\n"
                    : undefined;
              if (stdout === undefined)
                return yield* Effect.die(
                  new Error(`Unexpected capture command: ${input.args.join(" ")}`),
                );
              return {
                exitCode: ChildProcessSpawner.ExitCode(0),
                stdout,
                stderr: "",
                stdoutTruncated: false,
                stderrTruncated: false,
              };
            }),
        }),
      ),
    ),
  );
});

it.effect("capture metadata falls back from an echoed option with real linked index custody", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const liveProcess = yield* VcsProcess.VcsProcess;
    const fixtureDriver = yield* GitVcsDriver.makeVcsDriverShape();
    const fixtureRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-old-option-" });
    const { cwd, git, gitDir, checkpointRef } = yield* makeCheckpointFixture(
      fixtureDriver,
      fixtureRoot,
    );
    const originalIndex = yield* fs.readFile(path.join(gitDir, "index"));
    const commands: ReadonlyArray<string>[] = [];
    const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, {
        run: (input) => {
          commands.push(input.args);
          return liveProcess
            .run(input)
            .pipe(
              Effect.map((result) =>
                input.args.includes("--git-common-dir") && input.args.includes("--git-path")
                  ? { ...result, stdout: `--path-format=absolute\n${result.stdout}` }
                  : result,
              ),
            );
        },
      }),
    );
    yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
    assert.equal((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "unstaged\n");
    assert.deepEqual(yield* fs.readFile(path.join(gitDir, "index")), originalIndex);
    const names = commands.map((args) => {
      let index = 0;
      while (args[index] === "-C" || args[index] === "-c") index += 2;
      return args[index];
    });
    assert.deepEqual(names, [
      "rev-parse",
      "rev-parse",
      "rev-parse",
      "rev-parse",
      "config",
      "rev-parse",
      "read-tree",
      "ls-files",
      "add",
      "write-tree",
      "commit-tree",
      "update-ref",
      // The fallback deliberately has no trusted batched top-level optimization.
      "submodule",
    ]);
    assert.deepEqual(commands.slice(0, 3), [
      [
        "-C",
        cwd,
        "rev-parse",
        "--path-format=absolute",
        "--git-common-dir",
        "--git-dir",
        "--git-path",
        "index",
        "--show-toplevel",
        "--show-prefix",
        "--verify",
        "--quiet",
        "HEAD",
      ],
      ["-C", cwd, "rev-parse", "--git-common-dir"],
      ["-C", cwd, "rev-parse", "--git-dir"],
    ]);
    assert.deepEqual(commands[3], ["-C", cwd, "rev-parse", "--show-prefix"]);
    assert.deepEqual(commands[5], [
      "-C",
      cwd,
      "rev-parse",
      "--path-format=absolute",
      "--git-path",
      "index",
    ]);
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint capture keeps the legacy path when Git lacks add --sparse", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const liveProcess = yield* VcsProcess.VcsProcess;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-legacy-" });
    const { cwd, git, gitDir, checkpointRef } = yield* makeCheckpointFixture(driver, root);
    yield* git(["sparse-checkout", "set", "--cone", "included"]);
    const originalIndex = yield* fs.readFile(path.join(gitDir, "index"));
    const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, {
        run: (input) => {
          if (input.args.includes("-h"))
            return Effect.succeed({
              exitCode: ChildProcessSpawner.ExitCode(129),
              stdout: "usage: git add",
              stderr: "",
              stdoutTruncated: false,
              stderrTruncated: false,
            });
          return liveProcess.run(
            input.args.includes("--sparse")
              ? {
                  ...input,
                  args: input.args.map((arg) =>
                    arg === "--sparse" ? "--unsupported-sparse" : arg,
                  ),
                }
              : input,
          );
        },
      }),
    );
    yield* captureDriver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
    assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "unstaged\n");
    assert.deepEqual(yield* fs.readFile(path.join(gitDir, "index")), originalIndex);
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each(["normal", "flags", "sparse"] as const)(
  "checkpoint index inspection handles entries beyond the output cap (index=%s)",
  (indexMode) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const liveProcess = yield* VcsProcess.VcsProcess;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-inspection-" });
      const { cwd, git, gitDir, checkpointRef } = yield* makeCheckpointFixture(driver, root);
      yield* fs.writeFileString(path.join(cwd, ".gitattributes"), "stable filter=probe\n");
      yield* fs.writeFileString(path.join(cwd, "stable"), "unchanged\n");
      yield* fs.writeFileString(path.join(cwd, "z-skipped"), "original\n");
      yield* fs.makeDirectory(path.join(cwd, "excluded"));
      yield* fs.writeFileString(path.join(cwd, "excluded/file"), "absent\n");
      yield* fs.writeFileString(
        path.join(gitDir, "filter.cjs"),
        'require("node:fs").appendFileSync(require("node:path").join(__dirname, "reads"), "read\\n"); process.stdin.pipe(process.stdout);',
      );
      yield* git(["config", "filter.probe.clean", `node "${path.join(gitDir, "filter.cjs")}"`]);
      yield* fs.utimes(path.join(cwd, "stable"), 1_700_000_000, 1_700_000_000);
      yield* git(["add", "."]);
      yield* git(["commit", "-m", "inspection fixture"]);
      if (indexMode === "flags") yield* git(["update-index", "--skip-worktree", "z-skipped"]);
      if (indexMode === "sparse")
        yield* git(["sparse-checkout", "set", "--cone", "--sparse-index", "included"]);
      yield* fs.writeFileString(path.join(cwd, "z-skipped"), "modified\n");
      yield* fs.writeFileString(path.join(gitDir, "reads"), "");
      const originalIndex = yield* fs.readFile(path.join(gitDir, "index"));
      const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provideService(VcsProcess.VcsProcess, {
          run: (input) =>
            liveProcess.run(
              input.args.includes("ls-files")
                ? {
                    ...input,
                    maxOutputBytes: 8,
                    onStdoutChunk: (chunk) => {
                      for (let i = 0; i < chunk.length; i++)
                        input.onStdoutChunk?.(chunk.subarray(i, i + 1));
                    },
                  }
                : input,
            ),
        }),
      );
      yield* captureDriver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      assert.strictEqual((yield* git(["show", `${checkpointRef}:z-skipped`])).stdout, "modified\n");
      if (indexMode !== "flags")
        assert.strictEqual(yield* fs.readFileString(path.join(gitDir, "reads")), "");
      assert.deepEqual(yield* fs.readFile(path.join(gitDir, "index")), originalIndex);
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);
