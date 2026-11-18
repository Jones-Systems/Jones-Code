import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as TestClock from "effect/testing/TestClock";

import * as ServerConfig from "../../config.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import { makeRemoteFetchObservation, type RemoteFetchTarget } from "./remoteFetchObservation.ts";

const configured = { remoteName: "origin", coverage: { kind: "configured-refspecs" } } as const;
const branch = (branchName: string): RemoteFetchTarget => ({
  remoteName: "origin",
  coverage: { kind: "branch", branchName },
});
const unknown = { latestVerifiedAtMs: null, latestOutcome: null };

it.effect.each(["succeeded", "failed"] as const)(
  "orders late completion behind a newer %s outcome",
  (status) =>
    Effect.gen(function* () {
      const model = makeRemoteFetchObservation();
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const old = yield* model
        .observe(
          "/fixture/.git",
          configured,
          Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release))),
          () => ({ status: "succeeded" }),
        )
        .pipe(Effect.forkScoped);
      yield* Deferred.await(started);
      yield* TestClock.adjust(1);
      yield* model.observe("/fixture/.git", configured, Effect.void, () => ({ status }));
      const newerTime = yield* Clock.currentTimeMillis;
      yield* TestClock.adjust(1);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(old);
      expect(model.read("/fixture/.git", configured)).toEqual({
        latestOutcome: { status, completedAtMs: newerTime },
        latestVerifiedAtMs: status === "succeeded" ? newerTime : newerTime + 1,
      });
    }),
);

const dependencies = ServerConfig.layerTest(process.cwd(), {
  prefix: "remote-fetch-observation-config-",
}).pipe(Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)));

it.effect("observes only completed Git fetches for the requested remote and coverage", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const scratch = yield* fs.makeTempDirectoryScoped({ prefix: "remote-fetch-observation-" });
    const cwd = path.join(scratch, "repo");
    const remote = path.join(scratch, "remote.git");
    const linked = path.join(scratch, "linked");
    const globalConfig = path.join(scratch, "gitconfig");
    yield* fs.writeFileString(globalConfig, "");
    yield* fs.makeDirectory(cwd);
    yield* fs.makeDirectory(remote);
    const crossedLaunchGate = yield* Deferred.make<void>();
    let holdBranchFetch = false;
    const delegate = yield* ChildProcessSpawner.ChildProcessSpawner;
    const spawner = ChildProcessSpawner.make(
      Effect.fnUntraced(function* (command) {
        if (
          holdBranchFetch &&
          ChildProcess.isStandardCommand(command) &&
          command.args[0] === "fetch"
        ) {
          yield* Deferred.succeed(crossedLaunchGate, undefined);
          return yield* Effect.never;
        }
        return yield* delegate.spawn(command);
      }),
    );

    yield* Effect.gen(function* () {
      const driver = yield* GitVcsDriver.make;
      const git = (workingDir: string, ...args: string[]) =>
        driver.execute({ operation: "RemoteFetchObservation.test.git", cwd: workingDir, args });
      yield* git(cwd, "init", "-b", "main");
      yield* git(cwd, "commit", "--allow-empty", "-m", "fixture");
      yield* git(remote, "init", "--bare", "-b", "main");
      yield* git(cwd, "remote", "add", "origin", remote);
      yield* git(cwd, "push", "-u", "origin", "main");
      yield* git(cwd, "worktree", "add", "-b", "linked", linked, "main");

      const read = (target: RemoteFetchTarget, workingDir = cwd) =>
        driver.readRemoteFetchObservation({ cwd: workingDir, ...target });
      yield* driver.statusDetailsRemote(cwd, { refreshUpstream: false });
      expect(yield* read(configured)).toEqual(unknown);
      expect(yield* driver.fetchWorktreeBaseCommit({ cwd, refName: "HEAD" })).toBeNull();
      expect(yield* read(branch("HEAD"))).toEqual(unknown);

      // The fixture already has the tracking ref. An unchanged successful fetch still verifies it.
      yield* TestClock.adjust(1);
      const statusAt = yield* Clock.currentTimeMillis;
      yield* driver.statusDetailsRemote(cwd);
      const configuredSuccess = {
        latestVerifiedAtMs: statusAt,
        latestOutcome: { status: "succeeded", completedAtMs: statusAt },
      };
      expect(yield* read(configured, linked)).toEqual(configuredSuccess);
      expect(yield* read({ ...configured, remoteName: "mirror" })).toEqual(unknown);
      expect(yield* read(branch("main"))).toEqual(unknown);
      yield* TestClock.adjust(1);
      yield* driver.statusDetailsRemote(cwd);
      expect(yield* read(configured)).toEqual(configuredSuccess);

      const branchAt = yield* Clock.currentTimeMillis;
      const launchFetch = driver.fetchWorktreeBaseCommit({
        cwd: linked,
        refName: "origin/main",
        remote: { remoteName: "origin", branchName: "main" },
      });
      expect(yield* launchFetch).toMatchObject({ remoteRefName: "origin/main" });
      const branchSuccess = {
        latestVerifiedAtMs: branchAt,
        latestOutcome: { status: "succeeded", completedAtMs: branchAt },
      };
      expect(yield* read(branch("main"))).toEqual(branchSuccess);
      expect(yield* read(configured)).toEqual(configuredSuccess);

      holdBranchFetch = true;
      const launchGate = yield* launchFetch.pipe(
        Effect.timeoutOption("1 second"),
        Effect.forkScoped,
      );
      yield* Deferred.await(crossedLaunchGate);
      yield* TestClock.adjust("1 second");
      expect(yield* Fiber.join(launchGate)).toEqual(Option.none());
      expect(yield* read(branch("main"))).toEqual(branchSuccess);
      holdBranchFetch = false;

      yield* git(remote, "--git-dir=.", "update-ref", "-d", "refs/heads/main");
      const failureAt = yield* Clock.currentTimeMillis;
      expect(yield* launchFetch).toBeNull();
      expect(yield* read(branch("main"))).toEqual({
        latestVerifiedAtMs: branchAt,
        latestOutcome: {
          status: "failed",
          reason: "ref_not_found",
          completedAtMs: failureAt,
        },
      });
      expect(yield* read(configured)).toEqual(configuredSuccess);

      // The generic fetch falls back to a configured-refspec fetch after a missing branch.
      yield* driver.fetchRemote({ cwd, remoteName: "origin", refName: "unpublished" });
      expect(yield* read(branch("unpublished"))).toEqual(unknown);
      expect(yield* read(configured)).toEqual(configuredSuccess);

      yield* fs.rename(remote, path.join(scratch, "offline.git"));
      yield* TestClock.adjust("15 seconds");
      const statusFailureAt = yield* Clock.currentTimeMillis;
      yield* driver.statusDetailsRemote(cwd);
      expect(yield* read(configured)).toMatchObject({
        latestVerifiedAtMs: statusAt,
        latestOutcome: { status: "failed", completedAtMs: statusFailureAt },
      });

      const restarted = yield* GitVcsDriver.make;
      expect(yield* restarted.readRemoteFetchObservation({ cwd, ...configured })).toEqual(unknown);
      expect(yield* restarted.readRemoteFetchObservation({ cwd, ...branch("main") })).toEqual(
        unknown,
      );
    }).pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Effect.provideService(HostProcess.Environment, {
        PATH: process.env.PATH,
        HOME: scratch,
        GIT_CONFIG_NOSYSTEM: "1",
        GIT_CONFIG_GLOBAL: globalConfig,
        GIT_ALLOW_PROTOCOL: "file",
        GIT_AUTHOR_NAME: "Fixture",
        GIT_AUTHOR_EMAIL: "fixture@example.invalid",
        GIT_COMMITTER_NAME: "Fixture",
        GIT_COMMITTER_EMAIL: "fixture@example.invalid",
      }),
    );
  }).pipe(Effect.provide(dependencies)),
);
