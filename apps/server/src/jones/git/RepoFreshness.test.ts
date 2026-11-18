import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { ProjectId, type OrchestrationProjectShell } from "@t3tools/contracts";
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
import * as Tracer from "effect/Tracer";

import * as ServerConfig from "../../config.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import * as RepoFreshness from "./RepoFreshness.ts";

const dependencies = ServerConfig.layerTest(process.cwd(), {
  prefix: "repo-freshness-config-",
}).pipe(Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)));

it.effect(
  "checks the remote default, shares live fetches, and fences evidence after failure or retarget",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const scratch = yield* fs.makeTempDirectoryScoped({ prefix: "repo-freshness-" });
      const repo = path.join(scratch, "repo");
      const linked = path.join(scratch, "linked");
      const remote = path.join(scratch, "remote.git");
      const updater = path.join(scratch, "updater");
      yield* fs.makeDirectory(repo);
      const held = yield* Deferred.make<void>();
      const bothJoined = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const delegate = yield* ChildProcessSpawner.ChildProcessSpawner;
      let fetches = 0;
      let waits = 0;
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.gen(function* () {
          if (
            ChildProcess.isStandardCommand(command) &&
            command.args[0] === "fetch" &&
            command.args.includes("--refmap=")
          ) {
            fetches++;
            yield* Deferred.succeed(held, undefined);
            yield* Deferred.await(release);
          }
          return yield* delegate.spawn(command);
        }),
      );
      const tracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options);
          if (options.name === "RepoFreshness.startCheck") {
            const end = span.end.bind(span);
            span.end = (endTime, exit) => {
              end(endTime, exit);
              if (++waits === 2) Deferred.doneUnsafe(bothJoined, Effect.void);
            };
          }
          return span;
        },
      });

      yield* Effect.gen(function* () {
        const driver = yield* GitVcsDriver.make;
        const git = (cwd: string, ...args: string[]) =>
          driver.execute({ operation: "RepoFreshness.test.git", cwd, args });
        yield* git(repo, "init", "-b", "trunk");
        yield* git(repo, "commit", "--allow-empty", "-m", "base");
        yield* git(repo, "init", "--bare", "-b", "trunk", remote);
        yield* git(repo, "remote", "add", "mirror", remote);
        yield* git(repo, "push", "-u", "mirror", "trunk");
        yield* git(repo, "worktree", "add", "-b", "linked", linked, "trunk");
        yield* git(repo, "switch", "-c", "feature");
        yield* git(repo, "clone", remote, updater);
        yield* git(updater, "commit", "--allow-empty", "-m", "remote advance");
        yield* git(updater, "push", "origin", "trunk");
        const remoteSha = (yield* git(updater, "rev-parse", "HEAD")).stdout.trim();
        const localSha = (yield* git(repo, "rev-parse", "HEAD")).stdout.trim();

        const project = (id: string, workspaceRoot: string): OrchestrationProjectShell => ({
          id: ProjectId.make(id),
          title: id,
          workspaceRoot,
          defaultModelSelection: null,
          scripts: [],
          createdAt: "2026-01-01T00:00:00Z",
          updatedAt: "2026-01-01T00:00:00Z",
        });
        const primary = project("a", repo);
        const alias = project("b", linked);
        const rows = [primary, alias];

        yield* Effect.gen(function* () {
          const service = yield* RepoFreshness.RepoFreshness;
          const settings = yield* ServerSettings.ServerSettingsService;
          expect(yield* service.status(primary.id)).toMatchObject({
            cadenceSeconds: 0,
            latestVerified: null,
            freshness: "unknown",
          });
          yield* TestClock.adjust("30 seconds");
          expect(fetches).toBe(0);

          const manual = yield* Effect.all(
            [service.refresh(primary.id), service.refresh(alias.id)],
            { concurrency: 2 },
          ).pipe(Effect.withTracer(tracer), Effect.forkScoped);
          yield* Deferred.await(held);
          yield* Deferred.await(bothJoined);
          expect(fetches).toBe(1);
          expect((yield* service.status(primary.id)).latestVerified).toBeNull();
          yield* Deferred.succeed(release, undefined);
          const [first, joined] = yield* Fiber.join(manual);
          expect(first.status.latestVerified).toEqual(joined.status.latestVerified);
          expect(first.status.latestVerified).toMatchObject({
            remoteName: "mirror",
            branchName: "trunk",
            commitSha: remoteSha,
          });
          expect(first.checkout).toEqual({ status: "skipped", reason: "checkout-update-deferred" });
          expect((yield* git(repo, "rev-parse", "HEAD")).stdout.trim()).toBe(localSha);
          expect((yield* git(repo, "rev-parse", "refs/remotes/mirror/trunk")).stdout.trim()).toBe(
            localSha,
          );

          yield* TestClock.adjust("1 second");
          const unchanged = (yield* service.refresh(primary.id)).status;
          expect(fetches).toBe(2);
          expect(unchanged.latestVerified).toMatchObject({
            commitSha: remoteSha,
            checkedAtMs: yield* Clock.currentTimeMillis,
          });
          expect(unchanged.latestVerified?.checkedAtMs).toBeGreaterThan(
            first.status.latestVerified!.checkedAtMs,
          );

          yield* fs.rename(remote, path.join(scratch, "offline.git"));
          const failed = (yield* service.refresh(primary.id)).status;
          expect(failed.latestAttempt?.status).toBe("failed");
          expect(failed.latestVerified).toEqual(unchanged.latestVerified);

          yield* settings.updateSettings({
            projectSettingsOverrides: {
              [primary.id]: { repoFreshnessCadenceSeconds: 120 },
            },
          });
          expect((yield* service.status(primary.id)).freshness).toBe("fresh");
          yield* TestClock.adjust("300 seconds");
          expect((yield* service.status(primary.id)).freshness).toBe("stale");

          yield* git(repo, "remote", "set-url", "mirror", path.join(scratch, "another.git"));
          expect(yield* service.status(primary.id)).toMatchObject({
            latestVerified: null,
            latestAttempt: null,
            freshness: "unknown",
          });
        }).pipe(
          Effect.provide(
            RepoFreshness.layer.pipe(
              Layer.provideMerge(
                Layer.mergeAll(
                  Layer.succeed(GitVcsDriver.GitVcsDriver, driver),
                  ServerSettings.ServerSettingsService.layerTest(),
                  Layer.mock(ProjectStore.ProjectStoreV2)({
                    getShell: (id) =>
                      Effect.succeed(Option.fromUndefinedOr(rows.find((row) => row.id === id))),
                    listShells: () => Effect.succeed(rows),
                  }),
                ),
              ),
            ),
          ),
        );
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(HostProcess.Environment, {
          PATH: process.env.PATH,
          HOME: scratch,
          GIT_CONFIG_NOSYSTEM: "1",
          GIT_CONFIG_GLOBAL: path.join(scratch, "empty-global"),
          GIT_ALLOW_PROTOCOL: "file",
          GIT_AUTHOR_NAME: "Fixture",
          GIT_AUTHOR_EMAIL: "fixture@example.invalid",
          GIT_COMMITTER_NAME: "Fixture",
          GIT_COMMITTER_EMAIL: "fixture@example.invalid",
        }),
      );
    }).pipe(Effect.provide(dependencies)),
);

it.effect("starts staggered checks for registered opt-ins without a status or refresh call", () =>
  Effect.gen(function* () {
    const first = ProjectId.make("a");
    const alias = ProjectId.make("b");
    const other = ProjectId.make("c");
    const off = ProjectId.make("d");
    const removed = ProjectId.make("e");
    const rows = [first, alias, other, off].map((id): OrchestrationProjectShell => ({
      id,
      title: id,
      workspaceRoot: id,
      defaultModelSelection: null,
      scripts: [],
      createdAt: "2026-01-01T00:00:00Z",
      updatedAt: "2026-01-01T00:00:00Z",
    }));
    const checked: Array<{ target: string; time: number }> = [];
    const layers = Layer.mergeAll(
      ServerSettings.ServerSettingsService.layerTest({
        projectSettingsOverrides: {
          [first]: { repoFreshnessCadenceSeconds: 120 },
          [alias]: { repoFreshnessCadenceSeconds: 300 },
          [other]: { repoFreshnessCadenceSeconds: 300 },
          [removed]: { repoFreshnessCadenceSeconds: 120 },
        },
      }),
      Layer.mock(ProjectStore.ProjectStoreV2)({ listShells: () => Effect.succeed(rows) }),
      Layer.mock(GitVcsDriver.GitVcsDriver)({
        resolveRepoFreshnessRemote: (cwd) => {
          const identity = cwd === alias ? first : cwd;
          return Effect.succeed({
            identity,
            remoteName: "origin",
            check: Clock.currentTimeMillis.pipe(
              Effect.map((time) => {
                checked.push({ target: identity, time });
                return { remoteName: "origin", branchName: "main", commitSha: "a".repeat(40) };
              }),
            ),
          });
        },
      }),
    );
    yield* Effect.gen(function* () {
      yield* TestClock.adjust("30 seconds");
      expect(checked.map(({ target }) => target)).toEqual([first, other]);
      expect(checked[1]!.time - checked[0]!.time).toBeGreaterThanOrEqual(5_000);

      yield* TestClock.adjust("120 seconds");
      expect(checked.map(({ target }) => target)).toEqual([first, other, first]);
    }).pipe(Effect.provide(RepoFreshness.layer.pipe(Layer.provide(layers))));
  }),
);
