import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { vi } from "vite-plus/test";
import { checkpointWorkspace } from "./replayWorkspace.ts";

const git = (cwd: string, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    return yield* spawner.string(ChildProcess.make("git", args, { cwd }));
  });

const repositoryState = (repo: string, worktree: string, worktreeIndex: string) =>
  Effect.gen(function* () {
    return {
      config: yield* git(repo, ["config", "--local", "--list"]),
      refs: yield* git(repo, ["for-each-ref"]),
      worktrees: yield* git(repo, ["worktree", "list", "--porcelain"]),
      worktreeHead: yield* git(worktree, ["rev-parse", "HEAD"]),
      worktreeIndex: yield* git(repo, ["hash-object", "--no-filters", worktreeIndex]),
    };
  });

it.layer(NodeServices.layer)("checkpointWorkspace", (it) => {
  // Linked-worktree Git commands can inherit absolute repository selectors from their caller.
  it.effect.each([
    { runner: "git bisect run", selectors: ["GIT_DIR"] },
    { runner: "a git hook", selectors: ["GIT_DIR", "GIT_INDEX_FILE"] },
  ])("keeps to its own repository when run under $runner", ({ selectors }) =>
    Effect.gen(function* () {
      yield* Effect.addFinalizer(() => Effect.sync(() => vi.unstubAllEnvs()));
      vi.stubEnv("GIT_CONFIG_COUNT", "1");
      vi.stubEnv("GIT_CONFIG_KEY_0", "commit.gpgsign");
      vi.stubEnv("GIT_CONFIG_VALUE_0", "false");
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-replay-workspace-decoy-" });
      const repo = path.join(root, "repo");
      const worktree = path.join(root, "worktree");
      yield* fs.makeDirectory(repo);
      yield* git(repo, ["init"]);
      yield* git(repo, [
        "-c",
        "user.name=Decoy",
        "-c",
        "user.email=decoy@example.com",
        "commit",
        "--allow-empty",
        "-m",
        "decoy",
      ]);
      yield* git(repo, ["worktree", "add", "--detach", worktree]);
      const worktreeGitDir = (yield* git(worktree, ["rev-parse", "--absolute-git-dir"])).trim();
      const worktreeIndex = path.join(worktreeGitDir, "index");
      const before = yield* repositoryState(repo, worktree, worktreeIndex);

      const inherited: Record<string, string> = {
        GIT_DIR: worktreeGitDir,
        GIT_INDEX_FILE: worktreeIndex,
      };
      for (const name of selectors) vi.stubEnv(name, inherited[name]);
      const workspace = yield* checkpointWorkspace("decoy-guard", { "src/a.txt": "a\n" });
      vi.unstubAllEnvs();

      assert.deepStrictEqual(yield* repositoryState(repo, worktree, worktreeIndex), before);
      assert.strictEqual(
        yield* git(workspace, ["log", "--format=%s%x00%an", "-1"]),
        "initial\u0000T3 Code Test\n",
      );
      assert.strictEqual(
        (yield* git(workspace, ["ls-files"])).trim().split("\n").join(","),
        "README.md,src/a.txt",
      );
    }),
  );
});

it.live.each(["init", "repair"] as const)(
  "preserves bounded stderr and owned metadata before cleanup on %s failure",
  (stage) =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const bin = yield* fs.makeTempDirectory({ prefix: "t3-replay-git-diagnostics-" });
        const previousPath = process.env.PATH;
        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            if (previousPath === undefined) delete process.env.PATH;
            else process.env.PATH = previousPath;
            yield* fs.remove(bin, { recursive: true, force: true }).pipe(Effect.orDie);
          }),
        );
        const git = path.join(bin, "git");
        yield* fs.writeFileString(
          git,
          `#!/bin/sh
should_fail() {
  while [ "$1" = "-c" ]; do shift 2; done
  [ "$1" = "${stage === "init" ? "init" : "worktree"}" ]${stage === "repair" ? ' && [ "$2" = "repair" ]' : ""}
}
if should_fail "$@"; then
  printf '%s' '${"x".repeat(16384)}' >&2
  printf '%s' 'synthetic Git failure' >&2
  exit 73
fi
exec /usr/bin/git "$@"
`,
        );
        yield* fs.chmod(git, 0o700);
        process.env.PATH = bin + ":" + previousPath;
        const result = yield* Effect.scoped(checkpointWorkspace("diagnostic-failure")).pipe(
          Effect.catchTags({ ReplayFixtureGitCommandError: (error) => Effect.succeed(error) }),
        );
        assert.notEqual(typeof result, "string");
        if (typeof result === "string") return yield* Effect.die("Expected synthetic Git failure");
        assert.equal(result.exitCode, 73);
        assert.equal(result.stderr.length, 8192);
        assert.isTrue(result.stderr.endsWith("synthetic Git failure"));
        assert.include(result.command, stage);
        assert.include(result.message, result.cwd);
        const root = path.dirname(result.cwd);
        assert.isFalse(yield* fs.exists(root), "failed acquisition removes its exact owned root");
        if (stage === "repair") {
          assert.include(
            result.fixtureMetadata[".git"],
            root + "/.fixture-primary/.git/worktrees/",
          );
          assert.isTrue(
            Object.entries(result.fixtureMetadata).some(
              ([name, value]) =>
                name.endsWith("/gitdir") && value.includes("/.fixture-worktree/.git"),
            ),
          );
          assert.isTrue(
            Object.entries(result.fixtureMetadata).some(
              ([name, value]) => name.endsWith("/commondir") && value.trim() === "../..",
            ),
          );
        } else {
          assert.isTrue(
            Object.values(result.fixtureMetadata).every((value) => value === "<unavailable>"),
          );
        }
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
