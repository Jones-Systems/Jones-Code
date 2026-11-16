import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { checkpointWorkspace } from "./replayWorkspace.ts";

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
if [ "$1" = "${stage === "init" ? "init" : "worktree"}" ]${stage === "repair" ? ' && [ "$2" = "repair" ]' : ""}; then
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
