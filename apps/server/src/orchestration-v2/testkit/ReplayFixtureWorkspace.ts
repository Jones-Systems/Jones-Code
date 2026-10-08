import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

class ReplayFixtureGitCommandError extends Schema.TaggedError<ReplayFixtureGitCommandError>()(
  "ReplayFixtureGitCommandError",
  {
    command: Schema.String,
    exitCode: Schema.Number,
  },
) {
  override get message(): string {
    return `${this.command} failed with exit ${this.exitCode}.`;
  }
}

function runGit(
  cwd: string,
  args: ReadonlyArray<string>,
): Effect.Effect<
  void,
  ReplayFixtureGitCommandError | PlatformError.PlatformError,
  ChildProcessSpawner.ChildProcessSpawner
> {
  return Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const exitCode = yield* spawner.exitCode(ChildProcess.make("git", args, { cwd }));
    if (Number(exitCode) !== 0) {
      return yield* new ReplayFixtureGitCommandError({
        command: `git ${args.join(" ")}`,
        exitCode: Number(exitCode),
      });
    }
  });
}

/** Workspace-relative path to file contents, committed with the initial README. */
export type ReplayWorkspaceFiles = Readonly<Record<string, string>>;

const makeCheckpointWorkspaceEffect = Effect.fn("makeCheckpointWorkspace")(
  (fixtureName: string, files: ReplayWorkspaceFiles = {}) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const cwd = yield* fs.makeTempDirectory({
          prefix: `t3-orchestrator-v2-${fixtureName}-`,
        });
        return yield* restore(
          Effect.gen(function* () {
            // Keep the primary repository inside the returned root: async callers remove only cwd.
            const primary = path.join(cwd, ".fixture-primary");
            const staging = path.join(cwd, ".fixture-worktree");
            yield* fs.makeDirectory(primary);
            yield* runGit(primary, ["init"]);
            yield* runGit(primary, ["config", "user.name", "T3 Code Test"]);
            yield* runGit(primary, ["config", "user.email", "t3code-test@example.com"]);
            yield* fs.writeFileString(
              path.join(primary, ".git", "info", "exclude"),
              "/.fixture-primary/\n",
            );
            yield* fs.writeFileString(path.join(primary, "README.md"), `# ${fixtureName}\n`);
            for (const [relativePath, contents] of Object.entries(files)) {
              const filePath = path.join(primary, relativePath);
              yield* fs.makeDirectory(path.dirname(filePath), { recursive: true });
              yield* fs.writeFileString(filePath, contents);
            }
            yield* runGit(primary, ["add", "README.md", ...Object.keys(files)]);
            yield* runGit(primary, ["commit", "-m", "initial"]);
            // Git requires an empty worktree destination; repair its backlink after relocation.
            yield* runGit(primary, ["worktree", "add", "--detach", "--no-checkout", staging]);
            yield* fs.rename(path.join(staging, ".git"), path.join(cwd, ".git"));
            yield* fs.remove(staging, { recursive: true });
            yield* runGit(primary, ["worktree", "repair", cwd]);
            yield* runGit(cwd, ["checkout", "HEAD", "--", "."]);
            return cwd;
          }),
        ).pipe(
          Effect.onError(() => fs.remove(cwd, { recursive: true, force: true }).pipe(Effect.orDie)),
        );
      }),
    ),
);

const removeCheckpointWorkspaceEffect = Effect.fn("removeCheckpointWorkspace")(function* (
  cwd: string,
) {
  const fs = yield* FileSystem.FileSystem;
  yield* fs.remove(cwd, { recursive: true });
});

export const checkpointWorkspace = (fixtureName: string, files?: ReplayWorkspaceFiles) =>
  Effect.acquireRelease(makeCheckpointWorkspaceEffect(fixtureName, files), (cwd) =>
    removeCheckpointWorkspaceEffect(cwd).pipe(Effect.orDie),
  ).pipe(Effect.provide(NodeServices.layer));

export async function makeCheckpointWorkspace(
  fixtureName: string,
  files?: ReplayWorkspaceFiles,
): Promise<string> {
  return await Effect.runPromise(
    makeCheckpointWorkspaceEffect(fixtureName, files).pipe(Effect.provide(NodeServices.layer)),
  );
}
