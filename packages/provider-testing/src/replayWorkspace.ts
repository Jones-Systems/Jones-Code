import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

class ReplayFixtureGitCommandError extends Schema.TaggedError<ReplayFixtureGitCommandError>()(
  "ReplayFixtureGitCommandError",
  {
    command: Schema.String,
    exitCode: Schema.Number,
    cwd: Schema.String,
    stderr: Schema.String,
    fixtureMetadata: Schema.Record(Schema.String, Schema.String),
  },
) {
  override get message(): string {
    return `${this.command} failed with exit ${this.exitCode} in ${this.cwd}.\n${this.stderr}\nFixture metadata: ${JSON.stringify(this.fixtureMetadata)}`;
  }
}

function runGit(
  cwd: string,
  args: ReadonlyArray<string>,
  fixtureRoot: string,
): Effect.Effect<
  void,
  ReplayFixtureGitCommandError | PlatformError.PlatformError,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> {
  return Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const child = yield* spawner.spawn(
      ChildProcess.make("git", args, {
        cwd,
        stdin: "ignore",
        stdout: "ignore",
        stderr: "pipe",
      }),
    );
    let stderr = "";
    const [exitCode] = yield* Effect.all(
      [
        child.exitCode,
        child.stderr.pipe(
          Stream.decodeText(),
          Stream.runForEach((chunk) =>
            Effect.sync(() => {
              stderr = (stderr + chunk).slice(-8192);
            }),
          ),
          Effect.catch(() => Effect.void),
        ),
      ],
      { concurrency: "unbounded" },
    );
    if (Number(exitCode) !== 0) {
      return yield* new ReplayFixtureGitCommandError({
        command: `git ${args.join(" ")}`,
        exitCode: Number(exitCode),
        cwd,
        stderr,
        fixtureMetadata: yield* fixtureMetadata(fixtureRoot),
      });
    }
  }).pipe(Effect.scoped);
}

// Read only Git-created metadata in this invocation's disposable fixture before cleanup.
const fixtureMetadata = (root: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const admin = path.join(root, ".fixture-primary", ".git", "worktrees");
    const names = yield* fs
      .readDirectory(admin)
      .pipe(Effect.catch(() => Effect.succeed([] as string[])));
    const paths = [
      path.join(root, ".git"),
      path.join(root, ".fixture-worktree", ".git"),
      ...names
        .slice(0, 8)
        .flatMap((name) => ["gitdir", "commondir"].map((file) => path.join(admin, name, file))),
    ];
    const entries = yield* Effect.forEach(paths, (file) =>
      Effect.gen(function* () {
        const canonical = yield* fs.realPath(file);
        const relative = path.relative(root, canonical);
        if (
          path.isAbsolute(relative) ||
          relative === ".." ||
          relative.startsWith(".." + path.sep)
        ) {
          return [path.relative(root, file), "<outside fixture>"] as const;
        }
        const text = yield* fs.readFileString(canonical);
        return [path.relative(root, file), text.slice(0, 4096)] as const;
      }).pipe(
        Effect.catch(() => Effect.succeed([path.relative(root, file), "<unavailable>"] as const)),
      ),
    );
    return Object.fromEntries(entries);
  }).pipe(Effect.catch(() => Effect.succeed({} as Record<string, string>)));

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
            yield* runGit(primary, ["init"], cwd);
            yield* runGit(primary, ["config", "user.name", "T3 Code Test"], cwd);
            yield* runGit(primary, ["config", "user.email", "t3code-test@example.com"], cwd);
            yield* fs.writeFileString(
              path.join(primary, ".git", "info", "exclude"),
              "/.fixture-primary/\n",
            );
            // Commits start detached auto-maintenance; since Git 2.54 it prunes the index-less
            // --no-checkout worktree below while its .git file is being relocated.
            yield* runGit(primary, ["config", "maintenance.auto", "false"], cwd);
            yield* fs.writeFileString(path.join(primary, "README.md"), `# ${fixtureName}\n`);
            for (const [relativePath, contents] of Object.entries(files)) {
              const filePath = path.join(primary, relativePath);
              yield* fs.makeDirectory(path.dirname(filePath), { recursive: true });
              yield* fs.writeFileString(filePath, contents);
            }
            yield* runGit(primary, ["add", "README.md", ...Object.keys(files)], cwd);
            yield* runGit(primary, ["commit", "-m", "initial"], cwd);
            // Git requires an empty worktree destination; repair its backlink after relocation.
            yield* runGit(primary, ["worktree", "add", "--detach", "--no-checkout", staging], cwd);
            yield* fs.rename(path.join(staging, ".git"), path.join(cwd, ".git"));
            yield* fs.remove(staging, { recursive: true });
            yield* runGit(primary, ["worktree", "repair", cwd], cwd);
            yield* runGit(cwd, ["checkout", "HEAD", "--", "."], cwd);
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
