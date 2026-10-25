// @effect-diagnostics nodeBuiltinImport:off -- Realpath and bigint inode/birth observations must refer to the same physical directory.
import * as NodeFSP from "node:fs/promises";
import * as Path from "effect/Path";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Git from "../../vcs/GitVcsDriver.ts";
import { CheckoutError } from "./CheckoutProtocol.ts";
import { CheckoutTargetObservation, type Directory } from "./CheckoutTarget.ts";

const make = Effect.gen(function* () {
  const path = yield* Path.Path;
  const git = yield* Git.GitVcsDriver;
  const directory = (input: string) =>
    Effect.tryPromise({
      try: async (): Promise<Directory> => {
        const realPath = await NodeFSP.realpath(input);
        const before = await NodeFSP.stat(realPath, { bigint: true });
        const again = await NodeFSP.realpath(input);
        const after = await NodeFSP.stat(again, { bigint: true });
        if (
          !before.isDirectory() ||
          again !== realPath ||
          before.dev !== after.dev ||
          before.ino !== after.ino ||
          before.birthtimeNs !== after.birthtimeNs ||
          before.ino === 0n
        )
          throw new Error("directory changed");
        return {
          realPath,
          identity: {
            version: 1,
            device: String(before.dev),
            inode: String(before.ino),
            birth: String(before.birthtimeNs),
          },
        };
      },
      catch: (cause) => new CheckoutError({ reason: "unavailable", cause }),
    });
  const read = (cwd: string, args: readonly string[]) =>
    git
      .execute({
        operation: "CheckoutTarget.observe",
        cwd,
        args,
        allowNonZeroExit: true,
        env: { GIT_OPTIONAL_LOCKS: "0", LC_ALL: "C" },
        maxOutputBytes: 256 * 1024,
      })
      .pipe(Effect.mapError((cause) => new CheckoutError({ reason: "unavailable", cause })));
  const checked = (cwd: string, args: readonly string[]) =>
    read(cwd, args).pipe(
      Effect.flatMap((result) =>
        result.exitCode === 0 && !result.stdoutTruncated && !result.stderrTruncated
          ? Effect.succeed(
              result.stdout.endsWith("\n") ? result.stdout.slice(0, -1) : result.stdout,
            )
          : Effect.fail(new CheckoutError({ reason: "unavailable" })),
      ),
    );
  const marker = (cwd: string) =>
    Effect.tryPromise({
      try: async () => {
        let current = cwd;
        for (;;) {
          const candidate = path.join(current, ".git");
          const stat = await NodeFSP.lstat(candidate).catch((cause: unknown) => {
            if (
              typeof cause === "object" &&
              cause !== null &&
              "code" in cause &&
              cause.code === "ENOENT"
            )
              return null;
            throw cause;
          });
          if (stat !== null) {
            if (!stat.isDirectory()) return true;
            // Git ignores empty .git directories: they contain no repository identity.
            // Only confirmed emptiness is harmless; unreadable or nonempty markers still refuse.
            const entries = await NodeFSP.opendir(candidate);
            try {
              if ((await entries.read()) !== null) return true;
            } finally {
              await entries.close();
            }
          }
          const parent = path.dirname(current);
          if (parent === current) return false;
          current = parent;
        }
      },
      catch: (cause) => new CheckoutError({ reason: "unavailable", cause }),
    });
  return CheckoutTargetObservation.of({
    directory,
    git: Effect.fn("CheckoutTargetNode.git")(function* (cwd) {
      const inside = yield* read(cwd, ["rev-parse", "--is-inside-work-tree"]);
      if (inside.exitCode !== 0) {
        if (
          inside.exitCode !== 128 ||
          inside.stdout !== "" ||
          inside.stdoutTruncated ||
          inside.stderrTruncated ||
          !inside.stderr.startsWith("fatal: not a git repository") ||
          (yield* marker(cwd))
        )
          return yield* new CheckoutError({ reason: "unavailable" });
        // A failed probe without a repository marker is a non-Git workspace; bare repositories refuse below.
        return null;
      }
      if (inside.stdout.trim() !== "true" || inside.stdoutTruncated || inside.stderrTruncated)
        return yield* new CheckoutError({ reason: "unavailable" });
      const root = yield* directory(yield* checked(cwd, ["rev-parse", "--show-toplevel"]));
      const common = yield* directory(
        yield* checked(cwd, ["rev-parse", "--path-format=absolute", "--git-common-dir"]),
      );
      const gitDir = yield* directory(yield* checked(cwd, ["rev-parse", "--absolute-git-dir"]));
      const symbolic = yield* read(cwd, ["symbolic-ref", "--quiet", "HEAD"]);
      if (
        (symbolic.exitCode !== 0 && symbolic.exitCode !== 1) ||
        symbolic.stdoutTruncated ||
        symbolic.stderrTruncated
      )
        return yield* new CheckoutError({ reason: "unavailable" });
      const registrations = yield* checked(cwd, ["worktree", "list", "--porcelain", "-z"]);
      const registeredWorktrees: string[] = [];
      for (const item of registrations.split("\0")) {
        if (!item.startsWith("worktree ")) continue;
        // Stale unrelated registrations need not exist; the selected root must have a physical match.
        const registered = item.slice(9);
        if (registered === root.realPath) registeredWorktrees.push(root.realPath);
        else registeredWorktrees.push(registered);
      }
      return {
        root: root.realPath,
        commonDirectory: common.realPath,
        gitDirectory: gitDir.realPath,
        commonIdentity: common.identity,
        directoryIdentity: gitDir.identity,
        branch: symbolic.exitCode === 1 ? null : symbolic.stdout.trim(),
        registeredWorktrees,
      };
    }),
  });
});
export const layer = Layer.effect(CheckoutTargetObservation, make);
