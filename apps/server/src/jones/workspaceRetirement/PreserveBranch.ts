// @effect-diagnostics nodeBuiltinImport:off -- A private temporary index file per preserve build; every Git command runs through the owning server's native adapter.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import { RetirementError } from "./Receipts.ts";
import type { PreservePlan } from "./Recoverability.ts";

/**
 * The owning server's native Git adapter (`GitVcsDriver.execute`). Unlike `Filesystem.git`, it keeps
 * the user's Git configuration, so pushes reach the remote with its credentials, and it passes `env`
 * through, which the temporary index needs.
 */
export interface NativeGitDriver {
  readonly execute: (input: {
    readonly operation: string;
    readonly cwd: string;
    readonly args: readonly string[];
    readonly env?: Readonly<Record<string, string>>;
    readonly stdin?: string;
    readonly allowNonZeroExit: boolean;
    readonly timeoutMs: number;
    readonly maxOutputBytes?: number;
  }) => Effect.Effect<
    {
      readonly exitCode: number;
      readonly stdout: string;
      readonly stderr: string;
      readonly stdoutTruncated?: boolean;
    },
    { readonly message: string }
  >;
}

const SHORT_MS = 2 * 60 * 1000;
// Hashing a preserve set of up to 500 MiB, or a push over a slow link, can take minutes.
const LONG_MS = 15 * 60 * 1000;
const LISTING_BYTES = 4 * 1024 * 1024;
const MAX_SUFFIX = 100;

/** Runs one Git command; a failed spawn or a non-zero exit fails with `reason`, carrying Git's first stderr line. */
const run = Effect.fn("WorkspaceRetirement.preserve.git")(function* (
  driver: NativeGitDriver,
  reason: string,
  input: {
    readonly cwd: string;
    readonly args: readonly string[];
    readonly env?: Readonly<Record<string, string>>;
    readonly stdin?: string;
    readonly timeoutMs?: number;
    readonly allowNonZeroExit?: boolean;
  },
) {
  const result = yield* driver
    .execute({
      operation: "WorkspaceRetirement.preserve",
      cwd: input.cwd,
      args: input.args,
      ...(input.env ? { env: input.env } : {}),
      ...(input.stdin !== undefined ? { stdin: input.stdin } : {}),
      allowNonZeroExit: true,
      timeoutMs: input.timeoutMs ?? SHORT_MS,
      maxOutputBytes: LISTING_BYTES,
    })
    .pipe(
      Effect.mapError(
        (cause) => new RetirementError({ target: input.cwd, reason, cause: cause.message }),
      ),
    );
  if (result.stdoutTruncated)
    return yield* new RetirementError({ target: input.cwd, reason, cause: "output_truncated" });
  if (result.exitCode !== 0 && !input.allowNonZeroExit)
    return yield* new RetirementError({
      target: input.cwd,
      reason,
      cause: `git ${input.args[0]}: ${result.stderr.trim().split(/\r?\n/)[0] || `exit ${result.exitCode}`}`,
    });
  return result;
});

/** One lowercase ref component from free text: `[a-z0-9._-]`, never `.`-led, `..`, or ending in `.lock`. */
function refComponent(text: string, fallback: string) {
  const slug = text
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/\.{2,}/g, ".")
    .replace(/-{2,}/g, "-")
    .replace(/^[.-]+|[.-]+$/g, "")
    .slice(0, 100)
    .replace(/[.-]+$/, "")
    .replace(/\.lock$/, "-lock");
  return slug || fallback;
}

/** `preserve/<YYYYMMDD>/<host-slug>/<worktree-name>`, before any collision suffix. */
export const preserveBranchBase = (input: { at: string; host: string; worktree: string }) =>
  [
    "preserve",
    input.at.slice(0, 10).replaceAll("-", ""),
    refComponent(input.host.split(".")[0] ?? "", "host"),
    refComponent(NodePath.basename(input.worktree), "worktree"),
  ].join("/");

const shellWord = (word: string) =>
  /^[A-Za-z0-9_@%+=:,./-]+$/.test(word) ? word : `'${word.replaceAll("'", "'\\''")}'`;
/** Recreates the checkout at its old path from the preserve branch. */
export const restoreCommand = (projectRoot: string, worktree: string, branch: string) =>
  `git -C ${shellWord(projectRoot)} worktree add ${shellWord(worktree)} ${shellWord(branch)}`;

/**
 * The tree a preserve commit carries: HEAD with the selected paths added and the deletions removed,
 * built in a private index so the checkout's own index, branch and files are never written.
 */
export const preserveTree = Effect.fn("WorkspaceRetirement.preserve.tree")(function* (
  driver: NativeGitDriver,
  input: { readonly worktree: string; readonly head: string; readonly plan: PreservePlan },
) {
  const target = input.worktree;
  const directory = yield* Effect.tryPromise({
    try: () => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-preserve-")),
    catch: (cause) => new RetirementError({ target, reason: "preserve_commit_failed", cause }),
  });
  const index = NodePath.join(directory, "index");
  // Literal pathspecs: a file named `*.txt` must add that file, not every text file.
  const env = { GIT_INDEX_FILE: index, GIT_LITERAL_PATHSPECS: "1" };
  return yield* Effect.gen(function* () {
    // Refuse before anything writes an index: an adapter that dropped `env` would point Git at the
    // checkout's own index.
    const resolved = (yield* run(driver, "preserve_index_isolation_unverified", {
      cwd: target,
      args: ["rev-parse", "--git-path", "index"],
      env,
    })).stdout.trim();
    if (resolved !== index)
      return yield* new RetirementError({ target, reason: "preserve_index_isolation_unverified" });
    yield* run(driver, "preserve_commit_failed", {
      cwd: target,
      args: ["read-tree", input.head],
      env,
    });
    // An empty pathspec list means the whole tree to `git add -A`, so it runs only with a selection.
    if (input.plan.selected.length)
      yield* run(driver, "preserve_commit_failed", {
        cwd: target,
        args: ["add", "-A", "--pathspec-from-file=-", "--pathspec-file-nul"],
        env,
        stdin: input.plan.selected.join("\0"),
        timeoutMs: LONG_MS,
      });
    if (input.plan.deletions.length)
      yield* run(driver, "preserve_commit_failed", {
        cwd: target,
        args: ["update-index", "--force-remove", "-z", "--stdin"],
        env,
        stdin: input.plan.deletions.map((path) => path + "\0").join(""),
      });
    return (yield* run(driver, "preserve_commit_failed", {
      cwd: target,
      args: ["write-tree"],
      env,
    })).stdout.trim();
  }).pipe(
    Effect.ensuring(
      Effect.promise(() => NodeFSP.rm(directory, { recursive: true, force: true }).catch(() => {})),
    ),
  );
});

/** Commits the preserve tree on top of the qualified HEAD; nothing references it until a ref is created. */
export const preserveCommit = Effect.fn("WorkspaceRetirement.preserve.commit")(function* (
  driver: NativeGitDriver,
  input: {
    readonly worktree: string;
    readonly head: string;
    readonly plan: PreservePlan;
    readonly message: string;
  },
) {
  const tree = yield* preserveTree(driver, input);
  // A signing prompt would hang a server-side snapshot; the user's signing setting is not applied here.
  const sha = (yield* run(driver, "preserve_commit_failed", {
    cwd: input.worktree,
    args: ["commit-tree", "--no-gpg-sign", "-p", input.head, "-m", input.message, tree],
  })).stdout.trim();
  return { sha, tree };
});

/** The branch's upstream remote, else `origin`; null when the repository names no such remote. */
export const preserveRemote = Effect.fn("WorkspaceRetirement.preserve.remote")(function* (
  driver: NativeGitDriver,
  input: { readonly cwd: string; readonly branch: string | null },
) {
  const upstream = input.branch?.startsWith("refs/heads/")
    ? (yield* run(driver, "preserve_remote_unavailable", {
        cwd: input.cwd,
        args: ["config", "--get", `branch.${input.branch.slice("refs/heads/".length)}.remote`],
        allowNonZeroExit: true,
      })).stdout.trim()
    : "";
  const name = upstream && upstream !== "." ? upstream : "origin";
  const remotes = (yield* run(driver, "preserve_remote_unavailable", {
    cwd: input.cwd,
    args: ["remote"],
  })).stdout
    .split(/\r?\n/)
    .filter(Boolean);
  // An upstream can be recorded as a URL or path rather than a configured remote name.
  if (remotes.includes(name) || (name === upstream && /[:/]/.test(name))) return name;
  return null;
});

const refNames = (listing: string, field: number) =>
  listing
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => line.split(/\s+/)[field] ?? "");

/** Exact remote value of one branch, or null when the remote lists no such branch. */
export const remoteBranch = Effect.fn("WorkspaceRetirement.preserve.remoteBranch")(function* (
  driver: NativeGitDriver,
  input: { readonly cwd: string; readonly remote: string; readonly branch: string },
) {
  const ref = `refs/heads/${input.branch}`;
  const listing = (yield* run(driver, "preserve_remote_unavailable", {
    cwd: input.cwd,
    args: ["ls-remote", "--refs", input.remote, ref],
    timeoutMs: SHORT_MS,
  })).stdout;
  for (const line of listing.split(/\r?\n/)) {
    const [sha, name] = line.split(/\s+/);
    if (name === ref) return sha ?? null;
  }
  return null;
});

/**
 * The first free name among `<base>`, `<base>-2`, `<base>-3`, ... Both the local refs and the
 * remote's branches are checked, and a name with refs beneath it is taken too. When the remote
 * cannot be listed, `remoteListed` is false and nothing may be pushed under the chosen name.
 */
export const preserveBranchName = Effect.fn("WorkspaceRetirement.preserve.branchName")(function* (
  driver: NativeGitDriver,
  input: { readonly cwd: string; readonly remote: string | null; readonly base: string },
) {
  const patterns = [`refs/heads/${input.base}`, `refs/heads/${input.base}-*`];
  const taken = refNames(
    (yield* run(driver, "preserve_branch_unavailable", {
      cwd: input.cwd,
      args: ["for-each-ref", "--format=%(refname)", ...patterns],
    })).stdout,
    0,
  );
  let remoteListed = input.remote !== null;
  if (input.remote !== null) {
    const listed = yield* run(driver, "preserve_remote_unavailable", {
      cwd: input.cwd,
      args: ["ls-remote", "--refs", input.remote, ...patterns],
      timeoutMs: SHORT_MS,
    }).pipe(Effect.result);
    if (listed._tag === "Success") taken.push(...refNames(listed.success.stdout, 1));
    else remoteListed = false;
  }
  const free = (name: string) =>
    !taken.some((ref) => ref === `refs/heads/${name}` || ref.startsWith(`refs/heads/${name}/`));
  for (let n = 1; n <= MAX_SUFFIX; n++) {
    const branch = n === 1 ? input.base : `${input.base}-${n}`;
    if (free(branch)) return { branch, remoteListed };
  }
  return yield* new RetirementError({ target: input.cwd, reason: "preserve_branch_unavailable" });
});

/** Creates `refs/heads/<branch>` only if no such ref exists; an existing ref is never moved. */
export const createLocalBranch = Effect.fn("WorkspaceRetirement.preserve.createLocalBranch")(
  function* (
    driver: NativeGitDriver,
    input: { readonly cwd: string; readonly branch: string; readonly sha: string },
  ) {
    yield* run(driver, "preserve_branch_unavailable", {
      cwd: input.cwd,
      args: ["update-ref", `refs/heads/${input.branch}`, input.sha, "0".repeat(input.sha.length)],
    });
  },
);

/** Pushes without force. Any refusal or transport failure leaves the outcome to `remoteBranch` readback. */
export const pushPreserveBranch = Effect.fn("WorkspaceRetirement.preserve.push")(function* (
  driver: NativeGitDriver,
  input: {
    readonly cwd: string;
    readonly remote: string;
    readonly branch: string;
    readonly sha: string;
  },
) {
  const pushed = yield* run(driver, "preserve_push_refused", {
    cwd: input.cwd,
    args: ["push", "--porcelain", input.remote, `${input.sha}:refs/heads/${input.branch}`],
    // A credential prompt cannot be answered from the server.
    env: { GIT_TERMINAL_PROMPT: "0" },
    timeoutMs: LONG_MS,
  }).pipe(Effect.result);
  return pushed._tag === "Success";
});

/** The local branch still names the preserve commit. */
export const localBranchAt = Effect.fn("WorkspaceRetirement.preserve.localBranchAt")(function* (
  driver: NativeGitDriver,
  input: { readonly cwd: string; readonly branch: string; readonly sha: string },
) {
  const found = yield* run(driver, "preserve_readback_failed", {
    cwd: input.cwd,
    args: ["rev-parse", "--verify", "--quiet", `refs/heads/${input.branch}^{commit}`],
    allowNonZeroExit: true,
  });
  return found.exitCode === 0 && found.stdout.trim() === input.sha;
});

/** The tree of the preserve commit, read from the object store rather than from the receipt. */
export const commitTree = Effect.fn("WorkspaceRetirement.preserve.commitTree")(function* (
  driver: NativeGitDriver,
  input: { readonly cwd: string; readonly sha: string },
) {
  return (yield* run(driver, "preserve_tree_mismatch", {
    cwd: input.cwd,
    args: ["rev-parse", "--verify", `${input.sha}^{tree}`],
  })).stdout.trim();
});
