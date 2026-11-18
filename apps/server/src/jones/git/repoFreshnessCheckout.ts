// @effect-diagnostics nodeBuiltinImport:off -- Compare physical checkout and index identities with nanosecond timestamps.
import * as NodeFSP from "node:fs/promises";

import * as Effect from "effect/Effect";
import type * as Path from "effect/Path";

import type { GitVcsDriver } from "../../vcs/GitVcsDriver.ts";
import type { VerifiedRepoDefault } from "./repoFreshnessGit.ts";

export type RepoFreshnessCheckoutSkipReason =
  | "target-unavailable"
  | "target-changed"
  | "verification-failed"
  | "auto-pull-disabled"
  | "cadence-disabled"
  | "not-checkout-root"
  | "wrong-branch"
  | "upstream-mismatch"
  | "checkout-policy"
  | "checkout-busy"
  | "dirty-checkout"
  | "index-unverified"
  | "local-commits"
  | "checkout-changed"
  | "update-unverified";

export type RepoFreshnessCheckout =
  | { readonly status: "updated" }
  | { readonly status: "already-current" }
  | { readonly status: "skipped"; readonly reason: RepoFreshnessCheckoutSkipReason };

export const skipRepoFreshnessCheckout = (
  reason: RepoFreshnessCheckoutSkipReason,
): RepoFreshnessCheckout => ({ status: "skipped", reason });

type Location = { readonly cwd: string; readonly gitDir: string; readonly gitCommonDir: string };

const missing = (cause: unknown) =>
  typeof cause === "object" && cause !== null && "code" in cause && cause.code === "ENOENT";
const stat = (file: string) =>
  NodeFSP.lstat(file, { bigint: true }).catch((cause: unknown) => {
    if (missing(cause)) return null;
    throw cause;
  });
const identity = (value: NonNullable<Awaited<ReturnType<typeof stat>>>) =>
  `${value.dev}:${value.ino}:${value.birthtimeNs}`;
const stamp = (value: Awaited<ReturnType<typeof stat>>) =>
  value === null ? null : `${identity(value)}:${value.size}:${value.mtimeNs}:${value.ctimeNs}`;

export const captureRepoFreshnessCheckout = Effect.fn("GitVcsDriver.captureRepoFreshnessCheckout")(
  function* (workspaceRoot: string, location: Location, path: Path.Path) {
    return yield* Effect.tryPromise(async () => {
      const { cwd, gitDir, gitCommonDir } = location;
      if (!path.isAbsolute(workspaceRoot) || (await NodeFSP.realpath(workspaceRoot)) !== cwd)
        return null;
      const marker = path.join(cwd, ".git");
      const [root, dotGit, admin, common] = await Promise.all(
        [cwd, marker, gitDir, gitCommonDir].map(stat),
      );
      if (
        !root?.isDirectory() ||
        !admin?.isDirectory() ||
        !common?.isDirectory() ||
        (!dotGit?.isFile() && !dotGit?.isDirectory()) ||
        [root, dotGit, admin, common].some(({ ino }) => ino === 0n)
      ) {
        return null;
      }
      return [
        cwd,
        identity(root),
        dotGit.isFile() ? stamp(dotGit) : identity(dotGit),
        gitDir,
        identity(admin),
        gitCommonDir,
        identity(common),
      ].join("\0");
    }).pipe(Effect.orElseSucceed(() => null));
  },
);

const indexState = (location: Location, path: Path.Path) =>
  Effect.tryPromise(async () => {
    const { gitDir, gitCommonDir } = location;
    const busy = [
      ...[
        "index.lock",
        "HEAD.lock",
        "MERGE_HEAD",
        "MERGE_AUTOSTASH",
        "CHERRY_PICK_HEAD",
        "REVERT_HEAD",
        "REBASE_HEAD",
        "BISECT_START",
        "sequencer",
        "rebase-apply",
        "rebase-merge",
      ].map((name) => path.join(gitDir, name)),
      ...["config.lock", "packed-refs.lock", "info/grafts"].map((name) =>
        path.join(gitCommonDir, name),
      ),
    ];
    const files = [
      path.join(gitDir, "index"),
      path.join(gitDir, "HEAD"),
      path.join(gitDir, "config.worktree"),
      path.join(gitCommonDir, "config"),
      path.join(gitCommonDir, "shallow"),
    ];
    const [markers, state] = await Promise.all([
      Promise.all(busy.map(stat)),
      Promise.all(files.map(stat)),
    ]);
    if (markers.some((value) => value !== null)) return { reason: "checkout-busy" } as const;
    if (state.some((value) => value !== null && (!value.isFile() || value.ino === 0n)))
      return { reason: "index-unverified" } as const;
    return { fingerprint: state.map(stamp).join("\0") };
  }).pipe(Effect.orElseSucceed(() => ({ reason: "index-unverified" }) as const));

const safeReadConfig = [
  "--no-replace-objects",
  "-c",
  "core.fsmonitor=false",
  "-c",
  "core.untrackedCache=false",
  "-c",
  "core.ignoreStat=false",
  "-c",
  "core.trustctime=true",
  "-c",
  "core.checkStat=default",
];

const hasCheckoutHooks = (directory: string, path: Path.Path) =>
  Effect.tryPromise(async () => {
    const hooks = ["reference-transaction", "post-index-change", "post-merge"];
    const entries = await Promise.all(hooks.map((hook) => stat(path.join(directory, hook))));
    return entries.some((entry) => entry !== null);
  }).pipe(Effect.orElseSucceed(() => true));

export const updateRepoFreshnessCheckout = Effect.fn("GitVcsDriver.updateRepoFreshnessCheckout")(
  function* (input: {
    readonly location: Location;
    readonly path: Path.Path;
    readonly execute: GitVcsDriver["Service"]["execute"];
    readonly verified: VerifiedRepoDefault;
    readonly revalidate: Effect.Effect<boolean>;
    readonly verifyPhysical: Effect.Effect<boolean>;
    readonly authorize: Effect.Effect<RepoFreshnessCheckoutSkipReason | null>;
    readonly invalidate: Effect.Effect<void>;
    readonly platform: NodeJS.Platform;
  }): Effect.fn.Return<RepoFreshnessCheckout> {
    const { location, path, execute, verified } = input;
    const { cwd, gitDir, gitCommonDir } = location;
    const operation = "GitVcsDriver.updateRepoFreshnessCheckout";
    const { branchName, commitSha, remoteName } = verified;
    const ref = `refs/heads/${branchName}`;
    const offlineEnv = { LC_ALL: "C", GIT_NO_LAZY_FETCH: "1", GIT_ALLOW_PROTOCOL: "" };
    const selectors = { GIT_DIR: gitDir, GIT_COMMON_DIR: gitCommonDir, GIT_WORK_TREE: cwd };
    const read = (args: readonly string[], discovered = false, maxOutputBytes = 256 * 1024) =>
      execute({
        operation,
        cwd,
        args: [...safeReadConfig, ...args],
        env: discovered ? offlineEnv : { ...offlineEnv, ...selectors },
        allowNonZeroExit: true,
        maxOutputBytes,
        timeoutMs: 10_000,
      }).pipe(Effect.option);
    const output = (result: Effect.Success<ReturnType<typeof read>>) =>
      result._tag === "Some" &&
      result.value.exitCode === 0 &&
      !result.value.stdoutTruncated &&
      !result.value.stderrTruncated
        ? result.value.stdout
        : null;

    if (!(yield* input.revalidate)) return skipRepoFreshnessCheckout("checkout-changed");
    // Discover from cwd before pinning the mutation to the captured Git directories and tree.
    const paths = output(
      yield* read(
        [
          "rev-parse",
          "--path-format=absolute",
          "--absolute-git-dir",
          "--git-common-dir",
          "--show-toplevel",
        ],
        true,
      ),
    )?.split("\n");
    const samePath = (expected: string, reported: string | undefined) =>
      reported !== undefined &&
      path.isAbsolute(reported) &&
      path.relative(expected, reported) === "";
    if (
      paths?.length !== 4 ||
      paths[3] !== "" ||
      !samePath(gitDir, paths[0]) ||
      !samePath(gitCommonDir, paths[1]) ||
      !samePath(cwd, paths[2])
    )
      return skipRepoFreshnessCheckout("not-checkout-root");

    const worktrees = output(yield* read(["worktree", "list", "--porcelain", "-z"]));
    if (!worktrees?.endsWith("\0\0")) return skipRepoFreshnessCheckout("checkout-changed");
    const registered = worktrees
      .slice(0, -2)
      .split("\0\0")
      .map((record) => record.split("\0"))
      .filter((fields) => fields.includes(`branch ${ref}`));
    if (registered.length === 0) return skipRepoFreshnessCheckout("wrong-branch");
    const [record] = registered;
    const registeredPath = record?.find((field) => field.startsWith("worktree "))?.slice(9);
    if (
      registered.length !== 1 ||
      !samePath(cwd, registeredPath) ||
      record?.some((field) => field.startsWith("prunable"))
    )
      return skipRepoFreshnessCheckout("wrong-branch");
    const head = record?.find((field) => field.startsWith("HEAD "))?.slice(5);
    if (head === undefined || !/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(head))
      return skipRepoFreshnessCheckout("checkout-changed");

    const before = yield* indexState(location, path);
    if ("reason" in before) return skipRepoFreshnessCheckout(before.reason);

    const config = yield* read([
      "config",
      "--null",
      "--get-regexp",
      "^(branch\\..*\\.(remote|merge|mergeoptions)|core\\.(hookspath|sparsecheckout)|extensions\\.refstorage)$",
    ]);
    const configText = output(config);
    if (configText === null || !configText.endsWith("\0"))
      return skipRepoFreshnessCheckout("upstream-mismatch");
    const entries = configText.slice(0, -1).split("\0");
    const values = (key: string) =>
      entries.flatMap((entry) =>
        entry === key ? [""] : entry.startsWith(`${key}\n`) ? [entry.slice(key.length + 1)] : [],
      );
    const branchConfig = `branch.${branchName}`;
    const [branchRemote, ...otherRemotes] = values(`${branchConfig}.remote`);
    const [branchMerge, ...otherMerges] = values(`${branchConfig}.merge`);
    if (
      branchRemote !== remoteName ||
      branchMerge !== ref ||
      otherRemotes.length ||
      otherMerges.length
    )
      return skipRepoFreshnessCheckout("upstream-mismatch");
    if (values("core.sparsecheckout").some((value) => !/^(?:false|no|off|0)$/i.test(value)))
      return skipRepoFreshnessCheckout("index-unverified");
    const hooksPath = values("core.hookspath").at(-1);
    const nullDevice = input.platform === "win32" ? "NUL" : "/dev/null";
    const hooksDir =
      hooksPath === undefined ? path.join(gitCommonDir, "hooks") : path.resolve(cwd, hooksPath);
    if (values("extensions.refstorage").includes("reftable")) {
      // Reftable stores these operation refs outside the loose Git directory.
      for (const name of ["CHERRY_PICK_HEAD", "REVERT_HEAD"]) {
        const result = yield* read(["show-ref", "--exists", name]);
        if (result._tag !== "Some" || result.value.exitCode !== 2)
          return skipRepoFreshnessCheckout("checkout-busy");
      }
    }

    const upstream = output(
      yield* read([
        "for-each-ref",
        "--format=%(refname)%00%(symref)%00%(upstream:remotename)%00%(upstream:remoteref)%00%(upstream)",
        ref,
      ]),
    );
    if (
      !upstream?.startsWith(`${ref}\0\0${remoteName}\0${ref}\0refs/`) ||
      upstream.slice(0, -1).includes("\n") ||
      upstream.endsWith(`\0${ref}\n`)
    )
      return skipRepoFreshnessCheckout("upstream-mismatch");

    if (head !== commitSha) {
      const ancestor = yield* read(["merge-base", "--is-ancestor", head, commitSha]);
      if (ancestor._tag === "Some" && ancestor.value.exitCode === 1)
        return skipRepoFreshnessCheckout("local-commits");
      if (output(ancestor) !== "") return skipRepoFreshnessCheckout("checkout-changed");
    }

    const flags = output(yield* read(["ls-files", "-v", "-z"], false, 8 * 1024 * 1024));
    if (
      flags === null ||
      (flags !== "" &&
        (!flags.endsWith("\0") ||
          flags
            .slice(0, -1)
            .split("\0")
            .some((row) => !row.startsWith("H "))))
    )
      return skipRepoFreshnessCheckout("index-unverified");

    // Keep the worktree scan last. Status's explicit options ignore user settings that hide dirt.
    const status = output(
      yield* read([
        "status",
        "--porcelain=v2",
        "--branch",
        "--no-ahead-behind",
        "--untracked-files=all",
        "--ignore-submodules=none",
        "-z",
      ]),
    );
    const [oid, branch, ...changes] = status?.slice(0, -1).split("\0") ?? [];
    if (
      !status?.endsWith("\0") ||
      oid !== `# branch.oid ${head}` ||
      branch !== `# branch.head ${branchName}`
    )
      return skipRepoFreshnessCheckout("checkout-changed");
    if (
      changes.some(
        (row) => !row.startsWith("# branch.upstream ") && !row.startsWith("# branch.ab "),
      )
    )
      return skipRepoFreshnessCheckout("dirty-checkout");
    const permission = yield* input.authorize;
    if (permission !== null) return skipRepoFreshnessCheckout(permission);
    const after = yield* indexState(location, path);
    if ("reason" in after) return skipRepoFreshnessCheckout(after.reason);
    if (after.fingerprint !== before.fingerprint || !(yield* input.verifyPhysical))
      return skipRepoFreshnessCheckout("checkout-changed");
    if (head === commitSha) return { status: "already-current" };
    // Custom checkout policy stays under the user's control.
    if (
      values(`${branchConfig}.mergeoptions`).some((value) => value.trim() !== "") ||
      (hooksPath !== nullDevice && (yield* hasCheckoutHooks(hooksDir, path)))
    )
      return skipRepoFreshnessCheckout("checkout-policy");

    const merge = yield* execute({
      operation,
      cwd,
      args: [
        ...safeReadConfig,
        "-c",
        `core.hooksPath=${nullDevice}`,
        "-c",
        "submodule.recurse=false",
        "-c",
        "maintenance.auto=false",
        "merge",
        "--ff-only",
        "--no-autostash",
        "--no-overwrite-ignore",
        "--no-stat",
        "--",
        commitSha,
      ],
      env: { ...offlineEnv, ...selectors },
      // A timeout or caller disconnect must not interrupt Git while it writes the checkout.
      timeoutMs: null,
      allowNonZeroExit: true,
    }).pipe(Effect.option, Effect.ensuring(input.invalidate), Effect.uninterruptible);
    if (merge._tag !== "Some" || merge.value.exitCode !== 0)
      return skipRepoFreshnessCheckout("update-unverified");
    const afterHead = output(yield* read(["rev-parse", "--verify", ref]));
    const afterBranch = output(yield* read(["symbolic-ref", "--quiet", "--no-recurse", "HEAD"]));
    return afterHead === `${commitSha}\n` &&
      afterBranch === `${ref}\n` &&
      (yield* input.verifyPhysical)
      ? { status: "updated" }
      : skipRepoFreshnessCheckout("update-unverified");
  },
);
