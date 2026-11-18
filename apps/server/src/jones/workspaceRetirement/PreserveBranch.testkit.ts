// @effect-diagnostics nodeBuiltinImport:off -- Disposable repositories and a local bare remote exercise the preserve protocol through real Git processes.
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { CONSUMER_LIMITS } from "./ConsumerProbe.ts";
import { command, fixture } from "./Conformance.testkit.ts";
import { RetirementReceiptJson } from "./DurableCodecs.ts";
import { exists } from "./Filesystem.ts";
import { incarnation } from "./OutputAttestation.ts";
import { preserveBranchBase, restoreCommand, type NativeGitDriver } from "./PreserveBranch.ts";
import { qualify, type QualificationInput } from "./Qualification.ts";
import {
  layer,
  Receipts,
  RetirementError,
  type RetirementPreserve,
  type RetirementReceipt,
} from "./Receipts.ts";
import { acquireReservation, holderIdentity, readReservation } from "./Reservation.ts";
import { retire, type RetirementRequest } from "./RetirementExecutor.ts";
import { fixtureSql, receiptRow, type EvaluateRetirement } from "./Retirement.testkit.ts";

/** Git configuration and identity for fixture commands; nothing reads the user's configuration. */
export const HERMETIC_GIT_ENV = {
  GIT_CONFIG_NOSYSTEM: "1",
  GIT_CONFIG_GLOBAL: "/dev/null",
  GIT_AUTHOR_NAME: "Fixture",
  GIT_AUTHOR_EMAIL: "fixture@example.invalid",
  GIT_COMMITTER_NAME: "Fixture",
  GIT_COMMITTER_EMAIL: "fixture@example.invalid",
} as const;

type GitInput = Parameters<NativeGitDriver["execute"]>[0];
/**
 * A Git adapter shaped like `GitVcsDriver.execute`: `env` merges over a hermetic environment and
 * every call's arguments are recorded. `transform` rewrites each call before it runs.
 */
function fixtureGit(home: string, transform: (input: GitInput) => GitInput = (x) => x) {
  const calls: string[][] = [];
  const driver: NativeGitDriver = {
    execute: (original) =>
      Effect.tryPromise({
        try: () => {
          const input = transform(original);
          calls.push([...input.args]);
          return new Promise<{ exitCode: number; stdout: string; stderr: string }>(
            (resolve, reject) => {
              const child = NodeChildProcess.execFile(
                "git",
                [...input.args],
                {
                  cwd: input.cwd,
                  encoding: "utf8",
                  timeout: Math.min(input.timeoutMs, 60_000),
                  maxBuffer: input.maxOutputBytes ?? 1024 * 1024,
                  env: { PATH: process.env.PATH, HOME: home, ...HERMETIC_GIT_ENV, ...input.env },
                },
                (error, stdout, stderr) =>
                  error && typeof error.code !== "number"
                    ? reject(error)
                    : resolve({ exitCode: error ? Number(error.code) : 0, stdout, stderr }),
              );
              child.stdin?.end(input.stdin ?? "");
            },
          );
        },
        catch: (cause) => ({ message: String(cause) }),
      }),
  };
  return { driver, calls };
}

const AT = "2026-11-17T10:00:00.000Z";
const HOST = "mini.local";
const BRANCH = "preserve/20261117/mini/checkout";
/** Paths the dirty fixture's preserve commit carries, in Git's order. */
const PRESERVED = [".gitignore", "draft.md", "run.sh", "src/new.ts", "tracked"];
// Built at run time so the repository holds no token-shaped literal.
const fakeGithubToken = () => ["gh", "p_", "a1B2".repeat(9)].join("");

/** A local bare repository as `origin`. Pushes never leave the fixture directory. */
async function bareRemote(base: string, repo: string, options: { refuse?: boolean } = {}) {
  const remote = NodePath.join(base, "remote.git");
  await command(base, ["init", "-q", "--bare", "-b", "main", remote]);
  await command(repo, ["remote", "add", "origin", remote]);
  await command(repo, ["push", "-q", "origin", "main"]);
  if (options.refuse) {
    const hook = NodePath.join(remote, "hooks/pre-receive");
    await NodeFSP.writeFile(hook, "#!/bin/sh\necho 'fixture refuses every push' >&2\nexit 1\n");
    await NodeFSP.chmod(hook, 0o755);
  }
  return remote;
}

const write = async (root: string, relative: string, bytes: string, mode?: number) => {
  const path = NodePath.join(root, relative);
  await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true });
  await NodeFSP.writeFile(path, bytes);
  if (mode !== undefined) await NodeFSP.chmod(path, mode);
};

/** The owner's unpushed commit, then edits: one modified, one deleted, three new files. */
async function dirty(worktree: string) {
  await write(worktree, "gone.txt", "committed, then deleted\n");
  await command(worktree, ["add", "gone.txt"]);
  await command(worktree, ["commit", "-q", "-m", "owner work"]);
  await NodeFSP.rm(NodePath.join(worktree, "gone.txt"));
  await write(worktree, "tracked", "edited in the checkout\n");
  await write(worktree, "draft.md", "# draft\n");
  await write(worktree, "src/new.ts", "export const x = 1;\n");
  await write(worktree, "run.sh", "#!/bin/sh\necho preserved\n", 0o755);
}

/** Bytes and the executable bit of each path, for a byte-for-byte restore comparison. */
const snapshot = async (root: string, paths: readonly string[]) =>
  Object.fromEntries(
    await Promise.all(
      paths.map(async (path) => {
        const absolute = NodePath.join(root, path);
        const mode = (await NodeFSP.lstat(absolute)).mode;
        return [
          path,
          { bytes: (await NodeFSP.readFile(absolute)).toString("base64"), exec: !!(mode & 0o111) },
        ];
      }),
    ),
  );

const observing = (input: QualificationInput, beforeRemoval?: () => Promise<void>) => {
  let calls = 0;
  // The third observation is the last before removal: after the push and before the verified force.
  return Effect.tryPromise({
    try: async () => {
      if (++calls === 3) await beforeRemoval?.();
      return input;
    },
    catch: (cause) =>
      new RetirementError({ reason: "observation_failed", target: input.worktree, cause }),
  });
};

type SqlFixture = Awaited<ReturnType<typeof fixtureSql>>;
const runRetire = (
  f: SqlFixture,
  request: Pick<RetirementRequest, "projectRoot" | "worktree" | "observe"> &
    Partial<RetirementRequest>,
) =>
  Effect.runPromise(
    retire({
      cause: "owner_action",
      maxLevel: "branch-retained",
      admit: Effect.void,
      revalidate: Effect.void,
      finalizeOutputs: () => Effect.void,
      ...request,
    }).pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, f.sql)),
  );
const executed = (result: Awaited<ReturnType<typeof runRetire>>) => {
  NodeAssert.ok("id" in result, "a receipt was written");
  return result as RetirementReceipt;
};
const refs = (cwd: string, prefix: string) =>
  command(cwd, ["for-each-ref", "--format=%(refname)", prefix]);
const treeNames = async (cwd: string, sha: string) =>
  (await command(cwd, ["ls-tree", "-r", "--name-only", sha])).split("\n");

const decodeReceipt = Schema.decodeEffect(RetirementReceiptJson);
const encodeReceipt = Schema.encodeEffect(RetirementReceiptJson);

export const preserveCases: Record<string, () => Promise<void>> = {
  "preserve branch names are valid refs and restore commands quote their paths": () =>
    fixture(async ({ base }) => {
      const names = [
        [
          { host: "Dev's Mac mini.local", worktree: "/x/Feat..Fix.lock" },
          "dev-s-mac-mini/feat.fix-lock",
        ],
        [{ host: "-.", worktree: "/x/..." }, "host/worktree"],
        [{ host: "café", worktree: "/x/.hidden-" }, "caf/hidden"],
      ] as const;
      for (const [input, tail] of names) {
        const branch = preserveBranchBase({ at: AT, ...input });
        NodeAssert.equal(branch, `preserve/20261117/${tail}`);
        await command(base, ["check-ref-format", `refs/heads/${branch}`]);
      }
      NodeAssert.equal(
        restoreCommand("/repo", "/a b/it's", "preserve/x"),
        "git -C /repo worktree add '/a b/it'\\''s' preserve/x",
      );
    }),
  "owner Retire preserves edits to a verified branch, removes the checkout, and the branch restores it byte for byte":
    () =>
      fixture(async ({ base, repo, worktree, input }) => {
        const f = await fixtureSql(NodePath.join(base, "preserve.sqlite3"));
        try {
          const remote = await bareRemote(base, repo);
          await dirty(worktree);
          const head = await command(worktree, ["rev-parse", "HEAD"]);
          const before = await snapshot(worktree, PRESERVED);
          const proposal = await qualify(input);
          NodeAssert.equal(proposal.outcome, "hold");
          NodeAssert.equal(proposal.recoverability?.verdict, "retirable_after_preserve");
          NodeAssert.deepEqual(proposal.preservePlan?.selected, [
            "draft.md",
            "run.sh",
            "src/new.ts",
            "tracked",
          ]);
          NodeAssert.deepEqual(proposal.preservePlan?.deletions, ["gone.txt"]);
          const git = fixtureGit(base);
          const result = executed(
            await runRetire(f, {
              projectRoot: repo,
              worktree,
              observe: Effect.succeed(input),
              preserve: { git: git.driver, host: HOST, at: AT },
            }),
          );
          NodeAssert.equal(result.state, "retired");
          NodeAssert.deepEqual(result.reasons, []);
          const kept = result.preserve!;
          NodeAssert.deepEqual(kept, {
            branch: BRANCH,
            sha: kept.sha,
            remote: "origin",
            verification: "remote_verified",
            excluded: [],
            restore: `git -C ${repo} worktree add ${worktree} ${BRANCH}`,
            forced: true,
            readback: { local: "verified", remote: "verified" },
          } satisfies RetirementPreserve);
          NodeAssert.deepEqual(receiptRow(f, result.id).receipt.preserve, kept);
          NodeAssert.equal(receiptRow(f, result.id).state, "retired");
          NodeAssert.equal(await command(remote, ["rev-parse", `refs/heads/${BRANCH}`]), kept.sha);
          NodeAssert.equal(await command(repo, ["rev-parse", `refs/heads/${BRANCH}`]), kept.sha);
          NodeAssert.equal(await command(repo, ["rev-parse", `${kept.sha}^`]), head);
          NodeAssert.equal(await command(repo, ["rev-parse", "refs/heads/task"]), head);
          NodeAssert.equal(
            await command(repo, ["log", "-1", "--format=%B", kept.sha]),
            `preserve: ${worktree} ${AT} ${HOST}`,
          );
          NodeAssert.deepEqual(await treeNames(remote, kept.sha), PRESERVED);
          NodeAssert.equal(await exists(worktree), false);
          NodeAssert.equal(await readReservation(input.expected), null);
          // One unforced push, one forced removal; nothing deletes a ref.
          NodeAssert.deepEqual(
            git.calls.filter((args) => args[0] === "push"),
            [["push", "--porcelain", "origin", `${kept.sha}:refs/heads/${BRANCH}`]],
          );
          NodeAssert.deepEqual(
            git.calls.filter((args) => args[0] === "worktree"),
            [["worktree", "remove", "--force", "--", worktree]],
          );
          NodeAssert.ok(
            !git.calls.some((args) => args.some((arg) => ["-d", "-D", "--delete"].includes(arg))),
          );
          // The receipt's restore command recreates the checkout from the branch.
          await command(repo, ["worktree", "add", "-q", worktree, BRANCH]);
          NodeAssert.deepEqual(await snapshot(worktree, PRESERVED), before);
          NodeAssert.equal(await exists(NodePath.join(worktree, "gone.txt")), false);
          NodeAssert.equal(await command(worktree, ["status", "--porcelain"]), "");
        } finally {
          f.close();
        }
      }),
  "a taken preserve name, local or remote, is never overwritten; the next free suffix is used":
    async () => {
      for (const taken of [["local"], ["local", "remote-2"]] as const)
        await fixture(async ({ base, repo, worktree, input }) => {
          const f = await fixtureSql(NodePath.join(base, "collision.sqlite3"));
          try {
            const remote = await bareRemote(base, repo);
            const main = await command(repo, ["rev-parse", "main"]);
            await command(repo, ["update-ref", `refs/heads/${BRANCH}`, main]);
            if (taken.length > 1)
              await command(remote, ["update-ref", `refs/heads/${BRANCH}-2`, main]);
            await dirty(worktree);
            const git = fixtureGit(base);
            const result = executed(
              await runRetire(f, {
                projectRoot: repo,
                worktree,
                observe: Effect.succeed(input),
                preserve: { git: git.driver, host: HOST, at: AT },
              }),
            );
            const expected = taken.length > 1 ? `${BRANCH}-3` : `${BRANCH}-2`;
            NodeAssert.equal(result.state, "retired");
            NodeAssert.equal(result.preserve?.branch, expected);
            NodeAssert.equal(result.preserve?.verification, "remote_verified");
            NodeAssert.equal(await command(repo, ["rev-parse", `refs/heads/${BRANCH}`]), main);
            if (taken.length > 1)
              NodeAssert.equal(
                await command(remote, ["rev-parse", `refs/heads/${BRANCH}-2`]),
                main,
              );
            NodeAssert.equal(
              await command(remote, ["rev-parse", `refs/heads/${expected}`]),
              result.preserve?.sha,
            );
          } finally {
            f.close();
          }
        });
    },
  "with no remote, or a remote that refuses the push, the branch stays local and the anchored store keeps it":
    async () => {
      for (const route of ["no_remote", "refused"] as const)
        await fixture(async ({ base, repo, worktree, input }) => {
          const f = await fixtureSql(NodePath.join(base, "local-only.sqlite3"));
          try {
            const remote =
              route === "refused" ? await bareRemote(base, repo, { refuse: true }) : null;
            await dirty(worktree);
            const git = fixtureGit(base);
            const result = executed(
              await runRetire(f, {
                projectRoot: repo,
                worktree,
                observe: Effect.succeed(input),
                preserve: { git: git.driver, host: HOST, at: AT },
              }),
            );
            NodeAssert.equal(result.state, "retired");
            const kept = result.preserve!;
            NodeAssert.equal(kept.branch, BRANCH);
            NodeAssert.equal(kept.remote, remote ? "origin" : null);
            NodeAssert.equal(kept.verification, "local_only");
            NodeAssert.deepEqual(kept.readback, { local: "verified", remote: "not_applicable" });
            NodeAssert.equal(await command(repo, ["rev-parse", `refs/heads/${BRANCH}`]), kept.sha);
            NodeAssert.deepEqual(await treeNames(repo, kept.sha), PRESERVED);
            NodeAssert.equal(
              git.calls.filter((args) => args[0] === "push").length,
              route === "refused" ? 1 : 0,
            );
            if (remote) NodeAssert.equal(await refs(remote, "refs/heads/preserve/"), "");
            NodeAssert.equal(await exists(worktree), false);
          } finally {
            f.close();
          }
        });
    },
  "a local-only branch never counts as preserved when its object store is not anchored":
    async () => {
      for (const withRemote of [false, true])
        await fixture(async ({ base, repo, worktree, input }) => {
          const f = await fixtureSql(NodePath.join(base, "unanchored.sqlite3"));
          try {
            input.durableRepositories = [];
            if (withRemote) await bareRemote(base, repo);
            await dirty(worktree);
            NodeAssert.ok(
              (await qualify(input)).reasons.includes("repository_durability_unproven"),
            );
            const git = fixtureGit(base);
            const result = executed(
              await runRetire(f, {
                projectRoot: repo,
                worktree,
                observe: Effect.succeed(input),
                preserve: { git: git.driver, host: HOST, at: AT },
              }),
            );
            // A verified push makes the commits durable; without one the checkout is held.
            NodeAssert.equal(result.state, withRemote ? "retired" : "not_retired");
            NodeAssert.deepEqual(result.reasons, withRemote ? [] : ["preserve_store_not_durable"]);
            NodeAssert.equal(
              result.preserve?.verification,
              withRemote ? "remote_verified" : "local_only",
            );
            NodeAssert.equal(
              await command(repo, ["rev-parse", `refs/heads/${BRANCH}`]),
              result.preserve?.sha,
            );
            NodeAssert.equal(await exists(worktree), !withRemote);
          } finally {
            f.close();
          }
        });
    },
  "a file changed after the push holds the checkout with a tree mismatch and keeps the branch":
    () =>
      fixture(async ({ base, repo, worktree, input }) => {
        const f = await fixtureSql(NodePath.join(base, "mismatch.sqlite3"));
        try {
          const remote = await bareRemote(base, repo);
          await dirty(worktree);
          const stage = await command(worktree, ["ls-files", "--stage"]);
          const head = await command(worktree, ["rev-parse", "HEAD"]);
          const git = fixtureGit(base);
          const result = executed(
            await runRetire(f, {
              projectRoot: repo,
              worktree,
              observe: observing(input, () =>
                write(worktree, "tracked", "changed after the push\n"),
              ),
              preserve: { git: git.driver, host: HOST, at: AT },
            }),
          );
          NodeAssert.equal(result.state, "not_retired");
          NodeAssert.deepEqual(result.reasons, ["preserve_tree_mismatch"]);
          NodeAssert.equal(receiptRow(f, result.id).state, "not_retired");
          const kept = result.preserve!;
          NodeAssert.equal(kept.verification, "remote_verified");
          NodeAssert.equal(kept.forced, false);
          NodeAssert.equal(await command(remote, ["rev-parse", `refs/heads/${BRANCH}`]), kept.sha);
          NodeAssert.equal(await command(repo, ["rev-parse", `refs/heads/${BRANCH}`]), kept.sha);
          NodeAssert.equal(
            await command(remote, ["show", `${kept.sha}:tracked`]),
            "edited in the checkout",
          );
          // The checkout, its index and its branch are as the owner left them.
          NodeAssert.equal(
            await NodeFSP.readFile(NodePath.join(worktree, "tracked"), "utf8"),
            "changed after the push\n",
          );
          NodeAssert.equal(await command(worktree, ["ls-files", "--stage"]), stage);
          NodeAssert.equal(await command(worktree, ["rev-parse", "HEAD"]), head);
          NodeAssert.equal(await command(worktree, ["symbolic-ref", "HEAD"]), "refs/heads/task");
          NodeAssert.ok(!git.calls.some((args) => args[0] === "worktree"));
          NodeAssert.equal(await readReservation(input.expected), null);
        } finally {
          f.close();
        }
      }),
  "sensitive and secret-matching files hold the checkout and are never committed or pushed":
    async () => {
      const token = fakeGithubToken();
      // Present from the start: the plan excludes them, so nothing is committed or pushed.
      await fixture(async ({ base, repo, worktree, input }) => {
        const f = await fixtureSql(NodePath.join(base, "excluded.sqlite3"));
        try {
          const remote = await bareRemote(base, repo);
          await dirty(worktree);
          await write(worktree, ".env", "LOCAL_ONLY=1\n");
          await write(worktree, "config/gh.sh", `export GH=${token}\n`);
          const git = fixtureGit(base);
          const result = await runRetire(f, {
            projectRoot: repo,
            worktree,
            observe: Effect.succeed(input),
            preserve: { git: git.driver, host: HOST, at: AT },
          });
          NodeAssert.equal(result.state, "not_retired");
          NodeAssert.equal(result.qualification.recoverability?.verdict, "held");
          NodeAssert.deepEqual(
            result.qualification.preservePlan?.excluded.map(({ path, reason }) => [path, reason]),
            [
              [".env", "sensitive_name"],
              ["config/gh.sh", "secret_pattern:github_token"],
            ],
          );
          NodeAssert.deepEqual(git.calls, []);
          NodeAssert.equal(await refs(remote, "refs/heads/preserve/"), "");
          NodeAssert.equal(await refs(repo, "refs/heads/preserve/"), "");
          NodeAssert.equal(await exists(NodePath.join(worktree, ".env")), true);
        } finally {
          f.close();
        }
      });
      // Appearing after the push: the recheck names them and the pushed tree never held them.
      await fixture(async ({ base, repo, worktree, input }) => {
        const f = await fixtureSql(NodePath.join(base, "excluded-late.sqlite3"));
        try {
          const remote = await bareRemote(base, repo);
          await dirty(worktree);
          const git = fixtureGit(base);
          const result = executed(
            await runRetire(f, {
              projectRoot: repo,
              worktree,
              observe: observing(input, async () => {
                await write(worktree, ".env", "LOCAL_ONLY=1\n");
                await write(worktree, "gh.sh", `export GH=${token}\n`);
              }),
              preserve: { git: git.driver, host: HOST, at: AT },
            }),
          );
          NodeAssert.equal(result.state, "not_retired");
          NodeAssert.deepEqual(result.reasons, ["preserve_exclusions_changed"]);
          const kept = receiptRow(f, result.id).receipt.preserve!;
          NodeAssert.deepEqual(kept.excluded, [
            { path: ".env", reason: "sensitive_name" },
            { path: "gh.sh", reason: "secret_pattern:github_token" },
          ]);
          NodeAssert.deepEqual(await treeNames(remote, `refs/heads/${BRANCH}`), PRESERVED);
          NodeAssert.ok(
            !JSON.stringify(receiptRow(f, result.id).receipt).includes(token),
            "the receipt names the pattern, never the matched text",
          );
          NodeAssert.equal(await exists(NodePath.join(worktree, ".env")), true);
          NodeAssert.ok(!git.calls.some((args) => args[0] === "worktree"));
        } finally {
          f.close();
        }
      });
    },
  "automatic runs never preserve, push or force, even when offered a Git adapter": async () => {
    for (const cause of ["sweep", "settlement"] as const)
      await fixture(async ({ base, repo, worktree, input }) => {
        const f = await fixtureSql(NodePath.join(base, "automatic.sqlite3"));
        try {
          const remote = await bareRemote(base, repo);
          await dirty(worktree);
          const git = fixtureGit(base);
          const result = await runRetire(f, {
            cause,
            projectRoot: repo,
            worktree,
            observe: Effect.succeed(input),
            preserve: { git: git.driver, host: HOST, at: AT },
          });
          NodeAssert.equal(result.state, "not_retired");
          NodeAssert.ok(result.reasons.includes("dirty_tracked"));
          NodeAssert.deepEqual(git.calls, []);
          NodeAssert.equal(await refs(remote, "refs/heads/preserve/"), "");
          NodeAssert.equal(await refs(repo, "refs/heads/preserve/"), "");
          NodeAssert.equal(await exists(worktree), true);
        } finally {
          f.close();
        }
      });
  },
  "an adapter that drops the temporary index is refused before anything writes an index": () =>
    fixture(async ({ base, repo, worktree, input }) => {
      const f = await fixtureSql(NodePath.join(base, "isolation.sqlite3"));
      try {
        await bareRemote(base, repo);
        await dirty(worktree);
        const stage = await command(worktree, ["ls-files", "--stage"]);
        const git = fixtureGit(base, ({ env: _dropped, ...input }) => input);
        const result = executed(
          await runRetire(f, {
            projectRoot: repo,
            worktree,
            observe: Effect.succeed(input),
            preserve: { git: git.driver, host: HOST, at: AT },
          }),
        );
        NodeAssert.equal(result.state, "not_retired");
        NodeAssert.deepEqual(result.reasons, ["preserve_index_isolation_unverified"]);
        NodeAssert.equal(result.preserve, undefined);
        NodeAssert.deepEqual(git.calls, [["rev-parse", "--git-path", "index"]]);
        NodeAssert.equal(await command(worktree, ["ls-files", "--stage"]), stage);
        NodeAssert.equal(await refs(repo, "refs/heads/preserve/"), "");
      } finally {
        f.close();
      }
    }),
  "an interrupted preserving retirement keeps the owner's deletions and settles by readback":
    async () => {
      for (const lost of [false, true])
        await fixture(async ({ base, repo, worktree, input }) => {
          const f = await fixtureSql(NodePath.join(base, "interrupted.sqlite3"));
          try {
            await dirty(worktree);
            const head = await command(worktree, ["rev-parse", "HEAD"]);
            // The server stopped after the branch was created and before removal began.
            await command(repo, ["update-ref", `refs/heads/${BRANCH}`, head]);
            const bound = input.expected,
              id = "interrupted-preserve";
            await acquireReservation(bound, {
              schema: "workspace-reservation/v1",
              admin_id: bound.admin_id,
              incarnation: incarnation(bound),
              executor: "jones",
              receipt: id,
              ...(await holderIdentity()),
              boot_id: "a boot before the restart",
            });
            const receipt: RetirementReceipt = {
              schema: "worktree-retirement/v1",
              id,
              cause: "owner_action",
              state: "intent",
              binding: bound,
              incarnation: incarnation(bound),
              qualification: await qualify(input),
              coverage: {},
              preservationRefs: [],
              journal: id,
              disposed: [],
              reasons: [],
              limits: CONSUMER_LIMITS,
              bytes: { logical: null, allocated: null, statvfs_delta: null },
              preserve: {
                branch: BRANCH,
                sha: head,
                remote: null,
                verification: "local_only",
                excluded: [],
                restore: `git -C ${repo} worktree add ${worktree} ${BRANCH}`,
                forced: true,
              },
            };
            await Effect.runPromise(
              Effect.gen(function* () {
                const store = yield* Receipts;
                yield* store.begin(receipt);
                yield* store.journal(id, []);
                yield* store.finish(receipt);
              }).pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, f.sql)),
            );
            if (lost) await NodeFSP.rm(NodePath.join(worktree, "draft.md"));
            const git = fixtureGit(base);
            const result = executed(
              await runRetire(f, {
                projectRoot: repo,
                worktree,
                observe: Effect.succeed(input),
                preserve: { git: git.driver, host: HOST, at: AT },
              }),
            );
            if (lost) {
              // A missing file the commit carries means removal may have started.
              NodeAssert.equal(result.id, id);
              NodeAssert.equal(result.state, "partial");
              NodeAssert.deepEqual(result.reasons, ["interrupted_preserved_path_missing"]);
              NodeAssert.deepEqual(git.calls, []);
            } else {
              // The recorded deletion is the owner's edit, so the untouched checkout is retried.
              NodeAssert.deepEqual(receiptRow(f, id).receipt.reasons, [
                "interrupted_before_effect",
              ]);
              NodeAssert.equal(receiptRow(f, id).state, "not_retired");
              NodeAssert.equal(result.state, "retired");
              NodeAssert.equal(result.preserve?.branch, `${BRANCH}-2`);
              NodeAssert.deepEqual(await treeNames(repo, result.preserve!.sha), PRESERVED);
            }
            NodeAssert.equal(await command(repo, ["rev-parse", `refs/heads/${BRANCH}`]), head);
          } finally {
            f.close();
          }
        });
    },
  "receipts written before preserve branches decode, and preserve fields round-trip": () =>
    fixture(async ({ input }) => {
      const old: RetirementReceipt = {
        schema: "worktree-retirement/v1",
        id: "codec",
        cause: "owner_action",
        state: "retired",
        binding: input.expected,
        incarnation: incarnation(input.expected),
        qualification: {
          schema: "workspace-qualification/v1",
          authority_effect: "none",
          outcome: "hold",
          level: null,
          reasons: ["dirty_tracked"],
          decision: "hold",
          observationMs: 0,
        },
        coverage: {},
        preservationRefs: [],
        journal: "codec",
        disposed: [],
        reasons: [],
        limits: [],
        bytes: { logical: null, allocated: null, statvfs_delta: null },
      };
      const roundTrip = async (receipt: RetirementReceipt) =>
        Effect.runPromise(decodeReceipt(await Effect.runPromise(encodeReceipt(receipt))));
      const decodedOld = await Effect.runPromise(decodeReceipt(JSON.stringify(old)));
      NodeAssert.deepEqual(decodedOld, old);
      NodeAssert.equal("preserve" in decodedOld, false);
      NodeAssert.deepEqual(await roundTrip(old), old);
      const preserve: RetirementPreserve = {
        branch: BRANCH,
        sha: "a".repeat(40),
        remote: "origin",
        verification: "remote_verified",
        excluded: [{ path: ".env", reason: "sensitive_name" }],
        restore: `git -C /repo worktree add /repo-checkout ${BRANCH}`,
        forced: true,
      };
      for (const shape of [
        { ...old, preserve },
        { ...old, preserve: { ...preserve, remote: null, verification: "local_only" as const } },
        {
          ...old,
          preserve: { ...preserve, readback: { local: "verified", remote: "failed" } as const },
        },
      ])
        NodeAssert.deepEqual(await roundTrip(shape), shape);
      for (const bad of [
        { ...preserve, verification: "pushed" },
        { ...preserve, forced: "yes" },
        { ...preserve, readback: { local: "verified", remote: "maybe" } },
      ])
        await NodeAssert.rejects(
          Effect.runPromise(decodeReceipt(JSON.stringify({ ...old, preserve: bad }))),
        );
    }),
};

/** Settings evaluation with the server's Git adapter; `calls` records each Git invocation's arguments. */
export type PreserveEvaluation = (
  sql: SqlClient.SqlClient,
  request: Parameters<EvaluateRetirement>[1],
  git: { readonly home: string; readonly calls: string[][] },
) => ReturnType<EvaluateRetirement>;

export const preserveRuntimeCases = (
  evaluation: PreserveEvaluation,
): Record<string, () => Promise<void>> => ({
  "Settings evaluation preserves only on the owner's Retire; Propose and automatic runs never push":
    () =>
      fixture(async ({ base, repo, worktree }) => {
        const f = await fixtureSql(NodePath.join(base, "runtime.sqlite3"));
        try {
          // Settings evaluation reads these server tables; the checkout has no thread or native intent.
          f.db.exec(
            [
              "CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY,payload_json TEXT,deleted_at TEXT)",
              "CREATE TABLE projection_projects(workspace_root TEXT,deleted_at TEXT)",
              "CREATE TABLE native_creation_intents(worktree_path TEXT)",
              "CREATE TABLE jones_native_workspace_admissions(worktree_path TEXT)",
            ].join(";"),
          );
          f.db.prepare("INSERT INTO projection_projects VALUES (?,NULL)").run(repo);
          const remote = await bareRemote(base, repo);
          await dirty(worktree);
          const git = { home: base, calls: [] as string[][] };
          const evaluate = (request: Parameters<EvaluateRetirement>[1]) =>
            Effect.runPromise(evaluation(f.sql, request, git));
          const proposal = await evaluate({
            worktree,
            projectRoot: repo,
            execute: false,
            cause: "owner_action",
          });
          NodeAssert.equal(proposal.state, "held");
          NodeAssert.deepEqual(proposal.reasons, ["dirty_tracked", "untracked"]);
          // The proposal states what Retire will save and where, and names no untracked file as a disposal path.
          NodeAssert.equal(proposal.outcome.recoverability?.verdict, "retirable_after_preserve");
          NodeAssert.deepEqual(proposal.outcome.recoverability?.preserve, {
            files: 4,
            deletions: 1,
            bytes: proposal.outcome.recoverability?.preserve?.bytes,
            remote: "origin",
            excluded: [],
            excludedBytes: 0,
          });
          NodeAssert.equal(
            proposal.outcome.paths.some((path) => path.endsWith("draft.md")),
            false,
          );
          NodeAssert.equal(proposal.outcome.preserve, undefined);
          for (const cause of ["sweep", "settlement"] as const) {
            const automatic = await evaluate({ worktree, projectRoot: repo, execute: true, cause });
            NodeAssert.equal(automatic.state, "not_retired");
            NodeAssert.deepEqual(automatic.reasons, ["dirty_tracked", "untracked"]);
          }
          // The proposal reads the branch's remote to name it; that lookup is the only Git call before Retire.
          NodeAssert.deepEqual(git.calls, [["config", "--get", "branch.task.remote"], ["remote"]]);
          NodeAssert.equal(await refs(remote, "refs/heads/preserve/"), "");
          NodeAssert.equal(await exists(worktree), true);
          const retired = await evaluate({
            worktree,
            projectRoot: repo,
            execute: true,
            cause: "owner_action",
            ownerCoverage: { fingerprint: proposal.outcome.fingerprint!, paths: [] },
          });
          NodeAssert.equal(retired.state, "retired");
          const pushed = (await refs(remote, "refs/heads/preserve/")).split("\n");
          NodeAssert.equal(pushed.length, 1);
          NodeAssert.match(pushed[0]!, /^refs\/heads\/preserve\/\d{8}\/mini\/checkout$/);
          const sha = await command(remote, ["rev-parse", pushed[0]!]);
          NodeAssert.equal(await command(repo, ["rev-parse", pushed[0]!]), sha);
          NodeAssert.deepEqual(await treeNames(remote, sha), PRESERVED);
          // The result carries the receipt's branch, verification and restore command to the owner.
          NodeAssert.deepEqual(
            {
              branch: retired.outcome.preserve?.branch,
              sha: retired.outcome.preserve?.sha,
              remote: retired.outcome.preserve?.remote,
              verification: retired.outcome.preserve?.verification,
              forced: retired.outcome.preserve?.forced,
            },
            {
              branch: pushed[0]!.slice("refs/heads/".length),
              sha,
              remote: "origin",
              verification: "remote_verified",
              forced: true,
            },
          );
          NodeAssert.match(
            retired.outcome.preserve?.restore ?? "",
            /preserve\/\d{8}\/mini\/checkout/,
          );
          NodeAssert.equal(await exists(worktree), false);
          NodeAssert.ok(git.calls.some((args) => args[0] === "push"));
        } finally {
          f.close();
        }
      }),
});
