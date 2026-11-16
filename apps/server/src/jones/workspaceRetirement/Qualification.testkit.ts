// @effect-diagnostics nodeBuiltinImport:off -- Exact disposable Git fixtures exercise qualification and preservation without live application state.
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeModule from "node:module";
import * as NodeSqlite from "node:sqlite";
import * as NodeChildProcess from "node:child_process";
import { incarnation } from "./OutputAttestation.ts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { SqlError, UnknownError } from "effect/sql/SqlError";
import { command, completeConsumers } from "./Conformance.testkit.ts";
import { layer } from "./Receipts.ts";
import { retire } from "./RetirementExecutor.ts";
import { binding } from "./OutputAttestation.ts";
import { qualify, finalCheck, type QualificationInput } from "./Qualification.ts";

async function fixture(
  run: (f: { repo: string; worktree: string; input: QualificationInput }) => Promise<void>,
  backend: "files" | "reftable" = "files",
) {
  const parent = NodePath.resolve(
    import.meta.dirname,
    "../../../../../node_modules/.cache/jones/qualification-tests",
  );
  await NodeFSP.mkdir(parent, { recursive: true });
  const base = await NodeFSP.mkdtemp(NodePath.join(parent, "case-"));
  try {
    const repo = NodePath.join(base, "repo"),
      worktree = NodePath.join(base, "checkout");
    await NodeFSP.mkdir(repo);
    await command(repo, [
      "init",
      ...(backend === "reftable" ? ["--ref-format=reftable"] : []),
      "-b",
      "main",
    ]);
    await NodeFSP.writeFile(NodePath.join(repo, "tracked"), "original\n");
    await NodeFSP.writeFile(NodePath.join(repo, ".gitignore"), "build/\n");
    await command(repo, ["add", "."]);
    await command(repo, ["commit", "-m", "base"]);
    await command(repo, ["worktree", "add", "-b", "task", worktree]);
    const expected = await binding(worktree);
    await run({
      repo,
      worktree,
      input: {
        worktree,
        expected,
        projectRoots: [repo],
        durableRepositories: [expected.git_common_dir],
        temporaryRoots: [],
        nativeRecreation: "supported",
        consumers: completeConsumers(),
      },
    });
  } finally {
    await NodeFSP.rm(base, { recursive: true, force: true });
    await NodeAssert.rejects(NodeFSP.lstat(base), { code: "ENOENT" });
  }
}

async function retireAndVerify(
  repo: string,
  worktree: string,
  input: QualificationInput,
  heads: string[],
) {
  const db = new NodeSqlite.DatabaseSync(NodePath.join(NodePath.dirname(repo), "receipts.sqlite3"));
  try {
    for (const name of ["136_JonesWorkspaceProvenance.ts", "138_JonesWorkspaceRetirement.ts"]) {
      const source = await NodeFSP.readFile(
        new URL("../persistence/Migrations/" + name, import.meta.url),
        "utf8",
      );
      for (const statement of source.matchAll(/yield\* sql`([^`]+)`/g)) db.exec(statement[1]!);
    }
    const sql = Object.assign(
      (parts: TemplateStringsArray, ...values: unknown[]) =>
        Effect.tryPromise({
          try: async () =>
            db.prepare(parts.join("?")).all(...(values as (string | number | null)[])),
          catch: (cause) => new SqlError({ reason: new UnknownError({ cause }) }),
        }),
      {
        withTransaction: <A, E, R>(body: Effect.Effect<A, E, R>) =>
          Effect.suspend(() => {
            db.exec("BEGIN IMMEDIATE");
            return body.pipe(
              Effect.tap(() => Effect.sync(() => db.exec("COMMIT"))),
              Effect.catchCause((cause) =>
                Effect.sync(() => db.exec("ROLLBACK")).pipe(
                  Effect.andThen(Effect.failCause(cause)),
                ),
              ),
            );
          }),
      },
    ) as unknown as SqlClient.SqlClient;
    const result = await Effect.runPromise(
      retire({
        cause: "sweep",
        projectRoot: repo,
        worktree,
        maxLevel: "branch-retained",
        admit: Effect.void,
        observe: Effect.succeed(input),
        revalidate: Effect.void,
        finalizeOutputs: () => Effect.void,
      }).pipe(Effect.provide(layer), Effect.provideService(SqlClient.SqlClient, sql)),
    );
    NodeAssert.equal(result.state, "retired", JSON.stringify(result));
    NodeAssert.ok("preservationRefs" in result);
    NodeAssert.deepEqual(result.preservationRefs.map((ref) => ref.head).sort(), [...heads].sort());
    for (const ref of result.preservationRefs) {
      NodeAssert.match(ref.ref, /^refs\/retired-worktrees\//);
      NodeAssert.equal(await command(repo, ["rev-parse", ref.ref]), ref.head);
    }
    await NodeAssert.rejects(NodeFSP.lstat(worktree), { code: "ENOENT" });
  } finally {
    db.close();
  }
}

export const qualificationCases: Record<string, () => Promise<void>> = {
  "root refs without reflogs preserve unique current tips and FETCH_HEAD retains its operation hold":
    () =>
      fixture(async ({ repo, worktree, input }) => {
        const tree = await command(repo, ["rev-parse", "HEAD^{tree}"]);
        const orphan = await command(repo, ["commit-tree", tree, "-m", "root-only orphan"]);
        for (const ref of ["ORIG_HEAD", "AUTO_MERGE", "SYNTHETIC_HEAD"]) {
          await command(worktree, ["update-ref", ref, orphan]);
          const proposal = await qualify(input);
          NodeAssert.equal(proposal.outcome, "retirable", JSON.stringify(proposal));
          NodeAssert.deepEqual(proposal.preservationTips, [orphan]);
          await command(worktree, ["update-ref", "-d", ref]);
        }
        await NodeFSP.writeFile(
          NodePath.join(input.expected.git_dir, "FETCH_HEAD"),
          `${orphan}\t\tfixture\n`,
        );
        NodeAssert.ok((await qualify(input)).reasons.includes("operation_in_progress"));
      }),
  "candidate log aggregate bytes, symlinks and missing directory preserve conservative decisions":
    () =>
      fixture(async ({ worktree, input }) => {
        const logs = NodePath.join(input.expected.git_dir, "logs");
        const head = await command(worktree, ["rev-parse", "HEAD"]);
        const record = `${head} ${head} Fixture <fixture@example.invalid> 1 +0000\tfixture\n`;
        const bytes = record.repeat(Math.ceil(600000 / record.length));
        await NodeFSP.writeFile(NodePath.join(logs, "HEAD"), bytes);
        await NodeFSP.writeFile(NodePath.join(logs, "ORIG_HEAD"), bytes);
        const bounded = await qualify(input);
        NodeAssert.equal(bounded.outcome, "unknown");
        NodeAssert.deepEqual(bounded.observationFailure, {
          stage: "candidate_reflog",
          errorClass: "bounds",
        });
        await NodeFSP.unlink(NodePath.join(logs, "ORIG_HEAD"));
        await NodeFSP.symlink("HEAD", NodePath.join(logs, "ORIG_HEAD"));
        NodeAssert.equal((await qualify(input)).outcome, "unknown");
        await NodeFSP.rm(logs, { recursive: true });
        await command(worktree, ["-c", "core.logAllRefUpdates=false", "checkout", "--detach"]);
        await command(worktree, [
          "-c",
          "core.logAllRefUpdates=false",
          "commit",
          "--allow-empty",
          "-m",
          "no logs",
        ]);
        const detached = await command(worktree, ["rev-parse", "HEAD"]);
        NodeAssert.equal(
          await NodeFSP.stat(logs).then(
            () => true,
            () => false,
          ),
          false,
        );
        const result = await qualify(input);
        NodeAssert.equal(result.outcome, "retirable", JSON.stringify(result));
        NodeAssert.deepEqual(result.preservationTips, [detached]);
      }),
  "only this incarnation's recovery namespace is excluded from containment": () =>
    fixture(async ({ repo, input }) => {
      const before = await qualify(input);
      const own = `refs/retired-worktrees/${incarnation(input.expected)}/0`;
      await command(repo, ["update-ref", own, before.head!]);
      NodeAssert.equal((await finalCheck(input, before)).conflict, false);
      const sibling = `refs/retired-worktrees/${incarnation(input.expected)}-other/0`;
      await command(repo, ["update-ref", sibling, before.head!]);
      const after = await qualify(input);
      NodeAssert.ok(after.containingRefs?.includes(sibling));
      NodeAssert.ok(!after.containingRefs?.includes(own));
      NodeAssert.equal((await finalCheck(input, before)).conflict, true);
    }),
  "SC-C1 candidate rewritten old-only history is preserved and current local refs hold": () =>
    fixture(async ({ repo, worktree, input }) => {
      const original = await command(repo, ["rev-parse", "HEAD"]);
      const tree = await command(repo, ["rev-parse", "HEAD^{tree}"]);
      const orphan = await command(repo, ["commit-tree", tree, "-m", "rewritten old-only"]);
      for (const name of ["refs/rewritten/x", "refs/worktree/x", "refs/bisect/x", "ORIG_HEAD"]) {
        const log = NodePath.join(input.expected.git_dir, "logs", name);
        await NodeFSP.mkdir(NodePath.dirname(log), { recursive: true });
        await NodeFSP.writeFile(
          log,
          `${orphan} ${original} Fixture <fixture@example.invalid> 1 +0000\tfixture\n`,
        );
        const proposal = await qualify(input);
        NodeAssert.equal(proposal.outcome, "retirable", JSON.stringify(proposal));
        NodeAssert.deepEqual(proposal.preservationTips, [orphan]);
        await NodeFSP.unlink(log);
      }
      for (const namespace of ["refs/rewritten/", "refs/worktree/", "refs/bisect/"]) {
        await command(worktree, ["update-ref", `${namespace}x`, orphan]);
        const current = await qualify(input);
        NodeAssert.equal(current.outcome, "hold", JSON.stringify(current));
        NodeAssert.ok(current.reasons.includes("per_worktree_refs"));
        NodeAssert.ok(!current.containingRefs?.some((ref) => ref.startsWith(namespace)));
        await command(worktree, ["update-ref", "-d", `${namespace}x`]);
      }
    }),
  "common branch and other-checkout reflog-only tips do not consume candidate preservation work":
    () =>
      fixture(async ({ repo, worktree, input }) => {
        const other = NodePath.join(NodePath.dirname(worktree), "other");
        await command(repo, ["worktree", "add", "--detach", other]);
        const original = await command(repo, ["rev-parse", "HEAD"]);
        const tree = await command(repo, ["rev-parse", "HEAD^{tree}"]);
        const tips: string[] = [];
        // More orphan tips than the existing preservation bound, in carriers removal leaves intact.
        for (let i = 0; i < 54; i++) {
          const tip = await command(repo, ["commit-tree", tree, "-m", `unrelated-${i}`]);
          tips.push(tip);
          await command(i % 2 ? other : repo, [
            "update-ref",
            "--create-reflog",
            i % 2 ? "HEAD" : "refs/heads/history",
            tip,
          ]);
        }
        await command(repo, ["update-ref", "refs/heads/history", original]);
        await command(other, ["update-ref", "HEAD", original]);
        const commands = NodeTest.mock.method(
          NodeModule.createRequire(import.meta.url)(
            "node:child_process",
          ) as typeof import("node:child_process"),
          "execFile",
        );
        NodeModule.syncBuiltinESMExports();
        let result;
        try {
          result = await qualify(input);
          const containment = commands.mock.calls.filter(
            ({ arguments: args }) => Array.isArray(args[1]) && args[1].includes("--contains"),
          );
          NodeAssert.equal(containment.length, 2);
        } finally {
          commands.mock.restore();
          NodeModule.syncBuiltinESMExports();
        }
        NodeAssert.equal(result.outcome, "retirable", JSON.stringify(result));
        NodeAssert.deepEqual(result.preservationTips, []);
        NodeAssert.equal(result.level, "contained");
        NodeAssert.equal((await finalCheck(input, result)).conflict, false);
        // The same history in the candidate's carrier still requires preservation
        // and keeps the existing orphan bound, rather than silently truncating it.
        await NodeFSP.writeFile(
          NodePath.join(input.expected.git_dir, "logs/HEAD"),
          tips
            .map((tip) => `${original} ${tip} Fixture <fixture@example.invalid> 1 +0000\tfixture\n`)
            .join(""),
        );
        const candidate = await qualify(input);
        NodeAssert.equal(candidate.outcome, "hold");
        NodeAssert.ok(candidate.reasons.includes("orphan_commits_bound"));
        NodeAssert.equal(candidate.preservationTips?.length, 51);
      }),
  "candidate reflog-only orphan survives branch switch and changes the proposal fingerprint": () =>
    fixture(async ({ worktree, input }) => {
      const before = await qualify(input);
      await command(worktree, ["checkout", "--detach"]);
      await command(worktree, ["commit", "--allow-empty", "-m", "candidate orphan"]);
      const orphan = await command(worktree, ["rev-parse", "HEAD"]);
      const detached = await qualify(input);
      NodeAssert.deepEqual(detached.preservationTips, [orphan]);
      NodeAssert.equal(detached.level, "branch-retained");
      await command(worktree, ["checkout", "task"]);
      const after = await qualify(input);
      NodeAssert.equal(after.outcome, "retirable");
      NodeAssert.deepEqual(after.preservationTips, [orphan]);
      NodeAssert.notEqual(after.fingerprint, before.fingerprint);
      NodeAssert.equal((await finalCheck(input, before)).conflict, true);
      await command(worktree, ["branch", "retained", orphan]);
      NodeAssert.deepEqual((await qualify(input)).preservationTips, []);
      await command(worktree, ["commit", "--allow-empty", "-m", "branch retained"]);
      const branch = await qualify(input);
      NodeAssert.equal(branch.level, "branch-retained");
      NodeAssert.deepEqual(branch.preservationTips, []);
      await command(worktree, ["checkout", "--detach", "main"]);
      const contained = await qualify(input);
      NodeAssert.equal(contained.level, "contained");
      NodeAssert.deepEqual(contained.preservationTips, []);
      await command(worktree, ["commit", "--allow-empty", "-m", "detached without reflog"]);
      const head = await command(worktree, ["rev-parse", "HEAD"]);
      await NodeFSP.unlink(NodePath.join(input.expected.git_dir, "logs/HEAD"));
      NodeAssert.deepEqual((await qualify(input)).preservationTips, [head]);
    }),
  "retirement writes recovery refs for candidate old-only reflog and detached HEAD tips": () =>
    fixture(async ({ repo, worktree, input }) => {
      const tree = await command(repo, ["rev-parse", "HEAD^{tree}"]);
      const orphan = await command(repo, ["commit-tree", tree, "-m", "old-only candidate"]);
      await command(worktree, ["checkout", "--detach"]);
      await command(worktree, ["commit", "--allow-empty", "-m", "detached current"]);
      const head = await command(worktree, ["rev-parse", "HEAD"]);
      await NodeFSP.mkdir(NodePath.join(input.expected.git_dir, "logs/refs/rewritten"), {
        recursive: true,
      });
      await NodeFSP.writeFile(
        NodePath.join(input.expected.git_dir, "logs/refs/rewritten/x"),
        `${orphan} ${head} Fixture <fixture@example.invalid> 1 +0000\tfixture\n`,
      );
      const proposal = await qualify(input);
      NodeAssert.equal(proposal.outcome, "retirable");
      NodeAssert.deepEqual([...proposal.preservationTips!].sort(), [orphan, head].sort());
      await retireAndVerify(repo, worktree, input, [orphan, head]);
    }),
  "candidate reflog corruption and unreadability retain bounded sanitized observation diagnostics":
    () =>
      fixture(async ({ input }) => {
        const log = NodePath.join(input.expected.git_dir, "logs/HEAD");
        const original = await NodeFSP.readFile(log);
        for (const scenario of ["malformed", "permission_denied"] as const) {
          if (scenario === "malformed")
            await NodeFSP.writeFile(log, "private raw corruption must never be echoed\n");
          else {
            await NodeFSP.writeFile(log, original);
            await NodeFSP.chmod(log, 0);
          }
          try {
            const result = await qualify(input);
            NodeAssert.notEqual(result.outcome, "retirable");
            NodeAssert.ok(result.reasons.includes("observation_failed"));
            NodeAssert.deepEqual(result.observationFailure, {
              stage: "candidate_reflog",
              errorClass: scenario,
            });
            NodeAssert.ok(!JSON.stringify(result).includes("private raw corruption"));
            NodeAssert.match(
              result.details?.find((d) => d.reason === "observation_failed")?.fact ?? "",
              /candidate_reflog/,
            );
          } finally {
            await NodeFSP.chmod(log, 0o644);
          }
        }
      }),
  "candidate reflog bytes and distinct tips are bounded rather than silently dropped": () =>
    fixture(async ({ input }) => {
      const log = NodePath.join(input.expected.git_dir, "logs/HEAD");
      const head = await command(input.worktree, ["rev-parse", "HEAD"]);
      for (const bytes of [
        "x".repeat(1024 * 1024 + 1),
        Array.from(
          { length: 1001 },
          (_, i) =>
            `${head} ${i.toString(16).padStart(40, "0")} Fixture <fixture@example.invalid> 1 +0000\tfixture\n`,
        ).join(""),
      ]) {
        await NodeFSP.writeFile(log, bytes);
        const result = await qualify(input);
        NodeAssert.notEqual(result.outcome, "retirable");
        NodeAssert.deepEqual(result.observationFailure, {
          stage: "candidate_reflog",
          errorClass: "bounds",
        });
      }
    }),
  "Git command failures and elapsed budget identify their safe stage without copying native output":
    () =>
      fixture(async ({ input }) => {
        const index = NodePath.join(input.expected.git_dir, "index");
        const original = await NodeFSP.readFile(index);
        await NodeFSP.writeFile(index, "private invalid index");
        const failed = await qualify(input);
        NodeAssert.deepEqual(failed.observationFailure, {
          stage: "tracked_status",
          errorClass: "native_command",
        });
        NodeAssert.ok(!JSON.stringify(failed).includes("private invalid index"));
        await NodeFSP.writeFile(index, original);
        let calls = 0;
        const clock = NodeTest.mock.method(performance, "now", () => (calls++ ? 60001 : 0));
        try {
          const expired = await qualify(input);
          NodeAssert.notEqual(expired.outcome, "retirable");
          NodeAssert.deepEqual(expired.observationFailure, {
            stage: "registration",
            errorClass: "timeout",
          });
        } finally {
          clock.mock.restore();
        }
      }),
};

// Help capability observation is read-only; every admitted reftable case then
// initializes and verifies a real native backend. Older Git reports explicit skips.
const initHelp = NodeChildProcess.spawnSync("git", ["init", "-h"], { encoding: "utf8" });
export const reftableAvailable = `${initHelp.stdout}${initHelp.stderr}`.includes("--ref-format");
export const reftableCases: Record<string, () => Promise<void>> = {
  "native reftable root refs without reflogs preserve unique current tips and FETCH_HEAD retains its operation hold":
    () =>
      fixture(async ({ repo, worktree, input }) => {
        const tree = await command(repo, ["rev-parse", "HEAD^{tree}"]);
        const orphan = await command(repo, ["commit-tree", tree, "-m", "root-only orphan"]);
        for (const ref of ["ORIG_HEAD", "AUTO_MERGE", "SYNTHETIC_HEAD"]) {
          await command(worktree, ["update-ref", ref, orphan]);
          const proposal = await qualify(input);
          NodeAssert.equal(proposal.outcome, "retirable", JSON.stringify(proposal));
          NodeAssert.deepEqual(proposal.preservationTips, [orphan]);
          await command(worktree, ["update-ref", "-d", ref]);
        }
        await NodeFSP.writeFile(
          NodePath.join(input.expected.git_dir, "FETCH_HEAD"),
          `${orphan}\t\tfixture\n`,
        );
        NodeAssert.ok((await qualify(input)).reasons.includes("operation_in_progress"));
      }, "reftable"),
  "native reftable detached HEAD remains protected without a reflog": () =>
    fixture(async ({ worktree, input }) => {
      await command(worktree, ["checkout", "--detach"]);
      await command(worktree, ["commit", "--allow-empty", "-m", "detached no log"]);
      const head = await command(worktree, ["rev-parse", "HEAD"]);
      await command(worktree, ["reflog", "expire", "--expire=all", "HEAD"]);
      NodeAssert.equal(await command(worktree, ["reflog", "show", "--format=%H", "HEAD"]), "");
      const result = await qualify(input);
      NodeAssert.equal(result.outcome, "retirable", JSON.stringify(result));
      NodeAssert.deepEqual(result.preservationTips, [head]);
    }, "reftable"),
  "native reftable missing objects remain unknown and disposable views are removed on failure":
    () =>
      fixture(async ({ worktree, input }) => {
        await command(worktree, ["checkout", "--detach"]);
        await command(worktree, ["commit", "--allow-empty", "-m", "pruned fixture"]);
        const orphan = await command(worktree, ["rev-parse", "HEAD"]);
        await command(worktree, ["checkout", "task"]);
        await NodeFSP.unlink(
          NodePath.join(
            input.expected.git_common_dir,
            "objects",
            orphan.slice(0, 2),
            orphan.slice(2),
          ),
        );
        const commands = NodeTest.mock.method(
          NodeModule.createRequire(import.meta.url)(
            "node:child_process",
          ) as typeof import("node:child_process"),
          "execFile",
        );
        NodeModule.syncBuiltinESMExports();
        try {
          const result = await qualify(input);
          NodeAssert.equal(result.outcome, "unknown", JSON.stringify(result));
          NodeAssert.deepEqual(result.observationFailure, {
            stage: "candidate_reflog",
            errorClass: "native_command",
          });
          const view = commands.mock.calls.flatMap(({ arguments: args }) =>
            Array.isArray(args[1])
              ? args[1].filter((arg: string) => arg.startsWith("--git-dir="))
              : [],
          );
          NodeAssert.equal(view.length, 1);
          await NodeAssert.rejects(NodeFSP.lstat((view[0] as string).slice("--git-dir=".length)), {
            code: "ENOENT",
          });
        } finally {
          commands.mock.restore();
          NodeModule.syncBuiltinESMExports();
        }
      }, "reftable"),
  "SC-C2 native reftable detached history survives returning to a branch": () =>
    fixture(async ({ worktree, input }) => {
      NodeAssert.equal(await command(worktree, ["rev-parse", "--show-ref-format"]), "reftable");
      await command(worktree, ["checkout", "--detach"]);
      await command(worktree, ["commit", "--allow-empty", "-m", "reftable orphan"]);
      const orphan = await command(worktree, ["rev-parse", "HEAD"]);
      await command(worktree, ["checkout", "task"]);
      await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(input.expected.git_dir, "logs/HEAD")), {
        code: "ENOENT",
      });
      const proposal = await qualify(input);
      NodeAssert.equal(proposal.outcome, "retirable", JSON.stringify(proposal));
      NodeAssert.deepEqual(proposal.preservationTips, [orphan]);
    }, "reftable"),
  "native reftable old-only endpoints receive recovery refs through successful removal": () =>
    fixture(async ({ repo, worktree, input }) => {
      await command(worktree, ["checkout", "--detach"]);
      await command(worktree, ["commit", "--allow-empty", "-m", "old-only native"]);
      const orphan = await command(worktree, ["rev-parse", "HEAD"]);
      await command(worktree, ["checkout", "task"]);
      while (
        (await command(worktree, ["reflog", "show", "--format=%H", "HEAD"])).split("\n").length > 1
      )
        await command(worktree, ["reflog", "delete", "HEAD@{1}"]);
      // Only the old field of the remaining transition names the orphan.
      NodeAssert.ok(
        !(await command(worktree, ["reflog", "show", "--format=%H", "HEAD"])).includes(orphan),
      );
      const proposal = await qualify(input);
      NodeAssert.equal(proposal.outcome, "retirable", JSON.stringify(proposal));
      NodeAssert.deepEqual(proposal.preservationTips, [orphan]);
      await retireAndVerify(repo, worktree, input, [orphan]);
    }, "reftable"),
  "native reftable local refs hold and unrelated common history does not consume candidate work":
    () =>
      fixture(async ({ repo, worktree, input }) => {
        const original = await command(repo, ["rev-parse", "HEAD"]);
        const tree = await command(repo, ["rev-parse", "HEAD^{tree}"]);
        const other = NodePath.join(NodePath.dirname(worktree), "other");
        await command(repo, ["worktree", "add", "--detach", other]);
        const tips: string[] = [];
        for (let i = 0; i < 54; i++) {
          const tip = await command(repo, ["commit-tree", tree, "-m", `unrelated-${i}`]);
          tips.push(tip);
          await command(i % 2 ? other : repo, [
            "update-ref",
            "--create-reflog",
            i % 2 ? "HEAD" : "refs/heads/history",
            tip,
          ]);
        }
        await command(repo, ["update-ref", "refs/heads/history", original]);
        await command(other, ["update-ref", "HEAD", original]);
        const commands = NodeTest.mock.method(
          NodeModule.createRequire(import.meta.url)(
            "node:child_process",
          ) as typeof import("node:child_process"),
          "execFile",
        );
        NodeModule.syncBuiltinESMExports();
        try {
          const result = await qualify(input);
          NodeAssert.equal(result.outcome, "retirable", JSON.stringify(result));
          NodeAssert.deepEqual(result.preservationTips, []);
          NodeAssert.equal(
            commands.mock.calls.filter(
              ({ arguments: args }) => Array.isArray(args[1]) && args[1].includes("--contains"),
            ).length,
            2,
          );
          const view = commands.mock.calls.flatMap(({ arguments: args }) =>
            Array.isArray(args[1])
              ? args[1].filter((arg: string) => arg.startsWith("--git-dir="))
              : [],
          );
          NodeAssert.equal(view.length, 1);
          await NodeAssert.rejects(NodeFSP.lstat((view[0] as string).slice("--git-dir=".length)), {
            code: "ENOENT",
          });
        } finally {
          commands.mock.restore();
          NodeModule.syncBuiltinESMExports();
        }
        for (const namespace of ["refs/rewritten/", "refs/worktree/", "refs/bisect/"]) {
          await command(worktree, ["update-ref", `${namespace}x`, tips[0]!]);
          const guarded = await qualify(input);
          NodeAssert.equal(guarded.outcome, "hold");
          NodeAssert.ok(guarded.reasons.includes("per_worktree_refs"));
          NodeAssert.ok(!guarded.containingRefs?.some((ref) => ref.startsWith(namespace)));
          await command(worktree, ["update-ref", "-d", `${namespace}x`]);
        }
        await command(worktree, ["checkout", "--detach"]);
        for (const tip of tips) await command(worktree, ["update-ref", "HEAD", tip]);
        await command(worktree, ["update-ref", "HEAD", original]);
        const bounded = await qualify(input);
        NodeAssert.equal(bounded.outcome, "hold");
        NodeAssert.ok(bounded.reasons.includes("orphan_commits_bound"));
      }, "reftable"),
};
