// @effect-diagnostics nodeBuiltinImport:off -- Exact disposable Git fixtures exercise qualification and preservation without live application state.
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeModule from "node:module";
import * as NodeFS from "node:fs";
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
import { PRESERVE_LIMITS, SECRET_PATTERNS, secretPatternMatch } from "./Recoverability.ts";
import { RetirementReceiptJson } from "./DurableCodecs.ts";
import type { RetirementReceipt } from "./Receipts.ts";
import * as Schema from "effect/Schema";

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
  "candidate reflog injected read failures retain bounded sanitized diagnostics": () =>
    fixture(async ({ input }) => {
      const fs = NodeModule.createRequire(import.meta.url)(
        "node:fs/promises",
      ) as typeof import("node:fs/promises");
      const original = fs.open;
      for (const [code, expected] of [
        ["EACCES", "permission_denied"],
        ["EIO", "native_read"],
        ["bound_exceeded", "bounds"],
      ]) {
        const opened = NodeTest.mock.method(fs, "open", (...args: unknown[]) => {
          if (
            String(args[0]).endsWith("/HEAD") &&
            NodeFS.realpathSync(NodePath.dirname(String(args[0]))).endsWith("/logs")
          )
            return Promise.reject(
              code === "bound_exceeded"
                ? new Error(code)
                : Object.assign(new Error("private failure detail"), { code }),
            );
          return Reflect.apply(original, fs, args);
        });
        NodeModule.syncBuiltinESMExports();
        try {
          const result = await qualify(input);
          NodeAssert.notEqual(result.outcome, "retirable");
          NodeAssert.deepEqual(result.observationFailure, {
            stage: "candidate_reflog",
            errorClass: expected,
          });
          NodeAssert.ok(!JSON.stringify(result).includes("private failure detail"));
        } finally {
          opened.mock.restore();
          NodeModule.syncBuiltinESMExports();
        }
      }
    }),
  "Git command failures and elapsed budget identify their safe stage without copying native output":
    () =>
      fixture(async ({ input }) => {
        const child = NodeModule.createRequire(import.meta.url)(
          "node:child_process",
        ) as typeof import("node:child_process");
        const original = child.execFile;
        const commands = NodeTest.mock.method(child, "execFile", (...args: unknown[]) => {
          if ((args[1] as string[]).includes("status")) {
            (args[3] as (error: Error | null, stdout: Buffer, stderr: Buffer) => void)(
              Object.assign(new Error("private native failure"), { code: 1 }),
              Buffer.alloc(0),
              Buffer.alloc(0),
            );
            return;
          }
          return Reflect.apply(original, child, args);
        });
        NodeModule.syncBuiltinESMExports();
        try {
          const failed = await qualify(input);
          NodeAssert.deepEqual(failed.observationFailure, {
            stage: "tracked_status",
            errorClass: "native_command",
          });
          NodeAssert.ok(!JSON.stringify(failed).includes("private native failure"));
        } finally {
          commands.mock.restore();
          NodeModule.syncBuiltinESMExports();
        }
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

// Fake credentials are assembled at run time so no literal secret is committed.
const encodeReceipt = Schema.encodeSync(RetirementReceiptJson);
const decodeReceipt = Schema.decodeSync(RetirementReceiptJson);
const fake = {
  private_key: () => ["-----BEGIN ", "RSA PRIVATE KEY", "-----"].join(""),
  aws_access_key: () => ["AK", "IA", "ABCDEFGHIJKLMNOP"].join(""),
  github_token: () => ["gh", "p_", "a1B2".repeat(9)].join(""),
  github_pat: () => ["github", "_pat_", "x".repeat(22)].join(""),
  sk_key: () => ["sk", "-", "abc123".repeat(4)].join(""),
  slack_token: () => ["xo", "xb-", "1234567890", "abc"].join(""),
  credential_assignment: () => `${["pass", "word"].join("")} = "${"p".repeat(14)}"`,
};
// Local excludes keep ignored fixtures out of the tracked tree, so only the ignored root differs.
const exclude = (input: QualificationInput, ...patterns: string[]) =>
  NodeFSP.appendFile(
    NodePath.join(input.expected.git_common_dir, "info/exclude"),
    patterns.join("\n") + "\n",
  );
const write = async (worktree: string, relative: string, bytes: string | Buffer) => {
  const path = NodePath.join(worktree, relative);
  await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true });
  await NodeFSP.writeFile(path, bytes);
  return path;
};
const sparse = async (worktree: string, relative: string, size: number) => {
  const path = NodePath.join(worktree, relative);
  const handle = await NodeFSP.open(path, "w");
  try {
    await handle.truncate(size);
  } finally {
    await handle.close();
  }
};

export const recoverabilityCases: Record<string, () => Promise<void>> = {
  "a clean checkout is recreatable and produces no preserve plan": () =>
    fixture(async ({ input }) => {
      const result = await qualify(input);
      NodeAssert.equal(result.outcome, "retirable");
      NodeAssert.equal(result.recoverability?.verdict, "recreatable");
      NodeAssert.deepEqual(result.recoverability?.reasons, {});
      NodeAssert.equal(result.preservePlan, undefined);
    }),
  "dirty tracked edits, deletions and renames are preservable and name exact paths": () =>
    fixture(async ({ worktree, input }) => {
      await write(worktree, "tracked", "edited\n");
      const edited = await qualify(input);
      // The verdict semantics are unchanged: the target still holds.
      NodeAssert.equal(edited.outcome, "hold");
      NodeAssert.deepEqual(edited.reasons, ["dirty_tracked"]);
      NodeAssert.deepEqual(edited.recoverability?.reasons, { dirty_tracked: "preservable" });
      NodeAssert.equal(edited.recoverability?.verdict, "retirable_after_preserve");
      NodeAssert.deepEqual(edited.preservePlan, {
        schema: "workspace-preserve-plan/v1",
        selected: ["tracked"],
        deletions: [],
        excluded: [],
        selectedBytes: "edited\n".length,
        excludedBytes: 0,
      });
      await NodeFSP.rm(NodePath.join(worktree, "tracked"));
      const deleted = await qualify(input);
      NodeAssert.deepEqual(deleted.preservePlan?.deletions, ["tracked"]);
      NodeAssert.deepEqual(deleted.preservePlan?.selected, []);
      await command(worktree, ["checkout", "--", "tracked"]);
      await command(worktree, ["mv", "tracked", "renamed"]);
      const renamed = await qualify(input);
      NodeAssert.deepEqual(renamed.preservePlan?.selected, ["renamed"]);
      NodeAssert.deepEqual(renamed.preservePlan?.deletions, ["tracked"]);
      NodeAssert.equal(renamed.recoverability?.verdict, "retirable_after_preserve");
    }),
  "untracked files are preservable, and an unreadable sensitive file is excluded without being opened":
    () =>
      fixture(async ({ worktree, input }) => {
        await write(worktree, "notes.txt", "keep\n");
        await write(worktree, ".env.example", "SAMPLE=1\n");
        const plain = await qualify(input);
        NodeAssert.deepEqual(plain.reasons, ["untracked"]);
        NodeAssert.deepEqual(plain.recoverability?.reasons, { untracked: "preservable" });
        NodeAssert.equal(plain.recoverability?.verdict, "retirable_after_preserve");
        NodeAssert.deepEqual(plain.preservePlan?.selected, [".env.example", "notes.txt"]);
        const secret = await write(worktree, ".env", "SAMPLE=2\n");
        await write(worktree, "deploy.pfx", "bytes");
        await write(worktree, ".pypirc", "[pypi]\n");
        await write(worktree, "config/aws-credentials.json", "{}\n");
        // Never opened: a read of this file would fail with EACCES and make the result unknown.
        await NodeFSP.chmod(secret, 0o000);
        const result = await qualify(input);
        NodeAssert.equal(result.outcome, "hold", JSON.stringify(result));
        NodeAssert.deepEqual(
          result.preservePlan?.excluded.map((item) => [item.path, item.reason, item.origin]),
          [
            [".env", "sensitive_name", "untracked"],
            [".pypirc", "sensitive_name", "untracked"],
            ["config/aws-credentials.json", "sensitive_name", "untracked"],
            ["deploy.pfx", "sensitive_name", "untracked"],
          ],
        );
        NodeAssert.deepEqual(result.preservePlan?.selected, [".env.example", "notes.txt"]);
        NodeAssert.deepEqual(result.recoverability?.reasons, { untracked: "unrecreatable" });
        NodeAssert.equal(result.recoverability?.verdict, "held");
      }),
  "a secret-pattern match excludes the file, records only the pattern name, and the table covers every pattern":
    () =>
      fixture(async ({ worktree, input }) => {
        for (const { name } of SECRET_PATTERNS)
          NodeAssert.equal(
            secretPatternMatch(`prefix ${fake[name as keyof typeof fake]()} suffix`),
            name,
            name,
          );
        NodeAssert.equal(secretPatternMatch("export const task = 1;\n"), null);
        const token = fake.github_token();
        await write(worktree, "config.txt", `value=${token}\n`);
        await write(worktree, "plain.txt", "nothing here\n");
        // Neither scanned: binary content, and a file over the 2 MiB scan limit.
        await write(worktree, "blob.bin", Buffer.concat([Buffer.from([0]), Buffer.from(token)]));
        await write(
          worktree,
          "large.txt",
          Buffer.concat([
            Buffer.from(token),
            Buffer.alloc(PRESERVE_LIMITS.scanBytes + 1, "a".charCodeAt(0)),
          ]),
        );
        const result = await qualify(input);
        NodeAssert.deepEqual(result.preservePlan?.excluded, [
          { path: "config.txt", reason: "secret_pattern:github_token", origin: "untracked" },
        ]);
        NodeAssert.deepEqual(result.preservePlan?.selected, ["blob.bin", "large.txt", "plain.txt"]);
        NodeAssert.equal(result.recoverability?.verdict, "held");
        NodeAssert.ok(!JSON.stringify(result).includes(token), "no matched text is recorded");
      }),
  "an edited tracked file that matches a secret pattern makes dirty_tracked unrecreatable": () =>
    fixture(async ({ worktree, input }) => {
      await write(worktree, "tracked", `${fake.sk_key()}\n`);
      const result = await qualify(input);
      NodeAssert.deepEqual(result.preservePlan?.excluded, [
        { path: "tracked", reason: "secret_pattern:sk_key", origin: "tracked" },
      ]);
      NodeAssert.deepEqual(result.recoverability?.reasons, { dirty_tracked: "unrecreatable" });
      NodeAssert.equal(result.recoverability?.verdict, "held");
    }),
  "oversized files and a set over the total bound are excluded by size": () =>
    fixture(async ({ worktree, input }) => {
      await sparse(worktree, "huge.bin", PRESERVE_LIMITS.fileBytes + 1);
      await write(worktree, "small.txt", "x");
      const single = await qualify(input);
      NodeAssert.deepEqual(single.preservePlan?.excluded, [
        { path: "huge.bin", reason: "oversized", origin: "untracked" },
      ]);
      NodeAssert.equal(single.preservePlan?.excludedBytes, PRESERVE_LIMITS.fileBytes + 1);
      NodeAssert.equal(single.preservePlan?.selectedBytes, 1);
      NodeAssert.equal(single.recoverability?.verdict, "held");
      await NodeFSP.rm(NodePath.join(worktree, "huge.bin"));
      const each = 49 * 1024 ** 2;
      for (let i = 0; i < 11; i++)
        await sparse(worktree, `part-${String(i).padStart(2, "0")}`, each);
      const total = await qualify(input);
      NodeAssert.equal(total.preservePlan?.excluded.length, 1);
      NodeAssert.equal(total.preservePlan?.excluded[0]?.reason, "oversized");
      NodeAssert.ok((total.preservePlan?.selectedBytes ?? Infinity) <= PRESERVE_LIMITS.totalBytes);
      NodeAssert.equal(total.preservePlan?.excludedBytes, each);
      NodeAssert.equal(total.recoverability?.verdict, "held");
    }),
  "an untracked nested repository is excluded by exact path": () =>
    fixture(async ({ worktree, input }) => {
      const inner = NodePath.join(worktree, "inner");
      await NodeFSP.mkdir(inner);
      await command(inner, ["init", "-b", "main"]);
      await NodeFSP.writeFile(NodePath.join(inner, "file"), "x\n");
      await command(inner, ["add", "."]);
      await command(inner, ["commit", "-m", "inner"]);
      await write(worktree, "keep.txt", "keep\n");
      const result = await qualify(input);
      NodeAssert.deepEqual(result.preservePlan?.excluded, [
        { path: "inner/", reason: "nested_repository", origin: "untracked" },
      ]);
      NodeAssert.deepEqual(result.preservePlan?.selected, ["keep.txt"]);
      NodeAssert.equal(result.recoverability?.verdict, "held");
    }),
  "an unpublished HEAD is preservable, and an excluded path elsewhere does not change its class":
    () =>
      fixture(async ({ worktree, input }) => {
        input.durableRepositories = [];
        const result = await qualify(input);
        NodeAssert.equal(result.outcome, "hold");
        NodeAssert.deepEqual(result.reasons, ["repository_durability_unproven"]);
        NodeAssert.deepEqual(result.recoverability?.reasons, {
          repository_durability_unproven: "preservable",
        });
        NodeAssert.equal(result.recoverability?.verdict, "retirable_after_preserve");
        NodeAssert.equal(result.preservePlan?.selected.length, 0);
        await write(worktree, ".npmrc", "registry=x\n");
        const mixed = await qualify(input);
        NodeAssert.deepEqual(mixed.recoverability?.reasons, {
          repository_durability_unproven: "preservable",
          untracked: "unrecreatable",
        });
        NodeAssert.equal(mixed.recoverability?.verdict, "held");
      }),
  "locks, in-progress operations and mixed holds stay held and name their class": () =>
    fixture(async ({ repo, worktree, input }) => {
      await command(repo, ["worktree", "lock", worktree]);
      const locked = await qualify(input);
      NodeAssert.deepEqual(locked.reasons, ["locked"]);
      NodeAssert.deepEqual(locked.recoverability?.reasons, { locked: "liveness" });
      NodeAssert.equal(locked.recoverability?.verdict, "held");
      await command(repo, ["worktree", "unlock", worktree]);
      await write(worktree, "tracked", "edited\n");
      const head = await command(worktree, ["rev-parse", "HEAD"]);
      await NodeFSP.writeFile(NodePath.join(input.expected.git_dir, "MERGE_HEAD"), `${head}\n`);
      const merging = await qualify(input);
      NodeAssert.deepEqual(merging.recoverability?.reasons, {
        dirty_tracked: "preservable",
        operation_in_progress: "unrecreatable",
      });
      NodeAssert.equal(merging.recoverability?.verdict, "held");
    }),
  "regenerable roots are disposable by default classes, including a 0555 Go module cache": () =>
    fixture(async ({ worktree, input }) => {
      await exclude(
        input,
        "node_modules/",
        "dist/",
        "__pycache__/",
        ".pytest_cache/",
        "**/pkg/mod/",
      );
      await write(worktree, "node_modules/pkg/index.js", "x\n");
      await write(worktree, "dist/out.js", "x\n");
      await write(worktree, "__pycache__/m.pyc", "x\n");
      await write(worktree, ".pytest_cache/v/cache", "x\n");
      await write(worktree, "build/out.o", "x\n");
      const mod = NodePath.join(worktree, "go/pkg/mod");
      const module = NodePath.join(mod, "example.com/m@v1.0.0");
      await write(worktree, "go/pkg/mod/example.com/m@v1.0.0/main.go", "package m\n");
      await NodeFSP.chmod(NodePath.join(module, "main.go"), 0o444);
      for (const directory of [module, NodePath.join(mod, "example.com"), mod])
        await NodeFSP.chmod(directory, 0o555);
      try {
        const held = await qualify(input);
        NodeAssert.equal(held.outcome, "hold");
        NodeAssert.ok(held.reasons.includes("unattested_ignored"));
        NodeAssert.equal(held.recoverability?.reasons.unattested_ignored, "unrecreatable");
        NodeAssert.equal(held.recoverability?.verdict, "held");
        NodeAssert.deepEqual(held.recoverability?.regenerable, []);
        input.coverage = { classes: ["regenerable_output"] };
        const result = await qualify(input);
        NodeAssert.equal(result.outcome, "retirable", JSON.stringify(result.ignored));
        NodeAssert.equal(result.recoverability?.verdict, "recreatable");
        const root = result.binding!.path;
        NodeAssert.deepEqual(
          [...(result.recoverability?.regenerable ?? [])].sort(),
          [
            ".pytest_cache",
            "__pycache__",
            "build",
            "dist",
            "go",
            "go/pkg",
            "go/pkg/mod",
            "node_modules",
          ]
            .map((name) => NodePath.join(root, name))
            .sort(),
        );
        // Narrowing the classes returns these roots to a hold.
        input.coverage = { classes: ["declared_cache"] };
        NodeAssert.equal((await qualify(input)).recoverability?.verdict, "held");
      } finally {
        for (const directory of [mod, NodePath.join(mod, "example.com"), module])
          await NodeFSP.chmod(directory, 0o755).catch(() => undefined);
      }
    }),
  "a dependency root with a credentials-named file stays regenerable, while build output holding a secret name is held by exact path":
    () =>
      fixture(async ({ worktree, input }) => {
        await exclude(input, "node_modules/", "dist/", "build/", ".cache/");
        input.coverage = { classes: ["regenerable_output"] };
        await write(worktree, "node_modules/pkg/credentials.js", "x\n");
        await write(worktree, "node_modules/pkg/.env", "x\n");
        const dependency = await qualify(input);
        NodeAssert.equal(dependency.outcome, "retirable", JSON.stringify(dependency.ignored));
        NodeAssert.equal(dependency.recoverability?.verdict, "recreatable");
        await write(worktree, "build/.env", "SAMPLE=1\n");
        await write(worktree, "dist/cert.pem", "x\n");
        await write(worktree, ".cache/service-credentials.json", "{}\n");
        await NodeFSP.mkdir(NodePath.join(worktree, "dist/vendor/.git"), { recursive: true });
        const root = dependency.binding!.path;
        const result = await qualify(input);
        NodeAssert.equal(result.outcome, "hold");
        const reasonOf = (name: string) =>
          result.ignored?.find((item) => item.path === NodePath.join(root, name));
        NodeAssert.equal(reasonOf("build")?.reason, "sensitive_ignored");
        NodeAssert.deepEqual(reasonOf("build")?.paths, [NodePath.join(root, "build/.env")]);
        // dist holds a nested repository, which refuses before any name is read.
        NodeAssert.equal(reasonOf("dist")?.reason, "nested_repository");
        // The broader preserve-set names do not hold a cache root.
        NodeAssert.equal(reasonOf(".cache")?.outcome, "disposable");
        NodeAssert.equal(reasonOf("node_modules")?.outcome, "disposable");
        NodeAssert.deepEqual(result.recoverability?.regenerable.sort(), [
          NodePath.join(root, ".cache"),
          NodePath.join(root, "node_modules"),
        ]);
        NodeAssert.equal(result.recoverability?.verdict, "held");
      }),
  "the durable receipt codec round-trips recoverability and still decodes older receipts": () =>
    fixture(async ({ worktree, input }) => {
      await write(worktree, "tracked", "edited\n");
      await write(worktree, ".env", "SAMPLE=1\n");
      await exclude(input, "dist/");
      await write(worktree, "dist/out.js", "x\n");
      input.coverage = { classes: ["regenerable_output"] };
      const qualification = await qualify(input);
      NodeAssert.ok(qualification.preservePlan && qualification.recoverability);
      const receipt: RetirementReceipt = {
        schema: "worktree-retirement/v1",
        id: "fixture",
        cause: "owner_action",
        state: "not_retired",
        binding: input.expected,
        incarnation: incarnation(input.expected),
        qualification,
        coverage: null,
        preservationRefs: [],
        journal: "",
        disposed: [],
        reasons: qualification.reasons,
        limits: [],
        bytes: { logical: null, allocated: null, statvfs_delta: null },
      };
      const encoded = encodeReceipt(receipt);
      const decoded = decodeReceipt(encoded);
      NodeAssert.deepEqual(decoded.qualification.preservePlan, qualification.preservePlan);
      NodeAssert.deepEqual(decoded.qualification.recoverability, qualification.recoverability);
      const legacy = JSON.parse(encoded);
      delete legacy.qualification.preservePlan;
      delete legacy.qualification.recoverability;
      const older = decodeReceipt(JSON.stringify(legacy));
      NodeAssert.equal(older.qualification.preservePlan, undefined);
      NodeAssert.equal(older.qualification.recoverability, undefined);
      legacy.qualification.recoverability = { ...qualification.recoverability, verdict: "bogus" };
      NodeAssert.throws(() => decodeReceipt(JSON.stringify(legacy)));
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
  "native reftable object-validation failure remains unknown without partial tips or a copied view":
    () =>
      fixture(async ({ worktree, input }) => {
        await command(worktree, ["checkout", "--detach"]);
        await command(worktree, ["commit", "--allow-empty", "-m", "orphan fixture"]);
        await command(worktree, ["checkout", "task"]);
        const child = NodeModule.createRequire(import.meta.url)(
          "node:child_process",
        ) as typeof import("node:child_process");
        const original = child.execFile;
        const commands = NodeTest.mock.method(child, "execFile", (...args: unknown[]) => {
          if ((args[1] as string[]).includes("cat-file")) {
            (args[3] as (error: Error | null, stdout: Buffer, stderr: Buffer) => void)(
              Object.assign(new Error("injected object unavailable"), { code: 1 }),
              Buffer.alloc(0),
              Buffer.alloc(0),
            );
            return;
          }
          return Reflect.apply(original, child, args);
        });
        NodeModule.syncBuiltinESMExports();
        try {
          const result = await qualify(input);
          NodeAssert.equal(result.outcome, "unknown", JSON.stringify(result));
          NodeAssert.deepEqual(result.observationFailure, {
            stage: "candidate_reflog",
            errorClass: "native_command",
          });
          NodeAssert.equal(result.preservationTips, undefined);
          NodeAssert.ok(
            !commands.mock.calls.some(({ arguments: args }) =>
              (args[1] as string[]).some((arg) => arg.startsWith("--git-dir=")),
            ),
          );
        } finally {
          commands.mock.restore();
          NodeModule.syncBuiltinESMExports();
        }
      }, "reftable"),
  "SC-C4 success-shaped incomplete native enumeration cannot subtract decoded history or authorize a failed read":
    () =>
      fixture(async ({ worktree, input }) => {
        await command(worktree, ["checkout", "--detach"]);
        await command(worktree, ["commit", "--allow-empty", "-m", "candidate orphan"]);
        const orphan = await command(worktree, ["rev-parse", "HEAD"]);
        await command(worktree, ["checkout", "task"]);
        const head = await command(worktree, ["rev-parse", "HEAD"]);
        const child = NodeModule.createRequire(import.meta.url)(
          "node:child_process",
        ) as typeof import("node:child_process");
        const original = child.execFile;
        let incompleteCalls = 0;
        const commands = NodeTest.mock.method(child, "execFile", (...args: unknown[]) => {
          if ((args[1] as string[]).includes("rev-list")) {
            incompleteCalls++;
            (args[3] as (error: Error | null, stdout: Buffer, stderr: Buffer) => void)(
              null,
              Buffer.from(head + "\n"),
              Buffer.alloc(0),
            );
            return;
          }
          return Reflect.apply(original, child, args);
        });
        NodeModule.syncBuiltinESMExports();
        try {
          const proposal = await qualify(input);
          NodeAssert.equal(proposal.outcome, "retirable", JSON.stringify(proposal));
          NodeAssert.deepEqual(proposal.preservationTips, [orphan]);
          NodeAssert.equal(incompleteCalls, 0);
          const fs = NodeModule.createRequire(import.meta.url)(
            "node:fs/promises",
          ) as typeof import("node:fs/promises");
          const open = fs.open;
          const failedRead = NodeTest.mock.method(fs, "open", (...args: unknown[]) => {
            if (String(args[0]).endsWith("/tables.list"))
              return Promise.reject(
                Object.assign(new Error("injected read failure"), { code: "EIO" }),
              );
            return Reflect.apply(open, fs, args);
          });
          NodeModule.syncBuiltinESMExports();
          try {
            const failed = await finalCheck(input, proposal);
            NodeAssert.equal(failed.conflict, true);
            NodeAssert.equal(failed.result.outcome, "unknown");
            NodeAssert.equal(failed.result.preservationTips, undefined);
            NodeAssert.equal(incompleteCalls, 0);
          } finally {
            failedRead.mock.restore();
            NodeModule.syncBuiltinESMExports();
          }
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
          NodeAssert.equal(view.length, 0);
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
