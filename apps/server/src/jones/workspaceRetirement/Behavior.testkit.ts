// @effect-diagnostics nodeBuiltinImport:off -- Task-owned synthetic filesystem/SQLite fixtures; every fixture has exact finally cleanup.
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeChildProcess from "node:child_process";
import { fixture, command, completeConsumers } from "./Conformance.testkit.ts";
import { canonical, openAbsolute, openRelative } from "./Filesystem.ts";
import { attest, binding, contentManifest, manifestBytes, verify } from "./OutputAttestation.ts";
import { classifyIgnored } from "./IgnoredClassifier.ts";
import { participantGuard } from "./ParticipantGuard.ts";
import { beginProduction, finishProduction, outputObligations } from "./ProducerFinalizer.ts";
import { qualify, finalCheck } from "./Qualification.ts";
import { consumerDecision, processProbe, serviceProbe } from "./ConsumerProbe.ts";

export const behaviorCases: Record<string, () => Promise<void>> = {
  "descriptor-relative root duplication retains the directory and rejects symlink entries":
    async () =>
      fixture(async ({ base, worktree }) => {
        const root = NodePath.join(worktree, "build");
        await NodeFSP.mkdir(root);
        const directory = await openAbsolute(root, true);
        try {
          const copy = await openRelative(directory, "", true);
          try {
            NodeAssert.equal((await copy.stat()).ino, (await directory.stat()).ino);
            NodeAssert.notEqual(copy.fd, directory.fd);
          } finally {
            await copy.close();
          }
          await NodeFSP.symlink(base, NodePath.join(root, "escape"));
          await NodeAssert.rejects(openRelative(directory, "escape", true));
        } finally {
          await directory.close();
        }
      }),
  "wire ordering matches Unicode scalar order and invalid symlink bytes never become proof":
    async () =>
      fixture(async ({ worktree }) => {
        const entries = ["😀", "\ue000"].map((relpath) => ({
          relpath,
          type: "file",
          size: 0,
          mode: 420,
          sha256: "0".repeat(64),
        }));
        NodeAssert.deepEqual(
          manifestBytes(entries)
            .toString()
            .trim()
            .split("\n")
            .map((line) => JSON.parse(line).relpath),
          ["\ue000", "😀"],
        );
        const root = NodePath.join(worktree, "build");
        await NodeFSP.mkdir(root);
        await NodeFSP.symlink(Buffer.from([255]), NodePath.join(root, "invalid-target"));
        NodeAssert.equal((await contentManifest(root)).observation.verification, "unverifiable");
      }),
  "two-pass proof binds additions, modes, targets, immutable baselines and incarnation": async () =>
    fixture(async ({ base, worktree, input }) => {
      const root = NodePath.join(worktree, "build");
      await NodeFSP.mkdir(root);
      await NodeFSP.writeFile(NodePath.join(root, "a"), "abc");
      // This scenario changes an authenticated 0644 baseline to 0600 even under a private umask.
      await NodeFSP.chmod(NodePath.join(root, "a"), 0o644);
      NodeAssert.equal((await NodeFSP.lstat(NodePath.join(root, "a"))).mode & 0o777, 0o644);
      await NodeFSP.symlink(NodePath.join(base, "never-read"), NodePath.join(root, "link"));
      const record = await attest({
        worktree,
        root,
        producer: "fixture",
        status: "succeeded",
        participantGuard,
      });
      NodeAssert.equal((await verify(record.sidecar, worktree)).verification, "content");
      const second = await attest({
        worktree,
        root,
        producer: "fixture",
        status: "failed",
        participantGuard,
      });
      NodeAssert.notEqual(record.sidecar, second.sidecar);
      NodeAssert.equal(second.status, "failed");
      await NodeFSP.chmod(NodePath.join(root, "a"), 0o600);
      NodeAssert.equal((await verify(record.sidecar, worktree)).verification, "mismatch");
      await NodeFSP.chmod(NodePath.join(root, "a"), 0o644);
      await NodeFSP.unlink(NodePath.join(root, "link"));
      await NodeFSP.symlink("changed-target", NodePath.join(root, "link"));
      NodeAssert.equal((await verify(record.sidecar, worktree)).verification, "mismatch");
      await NodeFSP.writeFile(
        NodePath.join(input.expected.git_dir, "workspace-provenance.json"),
        canonical({ incarnation_id: "new-incarnation" }),
      );
      NodeAssert.equal((await verify(record.sidecar, worktree)).reason, "identity_mismatch");
    }),
  "protection scan refuses unreadable sensitive files before hashing and cannot follow directory symlinks":
    async () =>
      fixture(async ({ base, worktree, input }) => {
        const root = NodePath.join(worktree, "build");
        await NodeFSP.mkdir(root);
        await NodeFSP.writeFile(NodePath.join(root, "a"), "output");
        await NodeFSP.writeFile(NodePath.join(root, ".env"), "synthetic protected fixture", {
          mode: 0,
        });
        const proof = await contentManifest(root);
        NodeAssert.deepEqual(proof.observation.reasons, ["sensitive_ignored"]);
        NodeAssert.equal(proof.payload.length, 0);
        const result = await classifyIgnored({
          worktree,
          root,
          common: input.expected.git_common_dir,
          coverage: { exactRoots: [root], classes: ["declared_cache"] },
        });
        NodeAssert.equal(result.reason, "sensitive_ignored");
        await NodeFSP.mkdir(NodePath.join(root, "nested"));
        await NodeFSP.symlink(NodePath.join(base, "outside"), NodePath.join(root, "nested/.git"));
        NodeAssert.equal(
          (
            await classifyIgnored({
              worktree,
              root,
              common: input.expected.git_common_dir,
              coverage: { exactRoots: [root] },
            })
          ).reason,
          "nested_repository",
        );
        const linked = NodePath.join(worktree, "symlink-output");
        await NodeFSP.symlink(root, linked);
        NodeAssert.equal((await contentManifest(linked)).observation.verification, "unverifiable");
      }),
  "recognized roots still protect registered and bare common stores": async () =>
    fixture(async ({ base, worktree, input }) => {
      const root = NodePath.join(worktree, "cache"),
        bare = NodePath.join(root, "store.git");
      await NodeFSP.mkdir(root);
      await command(root, ["init", "--bare", bare]);
      const classified = await classifyIgnored({
        worktree,
        root,
        common: input.expected.git_common_dir,
        coverage: { exactRoots: [root] },
        registered: [{ path: NodePath.join(base, "external-checkout"), common: bare }],
      });
      NodeAssert.equal(classified.reason, "shared_common_store");
      NodeAssert.equal(
        (
          await classifyIgnored({
            worktree,
            root,
            common: input.expected.git_common_dir,
            coverage: { exactRoots: [root] },
          })
        ).reason,
        "nested_repository",
      );
      NodeAssert.ok(
        (await contentManifest(root)).observation.reasons.includes("nested_repository"),
      );
    }),
  "bounds may shorten but cannot expand and malformed evidence cannot become a baseline":
    async () =>
      fixture(async ({ worktree }) => {
        const root = NodePath.join(worktree, "build");
        await NodeFSP.mkdir(root);
        await NodeFSP.writeFile(NodePath.join(root, "a"), "abc");
        NodeAssert.ok(
          (await contentManifest(root, { bytes: 2 })).observation.reasons.includes(
            "bound_exceeded",
          ),
        );
        await NodeAssert.rejects(contentManifest(root, { seconds: 301 }), /invalid_bounds/);
        const record = await attest({
          worktree,
          root,
          producer: "fixture",
          status: "cancelled",
          participantGuard,
        });
        NodeAssert.equal(record.status, "cancelled");
        await NodeFSP.appendFile(record.sidecar.replace(/\.json$/, ".manifest.jsonl"), "altered\n");
        NodeAssert.equal((await verify(record.sidecar, worktree)).verification, "mismatch");
        NodeAssert.equal(
          (await verify(record.sidecar + ".absent", worktree)).verification,
          "unverifiable",
        );
      }),
  "cheap finalization records ownership and all terminal outcomes without hashing or proving disposability":
    async () =>
      fixture(async ({ worktree }) => {
        const root = NodePath.join(worktree, "node_modules");
        await NodeFSP.mkdir(root);
        await NodeFSP.writeFile(NodePath.join(root, ".env"), "synthetic", { mode: 0 });
        for (const status of ["succeeded", "failed", "cancelled"] as const) {
          const session = await beginProduction(worktree, "fixture", [root]);
          const result = await finishProduction(session, status);
          NodeAssert.equal(result.status, status);
          NodeAssert.equal(result.owner, "fixture");
          NodeAssert.equal(result.outcome, "retained");
          NodeAssert.equal(result.proofStatus, "deferred");
          NodeAssert.deepEqual(result.sidecars, []);
          NodeAssert.deepEqual(result.reasons, ["content_verification_deferred"]);
          NodeAssert.equal(
            await NodeFSP.stat(
              NodePath.join(session.record.binding.git_dir, "workspace-outputs"),
            ).catch(() => null),
            null,
          );
          await NodeAssert.rejects(finishProduction(session, status), /already_finalized/);
        }
        NodeAssert.deepEqual(await outputObligations(worktree), []);
      }),
  "explicit producer finalization captures content, unknown effects remain durable and target-local":
    async () =>
      fixture(async ({ worktree, input }) => {
        const root = NodePath.join(worktree, "build");
        await NodeFSP.mkdir(root);
        await NodeFSP.writeFile(NodePath.join(root, "a"), "abc");
        const session = await beginProduction(worktree, "fixture", [root]);
        const result = await finishProduction(session, "failed", [], { verifyContent: true });
        NodeAssert.equal(result.outcome, "retained");
        NodeAssert.equal(result.sidecars.length, 1);
        NodeAssert.equal(result.proofStatus, "attested");
        NodeAssert.equal((await verify(result.sidecars[0]!, worktree)).verification, "content");
        const unfinished = await beginProduction(worktree, "unknown-script", []);
        await finishProduction(unfinished, "cancelled");
        NodeAssert.equal((await outputObligations(worktree))[0]?.status, "cancelled");
        NodeAssert.ok(
          (
            await qualify({ ...input, attestations: { [root]: result.sidecars[0]! } })
          ).reasons.includes("obligation"),
        );
      }),
  "guard holds its transaction through writes and any target reservation refuses publication":
    async () =>
      fixture(async ({ worktree, input }) => {
        const bound = input.expected;
        await participantGuard(bound, async () => {
          const path = NodePath.join(
            bound.git_common_dir,
            "workspace-lifecycle/reservations.sqlite3",
          );
          const attempt = NodeChildProcess.spawnSync(
            process.execPath,
            [
              "--input-type=module",
              "-e",
              `import {DatabaseSync} from 'node:sqlite'; const db=new DatabaseSync(process.argv[1]);try{db.exec('PRAGMA busy_timeout=0; BEGIN IMMEDIATE');process.exitCode=3;}catch{process.stdout.write('locked');}finally{db.close();}`,
              path,
            ],
            { encoding: "utf8", timeout: 3000 },
          );
          NodeAssert.equal(attempt.status, 0, attempt.stderr);
          NodeAssert.equal(attempt.stdout, "locked");
        });
        const path = NodePath.join(
            bound.git_common_dir,
            "workspace-lifecycle/reservations.sqlite3",
          ),
          db = new NodeSqlite.DatabaseSync(path);
        try {
          db.exec("BEGIN IMMEDIATE");
          db.prepare("INSERT INTO reservations VALUES(?,?,?)").run(
            bound.admin_id,
            "different-incarnation",
            "malformed row still excludes",
          );
          db.exec("COMMIT");
        } finally {
          db.close();
        }
        let wrote = false;
        await NodeAssert.rejects(
          participantGuard(bound, async () => {
            wrote = true;
          }),
          /retiring/,
        );
        NodeAssert.equal(wrote, false);
        await NodeAssert.rejects(beginProduction(worktree, "fixture", []), /retiring/);
        NodeAssert.equal(
          await NodeFSP.stat(NodePath.join(bound.git_dir, "workspace-output-obligations")).catch(
            () => null,
          ),
          null,
        );
      }),
  "guard passes absent common stores without creating and refuses malformed stores": async () =>
    fixture(async ({ base, input }) => {
      const absent = NodePath.join(base, "absent-common");
      NodeAssert.equal(
        await participantGuard({ ...input.expected, git_common_dir: absent }, async () => 42),
        42,
      );
      NodeAssert.equal(await NodeFSP.stat(absent).catch(() => null), null);
      const parent = NodePath.join(input.expected.git_common_dir, "workspace-lifecycle");
      await NodeFSP.mkdir(parent);
      await NodeFSP.writeFile(NodePath.join(parent, "reservations.sqlite3"), "malformed store");
      await NodeAssert.rejects(participantGuard(input.expected, async () => 42));
    }),
  "consumer evidence names the missing adapter, freshness and platform limits without retaining argv":
    async () =>
      fixture(async ({ worktree, base }) => {
        const consumers = completeConsumers(1000);
        consumers.services = {
          ...consumers.services,
          status: "partial",
          limits: ["loaded services not observed"],
        };
        const decision = consumerDecision(worktree, consumers, 1000);
        NodeAssert.deepEqual(decision.unknown, ["consumer_evidence_unavailable"]);
        NodeAssert.equal(decision.coverage.services?.reason, "incomplete_services_coverage");
        NodeAssert.equal(
          consumerDecision(worktree, completeConsumers(0), 120001).coverage.native?.reason,
          "stale_or_invalid_observation",
        );
        const file = NodePath.join(base, "fixture.service");
        await NodeFSP.writeFile(
          file,
          "ExecStart=" + worktree + "/bin/server --synthetic-secret=do-not-emit",
        );
        const services = await serviceProbe(worktree, [file]);
        NodeAssert.equal(services.status, "partial");
        NodeAssert.deepEqual(services.paths, [worktree]);
        NodeAssert.ok(!JSON.stringify(services).includes("do-not-emit"));
        const unsupported = await processProbe(worktree, "win32");
        NodeAssert.equal(unsupported.status, "unavailable");
        NodeAssert.ok(unsupported.limits.includes("unsupported platform"));
      }),
  "Linux and Darwin process adapters report cwd, executable text and argv with partial coverage on failures":
    async () => {
      const target = "/fixture/checkout";
      const io = {
        uid: () => 501,
        list: async () => ["1", "2"],
        statUid: async () => 501,
        readlink: async (path: string) =>
          path.endsWith("cwd") ? target + "/working" : target + "/executable",
        read: async (path: string) =>
          path.endsWith("cmdline")
            ? "server\0--cwd=" + target + "/source\0--token=synthetic-do-not-emit"
            : "1000-2000 r-xp 0000 00:00 1 " + target + "/mapped\n",
        command: async (file: string, args: string[]) =>
          file.endsWith("lsof")
            ? "p1\nn" + target + "/working\nn" + target + "/mapped\n"
            : args.includes("-axo")
              ? "1 501\n2 0"
              : "server --cwd=" + target + "/source --token=synthetic-do-not-emit",
      };
      for (const platform of ["linux", "darwin"] as const) {
        const probe = await processProbe(target, platform, io);
        NodeAssert.equal(probe.status, "complete");
        NodeAssert.ok(probe.paths.includes(target));
        NodeAssert.ok(probe.paths.includes(target + "/mapped"));
        NodeAssert.ok(!JSON.stringify(probe).includes("synthetic-do-not-emit"));
        const failed = await processProbe(target, platform, {
          ...io,
          read: async () => {
            throw Error("unreadable");
          },
          command: async (file, args) => {
            if (file.endsWith("lsof")) throw Error("unavailable");
            return io.command(file, args);
          },
        });
        NodeAssert.equal(failed.status, "partial");
      }
    },
  "final check detects a new ignored root and stale bindings without acquiring removal authority":
    async () =>
      fixture(async ({ worktree, input }) => {
        const initial = await qualify(input);
        NodeAssert.equal(initial.outcome, "retirable");
        await NodeFSP.mkdir(NodePath.join(worktree, "build"));
        await NodeFSP.writeFile(NodePath.join(worktree, "build/note"), "unique");
        NodeAssert.equal((await finalCheck(input, initial)).conflict, true);
        const stale = await qualify({
          ...input,
          expected: { ...input.expected, ino: input.expected.ino + 1 },
        });
        NodeAssert.deepEqual(stale.reasons, ["identity_mismatch"]);
        NodeAssert.equal(stale.authority_effect, "none");
        NodeAssert.equal((await binding(worktree)).ino, input.expected.ino);
      }),
  "setup unknown effects survive missing producer records": async () =>
    fixture(async ({ worktree, input }) => {
      await NodeFSP.mkdir(NodePath.join(worktree, ".t3"));
      await NodeFSP.writeFile(
        NodePath.join(worktree, ".t3/setup-unfinalized"),
        "setup_unfinalized\n",
      );
      NodeAssert.ok((await qualify(input)).reasons.includes("setup_unfinalized"));
    }),
};
