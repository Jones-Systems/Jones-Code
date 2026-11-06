// @effect-diagnostics nodeBuiltinImport:off -- Task-owned synthetic filesystem/SQLite fixtures; every fixture has exact finally cleanup.
import assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { spawnSync } from "node:child_process";
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
        const root = Path.join(worktree, "build");
        await FS.mkdir(root);
        const directory = await openAbsolute(root, true);
        try {
          const copy = await openRelative(directory, "", true);
          try {
            assert.equal((await copy.stat()).ino, (await directory.stat()).ino);
            assert.notEqual(copy.fd, directory.fd);
          } finally {
            await copy.close();
          }
          await FS.symlink(base, Path.join(root, "escape"));
          await assert.rejects(openRelative(directory, "escape", true));
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
        assert.deepEqual(
          manifestBytes(entries)
            .toString()
            .trim()
            .split("\n")
            .map((line) => JSON.parse(line).relpath),
          ["\ue000", "😀"],
        );
        const root = Path.join(worktree, "build");
        await FS.mkdir(root);
        await FS.symlink(Buffer.from([255]), Path.join(root, "invalid-target"));
        assert.equal((await contentManifest(root)).observation.verification, "unverifiable");
      }),
  "two-pass proof binds additions, modes, targets, immutable baselines and incarnation": async () =>
    fixture(async ({ base, worktree, input }) => {
      const root = Path.join(worktree, "build");
      await FS.mkdir(root);
      await FS.writeFile(Path.join(root, "a"), "abc");
      await FS.symlink(Path.join(base, "never-read"), Path.join(root, "link"));
      const record = await attest({
        worktree,
        root,
        producer: "fixture",
        status: "succeeded",
        participantGuard,
      });
      assert.equal((await verify(record.sidecar, worktree)).verification, "content");
      const second = await attest({
        worktree,
        root,
        producer: "fixture",
        status: "failed",
        participantGuard,
      });
      assert.notEqual(record.sidecar, second.sidecar);
      assert.equal(second.status, "failed");
      await FS.chmod(Path.join(root, "a"), 0o600);
      assert.equal((await verify(record.sidecar, worktree)).verification, "mismatch");
      await FS.chmod(Path.join(root, "a"), 0o644);
      await FS.unlink(Path.join(root, "link"));
      await FS.symlink("changed-target", Path.join(root, "link"));
      assert.equal((await verify(record.sidecar, worktree)).verification, "mismatch");
      await FS.writeFile(
        Path.join(input.expected.git_dir, "workspace-provenance.json"),
        canonical({ incarnation_id: "new-incarnation" }),
      );
      assert.equal((await verify(record.sidecar, worktree)).reason, "identity_mismatch");
    }),
  "protection scan refuses unreadable sensitive files before hashing and cannot follow directory symlinks":
    async () =>
      fixture(async ({ base, worktree, input }) => {
        const root = Path.join(worktree, "build");
        await FS.mkdir(root);
        await FS.writeFile(Path.join(root, "a"), "output");
        await FS.writeFile(Path.join(root, ".env"), "synthetic protected fixture", { mode: 0 });
        const proof = await contentManifest(root);
        assert.deepEqual(proof.observation.reasons, ["sensitive_ignored"]);
        assert.equal(proof.payload.length, 0);
        const result = await classifyIgnored({
          worktree,
          root,
          common: input.expected.git_common_dir,
          coverage: { exactRoots: [root], classes: ["declared_cache"] },
        });
        assert.equal(result.reason, "sensitive_ignored");
        await FS.mkdir(Path.join(root, "nested"));
        await FS.symlink(Path.join(base, "outside"), Path.join(root, "nested/.git"));
        assert.equal(
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
        const linked = Path.join(worktree, "symlink-output");
        await FS.symlink(root, linked);
        assert.equal((await contentManifest(linked)).observation.verification, "unverifiable");
      }),
  "recognized roots still protect registered and bare common stores": async () =>
    fixture(async ({ base, worktree, input }) => {
      const root = Path.join(worktree, "cache"),
        bare = Path.join(root, "store.git");
      await FS.mkdir(root);
      await command(root, ["init", "--bare", bare]);
      const classified = await classifyIgnored({
        worktree,
        root,
        common: input.expected.git_common_dir,
        coverage: { exactRoots: [root] },
        registered: [{ path: Path.join(base, "external-checkout"), common: bare }],
      });
      assert.equal(classified.reason, "shared_common_store");
      assert.equal(
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
      assert.ok((await contentManifest(root)).observation.reasons.includes("nested_repository"));
    }),
  "bounds may shorten but cannot expand and malformed evidence cannot become a baseline":
    async () =>
      fixture(async ({ worktree }) => {
        const root = Path.join(worktree, "build");
        await FS.mkdir(root);
        await FS.writeFile(Path.join(root, "a"), "abc");
        assert.ok(
          (await contentManifest(root, { bytes: 2 })).observation.reasons.includes(
            "bound_exceeded",
          ),
        );
        await assert.rejects(contentManifest(root, { seconds: 301 }), /invalid_bounds/);
        const record = await attest({
          worktree,
          root,
          producer: "fixture",
          status: "cancelled",
          participantGuard,
        });
        assert.equal(record.status, "cancelled");
        await FS.appendFile(record.sidecar.replace(/\.json$/, ".manifest.jsonl"), "altered\n");
        assert.equal((await verify(record.sidecar, worktree)).verification, "mismatch");
        assert.equal(
          (await verify(record.sidecar + ".absent", worktree)).verification,
          "unverifiable",
        );
      }),
  "cheap finalization records ownership and all terminal outcomes without hashing or proving disposability":
    async () =>
      fixture(async ({ worktree }) => {
        const root = Path.join(worktree, "node_modules");
        await FS.mkdir(root);
        await FS.writeFile(Path.join(root, ".env"), "synthetic", { mode: 0 });
        for (const status of ["succeeded", "failed", "cancelled"] as const) {
          const session = await beginProduction(worktree, "fixture", [root]);
          const result = await finishProduction(session, status);
          assert.equal(result.status, status);
          assert.equal(result.owner, "fixture");
          assert.equal(result.outcome, "retained");
          assert.equal(result.proofStatus, "deferred");
          assert.deepEqual(result.sidecars, []);
          assert.deepEqual(result.reasons, ["content_verification_deferred"]);
          assert.equal(
            await FS.stat(Path.join(session.record.binding.git_dir, "workspace-outputs")).catch(
              () => null,
            ),
            null,
          );
          await assert.rejects(finishProduction(session, status), /already_finalized/);
        }
        assert.deepEqual(await outputObligations(worktree), []);
      }),
  "explicit producer finalization captures content, unknown effects remain durable and target-local":
    async () =>
      fixture(async ({ worktree, input }) => {
        const root = Path.join(worktree, "build");
        await FS.mkdir(root);
        await FS.writeFile(Path.join(root, "a"), "abc");
        const session = await beginProduction(worktree, "fixture", [root]);
        const result = await finishProduction(session, "failed", [], { verifyContent: true });
        assert.equal(result.outcome, "retained");
        assert.equal(result.sidecars.length, 1);
        assert.equal(result.proofStatus, "attested");
        assert.equal((await verify(result.sidecars[0]!, worktree)).verification, "content");
        const unfinished = await beginProduction(worktree, "unknown-script", []);
        await finishProduction(unfinished, "cancelled");
        assert.equal((await outputObligations(worktree))[0]?.status, "cancelled");
        assert.ok(
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
          const path = Path.join(bound.git_common_dir, "workspace-lifecycle/reservations.sqlite3");
          const attempt = spawnSync(
            process.execPath,
            [
              "--input-type=module",
              "-e",
              `import {DatabaseSync} from 'node:sqlite'; const db=new DatabaseSync(process.argv[1]);try{db.exec('PRAGMA busy_timeout=0; BEGIN IMMEDIATE');process.exitCode=3;}catch{process.stdout.write('locked');}finally{db.close();}`,
              path,
            ],
            { encoding: "utf8", timeout: 3000 },
          );
          assert.equal(attempt.status, 0, attempt.stderr);
          assert.equal(attempt.stdout, "locked");
        });
        const path = Path.join(bound.git_common_dir, "workspace-lifecycle/reservations.sqlite3"),
          db = new DatabaseSync(path);
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
        await assert.rejects(
          participantGuard(bound, async () => {
            wrote = true;
          }),
          /retiring/,
        );
        assert.equal(wrote, false);
        await assert.rejects(beginProduction(worktree, "fixture", []), /retiring/);
        assert.equal(
          await FS.stat(Path.join(bound.git_dir, "workspace-output-obligations")).catch(() => null),
          null,
        );
      }),
  "guard passes absent common stores without creating and refuses malformed stores": async () =>
    fixture(async ({ base, input }) => {
      const absent = Path.join(base, "absent-common");
      assert.equal(
        await participantGuard({ ...input.expected, git_common_dir: absent }, async () => 42),
        42,
      );
      assert.equal(await FS.stat(absent).catch(() => null), null);
      const parent = Path.join(input.expected.git_common_dir, "workspace-lifecycle");
      await FS.mkdir(parent);
      await FS.writeFile(Path.join(parent, "reservations.sqlite3"), "malformed store");
      await assert.rejects(participantGuard(input.expected, async () => 42));
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
        assert.deepEqual(decision.unknown, ["consumer_evidence_unavailable"]);
        assert.equal(decision.coverage.services?.reason, "incomplete_services_coverage");
        assert.equal(
          consumerDecision(worktree, completeConsumers(0), 120001).coverage.native?.reason,
          "stale_or_invalid_observation",
        );
        const file = Path.join(base, "fixture.service");
        await FS.writeFile(
          file,
          "ExecStart=" + worktree + "/bin/server --synthetic-secret=do-not-emit",
        );
        const services = await serviceProbe(worktree, [file]);
        assert.equal(services.status, "partial");
        assert.deepEqual(services.paths, [worktree]);
        assert.ok(!JSON.stringify(services).includes("do-not-emit"));
        const unsupported = await processProbe(worktree, "win32");
        assert.equal(unsupported.status, "unavailable");
        assert.ok(unsupported.limits.includes("unsupported platform"));
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
        assert.equal(probe.status, "complete");
        assert.ok(probe.paths.includes(target));
        assert.ok(probe.paths.includes(target + "/mapped"));
        assert.ok(!JSON.stringify(probe).includes("synthetic-do-not-emit"));
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
        assert.equal(failed.status, "partial");
      }
    },
  "final check detects a new ignored root and stale bindings without acquiring removal authority":
    async () =>
      fixture(async ({ worktree, input }) => {
        const initial = await qualify(input);
        assert.equal(initial.outcome, "retirable");
        await FS.mkdir(Path.join(worktree, "build"));
        await FS.writeFile(Path.join(worktree, "build/note"), "unique");
        assert.equal((await finalCheck(input, initial)).conflict, true);
        const stale = await qualify({
          ...input,
          expected: { ...input.expected, ino: input.expected.ino + 1 },
        });
        assert.deepEqual(stale.reasons, ["identity_mismatch"]);
        assert.equal(stale.authority_effect, "none");
        assert.equal((await binding(worktree)).ino, input.expected.ino);
      }),
  "setup unknown effects survive missing producer records": async () =>
    fixture(async ({ worktree, input }) => {
      await FS.mkdir(Path.join(worktree, ".t3"));
      await FS.writeFile(Path.join(worktree, ".t3/setup-unfinalized"), "setup_unfinalized\n");
      assert.ok((await qualify(input)).reasons.includes("setup_unfinalized"));
    }),
};
