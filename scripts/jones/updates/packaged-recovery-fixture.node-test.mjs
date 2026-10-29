import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as NodeTest from "node:test";
import * as NodeChildProcess from "node:child_process";
import * as NodeURL from "node:url";
import { seedPackagedRecoveryFixture, legacyRefusalCases } from "./packaged-recovery-fixture.mjs";
import {
  qualifiedPayloadDigest,
  verifyStagedQualifiedRuntime,
} from "../../../apps/server/src/jones/cloud/qualifiedRuntime.ts";
import {
  readOperationReservation,
  evaluateLegacyUpdateSettlement,
  readUpdateOperationSettlement,
  writeFailedUpdateOperation,
} from "../../../apps/server/src/jones/updates/launcherOperation.ts";

const linux = {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone fixture binds its actual host without loading application services.
  skip: process.platform === "linux" ? false : "Packaged Linux recovery fixture requires Linux",
};
const operationId = "12345678-1234-4234-8234-123456789abc";

async function fixture(body) {
  const parent = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-packaged-recovery-inputs-")),
  );
  try {
    await NodeFSP.chmod(parent, 0o700);
    const root = NodePath.join(parent, "jones-packaged-recovery-case");
    await NodeFSP.mkdir(root, { mode: 0o700 });
    async function payload(name, source, installedSource) {
      const payloadDirectory = NodePath.join(parent, name);
      await NodeFSP.mkdir(NodePath.join(payloadDirectory, "client"), {
        recursive: true,
        mode: 0o700,
      });
      const marker = NodePath.join(parent, "unexpected-execution");
      await NodeFSP.writeFile(
        NodePath.join(payloadDirectory, "t3"),
        "#!/bin/sh\nprintf executed > '" + marker + "'\n",
        { mode: 0o700 },
      );
      await NodeFSP.writeFile(NodePath.join(payloadDirectory, "client", "index.html"), name, {
        mode: 0o600,
      });
      const receipt = {
        protocol: 1,
        repository: "Jones-Systems/Jones-Code",
        channel: "jones-main",
        version: name === "active" ? "0.0.0-preview.20261002.100" : "0.0.0-preview.20261002.101.1",
        sourceSha: source.repeat(40),
        sourceTree: "b".repeat(40),
        installedSourceSha: installedSource.repeat(40),
        runId: name === "active" ? 100 : 101,
        runAttempt: 1,
        artifactId: name === "active" ? 200 : 201,
        workflow: ".github/workflows/artifact-cli-linux.yml",
        artifactDigest: "sha256:" + "d".repeat(64),
        archiveSha256: "e".repeat(64),
        platform: "linux",
        // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone fixture binds its actual host without loading application services.
        architecture: process.arch,
        payloadSha256: await qualifiedPayloadDigest(payloadDirectory),
      };
      return { payloadDirectory, receipt };
    }
    const active = await payload("active", "c", "f");
    const candidate = await payload("candidate", "a", "c");
    await body({ parent, root, active, candidate, operationId });
    await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(parent, "unexpected-execution")), {
      code: "ENOENT",
    });
  } finally {
    await NodeFSP.rm(parent, { recursive: true });
    await NodeAssert.rejects(NodeFSP.lstat(parent), { code: "ENOENT" });
  }
}

NodeTest.test(
  "seeds exact pending recovery inputs without executing or rewriting the payloads",
  linux,
  async () => {
    await fixture(async (input) => {
      const original = await NodeFSP.readFile(
        NodePath.join(input.candidate.payloadDirectory, "t3"),
      );
      const result = await seedPackagedRecoveryFixture(input);
      NodeAssert.deepEqual(result.evidence, {
        artifactExecuted: false,
        packagedAcceptance: false,
        stagingMeasuredInPackagedWorker: false,
      });
      NodeAssert.deepEqual(
        await NodeFSP.readFile(NodePath.join(input.candidate.payloadDirectory, "t3")),
        original,
      );
      NodeAssert.deepEqual(result.staged.receipt, input.candidate.receipt);
      NodeAssert.equal(
        result.launcher.sha256,
        NodeCrypto.createHash("sha256").update(original).digest("hex"),
      );
      const base = result.baseDir;
      const state = JSON.parse(
        await NodeFSP.readFile(NodePath.join(base, "runtime", "service-state.json")),
      );
      NodeAssert.equal(state.update.id, operationId);
      NodeAssert.equal(state.update.phase, "accepted");
      NodeAssert.equal(state.update.status, "pending");
      const reservation = await readOperationReservation(base, operationId);
      NodeAssert.equal(reservation.binding.environmentId, result.staged.binding.environmentId);
      NodeAssert.equal(reservation.binding.stagedHandle, result.staged.stagedHandle);
      const verified = await verifyStagedQualifiedRuntime(
        base,
        input.active.receipt.version,
        result.staged.stagedHandle,
      );
      NodeAssert.deepEqual(verified, result.staged);
      const pair = JSON.parse(
        await NodeFSP.readFile(
          NodePath.join(base, "runtime", "db-backup", operationId, "paired-binding.json"),
        ),
      );
      NodeAssert.deepEqual(pair, { transactionId: operationId, qualified: result.staged });
      NodeAssert.equal(
        (await NodeFSP.stat(NodePath.join(base, "userdata", "statev2.sqlite"))).mode & 0o077,
        0,
      );
      NodeAssert.equal(await readUpdateOperationSettlement(base, operationId), undefined);
    });
  },
);

NodeTest.test(
  "seeds legacy failed variants without fabricating a settlement or changing outcome history",
  linux,
  async () => {
    for (const archivedOutcome of [false, true]) {
      await fixture(async (input) => {
        const result = await seedPackagedRecoveryFixture({
          ...input,
          kind: "legacy-failed",
          archivedOutcome,
          recoveryPayload: false,
        });
        NodeAssert.equal(result.state.update.status, "failed");
        NodeAssert.equal(result.state.update.reason, "launcher-restarted");
        NodeAssert.equal(
          await readUpdateOperationSettlement(result.baseDir, operationId),
          undefined,
        );
        const outcomePath = NodePath.join(
          result.baseDir,
          "runtime",
          "jones-update-operations",
          operationId + ".outcome.json",
        );
        if (archivedOutcome) {
          const outcome = JSON.parse(await NodeFSP.readFile(outcomePath));
          NodeAssert.equal(outcome.status, "failed");
          NodeAssert.equal(Object.hasOwn(outcome, "reason"), false);
        } else await NodeAssert.rejects(NodeFSP.lstat(outcomePath), { code: "ENOENT" });
        await NodeAssert.rejects(
          NodeFSP.lstat(NodePath.join(result.baseDir, "runtime", "db-backup", operationId)),
          { code: "ENOENT" },
        );
      });
    }
  },
);

NodeTest.test(
  "seeds each unsafe legacy variant for typed refusal without settling or executing payloads",
  linux,
  async () => {
    for (const [legacyVariant, reason] of Object.entries(legacyRefusalCases)) {
      await fixture(async (input) => {
        const prepared = await seedPackagedRecoveryFixture({
          ...input,
          kind: "legacy-failed",
          legacyVariant,
          archivedOutcome: legacyVariant === "archived-without-reason",
        });
        const reservationPath = NodePath.join(
          prepared.baseDir,
          "runtime",
          "jones-update-operations",
          operationId + ".json",
        );
        const reservation = await NodeFSP.readFile(reservationPath);
        const statePath = NodePath.join(prepared.baseDir, "runtime", "service-state.json");
        const stateBytes = await NodeFSP.readFile(statePath);
        const outcomePath = NodePath.join(
          prepared.baseDir,
          "runtime",
          "jones-update-operations",
          operationId + ".outcome.json",
        );
        const outcome = prepared.archivedOutcome ? await NodeFSP.readFile(outcomePath) : undefined;
        for (let repeat = 0; repeat < 2; repeat++) {
          NodeAssert.deepEqual(
            await evaluateLegacyUpdateSettlement({
              baseDir: prepared.baseDir,
              operationId,
              current: prepared.state.update,
              active: {
                activeVersion: prepared.state.activeVersion,
                activeSourceSha: (legacyVariant === "active-changed"
                  ? input.candidate
                  : input.active
                ).receipt.sourceSha,
              },
            }),
            { result: "refused", reason },
          );
          NodeAssert.equal(
            await readUpdateOperationSettlement(prepared.baseDir, operationId),
            undefined,
          );
          NodeAssert.deepEqual(await NodeFSP.readFile(reservationPath), reservation);
          NodeAssert.deepEqual(await NodeFSP.readFile(statePath), stateBytes);
          if (outcome) NodeAssert.deepEqual(await NodeFSP.readFile(outcomePath), outcome);
          else await NodeAssert.rejects(NodeFSP.lstat(outcomePath), { code: "ENOENT" });
        }
      });
    }
  },
);

NodeTest.test(
  "refuses modified payloads and stale active bindings before writing the fixture root",
  linux,
  async () => {
    await fixture(async (input) => {
      await NodeFSP.appendFile(
        NodePath.join(input.candidate.payloadDirectory, "client", "index.html"),
        "tampered",
      );
      await NodeAssert.rejects(seedPackagedRecoveryFixture(input), /payload digest differs/);
      NodeAssert.deepEqual(await NodeFSP.readdir(input.root), []);
    });
    await fixture(async (input) => {
      input.candidate.receipt.installedSourceSha = "9".repeat(40);
      await NodeAssert.rejects(
        seedPackagedRecoveryFixture(input),
        /source\/version binding differs/,
      );
      NodeAssert.deepEqual(await NodeFSP.readdir(input.root), []);
    });
  },
);

NodeTest.test(
  "refuses occupied, shared and aliased roots while preserving existing contents",
  linux,
  async () => {
    await fixture(async (input) => {
      await NodeFSP.writeFile(NodePath.join(input.root, "preserve"), "old");
      await NodeAssert.rejects(seedPackagedRecoveryFixture(input), /root is occupied/);
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(input.root, "preserve"), "utf8"),
        "old",
      );
    });
    await fixture(async (input) => {
      await NodeFSP.chmod(input.root, 0o770);
      await NodeAssert.rejects(seedPackagedRecoveryFixture(input), /private owned root/);
      NodeAssert.deepEqual(await NodeFSP.readdir(input.root), []);
    });
    await fixture(async (input) => {
      const alias = NodePath.join(input.parent, "jones-packaged-recovery-alias");
      await NodeFSP.symlink(input.root, alias);
      await NodeAssert.rejects(
        seedPackagedRecoveryFixture({ ...input, root: alias }),
        /physical root/,
      );
      NodeAssert.deepEqual(await NodeFSP.readdir(input.root), []);
    });
  },
);

NodeTest.test("refuses unsupported seed combinations before allocating state", linux, async () => {
  await fixture(async (input) => {
    for (const patch of [
      { operationId: NodeCrypto.randomUUID().toUpperCase() },
      { kind: "committed" },
      { archivedOutcome: true },
      { recoveryPayload: "yes" },
      { legacyVariant: "restore-pending" },
      { kind: "legacy-failed", legacyVariant: "unknown" },
      { kind: "legacy-failed", legacyVariant: "archived-without-reason" },
      { kind: "legacy-failed", legacyVariant: "trial-evidence", recoveryPayload: false },
    ]) {
      await NodeAssert.rejects(
        seedPackagedRecoveryFixture({ ...input, ...patch }),
        /fixture refused/,
      );
      NodeAssert.deepEqual(await NodeFSP.readdir(input.root), []);
    }
  });
});

const preload = NodeURL.fileURLToPath(new URL("./packaged-recovery-preload.cjs", import.meta.url));
const ledgerModule = new URL(
  "../../../apps/server/src/jones/updates/launcherOperation.ts",
  import.meta.url,
).href;

for (const method of ["write-after", "sync-after", "link-after", "directory-sync-after"]) {
  NodeTest.test(
    "interrupts the actual settlement publisher at " + method + " and replays unchanged",
    linux,
    async () => {
      await fixture(async (input) => {
        const prepared = await seedPackagedRecoveryFixture({ ...input, recoveryPayload: false });
        const digests = await writeFailedUpdateOperation(prepared.baseDir, prepared.state.update);
        const receipt = {
          schema: 1,
          operationId,
          status: "failed",
          settlement: "no-effect",
          basis: "launcher-recorded",
          ...digests,
          evidence: {
            phaseAtFailure: "accepted",
            nativeReason: "launcher-restarted",
            candidateStarted: false,
            activeVersion: input.active.receipt.version,
            activeSourceSha: input.active.receipt.sourceSha,
            recoveryPayload: "absent",
            restorePending: false,
            cause: null,
            writerLauncherVersion: "1.0.0",
            recordedAt: new Date().toISOString(),
          },
        };
        const executable = await NodeFSP.realpath(process.execPath);
        const stat = await NodeFSP.stat(executable, { bigint: true });
        const identity = Object.fromEntries(
          ["dev", "ino", "size", "mtimeNs", "ctimeNs"].map((key) => [key, String(stat[key])]),
        );
        const target =
          method === "link-after"
            ? "runtime/jones-update-settlements/" + operationId + ".json"
            : method === "directory-sync-after"
              ? "runtime/jones-update-settlements"
              : "runtime/jones-update-settlements.staging";
        await NodeFSP.writeFile(
          NodePath.join(input.root, "fixture-owner.json"),
          JSON.stringify({
            protocol: 1,
            purpose: "jones-packaged-recovery",
            fixtureId: prepared.fixtureId,
            executable,
            identity,
          }),
          { mode: 0o600 },
        );
        await NodeFSP.writeFile(
          NodePath.join(input.root, "fault-plan.json"),
          JSON.stringify({
            protocol: 1,
            fixtureId: prepared.fixtureId,
            executable,
            identity,
            method,
            target,
            arm: "pending-update",
            operationId,
          }),
          { mode: 0o600 },
        );
        const program = `import {publishUpdateOperationSettlement} from ${JSON.stringify(ledgerModule)};
        await publishUpdateOperationSettlement(${JSON.stringify(prepared.baseDir)},${JSON.stringify(receipt)});`;
        const run = () =>
          NodeChildProcess.spawnSync(
            executable,
            ["--require", preload, "--input-type=module", "-e", program],
            {
              cwd: input.root,
              encoding: "utf8",
              timeout: 5000,
              maxBuffer: 65536,
              env: {
                PATH: "/usr/bin:/bin",
                HOME: input.root,
                TMPDIR: input.root,
                T3CODE_HOME: prepared.baseDir,
                JONES_PACKAGED_RECOVERY_FIXTURE: NodePath.join(input.root, "fault-plan.json"),
              },
            },
          );
        const crashed = run();
        NodeAssert.equal(crashed.error, undefined);
        NodeAssert.equal(crashed.signal, "SIGKILL", crashed.stderr);
        NodeAssert.equal(crashed.status, null);
        const claim = JSON.parse(
          await NodeFSP.readFile(NodePath.join(input.root, "fault-fired.json")),
        );
        NodeAssert.equal(claim.operationId, operationId);
        NodeAssert.equal(claim.method, method);
        const final = NodePath.join(
          prepared.baseDir,
          "runtime",
          "jones-update-settlements",
          operationId + ".json",
        );
        let before;
        if (method === "write-after" || method === "sync-after") {
          await NodeAssert.rejects(NodeFSP.lstat(final), { code: "ENOENT" });
          const staging = NodePath.join(
            prepared.baseDir,
            "runtime",
            "jones-update-settlements.staging",
          );
          const files = await NodeFSP.readdir(staging);
          NodeAssert.equal(files.length, 1);
          NodeAssert.deepEqual(
            JSON.parse(await NodeFSP.readFile(NodePath.join(staging, files[0]))),
            receipt,
          );
        } else {
          before = await NodeFSP.readFile(final);
          NodeAssert.deepEqual(JSON.parse(before), receipt);
        }
        const replay = run();
        NodeAssert.equal(replay.error, undefined);
        NodeAssert.equal(replay.signal, null);
        NodeAssert.equal(replay.status, 0, replay.stderr);
        NodeAssert.deepEqual(
          await readUpdateOperationSettlement(prepared.baseDir, operationId),
          receipt,
        );
        if (before) NodeAssert.deepEqual(await NodeFSP.readFile(final), before);
        const repeated = run();
        NodeAssert.equal(repeated.status, 0, repeated.stderr);
        NodeAssert.deepEqual(
          JSON.parse(await NodeFSP.readFile(NodePath.join(input.root, "fault-fired.json"))),
          claim,
        );
      });
    },
  );
}

NodeTest.test(
  "interrupts the actual pinned-descriptor cleanup and preserves the live database",
  linux,
  async () => {
    await fixture(async (input) => {
      const prepared = await seedPackagedRecoveryFixture(input);
      const executable = await NodeFSP.realpath(process.execPath);
      const stat = await NodeFSP.stat(executable, { bigint: true });
      const identity = Object.fromEntries(
        ["dev", "ino", "size", "mtimeNs", "ctimeNs"].map((key) => [key, String(stat[key])]),
      );
      await NodeFSP.writeFile(
        NodePath.join(input.root, "fixture-owner.json"),
        JSON.stringify({
          protocol: 1,
          purpose: "jones-packaged-recovery",
          fixtureId: prepared.fixtureId,
          executable,
          identity,
        }),
        { mode: 0o600 },
      );
      await NodeFSP.writeFile(
        NodePath.join(input.root, "fault-plan.json"),
        JSON.stringify({
          protocol: 1,
          fixtureId: prepared.fixtureId,
          executable,
          identity,
          method: "unlink-after",
          target: "runtime/db-backup/" + operationId + "/database",
          arm: "pending-update",
          operationId,
        }),
        { mode: 0o600 },
      );
      const module = new URL(
        "../../../apps/server/src/jones/cloud/qualifiedBackup.ts",
        import.meta.url,
      ).href;
      const program = `import {removeQualifiedRecoveryPayload} from ${JSON.stringify(module)};
      await removeQualifiedRecoveryPayload(${JSON.stringify(prepared.baseDir)},${JSON.stringify(operationId)},
        ${JSON.stringify(prepared.staged.binding.dbPath)},
        ${JSON.stringify({ transactionId: operationId, qualified: prepared.staged })},"failed-before-trial");`;
      const original = await NodeFSP.readFile(prepared.staged.binding.dbPath);
      const run = () =>
        NodeChildProcess.spawnSync(
          executable,
          ["--require", preload, "--input-type=module", "-e", program],
          {
            cwd: input.root,
            encoding: "utf8",
            timeout: 5000,
            maxBuffer: 65536,
            env: {
              PATH: "/usr/bin:/bin",
              HOME: input.root,
              TMPDIR: input.root,
              T3CODE_HOME: prepared.baseDir,
              JONES_PACKAGED_RECOVERY_FIXTURE: NodePath.join(input.root, "fault-plan.json"),
            },
          },
        );
      const crashed = run();
      NodeAssert.equal(crashed.error, undefined);
      NodeAssert.equal(crashed.signal, "SIGKILL", crashed.stderr);
      const backup = NodePath.join(
        prepared.baseDir,
        "runtime",
        "db-backup",
        operationId,
        "database",
      );
      await NodeAssert.rejects(NodeFSP.lstat(backup), { code: "ENOENT" });
      NodeAssert.deepEqual(await NodeFSP.readFile(prepared.staged.binding.dbPath), original);
      NodeAssert.equal(
        JSON.parse(await NodeFSP.readFile(NodePath.join(input.root, "fault-fired.json"))).method,
        "unlink-after",
      );
      const replay = run();
      NodeAssert.equal(replay.error, undefined);
      NodeAssert.equal(replay.signal, null);
      NodeAssert.equal(replay.status, 0, replay.stderr);
      NodeAssert.deepEqual(await NodeFSP.readFile(prepared.staged.binding.dbPath), original);
    });
  },
);
