// @effect-diagnostics nodeBuiltinImport:off
import { afterEach, describe, expect, it } from "vite-plus/test";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import type { PendingServiceUpdate, ServiceUpdateRecord } from "../../cloud/serviceProtocol.ts";
import {
  decodeServiceLauncherChildMessage,
  decodeServiceLauncherParentMessage,
} from "../../cloud/serviceProtocol.ts";
import type { StagedQualifiedRuntime } from "../cloud/qualifiedRuntime.ts";
import {
  archiveUpdateOperation,
  assertNoUnreconciledUpdateOperations,
  evaluateLegacyUpdateSettlement,
  FailedOperationUnsettledError,
  operationBinding,
  publishUpdateOperationSettlement,
  readUpdateOperationCause,
  readUpdateOperationSettlement,
  reconcileUpdateOperation,
  rejectUpdateOperation,
  reserveUpdateOperation,
  resolveUpdateOperation,
  settleAbandonedUpdateOperations,
  verifyRetiredUpdateOperation,
  writeFailedUpdateOperation,
  writeUpdateOperationCause,
  type UpdateOperationSettlement,
} from "./launcherOperation.ts";

const roots: string[] = [];
afterEach(async () => {
  for (const root of roots.splice(0)) {
    await NodeFSP.rm(root, { recursive: true, force: true });
    await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
  }
});

const operationId = "12345678-1234-4234-8234-123456789abc";
const laterId = "32345678-1234-4234-8234-123456789abc";
const receipts = (baseDir: string) => NodePath.join(baseDir, "runtime", "jones-update-operations");
const outcomeFile = (baseDir: string, id: string) =>
  NodePath.join(receipts(baseDir), `${id}.outcome.json`);

it("admits a fresh operation after a digest-bound launcher-recorded failure", async () => {
  const { baseDir, qualified, pending } = await fixture();
  await reserveUpdateOperation(baseDir, operationId, operationBinding(qualified));
  const failed = { ...pending, status: "failed" as const, reason: "launcher-restarted" };
  await archiveUpdateOperation(baseDir, failed);
  const digest = async (file: string) =>
    NodeCrypto.createHash("sha256")
      .update(await NodeFSP.readFile(file))
      .digest("hex");
  const settlements = NodePath.join(baseDir, "runtime", "jones-update-settlements");
  await NodeFSP.mkdir(settlements, { mode: 0o700 });
  await NodeFSP.writeFile(
    NodePath.join(settlements, `${operationId}.json`),
    JSON.stringify({
      schema: 1,
      operationId,
      basis: "launcher-recorded",
      status: "failed",
      settlement: "no-effect",
      reservationSha256: await digest(NodePath.join(receipts(baseDir), `${operationId}.json`)),
      outcomeSha256: await digest(outcomeFile(baseDir, operationId)),
      evidence: {
        phaseAtFailure: "accepted",
        nativeReason: "launcher-restarted",
        candidateStarted: false,
        activeVersion: qualified.binding.activeVersion,
        activeSourceSha: qualified.binding.activeSourceSha,
        recoveryPayload: "absent",
        restorePending: false,
        cause: null,
        writerLauncherVersion: qualified.binding.activeVersion,
        recordedAt: "2026-10-23T04:00:00.000Z",
      },
    }) + "\n",
    { mode: 0o600 },
  );
  await assertNoUnreconciledUpdateOperations(baseDir, failed);
  expect(await reconcileUpdateOperation(baseDir, operationId, failed)).toMatchObject({
    state: "blocked",
    updateId: operationId,
    settlement: { basis: "launcher-recorded", recordedAt: "2026-10-23T04:00:00.000Z" },
  });
  await expect(
    reserveUpdateOperation(baseDir, operationId, operationBinding(qualified)),
  ).rejects.toMatchObject({ code: "EEXIST" });
});
async function fixture() {
  const allocated = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.tmpdir(), "jones-update-operation-"),
  );
  roots.push(allocated);
  const baseDir = await NodeFSP.realpath(allocated);
  const qualified: StagedQualifiedRuntime = {
    protocol: 1,
    stagedHandle: "22345678-1234-4234-8234-123456789abc",
    binding: {
      baseDir,
      dbPath: NodePath.join(baseDir, "userdata", "statev2.sqlite"),
      environmentId: "test-environment",
      activeVersion: "0.0.0-preview.20261010.1.1",
      activeSourceSha: "a".repeat(40),
    },
    receipt: {
      protocol: 1,
      repository: "Jones-Systems/Jones-Code",
      channel: "jones-main",
      version: "0.0.0-preview.20261010.2.1",
      sourceSha: "b".repeat(40),
      installedSourceSha: "a".repeat(40),
      sourceTree: "c".repeat(40),
      runId: 2,
      runAttempt: 1,
      artifactId: 3,
      workflow: ".github/workflows/artifact-cli-linux.yml",
      artifactDigest: `sha256:${"d".repeat(64)}`,
      archiveSha256: "e".repeat(64),
      payloadSha256: "f".repeat(64),
      platform: "linux",
      architecture: "x64",
    },
  };
  const pending: PendingServiceUpdate = {
    id: operationId,
    fromVersion: qualified.binding.activeVersion,
    targetVersion: qualified.receipt.version,
    dbPath: qualified.binding.dbPath,
    status: "pending",
    phase: "accepted",
    qualified,
  };
  return { baseDir, qualified, pending };
}

const settlementFile = (baseDir: string) =>
  NodePath.join(baseDir, "runtime", "jones-update-settlements", `${operationId}.json`);
const sha256 = (bytes: string | Buffer) =>
  NodeCrypto.createHash("sha256").update(bytes).digest("hex");
async function settlementFixture() {
  const f = await fixture();
  await reserveUpdateOperation(f.baseDir, operationId, operationBinding(f.qualified));
  const digests = await writeFailedUpdateOperation(f.baseDir, f.pending);
  const receipt: Extract<
    UpdateOperationSettlement,
    { basis: "launcher-recorded" | "legacy-reconciled" }
  > = {
    schema: 1,
    operationId,
    basis: "launcher-recorded",
    status: "failed",
    settlement: "no-effect",
    ...digests,
    evidence: {
      phaseAtFailure: "accepted",
      nativeReason: "launcher-restarted",
      candidateStarted: false,
      activeVersion: f.qualified.binding.activeVersion,
      activeSourceSha: f.qualified.binding.activeSourceSha,
      recoveryPayload: "absent",
      restorePending: false,
      cause: null,
      writerLauncherVersion: f.qualified.binding.activeVersion,
      recordedAt: "2026-10-23T04:00:00.000Z",
    },
  };
  const failed: ServiceUpdateRecord = {
    id: operationId,
    fromVersion: f.pending.fromVersion,
    targetVersion: f.pending.targetVersion,
    status: "failed",
    reason: "launcher-restarted",
    qualified: f.qualified,
  };
  return { ...f, receipt, failed };
}

describe("failed-operation settlement", () => {
  it("shares the old archive bytes, including property order and omission of reason", async () => {
    const f = await settlementFixture();
    const expected =
      JSON.stringify({
        schema: 1,
        operationId,
        binding: operationBinding(f.qualified),
        status: "failed",
      }) + "\n";
    const file = outcomeFile(f.baseDir, operationId);
    expect(await NodeFSP.readFile(file, "utf8")).toBe(expected);
    expect(f.receipt.outcomeSha256).toBe(sha256(expected));
    await archiveUpdateOperation(f.baseDir, f.failed);
    expect(await NodeFSP.readFile(file, "utf8")).toBe(expected);
    // The old serializer accepts equivalent whitespace; settlement binds the retained bytes.
    const spaced = JSON.stringify(JSON.parse(expected), null, 2) + "\n";
    await NodeFSP.writeFile(file, spaced);
    const retained = await writeFailedUpdateOperation(f.baseDir, f.pending);
    expect(retained.outcomeSha256).toBe(sha256(spaced));
    await archiveUpdateOperation(f.baseDir, f.failed);
    expect(await NodeFSP.readFile(file, "utf8")).toBe(spaced);
  });

  it("holds every pre-terminal crash prefix and preserves accepted-failure semantics after settlement", async () => {
    const f = await fixture();
    await reserveUpdateOperation(f.baseDir, operationId, operationBinding(f.qualified));
    await expect(assertNoUnreconciledUpdateOperations(f.baseDir, undefined)).rejects.toThrow(
      "reconciliation",
    );
    await writeFailedUpdateOperation(f.baseDir, f.pending);
    await expect(assertNoUnreconciledUpdateOperations(f.baseDir, f.pending)).rejects.toThrow(
      "reconciliation",
    );
    const completed = await settlementFixture();
    await publishUpdateOperationSettlement(completed.baseDir, completed.receipt);
    await expect(
      assertNoUnreconciledUpdateOperations(completed.baseDir, completed.pending),
    ).rejects.toThrow("reconciliation");
    for (const current of [completed.failed, undefined]) {
      await assertNoUnreconciledUpdateOperations(completed.baseDir, current);
      expect(await reconcileUpdateOperation(completed.baseDir, operationId, current)).toMatchObject(
        {
          state: "blocked",
          operationId,
          updateId: operationId,
          binding: { targetSource: completed.qualified.receipt.sourceSha },
          settlement: {
            basis: "launcher-recorded",
            recordedAt: completed.receipt.evidence.recordedAt,
          },
        },
      );
      expect(await resolveUpdateOperation(completed.baseDir, operationId, current)).toEqual({
        outcome: "accepted",
      });
    }
    await expect(
      reserveUpdateOperation(completed.baseDir, operationId, operationBinding(completed.qualified)),
    ).rejects.toMatchObject({ code: "EEXIST" });
    await reserveUpdateOperation(completed.baseDir, laterId, operationBinding(completed.qualified));
  });

  it.each(["written", "file-synced", "linked", "directory-synced"] as const)(
    "replays a settlement interrupted after %s without replacing a visible receipt",
    async (stopAt) => {
      const f = await settlementFixture();
      const phases: string[] = [];
      await expect(
        publishUpdateOperationSettlement(f.baseDir, f.receipt, {
          onPhase: async (phase) => {
            phases.push(phase);
            if (phase === stopAt) throw new Error("interrupted");
          },
        }),
      ).rejects.toThrow("interrupted");
      const visible = stopAt === "linked" || stopAt === "directory-synced";
      expect(await readUpdateOperationSettlement(f.baseDir, operationId)).toEqual(
        visible ? f.receipt : undefined,
      );
      expect(
        await NodeFSP.readdir(
          NodePath.join(f.baseDir, "runtime", "jones-update-settlements.staging"),
        ),
      ).toEqual([]);
      const retry = {
        ...f.receipt,
        evidence: { ...f.receipt.evidence, recordedAt: "2026-10-23T05:00:00.000Z" },
      };
      const replay: string[] = [];
      const result = await publishUpdateOperationSettlement(f.baseDir, retry, {
        onPhase: async (phase) => {
          replay.push(phase);
        },
      });
      expect(result).toEqual({
        result: visible ? "already-settled" : "settled",
        receipt: visible ? f.receipt : retry,
      });
      expect(replay).toEqual(
        visible
          ? ["file-synced", "directory-synced"]
          : ["written", "file-synced", "linked", "directory-synced"],
      );
      expect(phases.at(-1)).toBe(stopAt);
      expect((await NodeFSP.readdir(receipts(f.baseDir))).sort()).toEqual([
        `${operationId}.json`,
        `${operationId}.outcome.json`,
      ]);
    },
  );

  it("accepts only the original identity tuple and re-syncs a lost successful reply", async () => {
    const f = await settlementFixture();
    expect((await publishUpdateOperationSettlement(f.baseDir, f.receipt)).result).toBe("settled");
    const before = await NodeFSP.readFile(settlementFile(f.baseDir));
    expect(
      await publishUpdateOperationSettlement(f.baseDir, {
        ...f.receipt,
        evidence: {
          ...f.receipt.evidence,
          cause: "backup-failed",
          recordedAt: "2026-10-23T05:00:00.000Z",
        },
      }),
    ).toEqual({ result: "already-settled", receipt: f.receipt });
    await expect(
      publishUpdateOperationSettlement(f.baseDir, { ...f.receipt, basis: "legacy-reconciled" }),
    ).rejects.toThrow("conflicts");
    await expect(
      publishUpdateOperationSettlement(f.baseDir, { ...f.receipt, outcomeSha256: "0".repeat(64) }),
    ).rejects.toThrow("digests");
    expect(await NodeFSP.readFile(settlementFile(f.baseDir))).toEqual(before);
    expect((await NodeFSP.stat(settlementFile(f.baseDir))).mode & 0o777).toBe(0o600);
    expect((await NodeFSP.stat(NodePath.dirname(settlementFile(f.baseDir)))).mode & 0o777).toBe(
      0o700,
    );
  });

  it("rejects non-closed shapes and unsupported evidence without publishing", async () => {
    const f = await settlementFixture();
    const invalid: unknown[] = [
      { ...f.receipt, schema: 2 },
      { ...f.receipt, operationId: "../escape" },
      { ...f.receipt, status: "committed" },
      { ...f.receipt, settlement: "retry" },
      { ...f.receipt, extra: true },
      { ...f.receipt, reservationSha256: "A".repeat(64) },
      ...[
        { extra: true },
        { phaseAtFailure: "trial-ready" },
        { nativeReason: "unknown" },
        { candidateStarted: true },
        { restorePending: true },
        { activeSourceSha: "bad" },
        { recoveryPayload: "unknown" },
        { writerLauncherVersion: "latest" },
        { recordedAt: "2026-02-31T04:00:00.000Z" },
        { cause: "/private/user/state" },
      ].map((evidence) => ({ ...f.receipt, evidence: { ...f.receipt.evidence, ...evidence } })),
    ];
    for (const receipt of invalid)
      await expect(
        publishUpdateOperationSettlement(f.baseDir, receipt as UpdateOperationSettlement),
      ).rejects.toThrow();
    expect(await readUpdateOperationSettlement(f.baseDir, operationId)).toBeUndefined();
  });

  it.each(["reservation", "outcome"] as const)(
    "binds exact %s bytes and preserves failure when supplementary evidence is invalid",
    async (which) => {
      const f = await settlementFixture();
      await publishUpdateOperationSettlement(f.baseDir, f.receipt);
      const file =
        which === "reservation"
          ? NodePath.join(receipts(f.baseDir), `${operationId}.json`)
          : outcomeFile(f.baseDir, operationId);
      await NodeFSP.appendFile(file, " ");
      await expect(readUpdateOperationSettlement(f.baseDir, operationId)).rejects.toThrow(
        "digests",
      );
      expect(await reconcileUpdateOperation(f.baseDir, operationId, f.failed)).toMatchObject({
        state: "blocked",
        updateId: operationId,
        binding: { currentVersion: f.pending.fromVersion },
      });
      expect(
        (await reconcileUpdateOperation(f.baseDir, operationId, f.failed)).settlement,
      ).toBeUndefined();
      await expect(assertNoUnreconciledUpdateOperations(f.baseDir, f.failed)).rejects.toMatchObject(
        { code: "failed-operation-unsettled", operationId },
      );
      expect(await resolveUpdateOperation(f.baseDir, operationId, undefined)).toEqual({
        outcome: "accepted",
      });
    },
  );

  it("distinguishes missing settlement from corrupted durable outcome", async () => {
    const f = await settlementFixture();
    await expect(assertNoUnreconciledUpdateOperations(f.baseDir, f.failed)).rejects.toBeInstanceOf(
      FailedOperationUnsettledError,
    );
    await publishUpdateOperationSettlement(f.baseDir, f.receipt);
    await NodeFSP.writeFile(settlementFile(f.baseDir), "{");
    await expect(assertNoUnreconciledUpdateOperations(f.baseDir, f.failed)).rejects.toMatchObject({
      code: "failed-operation-unsettled",
      operationId,
    });
    await NodeFSP.writeFile(outcomeFile(f.baseDir, operationId), "{");
    await expect(
      assertNoUnreconciledUpdateOperations(f.baseDir, f.failed),
    ).rejects.not.toBeInstanceOf(FailedOperationUnsettledError);
    expect((await reconcileUpdateOperation(f.baseDir, operationId, f.failed)).updateId).toBe(
      operationId,
    );
  });

  it("refuses symlink or public settlement evidence", async () => {
    const f = await settlementFixture();
    await publishUpdateOperationSettlement(f.baseDir, f.receipt);
    const file = settlementFile(f.baseDir);
    await NodeFSP.chmod(file, 0o644);
    await expect(readUpdateOperationSettlement(f.baseDir, operationId)).rejects.toThrow("mode");
    await NodeFSP.chmod(file, 0o600);
    const moved = NodePath.join(f.baseDir, "retained-receipt");
    await NodeFSP.rename(file, moved);
    await NodeFSP.symlink(moved, file);
    await expect(readUpdateOperationSettlement(f.baseDir, operationId)).rejects.toThrow(
      "regular file",
    );
    await expect(
      reserveUpdateOperation(f.baseDir, operationId, operationBinding(f.qualified)),
    ).rejects.toThrow();
  });
});

describe("bounded update causes", () => {
  it("redacts paths, bounds UTF-8 bytes, and publishes causes outside the old ledger", async () => {
    const f = await fixture();
    const cause = {
      schema: 1 as const,
      operationId,
      step: "backup" as const,
      code: "EIO",
      message:
        "Could not read /private/user/state or C:\\Users\\Owner\\state or ../relative/state\n" +
        "é".repeat(300),
    };
    await writeUpdateOperationCause(f.baseDir, cause);
    const record = await readUpdateOperationCause(f.baseDir, operationId);
    expect(record?.message).toContain("Could not read [path] or [path] or [path]");
    expect(record?.message).not.toMatch(/private|Users|relative|\n/);
    expect(Buffer.byteLength(record!.message)).toBeLessThanOrEqual(512);
    expect(record?.message.endsWith("é")).toBe(true);
    await writeUpdateOperationCause(f.baseDir, cause);
    await expect(writeUpdateOperationCause(f.baseDir, { ...cause, code: "OTHER" })).rejects.toThrow(
      "conflicts",
    );
    const file = NodePath.join(
      NodePath.dirname(settlementFile(f.baseDir)),
      "causes",
      `${operationId}.json`,
    );
    expect((await NodeFSP.stat(file)).mode & 0o777).toBe(0o600);
    expect((await NodeFSP.stat(NodePath.dirname(file))).mode & 0o777).toBe(0o700);
    expect((await NodeFSP.readdir(NodePath.join(f.baseDir, "runtime"))).sort()).toEqual([
      "jones-update-settlements",
      "jones-update-settlements.staging",
    ]);
    await NodeFSP.writeFile(file, JSON.stringify({ ...record, extra: true }));
    await expect(readUpdateOperationCause(f.baseDir, operationId)).rejects.toThrow("Invalid");
  });
});

async function retiredFixture() {
  const f = await settlementFixture();
  const retiredLocation = `jones-update-recovery/${operationId}`;
  const directory = NodePath.join(f.baseDir, "runtime", retiredLocation);
  await NodeFSP.mkdir(directory, { recursive: true });
  for (const suffix of [".json", ".outcome.json"])
    await NodeFSP.rename(
      NodePath.join(receipts(f.baseDir), operationId + suffix),
      NodePath.join(directory, operationId + suffix),
    );
  const tombstone: UpdateOperationSettlement = {
    schema: 1,
    operationId,
    basis: "retired-exception",
    status: "failed",
    settlement: "no-effect",
    reservationSha256: f.receipt.reservationSha256,
    outcomeSha256: f.receipt.outcomeSha256,
    evidence: {
      retiredLocation,
      recordedAt: f.receipt.evidence.recordedAt,
      writerLauncherVersion: f.receipt.evidence.writerLauncherVersion,
    },
  };
  return { ...f, directory, retiredLocation, tombstone };
}

describe("retired-operation tombstones", () => {
  it("binds retained copies and permanently refuses reuse even when those copies are lost", async () => {
    const f = await retiredFixture();
    expect(await verifyRetiredUpdateOperation({ ...f, operationId })).toMatchObject({
      result: "verified",
      reservationSha256: f.receipt.reservationSha256,
      outcomeSha256: f.receipt.outcomeSha256,
    });
    await publishUpdateOperationSettlement(f.baseDir, f.tombstone);
    expect(await readUpdateOperationSettlement(f.baseDir, operationId)).toEqual(f.tombstone);
    await expect(
      reserveUpdateOperation(f.baseDir, operationId, operationBinding(f.qualified)),
    ).rejects.toMatchObject({ code: "EEXIST" });
    await NodeFSP.unlink(NodePath.join(f.directory, `${operationId}.outcome.json`));
    await expect(readUpdateOperationSettlement(f.baseDir, operationId)).rejects.toThrow("digests");
    await expect(
      reserveUpdateOperation(f.baseDir, operationId, operationBinding(f.qualified)),
    ).rejects.toMatchObject({ code: "EEXIST" });
    await NodeFSP.writeFile(settlementFile(f.baseDir), "{");
    await expect(
      reserveUpdateOperation(f.baseDir, operationId, operationBinding(f.qualified)),
    ).rejects.toThrow();
  });

  it("refuses traversal, redirected copies, a live reservation, and nonfailed outcomes", async () => {
    const f = await retiredFixture();
    const refused = { result: "refused", reason: "retired-evidence-mismatch" };
    for (const retiredLocation of [
      "../outside",
      "/absolute",
      "jones-update-operations",
      "a/../b",
      "a//b",
      "a\\b",
    ])
      expect(await verifyRetiredUpdateOperation({ ...f, operationId, retiredLocation })).toEqual(
        refused,
      );
    const alias = NodePath.join(f.baseDir, "runtime", "retired-alias");
    await NodeFSP.symlink(f.directory, alias, "dir");
    expect(
      await verifyRetiredUpdateOperation({ ...f, operationId, retiredLocation: "retired-alias" }),
    ).toEqual(refused);
    const file = NodePath.join(f.directory, `${operationId}.outcome.json`);
    const bytes = await NodeFSP.readFile(file);
    await NodeFSP.writeFile(
      file,
      JSON.stringify({ ...JSON.parse(bytes.toString()), status: "committed" }),
    );
    expect(await verifyRetiredUpdateOperation({ ...f, operationId })).toEqual(refused);
    await NodeFSP.writeFile(file, bytes);
    await reserveUpdateOperation(f.baseDir, operationId, operationBinding(f.qualified));
    expect(await verifyRetiredUpdateOperation({ ...f, operationId })).toEqual(refused);
  });
});

describe("legacy failed-operation predicates", () => {
  it("requires matching native failure and unchanged active identity", async () => {
    const f = await settlementFixture();
    const evaluate = (current: ServiceUpdateRecord | undefined, extra = {}) =>
      evaluateLegacyUpdateSettlement({
        baseDir: f.baseDir,
        operationId,
        current,
        active: f.qualified.binding,
        ...extra,
      });
    expect(await evaluate(f.failed)).toMatchObject({
      result: "settle",
      binding: operationBinding(f.qualified),
    });
    expect(await evaluate(f.pending)).toEqual({ result: "refused", reason: "not-failed" });
    expect(await evaluate(undefined)).toEqual({
      result: "refused",
      reason: "insufficient-evidence",
    });
    expect(await evaluate({ ...f.failed, id: laterId })).toEqual({
      result: "refused",
      reason: "insufficient-evidence",
    });
    expect(await evaluate({ ...f.failed, reason: "other" })).toEqual({
      result: "refused",
      reason: "insufficient-evidence",
    });
    expect(
      await evaluate({ ...f.failed, qualified: undefined } as unknown as ServiceUpdateRecord),
    ).toEqual({
      result: "refused",
      reason: "insufficient-evidence",
    });
    expect(await evaluate(f.failed, { operationId: laterId })).toEqual({
      result: "refused",
      reason: "not-found",
    });
    expect(
      await evaluate(f.failed, {
        active: { ...f.qualified.binding, activeSourceSha: "c".repeat(40) },
      }),
    ).toEqual({ result: "refused", reason: "active-changed" });
    expect(
      await evaluate({ ...f.failed, phase: "trial-ready" } as unknown as ServiceUpdateRecord),
    ).toEqual({ result: "refused", reason: "trial-evidence-present" });
    expect(await evaluate({ ...f.failed, startupReceipt: {} } as ServiceUpdateRecord)).toEqual({
      result: "refused",
      reason: "trial-evidence-present",
    });
    await NodeFSP.unlink(outcomeFile(f.baseDir, operationId));
    expect(await evaluate(f.failed)).toMatchObject({ result: "settle" });
    await NodeFSP.writeFile(outcomeFile(f.baseDir, operationId), "{");
    expect(await evaluate(f.failed)).toEqual({
      result: "refused",
      reason: "insufficient-evidence",
    });
  });

  it.each([
    [".restore-pending", "restore-pending"],
    [".database-restore.json", "restore-pending"],
    [".database-wal-restore.json.complete", "restore-pending"],
    ["advanced-state", "trial-evidence-present"],
    ["resume-dispatched.json", "trial-evidence-present"],
    [".resume-dispatched.123.partial", "trial-evidence-present"],
  ])("refuses retained %s evidence without changing it", async (name, reason) => {
    const f = await settlementFixture();
    const directory = NodePath.join(f.baseDir, "runtime", "db-backup", operationId);
    await NodeFSP.mkdir(directory, { recursive: true });
    const file = NodePath.join(directory, name);
    await NodeFSP.writeFile(file, "retained evidence");
    expect(
      await evaluateLegacyUpdateSettlement({
        baseDir: f.baseDir,
        operationId,
        current: f.failed,
        active: f.qualified.binding,
      }),
    ).toEqual({ result: "refused", reason });
    expect(await NodeFSP.readFile(file, "utf8")).toBe("retained evidence");
  });
});

describe("native update operation receipts", () => {
  it("distinguishes no request from a crash between reservation and native acceptance", async () => {
    const { baseDir, qualified, pending } = await fixture();
    expect((await reconcileUpdateOperation(baseDir, operationId, undefined)).state).toBe("absent");
    await assertNoUnreconciledUpdateOperations(baseDir, undefined);
    await reserveUpdateOperation(baseDir, operationId, operationBinding(qualified));
    expect((await reconcileUpdateOperation(baseDir, operationId, undefined)).state).toBe("blocked");
    await expect(assertNoUnreconciledUpdateOperations(baseDir, undefined)).rejects.toThrow(
      "requires reconciliation",
    );
    await expect(
      reserveUpdateOperation(baseDir, operationId, operationBinding(qualified)),
    ).rejects.toMatchObject({ code: "EEXIST" });
    expect(await reconcileUpdateOperation(baseDir, operationId, pending)).toMatchObject({
      state: "pending",
      operationId,
      updateId: operationId,
      binding: { targetSource: "b".repeat(40), stagedHandle: qualified.stagedHandle },
    });
  });

  it.each(["committed", "rolled-back"] as const)(
    "retains %s readback after a later update replaces service state",
    async (status) => {
      const { baseDir, qualified, pending } = await fixture();
      await reserveUpdateOperation(baseDir, operationId, operationBinding(qualified));
      const terminal = { ...pending, status };
      await archiveUpdateOperation(baseDir, terminal);
      await archiveUpdateOperation(baseDir, terminal);
      expect((await reconcileUpdateOperation(baseDir, operationId, undefined)).state).toBe(status);
      await assertNoUnreconciledUpdateOperations(baseDir, undefined);
      await expect(
        archiveUpdateOperation(baseDir, { ...terminal, status: "failed" }),
      ).rejects.toThrow("conflicts");
    },
  );

  it("fences identity variance and malformed or path-like operation IDs", async () => {
    const { baseDir, qualified, pending } = await fixture();
    await reserveUpdateOperation(baseDir, operationId, operationBinding(qualified));
    expect(
      (
        await reconcileUpdateOperation(baseDir, operationId, {
          ...pending,
          qualified: { ...qualified, stagedHandle: "other" },
        })
      ).state,
    ).toBe("blocked");
    expect((await reconcileUpdateOperation(baseDir, "../other", pending)).state).toBe("blocked");
    const message = {
      type: "request-update",
      operationId,
      stagedHandle: qualified.stagedHandle,
      targetVersion: pending.targetVersion,
      dbPath: pending.dbPath,
    };
    expect(decodeServiceLauncherChildMessage(message)).toEqual(message);
    expect(
      decodeServiceLauncherChildMessage({ ...message, operationId: "../other" }),
    ).toBeUndefined();
    expect(
      decodeServiceLauncherChildMessage({ ...message, stagedHandle: undefined }),
    ).toBeUndefined();
  });

  it("uses the same canonical operation root through trailing separators, dot paths, and symlinks", async () => {
    const { baseDir, qualified, pending } = await fixture();
    const alias = NodePath.join(baseDir, "alias");
    await NodeFSP.symlink(baseDir, alias, "dir");
    await reserveUpdateOperation(`${baseDir}/`, operationId, operationBinding(qualified));
    for (const root of [`${baseDir}/.`, alias]) {
      expect((await reconcileUpdateOperation(root, operationId, pending)).state).toBe("pending");
      await archiveUpdateOperation(root, { ...pending, status: "committed" });
      expect((await reconcileUpdateOperation(root, operationId, undefined)).state).toBe(
        "committed",
      );
    }
  });

  it("records an abandoned reservation as rejected once, and the next operation may proceed", async () => {
    const { baseDir, qualified } = await fixture();
    await reserveUpdateOperation(baseDir, operationId, operationBinding(qualified));
    expect(await resolveUpdateOperation(baseDir, operationId, undefined)).toMatchObject({
      outcome: "rejected",
    });
    const written = JSON.parse(await NodeFSP.readFile(outcomeFile(baseDir, operationId), "utf8"));
    expect(written).toMatchObject({
      schema: 1,
      operationId,
      status: "rejected",
      binding: operationBinding(qualified),
    });
    // Repeated questions and settlements answer the same and write nothing new.
    expect((await resolveUpdateOperation(baseDir, operationId, undefined)).outcome).toBe(
      "rejected",
    );
    await rejectUpdateOperation(baseDir, operationId, undefined, "again");
    await settleAbandonedUpdateOperations(baseDir, undefined);
    expect(JSON.parse(await NodeFSP.readFile(outcomeFile(baseDir, operationId), "utf8"))).toEqual(
      written,
    );
    expect(await reconcileUpdateOperation(baseDir, operationId, undefined)).toMatchObject({
      state: "rejected",
      binding: { stagedHandle: qualified.stagedHandle },
    });
    await assertNoUnreconciledUpdateOperations(baseDir, undefined);
    expect((await NodeFSP.readdir(receipts(baseDir))).sort()).toEqual([
      `${operationId}.json`,
      `${operationId}.outcome.json`,
    ]);
  });

  it("never records rejection for an operation native state names or that has an outcome", async () => {
    const { baseDir, qualified, pending } = await fixture();
    await reserveUpdateOperation(baseDir, operationId, operationBinding(qualified));
    // Accepted: the pending record names the operation.
    expect((await resolveUpdateOperation(baseDir, operationId, pending)).outcome).toBe("accepted");
    await settleAbandonedUpdateOperations(baseDir, pending);
    await expect(rejectUpdateOperation(baseDir, operationId, pending, "late")).rejects.toThrow(
      "names this update operation",
    );
    await expect(NodeFSP.lstat(outcomeFile(baseDir, operationId))).rejects.toMatchObject({
      code: "ENOENT",
    });
    // Accepted, then finished: its terminal outcome outlives the native record.
    for (const status of ["committed", "rolled-back", "failed"] as const) {
      const other = await fixture();
      await reserveUpdateOperation(other.baseDir, operationId, operationBinding(other.qualified));
      await archiveUpdateOperation(other.baseDir, { ...other.pending, status });
      expect((await resolveUpdateOperation(other.baseDir, operationId, undefined)).outcome).toBe(
        "accepted",
      );
      await expect(
        rejectUpdateOperation(other.baseDir, operationId, undefined, "late"),
      ).rejects.toThrow("conflicts");
      await settleAbandonedUpdateOperations(other.baseDir, undefined);
    }
  });

  it("reads contradictory evidence as accepted and refuses to archive over a rejection", async () => {
    const { baseDir, qualified, pending } = await fixture();
    await reserveUpdateOperation(baseDir, operationId, operationBinding(qualified));
    await rejectUpdateOperation(baseDir, operationId, undefined, "abandoned");
    expect((await reconcileUpdateOperation(baseDir, operationId, pending)).state).toBe("pending");
    expect((await resolveUpdateOperation(baseDir, operationId, pending)).outcome).toBe("accepted");
    await expect(
      archiveUpdateOperation(baseDir, { ...pending, status: "committed" }),
    ).rejects.toThrow("conflicts");
  });

  it("answers an operation it never reserved as rejected without a tombstone", async () => {
    const { baseDir, qualified } = await fixture();
    expect(await resolveUpdateOperation(baseDir, operationId, undefined)).toMatchObject({
      outcome: "rejected",
      reason: expect.stringContaining("no reservation"),
    });
    await expect(NodeFSP.lstat(receipts(baseDir))).rejects.toMatchObject({ code: "ENOENT" });
    await expect(rejectUpdateOperation(baseDir, operationId, undefined, "x")).rejects.toThrow(
      "no reservation",
    );
    // A later request may still use the same operation as a new attempt.
    await reserveUpdateOperation(baseDir, operationId, operationBinding(qualified));
  });

  it("keeps an unreadable reservation or outcome unknown and refuses another operation", async () => {
    const torn = await fixture();
    await NodeFSP.mkdir(receipts(torn.baseDir), { recursive: true });
    await NodeFSP.writeFile(NodePath.join(receipts(torn.baseDir), `${operationId}.json`), '{"sche');
    expect((await resolveUpdateOperation(torn.baseDir, operationId, undefined)).outcome).toBe(
      "unknown",
    );
    await expect(settleAbandonedUpdateOperations(torn.baseDir, undefined)).rejects.toThrow();
    await expect(assertNoUnreconciledUpdateOperations(torn.baseDir, undefined)).rejects.toThrow(
      "requires reconciliation",
    );

    const mismatched = await fixture();
    await reserveUpdateOperation(
      mismatched.baseDir,
      operationId,
      operationBinding(mismatched.qualified),
    );
    await NodeFSP.writeFile(
      outcomeFile(mismatched.baseDir, operationId),
      JSON.stringify({
        schema: 1,
        operationId,
        binding: { ...operationBinding(mismatched.qualified), targetSource: "0".repeat(40) },
        status: "rejected",
      }),
    );
    expect((await resolveUpdateOperation(mismatched.baseDir, operationId, undefined)).outcome).toBe(
      "unknown",
    );
    expect((await reconcileUpdateOperation(mismatched.baseDir, operationId, undefined)).state).toBe(
      "blocked",
    );
    await expect(
      assertNoUnreconciledUpdateOperations(mismatched.baseDir, undefined),
    ).rejects.toThrow("requires reconciliation");
  });

  it("publishes receipts whole and drops a crashed write's leftovers", async () => {
    const { baseDir, qualified, pending } = await fixture();
    await reserveUpdateOperation(baseDir, operationId, operationBinding(qualified));
    const staging = `${receipts(baseDir)}.staging`;
    // A launcher that died after writing its temporary file but before publishing it.
    await NodeFSP.writeFile(NodePath.join(staging, "crashed"), '{"schema":1');
    expect(await NodeFSP.readdir(receipts(baseDir))).toEqual([`${operationId}.json`]);
    await settleAbandonedUpdateOperations(baseDir, pending);
    await expect(NodeFSP.lstat(staging)).rejects.toMatchObject({ code: "ENOENT" });
    await expect(NodeFSP.lstat(outcomeFile(baseDir, operationId))).rejects.toMatchObject({
      code: "ENOENT",
    });
  });

  it("settles only abandoned operations, so an older rejection never answers a newer one", async () => {
    const { baseDir, qualified, pending } = await fixture();
    await reserveUpdateOperation(baseDir, operationId, operationBinding(qualified));
    const later = { ...pending, id: laterId };
    await reserveUpdateOperation(baseDir, laterId, operationBinding(qualified));
    await settleAbandonedUpdateOperations(baseDir, later);
    expect((await resolveUpdateOperation(baseDir, operationId, later)).outcome).toBe("rejected");
    expect((await resolveUpdateOperation(baseDir, laterId, later)).outcome).toBe("accepted");
    expect((await reconcileUpdateOperation(baseDir, laterId, later)).state).toBe("pending");
    await expect(NodeFSP.lstat(outcomeFile(baseDir, laterId))).rejects.toMatchObject({
      code: "ENOENT",
    });
  });

  it("decodes outcome questions and answers only with their exact pairing", () => {
    const question = { type: "reconcile-update", operationId, queryId: 3 };
    expect(decodeServiceLauncherChildMessage(question)).toEqual(question);
    for (const queryId of [0, -1, 1.5, "3", undefined])
      expect(decodeServiceLauncherChildMessage({ ...question, queryId })).toBeUndefined();
    expect(
      decodeServiceLauncherChildMessage({ ...question, operationId: "../other" }),
    ).toBeUndefined();
    const answer = { type: "update-outcome", operationId, queryId: 3, outcome: "rejected" };
    expect(decodeServiceLauncherParentMessage(answer)).toEqual(answer);
    expect(decodeServiceLauncherParentMessage({ ...answer, outcome: "maybe" })).toBeUndefined();
    expect(decodeServiceLauncherParentMessage({ ...answer, queryId: "3" })).toBeUndefined();
  });
});
