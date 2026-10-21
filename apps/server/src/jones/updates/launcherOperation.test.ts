// @effect-diagnostics nodeBuiltinImport:off
import { afterEach, describe, expect, it } from "vite-plus/test";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import type { PendingServiceUpdate } from "../../cloud/serviceProtocol.ts";
import {
  decodeServiceLauncherChildMessage,
  decodeServiceLauncherParentMessage,
} from "../../cloud/serviceProtocol.ts";
import type { StagedQualifiedRuntime } from "../cloud/qualifiedRuntime.ts";
import {
  archiveUpdateOperation,
  assertNoUnreconciledUpdateOperations,
  operationBinding,
  reconcileUpdateOperation,
  rejectUpdateOperation,
  reserveUpdateOperation,
  resolveUpdateOperation,
  settleAbandonedUpdateOperations,
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
