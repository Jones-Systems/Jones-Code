// @effect-diagnostics nodeBuiltinImport:off
import { afterEach, describe, expect, it } from "vite-plus/test";
import * as Fs from "node:fs/promises";
import * as Os from "node:os";
import * as Path from "node:path";
import type { PendingServiceUpdate } from "../../cloud/serviceProtocol.ts";
import { decodeServiceLauncherChildMessage } from "../../cloud/serviceProtocol.ts";
import type { StagedQualifiedRuntime } from "../cloud/qualifiedRuntime.ts";
import {
  archiveUpdateOperation,
  assertNoUnreconciledUpdateOperations,
  operationBinding,
  reconcileUpdateOperation,
  reserveUpdateOperation,
} from "./launcherOperation.ts";

const roots: string[] = [];
afterEach(async () => {
  for (const root of roots.splice(0)) {
    await Fs.rm(root, { recursive: true, force: true });
    await expect(Fs.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
  }
});

const operationId = "12345678-1234-4234-8234-123456789abc";
async function fixture() {
  const allocated = await Fs.mkdtemp(Path.join(Os.tmpdir(), "jones-update-operation-"));
  roots.push(allocated);
  const baseDir = await Fs.realpath(allocated);
  const qualified: StagedQualifiedRuntime = {
    protocol: 1,
    stagedHandle: "22345678-1234-4234-8234-123456789abc",
    binding: {
      baseDir,
      dbPath: Path.join(baseDir, "userdata", "statev2.sqlite"),
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
    const alias = Path.join(baseDir, "alias");
    await Fs.symlink(baseDir, alias, "dir");
    await reserveUpdateOperation(`${baseDir}/`, operationId, operationBinding(qualified));
    for (const root of [`${baseDir}/.`, alias]) {
      expect((await reconcileUpdateOperation(root, operationId, pending)).state).toBe("pending");
      await archiveUpdateOperation(root, { ...pending, status: "committed" });
      expect((await reconcileUpdateOperation(root, operationId, undefined)).state).toBe(
        "committed",
      );
    }
  });
});
