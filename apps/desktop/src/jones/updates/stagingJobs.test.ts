// @effect-diagnostics nodeBuiltinImport:off - Synthetic staging roots under the user's cache.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  JONES_ACTIONS_RECEIPT_FILE,
  createJonesStageAttempt,
  publishJonesCompletedStage,
} from "@t3tools/shared/jones/jonesActions";
import { afterEach, describe, expect, it } from "vite-plus/test";

import { reclaimJonesStagePayload } from "./stagingJobs.ts";

const replaced = "a".repeat(64);
const current = "b".repeat(64);

const fixtures = new Set<string>();
afterEach(async () => {
  for (const root of fixtures) {
    await NodeFSP.rm(root, { recursive: true, force: true });
    await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
    fixtures.delete(root);
  }
});

async function updatesRoot() {
  const parent = NodePath.join(NodeOS.homedir(), ".cache", "jones-updater-test-fixtures");
  await NodeFSP.mkdir(parent, { recursive: true, mode: 0o700 });
  const home = await NodeFSP.mkdtemp(NodePath.join(parent, "staging-jobs-"));
  fixtures.add(home);
  const root = NodePath.join(home, "jones-updates");
  await NodeFSP.mkdir(NodePath.join(root, "artifacts"), { recursive: true });
  await NodeFSP.mkdir(NodePath.join(root, "apps"), { recursive: true });
  return root;
}

/** A completed attempt in each root, the way the client and app staging publish them. */
async function completedStage(root: string, handle: string) {
  const artifact = await createJonesStageAttempt(NodePath.join(root, "artifacts"), handle);
  await NodeFSP.writeFile(NodePath.join(artifact.directory, "qualified.dmg"), "dmg-bytes");
  await NodeFSP.writeFile(NodePath.join(artifact.directory, JONES_ACTIONS_RECEIPT_FILE), "{}");
  await publishJonesCompletedStage(
    NodePath.join(root, "artifacts"),
    handle,
    artifact.directory,
    JONES_ACTIONS_RECEIPT_FILE,
  );
  const app = await createJonesStageAttempt(NodePath.join(root, "apps"), handle);
  const contents = NodePath.join(app.directory, "Candidate.app", "Contents", "Resources");
  await NodeFSP.mkdir(contents, { recursive: true });
  await NodeFSP.writeFile(NodePath.join(contents, "app.asar"), "asar-bytes");
  await NodeFSP.symlink("Resources", NodePath.join(contents, "..", "Link"));
  await NodeFSP.writeFile(NodePath.join(app.directory, "mac-app-receipt.json"), "{}");
  await publishJonesCompletedStage(
    NodePath.join(root, "apps"),
    handle,
    app.directory,
    "mac-app-receipt.json",
  );
  return { artifactAttempt: artifact.attemptRoot, appAttempt: app.attemptRoot };
}

const exists = (path: string) =>
  NodeFSP.lstat(path).then(
    () => true,
    () => false,
  );

describe("reclaimJonesStagePayload", () => {
  it("removes only the replaced handle's indexed attempts", async () => {
    const root = await updatesRoot();
    const old = await completedStage(root, replaced);
    const kept = await completedStage(root, current);
    await NodeFSP.mkdir(NodePath.join(root, "staging"));
    await NodeFSP.writeFile(
      NodePath.join(root, "staging", "selection.json"),
      JSON.stringify({ app: { handle: current } }),
    );

    const result = await reclaimJonesStagePayload(root, replaced);
    expect(result.status).toBe("reclaimed");
    // The payloads plus each attempt's linked completion record.
    expect(result.status === "reclaimed" && result.bytes).toBeGreaterThan(
      "dmg-bytes".length + "{}".length * 2 + "asar-bytes".length,
    );
    for (const path of [
      old.artifactAttempt,
      old.appAttempt,
      NodePath.join(root, "artifacts", "completed", `${replaced}.json`),
      NodePath.join(root, "apps", "completed", `${replaced}.json`),
    ])
      expect(await exists(path)).toBe(false);
    for (const path of [
      kept.artifactAttempt,
      kept.appAttempt,
      NodePath.join(root, "apps", "completed", `${current}.json`),
    ])
      expect(await exists(path)).toBe(true);
    expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "absent" });
  });

  it.each([
    ["a selection", "staging", "selection.json"],
    ["an attempt selection", "attempt-selections", "attempt.json"],
    ["a server runtime selection", "selections", "runtime.json"],
    ["a transaction record", "transactions", "txn-1/journal.json"],
    ["a retired preparation", "retired-preparations", "txn-2/prepare-intent.json"],
  ])("keeps a payload that %s still names", async (_label, directory, file) => {
    const root = await updatesRoot();
    const old = await completedStage(root, replaced);
    const record = NodePath.join(root, directory, file);
    await NodeFSP.mkdir(NodePath.dirname(record), { recursive: true });
    await NodeFSP.writeFile(record, JSON.stringify({ staged: { handle: replaced } }));

    expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "referenced" });
    expect(await exists(old.artifactAttempt)).toBe(true);
    expect(await exists(old.appAttempt)).toBe(true);
  });

  it("treats a transaction named for the handle as evidence", async () => {
    const root = await updatesRoot();
    await completedStage(root, replaced);
    await NodeFSP.mkdir(NodePath.join(root, "transactions", replaced), { recursive: true });
    expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "referenced" });
  });

  it("skips a transaction's recovery reserve but refuses an unbounded record", async () => {
    const root = await updatesRoot();
    await completedStage(root, replaced);
    const transaction = NodePath.join(root, "transactions", "txn-1");
    await NodeFSP.mkdir(transaction, { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(transaction, "reserve.bin"),
      Buffer.alloc(2 * 1024 * 1024),
    );
    expect(await reclaimJonesStagePayload(root, replaced)).toMatchObject({ status: "reclaimed" });

    await completedStage(root, current);
    await NodeFSP.mkdir(NodePath.join(root, "staging"), { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(root, "staging", "huge.json"),
      Buffer.alloc(2 * 1024 * 1024),
    );
    await expect(reclaimJonesStagePayload(root, current)).rejects.toThrow("bounded regular file");
  });

  it("refuses an invalid handle without reading any record", async () => {
    const root = await updatesRoot();
    await expect(reclaimJonesStagePayload(root, "../apps")).rejects.toThrow("Invalid");
  });

  it("removes a legacy completed stage and leaves an unreceipted directory alone", async () => {
    const root = await updatesRoot();
    const legacy = NodePath.join(root, "apps", replaced);
    await NodeFSP.mkdir(NodePath.join(legacy, "Candidate.app"), { recursive: true });
    await NodeFSP.writeFile(NodePath.join(legacy, "mac-app-receipt.json"), "{}");
    const partial = NodePath.join(root, "artifacts", replaced);
    await NodeFSP.mkdir(partial);

    expect(await reclaimJonesStagePayload(root, replaced)).toEqual({
      status: "reclaimed",
      bytes: 2,
    });
    expect(await exists(legacy)).toBe(false);
    expect(await exists(partial)).toBe(true);
  });
});
