// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, describe, expect, it } from "vite-plus/test";
import {
  currentQualifiedRuntimeBinding,
  qualifiedPayloadDigest,
  type QualifiedRuntimeReceipt,
  type StagedQualifiedRuntime,
} from "../cloud/qualifiedRuntime.ts";
import { retainStagedSelection, restoreStagedSelection } from "./stagedSelection.ts";

const roots: string[] = [];
afterEach(async () => {
  for (const root of roots.splice(0)) {
    await NodeFSP.rm(root, { recursive: true, force: true });
    await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
  }
});
async function fixture() {
  const allocatedRoot = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-selection-"));
  roots.push(allocatedRoot);
  const root = await NodeFSP.realpath(allocatedRoot);
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Native receipts must bind the host executing these synthetic fixtures.
  const platform = NodeOS.platform();
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Native receipts must bind the host executing these synthetic fixtures.
  const architecture = NodeOS.arch();
  if (
    (platform !== "linux" && platform !== "darwin") ||
    (architecture !== "x64" && architecture !== "arm64") ||
    (platform === "darwin" && architecture !== "arm64")
  )
    throw new Error("Synthetic staging requires a supported native runtime host.");
  const workflow =
    platform === "darwin"
      ? ".github/workflows/artifact-cli-mac.yml"
      : ".github/workflows/artifact-cli-linux.yml";
  await NodeFSP.mkdir(NodePath.join(root, "userdata"));
  await NodeFSP.writeFile(NodePath.join(root, "userdata", "statev2.sqlite"), "unchanged-state");
  await NodeFSP.writeFile(NodePath.join(root, "userdata", "environment-id"), "own-environment");
  const baseline = "0.0.0-preview.20261002.1.1",
    version = "0.0.0-preview.20261002.2.1";
  let candidate: QualifiedRuntimeReceipt | undefined;
  for (const [current, source] of [
    [baseline, "a"],
    [version, "b"],
  ] as const) {
    const runtime = NodePath.join(root, "runtime", "versions", current);
    await NodeFSP.mkdir(runtime, { recursive: true });
    await NodeFSP.writeFile(NodePath.join(runtime, "t3"), `#!/bin/sh\necho ${source}\n`, {
      mode: 0o700,
    });
    const receipt: QualifiedRuntimeReceipt = {
      protocol: 1,
      repository: "Jones-Systems/Jones-Code",
      channel: "jones-main",
      sourceSha: source.repeat(40),
      sourceTree: "c".repeat(40),
      installedSourceSha: "a".repeat(40),
      runId: 2,
      runAttempt: 1,
      artifactId: 3,
      workflow,
      artifactDigest: `sha256:${"d".repeat(64)}`,
      archiveSha256: "e".repeat(64),
      platform,
      architecture,
      version: current,
      payloadSha256: await qualifiedPayloadDigest(runtime, platform, workflow),
    };
    await NodeFSP.writeFile(
      NodePath.join(runtime, ".jones-runtime-receipt.json"),
      JSON.stringify(receipt),
      { mode: 0o600 },
    );
    if (current === version) candidate = receipt;
  }
  const selection: StagedQualifiedRuntime = {
    protocol: 1,
    stagedHandle: NodeCrypto.randomUUID(),
    binding: await currentQualifiedRuntimeBinding(root, baseline),
    receipt: candidate!,
  };
  await NodeFSP.mkdir(NodePath.join(root, "runtime", "staged-updates"));
  await NodeFSP.writeFile(
    NodePath.join(root, "runtime", "staged-updates", `${selection.stagedHandle}.json`),
    JSON.stringify(selection),
    { mode: 0o600 },
  );
  return { root, baseline, selection };
}

describe("durable native staging selection", () => {
  it("restores the exact handle after checker restart without changing live state", async () => {
    const f = await fixture();
    await retainStagedSelection(f.selection);
    await retainStagedSelection(f.selection);
    expect(await restoreStagedSelection(f.root, f.baseline)).toEqual(f.selection);
    expect(
      await NodeFSP.readFile(NodePath.join(f.root, "userdata", "statev2.sqlite"), "utf8"),
    ).toBe("unchanged-state");
    await expect(
      retainStagedSelection({ ...f.selection, stagedHandle: NodeCrypto.randomUUID() }),
    ).rejects.toThrow("preserved");
    expect((await restoreStagedSelection(f.root, f.baseline))?.stagedHandle).toBe(
      f.selection.stagedHandle,
    );
  });
  it("holds a retained stage if its payload or native binding changes", async () => {
    const f = await fixture();
    await retainStagedSelection(f.selection);
    await NodeFSP.appendFile(
      NodePath.join(f.root, "runtime", "versions", f.selection.receipt.version, "t3"),
      "changed",
    );
    await expect(restoreStagedSelection(f.root, f.baseline)).rejects.toThrow("changed");
    await NodeFSP.writeFile(
      NodePath.join(f.root, "userdata", "environment-id"),
      "another-environment",
    );
    expect(await restoreStagedSelection(f.root, f.baseline)).toBeUndefined();
  });
});
