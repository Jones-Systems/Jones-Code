// @effect-diagnostics nodeBuiltinImport:off
// Synthetic native runtimes remain isolated and are removed by the fixture owner.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { afterEach, describe, expect, it } from "vite-plus/test";
import {
  currentQualifiedRuntimeBinding,
  qualifiedPayloadDigest,
  withQualifiedRuntimeLock,
  type QualifiedRuntimeReceipt,
  type StagedQualifiedRuntime,
} from "../cloud/qualifiedRuntime.ts";
import { restoreStagedSelection, retainStagedSelection, selectionPath } from "./stagedSelection.ts";
import { operationBinding, reserveUpdateOperation } from "./launcherOperation.ts";
import { recoverStagedSelection } from "./recoverStagedSelection.ts";

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
  await withQualifiedRuntimeLock(root, "qualified-runtime-lock", async () => undefined);
  await NodeFSP.writeFile(
    NodePath.join(root, "runtime", "service-state.json"),
    JSON.stringify({ protocol: 4, activeVersion: baseline }),
    { mode: 0o600 },
  );
  const input = {
    baseDir: root,
    activeVersion: baseline,
    expectedActiveSource: "a".repeat(40),
    environmentId: selection.binding.environmentId,
    targetSource: "b".repeat(40),
    targetVersion: version,
    stagedHandle: selection.stagedHandle,
  };
  return { root, baseline, selection, input };
}

async function snapshot(root: string): Promise<Record<string, string>> {
  const result: Record<string, string> = {};
  for (const entry of await NodeFSP.readdir(root, { recursive: true, withFileTypes: true })) {
    const path = NodePath.join(entry.parentPath, entry.name);
    if (entry.isFile())
      result[NodePath.relative(root, path)] = (await NodeFSP.readFile(path)).toString("hex");
    else result[NodePath.relative(root, path)] = "directory";
  }
  return result;
}

const apply = (f: Awaited<ReturnType<typeof fixture>>) =>
  recoverStagedSelection({ ...f.input, apply: true });

describe("orphan staged-selection recovery", () => {
  it("observes the missing native selection without creating files, then retains only that pointer", async () => {
    const f = await fixture();
    const before = await snapshot(f.root);
    expect(await restoreStagedSelection(f.root, f.baseline)).toBeUndefined();
    expect((await recoverStagedSelection(f.input)).status).toBe("would-retain");
    expect(await snapshot(f.root)).toEqual(before);
    expect((await apply(f)).status).toBe("retained");
    expect(await restoreStagedSelection(f.root, f.baseline)).toEqual(f.selection);
    const after = await snapshot(f.root);
    const additions = Object.keys(after).filter((key) => !(key in before));
    expect(additions.sort()).toEqual(
      [
        "runtime/jones-updates",
        "runtime/jones-updates/selections",
        NodePath.relative(f.root, selectionPath(f.selection.binding)),
      ].sort(),
    );
    for (const key of Object.keys(before)) expect(after[key]).toBe(before[key]);
    expect((await apply(f)).status).toBe("already-retained");
    expect(await snapshot(f.root)).toEqual(after);
  });

  it.each([
    ["activeVersion", "0.0.0-preview.20261001.1.1"],
    ["expectedActiveSource", "f".repeat(40)],
    ["environmentId", "other-environment"],
    ["targetSource", "f".repeat(40)],
    ["targetVersion", "0.0.0-preview.20261003.1.1"],
    ["stagedHandle", "../bad"],
    ["stagedHandle", "12345678-1234-4234-8234-123456789abc"],
  ])("refuses mismatched %s while preserving all files", async (key, value) => {
    const f = await fixture();
    const before = await snapshot(f.root);
    expect((await recoverStagedSelection({ ...f.input, [key]: value, apply: true })).status).toBe(
      "refused",
    );
    expect(await snapshot(f.root)).toEqual(before);
  });

  it.each(["dbPath", "environmentId"] as const)(
    "refuses a foreign staged binding %s",
    async (key) => {
      const f = await fixture();
      const path = NodePath.join(
        f.root,
        "runtime",
        "staged-updates",
        `${f.selection.stagedHandle}.json`,
      );
      await NodeFSP.writeFile(
        path,
        JSON.stringify({
          ...f.selection,
          binding: {
            ...f.selection.binding,
            [key]: key === "dbPath" ? NodePath.join(f.root, "foreign.sqlite") : "foreign",
          },
        }),
      );
      expect(await apply(f)).toMatchObject({ status: "refused", reason: "binding-mismatch" });
    },
  );

  it("refuses an unsupported host and altered or unenrolled runtime", async () => {
    const f = await fixture();
    expect(
      (
        await recoverStagedSelection(f.input, {
          host: { platform: "win32", architecture: "other" },
        })
      ).status,
    ).toBe("refused");
    const runtime = NodePath.join(f.root, "runtime", "versions", f.input.targetVersion);
    await NodeFSP.appendFile(NodePath.join(runtime, "t3"), "changed");
    expect(await apply(f)).toMatchObject({ status: "refused", reason: "integrity-mismatch" });
    await NodeFSP.unlink(NodePath.join(runtime, ".jones-runtime-receipt.json"));
    expect(await apply(f)).toMatchObject({ status: "refused", reason: "bootstrap-required" });
  });

  it("checks state inside the lock and refuses pending and unreconciled operations", async () => {
    const f = await fixture();
    const pending = {
      id: "update",
      fromVersion: f.baseline,
      targetVersion: f.input.targetVersion,
      dbPath: f.selection.binding.dbPath,
      status: "pending",
      phase: "accepted",
    };
    const result = await recoverStagedSelection(
      { ...f.input, apply: true },
      {
        lock: async (base, name, operation) =>
          withQualifiedRuntimeLock(base, name, async () => {
            await NodeFSP.writeFile(
              NodePath.join(f.root, "runtime", "service-state.json"),
              JSON.stringify({ protocol: 4, activeVersion: f.baseline, update: pending }),
            );
            return operation();
          }),
      },
    );
    expect(result).toMatchObject({ status: "refused", reason: "pending-update" });
    await NodeFSP.writeFile(
      NodePath.join(f.root, "runtime", "service-state.json"),
      JSON.stringify({ protocol: 4, activeVersion: f.baseline }),
    );
    await reserveUpdateOperation(
      f.root,
      "12345678-1234-4234-8234-123456789abc",
      operationBinding(f.selection),
    );
    expect(await apply(f)).toMatchObject({ status: "refused", reason: "unreconciled-operation" });
  });

  it("refuses a busy, missing, foreign-mode or symlink lock without creating one", async () => {
    const f = await fixture();
    await withQualifiedRuntimeLock(f.root, "qualified-runtime-lock", async () => {
      expect(await apply(f)).toMatchObject({ status: "refused", reason: "busy" });
    });
    const lock = NodePath.join(f.root, "runtime", "qualified-runtime-lock.sqlite");
    await NodeFSP.chmod(lock, 0o644);
    expect((await apply(f)).status).toBe("refused");
    await NodeFSP.unlink(lock);
    const before = await snapshot(f.root);
    expect(await recoverStagedSelection(f.input)).toMatchObject({
      status: "refused",
      reason: "lock-unavailable",
    });
    expect(await snapshot(f.root)).toEqual(before);
    await NodeFSP.symlink(NodePath.join(f.root, "userdata", "statev2.sqlite"), lock);
    expect((await apply(f)).status).toBe("refused");
  });

  it("refuses revoked handles", async () => {
    const f = await fixture();
    const operationId = "12345678-1234-4234-8234-123456789abc";
    const directory = NodePath.join(f.root, "runtime", "jones-update-revocations");
    await NodeFSP.mkdir(directory);
    await NodeFSP.writeFile(
      NodePath.join(directory, `${operationId}.json`),
      JSON.stringify({ schema: 1, operationId, binding: operationBinding(f.selection) }),
      { mode: 0o600 },
    );
    expect(await apply(f)).toMatchObject({ status: "refused", reason: "stage-revoked-or-unknown" });
  });

  it.each(["different", "invalid", "mode", "symlink"] as const)(
    "preserves an occupied %s selection",
    async (kind) => {
      const f = await fixture();
      const path = selectionPath(f.selection.binding);
      if (kind === "different") {
        const other = { ...f.selection, stagedHandle: NodeCrypto.randomUUID() };
        await NodeFSP.writeFile(
          NodePath.join(f.root, "runtime", "staged-updates", `${other.stagedHandle}.json`),
          JSON.stringify(other),
          { mode: 0o600 },
        );
        await retainStagedSelection(other);
      } else {
        await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true });
        if (kind === "symlink")
          await NodeFSP.symlink(NodePath.join(f.root, "userdata", "statev2.sqlite"), path);
        else
          await NodeFSP.writeFile(path, kind === "invalid" ? "{" : JSON.stringify(f.selection), {
            mode: kind === "mode" ? 0o644 : 0o600,
          });
      }
      const bytes = await NodeFSP.readFile(path);
      expect((await apply(f)).status).toBe("refused");
      expect(await NodeFSP.readFile(path)).toEqual(bytes);
    },
  );

  it("refuses symlink selection ancestry", async () => {
    const f = await fixture();
    await NodeFSP.symlink(
      NodePath.join(f.root, "userdata"),
      NodePath.join(f.root, "runtime", "jones-updates"),
    );
    expect(await apply(f)).toMatchObject({ status: "refused", reason: "directory-ownership" });
  });

  it("reports write/fsync ambiguity as unknown and converges only after verified readback", async () => {
    const f = await fixture();
    const fail = async () => {
      throw new Error("injected fsync failure");
    };
    expect(
      await recoverStagedSelection({ ...f.input, apply: true }, { retain: fail }),
    ).toMatchObject({ status: "unknown", reason: "retention-incomplete" });
    expect(await recoverStagedSelection({ ...f.input, apply: true }, { sync: fail })).toMatchObject(
      { status: "unknown", reason: "durability-unconfirmed" },
    );
    expect(await restoreStagedSelection(f.root, f.baseline)).toEqual(f.selection);
    expect(await recoverStagedSelection(f.input, { sync: fail })).toMatchObject({
      status: "unknown",
      reason: "durability-unconfirmed",
    });
    expect((await apply(f)).status).toBe("already-retained");
  });
});

it("refuses an independently held SQLite transaction", async () => {
  const f = await fixture();
  const database = new NodeSqlite.DatabaseSync(
    NodePath.join(f.root, "runtime", "qualified-runtime-lock.sqlite"),
  );
  try {
    database.exec("BEGIN EXCLUSIVE");
    expect(await apply(f)).toMatchObject({ status: "refused", reason: "busy" });
  } finally {
    database.close();
  }
});

it("preserves partial pointer bytes after an ambiguous write", async () => {
  const f = await fixture();
  const path = selectionPath(f.selection.binding);
  const result = await recoverStagedSelection(
    { ...f.input, apply: true },
    {
      retain: async () => {
        await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true });
        await NodeFSP.writeFile(path, "{", { mode: 0o600 });
        throw new Error("injected partial write");
      },
    },
  );
  expect(result.status).toBe("unknown");
  expect((await apply(f)).status).toBe("refused");
  expect(await NodeFSP.readFile(path, "utf8")).toBe("{");
});

it("restores the legacy binding key order and selection serialization", async () => {
  const f = await fixture();
  // 76fb3dc uses these binding keys, in this order, as the SHA256 selection key.
  const binding = {
    baseDir: f.root,
    dbPath: NodePath.join(f.root, "userdata", "statev2.sqlite"),
    environmentId: "own-environment",
    activeVersion: f.baseline,
    activeSourceSha: "a".repeat(40),
  };
  const key = NodeCrypto.createHash("sha256").update(JSON.stringify(binding)).digest("hex");
  const path = NodePath.join(f.root, "runtime", "jones-updates", "selections", `${key}.json`);
  await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true });
  await NodeFSP.writeFile(
    path,
    JSON.stringify({
      protocol: 1,
      stagedHandle: f.selection.stagedHandle,
      binding,
      receipt: f.selection.receipt,
    }) + "\n",
    { mode: 0o600 },
  );
  expect(await restoreStagedSelection(f.root, f.baseline)).toEqual(f.selection);
  expect((await apply(f)).status).toBe("already-retained");
});

it("refuses malformed state and an embedded handle differing from the requested handle", async () => {
  const f = await fixture();
  await NodeFSP.writeFile(
    NodePath.join(f.root, "runtime", "staged-updates", `${f.selection.stagedHandle}.json`),
    JSON.stringify({ ...f.selection, stagedHandle: NodeCrypto.randomUUID() }),
  );
  expect(await apply(f)).toMatchObject({ status: "refused", reason: "expected-binding-mismatch" });
  await NodeFSP.writeFile(NodePath.join(f.root, "runtime", "service-state.json"), "{");
  expect(await apply(f)).toMatchObject({ status: "refused", reason: "service-state-invalid" });
});
