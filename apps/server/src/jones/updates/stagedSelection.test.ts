// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import {
  currentQualifiedRuntimeBinding,
  qualifiedPayloadDigest,
  type QualifiedRuntimeReceipt,
  type StagedQualifiedRuntime,
} from "../cloud/qualifiedRuntime.ts";
import {
  retainStagedSelection,
  restoreStagedSelection,
  retireStagedSelection,
} from "./stagedSelection.ts";

import { assertStageNotRevoked, retireNativeStage } from "./stageRevocation.ts";
import {
  operationBinding,
  publishUpdateOperationSettlement,
  reconcileUpdateOperation,
  reserveUpdateOperation,
  writeFailedUpdateOperation,
} from "./launcherOperation.ts";

vi.mock("node:fs/promises", async (importOriginal) => {
  const original = await importOriginal<typeof NodeFSP>();
  return {
    ...original,
    opendir: vi.fn(original.opendir),
    lstat: vi.fn(original.lstat),
    open: vi.fn(original.open),
  };
});

const roots: string[] = [];
afterEach(async () => {
  const original = await vi.importActual<typeof NodeFSP>("node:fs/promises");
  vi.mocked(NodeFSP.opendir).mockReset().mockImplementation(original.opendir);
  vi.mocked(NodeFSP.lstat).mockReset().mockImplementation(original.lstat);
  vi.mocked(NodeFSP.open).mockReset().mockImplementation(original.open);
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

it("retires an exact selection pointer while retaining its qualified receipt and payload", async () => {
  const f = await fixture();
  await retainStagedSelection(f.selection);
  const expected = {
    environmentId: f.selection.binding.environmentId,
    expectedInstalledSource: f.selection.binding.activeSourceSha,
    targetSource: f.selection.receipt.sourceSha,
    stagedHandle: f.selection.stagedHandle,
  };
  await expect(
    retireStagedSelection(f.root, f.baseline, { ...expected, stagedHandle: "other" }),
  ).rejects.toThrow("preserved");
  expect(await restoreStagedSelection(f.root, f.baseline)).toEqual(f.selection);
  await retireStagedSelection(f.root, f.baseline, expected);
  await retireStagedSelection(f.root, f.baseline, expected);
  expect(await restoreStagedSelection(f.root, f.baseline)).toBeUndefined();
  expect(
    JSON.parse(
      await NodeFSP.readFile(
        NodePath.join(f.root, "runtime", "staged-updates", `${f.selection.stagedHandle}.json`),
        "utf8",
      ),
    ),
  ).toEqual(f.selection);
  expect(
    await NodeFSP.readFile(
      NodePath.join(f.root, "runtime", "versions", f.selection.receipt.version, "t3"),
      "utf8",
    ),
  ).toContain("echo b");
});

// Generate released revocation files lazily; keep real stage/pointer publication and
// tiny malformed-tail fixtures on disk under the existing afterEach cleanup owner.
async function streamedRevocations(f: Awaited<ReturnType<typeof fixture>>, count: number) {
  const original = await vi.importActual<typeof NodeFSP>("node:fs/promises");
  const path = NodePath.join(f.root, "runtime", "jones-update-revocations");
  await NodeFSP.mkdir(path, { mode: 0o700 });
  const sample = NodePath.join(f.root, "revocation-template");
  await NodeFSP.writeFile(sample, "{}", { mode: 0o600 });
  const stat = await original.lstat(sample);
  const id = (index: number) =>
    `${index.toString(16).padStart(8, "0")}-1234-4234-8234-123456789abc`;
  const indexOf = (file: unknown) => {
    if (typeof file !== "string" || NodePath.dirname(file) !== path) return undefined;
    const index = Number.parseInt(NodePath.basename(file).slice(0, 8), 16);
    return index < count && NodePath.basename(file) === `${id(index)}.json` ? index : undefined;
  };
  const counters = { reads: 0, active: 0, peak: 0, unread: 0, lead: 0, walks: 0, closed: 0 };
  vi.mocked(NodeFSP.opendir).mockImplementation(async (...args) => {
    if (String(args[0]) !== path) return original.opendir(...args);
    counters.walks++;
    return {
      async *[Symbol.asyncIterator]() {
        try {
          for (let i = 0; i < count; i++) {
            counters.unread++;
            counters.lead = Math.max(counters.lead, counters.unread);
            yield { name: `${id(i)}.json`, isFile: () => true };
          }
          for await (const entry of await original.opendir(path)) yield entry;
        } finally {
          counters.closed++;
        }
      },
    } as unknown as Awaited<ReturnType<typeof NodeFSP.opendir>>;
  });
  vi.mocked(NodeFSP.lstat).mockImplementation(async (...args) => {
    vi.mocked(NodeFSP.lstat).mockClear();
    return indexOf(args[0]) === undefined ? original.lstat(...args) : stat;
  });
  vi.mocked(NodeFSP.open).mockImplementation(async (...args) => {
    vi.mocked(NodeFSP.open).mockClear();
    const index = indexOf(args[0]);
    if (index === undefined) return original.open(...args);
    counters.unread = 0;
    counters.reads++;
    counters.active++;
    counters.peak = Math.max(counters.peak, counters.active);
    const bytes = Buffer.from(
      JSON.stringify({
        schema: 1,
        operationId: id(index),
        binding: { ...operationBinding(f.selection), stagedHandle: `retired-${index}` },
      }),
    );
    return {
      stat: async () => stat,
      read: async (buffer: Buffer, offset: number, length: number, position: number) => ({
        bytesRead: bytes.copy(buffer, offset, position, position + length),
        buffer,
      }),
      close: async () => {
        counters.active--;
      },
    } as unknown as Awaited<ReturnType<typeof NodeFSP.open>>;
  });
  return { path, id, counters };
}

it.each([4096, 4097])(
  "validates %i revocations with bounded streaming and permits unrelated stage retirement",
  async (count) => {
    const f = await fixture();
    await retainStagedSelection(f.selection);
    const { path, id, counters } = await streamedRevocations(f, count);
    await assertStageNotRevoked(f.root, f.selection.stagedHandle);
    await expect(assertStageNotRevoked(f.root, `retired-${count - 1}`)).rejects.toThrow(
      "durably retired",
    );
    await expect(assertStageNotRevoked(f.root, "unrelated", id(count - 1))).rejects.toThrow(
      "durably retired",
    );
    const input = {
      operationId: "12345678-1234-4234-8234-123456789abc",
      ...operationBinding(f.selection),
    };
    await expect(
      retireNativeStage(f.root, f.baseline, undefined, { ...input, operationId: id(count - 1) }),
    ).rejects.toThrow("already names another immutable binding");
    expect(await restoreStagedSelection(f.root, f.baseline)).toEqual(f.selection);
    await retireNativeStage(f.root, f.baseline, undefined, input);
    expect(
      JSON.parse(await NodeFSP.readFile(NodePath.join(path, `${input.operationId}.json`), "utf8")),
    ).toEqual({
      schema: 1,
      operationId: input.operationId,
      binding: operationBinding(f.selection),
    });
    expect(await restoreStagedSelection(f.root, f.baseline)).toBeUndefined();
    await expect(assertStageNotRevoked(f.root, input.stagedHandle)).rejects.toThrow(
      "durably retired",
    );
    expect(counters.reads).toBeGreaterThanOrEqual(count * 6);
    expect(counters.reads).toBeLessThanOrEqual(count * 8);
    expect(counters.peak).toBe(1);
    expect(counters.active).toBe(0);
    expect(counters.lead).toBe(1);
    expect(counters.closed).toBe(counters.walks);
  },
);

it.each(["malformed", "public", "symlink", "unknown", "interrupted"])(
  "holds %s revocation evidence after the old cap before pointer mutation or admission",
  async (damage) => {
    const f = await fixture();
    await retainStagedSelection(f.selection);
    const { path, counters } = await streamedRevocations(f, 4097);
    const input = {
      operationId: "12345678-1234-4234-8234-123456789abc",
      ...operationBinding(f.selection),
    };
    const tail = NodePath.join(path, `${input.operationId}.json`);
    const row = JSON.stringify({
      schema: 1,
      operationId: input.operationId,
      binding: operationBinding(f.selection),
    });
    if (damage === "interrupted") {
      const walk = vi.mocked(NodeFSP.opendir).getMockImplementation()!;
      vi.mocked(NodeFSP.opendir).mockImplementation(async (...args) => {
        const directory = await walk(...args);
        if (String(args[0]) !== path) return directory;
        return {
          async *[Symbol.asyncIterator]() {
            for await (const entry of directory) yield entry;
            throw new Error("injected incomplete revocation traversal");
          },
        } as unknown as Awaited<ReturnType<typeof NodeFSP.opendir>>;
      });
    } else if (damage === "symlink") {
      const target = NodePath.join(f.root, "revocation-copy");
      await NodeFSP.writeFile(target, row, { mode: 0o600 });
      await NodeFSP.symlink(target, tail);
    } else {
      await NodeFSP.writeFile(
        damage === "unknown" ? NodePath.join(path, "unknown-entry") : tail,
        damage === "malformed" ? "{" : row,
        { mode: damage === "public" ? 0o644 : 0o600 },
      );
    }
    await expect(assertStageNotRevoked(f.root, f.selection.stagedHandle)).rejects.toThrow();
    await expect(retireNativeStage(f.root, f.baseline, undefined, input)).rejects.toThrow();
    expect(await restoreStagedSelection(f.root, f.baseline)).toEqual(f.selection);
    expect(counters.reads).toBe(4097 * 2);
    expect(counters.active).toBe(0);
    expect(counters.closed).toBe(counters.walks);
  },
);

it("persists exact revocation before retirement and safely replays a lost reply", async () => {
  const f = await fixture();
  await retainStagedSelection(f.selection);
  const input = {
    operationId: "12345678-1234-4234-8234-123456789abc",
    ...operationBinding(f.selection),
  };
  await retireNativeStage(f.root, f.baseline, undefined, input);
  await retireNativeStage(f.root, f.baseline, undefined, input);
  expect(await restoreStagedSelection(f.root, f.baseline)).toBeUndefined();
  await expect(assertStageNotRevoked(f.root, input.stagedHandle)).rejects.toThrow(
    "durably retired",
  );
  await expect(
    retireNativeStage(f.root, f.baseline, undefined, { ...input, targetSource: "f".repeat(40) }),
  ).rejects.toThrow("binding");
  const replacement = { ...f.selection, stagedHandle: NodeCrypto.randomUUID() };
  await NodeFSP.writeFile(
    NodePath.join(f.root, "runtime", "staged-updates", `${replacement.stagedHandle}.json`),
    JSON.stringify(replacement),
    { mode: 0o600 },
  );
  await expect(
    retireNativeStage(f.root, f.baseline, undefined, {
      ...input,
      stagedHandle: replacement.stagedHandle,
    }),
  ).rejects.toThrow("already names another");
  await expect(
    assertStageNotRevoked(f.root, replacement.stagedHandle, input.operationId),
  ).rejects.toThrow("durably retired");
  await assertStageNotRevoked(f.root, replacement.stagedHandle);
  expect(
    await NodeFSP.readFile(
      NodePath.join(f.root, "runtime", "versions", f.selection.receipt.version, "t3"),
      "utf8",
    ),
  ).toContain("echo b");
});

it("permits retirement after settlement while refusing reuse of the accepted operation", async () => {
  const f = await fixture();
  await retainStagedSelection(f.selection);
  const operationId = "12345678-1234-4234-8234-123456789abc";
  const failed = {
    id: operationId,
    status: "failed" as const,
    reason: "launcher-restarted",
    fromVersion: f.baseline,
    targetVersion: f.selection.receipt.version,
    qualified: f.selection,
  };
  await reserveUpdateOperation(f.root, operationId, operationBinding(f.selection));
  const digests = await writeFailedUpdateOperation(f.root, failed);
  const input = {
    operationId: "32345678-1234-4234-8234-123456789abc",
    ...operationBinding(f.selection),
  };
  await expect(retireNativeStage(f.root, f.baseline, failed, input)).rejects.toMatchObject({
    code: "failed-operation-unsettled",
    operationId,
  });
  expect(await restoreStagedSelection(f.root, f.baseline)).toEqual(f.selection);
  await publishUpdateOperationSettlement(f.root, {
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
      activeVersion: f.baseline,
      activeSourceSha: f.selection.binding.activeSourceSha,
      recoveryPayload: "absent",
      restorePending: false,
      cause: null,
      writerLauncherVersion: f.baseline,
      recordedAt: "2026-10-23T04:00:00.000Z",
    },
  });
  await expect(
    retireNativeStage(f.root, f.baseline, failed, { ...input, operationId }),
  ).rejects.toThrow("may have been accepted");
  await retireNativeStage(f.root, f.baseline, failed, input);
  expect(await restoreStagedSelection(f.root, f.baseline)).toBeUndefined();
  expect(await reconcileUpdateOperation(f.root, operationId, undefined)).toMatchObject({
    state: "blocked",
    updateId: operationId,
    settlement: { basis: "launcher-recorded" },
  });
  expect(await NodeFSP.readFile(NodePath.join(f.root, "userdata", "statev2.sqlite"), "utf8")).toBe(
    "unchanged-state",
  );
});

it("preserves a stage after ambiguous native acceptance or revocation publication", async () => {
  const f = await fixture();
  await retainStagedSelection(f.selection);
  const input = {
    operationId: "12345678-1234-4234-8234-123456789abc",
    ...operationBinding(f.selection),
  };
  const revocations = NodePath.join(f.root, "runtime", "jones-update-revocations");
  await NodeFSP.mkdir(revocations);
  await NodeFSP.writeFile(NodePath.join(revocations, `${input.operationId}.json`), "{", {
    mode: 0o600,
  });
  await expect(retireNativeStage(f.root, f.baseline, undefined, input)).rejects.toThrow();
  await expect(assertStageNotRevoked(f.root, input.stagedHandle)).rejects.toThrow();
  expect(await restoreStagedSelection(f.root, f.baseline)).toEqual(f.selection);
  await reserveUpdateOperation(f.root, input.operationId, operationBinding(f.selection));
  await expect(retireNativeStage(f.root, f.baseline, undefined, input)).rejects.toThrow(
    "may have been accepted",
  );
  expect(await restoreStagedSelection(f.root, f.baseline)).toEqual(f.selection);
});

it("retries directory durability after a pointer removal succeeded but fsync failed", async () => {
  const f = await fixture();
  await retainStagedSelection(f.selection);
  const expected = {
    environmentId: f.selection.binding.environmentId,
    expectedInstalledSource: f.selection.binding.activeSourceSha,
    targetSource: f.selection.receipt.sourceSha,
    stagedHandle: f.selection.stagedHandle,
  };
  const synced: string[] = [];
  const syncDirectory = async (directory: string) => {
    synced.push(directory);
    if (synced.length <= 2)
      throw Object.assign(new Error("injected directory fsync failure"), { code: "EIO" });
    const parent = await NodeFSP.open(directory, "r");
    try {
      await parent.sync();
    } finally {
      await parent.close();
    }
  };
  await expect(retireStagedSelection(f.root, f.baseline, expected, syncDirectory)).rejects.toThrow(
    "fsync failure",
  );
  expect(await restoreStagedSelection(f.root, f.baseline)).toBeUndefined();
  await expect(retireStagedSelection(f.root, f.baseline, expected, syncDirectory)).rejects.toThrow(
    "fsync failure",
  );
  await retireStagedSelection(f.root, f.baseline, expected, syncDirectory);
  expect(synced).toEqual(
    Array(3).fill(NodePath.join(f.root, "runtime", "jones-updates", "selections")),
  );
  expect(
    JSON.parse(
      await NodeFSP.readFile(
        NodePath.join(f.root, "runtime", "staged-updates", `${f.selection.stagedHandle}.json`),
        "utf8",
      ),
    ),
  ).toEqual(f.selection);
});

it("reestablishes pointer durability after publication succeeds but directory fsync fails", async () => {
  const f = await fixture();
  let attempts = 0;
  const syncDirectory = async (directory: string) => {
    attempts += 1;
    if (attempts < 3) throw new Error("injected pointer fsync failure");
    const parent = await NodeFSP.open(directory, "r");
    try {
      await parent.sync();
    } finally {
      await parent.close();
    }
  };
  await expect(retainStagedSelection(f.selection, syncDirectory)).rejects.toThrow(
    "pointer fsync failure",
  );
  expect(await restoreStagedSelection(f.root, f.baseline)).toEqual(f.selection);
  await expect(retainStagedSelection(f.selection, syncDirectory)).rejects.toThrow(
    "pointer fsync failure",
  );
  await retainStagedSelection(f.selection, syncDirectory);
  expect(attempts).toBe(3);
  expect(await restoreStagedSelection(f.root, f.baseline)).toEqual(f.selection);
});
