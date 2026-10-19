// @effect-diagnostics nodeBuiltinImport:off - Synthetic native fixtures exercise real receipts and archive validation.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeZlib from "node:zlib";
import { afterEach, expect, it } from "vite-plus/test";
import {
  jonesCandidateHandle,
  type JonesActionsCandidate,
} from "@t3tools/shared/jones/jonesActions";
import {
  currentQualifiedRuntimeBinding,
  qualifiedPayloadDigest,
  stageQualifiedRuntime,
  type QualifiedRuntimeReceipt,
} from "../cloud/qualifiedRuntime.ts";
import { retainStagedSelection, restoreStagedSelection } from "./stagedSelection.ts";
import {
  applyQualifiedStageRecovery,
  inspectQualifiedStageRecovery,
  type RecoveryApi,
  type RecoveryInput,
} from "./recoverQualifiedStage.ts";

const roots: string[] = [];
afterEach(async () => {
  for (const root of roots.splice(0)) {
    await NodeFSP.rm(root, { recursive: true, force: true });
    await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
  }
});
const sha = (value: Buffer | string) => NodeCrypto.createHash("sha256").update(value).digest("hex");

function zip(entries: ReadonlyArray<{ name: string; bytes: Buffer }>): Buffer {
  const local: Buffer[] = [],
    central: Buffer[] = [];
  let offset = 0;
  for (const entry of entries) {
    const name = Buffer.from(entry.name),
      crc = NodeZlib.crc32(entry.bytes);
    const header = Buffer.alloc(30);
    header.writeUInt32LE(0x04034b50);
    header.writeUInt16LE(20, 4);
    header.writeUInt32LE(crc, 14);
    header.writeUInt32LE(entry.bytes.length, 18);
    header.writeUInt32LE(entry.bytes.length, 22);
    header.writeUInt16LE(name.length, 26);
    const index = Buffer.alloc(46);
    index.writeUInt32LE(0x02014b50);
    index.writeUInt16LE(0x0314, 4);
    index.writeUInt16LE(20, 6);
    index.writeUInt32LE(crc, 16);
    index.writeUInt32LE(entry.bytes.length, 20);
    index.writeUInt32LE(entry.bytes.length, 24);
    index.writeUInt16LE(name.length, 28);
    index.writeUInt32LE((0o100600 << 16) >>> 0, 38);
    index.writeUInt32LE(offset, 42);
    local.push(header, name, entry.bytes);
    central.push(index, name);
    offset += header.length + name.length + entry.bytes.length;
  }
  const directory = Buffer.concat(central),
    end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50);
  end.writeUInt16LE(entries.length, 8);
  end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(directory.length, 12);
  end.writeUInt32LE(offset, 16);
  return Buffer.concat([...local, directory, end]);
}

async function fixture() {
  const allocated = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-recover-stage-"));
  roots.push(allocated);
  const base = await NodeFSP.realpath(allocated);
  const currentVersion = "0.1.0-preview.20261018.100.1",
    targetVersion = "0.1.0-preview.20261018.101.1";
  const source = "a".repeat(40),
    currentSource = "c".repeat(40),
    tree = "b".repeat(40);
  const workflow = ".github/workflows/artifact-desktop-mac.yml";
  const dmg = Buffer.from("synthetic verified DMG bytes"),
    artifactName = `T3-Code-${targetVersion}-arm64.dmg`;
  const manifest = {
    schema: 1,
    repository: "Jones-Systems/Jones-Code",
    source,
    tree,
    workflow,
    event: "push",
    ref: "refs/heads/main",
    runId: "101",
    runAttempt: "1",
    version: targetVersion,
    platform: "darwin",
    architecture: "arm64",
    artifact: artifactName,
    sha256: sha(dmg),
  };
  const archive = zip([
    { name: "ARTIFACT.json", bytes: Buffer.from(JSON.stringify(manifest)) },
    { name: artifactName, bytes: dmg },
    { name: "SOURCE_COMMIT", bytes: Buffer.from(source) },
    { name: `${artifactName}.sha256`, bytes: Buffer.from(sha(dmg)) },
  ]);
  const candidate: JonesActionsCandidate = {
    schema: 1,
    repository: "Jones-Systems/Jones-Code",
    source,
    tree,
    installedSource: "9".repeat(40),
    workflow,
    workflowId: 42,
    runId: 101,
    runAttempt: 1,
    ciRunId: 102,
    artifactId: 103,
    artifactName: `desktop-mac-arm64-101-1--${targetVersion}`,
    artifactDigest: sha(archive),
    artifactBytes: archive.length,
    expiresAt: "2026-12-01T00:00:00Z",
    version: targetVersion,
    platform: "darwin",
    architecture: "arm64",
  };
  const stage = NodePath.join(base, "transport", jonesCandidateHandle(candidate));
  await NodeFSP.mkdir(stage, { recursive: true });
  await NodeFSP.writeFile(NodePath.join(stage, "github-artifact.zip"), archive);
  await NodeFSP.writeFile(NodePath.join(stage, artifactName), dmg);
  await NodeFSP.writeFile(
    NodePath.join(stage, "jones-actions-stage.json"),
    JSON.stringify({
      schema: 1,
      source: "jones-actions",
      channel: "jones-main",
      stagedHandle: jonesCandidateHandle(candidate),
      candidate,
      receipt: manifest,
      payloadPath: NodePath.join(stage, artifactName),
    }),
  );
  let targetReceipt!: QualifiedRuntimeReceipt;
  for (const [version, sourceSha] of [
    [currentVersion, currentSource],
    [targetVersion, source],
  ]) {
    const directory = NodePath.join(base, "runtime", "versions", version!);
    await NodeFSP.mkdir(directory, { recursive: true });
    await NodeFSP.writeFile(NodePath.join(directory, "t3"), "#!/bin/sh\nexit 0\n", { mode: 0o700 });
    const receipt: QualifiedRuntimeReceipt = {
      protocol: 1,
      repository: "Jones-Systems/Jones-Code",
      channel: "jones-main",
      version: version!,
      sourceSha: sourceSha!,
      sourceTree: tree,
      installedSourceSha: "9".repeat(40),
      runId: 101,
      runAttempt: 1,
      artifactId: 103,
      workflow,
      artifactDigest: `sha256:${sha(archive)}`,
      archiveSha256: sha(dmg),
      platform: "darwin",
      architecture: "arm64",
      payloadSha256: await qualifiedPayloadDigest(directory, "darwin"),
    };
    await NodeFSP.writeFile(
      NodePath.join(directory, ".jones-runtime-receipt.json"),
      JSON.stringify(receipt),
      { mode: 0o600 },
    );
    if (version === targetVersion) targetReceipt = receipt;
  }
  await NodeFSP.mkdir(NodePath.join(base, "userdata"));
  await NodeFSP.writeFile(NodePath.join(base, "userdata", "environment-id"), "fixture-env");
  await NodeFSP.writeFile(
    NodePath.join(base, "userdata", "statev2.sqlite"),
    "never opened database",
  );
  await NodeFSP.writeFile(
    NodePath.join(base, "runtime", "service-state.json"),
    JSON.stringify({ protocol: 4, activeVersion: currentVersion }),
  );
  const adoption = JSON.stringify({
    schema: 1,
    status: "applied",
    baseDir: base,
    environmentId: "fixture-env",
    activeVersion: currentVersion,
    launcherVersion: targetVersion,
  });
  await NodeFSP.writeFile(NodePath.join(base, "runtime", "jones-host-adoption.json"), adoption, {
    mode: 0o600,
  });
  const input: RecoveryInput = {
    operationId: NodeCrypto.randomUUID(),
    baseDir: base,
    currentVersion,
    currentSource,
    environmentId: "fixture-env",
    targetVersion,
    targetSource: source,
    targetTree: tree,
    runId: 101,
    runAttempt: 1,
    artifactId: 103,
    archiveSha256: sha(archive),
    transportStageDirectory: stage,
    transportStageReceiptSha256: sha(
      await NodeFSP.readFile(NodePath.join(stage, "jones-actions-stage.json")),
    ),
    adoptionReceiptSha256: sha(adoption),
    runtimeReceiptSha256: sha(JSON.stringify(targetReceipt)),
  };
  const run = {
    id: 101,
    run_attempt: 1,
    workflow_id: 42,
    path: workflow,
    head_sha: source,
    head_branch: "main",
    event: "push",
    status: "completed",
    conclusion: "success",
    repository: { id: 1, full_name: "Jones-Systems/Jones-Code" },
    head_repository: { id: 1, full_name: "Jones-Systems/Jones-Code" },
  };
  const responses = new Map<string, { status: number; body?: unknown }>([
    ["actions/runs/101/attempts/1", { status: 200, body: run }],
    [
      "actions/runs/102",
      { status: 200, body: { ...run, id: 102, path: ".github/workflows/ci.yml" } },
    ],
    [`git/commits/${source}`, { status: 200, body: { sha: source, tree: { sha: tree } } }],
    ["commits/main", { status: 200, body: { sha: source } }],
    [
      `compare/${currentSource}...${source}`,
      { status: 200, body: { status: "ahead", merge_base_commit: { sha: currentSource } } },
    ],
    ["actions/artifacts/103", { status: 404 }],
  ]);
  const requests: string[] = [];
  const api: RecoveryApi = {
    nativeHost: { platform: "darwin", architecture: "arm64" },
    get: async (endpoint) => {
      const path = endpoint.replace("repos/Jones-Systems/Jones-Code/", "");
      requests.push(path);
      const value = responses.get(path);
      if (value === undefined) throw new Error(`Unexpected API endpoint: ${path}`);
      return value;
    },
  };
  return { input, api, base, targetReceipt, responses, run, requests, stage, candidate };
}

it("recovers an already enrolled original attempt with unavailable metadata and preserves the D0 selection protocol", async () => {
  const f = await fixture();
  const target = NodePath.join(f.base, "runtime", "versions", f.input.targetVersion);
  const before = await NodeFSP.readFile(NodePath.join(target, ".jones-runtime-receipt.json"));
  const inode = (await NodeFSP.stat(NodePath.join(target, "t3"))).ino;
  const plan = await inspectQualifiedStageRecovery(f.input, f.api);
  expect(plan.artifactMetadata).toBe("unavailable-404");
  expect(plan.trustBasis).toBe("preserved-original-admission");
  await expect(
    NodeFSP.lstat(NodePath.join(f.base, "runtime", "jones-stage-recoveries")),
  ).rejects.toMatchObject({ code: "ENOENT" });
  const selected = await applyQualifiedStageRecovery(plan, f.api);
  expect(selected.stagedHandle).toBe(f.input.operationId);
  expect(selected.receipt).toEqual(f.targetReceipt);
  expect(selected.binding.activeSourceSha).toBe(f.input.currentSource);
  expect(await restoreStagedSelection(f.base, f.input.currentVersion, f.api.nativeHost)).toEqual(
    selected,
  );
  expect(await applyQualifiedStageRecovery(plan, f.api)).toEqual(selected);
  expect(await NodeFSP.readFile(NodePath.join(target, ".jones-runtime-receipt.json"))).toEqual(
    before,
  );
  expect((await NodeFSP.stat(NodePath.join(target, "t3"))).ino).toBe(inode);
  expect(await NodeFSP.readFile(NodePath.join(f.base, "userdata", "statev2.sqlite"), "utf8")).toBe(
    "never opened database",
  );
  expect(f.requests).toContain("actions/runs/101/attempts/1");
  expect(f.requests).not.toContain("actions/runs/101");
});

it.each([
  "missing-adoption",
  "invented-checksum",
  "wrong-attempt",
  "zip-drift",
  "payload-drift",
  "native-drift",
  "current-source",
  "pending",
  "conflicting-metadata",
  "revoked",
])("refuses %s without publishing a stage", async (fault) => {
  const f = await fixture();
  let input = f.input;
  if (fault === "missing-adoption")
    await NodeFSP.unlink(NodePath.join(f.base, "runtime", "jones-host-adoption.json"));
  if (fault === "invented-checksum") input = { ...input, runtimeReceiptSha256: "0".repeat(64) };
  if (fault === "wrong-attempt")
    f.responses.set("actions/runs/101/attempts/1", {
      status: 200,
      body: { ...f.run, run_attempt: 2, conclusion: "cancelled" },
    });
  if (fault === "zip-drift")
    await NodeFSP.appendFile(NodePath.join(f.stage, "github-artifact.zip"), "corrupt");
  if (fault === "payload-drift")
    await NodeFSP.appendFile(
      NodePath.join(f.base, "runtime", "versions", input.targetVersion, "t3"),
      "changed",
    );
  if (fault === "native-drift")
    await NodeFSP.writeFile(NodePath.join(f.base, "userdata", "environment-id"), "foreign");
  if (fault === "current-source") input = { ...input, currentSource: "f".repeat(40) };
  if (fault === "pending")
    await NodeFSP.writeFile(
      NodePath.join(f.base, "runtime", "service-state.json"),
      JSON.stringify({
        protocol: 4,
        activeVersion: input.currentVersion,
        update: {
          id: "pending",
          status: "pending",
          phase: "accepted",
          fromVersion: input.currentVersion,
          targetVersion: input.targetVersion,
          dbPath: NodePath.join(f.base, "userdata", "statev2.sqlite"),
        },
      }),
    );
  if (fault === "conflicting-metadata")
    f.responses.set("actions/artifacts/103", { status: 200, body: { id: 999, workflow_run: {} } });
  if (fault === "revoked") {
    const directory = NodePath.join(f.base, "runtime", "jones-update-revocations");
    await NodeFSP.mkdir(directory);
    await NodeFSP.writeFile(
      NodePath.join(directory, `${input.operationId}.json`),
      JSON.stringify({
        schema: 1,
        operationId: input.operationId,
        binding: {
          environmentId: input.environmentId,
          currentVersion: input.currentVersion,
          expectedInstalledSource: input.currentSource,
          targetSource: input.targetSource,
          stagedHandle: input.operationId,
          baseDir: f.base,
          dbPath: NodePath.join(f.base, "userdata", "statev2.sqlite"),
          targetVersion: input.targetVersion,
        },
      }),
      { mode: 0o600 },
    );
  }
  await expect(inspectQualifiedStageRecovery(input, f.api)).rejects.toThrow();
  await expect(
    NodeFSP.lstat(NodePath.join(f.base, "runtime", "staged-updates")),
  ).rejects.toMatchObject({ code: "ENOENT" });
});

it("reconciles the same fixed handle after interruption between stage and pointer publication", async () => {
  const f = await fixture(),
    plan = await inspectQualifiedStageRecovery(f.input, f.api);
  const directory = NodePath.join(f.base, "runtime", "jones-stage-recoveries");
  await NodeFSP.mkdir(directory, { mode: 0o700 });
  await NodeFSP.writeFile(
    NodePath.join(directory, `${f.input.operationId}.json`),
    JSON.stringify(plan),
    { mode: 0o600 },
  );
  const { protocol: _protocol, payloadSha256: _payload, ...artifact } = f.targetReceipt;
  const stage = await stageQualifiedRuntime({
    host: f.api.nativeHost!,
    stagedHandle: f.input.operationId,
    binding: plan.binding,
    artifact: {
      ...artifact,
      installedSourceSha: f.input.currentSource,
      payloadDirectory: NodePath.join(f.base, "runtime", "versions", f.input.targetVersion),
    },
    validate: async () => {},
  });
  expect(
    await restoreStagedSelection(f.base, f.input.currentVersion, f.api.nativeHost),
  ).toBeUndefined();
  expect(await applyQualifiedStageRecovery(plan, f.api)).toEqual(stage);
  expect(await NodeFSP.readdir(NodePath.join(f.base, "runtime", "staged-updates"))).toEqual([
    `${f.input.operationId}.json`,
  ]);
});

it("refuses a foreign selection, an unknown partial publication and a changed apply preimage", async () => {
  const f = await fixture(),
    plan = await inspectQualifiedStageRecovery(f.input, f.api);
  const binding = await currentQualifiedRuntimeBinding(
    f.base,
    f.input.currentVersion,
    f.api.nativeHost,
  );
  const { protocol: _protocol, payloadSha256: _payload, ...artifact } = f.targetReceipt;
  const foreign = await stageQualifiedRuntime({
    host: f.api.nativeHost!,
    binding,
    artifact: {
      ...artifact,
      installedSourceSha: f.input.currentSource,
      payloadDirectory: NodePath.join(f.base, "runtime", "versions", f.input.targetVersion),
    },
    validate: async () => {},
  });
  await retainStagedSelection(foreign);
  await expect(applyQualifiedStageRecovery(plan, f.api)).rejects.toThrow(
    "foreign staged selection",
  );
  const g = await fixture(),
    other = await inspectQualifiedStageRecovery(g.input, g.api);
  const directory = NodePath.join(g.base, "runtime", "staged-updates");
  await NodeFSP.mkdir(directory);
  await NodeFSP.writeFile(NodePath.join(directory, `${g.input.operationId}.json`), "{", {
    mode: 0o600,
  });
  await expect(applyQualifiedStageRecovery(other, g.api)).rejects.toThrow();
  const h = await fixture(),
    stale = await inspectQualifiedStageRecovery(h.input, h.api);
  await NodeFSP.writeFile(
    NodePath.join(h.base, "runtime", "service-state.json"),
    JSON.stringify({ protocol: 4, activeVersion: h.input.currentVersion }) + "\n",
  );
  await expect(applyQualifiedStageRecovery(stale, h.api)).rejects.toThrow("preimage changed");
});

it("refuses a new operation ID after durable intent and rejects unknown intent bytes", async () => {
  const f = await fixture(),
    plan = await inspectQualifiedStageRecovery(f.input, f.api);
  const directory = NodePath.join(f.base, "runtime", "jones-stage-recoveries");
  await NodeFSP.mkdir(directory, { mode: 0o700 });
  const operation = NodePath.join(directory, `${f.input.operationId}.json`);
  await NodeFSP.writeFile(operation, JSON.stringify(plan), { mode: 0o600 });
  await expect(
    inspectQualifiedStageRecovery({ ...f.input, operationId: NodeCrypto.randomUUID() }, f.api),
  ).rejects.toThrow("Another recovery operation");
  await NodeFSP.writeFile(operation, "{");
  await expect(applyQualifiedStageRecovery(plan, f.api)).rejects.toThrow();
  await expect(
    NodeFSP.lstat(NodePath.join(f.base, "runtime", "staged-updates")),
  ).rejects.toMatchObject({ code: "ENOENT" });
});

it.each(["unenrolled", "stage-evidence", "dmg", "manifest", "ci", "http"])(
  "rejects missing or conflicting %s evidence",
  async (fault) => {
    const f = await fixture();
    if (fault === "unenrolled")
      await NodeFSP.unlink(
        NodePath.join(
          f.base,
          "runtime",
          "versions",
          f.input.targetVersion,
          ".jones-runtime-receipt.json",
        ),
      );
    if (fault === "stage-evidence")
      await NodeFSP.appendFile(NodePath.join(f.stage, "jones-actions-stage.json"), " ");
    if (fault === "dmg")
      await NodeFSP.appendFile(
        NodePath.join(f.stage, `T3-Code-${f.input.targetVersion}-arm64.dmg`),
        "changed",
      );
    if (fault === "manifest") {
      const path = NodePath.join(f.stage, "jones-actions-stage.json");
      const record = JSON.parse(await NodeFSP.readFile(path, "utf8"));
      record.receipt.source = "f".repeat(40);
      await NodeFSP.writeFile(path, JSON.stringify(record));
      f.input = { ...f.input, transportStageReceiptSha256: sha(await NodeFSP.readFile(path)) };
    }
    if (fault === "ci")
      f.responses.set("actions/runs/102", {
        status: 200,
        body: { ...f.run, id: 102, path: ".github/workflows/ci.yml", conclusion: "failure" },
      });
    if (fault === "http") f.responses.set("actions/artifacts/103", { status: 503 });
    await expect(inspectQualifiedStageRecovery(f.input, f.api)).rejects.toThrow();
  },
);
