// @effect-diagnostics nodeBuiltinImport:off
import { assert, it } from "@effect/vitest";
import type { JonesStagedArtifact } from "@t3tools/shared/jonesActions";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  bundleFileSystem,
  currentQualifiedRuntimeBinding,
  qualifiedRuntimeArtifactFromJonesStage,
  qualifiedPayloadDigest,
  readQualifiedRuntimeReceipt,
  stageQualifiedRuntime,
  verifyStagedQualifiedRuntime,
  withQualifiedRuntimeLock,
  QUALIFIED_RUNTIME_RECEIPT,
  type QualifiedRuntimeArtifact,
  type QualifiedRuntimeReceipt,
} from "./qualifiedRuntime.ts";

const baseline = "0.0.0-preview.20261002.100";
const candidateVersion = "0.0.0-preview.20261002.101.1";
const receipt = (
  version: string,
  sourceSha: string,
): Omit<QualifiedRuntimeReceipt, "payloadSha256"> => ({
  protocol: 1,
  repository: "Jones-Systems/Jones-Code",
  channel: "jones-main",
  version,
  sourceSha,
  sourceTree: "b".repeat(40),
  installedSourceSha: "c".repeat(40),
  runId: 101,
  runAttempt: 1,
  artifactId: 102,
  workflow: ".github/workflows/artifact-cli-linux.yml",
  artifactDigest: `sha256:${"d".repeat(64)}`,
  archiveSha256: "e".repeat(64),
  platform: "linux",
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone launcher fixture must match the native host running its controlled executable.
  architecture: NodeOS.arch() === "arm64" ? "arm64" : "x64",
});

async function fixture<A>(
  body: (base: string, payload: string, artifact: QualifiedRuntimeArtifact) => Promise<A>,
): Promise<A> {
  const base = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-qualified-test-"));
  try {
    const active = NodePath.join(base, "runtime", "versions", baseline);
    const payload = NodePath.join(base, "candidate");
    for (const directory of [active, payload]) {
      await NodeFSP.mkdir(NodePath.join(directory, "client"), { recursive: true });
      await NodeFSP.writeFile(NodePath.join(directory, "t3"), "#!/bin/sh\nexit 0\n", {
        mode: 0o700,
      });
      await NodeFSP.writeFile(NodePath.join(directory, "client", "index.html"), "fixture");
    }
    await NodeFSP.writeFile(
      NodePath.join(active, QUALIFIED_RUNTIME_RECEIPT),
      JSON.stringify({
        ...receipt(baseline, "c".repeat(40)),
        payloadSha256: await qualifiedPayloadDigest(active),
      }),
      { mode: 0o600 },
    );
    await NodeFSP.mkdir(NodePath.join(base, "userdata"));
    await NodeFSP.writeFile(NodePath.join(base, "userdata", "statev2.sqlite"), "untouched state");
    await NodeFSP.writeFile(NodePath.join(base, "userdata", "settings.json"), "untouched settings");
    await NodeFSP.writeFile(
      NodePath.join(base, "userdata", "environment-id"),
      "fixture-environment",
    );
    const { protocol: _protocol, ...metadata } = receipt(candidateVersion, "a".repeat(40));
    return await body(base, payload, { ...metadata, payloadDirectory: payload });
  } finally {
    await NodeFSP.rm(base, { recursive: true, force: true });
  }
}

it("stages a fixed source and payload without changing live state or active runtime", async () => {
  await fixture(async (base, _payload, artifact) => {
    const binding = await currentQualifiedRuntimeBinding(base, baseline);
    const validated: string[] = [];
    const staged = await stageQualifiedRuntime({
      artifact,
      binding,
      validate: async (entry) => {
        validated.push(entry);
      },
    });
    assert.equal(staged.binding.activeSourceSha, "c".repeat(40));
    assert.equal(staged.receipt.sourceSha, "a".repeat(40));
    assert.lengthOf(validated, 1);
    assert.equal(await NodeFSP.readFile(binding.dbPath, "utf8"), "untouched state");
    assert.equal(
      await NodeFSP.readFile(NodePath.join(base, "userdata", "settings.json"), "utf8"),
      "untouched settings",
    );
    assert.equal((await currentQualifiedRuntimeBinding(base, baseline)).activeVersion, baseline);
    assert.deepEqual(
      await verifyStagedQualifiedRuntime(base, baseline, staged.stagedHandle),
      staged,
    );
    await NodeFSP.writeFile(
      NodePath.join(base, "userdata", "environment-id"),
      "different-environment",
    );
    await NodeAssert.rejects(
      verifyStagedQualifiedRuntime(base, baseline, staged.stagedHandle),
      /different home, database, environment or active source/,
    );
  });
});

it("preserves occupied runtime versions and rejects a same-version different-source cache", async () => {
  await fixture(async (base, _payload, artifact) => {
    const binding = await currentQualifiedRuntimeBinding(base, baseline);
    const staged = await stageQualifiedRuntime({ artifact, binding, validate: async () => {} });
    await NodeAssert.rejects(
      stageQualifiedRuntime({
        artifact: { ...artifact, sourceSha: "f".repeat(40) },
        binding,
        validate: async () => {},
      }),
      /occupied by a different candidate/,
    );
    assert.equal(
      (await readQualifiedRuntimeReceipt(base, staged.receipt.version)).sourceSha,
      artifact.sourceSha,
    );
    await NodeFSP.writeFile(
      NodePath.join(base, "runtime", "versions", staged.receipt.version, "client", "index.html"),
      "tampered",
    );
    await NodeAssert.rejects(
      verifyStagedQualifiedRuntime(base, baseline, staged.stagedHandle),
      /payload changed/,
    );
  });
});

it("requires source-qualified bootstrap even when a version-only sentinel exists", async () => {
  await fixture(async (base) => {
    const active = NodePath.join(base, "runtime", "versions", baseline);
    await NodeFSP.rm(NodePath.join(active, QUALIFIED_RUNTIME_RECEIPT));
    await NodeFSP.writeFile(NodePath.join(active, ".install-complete"), baseline);
    await NodeAssert.rejects(
      currentQualifiedRuntimeBinding(base, baseline),
      /enrolled with its qualified source/,
    );
  });
});

it("serializes ownership and releases the stable SQLite lock after failure", async () => {
  await fixture(async (base) => {
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const owner = withQualifiedRuntimeLock(base, "test-lock", async () => {
      entered.resolve();
      await release.promise;
    });
    await entered.promise;
    await NodeAssert.rejects(
      withQualifiedRuntimeLock(base, "test-lock", async () => {}),
      /already owns this home/,
    );
    release.resolve();
    await owner;
    await NodeAssert.rejects(
      withQualifiedRuntimeLock(base, "test-lock", async () => {
        throw new Error("fault");
      }),
      /fault/,
    );
    await withQualifiedRuntimeLock(base, "test-lock", async () => {});
  });
});

it("rejects escaping symlinks in a verified payload before copying it", async () => {
  await fixture(async (base, payload, artifact) => {
    await NodeFSP.symlink(
      NodePath.join(base, "userdata", "statev2.sqlite"),
      NodePath.join(payload, "client", "escape"),
    );
    await NodeAssert.rejects(
      stageQualifiedRuntime({
        artifact,
        binding: await currentQualifiedRuntimeBinding(base, baseline),
        validate: async () => {},
      }),
      /symlink escapes/,
    );
    assert.equal(
      await NodeFSP.readFile(NodePath.join(base, "userdata", "statev2.sqlite"), "utf8"),
      "untouched state",
    );
  });
});

it("binds a headless Darwin runtime to its Mac workflow and injected native platform", async () => {
  await fixture(async (base, payload, artifact) => {
    const host = { platform: "darwin", architecture: "arm64" };
    const active = NodePath.join(base, "runtime", "versions", baseline);
    for (const directory of [active, payload]) {
      await NodeFSP.rm(NodePath.join(directory, "client"), { recursive: true });
      await NodeFSP.mkdir(NodePath.join(directory, "Jones Code.app", "Contents"), {
        recursive: true,
      });
      await NodeFSP.writeFile(
        NodePath.join(directory, "Jones Code.app", "Contents", "metadata"),
        "immutable native fixture",
      );
    }
    const nativeMetadata = {
      platform: "darwin" as const,
      architecture: "arm64" as const,
      workflow: ".github/workflows/artifact-desktop-mac.yml" as const,
    };
    await NodeFSP.writeFile(
      NodePath.join(active, QUALIFIED_RUNTIME_RECEIPT),
      JSON.stringify({
        ...receipt(baseline, "c".repeat(40)),
        ...nativeMetadata,
        payloadSha256: await qualifiedPayloadDigest(active, "darwin"),
      }),
      { mode: 0o600 },
    );
    const binding = await currentQualifiedRuntimeBinding(base, baseline, host);
    const staged = await stageQualifiedRuntime({
      artifact: { ...artifact, ...nativeMetadata },
      binding,
      host,
      validate: async () => {},
    });
    assert.equal(staged.receipt.platform, "darwin");
    assert.equal(staged.receipt.workflow, nativeMetadata.workflow);
    assert.deepEqual(
      await verifyStagedQualifiedRuntime(base, baseline, staged.stagedHandle, host),
      staged,
    );
    await NodeAssert.rejects(
      verifyStagedQualifiedRuntime(base, baseline, staged.stagedHandle, {
        platform: "linux",
        architecture: "x64",
      }),
      /host platform/,
    );
  });
});

it("stages the shared transport's real raw artifact digest representation", async () => {
  await fixture(async (base, payload, artifact) => {
    const shared: JonesStagedArtifact = {
      schema: 1,
      source: "jones-actions",
      channel: "jones-main",
      stagedHandle: "transport-cache-handle",
      payloadPath: "/transport/runtime.tar.gz",
      candidate: {
        schema: 1,
        repository: "Jones-Systems/Jones-Code",
        source: artifact.sourceSha,
        tree: artifact.sourceTree,
        installedSource: artifact.installedSourceSha,
        workflow: artifact.workflow,
        workflowId: 1,
        runId: artifact.runId,
        runAttempt: artifact.runAttempt,
        ciRunId: 2,
        artifactId: artifact.artifactId,
        artifactName: "jones-code-cli-linux-x64-101-1",
        artifactDigest: "d".repeat(64),
        artifactBytes: 100,
        expiresAt: "2026-10-10T00:00:00Z",
        version: artifact.version,
        platform: artifact.platform,
        architecture: artifact.architecture,
      },
      receipt: {
        schema: 1,
        repository: "Jones-Systems/Jones-Code",
        source: artifact.sourceSha,
        tree: artifact.sourceTree,
        workflow: artifact.workflow,
        event: "push",
        ref: "refs/heads/main",
        runId: String(artifact.runId),
        runAttempt: String(artifact.runAttempt),
        version: artifact.version,
        platform: artifact.platform,
        architecture: artifact.architecture,
        artifact: "runtime.tar.gz",
        sha256: artifact.archiveSha256,
      },
    };
    const normalized = qualifiedRuntimeArtifactFromJonesStage(shared, payload);
    assert.equal(normalized.artifactDigest, `sha256:${shared.candidate.artifactDigest}`);
    const staged = await stageQualifiedRuntime({
      artifact: normalized,
      binding: await currentQualifiedRuntimeBinding(base, baseline),
      validate: async () => {},
    });
    assert.equal(staged.receipt.artifactDigest, normalized.artifactDigest);
    assert.deepEqual(
      await verifyStagedQualifiedRuntime(base, baseline, staged.stagedHandle),
      staged,
    );
  });
});

it("keeps ordinary Node filesystem selection without loading Electron modules", () => {
  assert.strictEqual(
    bundleFileSystem({}, () => {
      throw new Error("must not load");
    }),
    NodeFS,
  );
});

it("selects Electron raw filesystem and fails closed when it is unavailable", () => {
  const calls: string[] = [];
  assert.strictEqual(
    bundleFileSystem({ electron: "44.4.2" }, (id) => {
      calls.push(id);
      return NodeFS;
    }),
    NodeFS,
  );
  NodeAssert.deepStrictEqual(calls, ["original-fs"]);
  NodeAssert.throws(
    () =>
      bundleFileSystem({ electron: "44.4.2" }, () => {
        throw new Error("raw filesystem unavailable");
      }),
    /raw filesystem unavailable/,
  );
});
