// @effect-diagnostics nodeBuiltinImport:off - Synthetic native fixtures use Node filesystem and process APIs.
import { assert, it } from "@effect/vitest";
import { onTestFinished, vi } from "vite-plus/test";
import type { JonesStagedArtifact } from "@t3tools/shared/jones/jonesActions";
import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";
import * as NodeWorkerThreads from "node:worker_threads";
import * as NodeUtil from "node:util";
import * as NodeURL from "node:url";
import {
  bundleFileSystem,
  currentQualifiedRuntimeBinding,
  decodeStagedQualifiedRuntime,
  QualifiedRuntimeBlockedError,
  QUALIFIED_UPDATES_PROTOCOL,
  qualifiedRuntimeArtifactFromJonesStage,
  qualifiedPayloadDigest,
  recentQualifiedDigestTimings,
  retainQualifiedAttemptTimings,
  readQualifiedRuntimeReceipt,
  stageQualifiedRuntime,
  verifyStagedQualifiedRuntime,
  withQualifiedRuntimeLock,
  QUALIFIED_RUNTIME_RECEIPT,
  type QualifiedRuntimeArtifact,
  type QualifiedRuntimeReceipt,
  type QualifiedDigestTiming,
} from "./qualifiedRuntime.ts";

const baseline = "0.0.0-preview.20261002.100";
const fixtureHost = {
  platform: "linux",
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone launcher fixture uses the native architecture with an explicitly injected Linux platform.
  architecture: NodeOS.arch() === "arm64" ? "arm64" : "x64",
};
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
  const base = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-qualified-test-")),
  );
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
    await NodeAssert.rejects(NodeFSP.lstat(base), { code: "ENOENT" });
  }
}

it("stages a fixed source and payload without changing live state or active runtime", async () => {
  await fixture(async (base, _payload, artifact) => {
    const binding = await currentQualifiedRuntimeBinding(base, baseline, fixtureHost);
    const validated: string[] = [];
    const staged = await stageQualifiedRuntime({
      host: fixtureHost,
      artifact,
      binding,
      validate: async (entry) => {
        validated.push(entry);
      },
    });
    assert.equal(staged.protocol, QUALIFIED_UPDATES_PROTOCOL);
    assert.deepEqual(decodeStagedQualifiedRuntime(JSON.parse(JSON.stringify(staged))), staged);
    assert.equal(decodeStagedQualifiedRuntime({ ...staged, protocol: 0 }), undefined);
    assert.equal(
      decodeStagedQualifiedRuntime({
        ...staged,
        receipt: { ...staged.receipt, sourceSha: "not-a-source-sha" },
      }),
      undefined,
    );
    assert.equal(staged.binding.activeSourceSha, "c".repeat(40));
    assert.equal(staged.receipt.sourceSha, "a".repeat(40));
    assert.lengthOf(validated, 1);
    assert.equal(await NodeFSP.readFile(binding.dbPath, "utf8"), "untouched state");
    assert.equal(
      await NodeFSP.readFile(NodePath.join(base, "userdata", "settings.json"), "utf8"),
      "untouched settings",
    );
    assert.equal(
      (await currentQualifiedRuntimeBinding(base, baseline, fixtureHost)).activeVersion,
      baseline,
    );
    assert.deepEqual(
      await verifyStagedQualifiedRuntime(base, baseline, staged.stagedHandle, fixtureHost),
      staged,
    );
    await NodeFSP.writeFile(
      NodePath.join(base, "userdata", "environment-id"),
      "different-environment",
    );
    await NodeAssert.rejects(
      verifyStagedQualifiedRuntime(base, baseline, staged.stagedHandle, fixtureHost),
      (cause: unknown) =>
        cause instanceof QualifiedRuntimeBlockedError &&
        cause.reason === "binding-mismatch" &&
        /different home, database, environment or active source/.test(cause.message),
    );
  });
});

it("preserves occupied runtime versions and rejects a same-version different-source cache", async () => {
  await fixture(async (base, _payload, artifact) => {
    const binding = await currentQualifiedRuntimeBinding(base, baseline, fixtureHost);
    const staged = await stageQualifiedRuntime({
      artifact,
      binding,
      host: fixtureHost,
      validate: async () => {},
    });
    await NodeAssert.rejects(
      stageQualifiedRuntime({
        host: fixtureHost,
        artifact: { ...artifact, sourceSha: "f".repeat(40) },
        binding,
        validate: async () => {},
      }),
      /occupied by a different candidate/,
    );
    assert.equal(
      (await readQualifiedRuntimeReceipt(base, staged.receipt.version, fixtureHost)).sourceSha,
      artifact.sourceSha,
    );
    await NodeFSP.writeFile(
      NodePath.join(base, "runtime", "versions", staged.receipt.version, "client", "index.html"),
      "tampered",
    );
    await NodeAssert.rejects(
      verifyStagedQualifiedRuntime(base, baseline, staged.stagedHandle, fixtureHost),
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
      currentQualifiedRuntimeBinding(base, baseline, fixtureHost),
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
        host: fixtureHost,
        artifact,
        binding: await currentQualifiedRuntimeBinding(base, baseline, fixtureHost),
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
      host: fixtureHost,
      artifact: normalized,
      binding: await currentQualifiedRuntimeBinding(base, baseline, fixtureHost),
      validate: async () => {},
    });
    assert.equal(staged.receipt.artifactDigest, normalized.artifactDigest);
    assert.deepEqual(
      await verifyStagedQualifiedRuntime(base, baseline, staged.stagedHandle, fixtureHost),
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

it("qualifies a manual Mac CLI layout only with its CLI workflow receipt", async () => {
  await fixture(async (base, payload, artifact) => {
    const cliWorkflow = ".github/workflows/artifact-cli-mac.yml" as const;
    const host = { platform: "darwin", architecture: "arm64" };
    const active = NodePath.join(base, "runtime", "versions", baseline);
    await NodeFSP.writeFile(
      NodePath.join(active, QUALIFIED_RUNTIME_RECEIPT),
      JSON.stringify({
        ...receipt(baseline, "c".repeat(40)),
        platform: "darwin",
        architecture: "arm64",
        workflow: cliWorkflow,
        payloadSha256: await qualifiedPayloadDigest(active, "darwin", cliWorkflow),
      }),
      { mode: 0o600 },
    );
    const binding = await currentQualifiedRuntimeBinding(base, baseline, host);
    const staged = await stageQualifiedRuntime({
      artifact: { ...artifact, platform: "darwin", architecture: "arm64", workflow: cliWorkflow },
      binding,
      host,
      validate: async () => {},
    });
    assert.equal(staged.receipt.workflow, cliWorkflow);
    await NodeAssert.rejects(
      qualifiedPayloadDigest(payload, "darwin", ".github/workflows/artifact-desktop-mac.yml"),
      /unexpected root/,
    );
  });
});

it("aborts digest work without changing the default digest format", async () => {
  await fixture(async (_base, payload) => {
    const expected = "4e64a35abb1beb78d6a649a9fc06f586e186486c28bac6079d6756075811916b";
    assert.equal(await qualifiedPayloadDigest(payload), expected);
    assert.equal(await qualifiedPayloadDigest(payload, "linux", undefined, {}), expected);
    const controller = new AbortController();
    const reason = new Error("synthetic digest cancellation");
    controller.abort(reason);
    await NodeAssert.rejects(
      qualifiedPayloadDigest(payload, "linux", undefined, { signal: controller.signal }),
      (cause) => cause === reason,
    );
  });
});

it("closes the digest file after cancellation between chunks", async () => {
  await fixture(async (_base, payload) => {
    const controller = new AbortController();
    const reason = new Error("synthetic chunk cancellation");
    const fs = bundleFileSystem().promises;
    const open = fs.open.bind(fs);
    let closed = false;
    let chunks = 0;
    const spy = vi.spyOn(fs, "open").mockImplementation(async (...args) => {
      const handle = await open(...args);
      const close = handle.close.bind(handle);
      vi.spyOn(handle, "close").mockImplementation(async () => {
        closed = true;
        await close();
      });
      vi.spyOn(handle, "createReadStream").mockImplementation(
        () =>
          ({
            async *[Symbol.asyncIterator]() {
              chunks += 1;
              yield Buffer.from("first");
              controller.abort(reason);
              chunks += 1;
              yield Buffer.from("second");
              chunks += 1;
              yield Buffer.from("unreachable");
            },
          }) as ReturnType<typeof handle.createReadStream>,
      );
      return handle;
    });
    try {
      await NodeAssert.rejects(
        qualifiedPayloadDigest(payload, "linux", undefined, { signal: controller.signal }),
        (cause) => cause === reason,
      );
      assert.equal(chunks, 2);
      assert.equal(closed, true);
    } finally {
      spy.mockRestore();
    }
  });
});

it("propagates install cancellation through receipt, current binding and staged verification", async () => {
  await fixture(async (base, _payload, artifact) => {
    const binding = await currentQualifiedRuntimeBinding(base, baseline, fixtureHost);
    const staged = await stageQualifiedRuntime({
      artifact,
      binding,
      host: fixtureHost,
      validate: async () => {},
    });
    const controller = new AbortController();
    const reason = new Error("synthetic install cancellation");
    controller.abort(reason);
    const options = { signal: controller.signal };
    for (const operation of [
      () => readQualifiedRuntimeReceipt(base, baseline, fixtureHost, options),
      () => currentQualifiedRuntimeBinding(base, baseline, fixtureHost, options),
      () => verifyStagedQualifiedRuntime(base, baseline, staged.stagedHandle, fixtureHost, options),
    ])
      await NodeAssert.rejects(operation(), (cause) => cause === reason);
    assert.deepEqual(
      await verifyStagedQualifiedRuntime(base, baseline, staged.stagedHandle, fixtureHost),
      staged,
    );
  });
});

it("measures each digest without changing its bytes and isolates a failing timing sink", async () => {
  await fixture(async (_base, payload) => {
    const timings: QualifiedDigestTiming[] = [];
    const expected = "4e64a35abb1beb78d6a649a9fc06f586e186486c28bac6079d6756075811916b";
    assert.equal(
      await qualifiedPayloadDigest(payload, "linux", undefined, {
        role: "launcher-activation",
        runtimeVersion: candidateVersion,
        expectedSha256: expected,
        onDigestTiming: (timing) => {
          timings.push(timing);
          throw new Error("synthetic observation failure");
        },
      }),
      expected,
    );
    assert.lengthOf(timings, 1);
    const timing = timings[0]!;
    assert.equal(timing.schema, 1);
    assert.equal(timing.role, "launcher-activation");
    assert.equal(timing.runtime, candidateVersion);
    assert.equal(timing.result, "match");
    assert.equal(timing.files, 2);
    assert.equal(timing.bytes, Buffer.byteLength("#!/bin/sh\nexit 0\nfixture"));
    assert.isAtLeast(timing.wallMs, 0);
    assert.isAtLeast(timing.cpuMs, 0);
    assert.isTrue(Number.isFinite(timing.wallMs));
    assert.isTrue(Number.isFinite(timing.cpuMs));
    if (timing.eventLoopDelayP99Ms !== null) {
      assert.isAtLeast(timing.eventLoopDelayP99Ms, 0);
      assert.isTrue(Number.isFinite(timing.eventLoopDelayP99Ms));
    }
    assert.isTrue(Number.isFinite(Date.parse(timing.at)));
    assert.equal(recentQualifiedDigestTimings().at(-1), timing);
    assert.isTrue(Object.isFrozen(timing));
  });
});

it("reports a receipt mismatch while preserving the integrity refusal", async () => {
  await fixture(async (base) => {
    await NodeFSP.writeFile(
      NodePath.join(base, "runtime", "versions", baseline, "client", "index.html"),
      "tampered fixture",
    );
    const timings: QualifiedDigestTiming[] = [];
    await NodeAssert.rejects(
      readQualifiedRuntimeReceipt(base, baseline, fixtureHost, {
        role: "recovery",
        onDigestTiming: (value) => {
          timings.push(value);
        },
      }),
      (cause: unknown) =>
        cause instanceof QualifiedRuntimeBlockedError && cause.reason === "integrity-mismatch",
    );
    assert.lengthOf(timings, 1);
    assert.equal(timings[0]?.result, "mismatch");
    assert.equal(timings[0]?.runtime, baseline);
    assert.equal(timings[0]?.role, "recovery");
  });
});

it("records cancellation and file errors without replacing the original failure", async () => {
  await fixture(async (_base, payload) => {
    const controller = new AbortController();
    const reason = new Error("synthetic cancellation before digest");
    controller.abort(reason);
    const timings: QualifiedDigestTiming[] = [];
    await NodeAssert.rejects(
      qualifiedPayloadDigest(payload, "linux", undefined, {
        signal: controller.signal,
        onDigestTiming: (timing) => {
          timings.push(timing);
          throw new Error("secondary observation failure");
        },
      }),
      (cause) => cause === reason,
    );
    await NodeAssert.rejects(
      qualifiedPayloadDigest(NodePath.join(payload, "missing"), "linux", undefined, {
        onDigestTiming: (timing) => {
          timings.push(timing);
        },
      }),
      { code: "ENOENT" },
    );
    assert.deepEqual(
      timings.map((timing) => timing.result),
      ["error", "error"],
    );
    assert.deepEqual(
      timings.map((timing) => timing.files),
      [0, 0],
    );
    assert.deepEqual(
      timings.map((timing) => timing.bytes),
      [0, 0],
    );
    assert.deepEqual(
      timings.map((timing) => timing.runtime),
      ["unspecified", "unspecified"],
    );
  });
});

it("bounds recent timing history and returns an independent snapshot", async () => {
  await fixture(async (_base, payload) => {
    const observed: QualifiedDigestTiming[] = [];
    for (let index = 0; index < 35; index++)
      await qualifiedPayloadDigest(payload, "linux", undefined, {
        onDigestTiming: (timing) => {
          observed.push(timing);
        },
      });
    const snapshot = recentQualifiedDigestTimings();
    assert.deepEqual(snapshot, observed.slice(-32));
    await qualifiedPayloadDigest(payload);
    assert.deepEqual(snapshot, observed.slice(-32));
    assert.lengthOf(recentQualifiedDigestTimings(), 32);
  });
});

it("reports null loop delay in a real worker and closes the worker after its digest", async () => {
  await fixture(async (_base, payload) => {
    const worker = new NodeWorkerThreads.Worker(
      `const { parentPort, workerData } = require("node:worker_threads");
       import(workerData.moduleUrl).then(async ({ qualifiedPayloadDigest }) => {
         const digest = await qualifiedPayloadDigest(workerData.payload, "linux", undefined, {
           onDigestTiming: (timing) => parentPort.postMessage(timing)
         });
         if (digest !== workerData.expected) throw new Error("Worker digest changed");
       }).catch((cause) => { throw cause; });`,
      {
        eval: true,
        workerData: {
          moduleUrl: new URL("./qualifiedRuntime.ts", import.meta.url).href,
          payload,
          expected: "4e64a35abb1beb78d6a649a9fc06f586e186486c28bac6079d6756075811916b",
        },
      },
    );
    let completed = false;
    onTestFinished(async () => {
      if (!completed) await worker.terminate();
    });
    try {
      const result = await new Promise<QualifiedDigestTiming>((resolve, reject) => {
        let observed: QualifiedDigestTiming | undefined;
        worker.on("message", (value: QualifiedDigestTiming) => {
          observed = value;
        });
        worker.once("error", reject);
        worker.once("exit", (code) => {
          completed = true;
          if (code === 0 && observed !== undefined) resolve(observed);
          else reject(new Error("Owned digest worker failed without a complete measurement"));
        });
      });
      assert.equal(result.eventLoopDelayP99Ms, null);
      assert.equal(result.files, 2);
      assert.equal(result.result, "match");
    } finally {
      if (!completed) await worker.terminate();
    }
  });
});

it("benchmarks an isolated payload read-only and exits after bounded synthetic load", async () => {
  await fixture(async (_base, payload) => {
    const before = await qualifiedPayloadDigest(payload);
    const entries = await NodeFSP.readdir(payload);
    const { stdout } = await NodeUtil.promisify(NodeChildProcess.execFile)(
      process.execPath,
      [
        NodeURL.fileURLToPath(
          new URL("../../../../../scripts/jones/updates/digest-benchmark.ts", import.meta.url),
        ),
        payload,
        "--iterations",
        "1",
        "--version",
        candidateVersion,
      ],
      { timeout: 10_000, maxBuffer: 64 * 1024 },
    );
    const report = JSON.parse(stdout) as {
      schema: number;
      samples: Array<{
        mode: string;
        digest: string;
        bytesPerSecond: number;
        timing: QualifiedDigestTiming;
      }>;
    };
    assert.equal(report.schema, 1);
    assert.deepEqual(
      report.samples.map((sample) => sample.mode),
      ["idle", "loaded"],
    );
    for (const sample of report.samples) {
      assert.equal(sample.digest, before);
      assert.equal(sample.timing.runtime, candidateVersion);
      assert.equal(sample.timing.result, "match");
      assert.isTrue(Number.isFinite(sample.bytesPerSecond));
      assert.isAbove(sample.bytesPerSecond, 0);
    }
    assert.deepEqual(await NodeFSP.readdir(payload), entries);
    assert.equal(await qualifiedPayloadDigest(payload), before);
  });
});

it("omits shared-loop delay for a declared dedicated process", async () => {
  await fixture(async (_base, payload) => {
    const timings: QualifiedDigestTiming[] = [];
    await qualifiedPayloadDigest(payload, "linux", undefined, {
      eventLoop: "worker",
      onDigestTiming: (timing) => {
        timings.push(timing);
      },
    });
    assert.lengthOf(timings, 1);
    assert.equal(timings[0]!.eventLoopDelayP99Ms, null);
  });
});

it("isolates a rejected asynchronous timing sink", async () => {
  await fixture(async (_base, payload) => {
    const digest = await qualifiedPayloadDigest(payload, "linux", undefined, {
      onDigestTiming: async () => {
        throw new Error("synthetic asynchronous observation failure");
      },
    });
    assert.equal(digest, "4e64a35abb1beb78d6a649a9fc06f586e186486c28bac6079d6756075811916b");
  });
});

it("transfers an owned stage with one content digest and reuses its identity-bound receipt", async () => {
  await fixture(async (base, payload, artifact) => {
    const binding = await currentQualifiedRuntimeBinding(base, baseline, fixtureHost);
    const measured: QualifiedDigestTiming[] = [];
    const staged = await stageQualifiedRuntime({
      artifact,
      binding,
      host: fixtureHost,
      consumePayload: true,
      validate: async () => {},
      options: {
        onDigestTiming: (timing) => {
          measured.push(timing);
        },
      },
    });
    assert.equal(measured.filter((row) => row.runtime === candidateVersion).length, 1);
    await NodeAssert.rejects(NodeFSP.lstat(payload), { code: "ENOENT" });
    const reads: QualifiedDigestTiming[] = [];
    const options = {
      mode: "receipt" as const,
      onDigestTiming: (row: QualifiedDigestTiming) => {
        reads.push(row);
      },
    };
    assert.deepEqual(
      await verifyStagedQualifiedRuntime(base, baseline, staged.stagedHandle, fixtureHost, options),
      staged,
    );
    assert.equal(reads.length, 0);
    // Activation always performs a fresh full content verification, even with an exact cache hit.
    assert.deepEqual(
      await readQualifiedRuntimeReceipt(base, candidateVersion, fixtureHost, {
        ...options,
        mode: "full",
        role: "launcher-activation",
      }),
      staged.receipt,
    );
    assert.equal(reads.length, 1);
  });
});

it("falls back on changed metadata and refuses same-size tampering with restored mtime", async () => {
  await fixture(async (base, _payload, artifact) => {
    const binding = await currentQualifiedRuntimeBinding(base, baseline, fixtureHost);
    await stageQualifiedRuntime({
      artifact,
      binding,
      host: fixtureHost,
      consumePayload: true,
      validate: async () => {},
    });
    const file = NodePath.join(
      base,
      "runtime",
      "versions",
      candidateVersion,
      "client",
      "index.html",
    );
    const prior = await NodeFSP.stat(file);
    await NodeFSP.utimes(file, prior.atimeMs / 1000, prior.mtimeMs / 1000 - 1);
    const readings: QualifiedDigestTiming[] = [];
    const options = {
      mode: "receipt" as const,
      onDigestTiming: (row: QualifiedDigestTiming) => {
        readings.push(row);
      },
    };
    await readQualifiedRuntimeReceipt(base, candidateVersion, fixtureHost, options);
    assert.equal(readings.length, 1);
    await readQualifiedRuntimeReceipt(base, candidateVersion, fixtureHost, options);
    assert.equal(readings.length, 1);
    const unchangedTimes = await NodeFSP.stat(file);
    await NodeFSP.writeFile(file, "changed");
    await NodeFSP.utimes(file, unchangedTimes.atime, unchangedTimes.mtime);
    await NodeAssert.rejects(
      readQualifiedRuntimeReceipt(base, candidateVersion, fixtureHost, options),
      /payload changed/,
    );
    await NodeAssert.rejects(
      readQualifiedRuntimeReceipt(base, candidateVersion, fixtureHost, { mode: "full" }),
      /payload changed/,
    );
  });
});

it("refuses a serving-loop digest and delegates a missing receipt to the bounded worker seam", async () => {
  await fixture(async (base) => {
    await NodeAssert.rejects(
      readQualifiedRuntimeReceipt(base, baseline, fixtureHost, {
        mode: "full",
        eventLoop: "shared",
      }),
      /preparation worker or launcher/,
    );
    await NodeAssert.rejects(
      readQualifiedRuntimeReceipt(base, baseline, fixtureHost, {
        mode: "receipt",
        eventLoop: "shared",
      }),
      /preparation worker or launcher/,
    );
    let delegated = 0;
    const observed = await readQualifiedRuntimeReceipt(base, baseline, fixtureHost, {
      mode: "receipt",
      eventLoop: "shared",
      fullVerification: async (input) => {
        delegated += 1;
        assert.deepEqual(input, { baseDir: base, version: baseline, host: fixtureHost });
        return readQualifiedRuntimeReceipt(base, baseline, fixtureHost, {
          mode: "full",
          retainVerification: true,
          eventLoop: "worker",
        });
      },
    });
    assert.equal(observed.version, baseline);
    assert.equal(delegated, 1);
    await readQualifiedRuntimeReceipt(base, baseline, fixtureHost, {
      mode: "receipt",
      eventLoop: "shared",
    });
    assert.equal(delegated, 1);
  });
});

it("persists attempt timings exclusively and retains only the newest 64 owned records", async () => {
  await fixture(async (base) => {
    const id = NodeCrypto.randomUUID();
    const timing: QualifiedDigestTiming = {
      schema: 1,
      role: "launcher-activation",
      runtime: candidateVersion,
      files: 2,
      bytes: 14,
      wallMs: 1,
      cpuMs: 1,
      eventLoopDelayP99Ms: null,
      result: "match",
      at: "2026-10-25T00:00:00Z",
    };
    await retainQualifiedAttemptTimings(base, id, [timing]);
    const directory = NodePath.join(base, "runtime", "jones-update-timing");
    const first = await NodeFSP.readFile(NodePath.join(directory, `${id}.json`), "utf8");
    await retainQualifiedAttemptTimings(base, id, []);
    assert.equal(await NodeFSP.readFile(NodePath.join(directory, `${id}.json`), "utf8"), first);
    await NodeFSP.writeFile(NodePath.join(directory, "unowned.txt"), "preserve");
    for (let index = 0; index < 65; index++)
      await retainQualifiedAttemptTimings(base, NodeCrypto.randomUUID(), [timing]);
    assert.equal(
      (await NodeFSP.readdir(directory)).filter((name) => name.endsWith(".json")).length,
      64,
    );
    assert.equal(
      await NodeFSP.readFile(NodePath.join(directory, "unowned.txt"), "utf8"),
      "preserve",
    );
    await NodeAssert.rejects(
      retainQualifiedAttemptTimings(base, "../escape", []),
      /Invalid bounded/,
    );
  });
});
