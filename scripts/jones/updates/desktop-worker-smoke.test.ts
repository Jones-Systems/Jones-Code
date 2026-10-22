// @effect-diagnostics nodeBuiltinImport:off - Portable harness checks; packaged Electron proof runs only in hosted macOS CI.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeEvents from "node:events";
import { afterEach, describe, expect, it } from "vite-plus/test";
import {
  processAbsence,
  qualificationCancellation,
  verifyPackagedIdentity,
  withSmokeScratch,
} from "./desktop-worker-smoke.ts";

const fixtures = new Set<string>();
afterEach(async () => {
  for (const root of fixtures) {
    await NodeFSP.rm(root, { recursive: true, force: true });
    fixtures.delete(root);
  }
});

async function fixture() {
  const parent =
    process.env.JONES_DESKTOP_SMOKE_TEST_ROOT ??
    NodePath.join(NodeOS.homedir(), ".cache", "jones-updater-test-fixtures");
  await NodeFSP.mkdir(parent, { recursive: true, mode: 0o700 });
  const root = await NodeFSP.mkdtemp(NodePath.join(parent, "desktop-smoke-harness-"));
  fixtures.add(root);
  return root;
}

describe("desktop packaged-worker qualification harness", () => {
  const expected = {
    source: "a".repeat(40),
    tree: "b".repeat(40),
    version: "1.2.3-preview.20261022.1.1",
  };
  const identity = {
    metadata: {
      version: expected.version,
      jonesSource: {
        repository: "Jones-Systems/Jones-Code",
        sha: expected.source,
        tree: expected.tree,
      },
    },
    electron: "42.0.0",
    platform: "darwin",
    arch: "arm64",
    workerEntry: "dist-electron/jones/updates/jonesStagingWorker.cjs",
    workerSha256: "c".repeat(64),
  };

  it("requires packaged Electron and the desktop entry bound to the exact source, tree and version", () => {
    expect(() => verifyPackagedIdentity(identity, expected)).not.toThrow();
    for (const invalid of [
      { ...identity, electron: undefined },
      { ...identity, platform: "linux" },
      { ...identity, arch: "x64" },
      { ...identity, workerEntry: "apps/server/dist/bin.mjs" },
      { ...identity, workerSha256: "" },
      { ...identity, metadata: { ...identity.metadata, version: "different" } },
      ...["repository", "sha", "tree"].map((field) => ({
        ...identity,
        metadata: {
          ...identity.metadata,
          jonesSource: { ...identity.metadata.jonesSource, [field]: "different" },
        },
      })),
    ])
      expect(() => verifyPackagedIdentity(invalid, expected)).toThrow("binding failed");
  });

  it("counts only ESRCH as absence for a captured child or process group", () => {
    for (const pid of [42, -42]) {
      expect(processAbsence(pid, () => true)).toBe("fail");
      expect(
        processAbsence(pid, () => {
          throw Object.assign(new Error(), { code: "ESRCH" });
        }),
      ).toBe("pass");
      expect(
        processAbsence(pid, () => {
          throw Object.assign(new Error(), { code: "EPERM" });
        }),
      ).toBe("unknown");
    }
  });

  it("removes exact success and failure roots while preserving the original failure", async () => {
    const parent = await fixture();
    const sentinel = NodePath.join(parent, "sibling-evidence");
    await NodeFSP.writeFile(sentinel, "preserved");
    const failure = new Error("deliberate job failure");
    const roots: string[] = [];
    for (const fail of [false, true]) {
      const operation = withSmokeScratch(parent, async (root) => {
        roots.push(root);
        await NodeFSP.writeFile(NodePath.join(root, "owned"), "fixture");
        if (fail) throw failure;
        return "success";
      });
      if (fail) await expect(operation).rejects.toBe(failure);
      else await expect(operation).resolves.toBe("success");
    }
    for (const root of roots)
      await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
    expect(await NodeFSP.readFile(sentinel, "utf8")).toBe("preserved");
  });

  it("keeps overlapping invocations independent", async () => {
    const parent = await fixture();
    await withSmokeScratch(parent, async (outer) => {
      await NodeFSP.writeFile(NodePath.join(outer, "owned"), "outer");
      await withSmokeScratch(parent, async (inner) => {
        expect(inner).not.toBe(outer);
        await NodeFSP.writeFile(NodePath.join(inner, "owned"), "inner");
      });
      expect(await NodeFSP.readFile(NodePath.join(outer, "owned"), "utf8")).toBe("outer");
    });
    expect(await NodeFSP.readdir(parent)).toEqual([]);
  });

  it("preserves the root and both failures when mount or process absence is unknown", async () => {
    const parent = await fixture();
    const primary = new Error("job failed");
    const cleanup = new Error("captured process-group absence unknown");
    let owned: string | undefined;
    const result = await withSmokeScratch(
      parent,
      async (root) => {
        owned = root;
        await NodeFSP.writeFile(NodePath.join(root, "evidence"), "retained");
        throw primary;
      },
      async () => {
        throw cleanup;
      },
    ).catch((cause: unknown) => cause);
    expect(result).toBeInstanceOf(AggregateError);
    expect((result as AggregateError).errors).toEqual([primary, cleanup]);
    expect(await NodeFSP.readFile(NodePath.join(owned!, "evidence"), "utf8")).toBe("retained");
  });

  it("fails a successful job when cleanup cannot be proved", async () => {
    const parent = await fixture();
    const cleanup = new Error("mount absence unknown");
    await expect(
      withSmokeScratch(
        parent,
        async () => "success",
        async () => {
          throw cleanup;
        },
      ),
    ).rejects.toBe(cleanup);
    expect(await NodeFSP.readdir(parent)).toHaveLength(1);
  });

  it.each(["SIGTERM", "SIGINT"])(
    "routes %s through failure cleanup and removes its handlers",
    async (signal) => {
      const parent = await fixture();
      const events = new NodeEvents.EventEmitter();
      const cancellation = qualificationCancellation(events);
      try {
        await expect(
          withSmokeScratch(parent, async (root) => {
            await NodeFSP.writeFile(NodePath.join(root, "owned"), "fixture");
            events.emit(signal);
            cancellation.signal.throwIfAborted();
          }),
        ).rejects.toThrow("qualification cancelled");
        expect(await NodeFSP.readdir(parent)).toEqual([]);
      } finally {
        cancellation.dispose();
      }
      expect(events.listenerCount("SIGTERM")).toBe(0);
      expect(events.listenerCount("SIGINT")).toBe(0);
    },
  );
});
