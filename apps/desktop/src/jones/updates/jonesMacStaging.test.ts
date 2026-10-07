// @effect-diagnostics nodeBuiltinImport:off - Synthetic bundle integrity fixtures, never launches an app.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { describe, expect, it } from "vite-plus/test";
import {
  bundleFileSystem,
  hashMacApp,
  hashMacFile,
  JonesCandidateStartupGateUnavailableError,
  preflightJonesCandidateStartupGate,
  requireJonesCandidateStartupGate,
} from "./jonesMacStaging.ts";

async function withBundle(run: (root: string) => Promise<void>): Promise<void> {
  const parent = NodePath.join(NodeOS.homedir(), ".cache", "jones-updater-test-fixtures");
  await NodeFSP.mkdir(parent, { recursive: true, mode: 0o700 });
  const root = await NodeFSP.mkdtemp(NodePath.join(parent, "integrity-"));
  try {
    await run(root);
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
}

const electronVersions = { ...process.versions, electron: "44.4.2" };

describe("candidate startup gate qualification", () => {
  const binding = {
    version: "1.2.4-preview.20261007.1.2",
    sourceSha: "b".repeat(40),
    sourceTree: "c".repeat(40),
  };
  const metadata = {
    version: binding.version,
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: binding.sourceSha,
      tree: binding.sourceTree,
    },
  };

  it.each([undefined, null, false, true, "1", 0, 2])(
    "refuses the missing or wrong candidate marker %s",
    (startupGateProtocol) => {
      const candidate = {
        ...metadata,
        ...(startupGateProtocol === undefined ? {} : { startupGateProtocol }),
      };
      expect(() => requireJonesCandidateStartupGate(candidate, binding)).toThrow(
        JonesCandidateStartupGateUnavailableError,
      );
    },
  );

  it("requires the marker to bind the candidate version, source SHA, and source tree", () => {
    const supported = { ...metadata, startupGateProtocol: 1 };
    expect(requireJonesCandidateStartupGate(supported, binding)).toBe(1);
    for (const stale of [
      { ...binding, version: "1.2.3" },
      { ...binding, sourceSha: "f".repeat(40) },
      { ...binding, sourceTree: "f".repeat(40) },
    ]) {
      expect(() => requireJonesCandidateStartupGate(supported, stale)).toThrow(
        JonesCandidateStartupGateUnavailableError,
      );
    }
  });

  it("rechecks binary-bound metadata instead of trusting a staged receipt's marker", () =>
    withBundle(async (root) => {
      const asar = NodePath.join(root, "Contents", "Resources", "app.asar");
      await NodeFSP.mkdir(asar, { recursive: true });
      const file = NodePath.join(asar, "package.json");
      const staged = {
        ...binding,
        handle: "e".repeat(64),
        receiptPath: NodePath.join(root, "receipt.json"),
        appPath: root,
        executablePath: NodePath.join(root, "Jones"),
        appDigest: "a".repeat(64),
        asarDigest: "d".repeat(64),
        executableDigest: "f".repeat(64),
        startupGateProtocol: 1 as const,
      };
      await NodeFSP.writeFile(file, JSON.stringify({ ...metadata, startupGateProtocol: 1 }));
      await expect(preflightJonesCandidateStartupGate(staged)).resolves.toBeUndefined();
      for (const candidate of [metadata, { ...metadata, startupGateProtocol: 2 }]) {
        await NodeFSP.writeFile(file, JSON.stringify(candidate));
        await expect(preflightJonesCandidateStartupGate(staged)).rejects.toBeInstanceOf(
          JonesCandidateStartupGateUnavailableError,
        );
      }
    }));
});

describe("native bundle integrity filesystem", () => {
  it("uses Node's filesystem without loading an Electron-only module outside Electron", () => {
    const versions = { ...process.versions, electron: undefined };
    expect(
      bundleFileSystem(versions, () => {
        throw new Error("Electron-only module must not load under Node.");
      }),
    ).toBe(NodeFS);
  });

  it("selects the raw Electron filesystem and fails closed when it is unavailable", () => {
    const loads: string[] = [];
    expect(
      bundleFileSystem(electronVersions, (id) => {
        loads.push(id);
        return NodeFS;
      }),
    ).toBe(NodeFS);
    expect(loads).toEqual(["original-fs"]);
    expect(() =>
      bundleFileSystem(electronVersions, () => {
        throw new Error("Raw filesystem unavailable.");
      }),
    ).toThrow("Raw filesystem unavailable.");
  });

  it("hashes archive bytes and detects a same-size content change", () =>
    withBundle(async (root) => {
      const archive = NodePath.join(root, "app.asar");
      await NodeFSP.writeFile(archive, "archive-a");
      expect(await hashMacFile(archive)).toBe(
        NodeCrypto.createHash("sha256").update("archive-a").digest("hex"),
      );
      const before = await hashMacApp(root);
      await NodeFSP.writeFile(archive, "archive-b");
      expect(await hashMacApp(root)).not.toBe(before);
    }));

  it.skipIf(HostProcessPlatform.defaultValue() === "win32")(
    "keeps the canonical native layout digest, modes and internal links",
    () =>
      withBundle(async (root) => {
        const contents = NodePath.join(root, "Contents");
        const resources = NodePath.join(contents, "Resources");
        await NodeFSP.mkdir(resources, { recursive: true });
        await NodeFSP.chmod(contents, 0o755);
        await NodeFSP.chmod(resources, 0o755);
        const archive = NodePath.join(resources, "app.asar");
        await NodeFSP.writeFile(archive, "synthetic-archive");
        await NodeFSP.chmod(archive, 0o644);
        const executable = NodePath.join(root, "run");
        await NodeFSP.writeFile(executable, "synthetic-executable");
        await NodeFSP.chmod(executable, 0o755);
        await NodeFSP.symlink("Contents/Resources/app.asar", NodePath.join(root, "current"));
        expect(await hashMacApp(root)).toBe(
          "5d019ec44dbd8bfab5121552a5ce4b9ab3498e80c7d060bccf57b48802791b61",
        );
      }),
  );

  it.skipIf(HostProcessPlatform.defaultValue() === "win32")(
    "rejects a link escaping the bundle",
    () =>
      withBundle(async (root) => {
        const app = NodePath.join(root, "app");
        await NodeFSP.mkdir(app);
        await NodeFSP.writeFile(NodePath.join(root, "outside"), "outside-bundle");
        await NodeFSP.symlink("../outside", NodePath.join(app, "escape"));
        await expect(hashMacApp(app)).rejects.toThrow("App symlink escapes the staged app.");
      }),
  );
});
