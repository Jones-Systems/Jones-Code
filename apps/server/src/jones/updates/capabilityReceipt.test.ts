import { describe, expect, it } from "vite-plus/test";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Exercises native receipt replacement and mode bits on an isolated real filesystem.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Paths belong to the native filesystem fixture consumed by the Promise adapter.
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { publishJonesUpdateCapabilityReceipt } from "./capabilityReceipt.ts";

describe("auth-free update capability receipt", () => {
  it("publishes the actual process capability and atomically replaces it on state changes", async () => {
    const baseDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-capability-"));
    try {
      const receipt = {
        schema: 1 as const,
        baseDir,
        environmentId: "fixture",
        currentVersion: "1.0.0",
        processId: 123,
        capability: { install: true },
        qualifiedLauncher: true,
      };
      await publishJonesUpdateCapabilityReceipt(receipt);
      const target = NodePath.join(baseDir, "runtime", "jones-update-capability.json");
      expect(JSON.parse(await NodeFSP.readFile(target, "utf8"))).toEqual(receipt);
      expect((await NodeFSP.stat(target)).mode & 0o777).toBe(0o600);
      await publishJonesUpdateCapabilityReceipt({ ...receipt, capability: { install: false } });
      expect(JSON.parse(await NodeFSP.readFile(target, "utf8")).capability.install).toBe(false);
      expect(await NodeFSP.readdir(NodePath.dirname(target))).toEqual([
        "jones-update-capability.json",
      ]);
    } finally {
      await NodeFSP.rm(baseDir, { recursive: true, force: true });
    }
  });
  it("preserves a receipt with a different environment identity", async () => {
    const baseDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-capability-"));
    try {
      const receipt = {
        schema: 1 as const,
        baseDir,
        environmentId: "fixture",
        currentVersion: "1.0.0",
        processId: 123,
        capability: { install: true },
        qualifiedLauncher: true,
      };
      await publishJonesUpdateCapabilityReceipt(receipt);
      await expect(
        publishJonesUpdateCapabilityReceipt({ ...receipt, environmentId: "other" }),
      ).rejects.toThrow("different native identity");
      const target = NodePath.join(baseDir, "runtime", "jones-update-capability.json");
      expect(JSON.parse(await NodeFSP.readFile(target, "utf8"))).toEqual(receipt);
    } finally {
      await NodeFSP.rm(baseDir, { recursive: true, force: true });
    }
  });
});
