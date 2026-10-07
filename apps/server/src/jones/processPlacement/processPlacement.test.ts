// @effect-diagnostics nodeBuiltinImport:off - native rejection checks spawn only a captured test child.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeURL from "node:url";
import { HostProcessArchitecture, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { describe, expect, it } from "vite-plus/test";
import {
  assertControlProcessPlacement,
  assertServerProcessPlacement,
  helperIdentityArguments,
  placementArguments,
  placementCommand,
  readProcessPlacementBinding,
  validateProcessPlacementBinding,
  ProcessPlacementError,
  type ProcessPlacementBinding,
} from "./processPlacement.ts";

const binding: ProcessPlacementBinding = {
  version: 1,
  helperPath: "/opt/t3/process-placement",
  helperSha256: "a".repeat(64),
  helperDevice: "1",
  helperInode: "10",
  control: { path: "/sys/fs/cgroup/t3-test/control", device: "29", inode: "101" },
  workload: { path: "/sys/fs/cgroup/t3-test/workload", device: "29", inode: "102" },
};
const environment = (value: unknown) => ({ T3_PROCESS_PLACEMENT: JSON.stringify(value) });

describe("required process placement", () => {
  it.each(["darwin", "win32"] as const)(
    "honors the injected %s host and refuses Linux placement",
    (platform) => {
      expect(() => validateProcessPlacementBinding(binding, platform)).toThrow(
        "Linux cgroup v2 is required",
      );
    },
  );
  it("leaves disabled commands and arguments intact", () => {
    const args = ["argument with spaces", "$HOME", "--"];
    expect(readProcessPlacementBinding({})).toBeUndefined();
    expect(placementCommand("payload", args, "workload", undefined)).toEqual({
      command: "payload",
      args,
    });
  });
  it("rejects invalid control placement before checking server priority", () => {
    expect(() => assertControlProcessPlacement(binding, "darwin")).toThrow(
      "Linux cgroup v2 is required",
    );
    expect(() => assertServerProcessPlacement(binding, "darwin")).toThrow(
      "Linux cgroup v2 is required",
    );
  });
  it("decodes the versioned binding without exposing unrelated environment", () => {
    expect(readProcessPlacementBinding(environment(binding))).toEqual(binding);
    expect(
      readProcessPlacementBinding(
        environment({ ...binding, serverPriority: { nice: -15, resetOnFork: true } }),
      ),
    ).toEqual({ ...binding, serverPriority: { nice: -15, resetOnFork: true } });
  });
  it.each([
    "",
    "null",
    "{}",
    JSON.stringify({ ...binding, version: 2 }),
    JSON.stringify({ ...binding, helperPath: "helper" }),
    JSON.stringify({ ...binding, extra: true }),
    JSON.stringify({ ...binding, helperPath: "/opt/unsafe\nhelper" }),
    JSON.stringify({
      ...binding,
      workload: { ...binding.workload, device: "18446744073709551616" },
    }),
    JSON.stringify({ ...binding, workload: { ...binding.workload, inode: "1".repeat(21) } }),
    JSON.stringify({ ...binding, workload: { ...binding.workload, extra: true } }),
    JSON.stringify({ ...binding, serverPriority: { nice: -15, resetOnFork: true, extra: true } }),
    JSON.stringify({ ...binding, helperSha256: "bad" }),
    JSON.stringify({ ...binding, helperDevice: undefined }),
    JSON.stringify({ ...binding, helperInode: "0" }),
    JSON.stringify({ ...binding, helperDevice: "18446744073709551616" }),
    JSON.stringify({ ...binding, workload: binding.control }),
    JSON.stringify({
      ...binding,
      workload: { ...binding.workload, path: `${binding.control.path}/child` },
    }),
    JSON.stringify({
      ...binding,
      workload: { ...binding.workload, path: "/sys/fs/cgroup/a/../b" },
    }),
    JSON.stringify({ ...binding, serverPriority: { nice: -15, resetOnFork: false } }),
  ])("rejects malformed, unsupported, or overlapping required bindings", (text) => {
    expect(() => readProcessPlacementBinding({ T3_PROCESS_PLACEMENT: text })).toThrow(
      ProcessPlacementError,
    );
  });
  it("limits server priority to the actual server, not the launcher or telemetry", () => {
    const priority = { ...binding, serverPriority: { nice: -15, resetOnFork: true } } as const;
    expect(placementArguments(priority, "control")).not.toContain("--server-priority");
    expect(placementArguments(priority, "control", true)).toEqual([
      ...helperIdentityArguments(binding),
      "--role",
      "control",
      "--cgroup",
      binding.control.path,
      "--device",
      "29",
      "--inode",
      "101",
      "--server-priority",
      "-15",
      "--",
    ]);
    expect(placementArguments(priority, "workload")).not.toContain("--server-priority");
  });
  it("rejects unavailable placement before handing any executable to a spawner", () => {
    let executed = false;
    expect(() => {
      const command = placementCommand("sentinel", [], "workload", binding);
      executed = command.command === "sentinel";
    }).toThrow(ProcessPlacementError);
    expect(executed).toBe(false);
  });
});

const helperPath = NodeURL.fileURLToPath(
  new URL(
    `../../../dist/native/process-placement-linux-${HostProcessArchitecture.defaultValue()}`,
    import.meta.url,
  ),
);
describe.skipIf(HostProcessPlatform.defaultValue() !== "linux" || !NodeFS.existsSync(helperPath))(
  "native placement gate",
  () => {
    const helperIdentity = () => {
      const stat = NodeFS.statSync(helperPath, { bigint: true });
      return helperIdentityArguments({
        ...binding,
        helperPath,
        helperDevice: String(stat.dev),
        helperInode: String(stat.ino),
        helperSha256: NodeCrypto.createHash("sha256")
          .update(NodeFS.readFileSync(helperPath))
          .digest("hex"),
      });
    };
    it.each(["control", "workload"])("never executes %s payload when placement fails", (role) => {
      const result = NodeChildProcess.spawnSync(
        helperPath,
        [
          ...helperIdentity(),
          "--role",
          role,
          "--cgroup",
          "/sys/fs/cgroup/t3-placement-test-missing",
          "--device",
          "0",
          "--inode",
          "1",
          "--",
          process.execPath,
          "-e",
          "process.stdout.write('PAYLOAD_EXECUTED')",
        ],
        { encoding: "utf8" },
      );
      expect(result.error).toBeUndefined();
      expect(result.status).toBe(125);
      expect(result.stdout).toBe("");
      expect(result.stderr).toContain("cgroup unavailable or symlinked");
    });
    it.each(["path", "sha256", "device", "inode"])(
      "rejects stale executed helper %s identity",
      (field) => {
        const args = [...helperIdentity()];
        const index = args.indexOf(`--helper-${field}`) + 1;
        args[index] =
          field === "path" ? `${helperPath}.other` : field === "sha256" ? "0".repeat(64) : "0";
        const result = NodeChildProcess.spawnSync(
          helperPath,
          [...args, "--verify-server-priority", String(process.pid)],
          { encoding: "utf8" },
        );
        expect(result.status).toBe(125);
        expect(result.stderr).toMatch(/helper (identity|hash) changed/);
      },
    );
    it("rejects ordinary filesystem directories before payload execution", () => {
      const result = NodeChildProcess.spawnSync(
        helperPath,
        [
          ...helperIdentity(),
          "--role",
          "workload",
          "--cgroup",
          "/tmp",
          "--device",
          "0",
          "--inode",
          "1",
          "--",
          process.execPath,
          "-e",
          "process.stdout.write('PAYLOAD_EXECUTED')",
        ],
        { encoding: "utf8" },
      );
      expect(result.status).toBe(125);
      expect(result.stdout).toBe("");
    });
  },
);
