// @effect-diagnostics nodeBuiltinImport:off - Exercises the generated host program with synthetic filesystem and process services.
import { describe, expect, it } from "vitest";
import { createRequire } from "node:module";
import * as VM from "node:vm";
import { remoteDeviceScript } from "../../device/sshDeviceScript.ts";

const nodeRequire = createRequire(import.meta.url);

const invoke = async (
  mode: Parameters<typeof remoteDeviceScript>[1],
  setup: "ready" | "required" | "missing" | "timeout",
) => {
  const commands: { command: string; args: string[]; timeout: number }[] = [];
  const locks = new Map<string, string>();
  const output: string[] = [];
  const errors: string[] = [];
  const absent = () => { throw Object.assign(new Error("absent"), { code: "ENOENT" }); };
  const fs = {
    readdirSync: () => [], readFileSync: absent, existsSync: () => false,
    mkdirSync: () => {}, rmSync: () => {},
    symlinkSync: (token: string, path: string) => { locks.set(path, token); },
    readlinkSync: (path: string) => locks.get(path) ?? absent(),
    unlinkSync: (path: string) => { locks.delete(path); },
  };
  const context = {
    process: { platform: "darwin", pid: 123, versions: { node: "26.12.1" }, execPath: "/test/node", env: {}, exitCode: 0 },
    console: { log: (value: string) => output.push(value), error: (value: string) => errors.push(value) },
    require: (name: string) => {
      if (name === "node:fs") return fs;
      if (name === "node:os") return { homedir: () => "/test/home" };
      if (name === "node:child_process") return {
        spawn: () => { throw new Error("Probe/stop must not spawn helpers"); },
        spawnSync: (command: string, args: string[], options: { timeout: number }) => {
          commands.push({ command, args, timeout: options.timeout });
          if (command === "xcodebuild") return {
            status: setup === "required" ? 69 : setup === "ready" ? 0 : null,
            ...(setup === "missing" || setup === "timeout"
              ? { error: { code: setup === "missing" ? "ENOENT" : "ETIMEDOUT" } } : {}),
          };
          if (command === "xcrun" && setup !== "ready") throw new Error("simctl must not run before Xcode is ready");
          return { status: 0, stdout: "", stderr: "" };
        },
      };
      return nodeRequire(name);
    },
  };
  await VM.runInNewContext(remoteDeviceScript("test-owner", mode, { hostId: "mini", generation: "test-generation" }), context);
  expect(errors).toEqual([]);
  expect(context.process.exitCode).toBe(0);
  expect(locks.size).toBe(0);
  return { commands, output: output[0] ? JSON.parse(output[0]) : null };
};

describe("SSH Xcode readiness", () => {
  it.each([
    ["required", /setup is incomplete/],
    ["timeout", /timed out/],
    ["missing", /tools were not found/],
  ] as const)("reports %s setup without invoking simctl or blocking Android", async (setup, reason) => {
    const { commands, output } = await invoke("probe", setup);
    expect(output.platforms).toEqual([
      { platform: "ios", available: false, reason: expect.stringMatching(reason) },
      { platform: "android", available: true },
    ]);
    expect(commands.some((command) => command.command === "xcrun")).toBe(false);
    expect(commands.find((command) => command.command === "xcodebuild")?.timeout).toBe(5000);
  });

  it("probes iOS after setup completes", async () => {
    const { commands, output } = await invoke("probe", "ready");
    expect(output.platforms.every((platform: { available: boolean }) => platform.available)).toBe(true);
    expect(commands.map((command) => command.command)).toEqual(["xcodebuild", "xcrun", "adb", "npm"]);
  });

  it.each(["stop", "stop-agent", "stop-direct"] as const)("keeps %s independent of platform probes", async (mode) => {
    expect((await invoke(mode, "required")).commands).toEqual([]);
  });
});
