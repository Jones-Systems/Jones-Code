import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { SshDeviceHostConfig, DeviceDirectAccess, deviceToolInstallMessage } from "./device.ts";

describe("device tool install progress", () => {
  it("distinguishes a new install from an upgrade and chooses versions numerically", () => {
    expect(
      deviceToolInstallMessage("device hub", {
        requiredVersion: "0.11.0",
        installedVersions: [],
        runningVersion: null,
      }),
    ).toBe("Installing device hub 0.11.0…");
    expect(
      deviceToolInstallMessage("device hub", {
        requiredVersion: "0.11.0",
        installedVersions: ["0.9.0", "0.10.0"],
        runningVersion: null,
      }),
    ).toBe("Updating device hub from 0.10.0 to 0.11.0…");
  });
});

const decodeDirectAccess = Schema.decodeUnknownSync(DeviceDirectAccess);

it("accepts an optional desktop SSH target and rejects option or whitespace injection", () => {
  const parse = Schema.decodeUnknownSync(SshDeviceHostConfig);
  expect(
    parse({ id: "mini", label: "Mini", target: "server-mini" }).directSshTarget,
  ).toBeUndefined();
  expect(
    parse({ id: "mini", label: "Mini", target: "server-mini", directSshTarget: "laptop-mini" })
      .directSshTarget,
  ).toBe("laptop-mini");
  for (const directSshTarget of [
    "-oProxyCommand=command",
    "mini command",
    "mini\ncommand",
    "user@mini",
    "mini:22",
    "[::1]",
    "m".repeat(256),
  ])
    expect(() =>
      parse({ id: "mini", label: "Mini", target: "server-mini", directSshTarget }),
    ).toThrow();
  expect(() =>
    decodeDirectAccess({
      target: "mini",
      gatewayPort: 65536,
      owner: "owner",
      generation: "gen",
      grant: "grant",
      expiresAt: 1,
    }),
  ).toThrow();
});
