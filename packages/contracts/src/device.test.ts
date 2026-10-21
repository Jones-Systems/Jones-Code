import { describe, expect, it } from "@effect/vitest";
import { Schema } from "effect";
import {
  DeviceDirectAccess,
  DeviceServiceState,
  SshDeviceHostConfig,
  EasDeviceHostConfig,
  EasDeviceHostConfigs,
  SshDeviceHostConfigs,
} from "./device.ts";
import { secretSettingsFields } from "./settings.ts";

const decodeSshHosts = Schema.decodeUnknownSync(SshDeviceHostConfigs);
const decodeEas = Schema.decodeUnknownSync(EasDeviceHostConfigs);
const encodeState = Schema.encodeSync(DeviceServiceState);
const decodeState = Schema.decodeUnknownSync(DeviceServiceState);
// The host summary as clients decoded it before EAS hosts existed.
const decodeOlderState = Schema.decodeUnknownSync(
  Schema.Struct({
    hosts: Schema.Array(
      Schema.Struct({ id: Schema.String, kind: Schema.Literals(["local", "ssh"]) }),
    ),
  }),
);

describe("configured device hosts", () => {
  it("reads SSH hosts saved before hosts had a kind, and bounds EAS sessions", () => {
    expect(decodeSshHosts([{ id: "mac", label: "Mac mini", target: "julius@mac" }])).toEqual([
      { kind: "ssh", id: "mac", label: "Mac mini", target: "julius@mac" },
    ]);
    expect(decodeEas([{ kind: "eas", id: "eas", label: "EAS Simulator" }])).toEqual([
      {
        kind: "eas",
        id: "eas",
        label: "EAS Simulator",
        accessToken: "",
        maxSessionMinutes: 30,
        idleMinutes: 10,
        maxConcurrentSessions: 2,
      },
    ]);
    expect(() =>
      decodeEas([{ kind: "eas", id: "eas", label: "EAS", maxSessionMinutes: 600 }]),
    ).toThrow();
    // The default idle limit of 10 is not smaller than a session limit of 10.
    expect(() =>
      decodeEas([{ kind: "eas", id: "eas", label: "EAS", maxSessionMinutes: 10 }]),
    ).toThrow();
  });

  it("marks the EAS access token as a secret setting", () => {
    expect(secretSettingsFields(EasDeviceHostConfig)).toEqual(["accessToken"]);
  });
});

describe("device state for clients from before EAS hosts", () => {
  const summary = {
    label: "Host",
    platforms: [],
    hubInstalled: true,
    agentDeviceInstalled: true,
  };
  const state = {
    hosts: [
      { ...summary, id: "local", kind: "local" as const },
      { ...summary, id: "eas", kind: "eas" as const },
    ],
    hostStatus: "ready" as const,
    hostStatuses: {},
    devices: [],
    sessions: [],
    onboardingCompleted: true,
    agentAccessEnabled: true,
    hubBasePath: "/api/device-hub",
    revision: 1,
  };

  it("sends EAS hosts where an older client does not look, and reads them back", () => {
    const wire = encodeState(state);
    expect(decodeOlderState(wire).hosts.map((host) => host.id)).toEqual(["local"]);
    expect(decodeState(wire).hosts).toEqual(state.hosts);
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
