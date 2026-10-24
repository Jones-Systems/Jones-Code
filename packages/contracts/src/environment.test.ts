import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { ExecutionEnvironmentDescriptor } from "./environment.ts";

const decodeDescriptor = Schema.decodeUnknownSync(ExecutionEnvironmentDescriptor);

const descriptor = {
  environmentId: "environment-1",
  label: "Local",
  platform: { os: "darwin", arch: "arm64" },
  serverVersion: "0.0.32",
  capabilities: { repositoryIdentity: true },
} as const;

describe("ExecutionEnvironmentDescriptor", () => {
  it("keeps queued-run start opt-in without inferring it from other queue capabilities", () => {
    expect(decodeDescriptor(descriptor).capabilities.queuedRunStart).toBeUndefined();
    expect(
      decodeDescriptor({
        ...descriptor,
        capabilities: {
          ...descriptor.capabilities,
          threadStop: true,
          queuedToolBoundaryDelivery: true,
        },
      }).capabilities.queuedRunStart,
    ).toBeUndefined();
    for (const queuedRunStart of [true, false]) {
      expect(
        decodeDescriptor({
          ...descriptor,
          capabilities: { ...descriptor.capabilities, queuedRunStart },
        }).capabilities.queuedRunStart,
      ).toBe(queuedRunStart);
    }
  });
  it("round-trips old descriptors and accepts an optional build time", () => {
    expect(decodeDescriptor(descriptor)).toEqual(descriptor);
    const stamped = { ...descriptor, jonesBuildStartedAt: "2026-10-20T21:13:43Z" };
    expect(decodeDescriptor(stamped)).toEqual(stamped);
    expect(() =>
      decodeDescriptor({ ...descriptor, jonesBuildStartedAt: "2026-02-30T12:00:00Z" }),
    ).toThrow();
  });
  it("decodes old, recognized and future manual installation descriptors", () => {
    expect(decodeDescriptor(descriptor).capabilities.serverInstallation).toBeUndefined();
    for (const installation of [{ kind: "npx" }, { kind: "npm-global", prefix: "/opt/node" }]) {
      expect(
        decodeDescriptor({
          ...descriptor,
          capabilities: { ...descriptor.capabilities, serverInstallation: installation },
        }).capabilities.serverInstallation,
      ).toEqual(installation);
    }
    for (const installation of [{ kind: "future-manager" }, { kind: "npm-global" }]) {
      expect(
        decodeDescriptor({
          ...descriptor,
          capabilities: { ...descriptor.capabilities, serverInstallation: installation },
        }).capabilities.serverInstallation,
      ).toBeUndefined();
    }
  });
});
