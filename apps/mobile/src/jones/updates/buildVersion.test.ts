import { EnvironmentId, ExecutionEnvironmentDescriptor } from "@t3tools/contracts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import { mobileJonesBuildDescription, mobileJonesBuildRef } from "./buildVersion";

const installed = "0.0.45-preview.20261020.100.1";
const offered = "0.0.45-preview.20261020.101.1";
const environment = Schema.decodeUnknownSync(ExecutionEnvironmentDescriptor)({
  environmentId: EnvironmentId.make("mobile-build-test"),
  label: "Synthetic host",
  platform: { os: "linux", arch: "x64" },
  serverVersion: installed,
  jonesBuildStartedAt: "2026-10-20T21:13:43Z",
  capabilities: {},
});
const state: JonesUpdateState = {
  source: "jones-actions",
  channel: "jones-main",
  phase: "available",
  capability: { check: true, download: true, install: false },
  currentVersion: installed,
  currentBuildStartedAt: "2026-10-20T21:13:43Z",
  provenance: {
    repository: "Jones-Systems/Jones-Code",
    sourceSha: "b".repeat(40),
    sourceTree: "c".repeat(40),
    workflow: ".github/workflows/artifact-cli-linux.yml",
    runId: 101,
    runAttempt: 1,
    artifactId: 4,
    artifactDigest: "d".repeat(64),
    platform: "linux",
    architecture: "x64",
    version: offered,
    buildStartedAt: "2026-10-20T21:13:47Z",
  },
};

describe("mobile Jones build details", () => {
  it("uses the installed descriptor and offered provenance only for exact versions", () => {
    expect(mobileJonesBuildRef(installed, environment, state).startedAt).toBe(
      "2026-10-20T21:13:43Z",
    );
    expect(mobileJonesBuildRef(offered, environment, state).startedAt).toBe("2026-10-20T21:13:47Z");
    expect(mobileJonesBuildRef(installed, undefined, state).startedAt).toBe("2026-10-20T21:13:43Z");
    expect(mobileJonesBuildRef(`${offered}0`, environment, state)).toEqual({
      version: `${offered}0`,
    });
  });

  it("keeps the full version in update confirmation and success text", () => {
    expect(mobileJonesBuildDescription(mobileJonesBuildRef(offered, environment, state))).toBe(
      `1020·1713 (${offered})`,
    );
    expect(mobileJonesBuildDescription({ version: installed })).toBe(installed);
    expect(mobileJonesBuildDescription({ version: installed, sourceSha: "a".repeat(40) })).toBe(
      `aaaaaaa (${installed})`,
    );
  });
});
