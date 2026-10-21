import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import { JonesUpdateState } from "./jonesUpdates.ts";

const decode = Schema.decodeUnknownSync(JonesUpdateState);
const state = {
  source: "jones-actions",
  channel: "jones-main",
  phase: "available",
  capability: { check: true, download: true, install: true },
  currentVersion: "0.0.45-preview.20261020.100.1",
  provenance: {
    repository: "Jones-Systems/Jones-Code",
    sourceSha: "a".repeat(40),
    sourceTree: "b".repeat(40),
    workflow: ".github/workflows/artifact-cli-linux.yml",
    runId: 101,
    runAttempt: 1,
    artifactId: 500,
    artifactDigest: "c".repeat(64),
    platform: "linux",
    architecture: "x64",
    version: "0.0.45-preview.20261020.101.1",
  },
  stagedHandle: "d".repeat(64),
  outcome: { status: "committed", fromVersion: "old", targetVersion: "new" },
} as const;

describe("optional Jones update build times", () => {
  it("round-trips old payloads without adding fields", () => {
    expect(decode(state)).toEqual(state);
  });
  it("preserves optional times beside unchanged machine identities", () => {
    const stamped = {
      ...state,
      currentBuildStartedAt: "2026-10-20T21:13:43Z",
      provenance: { ...state.provenance, buildStartedAt: "2026-10-20T21:13:47.123Z" },
    };
    const result = decode(stamped);
    expect(result).toEqual(stamped);
    expect(result.stagedHandle).toBe(state.stagedHandle);
    expect(result.outcome).toEqual(state.outcome);
    expect(result.currentVersion).toBe(state.currentVersion);
    expect(result.provenance?.version).toBe(state.provenance.version);
  });
  it.each(["2026-02-30T12:00:00Z", "2026-10-20T24:00:00Z", "unknown"])(
    "rejects malformed optional timestamp %s",
    (value) => {
      expect(() => decode({ ...state, currentBuildStartedAt: value })).toThrow();
      expect(() =>
        decode({ ...state, provenance: { ...state.provenance, buildStartedAt: value } }),
      ).toThrow();
    },
  );
});
