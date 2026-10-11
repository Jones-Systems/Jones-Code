import { describe, expect, it, vi } from "vite-plus/test";

const branding = vi.hoisted(() => ({
  APP_VERSION: "0.0.45-preview.20261010.38048764253.1",
  APP_SOURCE_SHA: "a".repeat(40),
}));
vi.mock("../../branding", () => branding);

import { resolveVersionMismatch } from "../../versionSkew";
import { isJonesPreviewBuildPair } from "./versionSkew";

describe("Jones preview version skew", () => {
  it.each([
    "0.0.45-preview.20261008.37762212791.1",
    "0.0.45-preview.20261010.38048764252.1",
    "0.0.45-preview.20261010.38048764253",
  ])("does not infer source order from preview artifact %s", (serverVersion) => {
    expect(resolveVersionMismatch(serverVersion)).toBeNull();
    expect(resolveVersionMismatch(serverVersion, "behind")).toMatchObject({
      clientVersion: branding.APP_VERSION,
      serverVersion,
    });
  });

  it("ignores artifact ordering for a source that is equal, ahead, or unknown", () => {
    const lowerRun = "0.0.45-preview.20261008.37762212791.1";
    for (const currency of ["current", "ahead", "unknown", "diverged"] as const) {
      expect(resolveVersionMismatch(lowerRun, currency)).toBeNull();
    }
    expect(resolveVersionMismatch("0.0.45-preview.20261011.38048764299.2", "behind")).toMatchObject(
      { serverVersion: "0.0.45-preview.20261011.38048764299.2" },
    );
  });

  it.each([
    "0.0.45-preview.20261010.38048764253.1",
    "0.0.45-preview.20261010.38048764253.2",
    "0.0.45-preview.20261010.38048764254.1",
    "0.0.45-preview.20261011.38048764253.1",
  ])("does not offer a downgrade for current or newer preview %s", (serverVersion) => {
    expect(resolveVersionMismatch(serverVersion)).toBeNull();
  });

  it("keeps stable and upstream nightly channel comparisons unchanged", () => {
    expect(resolveVersionMismatch("0.0.45")).toBeNull();
    expect(resolveVersionMismatch("0.0.45-nightly.20261008.2801")).toBeNull();
    expect(isJonesPreviewBuildPair(branding.APP_VERSION, "0.0.45-preview.unqualified")).toBe(false);
  });
});
