import { describe, expect, it, vi } from "vite-plus/test";

const branding = vi.hoisted(() => ({ APP_VERSION: "0.0.45-preview.20261010.38048764253.1" }));
vi.mock("../../branding", () => branding);

import { resolveVersionMismatch } from "../../versionSkew";
import { isJonesPreviewBuildPair } from "./versionSkew";

describe("Jones preview version skew", () => {
  it.each([
    "0.0.45-preview.20261008.37762212791.1",
    "0.0.45-preview.20261010.38048764252.1",
    "0.0.45-preview.20261010.38048764253",
  ])("offers an update for older preview %s on the same core release", (serverVersion) => {
    expect(resolveVersionMismatch(serverVersion)).toMatchObject({
      clientVersion: branding.APP_VERSION,
      serverVersion,
    });
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
