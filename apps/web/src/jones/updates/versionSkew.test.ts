import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";

const branding = vi.hoisted(() => ({
  APP_VERSION: "0.0.45-preview.20261010.38048764253.1",
  APP_SOURCE_SHA: "a".repeat(40),
  APP_BUILD_STARTED_AT: undefined,
}));
vi.mock("../../branding", () => branding);

import {
  resolveServerConfigClientUpdateNotice,
  resolveServerConfigVersionMismatch,
  resolveVersionMismatch,
} from "../../versionSkew";
import {
  isJonesPreviewBuildPair,
  JONES_CLIENT_UPDATE_HINT,
  resolveJonesClientUpdateNotice,
} from "./versionSkew";

describe("Jones preview version skew", () => {
  it.each([
    "0.0.45-preview.20261008.37762212791.1",
    "0.0.45-preview.20261010.38048764252.1",
    "0.0.45-preview.20261010.38048764253",
  ])("does not infer source order from preview artifact %s", (serverVersion) => {
    expect(resolveVersionMismatch(serverVersion)).toBeNull();
    expect(resolveVersionMismatch(serverVersion, branding.APP_VERSION, "behind")).toMatchObject({
      clientVersion: branding.APP_VERSION,
      serverVersion,
    });
  });

  it("ignores artifact ordering for a source that is equal, ahead, or unknown", () => {
    const lowerRun = "0.0.45-preview.20261008.37762212791.1";
    for (const currency of ["current", "ahead", "unknown", "diverged"] as const) {
      expect(resolveVersionMismatch(lowerRun, branding.APP_VERSION, currency)).toBeNull();
    }
    const higherRun = "0.0.45-preview.20261011.38048764299.2";
    expect(resolveVersionMismatch(higherRun, branding.APP_VERSION, "behind")).toMatchObject({
      serverVersion: higherRun,
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

describe("Jones client update notice", () => {
  const environment = (serverVersion: string, sha?: string) => ({
    environment: {
      environmentId: EnvironmentId.make("environment-newer-server"),
      label: "Remote",
      platform: { os: "darwin", arch: "arm64" } as const,
      serverVersion,
      capabilities: { repositoryIdentity: true },
      ...(sha === undefined
        ? {}
        : { jonesSource: { repository: "Jones-Systems/Jones-Code" as const, sha, tree: sha } }),
    },
  });

  it.each([
    ["0.0.46", "0.0.45"],
    ["0.0.46+server", "0.0.45+client"],
    ["0.1.0", "0.0.45-preview.20261010.38048764253.1"],
    ["0.0.45-nightly.20261009.2", "0.0.45-nightly.20261009.1"],
  ])(
    "tells the user to update this app when server %s is newer than client %s",
    (server, client) => {
      expect(resolveJonesClientUpdateNotice(server, client)).toEqual({
        clientVersion: client,
        serverVersion: server,
        kind: "server-newer",
        hint: JONES_CLIENT_UPDATE_HINT,
      });
    },
  );

  it("reports unequal preview sources without claiming source order", () => {
    for (const server of ["0.0.45-preview.20261008.1", "0.0.45-preview.20261011.999"]) {
      expect(
        resolveJonesClientUpdateNotice(server, branding.APP_VERSION, {
          clientSourceSha: "a".repeat(40),
          serverSourceSha: "b".repeat(40),
        }),
      ).toMatchObject({
        kind: "preview-source-difference",
        serverVersion: server,
        hint: "Client and server use different Jones preview sources. Which source is newer is unknown.",
      });
    }
  });

  it.each([
    ["0.0.45-preview.20261010.1+client", "0.0.45-preview.20261011.2+server"],
    ["0.0.45-preview.20261010.1+client", "0.1.0-preview.20261011.2+server"],
    ["0.1.0-preview.20261010.1+client", "0.0.45-preview.20261011.2+server"],
    ["0.0.45-preview.20261010.1", "0.1.0-preview.20261011.2+server"],
  ])(
    "keeps preview source policy for client %s and server %s with build metadata",
    (client, server) => {
      expect(isJonesPreviewBuildPair(client, server)).toBe(true);
      expect(
        resolveJonesClientUpdateNotice(server, client, {
          clientSourceSha: "a".repeat(40),
          serverSourceSha: "b".repeat(40),
        }),
      ).toMatchObject({
        kind: "preview-source-difference",
        clientVersion: client,
        serverVersion: server,
        hint: "Client and server use different Jones preview sources. Which source is newer is unknown.",
      });
      for (const sources of [
        { clientSourceSha: "a".repeat(40), serverSourceSha: "a".repeat(40) },
        { clientSourceSha: "a".repeat(40), serverSourceSha: undefined },
        { clientSourceSha: undefined, serverSourceSha: "b".repeat(40) },
      ]) {
        expect(resolveJonesClientUpdateNotice(server, client, sources)).toBeNull();
      }
      expect(resolveVersionMismatch(server, client)).toBeNull();
      expect(resolveVersionMismatch(server, client, "ahead")).toBeNull();
      expect(resolveVersionMismatch(server, client, "behind")).toMatchObject({
        clientVersion: client,
        serverVersion: server,
        hint: "A newer Jones source is available. Use the qualified update controls for this host.",
      });
    },
  );

  it.each([undefined, "", "abc", "g".repeat(40), "a".repeat(40), "A".repeat(40)])(
    "keeps equal or invalid preview source %s quiet",
    (serverSourceSha) => {
      expect(
        resolveJonesClientUpdateNotice("0.0.45-preview.20261011.999", branding.APP_VERSION, {
          clientSourceSha: "a".repeat(40),
          serverSourceSha,
        }),
      ).toBeNull();
      expect(
        resolveJonesClientUpdateNotice("0.0.45-preview.20261011.999", branding.APP_VERSION, {
          clientSourceSha: undefined,
          serverSourceSha: "b".repeat(40),
        }),
      ).toBeNull();
    },
  );

  it.each([
    ["0.0.45", "0.0.45"],
    ["0.0.44", "0.0.45"],
    ["0.0.45-nightly.20261009.1", "0.0.45"],
    ["0.0.45-nightly.20261009.1", "0.0.45-nightly.20261009.2"],
    ["0.0.45", "0.0.45-nightly.20261009.1"],
    ["dev", "0.0.45"],
    ["dev-preview.20261010.1", "0.0.45-preview.20261010.1"],
    ["0.0.46", "dev"],
    ["", "0.0.45"],
  ])("does not warn for equal, older, or unordered server %s and client %s", (server, client) => {
    expect(resolveJonesClientUpdateNotice(server, client)).toBeNull();
  });

  it("never offers both a server update and a client update for one pair", () => {
    expect(resolveServerConfigVersionMismatch(environment("0.0.46"))).toBeNull();
    expect(resolveServerConfigClientUpdateNotice(environment("0.0.46"))).toMatchObject({
      serverVersion: "0.0.46",
      clientVersion: branding.APP_VERSION,
      hint: JONES_CLIENT_UPDATE_HINT,
    });
    expect(resolveServerConfigClientUpdateNotice(environment("0.0.44"))).toBeNull();
    expect(
      resolveServerConfigClientUpdateNotice(
        environment("0.0.45-preview.20261011.38048764299.2", branding.APP_SOURCE_SHA),
      ),
    ).toBeNull();
  });
});
