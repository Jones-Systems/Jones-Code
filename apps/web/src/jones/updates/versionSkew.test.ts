import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";

const branding = vi.hoisted(() => ({
  APP_VERSION: "0.1.2-preview.20261029.40652017132.1",
  APP_SOURCE_SHA: "a".repeat(40),
  APP_BUILD_STARTED_AT: "2026-10-29T18:36:00Z",
}));
vi.mock("../../branding", () => branding);

import {
  resolveServerConfigClientUpdateNotice,
  resolveServerConfigVersionMismatch,
  resolveVersionMismatch,
} from "../../versionSkew";
import {
  compareJonesPreviewVersions,
  isJonesPreviewBuildPair,
  JONES_CLIENT_UPDATE_HINT,
  resolveJonesClientUpdateNotice,
} from "./versionSkew";

const older = "0.1.2-preview.20261028.40652016025.1";
const sources = { clientSourceSha: "a".repeat(40), serverSourceSha: "b".repeat(40) };
const environment = (serverVersion: string, sha?: string) => ({
  environment: {
    environmentId: EnvironmentId.make("environment-version-skew"),
    label: "VPS",
    platform: { os: "linux", arch: "x64" } as const,
    serverVersion,
    capabilities: { repositoryIdentity: true, serverSelfUpdate: "respawn" as const },
    ...(sha === undefined
      ? {}
      : { jonesSource: { repository: "Jones-Systems/Jones-Code" as const, sha, tree: sha } }),
  },
});

describe("Jones preview build ordering", () => {
  it.each([
    [branding.APP_VERSION, older],
    ["0.1.3-preview.20261001.1.1", "0.1.2-preview.20261029.999.9"],
    ["0.1.2-preview.20261101.1.1", "0.1.2-preview.20261031.999.9"],
    ["0.1.2-preview.20261029.10.1", "0.1.2-preview.20261029.9.9"],
    ["0.1.2-preview.20261029.10.10", "0.1.2-preview.20261029.10.9"],
  ])("orders %s after %s in both directions", (client, server) => {
    expect(compareJonesPreviewVersions(client, server)).toBeGreaterThan(0);
    expect(compareJonesPreviewVersions(server, client)).toBeLessThan(0);
    expect(resolveJonesClientUpdateNotice(server, client, sources)?.kind).toBe("client-newer");
    expect(resolveJonesClientUpdateNotice(client, server, sources)).toMatchObject({
      kind: "server-newer",
      hint: JONES_CLIENT_UPDATE_HINT,
    });
  });

  it("ignores metadata and treats a legacy run as its first attempt", () => {
    expect(
      compareJonesPreviewVersions(
        " 0.1.2-preview.20261029.10+client ",
        "0.1.2-preview.20261029.10.1+server",
      ),
    ).toBe(0);
  });

  it.each([
    "dev",
    "dev-preview.20261029.1",
    "0.1.2-preview.unqualified",
    "",
    "0.1.2-nightly.20261029.1",
  ])("does not order an unrecognized preview pair containing %s", (version) => {
    expect(compareJonesPreviewVersions(branding.APP_VERSION, version)).toBeNull();
    expect(isJonesPreviewBuildPair(branding.APP_VERSION, version)).toBe(false);
  });

  it("does not turn a build comparison into an unqualified upstream server update", () => {
    expect(resolveVersionMismatch(older, branding.APP_VERSION)).toBeNull();
    expect(
      resolveServerConfigVersionMismatch(environment(older, sources.serverSourceSha)),
    ).toBeNull();
    expect(
      resolveServerConfigClientUpdateNotice(environment(older, sources.serverSourceSha)),
    ).toMatchObject({
      kind: "client-newer",
      clientVersion: branding.APP_VERSION,
      serverVersion: older,
      clientSourceSha: branding.APP_SOURCE_SHA,
    });
  });
});

describe("Jones version notices", () => {
  it("identifies an older VPS build even without source metadata", () => {
    expect(resolveJonesClientUpdateNotice(older, branding.APP_VERSION)).toMatchObject({
      kind: "client-newer",
      hint: "This app has a newer Jones preview build than this server. Update this server to catch up.",
    });
  });

  it("uses recorded build time to order a later rebuild of an earlier workflow run", () => {
    const stamps = {
      ...sources,
      clientBuildStartedAt: "2026-10-30T00:43:00Z",
      serverBuildStartedAt: "2026-10-29T18:36:00Z",
    };
    expect(resolveJonesClientUpdateNotice(branding.APP_VERSION, older, stamps)?.kind).toBe(
      "client-newer",
    );
    expect(
      resolveJonesClientUpdateNotice(older, branding.APP_VERSION, {
        ...sources,
        clientBuildStartedAt: stamps.serverBuildStartedAt,
        serverBuildStartedAt: stamps.clientBuildStartedAt,
      })?.kind,
    ).toBe("server-newer");
  });

  it("orders recorded instants across the repeated Eastern daylight-saving hour", () => {
    expect(
      resolveJonesClientUpdateNotice(older, branding.APP_VERSION, {
        ...sources,
        clientBuildStartedAt: "2026-11-01T05:59:00Z",
        serverBuildStartedAt: "2026-11-01T06:01:00Z",
      })?.kind,
    ).toBe("server-newer");
  });

  it.each([undefined, "invalid", "2026-10-28T15:26:00Z"])(
    "falls back to numeric preview ordering for absent, invalid or tied build times: %s",
    (serverBuildStartedAt) => {
      expect(
        resolveJonesClientUpdateNotice(older, branding.APP_VERSION, {
          ...sources,
          clientBuildStartedAt: "2026-10-28T15:26:00Z",
          serverBuildStartedAt,
        })?.kind,
      ).toBe("client-newer");
    },
  );

  it("passes build times from the actual server descriptor into the comparison", () => {
    const config = environment("0.1.2-preview.20261030.999.1", sources.serverSourceSha);
    expect(
      resolveServerConfigClientUpdateNotice({
        environment: { ...config.environment, jonesBuildStartedAt: "2026-10-28T15:26:00Z" },
      })?.kind,
    ).toBe("client-newer");
  });

  it("keeps rebuilds from the same verified source quiet", () => {
    for (const server of [older, "0.1.2-preview.20261030.999.2"])
      expect(
        resolveJonesClientUpdateNotice(server, branding.APP_VERSION, {
          clientSourceSha: "a".repeat(40),
          serverSourceSha: "A".repeat(40),
        }),
      ).toBeNull();
  });

  it("reports equal versions with conflicting source identities without choosing an update direction", () => {
    expect(
      resolveJonesClientUpdateNotice(branding.APP_VERSION, branding.APP_VERSION, sources),
    ).toMatchObject({
      kind: "preview-identity-mismatch",
      hint: "Client and server report the same Jones preview version but different sources. Check the build identities before updating.",
    });
  });

  it.each([undefined, "", "abc", "g".repeat(40), "a".repeat(40), "A".repeat(40)])(
    "does not report an identity conflict for equal versions and source %s",
    (serverSourceSha) => {
      expect(
        resolveJonesClientUpdateNotice(branding.APP_VERSION, branding.APP_VERSION, {
          clientSourceSha: "a".repeat(40),
          serverSourceSha,
        }),
      ).toBeNull();
    },
  );

  it.each([
    ["0.0.46", "0.0.45"],
    ["0.0.46+server", "0.0.45+client"],
    ["0.1.0", "0.0.45-preview.20261010.38048764253.1"],
    ["0.0.45-nightly.20261009.2", "0.0.45-nightly.20261009.1"],
  ])("retains client guidance for newer stable or nightly server %s", (server, client) => {
    expect(resolveJonesClientUpdateNotice(server, client)).toEqual({
      clientVersion: client,
      serverVersion: server,
      kind: "server-newer",
      hint: JONES_CLIENT_UPDATE_HINT,
    });
  });

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
  ])("keeps equal, older or unordered non-preview server %s quiet", (server, client) => {
    expect(resolveJonesClientUpdateNotice(server, client)).toBeNull();
  });
});
