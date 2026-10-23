import { describe, expect, it } from "vite-plus/test";
import {
  componentIdentifier,
  desktopAppId,
  JONES_MACOS_APP_ID,
  JONES_MACOS_TEAM,
  nativePasskeysEnabled,
  productionSigning,
  requireProductionBuild,
  requireProductionPasskeys,
  signingRequirement,
} from "./policy.ts";
import { buildEnvironment, requireTrustedSigningContext } from "./production-build.ts";

const configured = {
  JONES_MACOS_RELEASE_MODE: "production",
  JONES_MACOS_PRODUCTION_ID: JONES_MACOS_APP_ID,
  JONES_MACOS_NATIVE_PASSKEYS: "enabled",
  T3CODE_APPLE_TEAM_ID: JONES_MACOS_TEAM,
  T3CODE_MACOS_PROVISIONING_PROFILE: "/synthetic/profile",
  T3CODE_CLERK_PUBLISHABLE_KEY: "synthetic-public-key",
  T3CODE_CLERK_PASSKEY_RP_DOMAINS: "auth.example.test",
  CSC_KEYCHAIN: "/synthetic/keychain",
};

describe("production signing admission", () => {
  it("keeps development identity and requires explicit production activation", () => {
    expect(desktopAppId({}, "com.t3tools.t3code")).toBe("com.t3tools.t3code");
    expect(desktopAppId(configured, "com.t3tools.t3code")).toBe(JONES_MACOS_APP_ID);
    expect(() => productionSigning({ JONES_MACOS_RELEASE_MODE: "production" })).toThrow(
      "owner-approved",
    );
    expect(() => productionSigning({ JONES_MACOS_RELEASE_MODE: "typo" })).toThrow("Unknown");
  });
  it("fails closed on unsigned, companion, wrong team or absent passkey configuration", () => {
    expect(() =>
      requireProductionBuild(configured, { platform: "mac", signed: true }),
    ).not.toThrow();
    for (const options of [
      { platform: "mac", signed: false },
      { platform: "linux", signed: true },
      { platform: "mac", signed: true, variant: "preview-companion" },
    ]) {
      expect(() => requireProductionBuild(configured, options)).toThrow();
    }
    for (const key of [
      "T3CODE_APPLE_TEAM_ID",
      "T3CODE_MACOS_PROVISIONING_PROFILE",
      "CSC_KEYCHAIN",
    ]) {
      expect(() =>
        requireProductionBuild({ ...configured, [key]: "" }, { platform: "mac", signed: true }),
      ).toThrow();
    }
    expect(() =>
      requireProductionBuild(
        { ...configured, T3CODE_APPLE_TEAM_ID: "WRONGTEAM1" },
        { platform: "mac", signed: true },
      ),
    ).toThrow();
  });
  it("signs the baseline without optional native-browser passkey configuration", () => {
    const baseline = {
      JONES_MACOS_RELEASE_MODE: "production",
      JONES_MACOS_PRODUCTION_ID: JONES_MACOS_APP_ID,
      T3CODE_APPLE_TEAM_ID: JONES_MACOS_TEAM,
      CSC_KEYCHAIN: "/synthetic/keychain",
    };
    expect(() => requireProductionBuild(baseline, { platform: "mac", signed: true })).not.toThrow();
    expect(nativePasskeysEnabled(baseline)).toBe(false);
    expect(() =>
      nativePasskeysEnabled({ ...baseline, T3CODE_MACOS_PROVISIONING_PROFILE: "/profile" }),
    ).toThrow("opt-in");
    expect(() =>
      nativePasskeysEnabled({ ...baseline, JONES_MACOS_NATIVE_PASSKEYS: "enabled" }),
    ).toThrow("require");
  });
  it("does not silently discard an enabled passkey capability", () => {
    expect(() =>
      requireProductionPasskeys(configured, {
        touchIdKeychainAccessGroup: `${JONES_MACOS_TEAM}.${JONES_MACOS_APP_ID}.webauthn`,
        browserPasskeys: true,
      }),
    ).not.toThrow();
    expect(() =>
      requireProductionPasskeys(configured, {
        touchIdKeychainAccessGroup: undefined,
        browserPasskeys: true,
      }),
    ).toThrow();
    expect(() =>
      requireProductionPasskeys(configured, {
        touchIdKeychainAccessGroup: "wrong.group",
        browserPasskeys: false,
      }),
    ).toThrow();
  });
  it("keeps component identities across staging paths and dependency versions", () => {
    expect(componentIdentifier("/one/Jones.app", "/one/Jones.app")).toBe(JONES_MACOS_APP_ID);
    expect(componentIdentifier("/one/Jones.app", "/one/Jones.app/Contents/MacOS/Jones")).toBe(
      JONES_MACOS_APP_ID,
    );
    const native = (version: string) =>
      `Contents/Resources/app.asar.unpacked/node_modules/.pnpm/native@${version}/node_modules/native/addon.node`;
    expect(componentIdentifier("/one/Jones.app", `/one/Jones.app/${native("1.0")}`)).toBe(
      componentIdentifier("/two/Jones.app", `/two/Jones.app/${native("2.0")}`),
    );
    expect(
      componentIdentifier("/one/Jones.app", "/one/Jones.app/Contents/Frameworks/GPU.app"),
    ).not.toBe(JONES_MACOS_APP_ID);
    expect(
      componentIdentifier("/one/Jones.app", "/one/Jones.app/Contents/Frameworks/GPU.app"),
    ).not.toBe(
      componentIdentifier("/one/Jones.app", "/one/Jones.app/Contents/Frameworks/Renderer.app"),
    );
    expect(() => componentIdentifier("/one/Jones.app", "/other/code")).toThrow("escapes");
    expect(signingRequirement(JONES_MACOS_APP_ID)).toContain(`subject.OU] = "${JONES_MACOS_TEAM}"`);
    expect(signingRequirement(JONES_MACOS_APP_ID)).not.toContain("cdhash");
  });
  it("removes private material from builder descendants", () => {
    const result = buildEnvironment({
      MACOS_SIGNING_CERTIFICATE_P12_BASE64: "synthetic",
      MACOS_SIGNING_CERTIFICATE_PASSWORD: "synthetic",
      APPLE_NOTARY_API_KEY_P8_BASE64: "synthetic",
      CSC_KEY_PASSWORD: "synthetic",
      PUBLIC: "kept",
    });
    expect(result).toEqual({ PUBLIC: "kept" });
  });
  it("rejects PR, fork and branch signing contexts before credential handling", () => {
    for (const env of [
      {
        GITHUB_REPOSITORY: "other/fork",
        GITHUB_REF: "refs/heads/main",
        GITHUB_EVENT_NAME: "workflow_dispatch",
      },
      {
        GITHUB_REPOSITORY: "Jones-Systems/Jones-Code",
        GITHUB_REF: "refs/pull/1/merge",
        GITHUB_EVENT_NAME: "pull_request",
      },
      {
        GITHUB_REPOSITORY: "Jones-Systems/Jones-Code",
        GITHUB_REF: "refs/heads/topic",
        GITHUB_EVENT_NAME: "workflow_dispatch",
      },
    ])
      expect(() =>
        requireTrustedSigningContext({ ...configured, GITHUB_SHA: "a".repeat(40), ...env }),
      ).toThrow("default-branch");
  });
});
