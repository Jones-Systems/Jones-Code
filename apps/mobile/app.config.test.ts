import { DEFAULT_HOSTED_APP_URL } from "@t3tools/shared/connectAuth";
import { afterEach, expect, it, vi } from "vite-plus/test";

import appleAppSiteAssociation from "../web/public/.well-known/apple-app-site-association.json";
import assetLinks from "../web/public/.well-known/assetlinks.json";
import easConfig from "./eas.json";

// Keep config parsing independent of local env files and build credentials.
const { loadRepoEnv } = vi.hoisted(() => ({
  loadRepoEnv: vi.fn<() => Record<string, string | undefined>>(),
}));
vi.mock("../../scripts/lib/public-config.ts", () => ({ loadRepoEnv }));

const TESTFLIGHT_ENV = {
  JONES_TESTFLIGHT_EXPO_OWNER: "example-expo-owner",
  JONES_TESTFLIGHT_EAS_PROJECT_ID: "b15a369e-8eea-40d4-b83d-4679bcc7e090",
  JONES_TESTFLIGHT_IOS_BUNDLE_ID: "com.example.jonescode",
  JONES_TESTFLIGHT_APPLE_TEAM_ID: "A1B2C3D4E5",
};

afterEach(() => {
  vi.unstubAllEnvs();
});

async function loadConfig(
  variant: string | undefined,
  overrides: Record<string, string | undefined> = {},
) {
  vi.resetModules();
  const env = Object.fromEntries(
    Object.entries({ ...overrides, APP_VARIANT: variant }).filter(
      ([, value]) => value !== undefined,
    ),
  );
  for (const [key, value] of Object.entries(env)) vi.stubEnv(key, value);
  vi.stubEnv("EXPO_NO_TYPESCRIPT_SETUP", undefined);
  loadRepoEnv.mockReturnValue(env);
  return (await import("./app.config.ts")).default;
}

it.each(["prod", ""])("rejects APP_VARIANT=%j before selecting production IDs", async (value) => {
  await expect(loadConfig(value)).rejects.toThrow(
    "APP_VARIANT must be development, preview, production, or jones-testflight.",
  );
});

it.each([
  [undefined, "com.t3tools.t3code"],
  ["development", "com.t3tools.t3code.dev"],
  ["preview", "com.t3tools.t3code.preview"],
  ["production", "com.t3tools.t3code"],
])("keeps the iOS and Android identifiers for APP_VARIANT=%j", async (value, identifier) => {
  const config = await loadConfig(value, TESTFLIGHT_ENV);
  expect(config.ios?.bundleIdentifier).toBe(identifier);
  expect(config.android?.package).toBe(identifier);
  expect(config.owner).toBe("pingdotgg");
  expect(config.updates?.url).toBe("https://u.expo.dev/d763fcb8-d37c-41ea-a773-b54a0ab4a454");
  // Copied thread links open this app only while app.t3.codes lists it.
  const host = new URL(DEFAULT_HOSTED_APP_URL).host;
  expect(config.ios?.associatedDomains).toContain(`applinks:${host}`);
  expect(appleAppSiteAssociation.applinks.details[0]?.appIDs).toContain(
    `${config.ios?.appleTeamId}.${identifier}`,
  );
  expect(config.android?.intentFilters?.[0]?.data).toEqual([
    { scheme: "https", host, pathPrefix: "/threads/" },
  ]);
  expect(assetLinks.map((link) => link.target.package_name)).toContain(identifier);
});

it("resolves an independent iOS store build from the Jones TestFlight profile", async () => {
  const profile = easConfig.build["jones-testflight"];
  expect(profile).toMatchObject({ distribution: "store", credentialsSource: "remote" });
  expect(profile).not.toHaveProperty("extends");
  expect(profile).not.toHaveProperty("channel");
  expect(easConfig.submit).not.toHaveProperty("jones-testflight");

  const config = await loadConfig(profile.env.APP_VARIANT, {
    ...TESTFLIGHT_ENV,
    EAS_BUILD_PROFILE: "jones-testflight",
    EAS_BUILD_PROJECT_ID: TESTFLIGHT_ENV.JONES_TESTFLIGHT_EAS_PROJECT_ID,
    T3CODE_MOBILE_UPDATES_ENABLED: "1",
  });
  const bundleId = TESTFLIGHT_ENV.JONES_TESTFLIGHT_IOS_BUNDLE_ID;
  expect(config).toMatchObject({
    name: "Jones Code TestFlight",
    slug: "jones-code-testflight",
    scheme: "jonescode-testflight",
    owner: TESTFLIGHT_ENV.JONES_TESTFLIGHT_EXPO_OWNER,
    platforms: ["ios"],
    ios: {
      bundleIdentifier: bundleId,
      appleTeamId: TESTFLIGHT_ENV.JONES_TESTFLIGHT_APPLE_TEAM_ID,
      entitlements: {
        "keychain-access-groups": [`$(AppIdentifierPrefix)${bundleId}`],
        "aps-environment": "production",
      },
      infoPlist: {
        NSAppTransportSecurity: { NSAllowsArbitraryLoads: true },
        NSLocalNetworkUsageDescription: expect.stringContaining("local network or tailnet"),
      },
    },
    extra: { eas: { projectId: TESTFLIGHT_ENV.JONES_TESTFLIGHT_EAS_PROJECT_ID } },
  });
  expect(config.android).toBeUndefined();
  expect(config.ios?.associatedDomains).toBeUndefined();
  expect(config.updates).toEqual({ enabled: false });
  expect(config.plugins).toContainEqual([
    "expo-widgets",
    expect.objectContaining({
      bundleIdentifier: `${bundleId}.widgets`,
      groupIdentifier: `group.${bundleId}`,
    }),
  ]);
  expect(config.plugins).toContainEqual([
    "expo-sharing",
    expect.objectContaining({
      ios: expect.objectContaining({
        enabled: true,
        extensionBundleIdentifier: `${bundleId}.sharing`,
        appGroupId: `group.${bundleId}`,
      }),
    }),
  ]);
});

it.each(Object.keys(TESTFLIGHT_ENV))("requires %s for TestFlight", async (key) => {
  await expect(
    loadConfig("jones-testflight", { ...TESTFLIGHT_ENV, [key]: undefined }),
  ).rejects.toThrow(key);
});

it.each([
  ["JONES_TESTFLIGHT_EXPO_OWNER", " "],
  ["JONES_TESTFLIGHT_EAS_PROJECT_ID", "not-a-uuid"],
  ["JONES_TESTFLIGHT_IOS_BUNDLE_ID", "com.example.*"],
  ["JONES_TESTFLIGHT_APPLE_TEAM_ID", "short"],
])("rejects malformed TestFlight identity in %s", async (key, value) => {
  await expect(loadConfig("jones-testflight", { ...TESTFLIGHT_ENV, [key]: value })).rejects.toThrow(
    key,
  );
});

it.each([
  ["JONES_TESTFLIGHT_EXPO_OWNER", "PingDotGG"],
  ["JONES_TESTFLIGHT_EAS_PROJECT_ID", "D763FCB8-D37C-41EA-A773-B54A0AB4A454"],
  ["JONES_TESTFLIGHT_IOS_BUNDLE_ID", "com.T3Tools.t3code.preview"],
  ["JONES_TESTFLIGHT_APPLE_TEAM_ID", "ARK85ZXQ4Z"],
])("rejects upstream TestFlight identity in %s", async (key, value) => {
  await expect(loadConfig("jones-testflight", { ...TESTFLIGHT_ENV, [key]: value })).rejects.toThrow(
    "jones-testflight requires Jones-owned Expo and Apple identifiers.",
  );
});

it("blocks Android at profile resolution and rejects Personal Team signing", async () => {
  await expect(
    loadConfig("jones-testflight", {
      ...TESTFLIGHT_ENV,
      ...easConfig.build["jones-testflight"].android.env,
    }),
  ).rejects.toThrow("The jones-testflight profile only supports iOS.");
  await expect(
    loadConfig("jones-testflight", { ...TESTFLIGHT_ENV, T3CODE_IOS_PERSONAL_TEAM: "1" }),
  ).rejects.toThrow("jones-testflight requires an Apple Developer Program team for distribution.");
});

it("rejects a mismatched EAS job identity", async () => {
  await expect(loadConfig(undefined, { EAS_BUILD_PROFILE: "jones-testflight" })).rejects.toThrow(
    "The jones-testflight build requires APP_VARIANT=jones-testflight.",
  );
  await expect(
    loadConfig("jones-testflight", { ...TESTFLIGHT_ENV, EAS_BUILD_PROJECT_ID: "another-project" }),
  ).rejects.toThrow("EAS_BUILD_PROJECT_ID does not match JONES_TESTFLIGHT_EAS_PROJECT_ID.");
});

it("uses only the configured Clerk domain for TestFlight associated domains", async () => {
  const config = await loadConfig("jones-testflight", {
    ...TESTFLIGHT_ENV,
    JONES_TESTFLIGHT_CLERK_DOMAIN: "accounts.example.com",
  });
  expect(config.ios?.associatedDomains).toEqual([
    "applinks:accounts.example.com",
    "webcredentials:accounts.example.com",
  ]);
  expect(config.plugins).toContainEqual([
    "@clerk/expo",
    expect.objectContaining({ appleSignIn: true }),
  ]);
});
