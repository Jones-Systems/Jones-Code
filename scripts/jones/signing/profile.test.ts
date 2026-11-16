import Plist from "plist";
import { expect, it } from "vite-plus/test";
import { requireProductionProfile } from "./profile.ts";
import { JONES_MACOS_APP_ID, JONES_MACOS_TEAM } from "./policy.ts";

const env = {
  JONES_MACOS_RELEASE_MODE: "production",
  JONES_MACOS_PRODUCTION_ID: JONES_MACOS_APP_ID,
  JONES_MACOS_NATIVE_PASSKEYS: "enabled",
  T3CODE_MACOS_PROVISIONING_PROFILE: "/synthetic/profile",
  T3CODE_CLERK_PASSKEY_RP_DOMAINS: "auth.example.test",
};
const fields = {
  "com.apple.application-identifier": `${JONES_MACOS_TEAM}.${JONES_MACOS_APP_ID}`,
  "com.apple.developer.team-identifier": JONES_MACOS_TEAM,
  "com.apple.developer.associated-domains": ["webcredentials:auth.example.test"],
};
it("accepts only the opted-in production profile's team, app and RP domains", () => {
  expect(() =>
    requireProductionProfile(env, Plist.build({ Entitlements: fields }), ["auth.example.test"]),
  ).not.toThrow();
  for (const key of Object.keys(fields)) {
    expect(() =>
      requireProductionProfile(env, Plist.build({ Entitlements: { ...fields, [key]: "wrong" } }), [
        "auth.example.test",
      ]),
    ).toThrow();
  }
  expect(() => requireProductionProfile(env, "malformed", ["auth.example.test"])).toThrow();
  expect(() =>
    requireProductionProfile(env, Plist.build({ Entitlements: fields }), [
      "unapproved.example.test",
    ]),
  ).toThrow();
});
