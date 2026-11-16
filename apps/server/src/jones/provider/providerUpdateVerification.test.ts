import { describe, expect, it } from "vite-plus/test";
import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";

import { resolveProviderUpdateVerification } from "./providerUpdateVerification.ts";

const installed: ServerProvider = {
  instanceId: ProviderInstanceId.make("codex"),
  driver: ProviderDriverKind.make("codex"),
  enabled: true,
  installed: true,
  version: "2.0.0",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-04-10T00:00:00.000Z",
  models: [],
  slashCommands: [],
  skills: [],
};
const current: ServerProvider = {
  ...installed,
  versionAdvisory: {
    status: "current",
    currentVersion: "2.0.0",
    latestVersion: "2.0.0",
    updateCommand: "npm install -g @openai/codex@latest",
    canUpdate: true,
    checkedAt: "2026-04-10T00:00:00.000Z",
    message: null,
  },
};

const unverified = {
  status: "unchanged",
  message: "Update command completed, but T3 Code could not verify the provider version.",
};

describe("resolveProviderUpdateVerification", () => {
  it("accepts a readable installed version with a fresh current advisory", () => {
    expect(
      resolveProviderUpdateVerification({ provider: current, advisoryRefreshed: true }),
    ).toEqual({ status: "succeeded", message: "Provider updated." });
  });

  it.each([
    ["missing instance", undefined],
    ["missing binary", { ...current, installed: false }],
    ["unreadable version", { ...current, version: null }],
    ["empty version", { ...current, version: " " }],
    ["missing advisory", installed],
    [
      "unavailable latest",
      {
        ...current,
        versionAdvisory: { ...current.versionAdvisory!, status: "unknown", latestVersion: null },
      },
    ],
  ] as const)("keeps %s unverified", (_name, provider) => {
    expect(resolveProviderUpdateVerification({ provider, advisoryRefreshed: true })).toEqual(
      unverified,
    );
  });

  it("does not trust a retained current advisory when recomputation failed", () => {
    expect(
      resolveProviderUpdateVerification({ provider: current, advisoryRefreshed: false }),
    ).toEqual(unverified);
  });

  it("reports a freshly observed outdated version", () => {
    expect(
      resolveProviderUpdateVerification({
        provider: {
          ...current,
          versionAdvisory: {
            ...current.versionAdvisory!,
            status: "behind_latest",
            latestVersion: "3.0.0",
          },
        },
        advisoryRefreshed: true,
      }),
    ).toEqual({
      status: "unchanged",
      message: "Update command completed, but T3 Code still detects an outdated provider version.",
    });
  });

  it("accepts a normalized exact target without registry evidence", () => {
    expect(
      resolveProviderUpdateVerification({
        provider: { ...installed, version: " v2.0.0 " },
        advisoryRefreshed: false,
        targetVersion: "2.0.0",
      }),
    ).toEqual({ status: "succeeded", message: "Provider updated." });
  });

  it.each([
    ["mismatched", { ...current, version: "2.0.1" }],
    ["unreadable", { ...current, version: null }],
    ["missing", { ...current, installed: false }],
  ] as const)("rejects a %s target installation", (_name, provider) => {
    expect(
      resolveProviderUpdateVerification({
        provider,
        advisoryRefreshed: true,
        targetVersion: "2.0.0",
      }),
    ).toEqual(unverified);
  });
});
