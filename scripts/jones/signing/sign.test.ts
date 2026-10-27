import { afterEach, expect, it, vi } from "vite-plus/test";
import { productionSignOptions, signMac } from "./sign.ts";
import { JONES_MACOS_APP_ID, JONES_MACOS_IDENTITY, signingRequirement } from "./policy.ts";

it("preserves existing entitlements and pins each code object to its own stable identity", () => {
  const app = "/synthetic/Jones.app";
  const options = productionSignOptions(
    {
      app,
      provisioningProfile: "/synthetic/profile",
      optionsForFile: () => ({
        entitlements: "/synthetic/entitlements.plist",
        hardenedRuntime: false,
        timestamp: "none",
      }),
    },
    { CSC_KEYCHAIN: "/synthetic/keychain" },
  );
  expect(options.identity).toBe(JONES_MACOS_IDENTITY);
  expect(options.strictVerify).toBe(true);
  const main = options.optionsForFile!(app, { platform: "darwin" });
  expect(main.entitlements).toBe("/synthetic/entitlements.plist");
  expect(main.hardenedRuntime).toBe(true);
  expect(main.timestamp).not.toBe("none");
  expect(main.additionalArguments).toContain(JONES_MACOS_APP_ID);
  expect(
    options.optionsForFile!(`${app}/Contents/Frameworks/GPU.app`, { platform: "darwin" })
      .additionalArguments,
  ).not.toEqual(main.additionalArguments);
});

const nativeExec = vi.hoisted(() => vi.fn());
vi.mock("node:child_process", () => ({ execFile: nativeExec }));
vi.mock("@electron/osx-sign", () => ({ sign: vi.fn(async () => undefined) }));
afterEach(() => vi.unstubAllEnvs());

it("verifies the production requirement as literal source before notarizing", async () => {
  vi.stubEnv("JONES_MACOS_RELEASE_MODE", "production");
  vi.stubEnv("JONES_MACOS_PRODUCTION_ID", JONES_MACOS_APP_ID);
  vi.stubEnv("CSC_KEYCHAIN", "/synthetic/keychain");
  vi.stubEnv("JONES_NOTARY_KEY_FILE", "/synthetic/key.p8");
  vi.stubEnv("APPLE_NOTARY_KEY_ID", "synthetic-key");
  vi.stubEnv("APPLE_NOTARY_ISSUER_ID", "synthetic-issuer");
  nativeExec.mockImplementation((command, args, _options, callback) => {
    if (command === "/usr/bin/codesign" && !args[args.indexOf("-R") + 1].startsWith("=")) {
      callback(new Error("codesign would read this argument as a file"));
      return;
    }
    callback(null, "", "");
  });
  await signMac({ app: "/synthetic/Jones.app" });
  const verification = nativeExec.mock.calls.find(([command]) => command === "/usr/bin/codesign");
  expect(verification?.[1]).toContain(`=${signingRequirement(JONES_MACOS_APP_ID)}`);
});
