import { expect, it } from "vite-plus/test";
import { productionSignOptions } from "./sign.ts";
import { JONES_MACOS_APP_ID, JONES_MACOS_IDENTITY } from "./policy.ts";

it("preserves existing entitlements and pins each code object to its own stable identity", () => {
  const app = "/synthetic/Jones.app";
  const options = productionSignOptions({ app, provisioningProfile: "/synthetic/profile", optionsForFile: () => ({ entitlements: "/synthetic/entitlements.plist", hardenedRuntime: false, timestamp: "none" }) }, { CSC_KEYCHAIN: "/synthetic/keychain" });
  expect(options.identity).toBe(JONES_MACOS_IDENTITY);
  expect(options.strictVerify).toBe(true);
  const main = options.optionsForFile!(app, { platform: "darwin" });
  expect(main.entitlements).toBe("/synthetic/entitlements.plist");
  expect(main.hardenedRuntime).toBe(true);
  expect(main.timestamp).not.toBe("none");
  expect(main.additionalArguments).toContain(JONES_MACOS_APP_ID);
  expect(options.optionsForFile!(`${app}/Contents/Frameworks/GPU.app`, { platform: "darwin" }).additionalArguments).not.toEqual(main.additionalArguments);
});
