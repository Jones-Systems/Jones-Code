import { describe, expect, it } from "vite-plus/test";
import {
  buildArgv,
  buildFailureCode,
  productPaths,
  selectRuntime,
  selectXcode,
} from "./ios-native-build.ts";

describe("installed native tooling", () => {
  const xcode = (version: string) => ({
    version,
    developerDir: `/Applications/Xcode_${version}.app/Contents/Developer`,
    description: `Xcode ${version}`,
  });
  it("selects the highest actual Xcode at or above 26.4", () => {
    expect(selectXcode([xcode("26.4.1"), xcode("26.6"), xcode("26.2")]).version).toBe("26.6");
    expect(selectXcode([xcode("26.6"), xcode("27.0")]).version).toBe("27.0");
    expect(() => selectXcode([xcode("26.2")])).toThrow("hosted-xcode-below-floor");
  });
  it("requires an available iOS runtime within the selected SDK and iPhone 17", () => {
    const runtime = (version: string, isAvailable = true) => ({
      identifier: `com.apple.CoreSimulator.SimRuntime.iOS-${version}`,
      version,
      isAvailable,
    });
    const device = [
      { name: "iPhone 17", identifier: "com.apple.CoreSimulator.SimDeviceType.iPhone-17" },
    ];
    expect(
      selectRuntime(
        [runtime("26.2"), runtime("26.5"), runtime("27.0"), runtime("26.6", false)],
        device,
        "26.6",
      ).runtimeVersion,
    ).toBe("26.5");
    expect(() => selectRuntime([runtime("27.0")], device, "26.6")).toThrow(
      "hosted-runtime-unavailable",
    );
    expect(() => selectRuntime([runtime("26.5")], [], "26.6")).toThrow(
      "hosted-device-type-unavailable",
    );
  });
});

describe("captured build handoff", () => {
  it("preserves disk exhaustion through combined build and cleanup failures", () => {
    expect(
      buildFailureCode(
        new AggregateError([
          new Error("compile failed"),
          Object.assign(new Error("write failed"), { code: "ENOSPC" }),
        ]),
      ),
    ).toBe("hosted-disk-exhausted");
    expect(buildFailureCode(new Error("hosted-xcode-below-floor"))).toBe(
      "hosted-xcode-below-floor",
    );
    expect(buildFailureCode(new Error("unrelated failure"))).toBe("other");
  });
  it("uses Release simulator build-for-testing and owned products without provisioning", () => {
    const argv = buildArgv("/checkout/apps/mobile/ios/App.xcworkspace", "/job/build");
    expect(argv).toContain("build-for-testing");
    expect(argv[argv.indexOf("-scheme") + 1]).toBe("JonesE2eUITests");
    expect(argv[argv.indexOf("-configuration") + 1]).toBe("Release");
    expect(argv[argv.indexOf("-derivedDataPath") + 1]).toBe("/job/build/DerivedData");
    expect(argv).toContain("CODE_SIGNING_ALLOWED=NO");
    expect(argv.some((item) => item.startsWith("-allowProvisioning"))).toBe(false);
  });
  const target = () => ({
    BlueprintName: "JonesE2eUITests",
    IsUITestBundle: true,
    TestHostPath: "__TESTROOT__/Release/JonesE2eUITests-Runner.app",
    TestBundlePath: "__TESTHOST__/PlugIns/JonesE2eUITests.xctest",
    UITargetAppPath: "__TESTROOT__/Release/App.app",
  });
  it("uses actual xctestrun paths and rejects escaped or ambiguous targets", () => {
    expect(
      productPaths({ TestConfigurations: [{ TestTargets: [target()] }] }, "/build/Products"),
    ).toEqual({
      app: "/build/Products/Release/App.app",
      testHost: "/build/Products/Release/JonesE2eUITests-Runner.app",
      testBundle:
        "/build/Products/Release/JonesE2eUITests-Runner.app/PlugIns/JonesE2eUITests.xctest",
    });
    expect(() =>
      productPaths(
        { JonesE2eUITests: { ...target(), UITargetAppPath: "__TESTROOT__/../foreign.app" } },
        "/build/Products",
      ),
    ).toThrow("Escaping");
    expect(() =>
      productPaths(
        { TestConfigurations: [{ TestTargets: [target(), target()] }] },
        "/build/Products",
      ),
    ).toThrow("one test target");
  });
});
