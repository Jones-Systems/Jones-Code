"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { withXcodeProject } = require("expo/config-plugins");

const TARGET = "JonesE2eUITests";
const unquote = (value) => String(value).replace(/^"|"$/g, "");
const xml = (value) =>
  String(value)
    .replaceAll("&", "&amp;")
    .replaceAll('"', "&quot;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");

module.exports = function withJonesE2eUITests(config) {
  assert.equal(process.env.JONES_IOS_E2E, "1", "UI tests require explicit opt-in");
  assert.equal(
    process.env.T3CODE_MOBILE_UPDATES_ENABLED,
    "0",
    "UI tests require embedded Release JS",
  );
  return withXcodeProject(config, (cfg) => {
    const project = cfg.modResults;
    const targets = project.pbxNativeTargetSection();
    assert.ok(
      !Object.values(targets).some(
        (target) => typeof target === "object" && unquote(target.name) === TARGET,
      ),
      "E2E target already exists; regenerate owned ios project",
    );
    const applications = Object.entries(targets).filter(
      ([, target]) =>
        typeof target === "object" &&
        unquote(target.productType) === "com.apple.product-type.application",
    );
    assert.equal(applications.length, 1, "Expected one iOS application");
    const [appId, app] = applications[0];
    assert.equal(
      project.getFirstTarget().uuid,
      appId,
      "Application must be the first generated target",
    );
    const appName = unquote(app.name);
    const originalDependencies = [...app.dependencies];
    // node-xcode adds dependencies only when these optional sections exist.
    project.hash.project.objects.PBXTargetDependency ??= {};
    project.hash.project.objects.PBXContainerItemProxy ??= {};
    // node-xcode has no UI-test factory. Its unit-test product uses the same
    // bundle extension; specialize the product type and dependency direction.
    const added = project.addTarget(
      TARGET,
      "unit_test_bundle",
      TARGET,
      `${cfg.ios.bundleIdentifier}.e2e`,
    );
    const dependencySection = project.hash.project.objects.PBXTargetDependency;
    const proxySection = project.hash.project.objects.PBXContainerItemProxy;
    for (const dependency of app.dependencies) {
      if (originalDependencies.some((old) => old.value === dependency.value)) continue;
      const proxy = dependencySection[dependency.value]?.targetProxy;
      delete dependencySection[dependency.value];
      delete dependencySection[`${dependency.value}_comment`];
      if (proxy) {
        delete proxySection[proxy];
        delete proxySection[`${proxy}_comment`];
      }
    }
    app.dependencies = originalDependencies;
    added.pbxNativeTarget.productType = '"com.apple.product-type.bundle.ui-testing"';
    project.addTargetDependency(added.uuid, [appId]);
    project.addTargetAttribute("TestTargetID", appId, added);
    const configurations =
      project.pbxXCConfigurationList()[added.pbxNativeTarget.buildConfigurationList]
        .buildConfigurations;
    for (const entry of configurations) {
      const settings = project.pbxXCBuildConfigurationSection()[entry.value].buildSettings;
      Object.assign(settings, {
        PRODUCT_BUNDLE_IDENTIFIER: `"${cfg.ios.bundleIdentifier}.e2e"`,
        GENERATE_INFOPLIST_FILE: "YES",
        SWIFT_VERSION: "5.0",
        SDKROOT: "iphoneos",
        SUPPORTED_PLATFORMS: '"iphonesimulator"',
        IPHONEOS_DEPLOYMENT_TARGET: "18.0",
        TARGETED_DEVICE_FAMILY: '"1,2"',
        CODE_SIGNING_ALLOWED: "NO",
        TEST_TARGET_NAME: `"${appName}"`,
        ENABLE_TESTING_SEARCH_PATHS: "YES",
      });
      settings.INFOPLIST_FILE = `"${TARGET}/Info.plist"`;
    }
    project.addBuildPhase(
      [`${TARGET}/PinWorkflowTests.swift`],
      "PBXSourcesBuildPhase",
      "Sources",
      added.uuid,
    );
    project.addBuildPhase([], "PBXFrameworksBuildPhase", "Frameworks", added.uuid);
    project.addBuildPhase([], "PBXResourcesBuildPhase", "Resources", added.uuid);
    const destination = path.join(cfg.modRequest.platformProjectRoot, TARGET);
    fs.mkdirSync(destination, { recursive: true });
    fs.copyFileSync(
      path.join(cfg.modRequest.projectRoot, "e2e/xcuitest/PinWorkflowTests.swift"),
      path.join(destination, "PinWorkflowTests.swift"),
    );
    fs.writeFileSync(
      path.join(destination, "Info.plist"),
      `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict><key>NSAppTransportSecurity</key><dict><key>NSAllowsArbitraryLoads</key><true/></dict></dict></plist>\n`,
    );
    const projectDirectory = path.dirname(project.filepath);
    assert.equal(path.dirname(projectDirectory), cfg.modRequest.platformProjectRoot);
    const schemeDirectory = path.join(projectDirectory, "xcshareddata/xcschemes");
    fs.mkdirSync(schemeDirectory, { recursive: true });
    const reference = (id, name, product) =>
      `<BuildableReference BuildableIdentifier="primary" BlueprintIdentifier="${xml(id)}" BuildableName="${xml(product)}" BlueprintName="${xml(name)}" ReferencedContainer="container:${xml(path.basename(projectDirectory))}"/>`;
    fs.writeFileSync(
      path.join(schemeDirectory, `${TARGET}.xcscheme`),
      `<?xml version="1.0" encoding="UTF-8"?>
<Scheme LastUpgradeVersion="2640" version="1.3">
  <BuildAction parallelizeBuildables="NO" buildImplicitDependencies="YES"><BuildActionEntries>
    <BuildActionEntry buildForTesting="YES" buildForRunning="NO" buildForProfiling="NO" buildForArchiving="NO" buildForAnalyzing="NO">${reference(appId, appName, `${appName}.app`)}</BuildActionEntry>
    <BuildActionEntry buildForTesting="YES" buildForRunning="NO" buildForProfiling="NO" buildForArchiving="NO" buildForAnalyzing="NO">${reference(added.uuid, TARGET, `${TARGET}.xctest`)}</BuildActionEntry>
  </BuildActionEntries></BuildAction>
  <TestAction buildConfiguration="Release" selectedDebuggerIdentifier="" selectedLauncherIdentifier="Xcode.IDEFoundation.Launcher.PosixSpawn" shouldUseLaunchSchemeArgsEnv="NO" onlyGenerateCoverageForSpecifiedTargets="YES">
    <Testables><TestableReference skipped="NO" parallelizable="NO">${reference(added.uuid, TARGET, `${TARGET}.xctest`)}</TestableReference></Testables>
  </TestAction>
</Scheme>\n`,
    );
    return cfg;
  });
};
