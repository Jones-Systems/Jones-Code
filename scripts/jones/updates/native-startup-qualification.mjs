import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { extractFile } from "@electron/asar";
import {
  assertStartupRefused,
  cleanLaunchEnvironment,
  createNativeState,
  FixtureOwnership,
  initializeFixtureLeases,
  leasePaths,
  packagedStartupNames,
  runOwnedChild,
  snapshotTree,
  withExclusiveLeases,
} from "./native-startup-fixture.mjs";
import { observerControl, protectedMutations, startObserver } from "./native-startup-observer.mjs";

// This gate is deliberately unavailable on developer hosts. Cocoa default paths
// are not safely redirected by assuming HOME or CFFIXED_USER_HOME semantics.
assert.equal(process.platform, "darwin");
assert.equal(process.env.GITHUB_ACTIONS, "true");
assert.equal(process.env.RUNNER_ENVIRONMENT, "github-hosted");
const args = process.argv.slice(2);
assert.equal(args.length, 4, "Usage: --dmg <artifact.dmg> --evidence <result.json>");
assert.equal(args[0], "--dmg");
assert.equal(args[2], "--evidence");
const dmg = await fs.realpath(args[1]);
const evidencePath = path.resolve(args[3]);
const scratch = new FixtureOwnership();
const ownership = new FixtureOwnership();
const temporary = await scratch.temporary(path.join(os.tmpdir(), "jones-packaged-startup-"));
const root = await fs.realpath(temporary);
const mount = path.join(root, "mount");
const controller = new AbortController();
const cancel = () => controller.abort();
process.on("SIGINT", cancel);
process.on("SIGTERM", cancel);
const evidence = {
  schema: 1,
  qualification: "packaged-mac-normal-startup-refusal",
  outcome: "unknown",
  scenarios: [],
  limitations: [
    "Only the normal direct executable launch and sanitized environment are qualified.",
    "Explicit Chromium user-data-dir/logging flags and logging environment overrides are outside this result.",
    "Gatekeeper, signing, notarization, and an admitted application launch are not qualified.",
  ],
};
let mounted = false;
let observer;
let failure;
const recordCleanupFailure = (error) => {
  failure ??= error;
  evidence.outcome = "failed";
  (evidence.cleanupErrors ??= []).push(String(error).slice(0, 1024));
};
const persist = async () => {
  try {
    await fs.mkdir(path.dirname(evidencePath), { recursive: true });
    await fs.writeFile(evidencePath, JSON.stringify(evidence, null, 2) + "\n");
  } catch (error) {
    recordCleanupFailure(error);
  }
};
const command = async (executable, argv, timeout = 15000) => {
  const result = await runOwnedChild(executable, argv, { signal: controller.signal, timeout });
  assert.equal(result.code, 0, result.stderr);
  return result.stdout;
};
const requireAbsent = async (filename) => {
  await assert.rejects(
    fs.lstat(filename),
    { code: "ENOENT" },
    `Refusing to inspect or replace pre-existing runner state: ${filename}`,
  );
};
try {
  const executable = path.join(root, "observer");
  await command(
    "xcrun",
    [
      "clang",
      "-fobjc-arc",
      "-fblocks",
      "-Wall",
      "-Werror",
      "-framework",
      "Foundation",
      "-framework",
      "CoreServices",
      fileURLToPath(new URL("./native-startup-observer.m", import.meta.url)),
      "-o",
      executable,
    ],
    30000,
  );
  const native = JSON.parse(await command(executable, ["--paths"]));
  assert.equal(native.uid, process.getuid());
  assert.equal(native.home, native.accountHome);
  assert.equal(native.home, await fs.realpath(os.homedir()));
  assert.equal(native.home, await fs.realpath(process.env.HOME));
  assert.equal(native.applicationSupport, path.join(native.home, "Library", "Application Support"));
  assert.equal(native.caches, path.join(native.home, "Library", "Caches"));
  for (const directory of [native.applicationSupport, native.caches])
    assert.ok((await fs.stat(directory)).isDirectory());
  await fs.mkdir(mount);
  // Once attempted, detach this exact mountpoint even if attach's response is
  // lost. No device-wide detach or unrelated mounted-image discovery is used.
  mounted = true;
  await command("hdiutil", ["attach", "-readonly", "-nobrowse", "-mountpoint", mount, dmg], 30000);
  const applications = (await fs.readdir(mount)).filter((name) => name.endsWith(".app"));
  assert.equal(applications.length, 1);
  const app = path.join(mount, applications[0]);
  const metadata = JSON.parse(
    extractFile(path.join(app, "Contents", "Resources", "app.asar"), "package.json").toString(
      "utf8",
    ),
  );
  assert.equal(metadata.jonesSource.repository, "Jones-Systems/Jones-Code");
  assert.equal(metadata.jonesSource.sha, process.env.GITHUB_SHA);
  assert.match(metadata.jonesSource.tree, /^[a-f0-9]{40}$/);
  const pinnedElectron = JSON.parse(
    await fs.readFile(new URL("../../../apps/desktop/package.json", import.meta.url), "utf8"),
  ).dependencies.electron;
  const packagedElectron = (
    await command("/usr/libexec/PlistBuddy", [
      "-c",
      "Print :CFBundleVersion",
      path.join(
        app,
        "Contents",
        "Frameworks",
        "Electron Framework.framework",
        "Resources",
        "Info.plist",
      ),
    ])
  ).trim();
  assert.equal(packagedElectron, pinnedElectron);
  const executableName = (
    await command("/usr/libexec/PlistBuddy", [
      "-c",
      "Print :CFBundleExecutable",
      path.join(app, "Contents", "Info.plist"),
    ])
  ).trim();
  const bundleIdentifier = (
    await command("/usr/libexec/PlistBuddy", [
      "-c",
      "Print :CFBundleIdentifier",
      path.join(app, "Contents", "Info.plist"),
    ])
  ).trim();
  const bundleName = (
    await command("/usr/libexec/PlistBuddy", [
      "-c",
      "Print :CFBundleName",
      path.join(app, "Contents", "Info.plist"),
    ])
  ).trim();
  assert.equal(path.basename(executableName), executableName);
  const binary = path.join(app, "Contents", "MacOS", executableName);
  const home = path.join(native.home, ".t3");
  const profile = path.join(native.applicationSupport, "t3code-v2");
  const names = packagedStartupNames(metadata, { executableName, bundleIdentifier, bundleName });
  const protectedPaths = [
    ...new Set([
      home,
      profile,
      ...[...names].flatMap((name) => [
        path.join(native.applicationSupport, name),
        path.join(native.caches, name),
      ]),
    ]),
  ];
  for (const filename of protectedPaths) await requireAbsent(filename);
  const active = await createNativeState(home, profile, metadata, ownership);
  for (const filename of leasePaths(active).filter((name) => !name.startsWith(home + path.sep))) {
    await requireAbsent(filename);
    await requireAbsent(`${filename}.identity.json`);
  }
  await initializeFixtureLeases(active, ownership);
  const before = await Promise.all(protectedPaths.map(snapshotTree));
  const watchRoots = [native.home, native.applicationSupport, native.caches];
  observer = await startObserver(executable, watchRoots, controller.signal);
  await observerControl(observer, watchRoots);
  const start = await observer.flush();
  const env = cleanLaunchEnvironment(process.env, root);
  // No user-data-dir switch and no T3 path overrides: this is the packaged
  // executable's ordinary native default routing on this disposable account.
  const launch = () => runOwnedChild(binary, [], { env, signal: controller.signal });
  const unresolved = await launch();
  assertStartupRefused(unresolved, /Unstarted native activation holds new writers/);
  evidence.scenarios.push({
    name: "unresolved-activation",
    exitCode: unresolved.code,
    diagnostic: unresolved.stderr.slice(-1024),
  });
  await withExclusiveLeases(leasePaths(active), async () => {
    const contention = await launch();
    assertStartupRefused(contention, /locked/);
    evidence.scenarios.push({
      name: "exclusive-contention",
      exitCode: contention.code,
      diagnostic: contention.stderr.slice(-1024),
    });
  });
  await withExclusiveLeases(leasePaths(active), async () => {});
  await observer.flush();
  await observerControl(observer, watchRoots);
  await observer.flush();
  const mutations = protectedMutations(observer.events.slice(start), protectedPaths);
  evidence.protectedMutations = mutations.slice(0, 30);
  assert.deepEqual(
    mutations,
    [],
    "The packaged process changed protected state, including a transient change.",
  );
  assert.deepEqual(await Promise.all(protectedPaths.map(snapshotTree)), before);
  evidence.source = metadata.jonesSource;
  evidence.version = metadata.version;
  evidence.electron = packagedElectron;
  evidence.leasesReleased = true;
  evidence.observer = {
    positiveControls: "before-and-after",
    historyLoss: false,
    eventCount: observer.events.length,
  };
  evidence.protectedStateUnchanged = true;
  evidence.outcome = "passed";
} catch (error) {
  failure = error;
  evidence.outcome = "failed";
  evidence.error = String(error).slice(0, 2048);
} finally {
  if (observer) {
    try {
      await observer.close();
    } catch (error) {
      recordCleanupFailure(error);
    }
  }
  // Persist bounded evidence before removing any fixture state, including on a
  // failed qualification. Cleanup failures also make the job fail.
  await persist();
  try {
    await ownership.cleanup();
  } catch (error) {
    recordCleanupFailure(error);
  }
  if (mounted) {
    try {
      const detached = await runOwnedChild("hdiutil", ["detach", mount], { timeout: 30000 });
      assert.equal(detached.code, 0, detached.stderr);
      mounted = false;
    } catch (error) {
      recordCleanupFailure(error);
    }
  }
  if (failure) evidence.outcome = "failed";
  if (!mounted) {
    try {
      await scratch.cleanup();
    } catch (error) {
      recordCleanupFailure(error);
    }
  }
  await persist();
  process.removeListener("SIGINT", cancel);
  process.removeListener("SIGTERM", cancel);
}
if (failure) throw failure;
process.stdout.write(
  JSON.stringify({
    qualification: evidence.qualification,
    outcome: evidence.outcome,
    evidence: evidencePath,
  }) + "\n",
);
