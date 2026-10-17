import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeSqlite from "node:sqlite";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { extractFile } from "@electron/asar";
import {
  initializeNativeStoreAuthority,
  readNativeStoreAuthorityState,
} from "../../../apps/server/src/environment/nativeStoreAuthorityPersistence.ts";
import {
  closeStartupObservation,
  createLegacyBootstrapFixture,
  qualifyLegacyBootstrap,
} from "./legacy-bootstrap-fixture.mjs";
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
import {
  ObserverControlError,
  observerControl,
  startObserver,
} from "./native-startup-observer.mjs";

// This gate is deliberately unavailable on developer hosts. Cocoa default paths
// are not safely redirected by assuming HOME or CFFIXED_USER_HOME semantics.
// oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone packaged-app qualifier must require the real native macOS host before touching fixture state.
NodeAssert.equal(process.platform, "darwin");
NodeAssert.equal(process.env.GITHUB_ACTIONS, "true");
NodeAssert.equal(process.env.RUNNER_ENVIRONMENT, "github-hosted");
const args = process.argv.slice(2);
NodeAssert.equal(args.length, 4, "Usage: --dmg <artifact.dmg> --evidence <result.json>");
NodeAssert.equal(args[0], "--dmg");
NodeAssert.equal(args[2], "--evidence");
const dmg = await NodeFSP.realpath(args[1]);
const evidencePath = NodePath.resolve(args[3]);
const scratch = new FixtureOwnership();
const ownership = new FixtureOwnership();
const temporary = await scratch.temporary(
  NodePath.join(NodeOS.tmpdir(), "jones-packaged-startup-"),
);
const root = await NodeFSP.realpath(temporary);
const mount = NodePath.join(root, "mount");
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
    "Normal direct executable startup and the explicit packaged native authority command use sanitized environments.",
    "Explicit Chromium user-data-dir/logging flags and logging environment overrides are outside this result.",
    "Gatekeeper, signing, notarization, and an admitted application launch are not qualified.",
  ],
};
let mounted = false;
let observer;
let bootstrapFixture;
let failure;
const recordCleanupFailure = (error) => {
  failure ??= error;
  evidence.outcome = "failed";
  (evidence.cleanupErrors ??= []).push(String(error).slice(0, 1024));
};
const persist = async () => {
  try {
    await NodeFSP.mkdir(NodePath.dirname(evidencePath), { recursive: true });
    await NodeFSP.writeFile(evidencePath, JSON.stringify(evidence, null, 2) + "\n");
  } catch (error) {
    recordCleanupFailure(error);
  }
};
const command = async (executable, argv, timeout = 15000) => {
  const result = await runOwnedChild(executable, argv, { signal: controller.signal, timeout });
  NodeAssert.equal(result.code, 0, result.stderr);
  return result.stdout;
};
const requireAbsent = async (filename) => {
  await NodeAssert.rejects(
    NodeFSP.lstat(filename),
    { code: "ENOENT" },
    `Refusing to inspect or replace pre-existing runner state: ${filename}`,
  );
};
try {
  const executable = NodePath.join(root, "observer");
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
      NodeURL.fileURLToPath(new URL("./native-startup-observer.m", import.meta.url)),
      "-o",
      executable,
    ],
    30000,
  );
  const native = JSON.parse(await command(executable, ["--paths"]));
  NodeAssert.equal(native.uid, process.getuid());
  NodeAssert.equal(native.home, native.accountHome);
  NodeAssert.equal(native.home, await NodeFSP.realpath(NodeOS.homedir()));
  NodeAssert.equal(native.home, await NodeFSP.realpath(process.env.HOME));
  NodeAssert.equal(
    native.applicationSupport,
    NodePath.join(native.home, "Library", "Application Support"),
  );
  NodeAssert.equal(native.caches, NodePath.join(native.home, "Library", "Caches"));
  for (const directory of [native.applicationSupport, native.caches])
    NodeAssert.ok((await NodeFSP.stat(directory)).isDirectory());
  await NodeFSP.mkdir(mount);
  // Once attempted, detach this exact mountpoint even if attach's response is
  // lost. No device-wide detach or unrelated mounted-image discovery is used.
  mounted = true;
  await command("hdiutil", ["attach", "-readonly", "-nobrowse", "-mountpoint", mount, dmg], 30000);
  const applications = (await NodeFSP.readdir(mount)).filter((name) => name.endsWith(".app"));
  NodeAssert.equal(applications.length, 1);
  const app = NodePath.join(mount, applications[0]);
  const metadata = JSON.parse(
    extractFile(NodePath.join(app, "Contents", "Resources", "app.asar"), "package.json").toString(
      "utf8",
    ),
  );
  NodeAssert.equal(metadata.jonesSource.repository, "Jones-Systems/Jones-Code");
  NodeAssert.equal(metadata.jonesSource.sha, process.env.GITHUB_SHA);
  NodeAssert.match(metadata.jonesSource.tree, /^[a-f0-9]{40}$/);
  const pinnedElectron = JSON.parse(
    await NodeFSP.readFile(new URL("../../../apps/desktop/package.json", import.meta.url), "utf8"),
  ).dependencies.electron;
  const packagedElectron = (
    await command("/usr/libexec/PlistBuddy", [
      "-c",
      "Print :CFBundleVersion",
      NodePath.join(
        app,
        "Contents",
        "Frameworks",
        "Electron Framework.framework",
        "Resources",
        "Info.plist",
      ),
    ])
  ).trim();
  NodeAssert.equal(packagedElectron, pinnedElectron);
  const executableName = (
    await command("/usr/libexec/PlistBuddy", [
      "-c",
      "Print :CFBundleExecutable",
      NodePath.join(app, "Contents", "Info.plist"),
    ])
  ).trim();
  const bundleIdentifier = (
    await command("/usr/libexec/PlistBuddy", [
      "-c",
      "Print :CFBundleIdentifier",
      NodePath.join(app, "Contents", "Info.plist"),
    ])
  ).trim();
  const bundleName = (
    await command("/usr/libexec/PlistBuddy", [
      "-c",
      "Print :CFBundleName",
      NodePath.join(app, "Contents", "Info.plist"),
    ])
  ).trim();
  NodeAssert.equal(NodePath.basename(executableName), executableName);
  const binary = NodePath.join(app, "Contents", "MacOS", executableName);
  const home = NodePath.join(native.home, ".t3");
  const profile = NodePath.join(native.applicationSupport, "t3code-v2");
  const names = packagedStartupNames(metadata, { executableName, bundleIdentifier, bundleName });
  const protectedPaths = [
    ...new Set([
      home,
      profile,
      ...[...names].flatMap((name) => [
        NodePath.join(native.applicationSupport, name),
        NodePath.join(native.caches, name),
      ]),
    ]),
  ];
  for (const filename of protectedPaths) await requireAbsent(filename);
  const active = await createNativeState(home, profile, metadata, ownership);
  for (const filename of leasePaths(active).filter(
    (name) => !name.startsWith(home + NodePath.sep),
  )) {
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
  evidence.observer = {
    positiveControls: "before-and-after",
    ...(await closeStartupObservation(observer, start, protectedPaths, evidence)),
  };
  observer = undefined;
  NodeAssert.deepEqual(await Promise.all(protectedPaths.map(snapshotTree)), before);
  evidence.protectedStateUnchanged = true;
  evidence.bootstrap = { outcome: "unknown", scenarios: [] };
  bootstrapFixture = await createLegacyBootstrapFixture({
    root,
    app,
    metadata,
    executableName,
    ownership,
    signal: controller.signal,
    environment: process.env,
  });
  // Reuse the owned app copy whose full payload digest was compared with the mounted DMG.
  const stagedApp = bootstrapFixture.selection.app;
  const authorityHome = NodePath.join(root, "native-authority-command-home");
  const userdata = NodePath.join(authorityHome, "userdata");
  await ownership.mkdir(authorityHome);
  await ownership.mkdir(userdata);
  const environmentId = "packaged-native-authority-qualification";
  await ownership.file(NodePath.join(userdata, "environment-id"), `${environmentId}\n`);
  const databasePath = NodePath.join(userdata, "statev2.sqlite");
  const database = new NodeSqlite.DatabaseSync(databasePath);
  try {
    database.exec("CREATE TABLE qualification_sentinel (value INTEGER NOT NULL)");
  } finally {
    database.close();
  }
  await NodeFSP.chmod(databasePath, 0o600);
  const authorityDirectory = NodePath.join(authorityHome, "native-store-authority");
  const initial = initializeNativeStoreAuthority(authorityDirectory, environmentId);
  const packagedEntry = NodePath.join(
    stagedApp.appPath,
    "Contents/Resources/app.asar/apps/server/dist/bin.mjs",
  );
  const authorityScenario = {
    name: "packaged-native-authority-fence-advance",
    outcome: "unknown",
    source: metadata.jonesSource,
    version: metadata.version,
    electron: packagedElectron,
    appReceipt: stagedApp,
    initialRecord: initial,
    steps: [],
    limitations: [
      "The early command alone does not provide the enclosing helper transaction admission fence.",
    ],
  };
  evidence.scenarios.push(authorityScenario);
  const authorityRequest = {
    protocol: 1,
    baseDir: authorityHome,
    databasePath,
    environmentId,
    candidate: {
      version: metadata.version,
      sourceSha: metadata.jonesSource.sha,
      sourceTree: metadata.jonesSource.tree,
    },
  };
  const invokeAuthority = async (name, operation, guard) => {
    const requestFile = NodePath.join(authorityHome, `${name}.json`);
    const request = { ...authorityRequest, operation, ...(guard === undefined ? {} : { guard }) };
    const input = `${JSON.stringify(request)}\n`;
    NodeAssert.ok(Buffer.byteLength(input) <= 16 * 1024);
    await ownership.file(requestFile, input);
    const step = { name, operation, outcome: "unknown" };
    authorityScenario.steps.push(step);
    // Positional arguments carry all paths; exec retains the owned PID/group and gives the real command stdin.
    const result = await runOwnedChild(
      "/bin/sh",
      [
        "-c",
        'exec "$1" "$2" __jones-native-authority-transition < "$3"',
        "jones-native-authority-qualification",
        stagedApp.executablePath,
        packagedEntry,
        requestFile,
      ],
      {
        env: { ...cleanLaunchEnvironment(process.env, root), ELECTRON_RUN_AS_NODE: "1" },
        cwd: authorityHome,
        signal: controller.signal,
        timeout: 30000,
      },
    );
    Object.assign(step, {
      outcome: "exited",
      exitCode: result.code,
      signal: result.signal,
      stdout: result.stdout.slice(0, 16 * 1024),
      diagnostic: result.stderr.slice(-1024),
    });
    step.record = readNativeStoreAuthorityState(authorityDirectory);
    NodeAssert.ok(Buffer.byteLength(result.stdout) <= 16 * 1024);
    return result;
  };
  try {
    const fenced = await invokeAuthority("fence", "fence");
    NodeAssert.equal(fenced.code, 0, fenced.stderr);
    const fencedRecord = readNativeStoreAuthorityState(authorityDirectory);
    NodeAssert.deepEqual(fencedRecord, {
      ...initial,
      state: "fenced",
      transition_id: fencedRecord.transition_id,
    });
    NodeAssert.match(fencedRecord.transition_id, /^[0-9a-f-]{36}$/);
    const guard = {
      kind: "fenced",
      environmentId,
      authorityNamespace: initial.authority_namespace,
      storeGeneration: initial.store_generation,
      transitionId: fencedRecord.transition_id,
      recordSha256: NodeCrypto.createHash("sha256")
        .update(`${JSON.stringify(fencedRecord)}\n`)
        .digest("hex"),
    };
    NodeAssert.deepEqual(JSON.parse(fenced.stdout), { protocol: 1, operation: "fence", guard });
    const advanced = await invokeAuthority("advance", "advance", guard);
    NodeAssert.equal(advanced.code, 0, advanced.stderr);
    NodeAssert.deepEqual(JSON.parse(advanced.stdout), {
      protocol: 1,
      operation: "advance",
      state: "advanced",
      authorityNamespace: initial.authority_namespace,
      storeGeneration: initial.store_generation + 1,
    });
    const advancedRecord = { ...initial, store_generation: initial.store_generation + 1 };
    NodeAssert.deepEqual(readNativeStoreAuthorityState(authorityDirectory), advancedRecord);
    const replay = await invokeAuthority("replay", "advance", guard);
    NodeAssert.equal(replay.code, 1);
    NodeAssert.equal(replay.stdout, "");
    NodeAssert.match(
      replay.stderr,
      /Native authority transition refused; reconciliation required\./,
    );
    NodeAssert.deepEqual(readNativeStoreAuthorityState(authorityDirectory), advancedRecord);
    authorityScenario.finalRecord = advancedRecord;
    authorityScenario.outcome = "passed";
  } catch (error) {
    authorityScenario.outcome = "failed";
    authorityScenario.error = String(error).slice(0, 1024);
    try {
      authorityScenario.finalRecord = readNativeStoreAuthorityState(authorityDirectory);
    } catch (readbackError) {
      authorityScenario.readbackError = String(readbackError).slice(0, 1024);
    }
    throw error;
  }
  await qualifyLegacyBootstrap(bootstrapFixture, evidence.bootstrap);
  evidence.source = metadata.jonesSource;
  evidence.version = metadata.version;
  evidence.electron = packagedElectron;
  evidence.leasesReleased = true;
  evidence.outcome = "passed";
} catch (error) {
  failure = error;
  evidence.outcome = "failed";
  evidence.error = String(error).slice(0, 2048);
  if (evidence.bootstrap?.outcome === "unknown") evidence.bootstrap.outcome = "failed";
  if (error instanceof ObserverControlError) evidence.observerControlFailure = error.control;
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
    if (bootstrapFixture) {
      await NodeAssert.rejects(NodeFSP.lstat(bootstrapFixture.fixture), { code: "ENOENT" });
      if (evidence.bootstrap) evidence.bootstrap.fixtureRemoved = true;
    }
  } catch (error) {
    recordCleanupFailure(error);
  }
  if (mounted) {
    try {
      const detached = await runOwnedChild("hdiutil", ["detach", mount], { timeout: 30000 });
      NodeAssert.equal(detached.code, 0, detached.stderr);
      mounted = false;
    } catch (error) {
      recordCleanupFailure(error);
    }
  }
  if (failure) evidence.outcome = "failed";
  if (!mounted) {
    try {
      await scratch.cleanup();
      await NodeAssert.rejects(NodeFSP.lstat(root), { code: "ENOENT" });
      evidence.scratchRemoved = true;
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
