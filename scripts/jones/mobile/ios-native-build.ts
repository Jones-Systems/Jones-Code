// @effect-diagnostics nodeBuiltinImport:off globalDate:off globalTimers:off - Hosted native build glue uses the existing captured-child adapter and same-host admission.
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";
import * as NodeURL from "node:url";
import { collectAdmissionSample, waitForResourceAdmission } from "./ios-admission.ts";
import type { AdmissionWaitOptions } from "./ios-admission.ts";
import {
  configureTestRun,
  fileDigest,
  nativeEnvironment,
  object,
  ownedCommand,
  treeDigest,
} from "./xcuitest.ts";
import type { ChildReceipt } from "./xcuitest.ts";

export const versionNumber = (value: string) => {
  NodeAssert.match(value, /^\d+\.\d+(?:\.\d+)?$/);
  const [major = 0, minor = 0, patch = 0] = value.split(".").map(Number);
  return major * 1_000_000 + minor * 1_000 + patch;
};
export function selectXcode(
  candidates: { developerDir: string; version: string; description: string }[],
) {
  const selected = candidates
    .filter((item) => versionNumber(item.version) >= versionNumber("26.4"))
    .sort((a, b) => versionNumber(b.version) - versionNumber(a.version))[0];
  NodeAssert.ok(selected, "hosted-xcode-below-floor");
  return selected;
}
export function selectRuntime(runtimes: unknown[], deviceTypes: unknown[], sdk: string) {
  const selected = runtimes
    .map(object)
    .filter(
      (item) =>
        item.isAvailable === true &&
        typeof item.identifier === "string" &&
        item.identifier.startsWith("com.apple.CoreSimulator.SimRuntime.iOS-") &&
        typeof item.version === "string" &&
        versionNumber(item.version) <= versionNumber(sdk),
    )
    .sort((a, b) => versionNumber(String(b.version)) - versionNumber(String(a.version)))[0];
  NodeAssert.ok(selected, "hosted-runtime-unavailable");
  const device = deviceTypes
    .map(object)
    .find((item) => item.name === "iPhone 17" && typeof item.identifier === "string");
  NodeAssert.ok(device, "hosted-device-type-unavailable");
  return {
    runtime: String(selected.identifier),
    runtimeVersion: String(selected.version),
    deviceType: String(device.identifier),
    deviceName: "iPhone 17",
  };
}

export async function selectTooling(cwd: string, children: ChildReceipt[], signal: AbortSignal) {
  const candidates = [];
  const run = async (exe: string, argv: string[], developerDir?: string) =>
    ownedCommand(exe, argv, {
      cwd,
      env: {
        PATH: process.env.PATH ?? "/usr/bin:/bin",
        ...(developerDir ? { DEVELOPER_DIR: developerDir } : {}),
      },
      signal,
      receipts: children,
      timeoutMs: 120_000,
    }).result;
  const directories = new Set<string>();
  for (const name of await NodeFSP.readdir("/Applications")) {
    if (!/^Xcode.*\.app$/.test(name)) continue;
    const developerDir = await NodeFSP.realpath(
      NodePath.join("/Applications", name, "Contents/Developer"),
    );
    if (directories.has(developerDir)) continue;
    directories.add(developerDir);
    const result = await run(
      NodePath.join(developerDir, "usr/bin/xcodebuild"),
      ["-version"],
      developerDir,
    );
    const version = /^Xcode (\d+\.\d+(?:\.\d+)?)/m.exec(result.stdout)?.[1];
    if (result.code === 0 && version)
      candidates.push({ developerDir, version, description: result.stdout.trim() });
  }
  const selected = selectXcode(candidates);
  const sdk = await run(
    "/usr/bin/xcrun",
    ["--sdk", "iphonesimulator", "--show-sdk-version"],
    selected.developerDir,
  );
  const runtimes = await run(
    "/usr/bin/xcrun",
    ["simctl", "list", "runtimes", "--json"],
    selected.developerDir,
  );
  const types = await run(
    "/usr/bin/xcrun",
    ["simctl", "list", "devicetypes", "--json"],
    selected.developerDir,
  );
  NodeAssert.ok(
    [sdk, runtimes, types].every((item) => item.code === 0),
    "hosted-runtime-unavailable",
  );
  const runtimeRows = object(JSON.parse(runtimes.stdout)).runtimes;
  const typeRows = object(JSON.parse(types.stdout)).devicetypes;
  NodeAssert.ok(Array.isArray(runtimeRows) && Array.isArray(typeRows));
  return {
    ...selected,
    ...selectRuntime(runtimeRows, typeRows, sdk.stdout.trim()),
    sdkVersion: sdk.stdout.trim(),
    imageOS: process.env.ImageOS,
    imageVersion: process.env.ImageVersion,
  };
}

export function buildArgv(workspace: string, root: string): string[] {
  return [
    "-quiet",
    "build-for-testing",
    "-workspace",
    workspace,
    "-scheme",
    "JonesE2eUITests",
    "-configuration",
    "Release",
    "-sdk",
    "iphonesimulator",
    "-destination",
    "generic/platform=iOS Simulator",
    "-derivedDataPath",
    NodePath.join(root, "DerivedData"),
    "-jobs",
    "2",
    "ARCHS=arm64",
    "CODE_SIGNING_ALLOWED=NO",
    "COMPILER_INDEX_STORE_ENABLE=NO",
  ];
}

export function productPaths(decoded: unknown, products: string) {
  const configured = configureTestRun(decoded, products, {});
  const configs = configured.TestConfigurations;
  const target = Array.isArray(configs)
    ? object((object(configs[0]).TestTargets as unknown[])[0])
    : object(configured.JonesE2eUITests);
  return {
    app: String(target.UITargetAppPath),
    testHost: String(target.TestHostPath),
    testBundle: String(target.TestBundlePath),
  };
}

export interface BuildOptions {
  source: string;
  sourceSha: string;
  root: string;
  output: string;
  developerDir: string;
  serialSlot: string;
  children: ChildReceipt[];
  signal: AbortSignal;
}

export async function admitBuildBoundary(
  options: Omit<BuildOptions, "source" | "developerDir">,
  label: string,
  dependencies: Partial<
    Pick<AdmissionWaitOptions, "sample" | "now" | "monotonicNow" | "wait" | "env">
  > = {},
) {
  const eventsPath = NodePath.join(options.output, `${label}-admission-events.jsonl`);
  await NodeFSP.writeFile(eventsPath, "", { flag: "wx", mode: 0o600 });
  return waitForResourceAdmission({
    sourceSha: options.sourceSha,
    serialSlot: options.serialSlot,
    signal: options.signal,
    sample: (signal) =>
      collectAdmissionSample(
        options.sourceSha,
        options.serialSlot,
        options.root,
        options.children,
        signal,
      ),
    ...dependencies,
    record: async (event) => {
      await NodeFSP.appendFile(eventsPath, `${JSON.stringify(event)}\n`);
      if (event.kind === "ready")
        await NodeFSP.writeFile(
          NodePath.join(options.output, `${label}-admission.json`),
          JSON.stringify(event.sample),
          { flag: "wx", mode: 0o600 },
        );
    },
  });
}

// The caller owns the disposable checkout and build root through all native
// runs, then removes them only after the captured children and devices are gone.
async function performBuild(options: BuildOptions) {
  const { source, sourceSha, root, developerDir, children, signal } = options;
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Pre-install bootstrap must read the actual host without loading HostProcess workspace dependencies.
  NodeAssert.equal(process.arch, "arm64", "Hosted build requires Apple Silicon");
  const mobile = NodePath.join(source, "apps/mobile"),
    ios = NodePath.join(mobile, "ios");
  const env = {
    ...nativeEnvironment(root, developerDir, process.env.PATH ?? "/usr/bin:/bin"),
    EXPO_NO_DOTENV: "1",
    EXPO_NO_TELEMETRY: "1",
    JONES_IOS_E2E: "1",
    LANG: "en_US.UTF-8",
    LC_ALL: "en_US.UTF-8",
    CP_HOME_DIR: NodePath.join(root, "pod-home"),
    COCOAPODS_CACHE_PATH: NodePath.join(root, "pod-cache"),
    JONES_EXPECTED_SOURCE_SHA: sourceSha,
  };
  for (const directory of ["home", "tmp", "cache", "pod-home", "pod-cache"])
    await NodeFSP.mkdir(NodePath.join(root, directory), { recursive: true });
  const run = async (
    label: string,
    executable: string,
    argv: string[],
    cwd: string,
    timeoutMs = 120_000,
  ) => {
    signal.throwIfAborted();
    const result = await ownedCommand(executable, argv, {
      cwd,
      env,
      signal,
      timeoutMs,
      receipts: children,
    }).result;
    await NodeFSP.writeFile(NodePath.join(root, `${label}.log`), result.stdout + result.stderr, {
      flag: "wx",
      mode: 0o600,
    });
    NodeAssert.ok(
      !/ENOSPC|No space left on device/i.test(result.stdout + result.stderr),
      "hosted-disk-exhausted",
    );
    NodeAssert.equal(
      result.code,
      0,
      `${label} failed; private build output retained until owned cleanup`,
    );
    return result.stdout.trim();
  };
  const checkSource = async (label: string) => {
    NodeAssert.equal(
      await run(`${label}-head`, "git", ["rev-parse", "HEAD"], source),
      sourceSha,
      "Source changed",
    );
    NodeAssert.equal(
      await run(`${label}-status`, "git", ["status", "--porcelain"], source),
      "",
      "Source must be clean",
    );
    const branch = await run(`${label}-branch`, "git", ["branch", "--show-current"], source);
    NodeAssert.ok(
      branch && !["main", "master", "HEAD"].includes(branch),
      "Named task branch required",
    );
  };
  const admit = (label: string) => admitBuildBoundary(options, label);
  await checkSource("before");
  await run("ignored-ios", "git", ["check-ignore", "apps/mobile/ios/"], source);
  await NodeAssert.rejects(NodeFSP.lstat(ios), { code: "ENOENT" }, "Generated iOS must be absent");
  const xcodebuild = NodePath.join(developerDir, "usr/bin/xcodebuild");
  const xcodeVersion = await run("xcode-version", xcodebuild, ["-version"], source);
  await run("xcode-ready", xcodebuild, ["-checkFirstLaunchStatus"], source);
  const manifests = await Promise.all(
    ["apps/server/package.json", "apps/desktop/package.json"].map(async (relative) => {
      const path = NodePath.join(source, relative),
        bytes = await NodeFSP.readFile(path);
      await NodeFSP.writeFile(
        NodePath.join(root, `${relative.split("/")[1]}-package.original.json`),
        bytes,
        { flag: "wx", mode: 0o600 },
      );
      return { path, bytes };
    }),
  );
  try {
    await run(
      "stamp",
      process.execPath,
      [NodePath.join(source, "scripts/stamp-jones-build.ts")],
      source,
    );
    await admit("server");
    await run("server", "vp", ["run", "--filter", "t3", "build"], source, 10 * 60_000);
  } finally {
    for (const manifest of manifests) await NodeFSP.writeFile(manifest.path, manifest.bytes);
  }
  await NodeFSP.writeFile(
    NodePath.join(root, "generated-ios-owner.json"),
    JSON.stringify({ source, sourceSha, path: ios }),
    { flag: "wx", mode: 0o600 },
  );
  await admit("prebuild");
  await run(
    "prebuild",
    process.execPath,
    [
      NodePath.join(mobile, "node_modules/expo/bin/cli"),
      "prebuild",
      "--platform",
      "ios",
      "--no-install",
    ],
    mobile,
    10 * 60_000,
  );
  await admit("pods");
  await run("pods", "pod", ["install"], ios, 15 * 60_000);
  const workspaces = (await NodeFSP.readdir(ios)).filter((name) => name.endsWith(".xcworkspace"));
  NodeAssert.equal(workspaces.length, 1, "Expected one generated workspace");
  const argv = buildArgv(NodePath.join(ios, workspaces[0]!), root);
  await admit("compile");
  await run("build", xcodebuild, argv, ios, 35 * 60_000);
  await checkSource("after");
  const products = NodePath.join(root, "DerivedData/Build/Products");
  const runs = (await NodeFSP.readdir(products)).filter((name) => name.endsWith(".xctestrun"));
  NodeAssert.equal(runs.length, 1, "Expected one built test run");
  const xctestrun = NodePath.join(products, runs[0]!);
  const decoded: unknown = JSON.parse(
    await run(
      "decode-test-run",
      "/usr/bin/plutil",
      ["-convert", "json", "-o", "-", xctestrun],
      source,
    ),
  );
  const paths = productPaths(decoded, products);
  const artifacts: Record<string, { path: string; sha256: string }> = {};
  for (const [key, path] of Object.entries(paths))
    artifacts[key] = { path, sha256: await treeDigest(path) };
  artifacts.xctestrun = { path: xctestrun, sha256: await fileDigest(xctestrun) };
  const record = {
    sourceSha,
    developerDir,
    xcodeVersion,
    buildArgv: argv,
    children: [...children],
    ownedRoot: root,
    artifacts,
    bundleSha256: await fileDigest(NodePath.join(paths.app, "main.jsbundle")),
  };
  const buildRecord = NodePath.join(root, "build-record.json");
  await NodeFSP.writeFile(buildRecord, JSON.stringify(record, null, 2), {
    flag: "wx",
    mode: 0o600,
  });
  return { buildRecord, record };
}

export async function buildForTesting(options: BuildOptions, diskSamplesPath: string) {
  const samples: { measuredAt: number; freeDiskBytes: number }[] = [];
  const controller = new AbortController();
  let diskError: unknown, primary: unknown;
  const sample = async () => {
    const stat = await NodeFSP.statfs(options.root);
    samples.push({ measuredAt: Date.now(), freeDiskBytes: stat.bavail * stat.bsize });
    await NodeFSP.writeFile(diskSamplesPath, JSON.stringify(samples), { mode: 0o600 });
  };
  await NodeFSP.writeFile(diskSamplesPath, "[]", { flag: "wx", mode: 0o600 });
  await sample();
  const monitor = (async () => {
    while (!controller.signal.aborted) {
      await NodeTimersPromises.setTimeout(10_000, undefined, { signal: controller.signal });
      await sample();
    }
  })().catch((error: unknown) => {
    if (!controller.signal.aborted) diskError = error;
  });
  try {
    const result = await performBuild(options);
    if (diskError) throw diskError;
    return result;
  } catch (error) {
    primary = error;
    throw error;
  } finally {
    controller.abort();
    await monitor;
    try {
      await sample();
    } catch (error) {
      if (primary) throw new AggregateError([primary, error], "Build and final disk sample failed");
      throw error;
    }
  }
}

export function jobInputs() {
  const required = (key: string) => {
    const value = process.env[key];
    NodeAssert.ok(value, `Missing ${key}`);
    return value;
  };
  return {
    source: process.cwd(),
    sourceSha: required("JONES_IOS_SOURCE_SHA"),
    root: required("JONES_IOS_ROOT"),
    output: required("JONES_IOS_OUTPUT"),
    serialSlot: required("JONES_IOS_SERIAL_SLOT"),
  };
}

export function buildFailureCode(error: unknown): string {
  if (error instanceof AggregateError) {
    const nested = error.errors.map(buildFailureCode).find((code) => code !== "other");
    if (nested) return nested;
  }
  if (
    (error as NodeJS.ErrnoException)?.code === "ENOSPC" ||
    (error instanceof Error &&
      /ENOSPC|No space left on device|hosted-disk-exhausted/i.test(error.message))
  )
    return "hosted-disk-exhausted";
  for (const code of [
    "hosted-xcode-below-floor",
    "hosted-runtime-unavailable",
    "hosted-device-type-unavailable",
  ])
    if (error instanceof Error && error.message.includes(code)) return code;
  return "other";
}

// Only remove this job's private roots after every recorded native owner has
// proved cleanup. Missing or unknown native evidence retains those roots.
async function cleanupBuild(input: ReturnType<typeof jobInputs>) {
  const owner = object(
    JSON.parse(await NodeFSP.readFile(NodePath.join(input.output, "owner.json"), "utf8")),
  );
  const stat = await NodeFSP.lstat(input.root);
  NodeAssert.ok(stat.isDirectory() && !stat.isSymbolicLink());
  NodeAssert.equal(owner.root, input.root);
  NodeAssert.equal(owner.source, input.source);
  NodeAssert.equal(owner.sourceSha, input.sourceSha);
  NodeAssert.equal(owner.dev, stat.dev);
  NodeAssert.equal(owner.ino, stat.ino);
  for (const app of ["server", "desktop"]) {
    const original = await NodeFSP.readFile(
      NodePath.join(input.root, `${app}-package.original.json`),
    ).catch((error: NodeJS.ErrnoException) => {
      if (error.code === "ENOENT") return undefined;
      throw error;
    });
    if (original)
      NodeAssert.ok(
        original.equals(
          await NodeFSP.readFile(NodePath.join(input.source, `apps/${app}/package.json`)),
        ),
        "Package restoration unresolved; retain original bytes",
      );
  }
  for (const name of await NodeFSP.readdir(input.output)) {
    if (/^build-.*-state\.json$/.test(name)) {
      const state = object(
        JSON.parse(await NodeFSP.readFile(NodePath.join(input.output, name), "utf8")),
      );
      NodeAssert.equal(state.cleanupKnown, true, "Build lifetime unresolved; retain scratch");
    }
    if (/^build-.*-children\.json$/.test(name)) {
      const children: unknown = JSON.parse(
        await NodeFSP.readFile(NodePath.join(input.output, name), "utf8"),
      );
      NodeAssert.ok(
        Array.isArray(children) && children.every((raw) => object(raw).groupGone === true),
        "Build cleanup unresolved",
      );
    }
    if (name === "sentinel.json" || name === "control-pinned-check.json") {
      const receipt = object(
        JSON.parse(await NodeFSP.readFile(NodePath.join(input.output, name), "utf8")),
      );
      NodeAssert.equal(receipt.cleanupKnown, true, "Native cleanup unresolved; retain scratch");
    }
  }
  const marker = await NodeFSP.readFile(
    NodePath.join(input.root, "generated-ios-owner.json"),
    "utf8",
  ).catch((error: NodeJS.ErrnoException) => {
    if (error.code === "ENOENT") return undefined;
    throw error;
  });
  if (marker) {
    const value = object(JSON.parse(marker)),
      ios = NodePath.join(input.source, "apps/mobile/ios");
    NodeAssert.ok(
      value.source === input.source && value.sourceSha === input.sourceSha && value.path === ios,
    );
    await NodeFSP.rm(ios, { recursive: true, force: true });
    await NodeAssert.rejects(NodeFSP.lstat(ios), { code: "ENOENT" });
  }
  await NodeFSP.rm(input.root, { recursive: true });
  await NodeAssert.rejects(NodeFSP.lstat(input.root), { code: "ENOENT" });
  const elapsedMs =
    Number(process.hrtime.bigint() - BigInt(String(owner.startedMonotonicNs))) / 1_000_000;
  const budget = {
    schema: "jones-ios-hosted-budget/1",
    elapsedMs,
    budgetMs: 50 * 60_000,
    met: elapsedMs >= 0 && elapsedMs <= 50 * 60_000,
  };
  await NodeFSP.writeFile(NodePath.join(input.output, "job-budget.json"), JSON.stringify(budget), {
    flag: "wx",
    mode: 0o600,
  });
  NodeAssert.ok(budget.met, "Hosted 50-minute measured budget exceeded");
  return { outcome: "removed", nativeClaim: false };
}

export async function buildMain(mode: string) {
  const input = jobInputs(),
    children: ChildReceipt[] = [];
  const controller = new AbortController(),
    cancel = () => controller.abort();
  let started = false,
    failure: unknown;
  process.once("SIGINT", cancel);
  process.once("SIGTERM", cancel);
  try {
    await NodeFSP.mkdir(input.output, { recursive: true });
    if (mode === "--cleanup") return await cleanupBuild(input);
    NodeAssert.ok(
      ["--admission", "--tooling", "--build"].includes(mode),
      "Use --admission, --tooling, --build or --cleanup",
    );
    await NodeFSP.writeFile(
      NodePath.join(input.output, `build-${mode.slice(2)}-state.json`),
      JSON.stringify({ cleanupKnown: false }),
      { flag: "wx", mode: 0o600 },
    );
    started = true;
    if (mode === "--admission") {
      const stat = await NodeFSP.lstat(input.root);
      NodeAssert.ok(stat.isDirectory() && !stat.isSymbolicLink());
      await NodeFSP.writeFile(
        NodePath.join(input.output, "owner.json"),
        JSON.stringify({
          ...input,
          dev: stat.dev,
          ino: stat.ino,
          startedMonotonicNs: process.hrtime.bigint().toString(),
        }),
        { flag: "wx", mode: 0o600 },
      );
    }
    const admission = await admitBuildBoundary(
      { ...input, children, signal: controller.signal },
      mode.slice(2),
    );
    if (mode === "--admission") return admission;
    NodeAssert.ok(
      mode === "--tooling" || mode === "--build",
      "Use --admission, --tooling or --build",
    );
    if (mode === "--tooling") {
      const tooling = await selectTooling(input.root, children, controller.signal);
      await NodeFSP.writeFile(
        NodePath.join(input.output, "tooling.json"),
        JSON.stringify(tooling, null, 2),
        { flag: "wx", mode: 0o600 },
      );
      return tooling;
    }
    const tooling = object(
      JSON.parse(await NodeFSP.readFile(NodePath.join(input.output, "tooling.json"), "utf8")),
    );
    const result = await buildForTesting(
      { ...input, developerDir: String(tooling.developerDir), children, signal: controller.signal },
      NodePath.join(input.output, "disk-samples.json"),
    );
    await NodeFSP.copyFile(
      result.buildRecord,
      NodePath.join(input.output, "build-record.json"),
      NodeFSP.constants.COPYFILE_EXCL,
    );
    return { outcome: "pass", nativeClaim: false };
  } catch (error) {
    failure = error;
    throw error;
  } finally {
    process.removeListener("SIGINT", cancel);
    process.removeListener("SIGTERM", cancel);
    await NodeFSP.writeFile(
      NodePath.join(input.output, `build-${mode.slice(2)}-children.json`),
      JSON.stringify(children),
      { mode: 0o600 },
    );
    if (started)
      await NodeFSP.writeFile(
        NodePath.join(input.output, `build-${mode.slice(2)}-state.json`),
        JSON.stringify({
          outcome: failure ? "fail" : "pass",
          nativeClaim: false,
          failureCode: failure ? buildFailureCode(failure) : null,
          cleanupKnown: children.every((child) => child.groupGone === true),
          children,
        }),
        { mode: 0o600 },
      );
  }
}

if (
  process.argv[1] &&
  NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href === import.meta.url
) {
  buildMain(process.argv[2] ?? "")
    .then((result) => process.stdout.write(`${JSON.stringify(result)}\n`))
    .catch((error: unknown) => {
      process.stderr.write(
        `${(error as NodeJS.ErrnoException).code === "ENOSPC" ? "hosted-disk-exhausted" : error instanceof Error ? error.message : "Build failed"}\n`,
      );
      process.exitCode = 1;
    });
}
