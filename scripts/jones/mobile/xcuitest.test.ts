// @effect-diagnostics nodeBuiltinImport:off - Standalone native-adapter fixtures own temporary files and exercise Node subprocess cleanup without a server Effect runtime.
import { describe, expect, it, vi } from "vite-plus/test";
import * as NodeEvents from "node:events";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeModule from "node:module";
import {
  configureTestRun,
  nativeEnvironment,
  ownedCommand,
  testArgv,
  treeDigest,
} from "./xcuitest.ts";

const target = () => ({
  BlueprintName: "JonesE2eUITests",
  IsUITestBundle: true,
  TestBundlePath: "__TESTROOT__/Release/JonesE2eUITests-Runner.app/PlugIns/JonesE2eUITests.xctest",
  TestHostPath: "__TESTROOT__/Release/JonesE2eUITests-Runner.app",
  UITargetAppPath: "__TESTROOT__/Release/App.app",
  EnvironmentVariables: { FOREIGN_TOKEN: "secret" },
  UITargetAppEnvironmentVariables: { EXPO_PUBLIC_SHOWCASE: "1" },
  SkipTestIdentifiers: ["PinWorkflowTests"],
});

describe("XCUITest invocation", () => {
  it("binds one created destination, disables parallelism/repetition, uses owned results", () => {
    const args = testArgv(
      "/owned/run.xctestrun",
      "12345678-1234-1234-1234-123456789abc",
      "/owned/run",
    );
    expect(args).toContain("test-without-building");
    expect(args[args.indexOf("-destination") + 1]).toBe(
      "platform=iOS Simulator,id=12345678-1234-1234-1234-123456789abc",
    );
    expect(args[args.indexOf("-parallel-testing-enabled") + 1]).toBe("NO");
    expect(args[args.indexOf("-test-iterations") + 1]).toBe("1");
    expect(args[args.indexOf("-resultBundlePath") + 1]).toBe("/owned/run/result.xcresult");
    expect(args).not.toContain("-retry-tests-on-failure");
  });
  it("rebases the single built target and discards inherited application/runner inputs", () => {
    const run = configureTestRun(
      { TestConfigurations: [{ TestTargets: [target()] }] },
      "/build/Products",
      { JONES_E2E_URL: "http://127.0.0.1:1", JONES_E2E_TOKEN: "owned" },
    );
    const configuration = (
      run.TestConfigurations as { TestTargets: Record<string, unknown>[] }[]
    )[0]!;
    expect(configuration.TestTargets[0]).toMatchObject({
      TestHostPath: "/build/Products/Release/JonesE2eUITests-Runner.app",
      ParallelizationEnabled: false,
      EnvironmentVariables: { JONES_E2E_URL: "http://127.0.0.1:1", JONES_E2E_TOKEN: "owned" },
      UITargetAppEnvironmentVariables: {},
      SystemAttachmentLifetime: "keepNever",
      UserAttachmentLifetime: "keepNever",
    });
    expect(configuration.TestTargets[0]!.SkipTestIdentifiers).toBeUndefined();
  });
  it("rejects extra targets, configurations and escaping artifact paths", () => {
    expect(() =>
      configureTestRun(
        { TestConfigurations: [{ TestTargets: [target(), target()] }] },
        "/build",
        {},
      ),
    ).toThrow("one test target");
    expect(() => configureTestRun({ TestConfigurations: [] }, "/build", {})).toThrow(
      "one test configuration",
    );
    expect(() =>
      configureTestRun(
        { JonesE2eUITests: { ...target(), TestHostPath: "__TESTROOT__/../foreign.app" } },
        "/build",
        {},
      ),
    ).toThrow("Escaping");
    expect(() =>
      configureTestRun({ JonesE2eUITests: target(), OtherTests: target() }, "/build", {}),
    ).toThrow();
  });
  it("does not inherit provider state or change host Xcode selection", () => {
    const env = nativeEnvironment(
      "/owned",
      "/Applications/Selected.app/Contents/Developer",
      "/bin",
    );
    expect(env).toMatchObject({
      HOME: "/owned/home",
      TMPDIR: "/owned/tmp",
      DEVELOPER_DIR: "/Applications/Selected.app/Contents/Developer",
    });
    expect(env.OPENAI_API_KEY).toBeUndefined();
    expect(env.NODE_OPTIONS).toBeUndefined();
  });
});

describe("captured child lifecycle (no Simulator or Xcode)", () => {
  async function fixture(run: (root: string, env: NodeJS.ProcessEnv) => Promise<void>) {
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-xcuitest-test-"));
    try {
      await run(root, { PATH: "/usr/bin:/bin", HOME: root, TMPDIR: root });
    } finally {
      await NodeFSP.rm(root, { recursive: true });
    }
    await expect(NodeFSP.stat(root)).rejects.toMatchObject({ code: "ENOENT" });
  }
  it("captures PID/PGID and verifies disappearance on success and failure", async () => {
    await fixture(async (root, env) => {
      for (const code of [0, 7]) {
        const result = await ownedCommand(process.execPath, ["-e", `process.exit(${code})`], {
          cwd: root,
          env,
        }).result;
        expect(result.code).toBe(code);
        expect(result.receipt.pid).toBe(result.receipt.pgid);
        expect(result.receipt.groupGone).toBe(true);
      }
    });
  });
  it("bounds startup timeout and preserves a sibling sleep sentinel", async () => {
    await fixture(async (root, env) => {
      const sentinel = ownedCommand("/bin/sleep", ["30"], { cwd: root, env });
      void sentinel.result.catch(() => {});
      try {
        const timeout = ownedCommand(process.execPath, ["-e", "setInterval(()=>{}, 1000)"], {
          cwd: root,
          env,
          timeoutMs: 40,
        });
        await expect(timeout.result).rejects.toThrow("timed out");
        expect(timeout.receipt.groupGone).toBe(true);
        expect(process.kill(sentinel.child.pid!, 0)).toBe(true);
      } finally {
        await sentinel.stop();
        await sentinel.result;
      }
    });
  });
  it("cancels a process group containing a child without touching a sibling", async () => {
    await fixture(async (root, env) => {
      const controller = new AbortController();
      const command = ownedCommand(
        process.execPath,
        [
          "-e",
          "require('node:child_process').spawn('/bin/sleep',['30'],{stdio:'ignore'}); console.log('ready'); setInterval(()=>{},1000)",
        ],
        { cwd: root, env, signal: controller.signal },
      );
      const result = command.result;
      void result.catch(() => {});
      await new Promise<void>((resolve) => command.child.stdout!.once("data", () => resolve()));
      controller.abort();
      await expect(result).rejects.toThrow("cancelled");
      expect(command.receipt.groupGone).toBe(true);
    });
  });
  it("reports failed spawn and does not create a process after pre-cancellation", async () => {
    await fixture(async (root, env) => {
      await expect(
        ownedCommand(NodePath.join(root, "absent"), [], { cwd: root, env }).result,
      ).rejects.toThrow();
      expect(() =>
        ownedCommand(process.execPath, [], { cwd: root, env, signal: AbortSignal.abort() }),
      ).toThrow();
    });
  });
  it("hashes artifact contents and rejects foreign symlinks", async () => {
    await fixture(async (root) => {
      await NodeFSP.writeFile(NodePath.join(root, "bundle"), "first");
      const before = await treeDigest(root);
      await NodeFSP.writeFile(NodePath.join(root, "bundle"), "second");
      expect(await treeDigest(root)).not.toBe(before);
      await NodeFSP.symlink("/foreign", NodePath.join(root, "escape"));
      await expect(treeDigest(root)).rejects.toThrow("symlinks");
    });
  });
});

describe("opt-in generated UI-test target", () => {
  it("creates a one-way test dependency and Release scheme without modifying app settings", async () => {
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-xcode-project-test-"));
    try {
      const sourceRoot = NodePath.resolve(import.meta.dirname, "../../..");
      const mobileRoot = NodePath.join(sourceRoot, "apps/mobile");
      const require = NodeModule.createRequire(NodePath.join(mobileRoot, "package.json"));
      const expoRequire = NodeModule.createRequire(require.resolve("expo/config-plugins"));
      const pluginsRequire = NodeModule.createRequire(expoRequire.resolve("@expo/config-plugins"));
      const xcode = pluginsRequire("xcode");
      const plugin = require("./plugins/withJonesE2eUITests.cjs");
      const ios = NodePath.join(root, "ios");
      const projectFile = NodePath.join(ios, "Fixture.xcodeproj/project.pbxproj");
      await NodeFSP.mkdir(NodePath.dirname(projectFile), { recursive: true });
      await NodeFSP.mkdir(NodePath.join(root, "e2e/xcuitest"), { recursive: true });
      await NodeFSP.copyFile(
        NodePath.join(mobileRoot, "e2e/xcuitest/PinWorkflowTests.swift"),
        NodePath.join(root, "e2e/xcuitest/PinWorkflowTests.swift"),
      );
      // node-xcode uses section delimiters and reference comments to build its typed model.
      await NodeFSP.writeFile(
        projectFile,
        `// !$*UTF8*$!
{
 archiveVersion = 1; classes = {}; objectVersion = 60;
 objects = {
/* Begin PBXNativeTarget section */
  A00000000000000000000001 = { isa = PBXNativeTarget; buildConfigurationList = A00000000000000000000007; buildPhases = (); buildRules = (); dependencies = (); name = Fixture; productName = Fixture; productReference = A00000000000000000000005; productType = "com.apple.product-type.application"; };
/* End PBXNativeTarget section */
/* Begin PBXProject section */
  A00000000000000000000002 = { isa = PBXProject; attributes = {}; buildConfigurationList = A00000000000000000000007; compatibilityVersion = "Xcode 14.0"; mainGroup = A00000000000000000000003; productRefGroup = A00000000000000000000004; targets = (A00000000000000000000001 /* Fixture */,); };
/* End PBXProject section */
/* Begin PBXGroup section */
  A00000000000000000000003 = { isa = PBXGroup; children = (A00000000000000000000004 /* Products */,); sourceTree = "<group>"; };
  A00000000000000000000004 /* Products */ = { isa = PBXGroup; children = (A00000000000000000000005 /* Fixture.app */,); name = Products; sourceTree = "<group>"; };
/* End PBXGroup section */
/* Begin PBXFileReference section */
  A00000000000000000000005 = { isa = PBXFileReference; explicitFileType = wrapper.application; path = Fixture.app; sourceTree = BUILT_PRODUCTS_DIR; };
/* End PBXFileReference section */
/* Begin XCBuildConfiguration section */
  A00000000000000000000006 = { isa = XCBuildConfiguration; buildSettings = { ORIGINAL_APP_SETTING = retained; }; name = Release; };
/* End XCBuildConfiguration section */
/* Begin PBXBuildFile section */
  A00000000000000000000008 = { isa = PBXBuildFile; fileRef = A00000000000000000000005; };
/* End PBXBuildFile section */
/* Begin XCConfigurationList section */
  A00000000000000000000007 = { isa = XCConfigurationList; buildConfigurations = (A00000000000000000000006 /* Release */,); defaultConfigurationName = Release; };
/* End XCConfigurationList section */
 };
 rootObject = A00000000000000000000002;
}
`,
      );
      const project = xcode.project(projectFile);
      project.parseSync();
      const before = structuredClone(
        project.pbxXCBuildConfigurationSection().A00000000000000000000006,
      );
      vi.stubEnv("JONES_IOS_E2E", "1");
      vi.stubEnv("T3CODE_MOBILE_UPDATES_ENABLED", "0");
      const config = plugin({
        name: "Fixture",
        slug: "fixture",
        ios: { bundleIdentifier: "com.example.fixture" },
      });
      const input = {
        ...config,
        modResults: project,
        modRequest: {
          projectRoot: root,
          platformProjectRoot: ios,
          platform: "ios",
          modName: "xcodeproj",
        },
      };
      await config.mods.ios.xcodeproj(input);
      const targets = project.pbxNativeTargetSection();
      const ui = Object.values(targets).find(
        (value: unknown) =>
          value !== null &&
          typeof value === "object" &&
          "name" in value &&
          value.name === '"JonesE2eUITests"',
      ) as { productType: string; dependencies: { value: string }[] };
      expect(ui.productType).toBe('"com.apple.product-type.bundle.ui-testing"');
      expect(targets.A00000000000000000000001.dependencies).toEqual([]);
      expect(
        project.hash.project.objects.PBXTargetDependency[ui.dependencies[0]!.value].target,
      ).toBe("A00000000000000000000001");
      expect(project.pbxXCBuildConfigurationSection().A00000000000000000000006).toEqual(before);
      const scheme = await NodeFSP.readFile(
        NodePath.join(ios, "Fixture.xcodeproj/xcshareddata/xcschemes/JonesE2eUITests.xcscheme"),
        "utf8",
      );
      expect(scheme).toContain('buildConfiguration="Release"');
      expect(scheme).toContain('parallelizable="NO"');
      expect(
        await NodeFSP.readFile(
          NodePath.join(ios, "JonesE2eUITests/PinWorkflowTests.swift"),
          "utf8",
        ),
      ).toBe(
        await NodeFSP.readFile(
          NodePath.join(mobileRoot, "e2e/xcuitest/PinWorkflowTests.swift"),
          "utf8",
        ),
      );
      await expect(config.mods.ios.xcodeproj(input)).rejects.toThrow("already exists");
      vi.stubEnv("JONES_IOS_E2E", "0");
      expect(() => plugin({})).toThrow("explicit opt-in");
    } finally {
      vi.unstubAllEnvs();
      await NodeFSP.rm(root, { recursive: true });
    }
    await expect(NodeFSP.stat(root)).rejects.toMatchObject({ code: "ENOENT" });
  });
});

describe("pure command failure diagnostics", () => {
  function fixture(failAt: number, errno = "EPERM", close = true, timedOut = true) {
    type Lifecycle = NonNullable<Parameters<typeof ownedCommand>[3]>;
    const child = Object.assign(new NodeEvents.EventEmitter(), {
      pid: 1234,
      stdout: new NodeEvents.EventEmitter(),
      stderr: new NodeEvents.EventEmitter(),
      stdin: Object.assign(new NodeEvents.EventEmitter(), { end: () => {} }),
    }) as ReturnType<Lifecycle["spawn"]>;
    const calls: (0 | NodeJS.Signals)[] = [];
    let clock = 0;
    let timeout!: () => void;
    const syscallError = Object.assign(new Error(`kill ${errno}`), { code: errno });
    const spawn = vi.fn(() => child);
    const clearTimer = vi.fn();
    const lifecycle: Lifecycle = {
      spawn,
      now: () => clock,
      wait: async (ms) => {
        clock += ms;
      },
      setTimer: (callback) => {
        timeout = callback;
        return 1 as unknown as ReturnType<typeof setTimeout>;
      },
      clearTimer,
      kill: (pid, signal) => {
        expect(pid).toBe(-1234);
        calls.push(signal);
        if (signal === "SIGTERM" && close) child.emit("close", null, "SIGTERM");
        if (calls.length === failAt) throw syscallError;
        return true;
      },
    };
    const command = ownedCommand(
      "/private/tool",
      ["private-argument"],
      {
        cwd: "/private/scratch",
        env: {},
        stage: "simulator-boot-readiness",
      },
      lifecycle,
    );
    const result = command.result.catch((error: unknown) => error as AggregateError);
    clock = 120_000;
    if (timedOut) timeout();
    if (close) child.emit("close", null, "SIGTERM");
    return { command, result, syscallError, calls, spawn, clearTimer };
  }
  it.each([
    [1, "term", "0"],
    [2, "term", "SIGTERM"],
    [3, "term-wait", "0"],
    [83, "term-wait", "0"],
    [84, "term-wait", "0"],
    [85, "kill", "0"],
    [86, "kill", "SIGKILL"],
    [87, "kill-wait", "0"],
  ] as const)(
    "keeps timeout primary and EPERM unknown at syscall %s",
    async (position, phase, signal) => {
      const f = fixture(position);
      const error = (await f.result) as AggregateError;
      expect(error).toBeInstanceOf(AggregateError);
      expect(error.errors[0].message).toBe(
        "Owned command timed out; operation effect requires readback",
      );
      expect(error.errors[1]).toBe(f.syscallError);
      expect(f.command.receipt.groupGone).not.toBe(true);
      expect(f.command.receipt.signal).toBe("SIGTERM");
      expect(f.command.receipt.diagnostic).toMatchObject({
        stage: "simulator-boot-readiness",
        timeoutMs: 120_000,
        deadlineAt: "1970-01-01T00:02:00.000Z",
        timedOutAt: "1970-01-01T00:02:00.000Z",
        cleanupError: { phase, syscall: "kill", signal, errno: "EPERM" },
      });
      expect(f.command.receipt.diagnostic?.closedAt).toBeDefined();
      expect(f.calls).toHaveLength(position);
      expect(f.spawn).toHaveBeenCalledTimes(1);
      expect(f.clearTimer).toHaveBeenCalledTimes(1);
      const diagnostic = JSON.stringify(f.command.receipt.diagnostic);
      expect(diagnostic.length).toBeLessThan(512);
      expect(diagnostic).not.toMatch(/private|argv|stdout|stderr/);
    },
  );
  it.each([1, 2, 3, 85, 86, 87])(
    "accepts ESRCH with close at syscall %s without losing timeout",
    async (position) => {
      const f = fixture(position, "ESRCH");
      const error = (await f.result) as AggregateError;
      expect(error.errors).toHaveLength(1);
      expect(error.message).toContain("timed out");
      expect(f.command.receipt.groupGone).toBe(true);
      expect(f.command.receipt.diagnostic?.cleanupError).toBeUndefined();
    },
  );
  it("keeps a close-only EPERM settlement unresolved without inventing a timeout", async () => {
    const f = fixture(1, "EPERM", true, false);
    const error = (await f.result) as AggregateError;
    expect(error.errors).toEqual([f.syscallError]);
    expect(f.command.receipt.groupGone).not.toBe(true);
    expect(f.command.receipt.diagnostic).toMatchObject({
      closedAt: "1970-01-01T00:02:00.000Z",
      cleanupError: { phase: "settlement", syscall: "kill", signal: "0", errno: "EPERM" },
    });
    expect(f.command.receipt.diagnostic?.timedOutAt).toBeUndefined();
  });
  it("requires close even after ESRCH", async () => {
    const f = fixture(1, "ESRCH", false);
    const error = (await f.result) as AggregateError;
    expect(error.errors).toHaveLength(2);
    expect(error.errors[1].message).toContain("exit unresolved");
    expect(f.command.receipt.diagnostic?.closedAt).toBeUndefined();
  });
  it("bounds unexpected errno without copying syscall messages", async () => {
    const f = fixture(3, "/private/secret");
    await f.result;
    expect(f.command.receipt.diagnostic?.cleanupError?.errno).toBe("UNKNOWN");
    expect(JSON.stringify(f.command.receipt.diagnostic)).not.toContain("private");
  });
});
