// @effect-diagnostics nodeBuiltinImport:off -- Real Knip subprocesses inspect disposable source fixtures only.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeModule from "node:module";
import * as NodeProcess from "node:process";
import * as NodeChildProcess from "node:child_process";
import * as NodeUtil from "node:util";
import { expect, it } from "vite-plus/test";
import checkedForeignBindings, { inspectContracts } from "./foreignBindings.ts";
import { sourcePins } from "./foreignDeclarations.ts";
import * as Identity from "../tsrs/identity.ts";

const root = NodePath.resolve(import.meta.dirname, "../../..");
const read = (file: string) => NodeFSP.readFile(NodePath.join(root, file), "utf8");
const syntheticCompiler = async () => ({
  target: "synthetic-test-only",
  artifactSHA256: "a".repeat(64),
});
const inspect = (
  file: string,
  replacement: (source: string) => string,
  groups: Parameters<typeof inspectContracts>[0],
) =>
  inspectContracts(
    groups,
    async (name) => (name === file ? replacement(await read(name)) : read(name)),
    syntheticCompiler,
  );

it("validates complete exact graphs and only invokes the compiler proof for its owning finding", async () => {
  let calls = 0;
  const compiler = async () => {
    calls++;
    return syntheticCompiler();
  };
  const proof = await inspectContracts(["gjs", "pgrep", "hyprctl"], read, compiler);
  expect(proof.sources).toHaveLength(13);
  expect(calls).toBe(0);
  await inspectContracts(["compiler"], read, compiler);
  expect(calls).toBe(1);
  await expect(
    inspectContracts(["compiler"], read, async () => {
      throw new Error("corrupt official artifact");
    }),
  ).rejects.toThrow("corrupt official artifact");
});

it.each([
  [
    "unknown GI URI",
    "apps/desktop/gnome-extension/extension.js",
    "gi://Gio",
    "gi://Unknown",
    "gjs",
  ],
  [
    "wrong full resource",
    "apps/desktop/gnome-extension/extension.js",
    "shell/ui/main.js",
    "shell/ui/unknown.js",
    "gjs",
  ],
  [
    "default plus named binding",
    "apps/desktop/gnome-extension/extension.js",
    "import Gio from",
    "import Gio, { Unknown } from",
    "gjs",
  ],
  [
    "export-from",
    "apps/desktop/gnome-extension/extension.js",
    'import Gio from "gi://Gio";',
    'export { default as Gio } from "gi://Gio";',
    "gjs",
  ],
  [
    "dynamic loader",
    "apps/desktop/gnome-extension/extension.js",
    'import Gio from "gi://Gio";',
    'const Gio = await import("gi://Gio");',
    "gjs",
  ],
  [
    "escaped graph",
    "apps/desktop/gnome-extension/extension.js",
    "./captureService.js",
    "../captureService.js",
    "gjs",
  ],
  [
    "wrong UUID",
    "apps/desktop/gnome-extension/metadata.json",
    "snap-shot@t3.codes",
    "another@fixture",
    "gjs",
  ],
  [
    "wrong bundle target",
    "apps/desktop/gnome-extension/bundle.json",
    "captureService.js",
    "missing.js",
    "gjs",
  ],
  [
    "changed pgrep parent",
    "apps/desktop/scripts/electron-launcher.test.mjs",
    "String(process.pid)",
    "String(1)",
    "pgrep",
  ],
  [
    "changed pgrep platform gate",
    "apps/desktop/scripts/electron-launcher.test.mjs",
    'NodeOS.platform() === "win32"',
    'NodeOS.platform() === "linux"',
    "pgrep",
  ],
  [
    "changed binary argv",
    "apps/desktop/src/app/DesktopPreReadyPlatform.ts",
    '["version", "-j"]',
    '["version"]',
    "hyprctl",
  ],
  [
    "changed binary limit",
    "apps/desktop/src/app/DesktopPreReadyPlatform.ts",
    "timeout: 1000",
    "timeout: 10000",
    "hyprctl",
  ],
  [
    "changed failure fallback",
    "apps/desktop/src/app/DesktopEarlyElectronStartup.ts",
    "return false;",
    "return true;",
    "hyprctl",
  ],
  [
    "JS compiler route",
    "apps/web/vite.config.ts",
    "node ../../scripts/jones/tsrs/typecheck.ts --noEmit",
    "tsc --noEmit",
    "compiler",
  ],
  [
    "task-cache command rewrite",
    "scripts/jones/tsrs/task-cache.ts",
    "    command,",
    "    command: 'tsc',",
    "compiler",
  ],
  [
    "wrong compiler namespace",
    "apps/web/tsconfig.json",
    "@effect/language-service",
    "unknown-plugin",
    "compiler",
  ],
] as const)("refuses %s", async (_label, file, from, to, group) => {
  const source = await read(file);
  expect(source).toContain(from);
  await expect(inspect(file, (value) => value.replace(from, to), [group])).rejects.toThrow(
    "Foreign contract mismatch",
  );
});

const issue = (file: string, symbol: string, type: "unlisted" | "binaries" = "unlisted") => ({
  type,
  filePath: NodePath.join(root, file),
  workspace: file.startsWith("apps/desktop/") ? "apps/desktop" : "apps/web",
  symbol,
  specifier: symbol,
  fixes: [],
});
const options = () => {
  const unlisted: Record<string, Record<string, ReturnType<typeof issue>>> = {
    "apps/desktop/gnome-extension/extension.js": {
      gi: issue("apps/desktop/gnome-extension/extension.js", "gi"),
    },
  };
  return { cwd: root, issues: { unlisted, binaries: {} }, counters: { unlisted: 1, binaries: 0 } };
};
it("validates real containing identity before exact mutation and preserves unrelated symbols", async () => {
  const value = options();
  const other = issue("apps/desktop/gnome-extension/extension.js", "unknown");
  value.issues.unlisted["apps/desktop/gnome-extension/extension.js"]!.unknown = other;
  value.counters.unlisted++;
  await checkedForeignBindings(value);
  expect(value.counters.unlisted).toBe(1);
  expect(value.issues.unlisted["apps/desktop/gnome-extension/extension.js"]).toEqual({
    unknown: other,
  });
});
it("no observed keys is a no-op with no unrelated filesystem/compiler prerequisite", async () => {
  const value = {
    cwd: "/does-not-exist",
    issues: { unlisted: {}, binaries: {} },
    counters: { unlisted: 0, binaries: 0 },
  };
  expect(await checkedForeignBindings(value)).toBe(value);
});
it("a same-named finding in another file stays a stock failure", async () => {
  const value = {
    cwd: root,
    issues: {
      unlisted: { "apps/desktop/other.js": { gi: issue("apps/desktop/other.js", "gi") } },
      binaries: {},
    },
    counters: { unlisted: 1, binaries: 0 },
  };
  const before = structuredClone(value);
  await checkedForeignBindings(value);
  expect(value).toEqual(before);
});
it.each(["workspace", "filePath", "type", "symbols", "specifier", "counter"])(
  "rejects %s metadata without partially clearing a valid companion",
  async (key) => {
    const value = options();
    const one = value.issues.unlisted["apps/desktop/gnome-extension/extension.js"]!;
    one.resource = issue("apps/desktop/gnome-extension/extension.js", "resource");
    value.counters.unlisted = 2;
    if (key === "counter") value.counters.unlisted = 0;
    else
      Object.assign(one.resource, {
        [key]: key === "symbols" ? [{ symbol: "resource" }, { symbol: "unknown" }] : "wrong",
      });
    const before = structuredClone(value);
    await expect(checkedForeignBindings(value)).rejects.toThrow();
    expect(value).toEqual(before);
  },
);

const execute = NodeUtil.promisify(NodeChildProcess.execFile);
const require = NodeModule.createRequire(import.meta.url);
const cli = NodePath.join(NodePath.dirname(require.resolve("knip")), "cli.js");
const preprocessor = NodePath.join(root, "scripts/knip-schemas.ts");
it("real Knip CLI classifies full-URI declarations while changed/unknown/unrelated findings fail", async () => {
  const directory = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.tmpdir(), "jones-foreign-analysis-"),
  );
  const write = async (file: string, source: string) => {
    const target = NodePath.join(directory, file);
    await NodeFSP.mkdir(NodePath.dirname(target), { recursive: true });
    await NodeFSP.writeFile(target, source);
  };
  const run = async () => {
    try {
      const result = await execute(
        NodeProcess.execPath,
        [
          cli,
          "--directory",
          directory,
          "--config",
          NodePath.join(directory, "knip.json"),
          "--preprocessor",
          preprocessor,
          "--no-config-hints",
          "--reporter",
          "json",
        ],
        { maxBuffer: 2 * 1024 * 1024 },
      );
      return { code: 0, ...result };
    } catch (cause) {
      if (
        !cause ||
        typeof cause !== "object" ||
        !("code" in cause) ||
        !("stdout" in cause) ||
        !("stderr" in cause)
      )
        throw cause;
      return { code: cause.code, stdout: String(cause.stdout), stderr: String(cause.stderr) };
    }
  };
  try {
    await write("package.json", JSON.stringify({ private: true, workspaces: ["apps/desktop"] }));
    await write(
      "apps/desktop/package.json",
      JSON.stringify({ name: "foreign-fixture", type: "module", private: true }),
    );
    await write(
      "knip.json",
      JSON.stringify({
        workspaces: {
          ".": { project: [], entry: [] },
          "apps/desktop": {
            entry: ["gnome-extension/extension.js", "contract-consumer.js"],
            project: ["gnome-extension/*.js", "extra.js"],
          },
        },
      }),
    );
    for (const file of Object.keys(sourcePins.gjs)) await write(file, await read(file));
    await write(
      "apps/desktop/contract-consumer.js",
      'import { CLIENT_NAMES } from "./gnome-extension/captureService.js"; console.log(CLIENT_NAMES);',
    );
    const passed = await run();
    expect(passed.code, passed.stderr + passed.stdout).toBe(0);
    expect(passed.stderr).toContain("jones.checked-foreign-declarations/v1");
    const original = await read("apps/desktop/gnome-extension/extension.js");
    await write(
      "apps/desktop/gnome-extension/extension.js",
      original.replace("gi://Gio", "gi://Unknown"),
    );
    const changed = await run();
    expect(changed.code).not.toBe(0);
    expect(changed.stderr).toContain("Foreign contract mismatch");
    await write("apps/desktop/gnome-extension/extension.js", original + '\nimport "../extra.js";');
    await write(
      "apps/desktop/extra.js",
      'import X from "missing-runtime-dependency"; console.log(X);',
    );
    expect((await run()).code).not.toBe(0);
    await write("apps/desktop/gnome-extension/extension.js", original);
    await write("apps/desktop/extra.js", "export const orphan = 1;");
    const unrelated = await run();
    expect(unrelated.code).not.toBe(0);
    expect(unrelated.stdout).toContain("extra.js");
  } finally {
    await NodeFSP.rm(directory, { recursive: true });
  }
});

it("real compiler namespace CLI uses private genuine artifact identity and refuses changed routes/corrupt bytes", async () => {
  const target = Identity.hostTarget();
  if (!target) {
    const value = {
      cwd: root,
      issues: {
        unlisted: {
          "apps/web/tsconfig.json": {
            "@effect/language-service": issue("apps/web/tsconfig.json", "@effect/language-service"),
          },
        },
        binaries: {},
      },
      counters: { unlisted: 1, binaries: 0 },
    };
    await expect(checkedForeignBindings(value)).rejects.toThrow("supported official TSRS");
    return;
  }
  const installed = await Identity.verified(await Identity.resolveInstalled(root, target));
  const directory = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.tmpdir(), "jones-foreign-compiler-"),
  );
  const write = async (file: string, source: string) => {
    const name = NodePath.join(directory, file);
    await NodeFSP.mkdir(NodePath.dirname(name), { recursive: true });
    await NodeFSP.writeFile(name, source);
  };
  const run = async () => {
    try {
      const result = await execute(
        NodeProcess.execPath,
        [
          cli,
          "--directory",
          directory,
          "--config",
          NodePath.join(directory, "knip.json"),
          "--preprocessor",
          preprocessor,
          "--workspace",
          "apps/web",
          "--no-config-hints",
          "--reporter",
          "json",
        ],
        { maxBuffer: 2 * 1024 * 1024 },
      );
      return { code: 0, ...result };
    } catch (cause) {
      if (
        !cause ||
        typeof cause !== "object" ||
        !("code" in cause) ||
        !("stdout" in cause) ||
        !("stderr" in cause)
      )
        throw cause;
      return { code: cause.code, stdout: String(cause.stdout), stderr: String(cause.stderr) };
    }
  };
  try {
    await write("package.json", JSON.stringify({ private: true, workspaces: ["apps/web"] }));
    await write(
      "apps/web/package.json",
      JSON.stringify({
        name: "compiler-fixture",
        private: true,
        type: "module",
        devDependencies: { typescript: "*" },
      }),
    );
    await write(
      "knip.json",
      JSON.stringify({
        workspaces: {
          ".": { entry: [], project: [], typescript: false },
          "apps/web": { entry: [], project: ["*.ts"], typescript: { config: ["tsconfig.json"] } },
        },
      }),
    );
    for (const file of Object.keys(sourcePins.compiler)) await write(file, await read(file));
    const store = "node_modules/.pnpm/foreign-compiler-fixture/node_modules/";
    await write(store + "tsrs/package.json", await NodeFSP.readFile(installed.wrapper, "utf8"));
    await write(
      store + "@tsrs/" + target + "/package.json",
      await NodeFSP.readFile(installed.nativeManifest, "utf8"),
    );
    const executable = NodePath.join(directory, store, "@tsrs", target, "tsrs");
    for (const suffix of ["", ".original"]) {
      await NodeFSP.copyFile(installed.executable + suffix, executable + suffix);
      await NodeFSP.chmod(executable + suffix, 0o755);
    }
    await NodeFSP.symlink(
      NodePath.join(".pnpm/foreign-compiler-fixture/node_modules/tsrs"),
      NodePath.join(directory, "node_modules/tsrs"),
    );
    // TypeScript is a genuine existing fixture dependency, not the compiler namespace under test.
    await NodeFSP.symlink(
      NodePath.dirname(require.resolve("typescript/package.json")),
      NodePath.join(directory, "node_modules/typescript"),
    );
    const valid = await run();
    // This fixture deliberately retains an unreferenced real Vite source as contract evidence.
    // Its unrelated file finding must survive successful native-namespace classification.
    expect(valid.code, valid.stderr + valid.stdout).toBe(1);
    const remaining = JSON.parse(valid.stdout).issues;
    expect(remaining).toHaveLength(1);
    const { file, files, ...otherCategories } = remaining[0];
    expect(file).toBe("apps/web/vite.config.ts");
    expect(files).toEqual([{ name: "apps/web/vite.config.ts" }]);
    expect(
      Object.values(otherCategories).every((value) => Array.isArray(value) && value.length === 0),
    ).toBe(true);
    expect(valid.stderr).toContain(Identity.TARGETS[target].patched);
    const vite = "apps/web/vite.config.ts",
      original = await read(vite);
    await write(
      vite,
      original.replace("node ../../scripts/jones/tsrs/typecheck.ts --noEmit", "tsc --noEmit"),
    );
    const changed = await run();
    expect(changed.code).not.toBe(0);
    expect(changed.stderr).toContain("Foreign contract mismatch");
    await write(vite, original);
    await NodeFSP.writeFile(executable, "synthetic corrupt private compiler");
    const corrupt = await run();
    expect(corrupt.code).not.toBe(0);
    expect(corrupt.stderr).toContain("not verified");
  } finally {
    await NodeFSP.rm(directory, { recursive: true });
  }
});
