import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { afterEach, describe, expect, it } from "vite-plus/test";
import { parseArguments, resolveCompiler, selectProjects, VERSION } from "./runner.mjs";

const roots = [];
const children = [];
const runner = new URL("./runner.mjs", import.meta.url).href;
const repo = NodeURL.fileURLToPath(new URL("../../../", import.meta.url));
function write(root, name, content) {
  const target = NodePath.join(root, name);
  NodeFS.mkdirSync(NodePath.dirname(target), { recursive: true });
  NodeFS.writeFileSync(target, content);
}
function fixture(versionMode = "success") {
  const root = NodeFS.mkdtempSync(NodePath.join(repo, "node_modules", ".tsrs-test-"));
  roots.push(root);
  write(root, "package.json", JSON.stringify({ name: "fixture", private: true, type: "module" }));
  write(root, "pnpm-workspace.yaml", "packages:\n  - packages/*\n");
  for (const name of ["good", "bad", "wait", "signal", "astro"]) {
    write(
      root,
      `packages/${name}/package.json`,
      JSON.stringify({
        name: `@fixture/${name}`,
        scripts: { typecheck: name === "astro" ? "astro check" : "tsc-rs --noEmit" },
      }),
    );
    write(
      root,
      `packages/${name}/tsconfig.json`,
      JSON.stringify({ compilerOptions: { composite: true, strict: true } }),
    );
    write(root, `packages/${name}/tsconfig.tsbuildinfo`, "existing-build-info");
  }
  write(
    root,
    "node_modules/tsrs/package.json",
    JSON.stringify({
      name: "tsrs",
      version: VERSION,
      type: "module",
      tsrs: { typescriptVersion: "7.1.0-dev.20260929" },
      optionalDependencies: { "@tsrs/test-native": VERSION },
    }),
  );
  write(
    root,
    "node_modules/tsrs/lib/platform.js",
    'export const getPlatformPackageName = (os) => os === "unsupported" ? "@tsrs/unsupported" : "@tsrs/test-native";',
  );
  write(
    root,
    "node_modules/@tsrs/test-native/package.json",
    JSON.stringify({ name: "@tsrs/test-native", version: VERSION }),
  );
  write(
    root,
    "node_modules/@tsrs/test-native/tsrs",
    `#!/usr/bin/env node
const fs = require("node:fs");
const NodePath = require("node:path");
const args = process.argv.slice(2);
if (args.length === 1 && args[0] === "--version") {
  fs.appendFileSync(NodePath.join(__dirname, "version-queries"), "query\\n");
  console.log("Version 7.1.0-dev.20260930.4");
  if (${JSON.stringify(versionMode)} === "failure") {
    console.error("version query failed");
    process.exit(7);
  }
  if (${JSON.stringify(versionMode)} === "signal") process.kill(process.pid, "SIGTERM");
  else process.exit(0);
}
const name = NodePath.basename(process.cwd());
const config = JSON.parse(fs.readFileSync(args[args.indexOf("--project") + 1], "utf8"));
fs.writeFileSync(args[args.indexOf("--tsBuildInfoFile") + 1], "owned-info");
console.log("OBSERVATION " + JSON.stringify({ name, cwd: process.cwd(), args, config }));
if (name === "wait") {
  process.on("SIGTERM", () => { console.log("FORWARDED SIGTERM"); process.exit(0); });
  process.on("SIGINT", () => { console.log("FORWARDED SIGINT"); process.exit(0); });
  console.log("READY");
  setInterval(() => {}, 1000);
} else if (name === "signal") {
  process.kill(process.pid, "SIGTERM");
} else if (name === "bad") {
  console.error("fixture.ts(1,1): error TS2322: Type 'string' is not assignable to type 'number'.");
  process.exitCode = 2;
}
`,
  );
  NodeFS.chmodSync(NodePath.join(root, "node_modules/@tsrs/test-native/tsrs"), 0o755);
  return root;
}
function launch(root, args) {
  const source = `import { runExperimental, finish } from ${JSON.stringify(runner)}; finish(await runExperimental(${JSON.stringify(root)}, ${JSON.stringify(args)}));`;
  const env = { ...process.env };
  delete env.TSRS_BINARY;
  const child = NodeChildProcess.spawn(process.execPath, ["--input-type=module", "-e", source], {
    env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  children.push(child);
  let output = "";
  child.stdout.on("data", (chunk) => {
    output += chunk;
  });
  child.stderr.on("data", (chunk) => {
    output += chunk;
  });
  const completed = NodeEvents.once(child, "close").then(([code, signal]) => ({
    code,
    signal,
    output,
  }));
  return { child, completed, output: () => output };
}
function expectClean(root) {
  expect(
    NodeFS.readdirSync(NodePath.join(root, "node_modules")).filter((name) =>
      name.startsWith(".tsrs-experimental-"),
    ),
  ).toEqual([]);
  expect(
    NodeFS.readFileSync(NodePath.join(root, "packages/good/tsconfig.tsbuildinfo"), "utf8"),
  ).toBe("existing-build-info");
}
afterEach(async () => {
  for (const child of children.splice(0)) {
    if (child.exitCode === null && child.signalCode === null) {
      const closed = NodeEvents.once(child, "close");
      child.kill("SIGTERM");
      await closed;
    }
  }
  for (const root of roots.splice(0)) NodeFS.rmSync(root, { recursive: true });
});

describe("experimental typecheck", () => {
  it("requires explicit projects and rejects compiler or malformed checker arguments", () => {
    for (const args of [
      [],
      [""],
      ["--noEmit", "good"],
      ["good", "--", "--skipLibCheck"],
      ["good", "--checkers", "0"],
      ["good", "--checkers", "1.5"],
      ["good", "--checkers"],
      ["good", "--checkers", "2", "--checkers", "3"],
    ]) {
      expect(() => parseArguments(args)).toThrow();
    }
    expect(parseArguments(["packages/good"])).toEqual({ projects: ["packages/good"], checkers: 2 });
    expect(parseArguments(["@fixture/good", "--checkers", "3"])).toEqual({
      projects: ["@fixture/good"],
      checkers: 3,
    });
  });

  it("selects discovered package names and paths, deduplicates, and rejects unknown or unsupported scripts", () => {
    const root = fixture();
    const inventory = ["good", "astro"].map((name) => ({
      name: `@fixture/${name}`,
      path: NodePath.join(root, "packages", name),
    }));
    expect(selectProjects(root, inventory, ["@fixture/good", "./packages/good/"])).toHaveLength(1);
    expect(() => selectProjects(root, inventory, ["missing"])).toThrow("Unknown");
    expect(() => selectProjects(root, inventory, ["@fixture/astro"])).toThrow(
      "Astro is outside coverage",
    );
    for (const script of [
      "tsc-rs --noEmit --skipLibCheck",
      "tsc-rs --noEmit && echo done",
      "tsc --noEmit",
      "tsc-rs --noEmit -p other.json",
    ]) {
      write(root, "packages/good/package.json", JSON.stringify({ scripts: { typecheck: script } }));
      expect(() => selectProjects(root, inventory, ["@fixture/good"])).toThrow(
        "Unsupported typecheck script",
      );
    }
  });

  it("rejects unsupported platforms and absent native binaries without fallback", async () => {
    const root = fixture();
    await expect(resolveCompiler(root, "unsupported", "x64")).rejects.toThrow(
      "Unsupported tsrs platform",
    );
    NodeFS.rmSync(NodePath.join(root, "node_modules/@tsrs/test-native/tsrs"));
    await expect(resolveCompiler(root)).rejects.toThrow();
    NodeFS.rmSync(NodePath.join(root, "node_modules/tsrs"), { recursive: true });
    const result = await launch(root, ["packages/good"]).completed;
    expect(result.code).toBe(1);
    expect(result.output).toContain("Missing local tsrs");
    expect(result.output).toContain("omits configured Effect diagnostics");
    expectClean(root);
  });

  it("rejects mismatched package and native versions", async () => {
    const root = fixture();
    const manifestPath = NodePath.join(root, "node_modules/tsrs/package.json");
    const manifest = JSON.parse(NodeFS.readFileSync(manifestPath, "utf8"));
    NodeFS.writeFileSync(manifestPath, JSON.stringify({ ...manifest, version: "0.0.0" }));
    await expect(resolveCompiler(root)).rejects.toThrow(`Expected tsrs ${VERSION}`);
    NodeFS.writeFileSync(manifestPath, JSON.stringify(manifest));
    write(
      root,
      "node_modules/@tsrs/test-native/package.json",
      JSON.stringify({ version: "0.0.0" }),
    );
    await expect(resolveCompiler(root)).rejects.toThrow(`Expected @tsrs/test-native ${VERSION}`);
  });

  it("propagates a compiler signal and cleans its scratch before exiting", async () => {
    const root = fixture();
    const result = await launch(root, ["packages/signal", "packages/good"]).completed;
    expect(result.signal).toBe("SIGTERM");
    expect(result.output).not.toContain('"name":"good"');
    expectClean(root);
  });

  it("forwards existing cwd/config, displays versions and flags, and cleans successful build-info", async () => {
    const root = fixture();
    const result = await launch(root, ["@fixture/good", "--checkers", "3"]).completed;
    expect(result.code).toBe(0);
    expect(result.output).toContain(
      `tsrs package ${VERSION}; TypeScript package metadata 7.1.0-dev.20260929`,
    );
    expect(result.output).toContain("checkers=3");
    const observation = JSON.parse(
      result.output
        .split("\n")
        .find((line) => line.startsWith("OBSERVATION "))
        .slice(12),
    );
    expect(observation.cwd).toBe(NodePath.join(root, "packages/good"));
    expect(observation.config.compilerOptions).toEqual({ composite: true, strict: true });
    expect(observation.args.slice(0, 6)).toEqual([
      "--project",
      "tsconfig.json",
      "--noEmit",
      "--checkers",
      "3",
      "--incremental",
    ]);
    expectClean(root);
  });

  it("displays the reported native version separately from metadata and queries once for multiple projects", async () => {
    const root = fixture();
    const result = await launch(root, ["packages/good", "packages/bad"]).completed;
    expect(result.output).toContain(
      `tsrs package ${VERSION}; TypeScript package metadata 7.1.0-dev.20260929`,
    );
    expect(result.output).toContain("Native compiler (--version):\nVersion 7.1.0-dev.20260930.4\n");
    expect(
      NodeFS.readFileSync(
        NodePath.join(root, "node_modules/@tsrs/test-native/version-queries"),
        "utf8",
      ),
    ).toBe("query\n");
    expect(result.code).toBe(2);
    expectClean(root);
  });

  it.each(["failure", "signal"])(
    "preserves a version query %s and does not start compilation",
    async (mode) => {
      const root = fixture(mode);
      const result = await launch(root, ["packages/good"]).completed;
      if (mode === "failure") {
        expect(result.code).toBe(7);
        expect(result.output).toContain("version query failed");
      } else {
        expect(result.signal).toBe("SIGTERM");
      }
      expect(result.output).toContain("Version 7.1.0-dev.20260930.4");
      expect(result.output).not.toContain("OBSERVATION");
      expectClean(root);
    },
  );

  it("preserves diagnostics and the first nonzero status while continuing selected projects serially", async () => {
    const root = fixture();
    const result = await launch(root, ["packages/bad", "packages/good"]).completed;
    expect(result.code).toBe(2);
    expect(result.output).toContain("error TS2322");
    const observations = result.output
      .split("\n")
      .filter((line) => line.startsWith("OBSERVATION "))
      .map((line) => JSON.parse(line.slice(12)));
    expect(observations.map((entry) => entry.name)).toEqual(["bad", "good"]);
    expectClean(root);
  });

  it.each(["SIGTERM", "SIGINT"])(
    "forwards %s, cleans scratch, preserves cancellation, and does not start the next project",
    async (signal) => {
      const root = fixture();
      const running = launch(root, ["packages/wait", "packages/good"]);
      await new Promise((resolve, reject) => {
        running.child.once("close", () => reject(new Error("Runner exited before READY")));
        running.child.stdout.on("data", () => {
          if (running.output().includes("READY")) resolve();
        });
      });
      expect(
        NodeFS.readdirSync(NodePath.join(root, "node_modules")).some((name) =>
          name.startsWith(".tsrs-experimental-"),
        ),
      ).toBe(true);
      running.child.kill(signal);
      const result = await running.completed;
      expect(result.signal).toBe(signal);
      expect(result.output).toContain(`FORWARDED ${signal}`);
      expect(result.output).not.toContain('"name":"good"');
      expectClean(root);
    },
  );

  it("warns even for help, empty selections, and rejected arguments", async () => {
    const root = fixture();
    for (const args of [["--help"], [], ["packages/good", "--skipLibCheck"]]) {
      const result = await launch(root, args).completed;
      expect(result.output).toContain("normal authoritative typecheck");
      expect(result.output).not.toContain("OBSERVATION");
      expect(result.code).toBe(args[0] === "--help" ? 0 : 1);
    }
    expect(NodeFS.existsSync(NodePath.join(root, "packages/good/tsconfig.tsbuildinfo"))).toBe(true);
    expectClean(root);
  });
});
