import * as HostProcess from "@t3tools/shared/HostProcess";

import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";
import * as NodeTest from "node:test";

import {
  RAW_LIMIT,
  ENVELOPE_LIMIT,
  NAMESPACE_LIMIT,
  admitNamespace,
  cachePaths,
  checkProject,
  configurationInputs,
  digest,
  discoverWorkspaces,
  effectiveConfig,
  encodeEnvelope,
  parseArguments,
  projectIdentity,
  readSeed,
  resolveCompiler,
  runCli,
  runProcess,
  selectWorkspaces,
  sharedIdentity,
  validNativePayload,
} from "./incrementalTypecheck.ts";

const repository = NodePath.resolve(import.meta.dirname, "../../..");
const scratchParent =
  process.env.JONES_INCREMENTAL_TEST_ROOT ??
  NodePath.join(NodeOS.homedir(), ".cache", "jones-code", "tests");
const payload = (version = "source-v1") =>
  Buffer.from(
    JSON.stringify({
      version: "7.1.0-dev+effect-tsgo.0.51.1",
      fileNames: ["../../source.ts"],
      fileInfos: [version],
      root: [1],
      options: {},
      effect: {},
    }),
  );
const signal = () => new AbortController().signal;

function write(filename, bytes) {
  NodeFS.mkdirSync(NodePath.dirname(filename), { recursive: true });
  NodeFS.writeFileSync(filename, bytes);
}

function fixture(t) {
  NodeFS.mkdirSync(scratchParent, { recursive: true });
  const root = NodeFS.realpathSync(
    NodeFS.mkdtempSync(NodePath.join(scratchParent, "incremental-")),
  );
  t.after(() => NodeFS.rmSync(root, { recursive: true, force: true }));
  write(
    NodePath.join(root, "package.json"),
    JSON.stringify({ name: "root", type: "module", scripts: { typecheck: "vp run -r typecheck" } }),
  );
  write(NodePath.join(root, "pnpm-lock.yaml"), "lockfileVersion: 9.0\n");
  write(NodePath.join(root, "pnpm-workspace.yaml"), "packages:\n  - packages/*\n");
  NodeFS.mkdirSync(NodePath.join(root, "node_modules", ".pnpm"), { recursive: true });
  const projects = ["a", "b"].map((name) => {
    const directory = NodePath.join(root, "packages", name);
    const manifest = {
      name: `@fixture/${name}`,
      type: "module",
      scripts: { typecheck: "tsc-rs --noEmit" },
    };
    write(NodePath.join(directory, "package.json"), JSON.stringify(manifest));
    write(
      NodePath.join(directory, "tsconfig.json"),
      JSON.stringify({ compilerOptions: { strict: true, noEmit: true }, files: ["source.ts"] }),
    );
    write(NodePath.join(directory, "source.ts"), "export const answer: number = 42;\n");
    return { name: manifest.name, directory, script: manifest.scripts.typecheck };
  });
  const wrapper = NodePath.join(root, "node_modules", "tsc-rs", "package.json");
  const nativeManifest = NodePath.join(
    NodePath.dirname(wrapper),
    "node_modules",
    "@tsc-rs",
    `${HostProcess.Platform.defaultValue()}-${HostProcess.Architecture.defaultValue()}`,
    "package.json",
  );
  const executable = NodePath.join(
    NodePath.dirname(nativeManifest),
    "lib",
    HostProcess.Platform.defaultValue() === "win32" ? "tsc.exe" : "tsc",
  );
  write(wrapper, JSON.stringify({ name: "tsc-rs", version: "0.7.0" }));
  write(nativeManifest, JSON.stringify({ version: "0.7.0" }));
  write(executable, "fixture executable, never executed");
  NodeFS.chmodSync(executable, 0o700);
  write(
    NodePath.join(NodePath.dirname(executable), "lib.d.ts"),
    "declare const fixture: number;\n",
  );
  const compiler = { executable, wrapper, nativeManifest };
  const workspaces = [
    { name: "root", directory: root, script: "vp run -r typecheck" },
    ...projects,
  ];
  const logs = [];
  const options = {
    root,
    project: projects[0],
    compiler,
    identity: "identity-one",
    fresh: false,
    signal: signal(),
    inputsUnchanged: async () => true,
    log: (line) => logs.push(line),
  };
  return { root, projects, workspaces, compiler, logs, options };
}

function nativePath(command) {
  const index = command.args.indexOf("--tsBuildInfoFile");
  NodeAssert.notEqual(index, -1);
  NodeAssert.equal(command.capture, false);
  NodeAssert.deepEqual(command.args.slice(0, index), [
    "--noEmit",
    "--incremental",
    "--checkers",
    "2",
  ]);
  return command.args[index + 1];
}

function compilerSeam(f, check) {
  return async (command) => {
    if (command.args.includes("--recursive"))
      return {
        code: 0,
        stdout: JSON.stringify(f.workspaces.map((workspace) => ({ path: workspace.directory }))),
      };
    if (command.args.includes("--version")) return { code: 0, stdout: "Version 7.1.0-dev\n" };
    if (command.args.includes("--showConfig"))
      return {
        code: 0,
        stdout: NodeFS.readFileSync(NodePath.join(command.cwd, "tsconfig.json"), "utf8"),
      };
    return check(command);
  };
}

function assertOnlyEntry(f) {
  NodeAssert.deepEqual(NodeFS.readdirSync(cachePaths(f.root, f.projects[0]).directory), [
    "entry.json",
  ]);
}

NodeTest.test(
  "explicit selection, stable order, deduplication, ambiguous names and unsupported flags/scripts",
  (t) => {
    const f = fixture(t);
    NodeAssert.throws(() => parseArguments([]), /Explicit/);
    for (const args of [
      ["--watch"],
      ["--all", "--project"],
      ["--all", "packages/a"],
      ["--fresh", "--fresh"],
      ["--", "--all"],
      [f.root],
    ]) {
      NodeAssert.throws(() => parseArguments(args));
    }
    NodeAssert.equal(parseArguments(["--help"]).help, true);
    const picked = selectWorkspaces(
      f.root,
      f.workspaces,
      parseArguments(["packages/b", "@fixture/a", "./packages/a", "--fresh"]),
    );
    NodeAssert.deepEqual(picked.selected, f.projects);
    NodeAssert.equal(picked.excluded[0].name, "root");
    NodeAssert.deepEqual(
      selectWorkspaces(f.root, f.workspaces, parseArguments(["--all"])).selected,
      f.projects,
    );
    for (const selector of ["root", "..", "missing"])
      NodeAssert.throws(() => selectWorkspaces(f.root, f.workspaces, parseArguments([selector])));
    const duplicate = { ...f.projects[1], name: f.projects[0].name };
    NodeAssert.throws(
      () => selectWorkspaces(f.root, [f.projects[0], duplicate], parseArguments([duplicate.name])),
      /Ambiguous/,
    );
    for (const script of [
      "astro check",
      "tsc-rs --noEmit ",
      "tsc-rs --noEmit && echo done",
      undefined,
    ]) {
      NodeAssert.throws(
        () =>
          selectWorkspaces(f.root, [{ ...f.projects[0], script }], parseArguments(["packages/a"])),
        /Unsupported/,
      );
    }
  },
);

NodeTest.test(
  "discovery uses installed workspace-manager inventory and real manifests, rejects escaping entries",
  async (t) => {
    const f = fixture(t);
    const result = await discoverWorkspaces(
      f.root,
      signal(),
      compilerSeam(f, () => NodeAssert.fail("No compiler expected")),
    );
    NodeAssert.deepEqual(result, f.workspaces);
    await NodeAssert.rejects(
      discoverWorkspaces(f.root, signal(), async () => ({ code: 1, stdout: "discovery failure" })),
      /discovery failed/,
    );
    await NodeAssert.rejects(
      discoverWorkspaces(f.root, signal(), async () => ({
        code: 0,
        stdout: JSON.stringify([{ path: NodePath.dirname(f.root) }]),
      })),
      /escapes/,
    );
  },
);

NodeTest.test(
  "complete envelope validation rejects corruption, mismatches, oversize and obviously malformed native JSON",
  (t) => {
    const f = fixture(t);
    const entry = NodePath.join(f.root, "entry.json");
    const bytes = payload();
    write(entry, encodeEnvelope("id", bytes));
    NodeAssert.deepEqual(readSeed(entry, "id"), bytes);
    NodeAssert.equal(readSeed(entry, "other"), undefined);
    const good = JSON.parse(NodeFS.readFileSync(entry));
    for (const value of [
      "{",
      "null",
      JSON.stringify({ ...good, checksum: "invalid" }),
      JSON.stringify({ ...good, schema: "other" }),
      JSON.stringify({ ...good, payload: "e30=" }),
      JSON.stringify({ ...good, payload: `${good.payload}\n` }),
    ]) {
      write(entry, value);
      NodeAssert.equal(readSeed(entry, "id"), undefined);
    }
    write(entry, "");
    NodeFS.truncateSync(entry, ENVELOPE_LIMIT + 1);
    NodeAssert.equal(readSeed(entry, "id"), undefined);
    NodeAssert.equal(readSeed(NodePath.join(f.root, "missing"), "id"), undefined);
    const native = JSON.parse(bytes);
    for (const value of [
      {},
      { ...native, version: "wrong" },
      { ...native, fileNames: [null] },
      { ...native, fileInfos: [] },
      { ...native, root: [2] },
      { ...native, root: [[1, 0]] },
      { ...native, effect: null },
    ]) {
      const invalid = Buffer.from(JSON.stringify(value));
      NodeAssert.equal(validNativePayload(invalid), false);
      write(
        entry,
        JSON.stringify({ ...good, checksum: digest(invalid), payload: invalid.toString("base64") }),
      );
      NodeAssert.equal(readSeed(entry, "id"), undefined);
    }
    NodeAssert.equal(validNativePayload(Buffer.alloc(RAW_LIMIT + 1)), false);
  },
);

NodeTest.test(
  "identity covers native configuration/Effect settings, worktree, compiler, libraries, workspace and installed dependency metadata, not source/HEAD",
  async (t) => {
    const f = fixture(t);
    const shared = () => sharedIdentity(f.root, f.workspaces, f.compiler);
    const baseline = shared();
    write(NodePath.join(f.projects[0].directory, "source.ts"), "export const changed = true;\n");
    write(NodePath.join(f.root, ".git", "HEAD"), "another-commit");
    NodeAssert.equal(shared(), baseline);
    const inputs = [
      "pnpm-lock.yaml",
      "pnpm-workspace.yaml",
      "package.json",
      "packages/a/package.json",
      "node_modules/.modules.yaml",
      "node_modules/.pnpm-workspace-state-v1.json",
      "node_modules/.pnpm/lock.yaml",
      NodePath.relative(f.root, f.compiler.executable),
      NodePath.relative(f.root, f.compiler.wrapper),
      NodePath.relative(f.root, f.compiler.nativeManifest),
      NodePath.relative(f.root, NodePath.join(NodePath.dirname(f.compiler.executable), "lib.d.ts")),
      "node_modules/example/package.json",
      "node_modules/.pnpm/example@1/node_modules/example/package.json",
    ];
    for (const input of inputs) {
      const beforeIdentity = shared();
      const filename = NodePath.join(f.root, input);
      const before = NodeFS.existsSync(filename) ? NodeFS.readFileSync(filename) : undefined;
      write(filename, "changed metadata");
      NodeAssert.notEqual(shared(), beforeIdentity, input);
      if (before) write(filename, before);
      else NodeFS.rmSync(filename);
    }
    const config = {
      compilerOptions: {
        plugins: [
          { name: "@effect/language-service", diagnosticSeverity: { globalDate: "error" } },
        ],
      },
    };
    const id = projectIdentity(f.root, f.projects[0], baseline, config);
    NodeAssert.notEqual(id, projectIdentity(f.root, f.projects[1], baseline, config));
    NodeAssert.notEqual(id, projectIdentity(`${f.root}-other`, f.projects[0], baseline, config));
    config.compilerOptions.plugins[0].diagnosticSeverity.globalDate = "off";
    NodeAssert.notEqual(id, projectIdentity(f.root, f.projects[0], baseline, config));
    let observed;
    NodeAssert.deepEqual(
      await effectiveConfig(f.compiler, f.projects[0], signal(), async (command) => {
        observed = command;
        return { code: 0, stdout: JSON.stringify(config) };
      }),
      config,
    );
    NodeAssert.ok(observed.args.includes("--showConfig"));
    NodeAssert.ok(!observed.args.includes("--tsBuildInfoFile"));
  },
);

NodeTest.test(
  "success publishes, reuse always executes, failure preserves previous seed, fresh bypasses and cleans",
  async (t) => {
    const f = fixture(t);
    let calls = 0;
    const run = async (command) => {
      const filename = nativePath(command);
      NodeAssert.equal(NodePath.dirname(filename), cachePaths(f.root, f.projects[0]).directory);
      const old = NodeFS.readFileSync(filename);
      NodeAssert.equal(old.length > 0, calls === 1 || calls === 2);
      calls++;
      NodeFS.writeFileSync(filename, payload(`version-${calls}`));
      return { code: calls === 3 ? 7 : 0, stdout: "" };
    };
    NodeAssert.equal(await checkProject({ ...f.options, run }), 0);
    assertOnlyEntry(f);
    NodeAssert.equal(await checkProject({ ...f.options, run }), 0);
    const entry = cachePaths(f.root, f.projects[0]).entry;
    const previous = NodeFS.readFileSync(entry);
    NodeAssert.equal(await checkProject({ ...f.options, run }), 7);
    NodeAssert.deepEqual(NodeFS.readFileSync(entry), previous);
    NodeAssert.equal(await checkProject({ ...f.options, fresh: true, run }), 0);
    NodeAssert.deepEqual(NodeFS.readFileSync(entry), previous);
    NodeAssert.equal(calls, 4);
    assertOnlyEntry(f);
  },
);

NodeTest.test(
  "malformed/truncated/digest-invalid/oversize seeds are misses followed by real checking",
  async (t) => {
    const f = fixture(t);
    const { entry } = cachePaths(f.root, f.projects[0]);
    const invalids = [
      "{",
      "{}",
      JSON.stringify({
        ...JSON.parse(encodeEnvelope(f.options.identity, payload())),
        checksum: "bad",
      }),
    ];
    for (const invalid of invalids) {
      write(entry, invalid);
      let called = false;
      NodeAssert.equal(
        await checkProject({
          ...f.options,
          run: async (command) => {
            called = true;
            const filename = nativePath(command);
            NodeAssert.equal(NodeFS.readFileSync(filename).length, 0);
            NodeFS.writeFileSync(filename, payload());
            return { code: 0, stdout: "" };
          },
        }),
        0,
      );
      NodeAssert.equal(called, true);
    }
    NodeFS.truncateSync(entry, ENVELOPE_LIMIT + 1);
    NodeAssert.equal(
      await checkProject({
        ...f.options,
        run: async (command) => {
          NodeAssert.equal(NodeFS.statSync(nativePath(command)).size, 0);
          return { code: 2, stdout: "" };
        },
      }),
      2,
    );
    NodeAssert.equal(NodeFS.statSync(entry).size, ENVELOPE_LIMIT + 1);
    assertOnlyEntry(f);
  },
);

NodeTest.test(
  "changed inputs and publication failure preserve native success only with explicit warnings",
  async (t) => {
    const f = fixture(t);
    const run = async (command) => {
      write(nativePath(command), payload());
      return { code: 0, stdout: "" };
    };
    await checkProject({ ...f.options, run });
    const { entry } = cachePaths(f.root, f.projects[0]);
    const before = NodeFS.readFileSync(entry);
    NodeAssert.equal(
      await checkProject({ ...f.options, run, inputsUnchanged: async () => false }),
      0,
    );
    NodeAssert.deepEqual(NodeFS.readFileSync(entry), before);
    NodeAssert.match(f.logs.at(-1), /cache not retained: identity inputs changed/);
    NodeAssert.equal(
      await checkProject({
        ...f.options,
        run: async (command) => {
          write(nativePath(command), "{}");
          return { code: 0, stdout: "" };
        },
      }),
      0,
    );
    NodeAssert.match(f.logs.at(-1), /cache not retained: Native build info/);
    NodeAssert.deepEqual(NodeFS.readFileSync(entry), before);
    NodeFS.unlinkSync(entry);
    NodeFS.mkdirSync(entry);
    NodeAssert.equal(await checkProject({ ...f.options, fresh: true, run }), 0);
    NodeFS.rmdirSync(entry);
    NodeAssert.equal(
      await checkProject({
        ...f.options,
        run,
        inputsUnchanged: async () => {
          NodeFS.mkdirSync(entry);
          return true;
        },
      }),
      0,
    );
    NodeAssert.match(f.logs.at(-1), /cache not retained/);
    NodeAssert.deepEqual(NodeFS.readdirSync(NodePath.dirname(entry)), ["entry.json"]);
  },
);

NodeTest.test(
  "controlled concurrent writers own separate same-parent files and publish complete envelopes",
  async (t) => {
    const f = fixture(t);
    const started = [];
    const releases = [];
    const run = (command) =>
      new Promise((resolve) => {
        const filename = nativePath(command);
        started.push(filename);
        releases.push(() => {
          write(filename, payload(`writer-${started.indexOf(filename)}`));
          resolve({ code: 0, stdout: "" });
        });
      });
    const first = checkProject({ ...f.options, run });
    const second = checkProject({ ...f.options, run });
    NodeAssert.equal(started.length, 2);
    NodeAssert.notEqual(started[0], started[1]);
    NodeAssert.equal(NodePath.dirname(started[0]), NodePath.dirname(started[1]));
    releases[1]();
    NodeAssert.equal(await second, 0);
    NodeAssert.ok(NodeFS.existsSync(started[0]));
    const { entry } = cachePaths(f.root, f.projects[0]);
    NodeAssert.deepEqual(readSeed(entry, f.options.identity), payload("writer-1"));
    releases[0]();
    NodeAssert.equal(await first, 0);
    NodeAssert.deepEqual(readSeed(entry, f.options.identity), payload("writer-0"));
    assertOnlyEntry(f);
  },
);

NodeTest.test(
  "capacity counts orphan, active and staging files; no fallback, reclamation, or symlink traversal",
  async (t) => {
    const f = fixture(t);
    const { namespace, directory } = cachePaths(f.root, f.projects[0]);
    for (const name of ["orphan.tsbuildinfo", "entry-orphan.tmp", "run-active.tsbuildinfo"]) {
      const orphan = NodePath.join(directory, name);
      write(orphan, "");
      NodeFS.truncateSync(orphan, NAMESPACE_LIMIT);
      await NodeAssert.rejects(
        checkProject({
          ...f.options,
          run: async () => NodeAssert.fail("No compiler on capacity failure"),
        }),
        /capacity admission failed/,
      );
      NodeAssert.equal(NodeFS.statSync(orphan).size, NAMESPACE_LIMIT);
      NodeFS.unlinkSync(orphan);
    }
    const target = NodePath.join(f.root, "outside-cache");
    NodeFS.mkdirSync(target);
    NodeFS.symlinkSync(target, NodePath.join(directory, "linked"), "dir");
    NodeAssert.throws(() => admitNamespace(f.root, namespace, RAW_LIMIT), /Unsafe cache entry/);
    NodeFS.unlinkSync(NodePath.join(directory, "linked"));
    NodeFS.rmdirSync(directory);
    NodeFS.symlinkSync(target, directory, "dir");
    await NodeAssert.rejects(checkProject(f.options), /Unsafe cache entry/);
    NodeAssert.deepEqual(NodeFS.readdirSync(target), []);
  },
);

NodeTest.test(
  "spawn failure, pre-abort, mid-check abort and setup failure clean only owned files",
  async (t) => {
    const f = fixture(t);
    const { directory } = cachePaths(f.root, f.projects[0]);
    write(NodePath.join(directory, "someone-elses-run.tsbuildinfo"), "keep");
    await NodeAssert.rejects(
      checkProject({
        ...f.options,
        run: async () => {
          throw new Error("spawn failed");
        },
      }),
      /spawn failed/,
    );
    await NodeAssert.rejects(
      checkProject({
        ...f.options,
        log: () => {
          throw new Error("setup output failed");
        },
        run: async () => NodeAssert.fail("Compiler must not start after partial setup failure"),
      }),
      /setup output failed/,
    );
    NodeAssert.deepEqual(NodeFS.readdirSync(directory), ["someone-elses-run.tsbuildinfo"]);
    const aborted = new AbortController();
    aborted.abort("SIGINT");
    await NodeAssert.rejects(
      checkProject({ ...f.options, signal: aborted.signal }),
      /Interrupted by SIGINT/,
    );
    const active = new AbortController();
    await NodeAssert.rejects(
      checkProject({
        ...f.options,
        signal: active.signal,
        run: async (command) => {
          write(nativePath(command), payload());
          active.abort("SIGTERM");
          return { code: 0, stdout: "" };
        },
      }),
      /Interrupted by SIGTERM/,
    );
    NodeAssert.deepEqual(NodeFS.readdirSync(directory), ["someone-elses-run.tsbuildinfo"]);
    const linked = NodePath.join(directory, "entry.json");
    NodeFS.symlinkSync(NodePath.join(directory, "someone-elses-run.tsbuildinfo"), linked);
    await NodeAssert.rejects(checkProject(f.options), /Unsafe cache entry/);
    NodeAssert.equal(
      NodeFS.readFileSync(NodePath.join(directory, "someone-elses-run.tsbuildinfo"), "utf8"),
      "keep",
    );
  },
);

NodeTest.test("cleanup failure is a tool failure naming exact residue", async (t) => {
  const f = fixture(t);
  let residue;
  await NodeAssert.rejects(
    checkProject({
      ...f.options,
      fresh: true,
      run: async (command) => {
        residue = nativePath(command);
        NodeFS.unlinkSync(residue);
        NodeFS.mkdirSync(residue);
        return { code: 0, stdout: "" };
      },
    }),
    (error) =>
      error.message.includes("Invocation cleanup failed") && error.message.includes(residue),
  );
  NodeAssert.ok(NodeFS.statSync(residue).isDirectory());
});

NodeTest.test(
  "CLI continues diagnostic failures serially, returns first native status, stops on interruption",
  async (t) => {
    const f = fixture(t);
    const checked = [];
    const run = compilerSeam(f, async (command) => {
      checked.push(command.cwd);
      write(nativePath(command), payload());
      return { code: checked.length === 1 ? 3 : 9, stdout: "" };
    });
    NodeAssert.equal(await runCli(f.root, ["--all"], run), 3);
    NodeAssert.deepEqual(
      checked,
      f.projects.map((project) => project.directory),
    );
    checked.length = 0;
    NodeAssert.equal(
      await runCli(
        f.root,
        ["--all"],
        compilerSeam(f, async (command) => {
          checked.push(command.cwd);
          process.emit("SIGINT");
          return { code: 0, stdout: "" };
        }),
      ),
      130,
    );
    NodeAssert.equal(checked.length, 1);
    checked.length = 0;
    const successful = compilerSeam(f, async (command) => {
      checked.push(command.cwd);
      write(nativePath(command), payload());
      return { code: 0, stdout: "" };
    });
    NodeAssert.equal(
      await runCli(f.root, ["--all"], async (command) => {
        if (command.args.includes("--showConfig") && command.cwd === f.projects[0].directory) {
          return { code: 2, stdout: "fixture native config diagnostic\n" };
        }
        return successful(command);
      }),
      2,
    );
    NodeAssert.deepEqual(checked, [f.projects[1].directory]);
  },
);

async function waitForFile(filename) {
  for (let attempt = 0; attempt < 250; attempt++) {
    if (NodeFS.existsSync(filename)) return;
    await NodeTimersPromises.setTimeout(20);
  }
  throw new Error(`Timed out waiting for fixture: ${filename}`);
}

NodeTest.test(
  "process boundary propagates exit status and SIGINT/SIGTERM, escalates and awaits exit before cleanup",
  async (t) => {
    const f = fixture(t);
    NodeAssert.equal(
      (
        await runProcess({
          executable: process.execPath,
          args: ["-e", "process.exit(17)"],
          cwd: f.root,
          capture: true,
          signal: signal(),
        })
      ).code,
      17,
    );
    await NodeAssert.rejects(
      runProcess({
        executable: NodePath.join(f.root, "missing-executable"),
        args: [],
        cwd: f.root,
        capture: true,
        signal: signal(),
      }),
      /ENOENT/,
    );
    for (const interrupt of ["SIGINT", "SIGTERM"]) {
      const ready = NodePath.join(f.root, `${interrupt}-ready`);
      const stopped = NodePath.join(f.root, `${interrupt}-stopped`);
      const controller = new AbortController();
      const program = `const NodeFS = require('node:fs'); process.on(${JSON.stringify(interrupt)}, () => { NodeFS.writeFileSync(${JSON.stringify(stopped)}, 'stopped'); process.exit(0); }); NodeFS.writeFileSync(${JSON.stringify(ready)}, 'ready'); setInterval(() => {}, 1000);`;
      const promise = checkProject({
        ...f.options,
        signal: controller.signal,
        run: (command) =>
          runProcess({ ...command, executable: process.execPath, args: ["-e", program] }),
      });
      const rejected = NodeAssert.rejects(promise, new RegExp(`Interrupted by ${interrupt}`));
      await waitForFile(ready);
      controller.abort(interrupt);
      await rejected;
      NodeAssert.ok(NodeFS.existsSync(stopped));
      NodeAssert.deepEqual(NodeFS.readdirSync(cachePaths(f.root, f.projects[0]).directory), []);
    }
    if (HostProcess.Platform.defaultValue() !== "win32") {
      const ready = NodePath.join(f.root, "ignore-ready");
      const controller = new AbortController();
      const running = runProcess({
        executable: process.execPath,
        args: [
          "-e",
          `process.on('SIGTERM', () => {}); require('node:fs').writeFileSync(${JSON.stringify(ready)}, 'ready'); setInterval(() => {}, 1000);`,
        ],
        cwd: f.root,
        capture: true,
        signal: controller.signal,
      });
      const rejected = NodeAssert.rejects(running, /Interrupted by SIGTERM/);
      await waitForFile(ready);
      controller.abort("SIGTERM");
      await rejected;
    }
  },
);

NodeTest.test(
  "native retained cache notices source, declaration, unresolved/new module and corrected errors",
  async (t) => {
    const f = fixture(t);
    const compiler = resolveCompiler(repository);
    const project = f.projects[0];
    const source = NodePath.join(project.directory, "source.ts");
    const declaration = NodePath.join(project.directory, "dependency.d.ts");
    write(
      NodePath.join(project.directory, "tsconfig.json"),
      JSON.stringify({
        compilerOptions: {
          strict: true,
          noEmit: true,
          skipLibCheck: true,
          module: "nodenext",
          target: "esnext",
        },
        files: ["source.ts", "dependency.d.ts"],
      }),
    );
    write(declaration, "declare const dependency: number;\n");
    write(source, "export const answer: number = dependency;\n");
    const config = await effectiveConfig(compiler, project, signal(), runProcess);
    const identity = projectIdentity(f.root, project, "stable", config);
    const seeds = [];
    const output = [];
    const run = async (command) => {
      seeds.push(NodeFS.statSync(nativePath(command)).size > 0);
      const result = await runProcess({ ...command, capture: true });
      if (result.code === 0) {
        const bytes = NodeFS.readFileSync(nativePath(command));
        const native = JSON.parse(bytes);
        NodeAssert.equal(
          validNativePayload(bytes),
          true,
          JSON.stringify({
            keys: Object.keys(native),
            version: native.version,
            effect: native.effect,
            root: native.root,
          }),
        );
      }
      output.push(result.stdout);
      return result;
    };
    const check = () => checkProject({ ...f.options, compiler, identity, run });
    NodeAssert.equal(await check(), 0);
    NodeAssert.equal(await check(), 0);
    write(source, "export const answer: string = dependency;\n");
    NodeAssert.notEqual(await check(), 0);
    NodeAssert.match(output.at(-1), /TS2322/);
    write(source, "export const answer: number = dependency;\n");
    write(declaration, "declare const dependency: string;\n");
    NodeAssert.notEqual(await check(), 0);
    NodeAssert.match(output.at(-1), /TS2322/);
    write(declaration, "declare const dependency: number;\n");
    write(
      source,
      "import { value } from './new-module.js'; export const answer: number = value;\n",
    );
    NodeAssert.notEqual(await check(), 0);
    NodeAssert.match(output.at(-1), /TS2307/);
    write(NodePath.join(project.directory, "new-module.ts"), "export const value: number = 42;\n");
    NodeAssert.equal(await check(), 0);
    write(
      NodePath.join(project.directory, "new-module.ts"),
      "export const value: string = 'changed';\n",
    );
    NodeAssert.notEqual(await check(), 0);
    NodeAssert.match(output.at(-1), /TS2322/);
    NodeAssert.deepEqual(seeds, [false, true, true, true, true, true, true]);
    assertOnlyEntry(f);
  },
);

NodeTest.test(
  "raw inherited Effect inputs invalidate lossy native configuration identity and preserve retained diagnostics",
  async (t) => {
    const f = fixture(t);
    const compiler = resolveCompiler(repository);
    const project = f.projects[0];
    const source = NodePath.join(project.directory, "source.ts");
    const base = NodePath.join(f.root, "tsconfig.base.json");
    const baseBytes = NodeFS.readFileSync(NodePath.join(repository, "tsconfig.base.json"), "utf8");
    write(base, baseBytes);
    NodeFS.symlinkSync(
      NodeFS.realpathSync(NodePath.join(repository, "scripts", "node_modules", "effect")),
      NodePath.join(f.root, "node_modules", "effect"),
      "dir",
    );
    write(
      NodePath.join(project.directory, "tsconfig.json"),
      JSON.stringify({
        extends: "../../tsconfig.base.json",
        files: ["source.ts"],
      }),
    );
    write(
      source,
      "import * as Effect from 'effect/Effect'; export const probe = Effect.sync(() => 0);\n",
    );
    const config = await effectiveConfig(compiler, project, signal(), runProcess);
    const identityForInputs = () =>
      projectIdentity(f.root, project, "stable", {
        native: config,
        inputs: configurationInputs(f.root, [project]).get(project.directory),
      });
    let identity = identityForInputs();
    const outputs = [];
    const seeds = [];
    const check = () =>
      checkProject({
        ...f.options,
        compiler,
        identity,
        run: async (command) => {
          seeds.push(NodeFS.statSync(nativePath(command)).size > 0);
          const result = await runProcess({ ...command, capture: true });
          outputs.push(result.stdout);
          return result;
        },
      });
    NodeAssert.equal(await check(), 0, outputs.at(-1));
    NodeAssert.match(outputs.at(-1), /suggestion TS377103/);
    NodeAssert.equal(await check(), 0, outputs.at(-1));
    NodeAssert.match(outputs.at(-1), /suggestion TS377103/);
    write(
      source,
      "import * as Effect from 'effect/Effect'; export const probe = Effect.sync(() => Date.now());\n",
    );
    NodeAssert.notEqual(await check(), 0);
    NodeAssert.match(outputs.at(-1), /TS377067/);
    write(
      source,
      "import * as Effect from 'effect/Effect'; export const probe = Effect.sync(() => 0);\n",
    );
    NodeAssert.equal(await check(), 0, outputs.at(-1));
    NodeAssert.deepEqual(seeds, [false, true, true, true]);
    assertOnlyEntry(f);
    NodeAssert.doesNotMatch(JSON.stringify(config), /globalDate/);
    write(base, baseBytes.replace('"globalDateInEffect": "error"', '"globalDateInEffect": "off"'));
    NodeAssert.notEqual(identityForInputs(), identity);
    identity = identityForInputs();
    NodeAssert.deepEqual(await effectiveConfig(compiler, project, signal(), runProcess), config);
    write(
      source,
      "import * as Effect from 'effect/Effect'; export const probe = Effect.sync(() => Date.now());\n",
    );
    NodeAssert.equal(await check(), 0, outputs.at(-1));
    write(base, baseBytes);
    identity = identityForInputs();
    NodeAssert.notEqual(await check(), 0);
    NodeAssert.match(outputs.at(-1), /TS377067/);
    write(base, baseBytes.replace('"globalDateInEffect": "error"', '"globalDateInEffect": "off"'));
    identity = identityForInputs();
    NodeAssert.equal(await check(), 0, outputs.at(-1));
    NodeAssert.deepEqual(seeds, [false, true, true, true, false, false, true]);
  },
);

NodeTest.test(
  "POSIX cancellation reaches the owned process group and waits for the worker to be reaped",
  { skip: HostProcess.Platform.defaultValue() === "win32" },
  async (t) => {
    const f = fixture(t);
    const ready = NodePath.join(f.root, "family-ready");
    const workerStopped = NodePath.join(f.root, "worker-stopped");
    const parentStopped = NodePath.join(f.root, "parent-stopped");
    const worker = `const NodeFS = require('node:fs'); process.on('SIGTERM', () => { NodeFS.writeFileSync(${JSON.stringify(workerStopped)}, 'stopped'); process.exit(0); }); process.send('ready'); setInterval(() => {}, 1000);`;
    const parent = `const NodeFS = require('node:fs'); const child = require('node:child_process').spawn(process.execPath, ['-e', ${JSON.stringify(worker)}], { stdio: ['ignore','ignore','ignore','ipc'] }); process.on('SIGTERM', () => {}); child.on('message', () => NodeFS.writeFileSync(${JSON.stringify(ready)}, 'ready')); child.on('close', () => { NodeFS.writeFileSync(${JSON.stringify(parentStopped)}, 'reaped'); process.exit(0); });`;
    const controller = new AbortController();
    const running = checkProject({
      ...f.options,
      signal: controller.signal,
      run: (command) =>
        runProcess({ ...command, executable: process.execPath, args: ["-e", parent] }),
    });
    const rejected = NodeAssert.rejects(running, /Interrupted by SIGTERM/);
    await waitForFile(ready);
    controller.abort("SIGTERM");
    await rejected;
    NodeAssert.ok(NodeFS.existsSync(workerStopped));
    NodeAssert.ok(NodeFS.existsSync(parentStopped));
    NodeAssert.deepEqual(NodeFS.readdirSync(cachePaths(f.root, f.projects[0]).directory), []);
  },
);

NodeTest.test(
  "finite JSONC topology guard accepts decoded keys/comments/trailing commas and rejects ambiguity, cycles and escapes",
  (t) => {
    const f = fixture(t);
    const project = f.projects[0];
    const filename = NodePath.join(project.directory, "tsconfig.json");
    const base = NodePath.join(f.root, "tsconfig.base.json");
    write(
      base,
      '{"compilerOptions":{"plugins":[{"name":"@effect/language-service","diagnosticSeverity":{"globalDate":"error"}}]}}',
    );
    const read = () => configurationInputs(f.root, [project]).get(project.directory);
    NodeAssert.equal(typeof read(), "string", "no extends is supported");
    write(
      filename,
      '{ /* "extends": "ignored" */ "extend\\u0073": "..\\/..\\/tsconfig.base.json", "nested": {"extends": "ignored"}, "text": "extends", } // trailing comment',
    );
    const first = read();
    write(
      base,
      '{"compilerOptions":{"plugins":[{"name":"@effect/language-service","diagnosticSeverity":{"globalDate":"off"}}]}}',
    );
    NodeAssert.notEqual(read(), first);
    const validProject = NodeFS.readFileSync(filename);
    for (const bytes of [
      '{"extends":"../../tsconfig.base.json","extend\\u0073":"../../tsconfig.base.json"}',
      '{"extends":["../../tsconfig.base.json"]}',
      '{"extends":null}',
      '{"extends":true}',
      '{"extends":"other-package/config.json"}',
      '{"extends":"./other.json"}',
      JSON.stringify({ extends: base }),
      '{"extends":"../../tsconfig.base.json", "x": }',
      '{"extends":"../../tsconfig.base.json"} {}',
      "{/* unterminated",
      '{extends:"../../tsconfig.base.json"}',
      Buffer.from([0xff, 0xfe]),
    ]) {
      write(filename, bytes);
      NodeAssert.throws(read, undefined, String(bytes));
    }
    write(filename, validProject);
    for (const extension of ["./tsconfig.base.json", "./packages/a/tsconfig.json", null]) {
      write(base, JSON.stringify({ extends: extension }));
      NodeAssert.throws(read, /nonterminal/);
    }
    write(base, "{}");
    NodeFS.unlinkSync(filename);
    NodeFS.symlinkSync(base, filename);
    NodeAssert.throws(read, /symlink/);
  },
);

NodeTest.test(
  "qualified Expo binding pins its manifest and fingerprints terminal base bytes",
  (t) => {
    const f = fixture(t);
    const project = {
      name: "@fixture/mobile",
      directory: NodePath.join(f.root, "apps", "mobile"),
      script: "tsc-rs --noEmit",
    };
    write(NodePath.join(project.directory, "package.json"), '{"name":"@fixture/mobile"}');
    write(
      NodePath.join(project.directory, "tsconfig.json"),
      '{"extends":"expo/tsconfig.base.json"}',
    );
    const original = NodeFS.realpathSync(
      NodePath.join(repository, "apps/mobile/node_modules/expo"),
    );
    const packageDirectory = NodePath.join(f.root, NodePath.relative(repository, original));
    const manifest = NodePath.join(packageDirectory, "package.json");
    const base = NodePath.join(packageDirectory, "tsconfig.base.json");
    const manifestBytes = NodeFS.readFileSync(NodePath.join(original, "package.json"));
    write(manifest, manifestBytes);
    write(base, '{"compilerOptions":{"strict":true}}');
    const binding = NodePath.join(project.directory, "node_modules", "expo");
    NodeFS.mkdirSync(NodePath.dirname(binding), { recursive: true });
    NodeFS.symlinkSync(packageDirectory, binding, "dir");
    const read = () => configurationInputs(f.root, [project]).get(project.directory);
    const first = read();
    write(base, '{"compilerOptions":{"strict":false}}');
    NodeAssert.notEqual(read(), first);
    write(manifest, Buffer.concat([manifestBytes, Buffer.from("\n")]));
    NodeAssert.throws(read, /Unqualified Expo/);
    write(manifest, manifestBytes);
    write(base, '{"extends":"./another.json"}');
    NodeAssert.throws(read, /nonterminal/);
    write(base, "{}");
    const other = NodePath.join(f.root, "node_modules", ".pnpm", "changed-expo-binding");
    write(NodePath.join(other, "package.json"), manifestBytes);
    write(NodePath.join(other, "tsconfig.base.json"), "{}");
    NodeFS.unlinkSync(binding);
    NodeFS.symlinkSync(other, binding, "dir");
    NodeAssert.throws(read, /Expo config binding/);
  },
);

NodeTest.test(
  "CLI config guard runs before reuse, fresh bypasses it, and mid-check raw config changes cannot publish",
  async (t) => {
    const f = fixture(t);
    const project = f.projects[0];
    const config = NodePath.join(project.directory, "tsconfig.json");
    const base = NodePath.join(f.root, "tsconfig.base.json");
    write(base, "{}");
    write(config, '{"extends":"../../tsconfig.base.json","compilerOptions":{}}');
    let calls = 0;
    let mutate = false;
    const run = compilerSeam(f, async (command) => {
      calls++;
      write(nativePath(command), payload(`call-${calls}`));
      if (mutate) write(base, '{"extends":"unknown.json"}');
      return { code: 0, stdout: "" };
    });
    NodeAssert.equal(await runCli(f.root, ["packages/a"], run), 0);
    const { entry } = cachePaths(f.root, project);
    const before = NodeFS.readFileSync(entry);
    mutate = true;
    NodeAssert.equal(await runCli(f.root, ["packages/a"], run), 0);
    NodeAssert.deepEqual(NodeFS.readFileSync(entry), before);
    NodeAssert.equal(await runCli(f.root, ["packages/a"], run), 1);
    NodeAssert.equal(calls, 2);
    NodeAssert.equal(await runCli(f.root, ["packages/a", "--fresh"], run), 0);
    NodeAssert.equal(calls, 3);
    NodeAssert.deepEqual(NodeFS.readFileSync(entry), before);
    assertOnlyEntry(f);
  },
);
