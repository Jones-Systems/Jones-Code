import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import {
  classifyArguments,
  projectConfiguration,
  runWithBuildInfo,
  statePaths,
  convertSelfReference,
  STATE_SCHEMA,
  STATE_STORAGE,
  admitState,
} from "./build-info.ts";
import { typecheck, runFallback, launchVerified } from "./typecheck.ts";
import * as Identity from "./identity.ts";
import { runProcess } from "../incremental-typecheck/incrementalTypecheck.ts";
import { configurationInputs } from "./config-inputs.ts";

const repository = NodePath.resolve(import.meta.dirname, "../../..");
const scratch = NodePath.join(repository, "node_modules/.cache/jones/tsrs-build-info-tests");
const owned = new Set();
const jobs = new Set();
const stopping = new AbortController();
for (const signal of ["SIGINT", "SIGTERM"])
  process.once(signal, async () => {
    stopping.abort(signal);
    await Promise.allSettled(jobs);
    await Promise.all([...owned].map((root) => NodeFSP.rm(root, { recursive: true, force: true })));
    process.exitCode = signal === "SIGINT" ? 130 : 143;
  });
async function fixture(t) {
  await NodeFSP.mkdir(scratch, { recursive: true });
  const root = await NodeFSP.mkdtemp(NodePath.join(scratch, "run-"));
  owned.add(root);
  t.after(async () => {
    await NodeFSP.rm(root, { recursive: true, force: true });
    owned.delete(root);
  });
  const directory = NodePath.join(root, "project");
  await NodeFSP.mkdir(directory);
  await NodeFSP.writeFile(
    NodePath.join(root, "tsconfig.base.json"),
    JSON.stringify({ compilerOptions: { noEmit: true, incremental: true } }),
  );
  const config = NodePath.join(directory, "tsconfig.json");
  await NodeFSP.writeFile(
    config,
    JSON.stringify({ extends: "../tsconfig.base.json", files: ["source.ts"] }),
  );
  await NodeFSP.writeFile(
    NodePath.join(directory, "source.ts"),
    "export const value: number = 42;\n",
  );
  return { root, directory, config };
}
NodeTest.test(
  "one-shot grammar preserves qualified aliases, booleans and last-option precedence",
  () => {
    for (const args of [
      [],
      ["--noEmit"],
      ["--pretty", "false"],
      ["--diagnostics", "true", "--listFiles"],
      ["-p", "project"],
      ["-project", "project"],
      ["--PROJECT", "project"],
      ["--tsBuildInfoFile", "/absolute/state.tsbuildinfo"],
    ])
      NodeAssert.equal(classifyArguments(args).kind, "check");
    NodeAssert.deepEqual(
      classifyArguments([
        "--incremental",
        "true",
        "--incremental",
        "false",
        "-p",
        "a",
        "--project",
        "b",
      ]),
      { kind: "check", incremental: false, project: "b" },
    );
    for (const args of [["--version"], ["-v"], ["--VERSION"]])
      NodeAssert.equal(classifyArguments(args).kind, "information");
  },
);
NodeTest.test(
  "unqualified config-bypass, stateful and ambiguous grammar fails before launch",
  () => {
    for (const args of [
      ["-b"],
      ["--BUILD"],
      ["-watch"],
      ["--watch", "false"],
      ["@args.txt"],
      ["source.ts"],
      ["--"],
      ["--incremental=false"],
      ["--pretty", "FALSE"],
      ["--project"],
      ["--project", "null"],
      ["--version", "false"],
      ["--showConfig"],
      ["--noEmit", "false"],
      ["--unknown"],
      ["--checkers", "4"],
      ["--CHECKERASSIGNMENT", "adaptive"],
    ])
      NodeAssert.throws(() => classifyArguments(args), /one-shot|pinned/);
  },
);
NodeTest.test("project file/directory/default discovery and build-info precedence", async (t) => {
  const { root, directory, config } = await fixture(t);
  const nested = NodePath.join(directory, "nested");
  await NodeFSP.mkdir(nested);
  for (const [cwd, args] of [
    [directory, []],
    [nested, []],
    [root, ["-p", "project"]],
    [root, ["--project", "project/tsconfig.json"]],
  ]) {
    const selected = await projectConfiguration(root, cwd, classifyArguments(args));
    NodeAssert.equal(selected.config, config);
    NodeAssert.equal(selected.output, NodePath.join(directory, "tsconfig.tsbuildinfo"));
  }
  await NodeFSP.writeFile(
    config,
    JSON.stringify({
      extends: "../tsconfig.base.json",
      compilerOptions: { tsBuildInfoFile: "config.tsbuildinfo" },
    }),
  );
  NodeAssert.equal(
    (await projectConfiguration(root, directory, classifyArguments([]))).output,
    NodePath.join(directory, "config.tsbuildinfo"),
  );
  NodeAssert.equal(
    (
      await projectConfiguration(
        root,
        directory,
        classifyArguments(["--tsBuildInfoFile", "cli.tsbuildinfo"]),
      )
    ).output,
    NodePath.join(directory, "cli.tsbuildinfo"),
  );
  await NodeFSP.writeFile(config, JSON.stringify({ extends: "../tsconfig.base.json" }));
  NodeAssert.equal(
    (await projectConfiguration(root, directory, classifyArguments(["--incremental", "false"])))
      .output,
    undefined,
  );
});
NodeTest.test(
  "raw project/base config changes invalidate both retained and one-shot identities",
  async (t) => {
    const { root, directory, config } = await fixture(t);
    const before = (await projectConfiguration(root, directory, classifyArguments([]))).snapshot
      .identity;
    NodeAssert.equal((await configurationInputs(root, [{ directory }])).get(directory), before);
    const base = NodePath.join(root, "tsconfig.base.json");
    await NodeFSP.appendFile(
      base,
      "\n// Effect severity change is bound even when native showConfig omits it\n",
    );
    const changed = (await projectConfiguration(root, directory, classifyArguments([]))).snapshot
      .identity;
    NodeAssert.notEqual(changed, before);
    NodeAssert.equal((await configurationInputs(root, [{ directory }])).get(directory), changed);
    await NodeFSP.appendFile(config, "\n// project override\n");
    NodeAssert.notEqual(
      (await projectConfiguration(root, directory, classifyArguments([]))).snapshot.identity,
      changed,
    );
  },
);

NodeTest.test(
  "candidate and fallback reject before resolving or launching any compiler",
  async () => {
    for (const run of [typecheck, runFallback])
      await NodeAssert.rejects(
        run("/no-compiler-here", "/no-compiler-here", ["--watch"], new AbortController().signal),
        /one-shot/,
      );
  },
);
NodeTest.test(
  "pinned native grammar agrees on casing, aliases, arity and option precedence",
  async (t) => {
    const { root, directory } = await fixture(t);
    const installed = await Identity.resolveInstalled(repository, Identity.hostTarget());
    for (const args of [
      [
        "--PROJECT",
        directory,
        "--incremental",
        "true",
        "--incremental",
        "false",
        "--pretty",
        "false",
      ],
      ["-project", directory, "--pretty", "false", "--noEmit"],
      ["--p", directory, "--tsBuildInfoFile", NodePath.join(directory, "cli.tsbuildinfo")],
    ]) {
      const parsed = classifyArguments(args);
      const job = launchVerified(installed, stopping.signal, async (compiler, spawned) => {
        const output = await runProcess({
          executable: compiler.executable,
          args: [...args, ...Identity.CHECKER_OPTIONS],
          cwd: root,
          signal: stopping.signal,
          capture: true,
          spawned,
        });
        NodeAssert.equal(output.code, 0, output.stdout);
        if (parsed.incremental === false)
          await NodeAssert.rejects(NodeFSP.stat(NodePath.join(directory, "tsconfig.tsbuildinfo")), {
            code: "ENOENT",
          });
        if (parsed.buildInfo)
          NodeAssert.equal(
            JSON.parse(await NodeFSP.readFile(parsed.buildInfo, "utf8")).options.tsBuildInfoFile,
            "./cli.tsbuildinfo",
          );
        return output.code;
      });
      jobs.add(job);
      try {
        NodeAssert.equal(await job, 0);
      } finally {
        jobs.delete(job);
      }
    }
  },
);

NodeTest.test("pinned fallback keeps the same qualified argument forms", async (t) => {
  const { root, directory } = await fixture(t);
  const compiler = await Identity.resolveFallback(repository);
  for (const args of [
    [
      "--PROJECT",
      directory,
      "--incremental",
      "true",
      "--incremental",
      "false",
      "--pretty",
      "false",
    ],
    ["-project", directory, "--noEmit"],
    [
      "--p",
      directory,
      "--noEmit",
      "--tsBuildInfoFile",
      NodePath.join(directory, "fallback.tsbuildinfo"),
    ],
  ]) {
    classifyArguments(args);
    const job = runProcess({
      executable: compiler.executable,
      args,
      cwd: root,
      signal: stopping.signal,
      capture: true,
    });
    jobs.add(job);
    try {
      const result = await job;
      NodeAssert.equal(result.code, 0, result.stdout);
    } finally {
      jobs.delete(job);
    }
  }
});

async function nativeFixture(t) {
  const f = await fixture(t);
  await NodeFSP.writeFile(
    NodePath.join(f.root, "package.json"),
    JSON.stringify({ type: "module", name: "native-build-info-fixture" }),
  );
  const live = await Identity.resolveInstalled(repository, Identity.hostTarget());
  const wrapper = NodePath.join(f.root, "node_modules/tsrs/package.json");
  const nativeManifest = NodePath.join(
    f.root,
    "node_modules/.pnpm/native/node_modules/@tsrs",
    live.target,
    "package.json",
  );
  await NodeFSP.mkdir(NodePath.dirname(nativeManifest), { recursive: true });
  await NodeFSP.mkdir(NodePath.dirname(wrapper), { recursive: true });
  await NodeFSP.writeFile(
    wrapper,
    JSON.stringify({ name: "tsrs", version: Identity.TSRS_VERSION }),
  );
  await NodeFSP.writeFile(
    nativeManifest,
    JSON.stringify({ name: `@tsrs/${live.target}`, version: Identity.TSRS_VERSION }),
  );
  const executable = NodePath.join(NodePath.dirname(nativeManifest), "tsrs");
  await NodeFSP.copyFile(live.executable, executable);
  await NodeFSP.copyFile(`${live.executable}.original`, `${executable}.original`);
  await NodeFSP.chmod(executable, 0o700);
  const binding = NodePath.join(NodePath.dirname(wrapper), "node_modules/@tsrs", live.target);
  await NodeFSP.mkdir(NodePath.dirname(binding), { recursive: true });
  await NodeFSP.symlink(NodePath.dirname(nativeManifest), binding, "dir");
  const compiler = await Identity.resolveInstalled(f.root, live.target);
  await Identity.verified(compiler);
  await NodeFSP.mkdir(NodePath.join(f.directory, "node_modules"));
  await NodeFSP.symlink(
    NodePath.resolve(repository, "scripts/node_modules/effect"),
    NodePath.join(f.directory, "node_modules/effect"),
    "dir",
  );
  const baseFile = NodePath.join(f.root, "tsconfig.base.json");
  const base = await NodeFSP.readFile(NodePath.join(repository, "tsconfig.base.json"), "utf8");
  await NodeFSP.writeFile(baseFile, base);
  await NodeFSP.writeFile(
    f.config,
    JSON.stringify({
      extends: "../tsconfig.base.json",
      compilerOptions: { types: [], incremental: true },
      files: ["source.ts", "value.ts"],
    }),
  );
  await NodeFSP.writeFile(
    NodePath.join(f.directory, "source.ts"),
    'import * as Effect from "effect/Effect";\nimport { value } from "./value.ts";\nexport const effect = Effect.sync(() => Date.now() + value);\n',
  );
  await NodeFSP.writeFile(
    NodePath.join(f.directory, "value.ts"),
    "export const value: number = 42;\n",
  );
  const log = [];
  const outputs = [];
  const invoke = async (args, override) => {
    const job = runWithBuildInfo({
      root: f.root,
      cwd: f.directory,
      args,
      compiler,
      signal: stopping.signal,
      log: (line) => log.push(line),
      run:
        override ??
        ((effective, seen) =>
          launchVerified(compiler, stopping.signal, async (native, spawned) => {
            const result = await runProcess({
              executable: native.executable,
              args: [...effective, ...Identity.CHECKER_OPTIONS],
              cwd: f.directory,
              signal: stopping.signal,
              capture: true,
              spawned: async () => {
                seen();
                await spawned();
              },
            });
            outputs.push(result);
            return result.code;
          })),
    });
    jobs.add(job);
    try {
      return await job;
    } finally {
      jobs.delete(job);
    }
  };
  return { ...f, compiler, base, baseFile, invoke, log, outputs };
}
NodeTest.test(
  "native config-bound default and explicit outputs retain configured Effect errors through 2/0/0/2",
  async (t) => {
    const f = await nativeFixture(t);
    for (const explicit of [false, true]) {
      const output = NodePath.join(
        f.directory,
        explicit ? "explicit.tsbuildinfo" : "tsconfig.tsbuildinfo",
      );
      const args = [
        "--noEmit",
        "--pretty",
        "false",
        "--extendedDiagnostics",
        ...(explicit ? ["--tsBuildInfoFile", output] : []),
      ];
      const paths = statePaths(f.root, f.directory, output);
      const exits = [];
      for (const phase of ["error", "off", "off-fresh", "error"]) {
        await NodeFSP.writeFile(
          f.baseFile,
          phase.startsWith("off")
            ? f.base.replace('"globalDateInEffect": "error"', '"globalDateInEffect": "off"')
            : f.base,
        );
        if (phase === "off-fresh") await NodeFSP.rm(paths.entry);
        const code = await f.invoke(args);
        exits.push(code);
        NodeAssert.equal(
          f.outputs.at(-1).stdout.includes("error TS377067"),
          !phase.startsWith("off"),
        );
        const value = JSON.parse(await NodeFSP.readFile(output, "utf8"));
        NodeAssert.equal(NodePath.resolve(f.directory, value.options.tsBuildInfoFile), output);
        NodeAssert.equal(
          (await NodeFSP.readdir(f.directory)).some((name) => name.startsWith(".jones-tsrs-")),
          false,
        );
        NodeAssert.equal(
          (await NodeFSP.readdir(paths.namespace)).some(
            (name) => name.endsWith(".run.json") || name.endsWith(".tmp") || name.endsWith(".lock"),
          ),
          false,
        );
      }
      NodeAssert.deepEqual(exits, [2, 0, 0, 2]);
      console.log(
        JSON.stringify({
          nativeSequence: explicit ? "explicit" : "default",
          exits,
          diagnostics: f.outputs
            .at(-2)
            .stdout.split("\n")
            .filter((line) => /Types:|Instantiations:|Check time:|Bind time:/.test(line)),
        }),
      );
    }
  },
);

function nativeCounters(stdout) {
  const result = {};
  for (const key of ["Types", "Instantiations"]) {
    const match = new RegExp(`^${key}:\\s+(\\d+)`, "m").exec(stdout);
    NodeAssert.ok(match, `Missing native reuse counter ${key}: ${stdout}`);
    result[key] = Number(match[1]);
  }
  return result;
}
NodeTest.test(
  "public native representation is reusable directly and through a different private nonce",
  async (t) => {
    const f = await nativeFixture(t);
    await NodeFSP.writeFile(
      f.baseFile,
      f.base.replace('"globalDateInEffect": "error"', '"globalDateInEffect": "off"'),
    );
    const args = ["--noEmit", "--pretty", "false", "--extendedDiagnostics"];
    const output = NodePath.join(f.directory, "tsconfig.tsbuildinfo");
    NodeAssert.equal(await f.invoke(args), 0);
    const fresh = nativeCounters(f.outputs.at(-1).stdout);
    const diagnostics = (stdout) =>
      stdout.split("\n").filter((line) => /\b(?:error|warning|suggestion) TS\d+/.test(line));
    const freshDiagnostics = diagnostics(f.outputs.at(-1).stdout);
    const original = JSON.parse(await NodeFSP.readFile(output, "utf8"));
    NodeAssert.ok(original.fileNames.includes("./source.ts"));
    NodeAssert.ok(original.fileNames.includes("./value.ts"));
    NodeAssert.ok(
      original.fileNames.some((name) => name.startsWith("lib.") && name.endsWith(".d.ts")),
    );
    const effectManifest = await NodeFSP.realpath(
      NodePath.resolve(repository, "scripts/node_modules/effect/package.json"),
    );
    NodeAssert.ok(
      original.packageJsons.some((name) => NodePath.resolve(f.directory, name) === effectManifest),
    );
    for (const name of original.fileNames.filter((name) => !name.startsWith("lib.")))
      NodeAssert.ok((await NodeFSP.stat(NodePath.resolve(f.directory, name))).isFile(), name);
    NodeAssert.equal(NodePath.resolve(f.directory, original.options.tsBuildInfoFile), output);
    NodeAssert.equal(await f.invoke(args), 0);
    const warm = nativeCounters(f.outputs.at(-1).stdout);
    NodeAssert.deepEqual(diagnostics(f.outputs.at(-1).stdout), freshDiagnostics);
    NodeAssert.deepEqual(JSON.parse(await NodeFSP.readFile(output, "utf8")), original);
    NodeAssert.equal(f.log.length, 2);
    NodeAssert.match(f.log[1], /seed validated/);
    NodeAssert.notEqual(f.log[0].split("; ")[1], f.log[1].split("; ")[1]);
    NodeAssert.ok(warm.Types < fresh.Types, JSON.stringify({ fresh, warm }));
    const job = launchVerified(f.compiler, stopping.signal, async (compiler, spawned) => {
      const direct = await runProcess({
        executable: compiler.executable,
        args: [...args, "--tsBuildInfoFile", output, ...Identity.CHECKER_OPTIONS],
        cwd: f.directory,
        signal: stopping.signal,
        capture: true,
        spawned,
      });
      NodeAssert.equal(direct.code, 0, direct.stdout);
      const counters = nativeCounters(direct.stdout);
      NodeAssert.deepEqual(diagnostics(direct.stdout), freshDiagnostics);
      NodeAssert.ok(counters.Types < fresh.Types, JSON.stringify({ fresh, counters }));
      NodeAssert.deepEqual(JSON.parse(await NodeFSP.readFile(output, "utf8")), original);
      console.log(
        JSON.stringify({
          reuseProof: true,
          fresh,
          adapterWarm: warm,
          directLogicalConsumer: counters,
        }),
      );
      return direct.code;
    });
    jobs.add(job);
    try {
      NodeAssert.equal(await job, 0);
    } finally {
      jobs.delete(job);
    }
    NodeAssert.equal(
      (await NodeFSP.readdir(f.directory)).some((name) => name.startsWith(".jones-tsrs-")),
      false,
    );
  },
);

function samplePayload(working) {
  return Buffer.from(
    JSON.stringify({
      version: Identity.BUILD_VERSION,
      fileNames: ["lib.es5.d.ts", "./source.ts"],
      fileInfos: ["lib-v1", "source-v1"],
      root: [2],
      options: { tsBuildInfoFile: `./${NodePath.basename(working)}` },
      packageJsons: ["./package.json"],
      semanticDiagnosticsPerFile: [],
    }),
  );
}
NodeTest.test(
  "self-reference conversion preserves every other JSON value and rejects malformed references",
  () => {
    const working = "/project/.jones-tsrs-test.tsbuildinfo",
      logical = "/project/tsconfig.tsbuildinfo";
    const before = JSON.parse(samplePayload(working));
    const after = JSON.parse(convertSelfReference(samplePayload(working), working, logical));
    NodeAssert.equal(after.options.tsBuildInfoFile, "./tsconfig.tsbuildinfo");
    after.options.tsBuildInfoFile = before.options.tsBuildInfoFile;
    NodeAssert.deepEqual(after, before);
    for (const value of [undefined, 7, "./unexpected.tsbuildinfo"]) {
      const invalid = structuredClone(before);
      invalid.options.tsBuildInfoFile = value;
      NodeAssert.throws(
        () => convertSelfReference(Buffer.from(JSON.stringify(invalid)), working, logical),
        /self-reference|Malformed/,
      );
    }
  },
);
NodeTest.test(
  "same-target concurrent native runs publish coherent generations and clean exact scratch",
  async (t) => {
    const f = await nativeFixture(t);
    await NodeFSP.writeFile(
      f.baseFile,
      f.base.replace('"globalDateInEffect": "error"', '"globalDateInEffect": "off"'),
    );
    const args = ["--noEmit", "--pretty", "false"];
    NodeAssert.deepEqual(await Promise.all([f.invoke(args), f.invoke(args)]), [0, 0]);
    const output = NodePath.join(f.directory, "tsconfig.tsbuildinfo");
    const envelope = JSON.parse(
      await NodeFSP.readFile(statePaths(f.root, f.directory, output).entry, "utf8"),
    );
    NodeAssert.equal(envelope.schema, STATE_SCHEMA);
    NodeAssert.deepEqual(
      JSON.parse(Buffer.from(envelope.payload, "base64")),
      JSON.parse(await NodeFSP.readFile(output, "utf8")),
    );
    NodeAssert.equal(
      (await NodeFSP.readdir(f.directory)).some((name) => name.startsWith(".jones-tsrs-")),
      false,
    );
    NodeAssert.equal(
      (await NodeFSP.readdir(statePaths(f.root, f.directory, output).namespace)).some((name) =>
        /run\.json$|\.tmp$|\.lock$/.test(name),
      ),
      false,
    );
  },
);
NodeTest.test(
  "config drift and foreign replacement cannot publish a successful seed",
  async (t) => {
    const f = await nativeFixture(t);
    const args = ["--noEmit"];
    let output = NodePath.join(f.directory, "tsconfig.tsbuildinfo");
    await NodeAssert.rejects(
      f.invoke(args, async (effective) => {
        const working = effective.at(-1);
        await NodeFSP.writeFile(working, samplePayload(working));
        await NodeFSP.appendFile(f.baseFile, "\n// changed during native checking\n");
        return 0;
      }),
      /changed during checking/,
    );
    await NodeAssert.rejects(NodeFSP.stat(statePaths(f.root, f.directory, output).entry), {
      code: "ENOENT",
    });
    await NodeAssert.rejects(
      f.invoke(args, async (effective) => {
        const working = effective.at(-1);
        await NodeFSP.writeFile(working, samplePayload(working));
        await NodeFSP.writeFile(output, "foreign output");
        return 0;
      }),
      /Foreign logical output replacement/,
    );
    NodeAssert.equal(await NodeFSP.readFile(output, "utf8"), "foreign output");
    NodeAssert.equal(
      (await NodeFSP.readdir(f.directory)).some((name) => name.startsWith(".jones-tsrs-")),
      false,
    );
  },
);
NodeTest.test(
  "reserved, symlink and hard-linked logical outputs refuse before native execution",
  async (t) => {
    const f = await nativeFixture(t);
    let calls = 0;
    const fake = async () => {
      calls++;
      return 0;
    };
    await NodeAssert.rejects(
      f.invoke(["--noEmit", "--tsBuildInfoFile", ".jones-tsrs-foreign.tsbuildinfo"], fake),
      /Custom build-info/,
    );
    const source = NodePath.join(f.directory, "source.ts");
    const output = NodePath.join(f.directory, "unsafe.tsbuildinfo");
    await NodeFSP.symlink(source, output);
    await NodeAssert.rejects(f.invoke(["--noEmit", "--tsBuildInfoFile", output], fake), /symlink/);
    await NodeFSP.unlink(output);
    await NodeFSP.link(source, output);
    await NodeAssert.rejects(
      f.invoke(["--noEmit", "--tsBuildInfoFile", output], fake),
      /Unsafe|foreign-owned/,
    );
    const link = NodePath.join(f.root, "project-link");
    await NodeFSP.symlink(f.directory, link, "dir");
    await NodeAssert.rejects(
      runWithBuildInfo({
        root: f.root,
        cwd: link,
        args: ["--noEmit"],
        compiler: f.compiler,
        signal: stopping.signal,
        run: fake,
      }),
      /canonical/,
    );
    NodeAssert.equal(calls, 0);
  },
);
NodeTest.test(
  "confirmed cancellation cleans invocation state; unconfirmed exit preserves accounting and refuses reuse",
  async (t) => {
    const f = await nativeFixture(t);
    const controller = new AbortController();
    const run = (signal, execute) =>
      runWithBuildInfo({
        root: f.root,
        cwd: f.directory,
        args: ["--noEmit"],
        compiler: f.compiler,
        signal,
        run: execute,
      });
    NodeAssert.equal(
      await run(controller.signal, async (_args, spawned) => {
        spawned();
        controller.abort("SIGINT");
        return 130;
      }),
      130,
    );
    const output = NodePath.join(f.directory, "tsconfig.tsbuildinfo");
    const namespace = statePaths(f.root, f.directory, output).namespace;
    NodeAssert.equal(
      (await NodeFSP.readdir(namespace)).some((name) => /run\.json$|\.tmp$|\.lock$/.test(name)),
      false,
    );
    await NodeAssert.rejects(
      run(stopping.signal, async (_args, spawned) => {
        spawned();
        throw new Error("unconfirmed synthetic native exit");
      }),
      /Unconfirmed native exit/,
    );
    const accounts = (await NodeFSP.readdir(namespace)).filter((name) =>
      name.endsWith(".run.json"),
    );
    NodeAssert.equal(accounts.length, 1);
    const account = JSON.parse(
      await NodeFSP.readFile(NodePath.join(namespace, accounts[0]), "utf8"),
    );
    NodeAssert.equal(account.phase, "unknown");
    NodeAssert.ok((await NodeFSP.stat(account.working)).isFile());
    await NodeAssert.rejects(
      run(stopping.signal, async () => 0),
      /Unresolved build-info invocation residue/,
    );
  },
);

NodeTest.test(
  "ordinary source/declaration/options and project Effect changes invalidate without losing the valid seed",
  async (t) => {
    const f = await nativeFixture(t);
    await NodeFSP.writeFile(
      f.baseFile,
      f.base.replace('"globalDateInEffect": "error"', '"globalDateInEffect": "off"'),
    );
    const args = ["--noEmit", "--pretty", "false"];
    NodeAssert.equal(await f.invoke(args), 0);
    const paths = statePaths(
      f.root,
      f.directory,
      NodePath.join(f.directory, "tsconfig.tsbuildinfo"),
    );
    const seed = await NodeFSP.readFile(paths.entry);
    await NodeFSP.writeFile(
      NodePath.join(f.directory, "value.ts"),
      'export const value: number = "wrong";\n',
    );
    NodeAssert.equal(await f.invoke(args), 2);
    NodeAssert.match(f.outputs.at(-1).stdout, /error TS2322/);
    NodeAssert.deepEqual(await NodeFSP.readFile(paths.entry), seed);
    await NodeFSP.writeFile(
      NodePath.join(f.directory, "value.ts"),
      'import type { Shape } from "./shape.js";\nexport const shape: Shape = { value: 42 };\nexport const value = 42;\n',
    );
    const declaration = NodePath.join(f.directory, "shape.d.ts");
    await NodeFSP.writeFile(declaration, "export interface Shape { value: number }\n");
    NodeAssert.equal(await f.invoke(args), 0);
    await NodeFSP.writeFile(declaration, "export interface Shape { value: string }\n");
    NodeAssert.equal(await f.invoke(args), 2);
    NodeAssert.match(f.outputs.at(-1).stdout, /error TS2322/);
    await NodeFSP.writeFile(declaration, "export interface Shape { value: number }\n");
    NodeAssert.equal(await f.invoke(args), 0);
    const config = JSON.parse(await NodeFSP.readFile(f.config, "utf8"));
    config.compilerOptions.noUnusedLocals = true;
    await NodeFSP.writeFile(f.config, JSON.stringify(config));
    await NodeFSP.appendFile(NodePath.join(f.directory, "value.ts"), "const unused = 1;\n");
    NodeAssert.equal(await f.invoke(args), 2);
    NodeAssert.match(f.outputs.at(-1).stdout, /error TS6133/);
    config.compilerOptions.noUnusedLocals = false;
    config.compilerOptions.plugins = [
      { name: "@effect/language-service", diagnosticSeverity: { globalDateInEffect: "error" } },
    ];
    await NodeFSP.writeFile(f.config, JSON.stringify(config));
    NodeAssert.equal(await f.invoke(args), 2);
    NodeAssert.match(f.outputs.at(-1).stdout, /error TS377067/);
    config.compilerOptions.plugins[0].diagnosticSeverity.globalDateInEffect = "off";
    await NodeFSP.writeFile(f.config, JSON.stringify(config));
    NodeAssert.equal(await f.invoke(args), 0);
    const envelope = JSON.parse(await NodeFSP.readFile(paths.entry, "utf8"));
    envelope.identity = "different-compiler-or-platform-binding";
    await NodeFSP.writeFile(paths.entry, JSON.stringify(envelope));
    NodeAssert.equal(await f.invoke(args), 0);
    NodeAssert.match(f.log.at(-1), /cache miss/);
    await NodeFSP.writeFile(paths.entry, "malformed envelope");
    config.compilerOptions.plugins[0].diagnosticSeverity.globalDateInEffect = "error";
    await NodeFSP.writeFile(f.config, JSON.stringify(config));
    NodeAssert.equal(await f.invoke(args), 2);
    NodeAssert.match(f.log.at(-1), /cache miss/);
    NodeAssert.match(f.outputs.at(-1).stdout, /error TS377067/);
  },
);

NodeTest.test(
  "admission charges external retained raw output and counts crash entries",
  async (t) => {
    const f = await nativeFixture(t);
    await NodeFSP.writeFile(
      f.baseFile,
      f.base.replace('"globalDateInEffect": "error"', '"globalDateInEffect": "off"'),
    );
    NodeAssert.equal(await f.invoke(["--noEmit"]), 0);
    const paths = statePaths(
      f.root,
      f.directory,
      NodePath.join(f.directory, "tsconfig.tsbuildinfo"),
    );
    let bytes = 0;
    for (const name of await NodeFSP.readdir(paths.namespace))
      bytes += (await NodeFSP.stat(NodePath.join(paths.namespace, name))).size;
    const filler = await NodeFSP.open(
      NodePath.join(paths.namespace, "owned-sparse-capacity.tmp"),
      "wx",
    );
    try {
      await filler.truncate(STATE_STORAGE.aggregate - bytes - STATE_STORAGE.reserve - 512);
    } finally {
      await filler.close();
    }
    const prior = f.outputs.length;
    await NodeAssert.rejects(f.invoke(["--noEmit"]), /capacity admission failed/);
    NodeAssert.equal(f.outputs.length, prior);
    const smallNamespace = NodePath.join(f.root, "node_modules/.cache/entry-count-test");
    await NodeFSP.mkdir(smallNamespace, { recursive: true });
    await NodeFSP.writeFile(NodePath.join(smallNamespace, "crash-a.tmp"), "");
    await NodeFSP.writeFile(NodePath.join(smallNamespace, "crash-b.tmp"), "");
    await NodeAssert.rejects(
      admitState(f.root, smallNamespace, 0, STATE_STORAGE.entries - 1),
      /entries.*crash residue/,
    );
  },
);
NodeTest.test(
  "self-reference conversion enforces the native bound after filename expansion",
  () => {
    const working = "/project/.jones-tsrs-a.tsbuildinfo";
    const value = JSON.parse(samplePayload(working));
    value.fileInfos[1] = "";
    const overhead = Buffer.byteLength(JSON.stringify(value));
    value.fileInfos[1] = "x".repeat(STATE_STORAGE.payload - overhead - 20);
    const bytes = Buffer.from(JSON.stringify(value));
    NodeAssert.ok(bytes.length < STATE_STORAGE.payload);
    NodeAssert.throws(
      () => convertSelfReference(bytes, working, `/project/${"o".repeat(200)}.tsbuildinfo`),
      /Converted native build info exceeds/,
    );
  },
);

NodeTest.test(
  "concurrent different configs sharing one logical slot never mix identity and payload",
  async (t) => {
    const f = await nativeFixture(t);
    await NodeFSP.writeFile(
      f.baseFile,
      f.base.replace('"globalDateInEffect": "error"', '"globalDateInEffect": "off"'),
    );
    const alternative = NodePath.join(f.directory, "alternative.json");
    const config = JSON.parse(await NodeFSP.readFile(f.config, "utf8"));
    config.compilerOptions.noUnusedParameters = true;
    await NodeFSP.writeFile(alternative, JSON.stringify(config));
    const output = NodePath.join(f.directory, "joined.tsbuildinfo");
    const args = ["--noEmit", "--pretty", "false", "--tsBuildInfoFile", output];
    NodeAssert.deepEqual(
      await Promise.all([f.invoke(args), f.invoke([...args, "-p", alternative])]),
      [0, 0],
      f.outputs.map((entry) => entry.stdout).join("\n"),
    );
    const paths = statePaths(f.root, f.directory, output);
    const envelope = JSON.parse(await NodeFSP.readFile(paths.entry, "utf8"));
    NodeAssert.equal(envelope.schema, STATE_SCHEMA);
    NodeAssert.deepEqual(
      JSON.parse(Buffer.from(envelope.payload, "base64")),
      JSON.parse(await NodeFSP.readFile(output, "utf8")),
    );
    NodeAssert.equal(
      (await NodeFSP.readdir(paths.namespace)).some((name) =>
        /run\.json$|\.tmp$|\.lock$/.test(name),
      ),
      false,
    );
  },
);
NodeTest.test(
  "a changed output-parent inode preserves uncertain paths instead of deleting through its replacement",
  async (t) => {
    const f = await nativeFixture(t);
    const held = NodePath.join(f.root, "held-original-project");
    await NodeAssert.rejects(
      f.invoke(["--noEmit"], async (effective) => {
        const working = effective.at(-1);
        await NodeFSP.writeFile(working, samplePayload(working));
        await NodeFSP.rename(f.directory, held);
        await NodeFSP.mkdir(f.directory);
        await NodeFSP.copyFile(NodePath.join(held, "tsconfig.json"), f.config);
        return 0;
      }),
      /cleanup refused/,
    );
    NodeAssert.ok((await NodeFSP.readdir(held)).some((name) => name.startsWith(".jones-tsrs-")));
    NodeAssert.equal(
      (await NodeFSP.readdir(f.directory)).some((name) => name.startsWith(".jones-tsrs-")),
      false,
    );
    const namespace = statePaths(
      f.root,
      f.directory,
      NodePath.join(f.directory, "tsconfig.tsbuildinfo"),
    ).namespace;
    const accountName = (await NodeFSP.readdir(namespace)).find((name) =>
      name.endsWith(".run.json"),
    );
    NodeAssert.ok(accountName);
    NodeAssert.equal(
      JSON.parse(await NodeFSP.readFile(NodePath.join(namespace, accountName), "utf8")).phase,
      "unknown",
    );
  },
);

for (const mode of ["cli", "configuration"])
  NodeTest.test(
    `N01 composite ${mode} incremental false still qualifies private output`,
    async (t) => {
      const { root, directory, config } = await fixture(t);
      await NodeFSP.writeFile(
        config,
        JSON.stringify({
          extends: "../tsconfig.base.json",
          compilerOptions: { composite: true, incremental: mode === "cli" },
          files: ["source.ts"],
        }),
      );
      const args = mode === "cli" ? ["--incremental", "false"] : [];
      const selected = await projectConfiguration(root, directory, classifyArguments(args));
      NodeAssert.equal(selected.output, NodePath.join(directory, "tsconfig.tsbuildinfo"));
      for (const output of [
        NodePath.join(directory, ".jones-tsrs-caller.tsbuildinfo"),
        NodePath.join(root, "outside.tsbuildinfo"),
      ])
        await NodeAssert.rejects(
          projectConfiguration(
            root,
            directory,
            classifyArguments([...args, "--tsBuildInfoFile", output]),
          ),
          /Custom build-info output/,
        );
      const ordinary = JSON.parse(await NodeFSP.readFile(config, "utf8"));
      ordinary.compilerOptions.composite = false;
      await NodeFSP.writeFile(config, JSON.stringify(ordinary));
      NodeAssert.equal(
        (await projectConfiguration(root, directory, classifyArguments(args))).output,
        undefined,
      );
    },
  );

NodeTest.test(
  "N01 native composite false options retain diagnostics and guarded output",
  async (t) => {
    for (const mode of ["cli", "configuration"]) {
      const f = await nativeFixture(t);
      const config = JSON.parse(await NodeFSP.readFile(f.config, "utf8"));
      config.compilerOptions.composite = true;
      config.compilerOptions.incremental = mode === "cli";
      await NodeFSP.writeFile(f.config, JSON.stringify(config));
      await NodeFSP.writeFile(
        NodePath.join(f.directory, "source.ts"),
        'import { value } from "./value.ts";\nexport const result = value;\n',
      );
      const output = NodePath.join(f.directory, `${mode}.tsbuildinfo`);
      const args = [
        "--noEmit",
        "--pretty",
        "false",
        ...(mode === "cli" ? ["--incremental", "false"] : []),
        "--tsBuildInfoFile",
        output,
      ];
      NodeAssert.equal(await f.invoke(args), 2);
      NodeAssert.match(f.outputs.at(-1).stdout, /TS6379/);
      NodeAssert.match(f.log.at(-1), /\.jones-tsrs-[a-f0-9-]+\.tsbuildinfo/);
      const raw = JSON.parse(await NodeFSP.readFile(output, "utf8"));
      NodeAssert.equal(NodePath.resolve(f.directory, raw.options.tsBuildInfoFile), output);
      NodeAssert.equal(
        (await NodeFSP.readdir(f.directory)).some((name) => name.startsWith(".jones-tsrs-")),
        false,
      );
      const paths = statePaths(f.root, f.directory, output);
      await NodeAssert.rejects(NodeFSP.stat(paths.entry), { code: "ENOENT" });
      NodeAssert.equal(
        (await NodeFSP.readdir(paths.namespace)).some(
          (name) => name.endsWith(".run.json") || name.endsWith(".tmp") || name.endsWith(".lock"),
        ),
        false,
      );
      let spawned = false;
      await NodeAssert.rejects(
        f.invoke(
          [
            ...args,
            "--tsBuildInfoFile",
            NodePath.join(f.directory, ".jones-tsrs-caller.tsbuildinfo"),
          ],
          async () => {
            spawned = true;
            return 0;
          },
        ),
        /Custom build-info output/,
      );
      NodeAssert.equal(spawned, false);
    }
  },
);
