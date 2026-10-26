import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import { classifyArguments, projectConfiguration } from "./build-info.ts";
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
