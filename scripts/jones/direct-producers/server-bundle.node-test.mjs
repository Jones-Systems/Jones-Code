// The native fake evaluates the real checked-in config, then checks ownership
// before its first output write. It never invokes the actual bundler.
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeChildProcess from "node:child_process";
import * as NodeTest from "node:test";
import { finiteFixture, finiteScratchReceipts } from "./finite-production.testkit.ts";

const sourceRoot = NodePath.resolve(NodeURL.fileURLToPath(new URL("../../..", import.meta.url)));

async function copyModule(relative, destination, copied = new Set()) {
  if (copied.has(relative)) return;
  copied.add(relative);
  const input = NodePath.resolve(sourceRoot, relative);
  NodeAssert.ok(input.startsWith(sourceRoot + NodePath.sep));
  const bytes = await NodeFSP.readFile(input);
  const output = NodePath.join(destination, relative);
  await NodeFSP.mkdir(NodePath.dirname(output), { recursive: true });
  await NodeFSP.writeFile(output, bytes);
  if (!/\.[cm]?[jt]s$/.test(relative)) return;
  for (const match of bytes
    .toString()
    .matchAll(/(?:from\s*|import\s*\(\s*|import\s*)["']([^"']+)["']/g)) {
    if (!match[1].startsWith(".")) continue;
    const next = NodePath.relative(sourceRoot, NodePath.resolve(NodePath.dirname(input), match[1]));
    await copyModule(next, destination, copied);
  }
}

async function runRoute(worktree, args, scenario, records) {
  const base = NodePath.dirname(worktree);
  const tmp = NodePath.join(base, "native-tmp");
  await NodeFSP.mkdir(tmp);
  const bin = NodePath.join(base, "fake-bin");
  await NodeFSP.mkdir(bin);
  const events = NodePath.join(base, "native-events.json");
  const entered = NodePath.join(base, "config-entered");
  const hook = NodePath.join(base, "resolve.mjs");
  await NodeFSP.writeFile(
    hook,
    `
import { registerHooks } from "node:module";
import * as fs from "node:fs";
const fixturePrefix = ${JSON.stringify(NodeURL.pathToFileURL(worktree + NodePath.sep).href)};
const qualifiedParent = ${JSON.stringify(NodeURL.pathToFileURL(NodePath.join(sourceRoot, "scripts/native-fixture.mjs")).href)};
if(process.argv[1]?.endsWith("/server-bundle.ts") || process.argv[1]?.endsWith("/apps/server/scripts/cli.ts"))
  fs.writeFileSync(${JSON.stringify(NodePath.join(base, "cli-pid"))}, String(process.pid));
registerHooks({
 resolve(specifier, context, next) {
  if(specifier.endsWith("/completion.ts") || specifier === "./completion.ts")
   return {url:"data:text/javascript,export const completeProducer=()=>{throw Error('optional capture unavailable')}",shortCircuit:true};
  if(context.parentURL?.startsWith(fixturePrefix) && !specifier.startsWith(".") && !specifier.startsWith("/") && !specifier.startsWith("node:") && !specifier.startsWith("file:") && !specifier.startsWith("data:"))
   return next(specifier,{...context,parentURL:qualifiedParent});
  return next(specifier,context);
 }
});
`,
  );
  await NodeFSP.writeFile(
    NodePath.join(bin, "vp"),
    `#!${process.execPath}
import * as fs from "node:fs";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { spawn } from "node:child_process";
import assert from "node:assert/strict";
assert.equal(fs.existsSync(${JSON.stringify(entered)}),false,"bundler must never replay");
fs.writeFileSync(${JSON.stringify(entered)}, "entered");
const config=(await import(pathToFileURL(path.resolve("vite.config.ts")).href)).default;
assert.equal(config.pack.outDir,"dist");
assert.equal(config.pack.clean,true);
assert.equal(config.pack.copy,undefined);
assert.equal(config.pack.hooks,undefined);
assert.equal(config.pack.outputOptions,undefined);
assert.equal(config.pack.exe,undefined);
assert.deepEqual(config.pack.entry,["src/bin.ts","src/claude-history-worker.ts"]);
assert.equal(config.pack.sourcemap,true);
assert.deepEqual(config.pack.plugins.map(p=>p.name),["jones-server-compute-build-identity"]);
const root=path.resolve("dist");
const gitDir=fs.readFileSync(path.resolve("../../.git"),"utf8").trim().slice(8);
const directory=path.join(gitDir,"workspace-output-obligations");
const names=fs.readdirSync(directory);
assert.equal(names.length,1);
const record=JSON.parse(fs.readFileSync(path.join(directory,names[0]),"utf8"));
assert.equal(record.producer,${JSON.stringify(scenario)}==="full-build"?"jones-server-build":"jones-server-bundle");
assert.equal(record.status,"active");
assert.deepEqual(record.roots,[root]);
assert.equal(record.holders.version,1);
assert.equal(record.holders.in_process,"running");
assert.equal(record.holders.admission_closed,false);
assert.equal(record.holders.spawns.length,1);
assert.ok(["pending","session"].includes(record.holders.spawns[0].state));
assert.equal(record.binding.path,path.resolve("../.."));
assert.ok(record.holders.self);
fs.mkdirSync(root,{recursive:true});
fs.writeFileSync(path.join(root,"bin.mjs"),"export {};\\n");
fs.writeFileSync(${JSON.stringify(events)},JSON.stringify({pid:process.pid,ppid:process.ppid,record,argv:process.argv.slice(2)}));
if(${JSON.stringify(scenario)}==="cancel")console.log("BUNDLE_FIXTURE_READY");
if(${JSON.stringify(scenario)}==="pending-writer"){
 const child=spawn(process.execPath,["-e",${JSON.stringify("setTimeout(()=>{require('node:fs').writeFileSync(" + JSON.stringify(NodePath.join(worktree, "apps/server/dist/late")) + ",'late');},100)")}],{stdio:["ignore","inherit","inherit"],detached:true});
 fs.writeFileSync(${JSON.stringify(NodePath.join(base, "writer-pid"))},String(child.pid));
 process.exit(0);
}
if(${JSON.stringify(scenario)}==="cancel"){ setInterval(()=>{},1000); setTimeout(()=>process.exit(99),5000); }
else process.exit(["failed","failed-direct"].includes(${JSON.stringify(scenario)})?7:0);
`,
  );
  await NodeFSP.chmod(NodePath.join(bin, "vp"), 0o700);
  const env = {
    PATH:
      scenario === "spawn-failed"
        ? NodePath.dirname(process.execPath) + ":/usr/bin:/bin"
        : bin +
          NodePath.delimiter +
          NodePath.dirname(process.execPath) +
          NodePath.delimiter +
          "/usr/bin:/bin",
    HOME: base,
    TMPDIR: tmp,
    NODE_DISABLE_COMPILE_CACHE: "1",
    NODE_OPTIONS: "--import=" + hook,
    VP_REMOTE_CACHE: "off",
    VP_REMOTE_CACHE_URL: "",
    CI: "1",
    T3CODE_DEV_SEED: "0",
  };
  let child;
  let cancellation;
  let signalled;
  try {
    return await new Promise((resolve, reject) => {
      child = NodeChildProcess.execFile(
        process.execPath,
        scenario === "failed-direct"
          ? [NodePath.join(worktree, "scripts/jones/direct-producers/server-bundle.ts"), ...args]
          : scenario === "full-build"
            ? [NodePath.join(worktree, "apps/server/scripts/cli.ts"), "build"]
            : ["--run", "build:bundle", "--", ...args],
        {
          cwd: NodePath.join(worktree, "apps/server"),
          env,
          timeout: 15000,
          maxBuffer: 65536,
        },
        async (error, stdout, stderr) => {
          try {
            await cancellation;
            const observed = await NodeFSP.readFile(events, "utf8")
              .then(JSON.parse)
              .catch((error) => {
                if (error.code === "ENOENT") return null;
                throw error;
              });
            resolve({
              error,
              stdout,
              stderr,
              signalled,
              cliPid: await NodeFSP.readFile(NodePath.join(base, "cli-pid"), "utf8"),
              observed,
              entered: await NodeFSP.stat(entered).then(
                () => true,
                () => false,
              ),
              records: await records(),
            });
          } catch (failure) {
            reject(failure);
          }
        },
      );
      if (scenario === "cancel")
        child.stdout.on("data", (data) => {
          if (!data.toString().includes("BUNDLE_FIXTURE_READY")) return;
          cancellation ??= NodeFSP.readFile(NodePath.join(base, "cli-pid"), "utf8").then((pid) => {
            signalled = Number(pid);
            process.kill(signalled, "SIGTERM");
          });
        });
    });
  } finally {
    NodeAssert.ok(child?.exitCode !== null || child?.signalCode !== null);
  }
}

for (const scenario of [
  "succeeded",
  "full-build",
  "failed",
  "failed-direct",
  "pending-writer",
  "spawn-failed",
  "cancel",
]) {
  NodeTest.test("actual managed package route: " + scenario, () =>
    finiteFixture(async ({ worktree, root, records }) => {
      await copyModule("scripts/jones/direct-producers/server-bundle.ts", worktree);
      await copyModule("apps/server/vite.config.ts", worktree);
      const result = await runRoute(worktree, [], scenario, records);
      if (scenario !== "spawn-failed") {
        NodeAssert.ok(result.observed, result.stderr);
        NodeAssert.deepEqual(result.observed.argv, ["pack"]);
        NodeAssert.equal(result.records.length, 1);
      }
      if (scenario === "succeeded" || scenario === "full-build" || scenario === "pending-writer") {
        NodeAssert.equal(result.error, null, result.stderr);
        NodeAssert.equal(result.records[0].status, "succeeded");
        if (scenario === "pending-writer")
          NodeAssert.equal(await NodeFSP.readFile(NodePath.join(root, "late"), "utf8"), "late");
      } else {
        NodeAssert.ok(result.error);
        if (scenario === "failed" || scenario === "failed-direct") {
          NodeAssert.equal(
            result.error.code,
            scenario === "failed-direct" ? 7 : 1,
            JSON.stringify(result),
          );
          NodeAssert.match(result.stdout, /non-zero exit code \(7\)/);
          NodeAssert.equal(result.records[0].status, "failed");
        } else {
          NodeAssert.equal(result.records[0].status, "active", JSON.stringify(result));
          if (scenario === "cancel") {
            NodeAssert.ok(Number.isSafeInteger(result.signalled) && result.signalled > 0);
            NodeAssert.equal(result.signalled, Number(result.cliPid));
          }
        }
      }
      NodeAssert.equal(result.records[0].outcome, "unresolved");
      NodeAssert.ok(result.records[0].holders.spawns.length === 1);
      NodeAssert.match(
        result.stderr,
        scenario === "full-build" ? /id=jones-server-build-/ : /id=jones-server-bundle-/,
      );
      if (scenario === "succeeded" || scenario === "failed" || scenario === "failed-direct")
        NodeAssert.match(result.stderr, /optional_capture_unavailable/);
    }),
  );
}

NodeTest.test(
  "managed extra arguments refuse before imports, config, output or obligation creation",
  () =>
    finiteFixture(async ({ worktree, root, records }) => {
      await copyModule("scripts/jones/direct-producers/server-bundle.ts", worktree);
      await copyModule("apps/server/package.json", worktree);
      for (const args of [
        ["--no-config"],
        ["--out-dir", "elsewhere"],
        ["--outDir", "elsewhere"],
        ["entry.ts"],
        ["--help"],
        ["--"],
      ]) {
        const child = await new Promise((resolve) =>
          NodeChildProcess.execFile(
            process.execPath,
            ["--run", "build:bundle", "--", ...args],
            {
              cwd: NodePath.join(worktree, "apps/server"),
              env: {
                PATH: NodePath.dirname(process.execPath) + ":/usr/bin:/bin",
                HOME: NodePath.dirname(worktree),
              },
              timeout: 10000,
              maxBuffer: 16384,
            },
            (error, stdout, stderr) => resolve({ error, stdout, stderr }),
          ),
        );
        NodeAssert.ok(child.error);
        NodeAssert.match(child.stderr, /Managed build:bundle accepts no arguments/);
        NodeAssert.doesNotMatch(child.stderr, /ERR_MODULE_NOT_FOUND/);
        NodeAssert.equal(
          await NodeFSP.stat(root).then(
            () => true,
            () => false,
          ),
          false,
        );
        NodeAssert.deepEqual(await records(), []);
      }
    }),
);

NodeTest.after(() => NodeAssert.ok(finiteScratchReceipts.every((receipt) => receipt.cleaned)));
