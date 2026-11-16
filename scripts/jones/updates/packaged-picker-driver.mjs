/* oxlint-disable t3code/no-global-process-runtime -- CI-native adapter verifies its own runtime before package execution. */
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeModule from "node:module";
import * as NodeChildProcess from "node:child_process";
import * as NodeNet from "node:net";
import * as NodeHttp from "node:http";
import * as NodeHttps from "node:https";
import * as NodeTls from "node:tls";
import * as NodeDgram from "node:dgram";
import * as NodeWorkerThreads from "node:worker_threads";
import {
  createPickerFixture,
  createPickerObservations,
  runPickerScenario,
} from "./packaged-picker-scenario.mjs";
const require = NodeModule.createRequire(import.meta.url);

export function run(config) {
  const { app } = require("electron");
  const loads = [],
    effects = [],
    violations = [];
  const fail = (name) => {
    violations.push(name);
    throw new Error(`Forbidden picker effect: ${name}`);
  };
  if (
    process.platform !== "darwin" ||
    process.arch !== "arm64" ||
    process.versions.electron !== "44.10.1"
  )
    throw new Error("Native picker requires Electron44.10.1 darwin/arm64.");
  if (
    process.env.HOME !== config.osHome ||
    process.env.TMPDIR !== config.temp ||
    process.env.PATH !== "/usr/bin:/bin" ||
    process.env.NODE_OPTIONS ||
    process.env.ELECTRON_RUN_AS_NODE ||
    Object.keys(process.env).some((k) => /TOKEN|SECRET|PASSWORD|CREDENTIAL|API_KEY/.test(k))
  )
    throw new Error("Picker environment is not the controlled synthetic environment.");
  if (
    NodeFS.realpathSync(config.root) !== config.root ||
    NodeFS.readFileSync(NodePath.join(config.root, ".picker-owner"), "utf8") !== config.nonce ||
    !config.report.startsWith(config.root + NodePath.sep)
  )
    throw new Error("Picker driver ownership mismatch.");
  const allowedLoads = new Set([config.main, config.driver]);
  const originalLoad = NodeModule.default._load;
  NodeModule.default._load = function (request, parent, ...rest) {
    loads.push(request);
    if (!NodeModule.isBuiltin(request) && request !== "electron" && !allowedLoads.has(request))
      fail(`module:${request}`);
    return Reflect.apply(originalLoad, this, [request, parent, ...rest]);
  };
  const originalExec = NodeChildProcess.default.execFile;
  NodeChildProcess.default.execFile = function (file, args, ...rest) {
    effects.push({ kind: "execFile", file, args });
    const expected = [
      "-convert",
      "json",
      "-o",
      "-",
      NodePath.join(config.appPath, "Contents/Info.plist"),
    ];
    if (file !== "/usr/bin/plutil" || JSON.stringify(args) !== JSON.stringify(expected))
      fail("child-process");
    return Reflect.apply(originalExec, this, [file, args, ...rest]);
  };
  for (const key of ["spawn", "spawnSync", "exec", "execSync", "execFileSync", "fork"])
    NodeChildProcess.default[key] = () => fail(`child:${key}`);
  for (const [target, keys] of [
    [NodeNet.default, ["connect", "createConnection", "createServer"]],
    [NodeHttp.default, ["request", "get", "createServer"]],
    [NodeHttps.default, ["request", "get", "createServer"]],
    [NodeTls.default, ["connect", "createServer"]],
    [NodeDgram.default, ["createSocket"]],
  ])
    for (const key of keys) target[key] = () => fail(`network:${key}`);
  NodeWorkerThreads.default.Worker = function ForbiddenWorker() {
    fail("worker-thread");
  };
  globalThis.fetch = () => fail("fetch");
  const originalKill = process.kill;
  process.kill = (pid, signal) => {
    if (signal !== 0) fail("signal");
    return originalKill(pid, signal);
  };
  NodeModule.syncBuiltinESMExports();
  const snapshot = () => {
    const rows = [];
    const walk = (dir) => {
      for (const name of NodeFS.readdirSync(dir)) {
        const file = NodePath.join(dir, name),
          stat = NodeFS.lstatSync(file);
        if (stat.isSymbolicLink()) fail("owned-state-symlink");
        rows.push({
          path: NodePath.relative(config.root, file),
          bytes: stat.size,
          directory: stat.isDirectory(),
        });
        if (stat.isDirectory() && rows.length < 1000) walk(file);
      }
    };
    walk(config.home);
    return rows;
  };
  const snapshots = [{ step: "before-import", tree: snapshot(), effects: effects.length }];
  const entry = require(config.main);
  snapshots.push({ step: "import", tree: snapshot(), effects: effects.length });
  const paths = entry.prepare(config);
  snapshots.push({ step: "prepare", tree: snapshot(), effects: effects.length });
  // prepare executes synchronously before the first ready await.
  return (async () => {
    let host;
    try {
      await app.whenReady();
      snapshots.push({ step: "ready", tree: snapshot(), effects: effects.length });
      const observations = createPickerObservations(),
        fixture = createPickerFixture(config.source, () => observations.notify());
      host = await entry.createHost({
        ...config,
        transport: fixture.transport,
        stagingWorker: fixture.worker,
        onPlan: () => observations.notify(),
      });
      snapshots.push({ step: "registered-window", tree: snapshot(), effects: effects.length });
      const report = await runPickerScenario(
        config.phase,
        {
          ...host,
          waitForJobs: (count) =>
            observations.wait(
              () => fixture.jobs.length >= count,
              async () => fixture.jobs,
            ),
          waitForPlan: (predicate) =>
            observations.wait(predicate, () => host.bridgeCall("installPlan", [{ type: "read" }])),
          assertNoTransactions: async () => {
            try {
              const rows = await NodeFSP.readdir(
                NodePath.join(config.home, "runtime/jones-updates/transactions"),
              );
              if (rows.length) fail("transactions");
            } catch (error) {
              if (error.code !== "ENOENT") throw error;
            }
          },
        },
        fixture,
      );
      snapshots.push({ step: "scenario-complete", tree: snapshot(), effects: effects.length });
      if (violations.length) throw new Error("Picker effect violation.");
      const { contextIsolation, nodeIntegration, sandbox } = host.preferences;
      NodeFS.writeFileSync(
        config.report,
        JSON.stringify(
          {
            ...report,
            status: "pass",
            nativeHop: true,
            paths,
            preferences: { contextIsolation, nodeIntegration, sandbox },
            loads,
            effects,
            violations,
            snapshots,
            network: host.network,
            nativeCalls: host.nativeCalls,
            installCalls: host.installCalls(),
          },
          null,
          2,
        ),
      );
      if (config.phase !== "inflight") await host.close();
      app.exit(0);
    } catch (error) {
      NodeFS.writeFileSync(
        config.report,
        JSON.stringify(
          {
            phase: config.phase,
            status: "fail",
            error: String(error),
            loads,
            effects,
            violations,
            snapshots,
          },
          null,
          2,
        ),
      );
      // Unknown effects are left to the owning orchestrator's cleanup proof.
      app.exit(1);
    }
  })();
}
