// @effect-diagnostics nodeBuiltinImport:off - Packaging smoke drives the packaged executable through native Node adapters.
// Usage: node apps/server/src/jones/updates/preparation/packagedWorkerSmoke.ts --worker <executable> [--worker-entry <entry>]
// Proves a packaged server can run its own off-process update preparation worker: the protocol
// probe that staging requires of every Linux candidate, then real sweep and verify jobs and the
// integrity refusal of a tampered stage, all against a synthetic home. A single executable is
// started as itself; a script install (Electron's ELECTRON_RUN_AS_NODE) through its entry.
// It never reads real state, credentials or the network.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import { QualifiedRuntimeBlockedError } from "../../cloud/qualifiedRuntime.ts";
import { PREPARATION_COMMAND, preparationJobsDirectory, type PreparationJob } from "./protocol.ts";
import { adoptVerifiedStage } from "./rebind.ts";
import {
  makePreparationSupervisor,
  resolvePreparationWorkerCommand,
  type PreparationWorkerExit,
} from "./supervisor.ts";
import {
  BASELINE,
  fixtureHost,
  makePreparationHome,
  stageFixtureRuntime,
  VERSION,
} from "./Preparation.testkit.ts";

const fail = (step: string, detail: string): never => {
  throw new Error(`[preparation-smoke] ${step} failed: ${detail}`);
};

const { values } = NodeUtil.parseArgs({
  options: {
    worker: { type: "string" },
    "worker-entry": { type: "string" },
  },
});
const worker = values.worker ?? fail("arguments", "--worker <executable> is required");
const entry = values["worker-entry"];
const command =
  resolvePreparationWorkerCommand({
    isExecutable: entry === undefined,
    execPath: NodePath.resolve(worker),
    entryPath: entry,
    // An entry inside app.asar is visible only to Electron's own file system.
    entryExists: () => true,
  }) ?? fail("arguments", "--worker-entry must be absolute");

const probe = NodeChildProcess.spawnSync(
  command.file,
  [...command.args, PREPARATION_COMMAND, "--protocol"],
  { encoding: "utf8", timeout: 60_000 },
);
if (probe.status !== 0 || probe.stdout.trim() !== JSON.stringify({ protocol: 1 }))
  fail("protocol probe", `exit ${String(probe.status)}\n${probe.stdout}${probe.stderr}`);

const home = await makePreparationHome();
try {
  const staged = await stageFixtureRuntime(home, { bytes: 16 * 1024 * 1024 });
  const exits: PreparationWorkerExit[] = [];
  const supervisor = makePreparationSupervisor({
    baseDir: home.base,
    cacheRoot: home.cacheRoot,
    command,
    observe: (exit) => exits.push(exit),
  });
  const verify: PreparationJob = {
    kind: "verify",
    stagedHandle: staged.stagedHandle,
    activeVersion: BASELINE,
    host: fixtureHost,
  };
  const sweep = await supervisor.sweep();
  if (sweep.failed !== 0) fail("sweep job", JSON.stringify(sweep));
  const verified = await supervisor.run(verify);
  if (verified.kind !== "verify" || JSON.stringify(verified.staged) !== JSON.stringify(staged))
    fail("verify job", JSON.stringify(verified));
  await adoptVerifiedStage({
    baseDir: home.base,
    activeVersion: BASELINE,
    stagedHandle: staged.stagedHandle,
    staged: verified.staged,
  });
  await NodeFSP.writeFile(
    NodePath.join(home.base, "runtime", "versions", VERSION, "client", "index.html"),
    "tampered",
  );
  const refused = await supervisor.run(verify).then(
    () => undefined,
    (cause: unknown) => cause,
  );
  if (!(refused instanceof QualifiedRuntimeBlockedError && refused.reason === "integrity-mismatch"))
    fail("tampered verify job", String(refused));
  // The refusal queued a sweep; this one runs after it.
  await supervisor.sweep();
  const left = await NodeFSP.readdir(preparationJobsDirectory(home.base)).catch(() => []);
  if (left.length > 0) fail("job storage cleanup", left.join(", "));
  for (const exit of exits)
    if (exit.pid === undefined || exit.pid === process.pid || !exit.reaped || exit.priority !== 19)
      fail("worker isolation", JSON.stringify(exit));
  process.stdout.write(
    `[preparation-smoke] ${command.file} ${command.args.join(" ")}: protocol probe, sweep, verify and tamper refusal passed across ${String(exits.length)} reaped workers at priority 19.\n`,
  );
} finally {
  await home.remove();
}
