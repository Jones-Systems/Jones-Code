import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

export const installedProducer = "47cad979dcb769e511581e6de9e1aa5be99fd461";

const preparation = new URL("../../../apps/server/src/jones/updates/preparation/", import.meta.url);
const absent = (pid) => {
  try {
    process.kill(pid, 0);
    return false;
  } catch (cause) {
    return cause.code === "ESRCH";
  }
};

// Observe the stand-in transfer's durable PID receipt, rather than sleeping until it probably ran.
function transferMilestone(actions, signal) {
  let watcher;
  let abort;
  const promise = new Promise((resolve, reject) => {
    abort = () => reject(signal.reason);
    const read = async () => {
      try {
        const text = await NodeFSP.readFile(actions.pids, "utf8");
        const match = /^(\d+) (\d+)\n$/.exec(text);
        if (match) resolve({ tool: Number(match[1]), worker: Number(match[2]) });
      } catch (cause) {
        if (cause.code !== "ENOENT") reject(cause);
      }
    };
    watcher = NodeFS.watch(NodePath.dirname(actions.pids), () => void read());
    watcher.on("error", reject);
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
    else void read();
  });
  return promise.finally(() => {
    watcher?.close();
    signal.removeEventListener("abort", abort);
  });
}

export async function runSyntheticTransferRegression() {
  const kit = await import(new URL("Preparation.testkit.ts", preparation));
  const { makePreparationSupervisor } = await import(new URL("supervisor.ts", preparation));
  const { preparationJobsDirectory } = await import(new URL("protocol.ts", preparation));
  const home = await kit.makePreparationHome();
  const controller = new AbortController();
  const deadline = AbortSignal.timeout(60_000);
  const onDeadline = () => controller.abort(deadline.reason);
  deadline.addEventListener("abort", onDeadline, { once: true });
  let supervisor;
  let running;
  let restaging;
  let milestone;
  const exits = [];
  let failure;
  let report;
  let cleanupProved = false;
  try {
    const actions = await kit.actionsFixture(home, { hang: true });
    supervisor = makePreparationSupervisor({
      baseDir: home.base,
      cacheRoot: home.cacheRoot,
      command: kit.sourceWorkerCommand,
      env: actions.env,
      observe: (exit) => exits.push(exit),
      limits: { graceMs: 1_000, reapMs: 5_000 },
    });
    const job = (candidate) => ({
      kind: "stage",
      candidate,
      cacheRoot: home.cacheRoot,
      dbPath: home.dbPath,
      activeVersion: kit.BASELINE,
      host: kit.fixtureHost,
    });
    const jobId = NodeCrypto.randomUUID();
    milestone = transferMilestone(actions, controller.signal);
    void milestone.catch(() => undefined);
    running = supervisor.start(job(actions.candidate), { jobId, signal: controller.signal });
    const outcome = running.outcome.catch((error) => error);
    const captured = await milestone;
    NodeAssert.ok(!absent(captured.tool) && !absent(captured.worker));
    const lifecycleRoot = NodePath.join(home.base, "runtime", "jones-preparation", "lifecycle");
    const effects = JSON.parse(
      await NodeFSP.readFile(NodePath.join(lifecycleRoot, jobId + ".effects.json")),
    );
    NodeAssert.equal(effects.phase, "transfer");
    NodeAssert.equal(effects.pid, captured.worker);
    controller.abort(new Error("Synthetic interrupted transfer"));
    NodeAssert.match((await outcome).message, /Synthetic interrupted transfer/);
    NodeAssert.equal((await running.settled).kind, "stopped");
    NodeAssert.ok(absent(captured.tool) && absent(captured.worker) && absent(-captured.worker));
    const sweep = await supervisor.sweep();
    NodeAssert.equal(sweep.failed, 0);
    NodeAssert.deepEqual(sweep.retainedJobIds ?? [], []);
    NodeAssert.deepEqual(await NodeFSP.readdir(preparationJobsDirectory(home.base)), []);
    NodeAssert.deepEqual(
      await NodeFSP.readdir(NodePath.join(home.cacheRoot, "attempts")).catch((cause) => {
        if (cause.code === "ENOENT") return [];
        throw cause;
      }),
      [],
    );
    const lifecycle = JSON.parse(
      await NodeFSP.readFile(NodePath.join(lifecycleRoot, jobId + ".json")),
    );
    NodeAssert.equal(lifecycle.state, "released");
    NodeAssert.equal(lifecycle.worker.processGroup, captured.worker);

    // Same isolated home and candidate, now with the transfer tool answering normally.
    const available = await kit.actionsFixture(home);
    NodeAssert.equal(available.candidate.artifactDigest, actions.candidate.artifactDigest);
    restaging = supervisor.start(job(available.candidate), {
      jobId: NodeCrypto.randomUUID(),
      signal: deadline,
    });
    const staged = await restaging.outcome;
    NodeAssert.equal((await restaging.settled).kind, "completed");
    NodeAssert.equal(staged.kind, "stage");
    NodeAssert.equal(staged.staged.receipt.sourceSha, available.candidate.source);
    report = {
      status: "passed",
      fixture: "synthetic-actions-and-installed-binding",
      worker: kit.sourceWorkerCommand,
      installedBinding: kit.INSTALLED,
      artifactDigest: available.candidate.artifactDigest,
      payloadSha256: staged.staged.receipt.archiveSha256,
      milestone: "durable-transfer-phase-and-live-tool",
      captured,
      groupAbsent: true,
      scratchCleaned: true,
      restaged: true,
      packagedProof: false,
    };
  } catch (cause) {
    failure = cause;
  } finally {
    controller.abort(new Error("Fixture cleanup"));
    try {
      if (milestone) await milestone.catch(() => undefined);
      if (running) await running.settled;
      if (restaging) await restaging.settled;
      if (supervisor) {
        const swept = await supervisor.sweep();
        NodeAssert.equal(swept.failed, 0);
        NodeAssert.deepEqual(await supervisor.unreleasedJobs(), []);
      }
      for (const exit of exits) {
        NodeAssert.equal(exit.reaped, true);
        NodeAssert.ok(exit.pid > 1 && absent(exit.pid) && absent(-exit.pid));
      }
      cleanupProved = true;
    } catch (cause) {
      failure = new AggregateError(
        [failure, cause].filter(Boolean),
        `Cleanup unproved; retained ${home.root}`,
      );
    }
    deadline.removeEventListener("abort", onDeadline);
    if (cleanupProved) {
      try {
        await home.remove();
      } catch (cause) {
        failure = new AggregateError(
          [failure, cause].filter(Boolean),
          `Fixture removal failed at ${home.root}`,
        );
      }
    }
  }
  if (failure) throw failure;
  return report;
}

export function packagedRows() {
  // These are prerequisites, not skipped tests or evidence that either artifact was executed.
  return ["transfer-interruption", "qualified-native-cleanup", "abrupt-native-kill"].map(
    (scenario) => ({
      scenario,
      status: "unavailable",
      packagedProof: false,
      installedProducer,
      reason:
        "No bound packaged execution with a controlled interruption milestone is supplied. Synthetic testkit identities cannot qualify an installed producer.",
      prerequisites: [
        "Actual producer executable and entry bound to source/tree, Actions archive and payload digests",
        "Captured worker/group and durable transfer or native-effects interruption milestone",
        "Exact request/result/lifecycle/effects and current recovery-code digests",
        "Qualified native row additionally requires retained busy result, qualified producer contract, inspect/consent/proof and real restage",
      ],
    }),
  );
}

async function main() {
  const reportPath = process.argv[2];
  NodeAssert.ok(
    reportPath && NodePath.isAbsolute(reportPath),
    "An absolute new report path is required.",
  );
  let failure;
  let syntheticTransfer;
  try {
    syntheticTransfer = await runSyntheticTransferRegression();
  } catch (cause) {
    failure = cause;
    syntheticTransfer = { status: "failed", packagedProof: false, reason: cause.message };
  }
  const report = {
    schema: "jones.interrupted-staging/v1",
    syntheticTransfer,
    recoveryCodeSha256: NodeCrypto.createHash("sha256")
      .update(await NodeFSP.readFile(new URL("nativeEffectsRecovery.ts", preparation)))
      .digest("hex"),
    packaged: packagedRows(),
    packagedQualification: "unavailable",
  };
  await NodeFSP.writeFile(reportPath, JSON.stringify(report, null, 2) + "\n", {
    flag: "wx",
    mode: 0o600,
  });
  if (failure) throw failure;
  process.stdout.write(
    "Synthetic interrupted transfer passed; packaged interrupted-staging qualification remains unavailable.\n",
  );
}

if (process.argv[1] && NodePath.resolve(process.argv[1]) === NodeURL.fileURLToPath(import.meta.url))
  await main();
