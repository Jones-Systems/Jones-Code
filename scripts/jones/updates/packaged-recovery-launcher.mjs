import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodeAssert from "node:assert/strict";
import * as NodeSqlite from "node:sqlite";
import * as NodeHttp from "node:http";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeURL from "node:url";
import { runNativeFixtureCommand } from "./native-fixture-support.mjs";

const preload = NodeURL.fileURLToPath(new URL("./packaged-recovery-preload.cjs", import.meta.url));
const fail = (message) => {
  throw new Error("Packaged launcher preload probe refused: " + message);
};

async function digest(filename) {
  const hash = NodeCrypto.createHash("sha256");
  for await (const bytes of NodeFS.createReadStream(filename)) hash.update(bytes);
  return hash.digest("hex");
}

async function executableIdentity(filename) {
  const stat = await NodeFSP.lstat(filename, { bigint: true });
  if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o111n) === 0n)
    fail("regular executable required");
  return Object.fromEntries(
    ["dev", "ino", "size", "mtimeNs", "ctimeNs"].map((key) => [key, String(stat[key])]),
  );
}

async function writePrivate(filename, value) {
  const handle = await NodeFSP.open(filename, "wx", 0o600);
  try {
    await handle.writeFile(JSON.stringify(value) + "\n");
    await handle.sync();
  } finally {
    await handle.close();
  }
}

export async function probePackagedLauncherPreload({ root, executable, sha256, signal }) {
  signal?.throwIfAborted();
  // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone fixture binds its actual host without loading application services.
  if (process.platform !== "linux") fail("Linux required");
  if (!/^[a-f0-9]{64}$/.test(sha256)) fail("exact executable digest required");
  if (
    !NodePath.isAbsolute(root) ||
    (await NodeFSP.realpath(root)) !== root ||
    !NodePath.basename(root).startsWith("jones-packaged-recovery-")
  )
    fail("dedicated physical root required");
  const allocation = await NodeFSP.lstat(root);
  if (
    !allocation.isDirectory() ||
    allocation.uid !== process.getuid() ||
    (allocation.mode & 0o077) !== 0 ||
    (await NodeFSP.readdir(root)).length !== 0
  )
    fail("empty private owned root required");
  if (
    !NodePath.isAbsolute(executable) ||
    (await NodeFSP.realpath(executable)) !== executable ||
    executable.startsWith(root + NodePath.sep)
  )
    fail("separate physical executable required");
  const identity = await executableIdentity(executable);
  if ((await digest(executable)) !== sha256) fail("executable digest differs");

  const home = NodePath.join(root, "home");
  await NodeFSP.mkdir(NodePath.join(home, "runtime"), { recursive: true, mode: 0o700 });
  await writePrivate(NodePath.join(home, "runtime", "service-state.json"), {});
  const args = ["__service-launcher"];
  const run = (enabled) =>
    runNativeFixtureCommand(executable, args, root, {
      signal,
      spawn: (file, argv, options) =>
        NodeChildProcess.spawn(file, argv, {
          ...options,
          env: {
            ...options.env,
            T3CODE_HOME: home,
            ...(enabled
              ? {
                  NODE_OPTIONS: "--require " + JSON.stringify(preload),
                  JONES_PACKAGED_RECOVERY_FIXTURE: NodePath.join(root, "fault-plan.json"),
                }
              : {}),
          },
        }),
    });
  const baseline = await run(false);
  if (
    baseline.code !== 1 ||
    baseline.stoppedBy !== null ||
    baseline.stderr.includes("Injected packaged recovery")
  )
    fail("unarmed invalid-state launcher did not refuse normally");

  const fixtureId = NodeCrypto.randomUUID().replaceAll("-", "");
  const operationId = NodeCrypto.randomUUID();
  await writePrivate(NodePath.join(root, "fixture-owner.json"), {
    protocol: 1,
    purpose: "jones-packaged-recovery",
    fixtureId,
    executable,
    identity,
  });
  await writePrivate(NodePath.join(root, "fault-plan.json"), {
    protocol: 1,
    fixtureId,
    executable,
    identity,
    method: "readFile-before",
    target: "runtime/service-state.json",
    arm: "immediate",
    operationId,
  });
  const injected = await run(true);
  if (
    injected.code !== 1 ||
    injected.stoppedBy !== null ||
    !injected.stderr.includes("Injected packaged recovery readFile-before")
  )
    fail("packaged launcher did not execute the external preload");
  const claim = JSON.parse(await NodeFSP.readFile(NodePath.join(root, "fault-fired.json"), "utf8"));
  if (
    claim.protocol !== 1 ||
    claim.fixtureId !== fixtureId ||
    claim.operationId !== operationId ||
    claim.method !== "readFile-before" ||
    claim.target !== "runtime/service-state.json"
  )
    fail("durable fault claim differs");
  if (
    JSON.stringify(await executableIdentity(executable)) !== JSON.stringify(identity) ||
    (await digest(executable)) !== sha256
  )
    fail("executable changed during probe");

  const result = {
    schema: "jones.packaged-launcher-preload-probe/v1",
    scope: "external-preload-before-invalid-service-state",
    executable,
    sha256,
    identity,
    command: { executable, args },
    baseline,
    injected,
    claim,
    artifactUnchanged: true,
    packagedRecoveryMatrixPassed: false,
    processIdentityScope: "captured-child-handle-only",
    externalDescendantSettlementRequired: true,
  };
  await writePrivate(NodePath.join(root, "preload-probe.json"), result);
  return result;
}

export const settlementRestartPhases = [
  "outcome",
  "cleanup",
  "written",
  "file-synced",
  "linked",
  "directory-synced",
  "native-state",
];

async function boundedRecord(filename) {
  const handle = await NodeFSP.open(
    filename,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  );
  try {
    const stat = await handle.stat();
    if (
      !stat.isFile() ||
      stat.uid !== process.getuid() ||
      (stat.mode & 0o077) !== 0 ||
      stat.size > 65536
    )
      fail("unsafe recovery evidence");
    const bytes = await handle.readFile();
    return { bytes, value: JSON.parse(bytes.toString("utf8")) };
  } finally {
    await handle.close();
  }
}

export async function exercisePackagedSettlementRestart({ prepared, phase, signal }) {
  signal?.throwIfAborted();
  // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone fixture binds its actual host without loading application services.
  if (process.platform !== "linux" || !settlementRestartPhases.includes(phase))
    fail("unsupported settlement restart phase");
  const { root, baseDir, operationId, launcher } = prepared;
  if (
    prepared.schema !== "jones.packaged-recovery-fixture/v1" ||
    prepared.kind !== "pending" ||
    prepared.recoveryPayload !== true ||
    prepared.archivedOutcome !== false ||
    baseDir !== NodePath.join(root, "home") ||
    (await NodeFSP.realpath(root)) !== root ||
    !NodePath.basename(root).startsWith("jones-packaged-recovery-")
  )
    fail("fresh pending recovery fixture required");
  const allocation = await NodeFSP.lstat(root);
  if (
    !allocation.isDirectory() ||
    allocation.uid !== process.getuid() ||
    (allocation.mode & 0o077) !== 0 ||
    allocation.dev !== prepared.allocation.dev ||
    allocation.ino !== prepared.allocation.ino
  )
    fail("fixture allocation differs");
  NodeAssert.deepEqual(
    (await boundedRecord(NodePath.join(root, "prepared-fixture.json"))).value,
    prepared,
  );
  NodeAssert.deepEqual(
    (await boundedRecord(NodePath.join(baseDir, "runtime", "service-state.json"))).value,
    prepared.state,
  );
  if (
    (await NodeFSP.realpath(launcher.executable)) !== launcher.executable ||
    launcher.executable !== NodePath.join(prepared.inputs.candidate.payloadDirectory, "t3") ||
    (await digest(launcher.executable)) !== launcher.sha256
  )
    fail("launcher executable differs");
  NodeAssert.deepEqual(await executableIdentity(launcher.executable), launcher.identity);

  const operations = NodePath.join(baseDir, "runtime", "jones-update-operations");
  const reservation = await boundedRecord(NodePath.join(operations, operationId + ".json"));
  const binding = prepared.staged.binding;
  NodeAssert.deepEqual(reservation.value, {
    schema: 1,
    operationId,
    binding: {
      environmentId: binding.environmentId,
      currentVersion: binding.activeVersion,
      expectedInstalledSource: binding.activeSourceSha,
      targetSource: prepared.staged.receipt.sourceSha,
      stagedHandle: prepared.staged.stagedHandle,
      baseDir,
      dbPath: binding.dbPath,
      targetVersion: prepared.staged.receipt.version,
    },
  });
  const databaseBefore = await digest(binding.dbPath);
  const fault = {
    outcome: ["outcome-sync-after", "runtime/jones-update-operations"],
    cleanup: ["unlink-after", "runtime/db-backup/" + operationId + "/database"],
    written: ["write-after", "runtime/jones-update-settlements.staging"],
    "file-synced": ["sync-after", "runtime/jones-update-settlements.staging"],
    linked: ["link-after", "runtime/jones-update-settlements/" + operationId + ".json"],
    "directory-synced": ["directory-sync-after", "runtime/jones-update-settlements"],
    "native-state": ["rename-after", "runtime/service-state.json"],
  }[phase];
  await writePrivate(NodePath.join(root, "fault-plan.json"), {
    protocol: 1,
    fixtureId: prepared.fixtureId,
    executable: launcher.executable,
    identity: launcher.identity,
    method: fault[0],
    target: fault[1],
    arm: "pending-update",
    operationId,
  });
  const args = ["__service-launcher"];
  let observed;
  const run = (stopWhenRecovered) => {
    let inspection = Promise.resolve();
    let watcher;
    let monitorFailure;
    const execute = runNativeFixtureCommand(launcher.executable, args, root, {
      signal,
      spawn: (file, argv, options) => {
        const child = NodeChildProcess.spawn(file, argv, {
          ...options,
          env: {
            ...options.env,
            T3CODE_HOME: baseDir,
            T3CODE_HOST: "127.0.0.1",
            T3CODE_PORT: "0",
            NODE_OPTIONS: "--require " + JSON.stringify(preload),
            JONES_PACKAGED_RECOVERY_FIXTURE: NodePath.join(root, "fault-plan.json"),
          },
        });
        if (stopWhenRecovered) {
          const stop = () => {
            watcher?.close();
            if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
          };
          const inspect = async () => {
            if (observed !== undefined || monitorFailure !== undefined) return;
            try {
              const state = (
                await boundedRecord(NodePath.join(baseDir, "runtime", "service-state.json"))
              ).value;
              const capability = (
                await boundedRecord(
                  NodePath.join(baseDir, "runtime", "jones-launcher-capability.json"),
                )
              ).value;
              if (
                state.activeVersion === binding.activeVersion &&
                state.update?.id === operationId &&
                state.update.status === "failed" &&
                state.update.reason === "launcher-restarted" &&
                capability.launcherPid === child.pid &&
                Number.isSafeInteger(capability.childPid) &&
                capability.childPid > 0 &&
                capability.childPid !== child.pid
              ) {
                observed = { state, capability };
                stop();
              }
            } catch (cause) {
              if (cause.code !== "ENOENT") {
                monitorFailure = cause;
                stop();
              }
            }
          };
          const enqueue = () => {
            inspection = inspection.then(inspect);
          };
          watcher = NodeFS.watch(
            NodePath.join(baseDir, "runtime"),
            { persistent: false },
            (_event, filename) => {
              if (
                filename === null ||
                filename === "service-state.json" ||
                filename === "jones-launcher-capability.json"
              )
                enqueue();
            },
          );
          watcher.once("error", (cause) => {
            monitorFailure = cause;
            stop();
          });
          child.once("close", () => watcher.close());
          child.once("error", () => watcher.close());
          enqueue();
        }
        return child;
      },
    });
    return execute
      .finally(async () => {
        watcher?.close();
        await inspection;
      })
      .then((result) => {
        if (monitorFailure) throw monitorFailure;
        return result;
      });
  };

  const crash = await run(false);
  await writePrivate(NodePath.join(root, "restart-crash-command.json"), crash);
  if (crash.stoppedBy !== "SIGKILL" || crash.code !== null)
    throw new Error("Packaged recovery fault did not stop the launcher.", { cause: crash });
  NodeAssert.deepEqual((await boundedRecord(NodePath.join(root, "fault-fired.json"))).value, {
    protocol: 1,
    fixtureId: prepared.fixtureId,
    operationId,
    method: fault[0],
    target: fault[1],
  });
  NodeAssert.equal(
    await digest(binding.dbPath),
    databaseBefore,
    "pre-trial failure changed the database",
  );
  const outcomeBefore = await boundedRecord(
    NodePath.join(operations, operationId + ".outcome.json"),
  );
  NodeAssert.deepEqual(outcomeBefore.value, {
    schema: 1,
    operationId,
    binding: reservation.value.binding,
    status: "failed",
  });
  const settlementPath = NodePath.join(
    baseDir,
    "runtime",
    "jones-update-settlements",
    operationId + ".json",
  );
  let settlementBefore;
  try {
    settlementBefore = await boundedRecord(settlementPath);
  } catch (cause) {
    if (cause.code !== "ENOENT") throw cause;
  }
  const restart = await run(true);
  await writePrivate(NodePath.join(root, "restart-recovery-command.json"), restart);
  if (
    observed === undefined ||
    !(
      (restart.code === 0 && restart.stoppedBy === null) ||
      (restart.code === null && restart.stoppedBy === "SIGTERM")
    )
  )
    fail("restart did not settle and admit the retained child");
  const finalState = (await boundedRecord(NodePath.join(baseDir, "runtime", "service-state.json")))
    .value;
  NodeAssert.deepEqual(finalState.update.qualified, prepared.staged);
  NodeAssert.equal(finalState.update.id, operationId);
  NodeAssert.equal(finalState.update.status, "failed");
  NodeAssert.equal(finalState.activeVersion, binding.activeVersion);
  NodeAssert.deepEqual(
    (await boundedRecord(NodePath.join(operations, operationId + ".json"))).bytes,
    reservation.bytes,
  );
  NodeAssert.deepEqual(
    (await boundedRecord(NodePath.join(operations, operationId + ".outcome.json"))).bytes,
    outcomeBefore.bytes,
  );
  const settlementAfter = await boundedRecord(settlementPath);
  if (settlementBefore !== undefined)
    NodeAssert.deepEqual(settlementAfter.bytes, settlementBefore.bytes);
  const settlement = settlementAfter.value;
  NodeAssert.equal(settlement.schema, 1);
  NodeAssert.equal(settlement.operationId, operationId);
  NodeAssert.equal(settlement.status, "failed");
  NodeAssert.equal(settlement.settlement, "no-effect");
  NodeAssert.equal(settlement.basis, "launcher-recorded");
  NodeAssert.equal(
    settlement.reservationSha256,
    NodeCrypto.createHash("sha256").update(reservation.bytes).digest("hex"),
  );
  NodeAssert.equal(
    settlement.outcomeSha256,
    NodeCrypto.createHash("sha256").update(outcomeBefore.bytes).digest("hex"),
  );
  NodeAssert.equal(settlement.evidence.phaseAtFailure, "accepted");
  NodeAssert.equal(settlement.evidence.candidateStarted, false);
  NodeAssert.equal(settlement.evidence.activeVersion, binding.activeVersion);
  NodeAssert.equal(settlement.evidence.activeSourceSha, binding.activeSourceSha);
  NodeAssert.equal(settlement.evidence.restorePending, false);
  NodeAssert.equal(settlement.evidence.recoveryPayload, "removed");
  await NodeAssert.rejects(
    NodeFSP.lstat(NodePath.join(baseDir, "runtime", "db-backup", operationId, "database")),
    { code: "ENOENT" },
  );
  const database = new NodeSqlite.DatabaseSync(binding.dbPath, { readOnly: true });
  try {
    NodeAssert.equal(database.prepare("SELECT value FROM fixture_marker").get().value, "before");
  } finally {
    database.close();
  }
  if ((await digest(launcher.executable)) !== launcher.sha256)
    fail("launcher changed during restart");
  NodeAssert.deepEqual(await executableIdentity(launcher.executable), launcher.identity);
  const result = {
    schema: "jones.packaged-settlement-restart/v1",
    phase,
    operationId,
    executable: launcher.executable,
    executableSha256: launcher.sha256,
    crash,
    restart,
    state: finalState,
    settlement,
    assertionsPassed: true,
    packagedRecoveryMatrixPassed: false,
    processIdentityScope: "captured-child-handle-only",
    externalDescendantSettlementRequired: true,
  };
  await writePrivate(NodePath.join(root, "settlement-restart-result.json"), result);
  return result;
}

export async function createPackagedRecoveryHttpClient({
  prepared,
  launcherPid,
  childPid,
  token,
  signal,
}) {
  signal?.throwIfAborted();
  const { root, baseDir } = prepared;
  if (
    // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone fixture binds its actual host without loading application services.
    process.platform !== "linux" ||
    prepared.schema !== "jones.packaged-recovery-fixture/v1" ||
    !NodePath.isAbsolute(root) ||
    (await NodeFSP.realpath(root)) !== root ||
    !NodePath.basename(root).startsWith("jones-packaged-recovery-") ||
    baseDir !== NodePath.join(root, "home") ||
    (await NodeFSP.realpath(baseDir)) !== baseDir
  )
    fail("isolated HTTP fixture required");
  const allocation = await NodeFSP.lstat(root);
  if (
    !allocation.isDirectory() ||
    allocation.uid !== process.getuid() ||
    (allocation.mode & 0o077) !== 0 ||
    allocation.dev !== prepared.allocation.dev ||
    allocation.ino !== prepared.allocation.ino
  )
    fail("HTTP fixture allocation differs");
  NodeAssert.deepEqual(
    (await boundedRecord(NodePath.join(root, "prepared-fixture.json"))).value,
    prepared,
  );
  if (
    ![launcherPid, childPid].every((value) => Number.isSafeInteger(value) && value > 0) ||
    launcherPid === childPid
  )
    fail("distinct captured launcher and child required");
  if (typeof token !== "string" || !/^[\x21-\x7e]{16,8192}$/.test(token))
    fail("explicit in-memory fixture bearer required");
  if (
    !/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(
      prepared.operationId,
    )
  )
    fail("fixed fixture operation ID required");
  const operationId = prepared.operationId;
  const stagedHandle = prepared.staged.stagedHandle;
  const binding = structuredClone(prepared.staged.binding);
  const runtimePath = NodePath.join(baseDir, "userdata", "server-runtime.json");
  const capabilityPath = NodePath.join(baseDir, "runtime", "jones-launcher-capability.json");
  const boundRuntime = async () => {
    for (const directory of [NodePath.dirname(runtimePath), NodePath.dirname(capabilityPath)]) {
      if ((await NodeFSP.realpath(directory)) !== directory)
        fail("physical HTTP fixture directories required");
      const stat = await NodeFSP.lstat(directory);
      if (!stat.isDirectory() || stat.uid !== process.getuid() || (stat.mode & 0o077) !== 0)
        fail("private HTTP fixture directories required");
    }
    const runtime = (await boundedRecord(runtimePath)).value;
    const capability = (await boundedRecord(capabilityPath)).value;
    if (
      runtime.version !== 1 ||
      runtime.pid !== childPid ||
      runtime.serviceManaged !== true ||
      runtime.host !== "127.0.0.1" ||
      runtime.devUrl !== undefined ||
      !Number.isInteger(runtime.port) ||
      runtime.port < 1 ||
      runtime.port > 65535 ||
      runtime.origin !== "http://127.0.0.1:" + runtime.port ||
      capability.launcherPid !== launcherPid ||
      capability.childPid !== childPid
    )
      fail("HTTP runtime does not name the captured loopback child");
    return { runtime, capability };
  };
  const initial = await boundRuntime();
  const endpoint = async (route, payload) => {
    signal?.throwIfAborted();
    NodeAssert.deepEqual(await boundRuntime(), initial, "HTTP child changed before request");
    const mutation = payload !== undefined;
    let started = false;
    try {
      const value = await new Promise((resolve, reject) => {
        const body = mutation ? JSON.stringify(payload) : undefined;
        const request = NodeHttp.request(
          {
            host: "127.0.0.1",
            port: initial.runtime.port,
            path: route,
            method: mutation ? "POST" : "GET",
            agent: false,
            signal:
              signal === undefined
                ? AbortSignal.timeout(30000)
                : AbortSignal.any([signal, AbortSignal.timeout(30000)]),
            headers: {
              authorization: "Bearer " + token,
              accept: "application/json",
              ...(body === undefined
                ? {}
                : {
                    "content-type": "application/json",
                    "content-length": Buffer.byteLength(body),
                  }),
            },
          },
          (response) => {
            if (
              response.statusCode !== 200 ||
              !/^application\/json(?:;|$)/i.test(response.headers["content-type"] ?? "")
            ) {
              response.destroy();
              const error = new Error("Packaged recovery HTTP response refused.");
              error.statusCode = response.statusCode;
              reject(error);
              return;
            }
            let size = 0;
            const chunks = [];
            response.on("data", (chunk) => {
              size += chunk.length;
              if (size > 65536) {
                response.destroy(new Error("Packaged recovery HTTP response exceeded its bound."));
                return;
              }
              chunks.push(chunk);
            });
            response.once("error", reject);
            response.once("end", () => {
              try {
                resolve(JSON.parse(Buffer.concat(chunks).toString("utf8")));
              } catch {
                reject(new Error("Packaged recovery HTTP response is not JSON."));
              }
            });
          },
        );
        request.once("error", reject);
        started = true;
        request.end(body);
      });
      if (value === null && !mutation) {
        NodeAssert.deepEqual(await boundRuntime(), initial, "HTTP child changed during request");
        return value;
      }
      if (
        value === null ||
        typeof value !== "object" ||
        value.source !== "jones-actions" ||
        value.channel !== "jones-main" ||
        ![
          "checking",
          "building",
          "no-new",
          "available",
          "downloading",
          "verifying",
          "staged",
          "preparing",
          "installing",
          "committed",
          "rolled-back",
          "blocked",
          "error",
        ].includes(value.phase) ||
        value.environmentId !== binding.environmentId ||
        value.currentVersion !== binding.activeVersion ||
        value.capability === null ||
        typeof value.capability !== "object" ||
        !["check", "download", "install"].every((key) => typeof value.capability[key] === "boolean")
      )
        throw new Error("Packaged recovery HTTP state differs from the bound fixture.");
      NodeAssert.deepEqual(await boundRuntime(), initial, "HTTP child changed during request");
      return value;
    } catch (cause) {
      const error = new Error("Packaged recovery HTTP exchange failed.");
      error.unknownEffect =
        mutation && started && cause.statusCode !== 401 && cause.statusCode !== 403;
      error.statusCode = cause.statusCode;
      throw error;
    }
  };
  return Object.freeze({
    state: () => endpoint("/api/jones-updates"),
    reconcile: (requestId) => {
      if (!/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(requestId))
        fail("fixed reconciliation request ID required");
      return endpoint("/api/jones-updates/reconcile", { operationId, requestId });
    },
    install: () =>
      endpoint("/api/jones-updates/install", {
        stagedHandle,
        environmentId: binding.environmentId,
        currentVersion: binding.activeVersion,
      }),
  });
}
