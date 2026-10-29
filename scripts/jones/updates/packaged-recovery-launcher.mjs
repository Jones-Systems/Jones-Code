import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodeAssert from "node:assert/strict";
import * as NodeSqlite from "node:sqlite";
import * as NodeHttp from "node:http";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeURL from "node:url";
import { runNativeFixtureCommand } from "./native-fixture-support.mjs";
import { legacyRefusalCases } from "./packaged-recovery-fixture.mjs";
import { operationBinding } from "../../../apps/server/src/jones/updates/launcherOperation.ts";

const preload = NodeURL.fileURLToPath(new URL("./packaged-recovery-preload.cjs", import.meta.url));
const fail = (message) => {
  throw new Error("Packaged launcher preload probe refused: " + message);
};

async function availableLoopbackPort(signal) {
  signal?.throwIfAborted();
  const port = await new Promise((resolve, reject) => {
    const server = NodeNet.createServer();
    server.once("error", reject);
    server.listen({ host: "127.0.0.1", port: 0, exclusive: true }, () => {
      const address = server.address();
      server.close((cause) => {
        if (cause) reject(cause);
        else if (address === null || typeof address === "string")
          reject(new Error("Loopback port unavailable."));
        else resolve(String(address.port));
      });
    });
  });
  signal?.throwIfAborted();
  return port;
}

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
  const port = await availableLoopbackPort(signal);
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
            T3CODE_PORT: port,
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
  const targetSource = prepared.staged.receipt.sourceSha;
  const currentVersion =
    prepared.legacyVariant === "active-changed"
      ? prepared.state.activeVersion
      : binding.activeVersion;
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
  const endpoint = async (route, payload, project) => {
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
      if (project !== undefined) {
        const projected = project(value);
        NodeAssert.deepEqual(await boundRuntime(), initial, "HTTP child changed during request");
        return projected;
      }
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
        value.currentVersion !== currentVersion ||
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
    installOperation: (selectedId = operationId) => {
      if (
        typeof selectedId !== "string" ||
        !/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(selectedId)
      )
        fail("fixed Install operation ID required");
      const input = {
        operationId: selectedId,
        environmentId: binding.environmentId,
        expectedInstalledSource: binding.activeSourceSha,
        action: { kind: "install", stagedHandle, currentVersion, targetSource },
      };
      return endpoint("/api/jones-updates/operations/start", input, (value) => {
        if (
          value === null ||
          typeof value !== "object" ||
          Array.isArray(value) ||
          value.protocol !== 1 ||
          value.operationId !== selectedId ||
          value.environmentId !== input.environmentId ||
          value.expectedInstalledSource !== input.expectedInstalledSource ||
          ![
            "accepted",
            "running",
            "stop-requested",
            "adopting",
            "handed-off",
            "held",
            "completed",
            "rolled-back",
            "stopped",
            "refused",
            "failed",
            "blocked",
            "fenced",
            "interrupted",
          ].includes(value.phase) ||
          typeof value.updatedAt !== "string" ||
          value.updatedAt.length > 64 ||
          !Number.isFinite(Date.parse(value.updatedAt)) ||
          (value.acceptedAt !== undefined &&
            (typeof value.acceptedAt !== "string" ||
              value.acceptedAt.length > 64 ||
              !Number.isFinite(Date.parse(value.acceptedAt)))) ||
          (value.updateId !== undefined && value.updateId !== selectedId) ||
          (value.code !== undefined &&
            (typeof value.code !== "string" || !/^[a-z][a-z0-9-]{0,47}$/.test(value.code))) ||
          (value.message !== undefined &&
            (typeof value.message !== "string" || value.message.length > 1024))
        )
          fail("Install operation receipt differs from the bound fixture");
        NodeAssert.deepEqual(value.action, input.action);
        return {
          protocol: 1,
          operationId: selectedId,
          environmentId: input.environmentId,
          expectedInstalledSource: input.expectedInstalledSource,
          action: { ...input.action },
          phase: value.phase,
          updatedAt: value.updatedAt,
          ...Object.fromEntries(
            ["acceptedAt", "updateId", "code", "message"]
              .filter((key) => value[key] !== undefined)
              .map((key) => [key, value[key]]),
          ),
        };
      });
    },
    install: () =>
      endpoint("/api/jones-updates/install", {
        stagedHandle,
        environmentId: binding.environmentId,
        currentVersion,
      }),
  });
}

export function preparePackagedReconciliationObservation(input) {
  return preparePackagedObservation({ ...input, installing: false });
}

export function preparePackagedInstallObservation(input) {
  return preparePackagedObservation({ ...input, installing: true });
}

async function preparePackagedObservation({
  prepared,
  requestIds,
  installOperationId,
  signal,
  installing,
}) {
  signal?.throwIfAborted();
  prepared = structuredClone(prepared);
  requestIds = Array.isArray(requestIds) ? [...requestIds] : requestIds;
  const refusalReason = Object.hasOwn(legacyRefusalCases, prepared.legacyVariant)
    ? legacyRefusalCases[prepared.legacyVariant]
    : undefined;
  const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
  const { root, baseDir, launcher, operationId } = structuredClone(prepared);
  if (
    prepared.schema !== "jones.packaged-recovery-fixture/v1" ||
    !NodePath.isAbsolute(root) ||
    (await NodeFSP.realpath(root)) !== root ||
    !NodePath.basename(root).startsWith("jones-packaged-recovery-") ||
    baseDir !== NodePath.join(root, "home") ||
    (await NodeFSP.realpath(baseDir)) !== baseDir ||
    !uuid.test(operationId) ||
    (!installing &&
      (!Array.isArray(requestIds) ||
        requestIds.length !== 2 ||
        !requestIds.every((id) => typeof id === "string" && uuid.test(id)) ||
        new Set(requestIds).size !== 2)) ||
    (installing && (prepared.kind !== "staged" || prepared.state.update !== undefined))
  )
    fail(
      installing
        ? "idle staged Install fixture required"
        : "isolated reconciliation fixture and two distinct request IDs required",
    );
  if (
    installOperationId !== undefined &&
    (installing ||
      typeof installOperationId !== "string" ||
      !uuid.test(installOperationId) ||
      installOperationId === operationId ||
      requestIds.includes(installOperationId) ||
      prepared.kind !== "legacy-failed" ||
      prepared.state.update?.status !== "failed")
  )
    fail("distinct fixed unsettled Install operation ID and legacy failed fixture required");
  const allocation = await NodeFSP.lstat(root);
  if (
    !allocation.isDirectory() ||
    allocation.uid !== process.getuid() ||
    (allocation.mode & 0o077) !== 0 ||
    allocation.dev !== prepared.allocation.dev ||
    allocation.ino !== prepared.allocation.ino
  )
    fail("reconciliation fixture allocation differs");
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
    fail("observer launcher executable differs");
  NodeAssert.deepEqual(await executableIdentity(launcher.executable), launcher.identity);
  NodeAssert.deepEqual((await boundedRecord(NodePath.join(root, "fixture-owner.json"))).value, {
    protocol: 1,
    purpose: "jones-packaged-recovery",
    fixtureId: prepared.fixtureId,
    executable: launcher.executable,
    identity: launcher.identity,
  });
  const childVersion =
    prepared.legacyVariant === "active-changed"
      ? prepared.state.activeVersion
      : prepared.staged.binding.activeVersion;
  const childInput =
    childVersion === prepared.inputs.active.receipt.version
      ? prepared.inputs.active
      : prepared.inputs.candidate;
  if (childVersion !== childInput.receipt.version) fail("observer child version differs");
  const childExecutable = NodePath.join(baseDir, "runtime", "versions", childVersion, "t3");
  if (
    (await NodeFSP.realpath(childExecutable)) !== childExecutable ||
    (await digest(childExecutable)) !== childInput.executableSha256
  )
    fail("observer active payload differs");
  const childIdentity = await executableIdentity(childExecutable);
  const ids = installing ? [] : [...requestIds];
  const fixtureId = prepared.fixtureId;
  const planPath = NodePath.join(root, "observer-plan.json");
  const directory = NodePath.join(root, "ipc-observations");
  try {
    await NodeFSP.lstat(directory);
    fail("observer directory is occupied");
  } catch (cause) {
    if (cause.code !== "ENOENT") throw cause;
  }
  await writePrivate(planPath, {
    protocol: installing ? 2 : 1,
    fixtureId,
    executable: launcher.executable,
    identity: launcher.identity,
    operationId,
    ...(installing
      ? { binding: operationBinding(prepared.staged) }
      : { requestIds: ids, retainedUpdate: prepared.state.update }),
    ...(installOperationId === undefined
      ? {}
      : {
          installOperationId,
          installBinding: {
            targetVersion: prepared.staged.receipt.version,
            dbPath: prepared.staged.binding.dbPath,
            stagedHandle: prepared.staged.stagedHandle,
          },
        }),
    childExecutable,
    childIdentity,
    childVersion,
  });
  const parent = await NodeFSP.open(root, NodeFS.constants.O_RDONLY | NodeFS.constants.O_DIRECTORY);
  try {
    await parent.sync();
  } finally {
    await parent.close();
  }
  const read = async ({ launcherPid, childPid }, expectedRefusal) => {
    signal?.throwIfAborted();
    if (
      ![launcherPid, childPid].every((pid) => Number.isSafeInteger(pid) && pid > 0) ||
      launcherPid === childPid
    )
      fail("distinct captured observer processes required");
    const current = await NodeFSP.lstat(root);
    if (
      (await NodeFSP.realpath(root)) !== root ||
      current.dev !== allocation.dev ||
      current.ino !== allocation.ino ||
      current.uid !== allocation.uid ||
      (current.mode & 0o077) !== 0
    )
      fail("reconciliation fixture changed");
    const stat = await NodeFSP.lstat(directory);
    if (
      (await NodeFSP.realpath(directory)) !== directory ||
      !stat.isDirectory() ||
      stat.uid !== process.getuid() ||
      (stat.mode & 0o077) !== 0
    )
      fail("unsafe observer directory");
    if (installing) {
      NodeAssert.deepEqual(
        (await NodeFSP.readdir(directory)).sort(),
        ["ready.json", operationId + ".request.json", operationId + ".accepted.json"].sort(),
      );
      NodeAssert.deepEqual((await boundedRecord(NodePath.join(directory, "ready.json"))).value, {
        protocol: 2,
        fixtureId,
        operationId,
        launcherPid,
      });
      const common = { protocol: 2, fixtureId, operationId, launcherPid, childPid };
      NodeAssert.deepEqual(
        (await boundedRecord(NodePath.join(directory, operationId + ".request.json"))).value,
        { ...common, phase: "request" },
      );
      const receipt = (
        await boundedRecord(NodePath.join(directory, operationId + ".accepted.json"))
      ).value;
      NodeAssert.deepEqual(receipt, {
        ...common,
        phase: "accepted",
        sendCallbackCompleted: true,
        stateSha256: receipt.stateSha256,
        reservationSha256: receipt.reservationSha256,
      });
      if (
        ![receipt.stateSha256, receipt.reservationSha256].every(
          (value) => typeof value === "string" && /^[a-f0-9]{64}$/.test(value),
        )
      )
        fail("acceptance digests differ");
      NodeAssert.deepEqual(await executableIdentity(launcher.executable), launcher.identity);
      NodeAssert.deepEqual(await executableIdentity(childExecutable), childIdentity);
      return {
        schema: "jones.packaged-install-observation/v1",
        operationId,
        receipt,
        packagedRecoveryMatrixPassed: false,
        processIdentityScope: "captured-child-handle-only",
        externalDescendantSettlementRequired: true,
      };
    }
    NodeAssert.deepEqual(
      (await NodeFSP.readdir(directory)).sort(),
      [
        "ready.json",
        ...ids.flatMap((id) => [id + ".request.json", id + ".reply.json"]),
        ...(installOperationId === undefined
          ? []
          : [installOperationId + ".request.json", installOperationId + ".refused.json"]),
      ].sort(),
      "incomplete or unexpected observer records",
    );
    NodeAssert.deepEqual((await boundedRecord(NodePath.join(directory, "ready.json"))).value, {
      protocol: 1,
      fixtureId,
      operationId,
      requestIds: ids,
      ...(installOperationId === undefined ? {} : { installOperationId }),
      launcherPid,
    });
    let admissionRefusal;
    if (installOperationId !== undefined) {
      const common = {
        protocol: 1,
        fixtureId,
        operationId,
        requestId: installOperationId,
        launcherPid,
        childPid,
      };
      NodeAssert.deepEqual(
        (await boundedRecord(NodePath.join(directory, installOperationId + ".request.json"))).value,
        { ...common, phase: "install-request" },
      );
      admissionRefusal = (
        await boundedRecord(NodePath.join(directory, installOperationId + ".refused.json"))
      ).value;
      NodeAssert.deepEqual(admissionRefusal, {
        ...common,
        phase: "install-refused",
        reason:
          "failed-operation-unsettled: " +
          operationId +
          " requires reconciliation before another Install; the current server remains active.",
        sendCallbackCompleted: true,
      });
    }
    const replies = [];
    for (const [index, requestId] of ids.entries()) {
      const common = { protocol: 1, fixtureId, operationId, requestId, launcherPid, childPid };
      NodeAssert.deepEqual(
        (await boundedRecord(NodePath.join(directory, requestId + ".request.json"))).value,
        { ...common, phase: "request" },
      );
      const reply = (await boundedRecord(NodePath.join(directory, requestId + ".reply.json")))
        .value;
      NodeAssert.deepEqual(reply, {
        ...common,
        phase: "reply",
        ...(expectedRefusal === undefined
          ? { result: index === 0 ? "settled" : "already-settled" }
          : { result: "refused", reason: expectedRefusal }),
        sendCallbackCompleted: true,
      });
      replies.push(reply);
    }
    NodeAssert.deepEqual(await executableIdentity(launcher.executable), launcher.identity);
    NodeAssert.deepEqual(await executableIdentity(childExecutable), childIdentity);
    return {
      schema: "jones.packaged-reconciliation-observation/v1",
      operationId,
      replies,
      ...(admissionRefusal === undefined ? {} : { admissionRefusal }),
      packagedRecoveryMatrixPassed: false,
      processIdentityScope: "captured-child-handle-only",
      externalDescendantSettlementRequired: true,
    };
  };
  return Object.freeze({
    environment: Object.freeze({
      NODE_OPTIONS: "--require " + JSON.stringify(preload),
      JONES_PACKAGED_RECOVERY_OBSERVER: planPath,
    }),
    ...(installing
      ? { readAcceptance: (captured) => read(captured, undefined) }
      : {
          readPair: (captured) => read(captured, undefined),
          readRefusalPair: (captured) => {
            if (refusalReason === undefined) fail("named legacy refusal fixture required");
            return read(captured, refusalReason);
          },
        }),
  });
}

export async function exercisePackagedLegacyReconciliation({
  prepared,
  requestIds,
  installOperationId,
  token,
  signal,
}) {
  signal?.throwIfAborted();
  prepared = structuredClone(prepared);
  requestIds = Array.isArray(requestIds) ? [...requestIds] : requestIds;
  const refusalReason = Object.hasOwn(legacyRefusalCases, prepared.legacyVariant)
    ? legacyRefusalCases[prepared.legacyVariant]
    : undefined;
  if (
    prepared.kind !== "legacy-failed" ||
    prepared.state.update?.status !== "failed" ||
    (prepared.legacyVariant !== undefined && refusalReason === undefined) ||
    (refusalReason === undefined && prepared.state.update.reason !== "launcher-restarted") ||
    typeof token !== "string" ||
    !/^[\x21-\x7e]{16,8192}$/.test(token)
  )
    fail("legacy failed fixture and explicit in-memory bearer required");
  const observation = await preparePackagedReconciliationObservation({
    prepared,
    requestIds,
    installOperationId,
    signal,
  });
  const ids = [...requestIds];
  const { root, baseDir, operationId, launcher } = prepared;
  const operationDirectory = NodePath.join(baseDir, "runtime", "jones-update-operations");
  const reservationPath = NodePath.join(operationDirectory, operationId + ".json");
  const outcomePath = NodePath.join(operationDirectory, operationId + ".outcome.json");
  const settlementPath = NodePath.join(
    baseDir,
    "runtime",
    "jones-update-settlements",
    operationId + ".json",
  );
  const reservation = await boundedRecord(reservationPath);
  let originalOutcome;
  try {
    originalOutcome = await boundedRecord(outcomePath);
  } catch (cause) {
    if (cause.code !== "ENOENT") throw cause;
  }
  if (prepared.archivedOutcome !== (originalOutcome !== undefined))
    fail("legacy outcome presence differs");
  try {
    await NodeFSP.lstat(settlementPath);
    fail("legacy operation is already settled");
  } catch (cause) {
    if (cause.code !== "ENOENT") throw cause;
  }
  const databaseBefore = await NodeFSP.lstat(prepared.staged.binding.dbPath);
  const marker = () => {
    const database = new NodeSqlite.DatabaseSync(prepared.staged.binding.dbPath, {
      readOnly: true,
    });
    try {
      return database.prepare("SELECT value FROM fixture_marker").get().value;
    } finally {
      database.close();
    }
  };
  NodeAssert.equal(marker(), "before");
  const backupDirectory = NodePath.join(baseDir, "runtime", "db-backup", operationId);
  const snapshotHeld = async () => {
    const entries = [];
    const visit = async (filename, depth) => {
      if (depth > 4 || entries.length >= 128) fail("held recovery evidence exceeded bound");
      let stat;
      try {
        stat = await NodeFSP.lstat(filename);
      } catch (cause) {
        if (cause.code === "ENOENT" && filename === backupDirectory) return;
        throw cause;
      }
      if (stat.isSymbolicLink() || (!stat.isFile() && !stat.isDirectory()))
        fail("regular held recovery evidence required");
      entries.push({
        path: NodePath.relative(backupDirectory, filename),
        dev: stat.dev,
        ino: stat.ino,
        mode: stat.mode,
        sha256: stat.isFile() ? await digest(filename) : null,
      });
      if (stat.isDirectory())
        for (const name of (await NodeFSP.readdir(filename)).sort())
          await visit(NodePath.join(filename, name), depth + 1);
    };
    await visit(backupDirectory, 0);
    return entries;
  };
  const heldBefore = refusalReason === undefined ? undefined : await snapshotHeld();
  const port = await availableLoopbackPort(signal);
  let stage = "runtime";
  let firstSettlement;
  let firstOutcome;
  let pair;
  let captured;
  let failure;
  let work = Promise.resolve();
  let child;
  const watches = [];
  const cancellation = new AbortController();
  const controlSignal =
    signal === undefined ? cancellation.signal : AbortSignal.any([signal, cancellation.signal]);
  const stop = () => {
    for (const watcher of watches) watcher.close();
    if (child && child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
  };
  const failControl = (cause) => {
    failure ??= cause;
    stop();
  };
  const intact = async () => {
    NodeAssert.deepEqual((await boundedRecord(reservationPath)).bytes, reservation.bytes);
    const state = (await boundedRecord(NodePath.join(baseDir, "runtime", "service-state.json")))
      .value;
    NodeAssert.equal(state.activeVersion, prepared.state.activeVersion);
    NodeAssert.deepEqual(state.update, prepared.state.update);
    const database = await NodeFSP.lstat(prepared.staged.binding.dbPath);
    NodeAssert.equal(database.dev, databaseBefore.dev);
    NodeAssert.equal(database.ino, databaseBefore.ino);
    NodeAssert.equal(marker(), "before");
    if (refusalReason !== undefined) {
      NodeAssert.deepEqual(await snapshotHeld(), heldBefore);
      await NodeAssert.rejects(NodeFSP.lstat(settlementPath), { code: "ENOENT" });
      if (originalOutcome)
        NodeAssert.deepEqual((await boundedRecord(outcomePath)).bytes, originalOutcome.bytes);
      else await NodeAssert.rejects(NodeFSP.lstat(outcomePath), { code: "ENOENT" });
    }
    return state;
  };
  let client;
  let installReceipt;
  let installArchive;
  const inspect = async () => {
    if (failure || stage === "complete" || !child) return;
    if (stage === "runtime") {
      let runtime;
      let capability;
      try {
        runtime = (await boundedRecord(NodePath.join(baseDir, "userdata", "server-runtime.json")))
          .value;
        capability = (
          await boundedRecord(NodePath.join(baseDir, "runtime", "jones-launcher-capability.json"))
        ).value;
      } catch (cause) {
        if (cause.code === "ENOENT") return;
        throw cause;
      }
      if (
        capability.launcherPid !== child.pid ||
        capability.childPid !== runtime.pid ||
        capability.updateSettlementProtocol !== 1
      )
        fail("reconciliation capability differs from the captured launcher");
      captured = { launcherPid: child.pid, childPid: runtime.pid };
      client = await createPackagedRecoveryHttpClient({
        prepared,
        ...captured,
        token,
        signal: controlSignal,
      });
      if (originalOutcome)
        NodeAssert.deepEqual((await boundedRecord(outcomePath)).bytes, originalOutcome.bytes);
      else await NodeAssert.rejects(NodeFSP.lstat(outcomePath), { code: "ENOENT" });
      await NodeAssert.rejects(NodeFSP.lstat(settlementPath), { code: "ENOENT" });
      await intact();
      await client.state();
      const directory = NodePath.join(root, "ipc-observations");
      if ((await NodeFSP.realpath(directory)) !== directory)
        fail("physical IPC observation directory required");
      const watcher = NodeFS.watch(directory, { persistent: false }, () => enqueue());
      watches.push(watcher);
      watcher.once("error", failControl);
      if (installOperationId === undefined) {
        stage = "first-reply";
        await client.reconcile(ids[0]);
      } else {
        stage = "install-refusal";
        installReceipt = await client.installOperation(installOperationId);
      }
    }
    if (stage === "install-refusal") {
      let refusal;
      try {
        refusal = (
          await boundedRecord(
            NodePath.join(root, "ipc-observations", installOperationId + ".refused.json"),
          )
        ).value;
      } catch (cause) {
        if (cause.code === "ENOENT") return;
        throw cause;
      }
      NodeAssert.deepEqual(refusal, {
        protocol: 1,
        fixtureId: prepared.fixtureId,
        operationId,
        requestId: installOperationId,
        phase: "install-refused",
        ...captured,
        reason:
          "failed-operation-unsettled: " +
          operationId +
          " requires reconciliation before another Install; the current server remains active.",
        sendCallbackCompleted: true,
      });
      const archived = await boundedRecord(outcomePath);
      NodeAssert.deepEqual(archived.value, {
        schema: 1,
        operationId,
        binding: reservation.value.binding,
        status: "failed",
      });
      if (originalOutcome) NodeAssert.deepEqual(archived.bytes, originalOutcome.bytes);
      installArchive = {
        created: originalOutcome === undefined,
        sha256: NodeCrypto.createHash("sha256").update(archived.bytes).digest("hex"),
      };
      originalOutcome = archived;
      await intact();
      await NodeAssert.rejects(NodeFSP.lstat(settlementPath), { code: "ENOENT" });
      await NodeAssert.rejects(
        NodeFSP.lstat(NodePath.join(operationDirectory, installOperationId + ".json")),
        { code: "ENOENT" },
      );
      await client.state();
      stage = "first-reply";
      await client.reconcile(ids[0]);
    }
    if (stage === "first-reply") {
      let reply;
      try {
        reply = (
          await boundedRecord(NodePath.join(root, "ipc-observations", ids[0] + ".reply.json"))
        ).value;
      } catch (cause) {
        if (cause.code === "ENOENT") return;
        throw cause;
      }
      NodeAssert.deepEqual(reply, {
        protocol: 1,
        fixtureId: prepared.fixtureId,
        operationId,
        requestId: ids[0],
        phase: "reply",
        ...captured,
        ...(refusalReason === undefined
          ? { result: "settled" }
          : { result: "refused", reason: refusalReason }),
        sendCallbackCompleted: true,
      });
      if (refusalReason !== undefined) {
        await intact();
        stage = "second-reply";
        await client.reconcile(ids[1]);
      } else {
        firstSettlement = await boundedRecord(settlementPath);
        firstOutcome = await boundedRecord(outcomePath);
        NodeAssert.deepEqual(firstOutcome.value, {
          schema: 1,
          operationId,
          binding: reservation.value.binding,
          status: "failed",
        });
        if (originalOutcome) NodeAssert.deepEqual(firstOutcome.bytes, originalOutcome.bytes);
        const receipt = firstSettlement.value;
        NodeAssert.equal(receipt.schema, 1);
        NodeAssert.equal(receipt.operationId, operationId);
        NodeAssert.equal(receipt.status, "failed");
        NodeAssert.equal(receipt.settlement, "no-effect");
        NodeAssert.equal(receipt.basis, "legacy-reconciled");
        const sha = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
        NodeAssert.equal(receipt.reservationSha256, sha(reservation.bytes));
        NodeAssert.equal(receipt.outcomeSha256, sha(firstOutcome.bytes));
        NodeAssert.equal(receipt.evidence.candidateStarted, false);
        NodeAssert.equal(receipt.evidence.restorePending, false);
        NodeAssert.equal(
          receipt.evidence.recoveryPayload,
          prepared.recoveryPayload ? "removed" : "absent",
        );
        await intact();
        stage = "second-reply";
        await client.reconcile(ids[1]);
      }
    }
    if (stage === "second-reply") {
      try {
        await boundedRecord(NodePath.join(root, "ipc-observations", ids[1] + ".reply.json"));
      } catch (cause) {
        if (cause.code === "ENOENT") return;
        throw cause;
      }
      if (refusalReason !== undefined) pair = await observation.readRefusalPair(captured);
      else {
        pair = await observation.readPair(captured);
        NodeAssert.deepEqual((await boundedRecord(settlementPath)).bytes, firstSettlement.bytes);
        NodeAssert.deepEqual((await boundedRecord(outcomePath)).bytes, firstOutcome.bytes);
        await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(backupDirectory, "database")), {
          code: "ENOENT",
        });
      }
      await intact();
      stage = "complete";
      stop();
    }
  };
  const enqueue = () => {
    work = work.then(inspect).catch(failControl);
  };
  let command;
  try {
    command = await runNativeFixtureCommand(launcher.executable, ["__service-launcher"], root, {
      signal,
      spawn: (file, argv, options) => {
        child = NodeChildProcess.spawn(file, argv, {
          ...options,
          env: {
            ...options.env,
            ...observation.environment,
            T3CODE_HOME: baseDir,
            T3CODE_HOST: "127.0.0.1",
            T3CODE_PORT: port,
            T3CODE_NO_BROWSER: "true",
            T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: "false",
          },
        });
        for (const directory of [
          NodePath.join(baseDir, "runtime"),
          NodePath.join(baseDir, "userdata"),
        ]) {
          const watcher = NodeFS.watch(directory, { persistent: false }, (_event, name) => {
            if (
              name === null ||
              ["server-runtime.json", "jones-launcher-capability.json"].includes(String(name))
            )
              enqueue();
          });
          watches.push(watcher);
          watcher.once("error", failControl);
        }
        child.once("close", () => {
          for (const watcher of watches) watcher.close();
          cancellation.abort(new Error("Captured reconciliation launcher closed."));
        });
        child.once("error", failControl);
        enqueue();
        return child;
      },
    });
    await work;
    if (failure) throw failure;
    if (
      stage !== "complete" ||
      !(
        (command.code === 0 && command.stoppedBy === null) ||
        (command.code === null && command.stoppedBy === "SIGTERM")
      )
    )
      fail("legacy reconciliation did not finish before launcher exit");
    const state = await intact();
    const result = {
      schema: "jones.packaged-legacy-reconciliation/v1",
      operationId,
      archivedOutcome: prepared.archivedOutcome,
      ...(installArchive === undefined ? {} : { installArchive }),
      command,
      observation: pair,
      ...(installReceipt === undefined ? {} : { installReceipt }),
      state,
      settlement: firstSettlement?.value ?? null,
      ...(refusalReason === undefined
        ? {}
        : {
            refusal: {
              variant: prepared.legacyVariant,
              reason: refusalReason,
              heldEvidenceUnchanged: true,
            },
          }),
      assertionsPassed: true,
      packagedRecoveryMatrixPassed: false,
      processIdentityScope: "captured-child-handle-only",
      externalDescendantSettlementRequired: true,
    };
    await writePrivate(NodePath.join(root, "legacy-reconciliation-result.json"), result);
    return result;
  } catch (cause) {
    if (child) cause.preserveScratch = true;
    throw cause;
  } finally {
    for (const watcher of watches) watcher.close();
    cancellation.abort(new Error("Reconciliation control finished."));
    await work;
  }
}

export async function exercisePackagedAcceptedFailure({
  prepared,
  step,
  freshOperationId,
  token,
  signal,
}) {
  signal?.throwIfAborted();
  prepared = structuredClone(prepared);
  const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
  if (
    prepared.kind !== "staged" ||
    !["reverify", "backup", "ownership"].includes(step) ||
    typeof freshOperationId !== "string" ||
    !uuid.test(freshOperationId) ||
    freshOperationId === prepared.operationId ||
    typeof token !== "string" ||
    !/^[\x21-\x7e]{16,8192}$/.test(token)
  )
    fail("idle staged fixture, named failure, distinct fresh ID and in-memory bearer required");
  const { root, baseDir, operationId, launcher, staged } = prepared;
  const observation = await preparePackagedInstallObservation({ prepared, signal });
  const fault =
    step === "reverify"
      ? { method: "open-before", target: "runtime/staged-updates/" + staged.stagedHandle + ".json" }
      : step === "backup"
        ? { method: "mkdir-before", target: "runtime/db-backup/" + operationId + ".staging" }
        : { method: "readFileSync-before", target: "userdata/server-runtime.json" };
  await writePrivate(NodePath.join(root, "fault-plan.json"), {
    protocol: 1,
    fixtureId: prepared.fixtureId,
    executable: launcher.executable,
    identity: launcher.identity,
    operationId,
    arm: "accepted-update",
    ...fault,
  });
  const port = await availableLoopbackPort(signal);
  const nativePath = NodePath.join(baseDir, "runtime", "service-state.json");
  const operations = NodePath.join(baseDir, "runtime", "jones-update-operations");
  const reservedPath = NodePath.join(operations, operationId + ".json");
  const outcomePath = NodePath.join(operations, operationId + ".outcome.json");
  const settlementPath = NodePath.join(
    baseDir,
    "runtime",
    "jones-update-settlements",
    operationId + ".json",
  );
  const causePath = NodePath.join(
    baseDir,
    "runtime",
    "jones-update-settlements",
    "causes",
    operationId + ".json",
  );
  const databaseBefore = await NodeFSP.lstat(staged.binding.dbPath);
  const intactDatabase = async () => {
    const stat = await NodeFSP.lstat(staged.binding.dbPath);
    NodeAssert.equal(stat.dev, databaseBefore.dev);
    NodeAssert.equal(stat.ino, databaseBefore.ino);
    const db = new NodeSqlite.DatabaseSync(staged.binding.dbPath, { readOnly: true });
    try {
      NodeAssert.equal(db.prepare("SELECT value FROM fixture_marker").get().value, "before");
    } finally {
      db.close();
    }
  };
  let acceptance, originalReservation, originalOutcome, originalSettlement, causeRecord, committed;
  const runs = [];
  const launch = async (first) => {
    let child,
      captured,
      client,
      failure,
      ready = false,
      freshStarted = false,
      complete = false;
    let work = Promise.resolve();
    const watches = [];
    const cancellation = new AbortController();
    const controlSignal = signal
      ? AbortSignal.any([signal, cancellation.signal])
      : cancellation.signal;
    const stop = () => {
      for (const w of watches) w.close();
      if (child && child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    };
    const failed = (error) => {
      failure ??= error;
      stop();
    };
    const inspect = async () => {
      if (failure || complete || !child) return;
      if (!ready) {
        let runtime, capability;
        try {
          runtime = (await boundedRecord(NodePath.join(baseDir, "userdata", "server-runtime.json")))
            .value;
          capability = (
            await boundedRecord(NodePath.join(baseDir, "runtime", "jones-launcher-capability.json"))
          ).value;
        } catch (error) {
          if (error.code === "ENOENT") return;
          throw error;
        }
        if (capability.launcherPid !== child.pid || capability.childPid !== runtime.pid) return;
        captured = { launcherPid: child.pid, childPid: runtime.pid };
        client = await createPackagedRecoveryHttpClient({
          prepared,
          ...captured,
          token,
          signal: controlSignal,
        });
        ready = true;
        if (first) {
          const receipt = await client.installOperation();
          if (!["accepted", "running", "handed-off"].includes(receipt.phase))
            fail("first Install was not admitted");
          return;
        }
        const native = (await boundedRecord(nativePath)).value;
        NodeAssert.equal(native.activeVersion, prepared.state.activeVersion);
        NodeAssert.equal(native.update.id, operationId);
        NodeAssert.equal(native.update.status, "failed");
        NodeAssert.equal(native.update.reason, "launcher-restarted");
        NodeAssert.deepEqual((await boundedRecord(reservedPath)).bytes, originalReservation.bytes);
        originalOutcome = await boundedRecord(outcomePath);
        NodeAssert.deepEqual(originalOutcome.value, {
          schema: 1,
          operationId,
          binding: originalReservation.value.binding,
          status: "failed",
        });
        originalSettlement = await boundedRecord(settlementPath);
        const settled = originalSettlement.value;
        NodeAssert.equal(settled.schema, 1);
        NodeAssert.equal(settled.operationId, operationId);
        NodeAssert.equal(settled.status, "failed");
        NodeAssert.equal(settled.settlement, "no-effect");
        NodeAssert.equal(settled.basis, "launcher-recorded");
        NodeAssert.equal(
          settled.reservationSha256,
          NodeCrypto.createHash("sha256").update(originalReservation.bytes).digest("hex"),
        );
        NodeAssert.equal(
          settled.outcomeSha256,
          NodeCrypto.createHash("sha256").update(originalOutcome.bytes).digest("hex"),
        );
        NodeAssert.equal(settled.evidence.phaseAtFailure, "accepted");
        NodeAssert.equal(settled.evidence.nativeReason, "launcher-restarted");
        NodeAssert.equal(settled.evidence.candidateStarted, false);
        NodeAssert.equal(settled.evidence.restorePending, false);
        NodeAssert.ok(["absent", "removed"].includes(settled.evidence.recoveryPayload));
        NodeAssert.equal(
          settled.evidence.cause,
          causeRecord.value.message || causeRecord.value.code,
        );
        NodeAssert.equal(settled.evidence.activeVersion, staged.binding.activeVersion);
        NodeAssert.equal(settled.evidence.activeSourceSha, staged.binding.activeSourceSha);
        await NodeAssert.rejects(
          NodeFSP.lstat(NodePath.join(baseDir, "runtime", "db-backup", operationId, "database")),
          { code: "ENOENT" },
        );
        await intactDatabase();
        const refused = await client.installOperation(operationId);
        if (!["blocked", "failed", "refused", "interrupted"].includes(refused.phase))
          fail("failed Install ID was readmitted");
        NodeAssert.deepEqual((await boundedRecord(nativePath)).value, native);
        const next = await client.installOperation(freshOperationId);
        if (!["accepted", "running", "handed-off"].includes(next.phase))
          fail("fresh Install was not admitted");
        freshStarted = true;
      }
      if (!first && freshStarted) {
        const state = (await boundedRecord(nativePath)).value;
        if (state.update?.id !== freshOperationId || state.update.status !== "committed") return;
        NodeAssert.equal(state.activeVersion, staged.receipt.version);
        NodeAssert.equal(state.update.fromVersion, staged.binding.activeVersion);
        NodeAssert.equal(state.update.targetVersion, staged.receipt.version);
        NodeAssert.deepEqual((await boundedRecord(reservedPath)).bytes, originalReservation.bytes);
        NodeAssert.deepEqual((await boundedRecord(outcomePath)).bytes, originalOutcome.bytes);
        NodeAssert.deepEqual((await boundedRecord(settlementPath)).bytes, originalSettlement.bytes);
        await intactDatabase();
        committed = state;
        complete = true;
        stop();
      }
    };
    const enqueue = () => {
      work = work.then(inspect).catch(failed);
    };
    let command;
    try {
      command = await runNativeFixtureCommand(launcher.executable, ["__service-launcher"], root, {
        signal,
        spawn: (file, args, options) => {
          child = NodeChildProcess.spawn(file, args, {
            ...options,
            env: {
              ...options.env,
              ...(first
                ? observation.environment
                : { NODE_OPTIONS: "--require " + JSON.stringify(preload) }),
              JONES_PACKAGED_RECOVERY_FIXTURE: NodePath.join(root, "fault-plan.json"),
              T3CODE_HOME: baseDir,
              T3CODE_HOST: "127.0.0.1",
              T3CODE_PORT: port,
              T3CODE_NO_BROWSER: "true",
              T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: "false",
            },
          });
          for (const directory of [
            NodePath.join(baseDir, "runtime"),
            NodePath.join(baseDir, "userdata"),
          ]) {
            const watcher = NodeFS.watch(directory, { persistent: false }, () => enqueue());
            watches.push(watcher);
            watcher.once("error", failed);
          }
          child.once("error", failed);
          child.once("close", () => {
            for (const w of watches) w.close();
            cancellation.abort(new Error("Captured Install launcher closed"));
          });
          enqueue();
          return child;
        },
      });
      await work;
      if (failure) throw failure;
      if (first) {
        if (command.code !== 1 || command.stoppedBy !== null || !ready)
          fail("accepted failure did not exit through the expected launcher boundary");
        acceptance = await observation.readAcceptance(captured);
        NodeAssert.deepEqual((await boundedRecord(NodePath.join(root, "fault-fired.json"))).value, {
          protocol: 1,
          fixtureId: prepared.fixtureId,
          operationId,
          ...fault,
        });
        originalReservation = await boundedRecord(reservedPath);
        NodeAssert.equal(
          NodeCrypto.createHash("sha256").update(originalReservation.bytes).digest("hex"),
          acceptance.receipt.reservationSha256,
        );
        causeRecord = await boundedRecord(causePath);
        NodeAssert.equal(causeRecord.value.schema, 1);
        NodeAssert.equal(causeRecord.value.operationId, operationId);
        NodeAssert.equal(causeRecord.value.step, step);
        NodeAssert.match(causeRecord.value.code, /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/);
        NodeAssert.equal(typeof causeRecord.value.message, "string");
        NodeAssert.ok(Buffer.byteLength(causeRecord.value.message) <= 512);
        await intactDatabase();
      } else if (
        !complete ||
        !(
          (command.code === 0 && command.stoppedBy === null) ||
          (command.code === null && command.stoppedBy === "SIGTERM")
        )
      )
        fail("fresh Install did not commit before launcher exit");
      runs.push({ code: command.code, stoppedBy: command.stoppedBy, captured });
    } finally {
      cancellation.abort(new Error("Install control finished"));
      stop();
      await work;
    }
  };
  try {
    await launch(true);
    await launch(false);
    NodeAssert.deepEqual(await executableIdentity(launcher.executable), launcher.identity);
    if ((await digest(launcher.executable)) !== launcher.sha256) fail("launcher artifact changed");
    const result = {
      schema: "jones.packaged-accepted-failure/v1",
      step,
      operationId,
      freshOperationId,
      acceptance,
      cause: causeRecord.value,
      settlement: originalSettlement.value,
      committed,
      runs,
      assertionsPassed: true,
      packagedRecoveryMatrixPassed: false,
      processIdentityScope: "captured-child-handle-only",
      externalDescendantSettlementRequired: true,
    };
    await writePrivate(NodePath.join(root, "accepted-failure-result.json"), result);
    return result;
  } catch (error) {
    error.preserveScratch = true;
    error.scratchRoot = root;
    throw error;
  }
}
