// @effect-diagnostics nodeBuiltinImport:off
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeTimersPromises from "node:timers/promises";
import packageJson from "../../../package.json" with { type: "json" };
import { Launcher, main, readServiceState, writeServiceState } from "../../serviceLauncher.ts";
import {
  QUALIFIED_STARTUP_STATE_FILES,
  readQualifiedBackupReceipt,
  removeQualifiedRecoveryPayload,
  type QualifiedBackupAdapter,
} from "./qualifiedBackup.ts";
import {
  assertQualifiedResumeUnreserved,
  qualifiedResumeReservationPath,
} from "./qualifiedStartup.ts";
import {
  archiveUpdateOperation,
  operationBinding,
  publishUpdateOperationSettlement,
  readOperationReservation,
  readUpdateOperationCause,
  readUpdateOperationSettlement,
  reconcileUpdateOperation,
  reserveUpdateOperation,
  writeFailedUpdateOperation,
} from "../updates/launcherOperation.ts";
import {
  currentQualifiedRuntimeBinding,
  qualifiedPayloadDigest,
  stageQualifiedRuntime,
  QUALIFIED_RUNTIME_RECEIPT,
  type QualifiedRuntimeReceipt,
  type StagedQualifiedRuntime,
} from "./qualifiedRuntime.ts";
import type { ServiceLauncherChildMessage, ServiceState } from "../../cloud/serviceProtocol.ts";

import { retainStagedSelection } from "../updates/stagedSelection.ts";
import { assertStageNotRevoked } from "../updates/stageRevocation.ts";

const baseline = "0.0.0-preview.20261002.100";
const target = "0.0.0-preview.20261002.101.1";
const previousId = "abcd1234-1234-4234-8234-123456789abc";
const laterTarget = "0.0.0-preview.20261002.102.1";
// oxlint-disable-next-line t3code/no-global-process-runtime -- These controlled launcher executables must bind the actual native fixture host.
const fixturePlatform = NodeOS.platform() === "darwin" ? "darwin" : "linux";
const readMarker = (file: string) => {
  const database = new NodeSqlite.DatabaseSync(file, { readOnly: true });
  try {
    return database.prepare("SELECT value FROM marker").get()?.value;
  } finally {
    database.close();
  }
};

async function runScenario(
  mode:
    | "commit"
    | "commit-then-rollback"
    | "rollback"
    | "timeout"
    | "stale-child"
    | "missing-gate"
    | "wrong-gate"
    | "blocked-candidate",
  body: (base: string, launcherFailure: unknown) => Promise<void>,
  onLauncher?: (launcher: Launcher) => void,
  options: {
    readonly backupAdapter?: QualifiedBackupAdapter;
    readonly productionMain?: boolean;
    readonly startupGateProtocol?: 1 | "missing";
    readonly operationFailure?: "archive" | "reserve";
    readonly afterOperationReserve?: (baseDir: string) => void | Promise<void>;
    readonly retryWithDifferentOperation?: boolean;
    /** The child asks how its Install and an unknown operation ended right after requesting. */
    readonly outcomeQueries?: boolean;
    readonly delayedGracefulClose?: boolean;
    readonly retirementRace?: "accept-first" | "retire-first";
    readonly now?: () => number;
    readonly pointerWriteFailure?: boolean;
    readonly reserveAfterCleanup?: boolean;
    readonly cleanupFailure?: boolean;
    readonly previous?: "committed" | "rolled-back" | "failed" | "accepted";
    readonly previousFault?: "directory" | "binding" | "restore-intent";
    readonly recoveryOnly?: boolean;
    readonly settlementRetry?: boolean;
    readonly settlementRequests?: readonly ServiceLauncherChildMessage[];
    readonly settlementActiveDrift?: boolean;
    readonly clearRestoreAfterReply?: boolean;
    readonly beforeLaunch?: (base: string, staged: StagedQualifiedRuntime) => Promise<void>;
    readonly recoveryIO?: NonNullable<ConstructorParameters<typeof Launcher>[2]>["recoveryIO"];
    readonly preparedTimeoutMs?: number;
  } = {},
) {
  const allocated = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-launcher-test-"));
  try {
    const base = await NodeFSP.realpath(allocated);
    const userdata = NodePath.join(base, "userdata");
    await NodeFSP.mkdir(userdata, { mode: 0o700 });
    const dbPath = NodePath.join(userdata, "statev2.sqlite");
    const database = new NodeSqlite.DatabaseSync(dbPath);
    database.exec(
      "PRAGMA journal_mode=WAL; CREATE TABLE marker(value TEXT); INSERT INTO marker VALUES ('before');",
    );
    database.close();
    await NodeFSP.writeFile(
      NodePath.join(userdata, "environment-id"),
      "synthetic-jones-environment",
    );
    await NodeFSP.writeFile(NodePath.join(userdata, "settings.json"), "before-settings");
    const child = `#!${process.execPath}
import {
  readFileSync,
  existsSync,
  writeFileSync,
  renameSync,
  mkdirSync,
} from "node:fs";

import * as NodeNet from "node:net";
import { currentQualifiedRuntimeBinding, stageQualifiedRuntime } from "${NodePath.join(import.meta.dirname, "qualifiedRuntime.ts")}";
import { retainStagedSelection } from "${NodePath.join(import.meta.dirname, "../updates/stagedSelection.ts")}";
import * as NodeSqlite from "node:sqlite";
import { makeQualifiedTrialReceipt, reserveQualifiedResume, sameQualifiedTrialIdentity } from "${NodePath.join(import.meta.dirname, "qualifiedStartup.ts")}";
if (process.argv.includes("__service-preflight")) {
  if (${JSON.stringify(mode)} === "blocked-candidate") {
    console.log(JSON.stringify({status:"blocked",version:${JSON.stringify(target)},reason:"candidate-older-than-database"}));
    process.exit(1);
  }
  console.log(JSON.stringify({status:"ready",version:${JSON.stringify(target)},launcherProtocol:4,
    ...(${JSON.stringify(mode)} === "missing-gate" ? {} : {startupGateProtocol:${JSON.stringify(mode)} === "wrong-gate" ? 2 : 1})}));
  process.exit(0);
}
const ownership = new NodeSqlite.DatabaseSync(${JSON.stringify(NodePath.join(userdata, "server-owner.sqlite"))});
ownership.exec("PRAGMA busy_timeout=0; BEGIN EXCLUSIVE;");
const context = JSON.parse(process.env.T3_SERVICE_LAUNCHER_CONTEXT);
if (${JSON.stringify(options.settlementRequests !== undefined)}) {
  const requests = ${JSON.stringify(options.settlementRequests ?? [])};
  const replies = [];
  if (${JSON.stringify(options.settlementActiveDrift === true)}) {
    const file = ${JSON.stringify(NodePath.join(base, "runtime", "versions", baseline, QUALIFIED_RUNTIME_RECEIPT))};
    writeFileSync(file, JSON.stringify({...JSON.parse(readFileSync(file, "utf8")), sourceSha:${JSON.stringify("e".repeat(40))}}));
  }
  process.on("message", async m => {
    if (m.type !== "update-reconciled") return;
    replies.push(m);
    if (${JSON.stringify(options.clearRestoreAfterReply === true)} && replies.length === 1) {
      const {unlinkSync} = await import("node:fs");
      unlinkSync(${JSON.stringify(NodePath.join(base, "runtime", "db-backup", previousId, ".restore-pending"))});
    }
    if (replies.length < requests.length) process.send(requests[replies.length]);
    else {writeFileSync(${JSON.stringify(NodePath.join(base, "runtime", "settlement-replies.json"))}, JSON.stringify({context, replies})); process.exit(0);}
  });
  process.send(requests[0]);
  setInterval(() => {}, 1000);
} else if (${JSON.stringify(options.settlementRetry === true)} && context.update?.status === "failed" && existsSync(${JSON.stringify(NodePath.join(base, "runtime", "retry-settled"))})) {
  const install = {type:"request-update", targetVersion:${JSON.stringify(target)}, dbPath:${JSON.stringify(dbPath)}, stagedHandle:readFileSync(${JSON.stringify(NodePath.join(base, "runtime", "test-handle"))}, "utf8"), operationId:context.update.id};
  let rejections = 0;
  process.on("message", m => {
    if (m.type !== "update-rejected") return;
    writeFileSync(${JSON.stringify(NodePath.join(base, "runtime", "settlement-rejection-"))} + (++rejections) + ".json", JSON.stringify(m));
    if (rejections === 1) process.send({...install, operationId:"32345678-1234-4234-8234-123456789abc"});
    else process.exit(0);
  });
  process.send(install);
  setInterval(() => {}, 1000);
} else if (${JSON.stringify(options.retirementRace !== undefined)} && context.update === undefined) {
  const handle = readFileSync(${JSON.stringify(NodePath.join(base, "runtime", "test-handle"))}, "utf8");
  const install = {type:"request-update", targetVersion:${JSON.stringify(target)}, dbPath:${JSON.stringify(dbPath)}, stagedHandle:handle, operationId:"12345678-1234-4234-8234-123456789abc"};
  const retire = {type:"request-retire-update", operationId:install.operationId, stagedHandle:handle, currentVersion:${JSON.stringify(baseline)}, environmentId:"synthetic-jones-environment", expectedInstalledSource:${JSON.stringify("c".repeat(40))}, targetSource:${JSON.stringify("a".repeat(40))}};
  let rejected = 0;
  process.on("message", m => {
    if (m.type === "update-retired") {
      writeFileSync(${JSON.stringify(NodePath.join(base, "runtime", "retirement-result.json"))}, JSON.stringify(m));
      if (${JSON.stringify(options.retirementRace)} === "retire-first") process.send(install);
    }
    if (m.type === "update-rejected") {
      writeFileSync(${JSON.stringify(NodePath.join(base, "runtime", "late-rejection-"))} + (++rejected) + ".json", JSON.stringify(m));
      if (rejected === 1) {const {operationId, ...manual} = install; process.send(manual);}
      else process.exit(0);
    }
  });
  // The second IPC message reaches the launcher while the first request is
  // still in its asynchronous candidate preflight. It must wait for acceptance.
  if (${JSON.stringify(options.retirementRace)} === "accept-first") process.send(install);
  process.send(retire);
  setInterval(() => {}, 1000);
} else if (context.update?.status === "pending") {
  const db = new NodeSqlite.DatabaseSync(${JSON.stringify(dbPath)});
  db.exec("UPDATE marker SET value='after'"); db.close();
  writeFileSync(${JSON.stringify(NodePath.join(userdata, "settings.json"))}, "after-settings");
  writeFileSync(${JSON.stringify(NodePath.join(userdata, "keybindings.json"))}, "created-by-trial");
  if (${JSON.stringify(mode)} === "rollback" || (${JSON.stringify(mode)} === "commit-then-rollback" && context.update.fromVersion !== ${JSON.stringify(baseline)})) process.exit(1);
  if (${JSON.stringify(mode)} === "timeout") { await new Promise(() => setInterval(() => {}, 1000)); }
  const server = NodeNet.createServer();
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  const socket = server.address();
  const receipt = await makeQualifiedTrialReceipt({
    updateId: context.update.id, qualified: context.update.qualified,
    witness: {
      home: process.env.T3CODE_HOME, databasePath: ${JSON.stringify(dbPath)},
      serviceUserdata: ${JSON.stringify(userdata)},
      environmentId: readFileSync(${JSON.stringify(NodePath.join(userdata, "environment-id"))}, "utf8").trim(),
      version: ${JSON.stringify(target)},
      buildMetadata: {jonesSource: {repository:"Jones-Systems/Jones-Code", sha:${JSON.stringify("a".repeat(40))}, tree:${JSON.stringify("b".repeat(40))}}},
      processId: process.pid,
      listener: {_tag:"InetAddressV4",address:{toString:()=>socket.address},port:socket.port},
    },
  });
  if (${JSON.stringify(mode)} === "stale-child") receipt.processId += 1;
  process.on("message", async m => {
    if (m.type !== "committed") return;
    if (m.startupGateProtocol !== 1 || m.qualified?.generation !== context.update.id ||
      !sameQualifiedTrialIdentity(receipt, m.qualified)) process.exit(2);
    writeFileSync(${JSON.stringify(NodePath.join(base, "runtime", "observed-trial-launcher.json"))}, JSON.stringify({
      receipt: JSON.parse(readFileSync(${JSON.stringify(NodePath.join(base, "runtime", "jones-launcher-capability.json"))}, "utf8")), childPid:process.pid,
    }));
    if (${JSON.stringify(options.reserveAfterCleanup === true)}) {
      const databaseCopy = ${JSON.stringify(NodePath.join(base, "runtime", "db-backup"))} + "/" + context.update.id + "/database";
      const deadline = Date.now() + 5000;
      while (existsSync(databaseCopy)) {
        if (Date.now() >= deadline) throw new Error("Recovery payload cleanup did not finish");
        await new Promise(resolve => setTimeout(resolve, 10));
      }
    }
    await reserveQualifiedResume({receipt});
    if (${JSON.stringify(mode)} === "commit-then-rollback") {
      const { protocol, payloadSha256, ...artifact } = JSON.parse(readFileSync(${JSON.stringify(NodePath.join(base, "runtime", "versions", target, QUALIFIED_RUNTIME_RECEIPT))}, "utf8"));
      const staged = await stageQualifiedRuntime({
        artifact: {...artifact, version:${JSON.stringify(laterTarget)}, sourceSha:${JSON.stringify("e".repeat(40))}, installedSourceSha:artifact.sourceSha, runId:102, payloadDirectory:${JSON.stringify(NodePath.join(base, "later-payload"))}},
        binding: await currentQualifiedRuntimeBinding(process.env.T3CODE_HOME, context.childVersion),
        validate: async () => {},
      });
      await retainStagedSelection(staged);
      process.send({type:"request-update", targetVersion:${JSON.stringify(laterTarget)}, dbPath:${JSON.stringify(dbPath)}, stagedHandle:staged.stagedHandle});
    } else process.exit(0);
  });
  if (${JSON.stringify(options.cleanupFailure === true)}) {
    mkdirSync(${JSON.stringify(NodePath.join(base, "runtime", "db-backup"))} + "/" + context.update.id + "/database-shm");
  }
  if (${JSON.stringify(options.pointerWriteFailure === true)}) {
    renameSync(${JSON.stringify(NodePath.join(base, "runtime", "service-state.json"))}, ${JSON.stringify(NodePath.join(base, "runtime", "before-commit.json"))});
    mkdirSync(${JSON.stringify(NodePath.join(base, "runtime", "service-state.json"))});
  }
  process.send({type:"prepared", updateId:context.update.id, startupGateProtocol:1, qualified:receipt});
} else if (context.update === undefined && ${JSON.stringify(options.outcomeQueries === true)}) {
  const handle = readFileSync(${JSON.stringify(NodePath.join(base, "runtime", "test-handle"))}, "utf8");
  const operationId = "12345678-1234-4234-8234-123456789abc";
  const outcomes = [];
  let rejection;
  process.on("message", m => {
    if (m.type === "update-accepted") writeFileSync(${JSON.stringify(NodePath.join(base, "runtime", "observed-active-launcher.json"))}, JSON.stringify(m));
    if (m.type === "update-rejected") rejection = m;
    if (m.type !== "update-outcome") return;
    outcomes.push(m);
    if (outcomes.length < 2) return;
    writeFileSync(${JSON.stringify(NodePath.join(base, "runtime", "outcomes.json"))}, JSON.stringify({ context, outcomes, rejection }));
    if (rejection !== undefined) setImmediate(() => process.exit(0));
  });
  // Pipelined behind the request: IPC order puts both questions after its handler.
  process.send({type:"request-update", targetVersion:${JSON.stringify(target)}, dbPath:${JSON.stringify(dbPath)}, stagedHandle:handle, operationId});
  process.send({type:"reconcile-update", operationId, queryId:1});
  process.send({type:"reconcile-update", operationId:"42345678-1234-4234-8234-123456789abc", queryId:2});
  setInterval(() => {}, 1000);
} else if (context.update === undefined || (${JSON.stringify(options.previous !== undefined && options.recoveryOnly !== true)} && context.update.id === ${JSON.stringify(previousId)})) {
  const handle = readFileSync(${JSON.stringify(NodePath.join(base, "runtime", "test-handle"))}, "utf8");
  if (${JSON.stringify(options.delayedGracefulClose === true)}) process.once("SIGTERM", () => {
    setTimeout(() => {
      const db = new NodeSqlite.DatabaseSync(${JSON.stringify(dbPath)});
      try { db.exec("UPDATE marker SET value='gracefully-drained'"); } finally { db.close(); }
      process.exit(0);
    }, 10);
  });
  let rejections = 0;
  process.on("message", m => {if (m.type === "update-accepted") {
    writeFileSync(${JSON.stringify(NodePath.join(base, "runtime", "observed-active-launcher.json"))}, JSON.stringify({
      receipt: JSON.parse(readFileSync(${JSON.stringify(NodePath.join(base, "runtime", "jones-launcher-capability.json"))}, "utf8")), childPid:process.pid,
    }));
  } else if (m.type === "update-rejected") {
    rejections += 1;
    if (${JSON.stringify(options.retryWithDifferentOperation === true)} && rejections === 1) {
      process.send({type:"request-update", targetVersion:${JSON.stringify(target)}, dbPath:${JSON.stringify(dbPath)}, stagedHandle:handle,
        operationId:"32345678-1234-4234-8234-123456789abc"});
      return;
    }
    writeFileSync(${JSON.stringify(NodePath.join(base, "runtime", "rejected.json"))}, JSON.stringify(m));
    setImmediate(() => {
      writeFileSync(${JSON.stringify(NodePath.join(base, "runtime", "old-child-survived.json"))}, JSON.stringify({pid:process.pid, context}));
      process.exit(0);
    });
  }});
  process.send({type:"request-update", targetVersion:${JSON.stringify(target)}, dbPath:${JSON.stringify(dbPath)}, stagedHandle:handle,
    ...(${JSON.stringify(options.operationFailure !== undefined || options.afterOperationReserve !== undefined)} ? {operationId:"12345678-1234-4234-8234-123456789abc"} : {})});
  setInterval(() => {}, 1000);
} else {
  writeFileSync(${JSON.stringify(NodePath.join(base, "runtime", "restart-context.json"))}, JSON.stringify(context));
  process.exit(0);
}
`;
    const active = NodePath.join(base, "runtime", "versions", baseline);
    const payload = NodePath.join(base, "payload");
    for (const directory of [active, payload]) {
      await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
      await NodeFSP.writeFile(NodePath.join(directory, "t3"), child, { mode: 0o755 });
    }
    if (mode === "commit-then-rollback") {
      const laterPayload = NodePath.join(base, "later-payload");
      await NodeFSP.mkdir(laterPayload, { mode: 0o700 });
      await NodeFSP.writeFile(
        NodePath.join(laterPayload, "t3"),
        child.replaceAll(target, laterTarget).replaceAll("a".repeat(40), "e".repeat(40)),
        { mode: 0o755 },
      );
    }
    const metadata: Omit<QualifiedRuntimeReceipt, "payloadSha256"> = {
      protocol: 1,
      repository: "Jones-Systems/Jones-Code",
      channel: "jones-main",
      version: baseline,
      sourceSha: "c".repeat(40),
      sourceTree: "b".repeat(40),
      installedSourceSha: "d".repeat(40),
      workflow:
        fixturePlatform === "darwin"
          ? ".github/workflows/artifact-desktop-mac.yml"
          : ".github/workflows/artifact-cli-linux.yml",
      runId: 100,
      runAttempt: 1,
      artifactId: 99,
      artifactDigest: `sha256:${"e".repeat(64)}`,
      archiveSha256: "f".repeat(64),
      platform: fixturePlatform,
      // oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone launcher fixture must match the native host running its controlled executable.
      architecture: NodeOS.arch() === "arm64" ? "arm64" : "x64",
    };
    await NodeFSP.writeFile(
      NodePath.join(active, QUALIFIED_RUNTIME_RECEIPT),
      JSON.stringify({ ...metadata, payloadSha256: await qualifiedPayloadDigest(active) }),
      { mode: 0o600 },
    );
    await NodeFSP.writeFile(NodePath.join(active, ".install-complete"), baseline);
    const { protocol: _protocol, ...candidate } = metadata;
    const staged = await stageQualifiedRuntime({
      artifact: {
        ...candidate,
        version: target,
        sourceSha: "a".repeat(40),
        installedSourceSha: metadata.sourceSha,
        runId: 101,
        payloadDirectory: payload,
      },
      binding: await currentQualifiedRuntimeBinding(base, baseline),
      validate: async () => {},
    });
    await retainStagedSelection(staged);
    await NodeFSP.writeFile(NodePath.join(base, "runtime", "test-handle"), staged.stagedHandle);
    const statePath = NodePath.join(base, "runtime", "service-state.json");
    let previousUpdate;
    if (options.previous !== undefined) {
      const previousQualified =
        options.previous === "committed"
          ? {
              ...staged,
              receipt: {
                ...staged.receipt,
                version: baseline,
                sourceSha: metadata.sourceSha,
                installedSourceSha: metadata.installedSourceSha,
              },
              binding: {
                ...staged.binding,
                activeVersion: "0.0.0-preview.20261002.99",
                activeSourceSha: metadata.installedSourceSha,
              },
            }
          : staged;
      const generation = NodePath.join(base, "runtime", "db-backup", previousId);
      await NodeFSP.mkdir(generation, { recursive: true, mode: 0o700 });
      await NodeFSP.copyFile(dbPath, NodePath.join(generation, "database"));
      await NodeFSP.writeFile(
        NodePath.join(generation, "paired-binding.json"),
        JSON.stringify(
          options.previousFault === "binding"
            ? {}
            : { transactionId: previousId, qualified: previousQualified },
        ),
      );
      await NodeFSP.writeFile(NodePath.join(generation, "paired-settings.json"), "[]");
      await NodeFSP.writeFile(NodePath.join(generation, "resume-dispatched.json"), "keep");
      await NodeFSP.writeFile(NodePath.join(generation, "unknown-extra"), "keep");
      if (options.previousFault === "directory")
        await NodeFSP.mkdir(NodePath.join(generation, "database-shm"));
      if (options.previousFault === "restore-intent")
        await NodeFSP.writeFile(NodePath.join(generation, ".database-restore.json"), "{}");
      if (options.previous === "rolled-back") {
        const source = NodePath.join(generation, "database");
        const temporary = `${dbPath}.restore-${NodeCrypto.createHash("sha256").update(source).digest("hex").slice(0, 16)}`;
        await NodeFSP.writeFile(
          NodePath.join(generation, ".database-restore.json"),
          JSON.stringify({ source, target: dbPath, temporary }),
        );
        await NodeFSP.writeFile(
          NodePath.join(generation, ".database-restore.json.complete"),
          JSON.stringify({ device: "0", inode: "0" }),
        );
        await NodeFSP.writeFile(NodePath.join(generation, ".restore-pending"), "");
      }
      previousUpdate = {
        id: previousId,
        fromVersion: previousQualified.binding.activeVersion,
        targetVersion: previousQualified.receipt.version,
        qualified: previousQualified,
        ...(options.previous === "accepted"
          ? { status: "pending" as const, phase: "accepted" as const, dbPath }
          : { status: options.previous }),
      };
    }
    await writeServiceState(statePath, {
      protocol: 4,
      activeVersion: baseline,
      ...(previousUpdate === undefined ? {} : { update: previousUpdate }),
    });
    await options.beforeLaunch?.(base, staged);
    let reservations = 0;
    const launcher = new Launcher(base, await readServiceState(statePath), {
      ...(options.startupGateProtocol === "missing" ? {} : { startupGateProtocol: 1 as const }),
      ...(options.backupAdapter === undefined ? {} : { backupAdapter: options.backupAdapter }),
      ...(options.recoveryIO === undefined ? {} : { recoveryIO: options.recoveryIO }),
      ...(options.operationFailure === undefined && options.afterOperationReserve === undefined
        ? {}
        : {
            updateOperationIO: {
              archive:
                options.operationFailure === "archive"
                  ? async () => {
                      throw Object.assign(new Error("fixture archive EIO"), { code: "EIO" });
                    }
                  : archiveUpdateOperation,
              reserve: async (...args: Parameters<typeof reserveUpdateOperation>) => {
                await reserveUpdateOperation(...args);
                await options.afterOperationReserve?.(args[0]);
                if (options.operationFailure === "reserve" && ++reservations === 1) {
                  // A receipt can exist even though its final durability step failed.
                  throw Object.assign(new Error("fixture reserve fsync EIO"), { code: "EIO" });
                }
              },
            },
          }),
      ...(options.now === undefined ? {} : { now: options.now }),
      ...(options.preparedTimeoutMs === undefined
        ? {}
        : { preparedTimeoutMs: options.preparedTimeoutMs }),
    });
    onLauncher?.(launcher);
    const unchangedLive =
      options.recoveryOnly ||
      (options.previous === "committed" && options.previousFault !== undefined)
        ? await Promise.all(
            [
              dbPath,
              NodePath.join(userdata, "settings.json"),
              NodePath.join(userdata, "environment-id"),
            ].map(async (path) => ({
              path,
              bytes: await NodeFSP.readFile(path),
              stat: await NodeFSP.stat(path, { bigint: true }),
            })),
          )
        : [];
    // Captured children exit or are reaped before the launcher settles; cleanup never polls by process name.
    const previousHome = process.env.T3CODE_HOME;
    let launcherFailure: unknown;
    try {
      if (options.productionMain) process.env.T3CODE_HOME = base;
      await (options.productionMain ? main() : launcher.run()).then(
        () => {
          throw new Error("Unexpected launcher completion");
        },
        (cause: unknown) => {
          launcherFailure = cause;
        },
      );
    } finally {
      if (options.productionMain) {
        if (previousHome === undefined) delete process.env.T3CODE_HOME;
        else process.env.T3CODE_HOME = previousHome;
      }
    }
    for (const file of unchangedLive) {
      const stat = await NodeFSP.stat(file.path, { bigint: true });
      assert.equal(stat.dev, file.stat.dev);
      assert.equal(stat.ino, file.stat.ino);
      assert.deepEqual(await NodeFSP.readFile(file.path), file.bytes);
    }
    await body(base, launcherFailure);
  } finally {
    await NodeFSP.rm(allocated, { recursive: true, force: true });
    await NodeAssert.rejects(NodeFSP.lstat(allocated), { code: "ENOENT" });
  }
}

async function assertRecoveryPayloadRemoved(base: string, updateId: string) {
  const backup = NodePath.join(base, "runtime", "db-backup", updateId);
  for (const name of [
    "database",
    "database-wal",
    "database-shm",
    ...QUALIFIED_STARTUP_STATE_FILES,
  ]) {
    await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(backup, name)), { code: "ENOENT" });
  }
  if (NodeFS.existsSync(NodePath.join(backup, ".restore-pending"))) {
    const advanced = NodePath.join(backup, "advanced-state");
    assert.isTrue((await NodeFSP.lstat(advanced)).isDirectory());
    for (const name of ["rename-journal.json", "paired-settings.json"])
      assert.isTrue((await NodeFSP.lstat(NodePath.join(advanced, name))).isFile());
    for (const name of [
      "database",
      "database-wal",
      "database-shm",
      ...QUALIFIED_STARTUP_STATE_FILES,
    ])
      await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(advanced, name)), { code: "ENOENT" });
    for (const name of [".database-restore.json", ".database-restore.json.complete"])
      assert.isTrue((await NodeFSP.lstat(NodePath.join(backup, name))).isFile());
  }
  const directory = await NodeFSP.lstat(backup);
  assert.isTrue(directory.isDirectory());
  assert.isFalse(directory.isSymbolicLink());
  assert.equal(directory.mode & 0o777, 0o700);
  assert.isDefined(await readQualifiedBackupReceipt(base, updateId));
}

it.each(["commit", "rollback"] as const)(
  "F2 removes the recovery copy after a durable %s outcome",
  async (mode) => {
    await runScenario(
      mode,
      async (base) => {
        const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
        assert.equal(state.update?.status, mode === "commit" ? "committed" : "rolled-back");
        assert.equal(
          readMarker(NodePath.join(base, "userdata", "statev2.sqlite")),
          mode === "commit" ? "after" : "before",
        );
        await assertRecoveryPayloadRemoved(base, state.update!.id);
        if (mode === "commit") {
          assert.deepEqual(
            JSON.parse(
              await NodeFSP.readFile(
                qualifiedResumeReservationPath(state.update!.startupReceipt!),
                "utf8",
              ),
            ),
            state.update!.startupReceipt,
          );
          await NodeAssert.rejects(
            assertQualifiedResumeUnreserved(state.update!.startupReceipt!),
            /already reserved/,
          );
        }
      },
      undefined,
      { reserveAfterCleanup: mode === "commit" },
    );
  },
);

it.each(["clone", "copy"] as const)(
  "F2 keeps the SQLite sentinel through a %s snapshot lifecycle",
  async (method) => {
    const flags: number[] = [];
    await runScenario(
      "commit",
      async (base, failure) => {
        const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
        assert.equal(state.update?.status, "committed", String(failure));
        assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "after");
        assert.isTrue(flags.some((flag) => (flag & NodeFS.constants.COPYFILE_FICLONE_FORCE) !== 0));
        if (method === "copy") assert.include(flags, NodeFS.constants.COPYFILE_EXCL);
        await assertRecoveryPayloadRemoved(base, state.update!.id);
      },
      undefined,
      {
        backupAdapter: {
          availableBytes: async () => 100 * 1024 * 1024,
          copyFile: async (source, target, flag) => {
            flags.push(flag);
            if (method === "copy" && flag & NodeFS.constants.COPYFILE_FICLONE_FORCE)
              throw Object.assign(new Error("synthetic unsupported clone"), { code: "ENOTSUP" });
            await NodeFSP.copyFile(source, target, NodeFS.constants.COPYFILE_EXCL);
          },
        },
      },
    );
  },
);

it.each(["archive", "reserve"] as const)(
  "keeps the active child and pointer unchanged when operation %s fails before acceptance",
  async (operationFailure) => {
    await runScenario(
      "commit",
      async (base, failure) => {
        const rejected = JSON.parse(
          await NodeFSP.readFile(NodePath.join(base, "runtime", "rejected.json"), "utf8"),
        );
        assert.match(rejected.reason, /^operation-reconciliation-required:/);
        assert.equal(rejected.operationId, "12345678-1234-4234-8234-123456789abc");
        const survived = JSON.parse(
          await NodeFSP.readFile(NodePath.join(base, "runtime", "old-child-survived.json"), "utf8"),
        );
        assert.equal(survived.context.childVersion, baseline);
        assert.isAbove(survived.pid, 0);
        assert.match(String(failure), /exited unexpectedly/);
        assert.deepEqual(
          await readServiceState(NodePath.join(base, "runtime", "service-state.json")),
          { protocol: 4, activeVersion: baseline },
        );
        assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
        await NodeAssert.rejects(
          NodeFSP.access(NodePath.join(base, "runtime", "observed-active-launcher.json")),
          { code: "ENOENT" },
        );
        assert.deepEqual(await NodeFSP.readdir(NodePath.join(base, "runtime", "db-backup")), []);
        const reserved = NodePath.join(
          base,
          "runtime",
          "jones-update-operations",
          `${rejected.operationId}.json`,
        );
        if (operationFailure === "reserve") {
          await NodeFSP.access(reserved);
          // The refusal proved the reservation can never start.
          assert.equal(
            (await reconcileUpdateOperation(base, rejected.operationId, undefined)).state,
            "rejected",
          );
        } else await NodeAssert.rejects(NodeFSP.access(reserved), { code: "ENOENT" });
      },
      undefined,
      { operationFailure },
    );
  },
);

it("admits a different operation once the refused reservation is recorded as rejected", async () => {
  const first = "12345678-1234-4234-8234-123456789abc";
  const second = "32345678-1234-4234-8234-123456789abc";
  await runScenario(
    "commit",
    async (base, failure) => {
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.update?.id, second, String(failure));
      assert.equal(state.update?.status, "committed");
      assert.equal((await reconcileUpdateOperation(base, first, state.update)).state, "rejected");
      assert.equal((await reconcileUpdateOperation(base, second, state.update)).state, "committed");
      assert.deepEqual(
        (await NodeFSP.readdir(NodePath.join(base, "runtime", "jones-update-operations"))).sort(),
        [`${first}.json`, `${first}.outcome.json`, `${second}.json`],
      );
    },
    undefined,
    { operationFailure: "reserve", retryWithDifferentOperation: true },
  );
});

it("a fresh launcher records only an abandoned reservation as rejected", async () => {
  const accepted = "12345678-1234-4234-8234-123456789abc";
  const abandoned = "52345678-1234-4234-8234-123456789abc";
  await runScenario(
    "commit",
    async (base) => {
      const statePath = NodePath.join(base, "runtime", "service-state.json");
      const state = await readServiceState(statePath);
      assert.equal(state.update?.id, accepted);
      // A launcher that died between reserving and accepting left only this reservation.
      await reserveUpdateOperation(base, abandoned, {
        environmentId: "synthetic-jones-environment",
        currentVersion: target,
        expectedInstalledSource: "a".repeat(40),
        targetSource: "e".repeat(40),
        stagedHandle: "62345678-1234-4234-8234-123456789abc",
        baseDir: base,
        dbPath: NodePath.join(base, "userdata", "statev2.sqlite"),
        targetVersion: laterTarget,
      });
      assert.equal(
        (await reconcileUpdateOperation(base, abandoned, state.update)).state,
        "blocked",
      );
      await NodeAssert.rejects(
        new Launcher(base, state, { startupGateProtocol: 1 }).run(),
        /Active child exited unexpectedly/,
      );
      const recovered = await readServiceState(statePath);
      assert.equal(
        (await reconcileUpdateOperation(base, abandoned, recovered.update)).state,
        "rejected",
      );
      // The operation native state names is never rejected, even without an outcome receipt.
      assert.equal(
        (await reconcileUpdateOperation(base, accepted, recovered.update)).state,
        "committed",
      );
      assert.deepEqual(
        (await NodeFSP.readdir(NodePath.join(base, "runtime", "jones-update-operations"))).sort(),
        [`${accepted}.json`, `${abandoned}.json`, `${abandoned}.outcome.json`],
      );
    },
    undefined,
    { afterOperationReserve: () => {} },
  );
});

it("answers a pipelined outcome question only after the slow Install it follows is accepted", async () => {
  const operationId = "12345678-1234-4234-8234-123456789abc";
  let window: string | undefined;
  await runScenario(
    "commit",
    async (base, failure) => {
      const { outcomes, rejection } = JSON.parse(
        await NodeFSP.readFile(NodePath.join(base, "runtime", "outcomes.json"), "utf8"),
      );
      // HFR-440-F1: the receipts alone showed a reservation without an outcome.
      assert.equal(window, "blocked");
      assert.isUndefined(rejection);
      assert.deepEqual(outcomes, [
        { type: "update-outcome", operationId, queryId: 1, outcome: "accepted" },
        {
          type: "update-outcome",
          operationId: "42345678-1234-4234-8234-123456789abc",
          queryId: 2,
          outcome: "rejected",
          reason: "The launcher holds no reservation for this operation.",
        },
      ]);
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.update?.id, operationId, String(failure));
      assert.equal(state.update?.status, "committed");
    },
    undefined,
    {
      outcomeQueries: true,
      afterOperationReserve: async (baseDir) => {
        // Slow durability between reservation and acceptance, past the server's timeout.
        window = (await reconcileUpdateOperation(baseDir, operationId, undefined)).state;
        await NodeTimersPromises.setTimeout(200);
      },
    },
  );
});

it("answers rejected, durably, for an Install refused after its reservation", async () => {
  const operationId = "12345678-1234-4234-8234-123456789abc";
  let clock = 0;
  await runScenario(
    "commit",
    async (base) => {
      const { outcomes, rejection, context } = JSON.parse(
        await NodeFSP.readFile(NodePath.join(base, "runtime", "outcomes.json"), "utf8"),
      );
      assert.equal(context.updateOutcomeProtocol, 1);
      assert.match(rejection.reason, /^request-expired:/);
      assert.equal(outcomes[0].outcome, "rejected");
      assert.equal(outcomes[0].queryId, 1);
      assert.equal(outcomes[1].outcome, "rejected");
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.update, undefined);
      assert.equal(
        (await reconcileUpdateOperation(base, operationId, undefined)).state,
        "rejected",
      );
      await NodeAssert.rejects(
        NodeFSP.access(NodePath.join(base, "runtime", "observed-active-launcher.json")),
      );
      assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
    },
    undefined,
    {
      outcomeQueries: true,
      now: () => clock,
      afterOperationReserve: () => {
        clock += 60_000;
      },
    },
  );
});

it("waits for graceful child drain before capturing the rollback database", async () => {
  let snapshot: unknown;
  await runScenario(
    "commit",
    async (base) => {
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.update?.status, "committed");
      assert.equal(snapshot, "gracefully-drained");
      assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "after");
    },
    undefined,
    {
      delayedGracefulClose: true,
      backupAdapter: {
        availableBytes: async () => 100 * 1024 * 1024,
        copyFile: async (source, target, flags) => {
          if (source.endsWith("statev2.sqlite")) snapshot = readMarker(source);
          await NodeFSP.copyFile(source, target, flags);
        },
      },
    },
  );
});

it("commits a qualified trial and retains the resume guard after snapshot cleanup", async () => {
  await runScenario("commit", async (base) => {
    for (const [role, childVersion] of [
      ["active", baseline],
      ["trial", target],
    ] as const) {
      const observed = JSON.parse(
        await NodeFSP.readFile(
          NodePath.join(base, "runtime", `observed-${role}-launcher.json`),
          "utf8",
        ),
      );
      assert.deepEqual(observed.receipt, {
        schema: 1,
        baseDir: base,
        launcherVersion: packageJson.version,
        launcherPid: process.pid,
        launcherProtocol: 4,
        qualifiedUpdatesProtocol: 1,
        startupGateProtocol: 1,
        updateSettlementProtocol: 1,
        childPid: observed.childPid,
        childVersion,
      });
    }
    await NodeAssert.rejects(
      NodeFSP.access(NodePath.join(base, "runtime", "jones-launcher-capability.json")),
    );
    const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
    assert.equal(state.activeVersion, target);
    assert.equal(state.update?.status, "committed");
    assert.equal(state.update?.startupReceipt?.startupGateProtocol, 1);
    await assertRecoveryPayloadRemoved(base, state.update!.id);
    const timing = await readQualifiedBackupReceipt(base, state.update!.id);
    assert.isNumber(timing?.copyMs);
    assert.isNumber(timing?.syncMs);
    assert.isAtLeast(timing!.copyMs!, 0);
    assert.isAtLeast(timing!.syncMs!, 0);
    assert.isAtMost(timing!.copyMs! + timing!.syncMs!, timing!.durationMs);
    assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "after");
    await NodeFSP.access(NodePath.join(base, "runtime", "versions", baseline, "t3"));
    const reservation = qualifiedResumeReservationPath(state.update!.startupReceipt!);
    assert.deepEqual(
      JSON.parse(await NodeFSP.readFile(reservation, "utf8")),
      state.update!.startupReceipt,
    );
    const statePath = NodePath.join(base, "runtime", "service-state.json");
    const beforeRecovery = await NodeFSP.readFile(statePath, "utf8");
    const beforeReservation = await NodeFSP.readFile(reservation, "utf8");
    await NodeAssert.rejects(
      assertQualifiedResumeUnreserved(state.update!.startupReceipt!),
      /already reserved/,
    );
    await NodeAssert.rejects(
      new Launcher(base, state, {
        startupGateProtocol: 1,
      }).run(),
      /Active child exited unexpectedly/,
    );
    assert.equal(await NodeFSP.readFile(statePath, "utf8"), beforeRecovery);
    assert.equal(await NodeFSP.readFile(reservation, "utf8"), beforeReservation);
    const restarted = JSON.parse(
      await NodeFSP.readFile(NodePath.join(base, "runtime", "restart-context.json"), "utf8"),
    );
    assert.equal(restarted.childVersion, target);
    assert.equal(restarted.update.status, "committed");
    assert.equal(restarted.startupGateProtocol, 1);
  });
});

it("holds a stale child transaction until the lifetime owner releases the home", async () => {
  await runScenario("stale-child", async (base) => {
    const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
    assert.equal(state.activeVersion, baseline);
    assert.equal(state.update?.status, "pending");
    assert.equal(state.update?.startupReceipt, undefined);
    const backup = NodePath.join(base, "runtime", "db-backup", state.update!.id);
    assert.equal(readMarker(NodePath.join(backup, "database")), "before");
    assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "after");
    await NodeAssert.rejects(NodeFSP.access(NodePath.join(backup, "resume-dispatched.json")));
    const statePath = NodePath.join(base, "runtime", "service-state.json");
    const beforeRecovery = await NodeFSP.readFile(statePath, "utf8");
    const owner = new NodeSqlite.DatabaseSync(
      NodePath.join(base, "userdata", "server-owner.sqlite"),
    );
    owner.exec("BEGIN EXCLUSIVE;");
    try {
      await NodeAssert.rejects(
        new Launcher(base, state, { startupGateProtocol: 1 }).run(),
        /state-dir-owned/,
      );
      assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "after");
      assert.equal(readMarker(NodePath.join(backup, "database")), "before");
    } finally {
      owner.close();
    }
    assert.equal(await NodeFSP.readFile(statePath, "utf8"), beforeRecovery);
    assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "after");
    await NodeAssert.rejects(
      new Launcher(base, state, {
        startupGateProtocol: 1,
      }).run(),
      /Active child exited unexpectedly/,
    );
    const recovered = await readServiceState(statePath);
    assert.equal(recovered.activeVersion, baseline);
    assert.equal(recovered.update?.status, "rolled-back");
    assert.equal(
      recovered.update?.status === "pending" ? undefined : recovered.update?.reason,
      "launcher-restarted",
    );
    assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
    await assertRecoveryPayloadRemoved(base, state.update!.id);
  });
});

it("rejects insufficient copy capacity before accepting Install or stopping the active child", async () => {
  await runScenario(
    "commit",
    async (base) => {
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.update, undefined);
      assert.equal(state.activeVersion, baseline);
      const rejected = JSON.parse(
        await NodeFSP.readFile(NodePath.join(base, "runtime", "rejected.json"), "utf8"),
      );
      assert.match(rejected.reason, /recovery-capacity/);
      assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
      assert.deepEqual(await NodeFSP.readdir(NodePath.join(base, "runtime", "db-backup")), []);
    },
    undefined,
    {
      backupAdapter: {
        availableBytes: async (directory) => {
          const userdata = NodePath.resolve(directory, "../../userdata");
          let bytes = 0;
          for (const name of [
            "statev2.sqlite",
            "statev2.sqlite-wal",
            "statev2.sqlite-shm",
            ...QUALIFIED_STARTUP_STATE_FILES,
          ]) {
            bytes += await NodeFSP.stat(NodePath.join(userdata, name)).then(
              (stat) => stat.size,
              (cause) => {
                if (cause.code === "ENOENT") return 0;
                throw cause;
              },
            );
          }
          return 2 * bytes + Math.ceil(0.05 * bytes) + 1024 * 1024 - 1;
        },
        copyFile: async () => {
          throw Object.assign(new Error("synthetic unsupported clone"), { code: "ENOTSUP" });
        },
      },
    },
  );
});

it("binds the qualified startup gate through production main", async () => {
  await runScenario(
    "commit",
    async (base, launcherFailure) => {
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(
        state.activeVersion,
        target,
        `Launcher failure: ${String(launcherFailure)}; state: ${JSON.stringify(state)}`,
      );
      assert.equal(state.update?.status, "committed");
    },
    undefined,
    { productionMain: true },
  );
});

it("rejects a staged Install when the launcher's startup gate is absent", async () => {
  await runScenario(
    "commit",
    async (base) => {
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.update, undefined);
      const rejected = JSON.parse(
        await NodeFSP.readFile(NodePath.join(base, "runtime", "rejected.json"), "utf8"),
      );
      assert.match(rejected.reason, /bootstrap-required/);
    },
    undefined,
    { startupGateProtocol: "missing" },
  );
});

it("reports a candidate's bound migration refusal before accepting or stopping the server", async () => {
  await runScenario("blocked-candidate", async (base) => {
    const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
    assert.equal(state.update, undefined);
    assert.equal(state.activeVersion, baseline);
    const rejected = JSON.parse(
      await NodeFSP.readFile(NodePath.join(base, "runtime", "rejected.json"), "utf8"),
    );
    assert.equal(rejected.reason, "candidate-older-than-database");
    assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
    await NodeAssert.rejects(NodeFSP.access(NodePath.join(base, "runtime", "db-backup")));
  });
});

it("F2 resumes an interrupted restore marker before starting the previous runtime", async () => {
  await runScenario("stale-child", async (base) => {
    const statePath = NodePath.join(base, "runtime", "service-state.json");
    const state = await readServiceState(statePath);
    const backup = NodePath.join(base, "runtime", "db-backup", state.update!.id);
    const advanced = NodePath.join(backup, "advanced-state");
    const database = NodePath.join(base, "userdata", "statev2.sqlite");
    await NodeFSP.mkdir(advanced, { mode: 0o700 });
    const names = ["database", ...QUALIFIED_STARTUP_STATE_FILES];
    const entries = [];
    for (const name of names) {
      const source = name === "database" ? database : NodePath.join(base, "userdata", name);
      const stat = await NodeFSP.stat(source, { bigint: true }).catch(
        (cause: NodeJS.ErrnoException) => {
          if (cause.code === "ENOENT") return undefined;
          throw cause;
        },
      );
      if (stat !== undefined)
        entries.push({ name, device: stat.dev.toString(), inode: stat.ino.toString() });
    }
    await NodeFSP.writeFile(
      NodePath.join(advanced, "rename-journal.json"),
      JSON.stringify(entries),
    );
    await NodeFSP.writeFile(NodePath.join(backup, ".restore-pending"), "");
    await NodeFSP.rename(database, NodePath.join(advanced, "database"));
    await NodeFSP.copyFile(NodePath.join(backup, "database"), database);
    await NodeAssert.rejects(
      new Launcher(base, state, {
        startupGateProtocol: 1,
      }).run(),
      /Active child exited unexpectedly/,
    );
    const recovered = await readServiceState(statePath);
    assert.equal(recovered.update?.status, "rolled-back");
    assert.equal(readMarker(database), "before");
    assert.equal(
      await NodeFSP.readFile(NodePath.join(base, "userdata", "settings.json"), "utf8"),
      "before-settings",
    );
    await assertRecoveryPayloadRemoved(base, state.update!.id);
    const restarted = JSON.parse(
      await NodeFSP.readFile(NodePath.join(base, "runtime", "restart-context.json"), "utf8"),
    );
    assert.equal(restarted.childVersion, baseline);
    assert.equal(restarted.update.status, "rolled-back");
  });
});

it("records stop intent during prepared validation before committing or granting qualified resume", async () => {
  let captured: Launcher | undefined;
  let stop: Promise<void> | undefined;
  await runScenario(
    "commit",
    async (base) => {
      await stop;
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.activeVersion, baseline);
      assert.equal(state.update?.status, "pending");
      assert.equal(state.update?.startupReceipt, undefined);
      await NodeFSP.access(NodePath.join(base, "runtime", ".service-stopping"));
      await NodeAssert.rejects(
        NodeFSP.access(
          NodePath.join(base, "runtime", "db-backup", state.update!.id, "resume-dispatched.json"),
        ),
      );
    },
    (launcher) => {
      captured = launcher;
    },
    {
      now: (() => {
        let reads = 0;
        return () => {
          if (++reads === 5) {
            if (captured === undefined) throw new Error("missing controlled launcher");
            stop = captured.stop("SIGTERM");
          }
          return 0;
        };
      })(),
    },
  );
});

it("restores paired settings and SQLite before removing a failed trial snapshot", async () => {
  await runScenario("rollback", async (base) => {
    const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
    assert.equal(state.activeVersion, baseline);
    assert.equal(state.update?.status, "rolled-back");
    assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
    assert.equal(
      await NodeFSP.readFile(NodePath.join(base, "userdata", "settings.json"), "utf8"),
      "before-settings",
    );
    assert.equal(
      await NodeFSP.lstat(NodePath.join(base, "userdata", "keybindings.json")).then(
        () => true,
        () => false,
      ),
      false,
    );
    await assertRecoveryPayloadRemoved(base, state.update!.id);
  });
});

it("does not accept an Install whose admission outlasted the server's exchange", async () => {
  let clock = 0;
  await runScenario(
    "commit",
    async (base) => {
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.update, undefined);
      assert.equal(state.activeVersion, baseline);
      const rejected = JSON.parse(
        await NodeFSP.readFile(NodePath.join(base, "runtime", "rejected.json"), "utf8"),
      );
      assert.match(rejected.reason, /^request-expired:/);
      await NodeAssert.rejects(
        NodeFSP.access(NodePath.join(base, "runtime", "observed-active-launcher.json")),
      );
      assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
    },
    undefined,
    { now: () => (++clock === 1 ? 0 : 60_000) },
  );
});

it("preserves the active child and records the reservation rejected when receipt preparation expires the exchange", async () => {
  let clock = 0;
  await runScenario(
    "commit",
    async (base) => {
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.activeVersion, baseline);
      assert.equal(state.update, undefined);
      const rejected = JSON.parse(
        await NodeFSP.readFile(NodePath.join(base, "runtime", "rejected.json"), "utf8"),
      );
      const operationId = "12345678-1234-4234-8234-123456789abc";
      assert.match(rejected.reason, /^request-expired:/);
      assert.equal(rejected.operationId, operationId);
      const reservation = await readOperationReservation(base, operationId);
      assert.equal(reservation?.binding.currentVersion, baseline);
      assert.equal(reservation?.binding.targetVersion, target);
      assert.equal(reservation?.binding.expectedInstalledSource, "c".repeat(40));
      assert.equal(
        (await reconcileUpdateOperation(base, operationId, state.update)).state,
        "rejected",
      );
      const survived = JSON.parse(
        await NodeFSP.readFile(NodePath.join(base, "runtime", "old-child-survived.json"), "utf8"),
      );
      assert.equal(survived.context.childVersion, baseline);
      await NodeAssert.rejects(
        NodeFSP.access(NodePath.join(base, "runtime", "observed-active-launcher.json")),
      );
      assert.deepEqual(await NodeFSP.readdir(NodePath.join(base, "runtime", "db-backup")), []);
      assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
    },
    undefined,
    {
      now: () => clock,
      afterOperationReserve: () => {
        clock += 60_000;
      },
    },
  );
});

it("rolls back instead of committing a pointer after the prepared exchange expired", async () => {
  let clock = 0;
  await runScenario(
    "commit",
    async (base) => {
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.activeVersion, baseline);
      assert.equal(state.update?.status, "rolled-back");
      assert.equal(
        state.update?.status === "pending" ? undefined : state.update?.reason,
        "prepared-exchange-expired",
      );
      assert.equal(state.update?.startupReceipt, undefined);
      assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
      await NodeAssert.rejects(
        NodeFSP.access(
          NodePath.join(base, "runtime", "db-backup", state.update!.id, "resume-dispatched.json"),
        ),
      );
    },
    undefined,
    { now: () => (++clock < 5 ? 0 : 60_000) },
  );
});

it.each(["missing-gate", "wrong-gate"] as const)(
  "rejects candidate %s before recording or launching a qualified transaction",
  async (mode) => {
    await runScenario(mode, async (base) => {
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.activeVersion, baseline);
      assert.equal(state.update, undefined);
      assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
      assert.equal(
        await NodeFSP.readFile(NodePath.join(base, "userdata", "settings.json"), "utf8"),
        "before-settings",
      );
      await NodeAssert.rejects(NodeFSP.access(NodePath.join(base, "runtime", "db-backup")));
      await NodeFSP.access(NodePath.join(base, "runtime", "test-handle"));
    });
  },
);

it.each(["accept-first", "retire-first"] as const)(
  "serializes actual IPC retirement against %s delivery",
  async (retirementRace) => {
    await runScenario(
      "commit",
      async (base) => {
        const result = JSON.parse(
          await NodeFSP.readFile(NodePath.join(base, "runtime", "retirement-result.json"), "utf8"),
        );
        assert.equal(result.retired, retirementRace === "retire-first");
        const handle = await NodeFSP.readFile(
          NodePath.join(base, "runtime", "test-handle"),
          "utf8",
        );
        if (retirementRace === "accept-first") {
          assert.match(result.reason, /may have been accepted/);
          await assertStageNotRevoked(base, handle);
        } else {
          for (const index of [1, 2]) {
            const rejected = JSON.parse(
              await NodeFSP.readFile(
                NodePath.join(base, "runtime", `late-rejection-${index}.json`),
                "utf8",
              ),
            );
            assert.match(rejected.reason, /durably retired/);
          }
          assert.equal(
            (await readServiceState(NodePath.join(base, "runtime", "service-state.json"))).update,
            undefined,
          );
          await NodeAssert.rejects(assertStageNotRevoked(base, handle), /durably retired/);
        }
        assert.equal(
          typeof (await NodeFSP.readFile(
            NodePath.join(base, "runtime", "versions", target, "t3"),
            "utf8",
          )),
          "string",
        );
      },
      undefined,
      { retirementRace },
    );
  },
);

it("names the pointer-write failure and retains the pending pair", async () => {
  await runScenario(
    "commit",
    async (base, failure) => {
      assert.match(String(failure), /Qualified pointer commit failed at step pointer-write/);
      assert.match(String(failure), /EISDIR/);
      const state = await readServiceState(NodePath.join(base, "runtime", "before-commit.json"));
      assert.equal(state.activeVersion, baseline);
      assert.equal(state.update?.status, "pending");
      assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "after");
      assert.equal(
        readMarker(NodePath.join(base, "runtime", "db-backup", state.update!.id, "database")),
        "before",
      );
    },
    undefined,
    {
      pointerWriteFailure: true,
    },
  );
});

it("F2 restores the SQLite sentinel after a prepared timeout and removes the snapshot", async () => {
  await runScenario(
    "timeout",
    async (base) => {
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.activeVersion, baseline);
      assert.equal(state.update?.status, "rolled-back");
      assert.equal(
        state.update?.status === "pending" ? undefined : state.update?.reason,
        "prepared-timeout",
      );
      assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
      await assertRecoveryPayloadRemoved(base, state.update!.id);
    },
    undefined,
    { preparedTimeoutMs: 1000 },
  );
});

it("keeps a durable commit and its resume guard when recovery payload cleanup fails", async () => {
  const output: string[] = [];
  const stderr = vi.spyOn(process.stderr, "write").mockImplementation((chunk) => {
    output.push(String(chunk));
    return true;
  });
  try {
    await runScenario(
      "commit",
      async (base, failure) => {
        const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
        assert.equal(state.activeVersion, target);
        assert.equal(state.update?.status, "committed");
        assert.match(String(failure), /Active child exited unexpectedly/);
        assert.notMatch(String(failure), /Qualified pointer commit failed/);
        assert.include(output.join(""), "Recovery payload cleanup did not finish");
        assert.deepEqual(
          JSON.parse(
            await NodeFSP.readFile(
              qualifiedResumeReservationPath(state.update!.startupReceipt!),
              "utf8",
            ),
          ),
          state.update!.startupReceipt,
        );
        await NodeAssert.rejects(
          assertQualifiedResumeUnreserved(state.update!.startupReceipt!),
          /already reserved/,
        );
        assert.isTrue(
          (
            await NodeFSP.lstat(
              NodePath.join(base, "runtime", "db-backup", state.update!.id, "database-shm"),
            )
          ).isDirectory(),
        );
      },
      undefined,
      { cleanupFailure: true },
    );
  } finally {
    stderr.mockRestore();
  }
});

it("reports launcher drift when a qualified trial becomes active", async () => {
  const output: string[] = [];
  const stderr = vi.spyOn(process.stderr, "write").mockImplementation((chunk) => {
    output.push(String(chunk));
    return true;
  });
  try {
    await runScenario("commit", async (base) => {
      assert.equal(
        (await readServiceState(NodePath.join(base, "runtime", "service-state.json")))
          .activeVersion,
        target,
      );
      assert.include(
        output.join(""),
        `Launcher ${packageJson.version} is supervising server ${target}.`,
      );
      assert.include(output.join(""), "planned restart");
    });
  } finally {
    stderr.mockRestore();
  }
});

it("rolls back a later failed trial in the launcher that committed its predecessor", async () => {
  await runScenario("commit-then-rollback", async (base, failure) => {
    const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
    assert.equal(state.activeVersion, target, String(failure));
    assert.equal(state.update?.targetVersion, laterTarget);
    assert.equal(state.update?.status, "rolled-back", String(failure));
    assert.match(String(failure), /Active child exited unexpectedly/);
    assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "after");
    const restarted = JSON.parse(
      await NodeFSP.readFile(NodePath.join(base, "runtime", "restart-context.json"), "utf8"),
    );
    assert.equal(restarted.childVersion, target);
    assert.equal(restarted.update.status, "rolled-back");
  });
});

it.each(["committed", "rolled-back"] as const)(
  "retries the previous %s payload before new Install capacity checks",
  async (previous) => {
    let capacityObserved = false;
    await runScenario(
      "commit",
      async (base, failure) => {
        assert.match(String(failure), /Active child exited unexpectedly/);
        assert.isTrue(capacityObserved);
        const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
        assert.equal(state.update?.status, "committed");
        assert.notEqual(state.update?.id, previousId);
        const generation = NodePath.join(base, "runtime", "db-backup", previousId);
        await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(generation, "database")), {
          code: "ENOENT",
        });
        for (const name of [
          "paired-binding.json",
          "paired-settings.json",
          "resume-dispatched.json",
          "unknown-extra",
        ])
          assert.isTrue((await NodeFSP.lstat(NodePath.join(generation, name))).isFile());
        if (previous === "rolled-back") {
          assert.isTrue(
            (
              await NodeFSP.lstat(NodePath.join(generation, ".database-restore.json.complete"))
            ).isFile(),
          );
          assert.isTrue(
            (await NodeFSP.lstat(NodePath.join(generation, ".restore-pending"))).isFile(),
          );
        }
      },
      undefined,
      {
        previous,
        backupAdapter: {
          copyFile: async (source, target) =>
            NodeFSP.copyFile(source, target, NodeFS.constants.COPYFILE_EXCL),
          availableBytes: async (directory) => {
            const generation = NodePath.join(directory, previousId);
            assert.isTrue((await NodeFSP.lstat(generation)).isDirectory());
            await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(generation, "database")), {
              code: "ENOENT",
            });
            capacityObserved = true;
            return Number.MAX_SAFE_INTEGER;
          },
        },
      },
    );
  },
);

it.each(["directory", "binding", "restore-intent"] as const)(
  "refuses a new Install and preserves the active child on prior cleanup %s",
  async (previousFault) => {
    await runScenario(
      "commit",
      async (base, failure) => {
        assert.match(String(failure), /Active child exited unexpectedly/);
        const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
        assert.equal(state.update?.id, previousId);
        assert.equal(state.activeVersion, baseline);
        const rejection = JSON.parse(
          await NodeFSP.readFile(NodePath.join(base, "runtime", "rejected.json"), "utf8"),
        );
        assert.match(
          rejection.reason,
          /^recovery-cleanup-required: .*the current server remains active/,
        );
        const survivor = JSON.parse(
          await NodeFSP.readFile(NodePath.join(base, "runtime", "old-child-survived.json"), "utf8"),
        );
        assert.equal(survivor.context.childVersion, baseline);
        assert.equal(survivor.context.update.id, previousId);
        assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
        assert.equal(
          readMarker(NodePath.join(base, "runtime", "db-backup", previousId, "database")),
          "before",
        );
      },
      undefined,
      { previous: "committed", previousFault },
    );
  },
);

it("leaves a previous failed generation held while permitting a later Install", async () => {
  await runScenario(
    "commit",
    async (base) => {
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.update?.status, "committed");
      assert.notEqual(state.update?.id, previousId);
      assert.equal(
        readMarker(NodePath.join(base, "runtime", "db-backup", previousId, "database")),
        "before",
      );
    },
    undefined,
    { previous: "failed", previousFault: "restore-intent" },
  );
});

it.each([false, true])(
  "reconciles accepted pre-trial restart without changing live state (restore intent: %s)",
  async (hold) => {
    await runScenario(
      "commit",
      async (base, failure) => {
        assert.match(String(failure), /Active child exited unexpectedly/);
        const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
        assert.equal(state.update?.id, previousId);
        assert.equal(state.update?.status, "failed");
        assert.equal(
          state.update?.status === "pending" ? undefined : state.update?.reason,
          "launcher-restarted",
        );
        assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
        const generation = NodePath.join(base, "runtime", "db-backup", previousId);
        if (hold) assert.equal(readMarker(NodePath.join(generation, "database")), "before");
        else
          await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(generation, "database")), {
            code: "ENOENT",
          });
        assert.isTrue(
          (await NodeFSP.lstat(NodePath.join(generation, "paired-binding.json"))).isFile(),
        );
      },
      undefined,
      {
        previous: "accepted",
        recoveryOnly: true,
        ...(hold ? { previousFault: "restore-intent" } : {}),
      },
    );
  },
);

it("settles a pre-trial re-verification failure before admitting a fresh Install ID", async () => {
  let original: Buffer | undefined;
  await runScenario(
    "commit",
    async (base, failure) => {
      assert.match(String(failure), /payload|digest/i);
      const statePath = NodePath.join(base, "runtime", "service-state.json");
      const accepted = await readServiceState(statePath);
      assert.equal(accepted.update?.status, "pending");
      const failedId = accepted.update!.id;
      await NodeFSP.writeFile(NodePath.join(base, "runtime", "versions", target, "t3"), original!);
      const recover = new Launcher(base, accepted, { startupGateProtocol: 1 });
      await NodeAssert.rejects(recover.run(), /Active child exited unexpectedly/);
      const failed = await readServiceState(statePath);
      assert.equal(failed.update?.status, "failed");
      assert.equal(failed.activeVersion, baseline);
      assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
      const settled = await readUpdateOperationSettlement(base, failedId);
      assert.equal(settled?.basis, "launcher-recorded");
      assert.equal((await readUpdateOperationCause(base, failedId))?.step, "reverify");
      await NodeFSP.writeFile(NodePath.join(base, "runtime", "retry-settled"), "");
      await NodeAssert.rejects(
        new Launcher(base, failed, { startupGateProtocol: 1 }).run(),
        /Active child exited unexpectedly/,
      );
      const final = await readServiceState(statePath);
      const rejectionPath = NodePath.join(base, "runtime", "settlement-rejection-2.json");
      assert.equal(
        final.update?.status,
        "committed",
        NodeFS.existsSync(rejectionPath)
          ? await NodeFSP.readFile(rejectionPath, "utf8")
          : "Fresh Install must commit",
      );
      assert.equal(final.update?.id, "32345678-1234-4234-8234-123456789abc");
      const rejected = JSON.parse(
        await NodeFSP.readFile(
          NodePath.join(base, "runtime", "settlement-rejection-1.json"),
          "utf8",
        ),
      );
      assert.equal(rejected.operationId, failedId);
      assert.equal((await reconcileUpdateOperation(base, failedId, final.update)).state, "blocked");
    },
    undefined,
    {
      settlementRetry: true,
      afterOperationReserve: async (base) => {
        const entry = NodePath.join(base, "runtime", "versions", target, "t3");
        original = await NodeFSP.readFile(entry);
        await NodeFSP.appendFile(entry, "\n// synthetic changed payload\n");
      },
    },
  );
});

it.each(["reverify", "backup", "ownership", "cause-write"] as const)(
  "retains a qualified %s failure without changing live state and settles on restart",
  async (step) => {
    const injected = Object.assign(new Error("synthetic preparation failure at /private/fixture"), {
      code: "EIO",
    });
    await runScenario(
      "commit",
      async (base, failure) => {
        assert.equal(failure, injected);
        const statePath = NodePath.join(base, "runtime", "service-state.json");
        const accepted = await readServiceState(statePath);
        assert.equal(accepted.update?.status, "pending");
        const id = accepted.update!.id;
        const cause = await readUpdateOperationCause(base, id);
        if (step === "cause-write") assert.isUndefined(cause);
        else {
          assert.equal(cause?.step, step);
          assert.equal(cause?.code, "EIO");
          assert.notInclude(cause!.message, "/private/fixture");
        }
        await NodeAssert.rejects(
          new Launcher(base, accepted, { startupGateProtocol: 1 }).run(),
          /Active child exited unexpectedly/,
        );
        const failed = await readServiceState(statePath);
        assert.equal(failed.update?.status, "failed");
        assert.equal(failed.activeVersion, baseline);
        assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
        const settlement = await readUpdateOperationSettlement(base, id);
        assert.equal(settlement?.basis, "launcher-recorded");
        assert.isFalse(
          NodeFS.existsSync(NodePath.join(base, "runtime", "db-backup", id, "database")),
        );
        if (settlement?.basis !== "retired-exception")
          assert.equal(settlement?.evidence.cause, cause?.message ?? null);
      },
      undefined,
      {
        afterOperationReserve: async () => {},
        ...(step === "backup"
          ? {
              backupAdapter: {
                availableBytes: async () => 100 * 1024 * 1024,
                copyFile: async (source, destination, flags) => {
                  if (source.endsWith("statev2.sqlite")) throw injected;
                  await NodeFSP.copyFile(source, destination, flags);
                },
              },
            }
          : {}),
        recoveryIO: {
          ...(step === "reverify" || step === "cause-write"
            ? {
                verify: async () => {
                  throw injected;
                },
              }
            : {}),
          ...(step === "ownership"
            ? {
                ownership: async () => {
                  throw injected;
                },
              }
            : {}),
          ...(step === "cause-write"
            ? {
                cause: async () => {
                  throw new Error("diagnostic write failure");
                },
              }
            : {}),
        },
      },
    );
  },
);

async function seedSettlementFixture(
  base: string,
  qualified: StagedQualifiedRuntime,
  pending = false,
) {
  await reserveUpdateOperation(base, previousId, operationBinding(qualified));
  const update = {
    id: previousId,
    fromVersion: baseline,
    targetVersion: target,
    qualified,
    ...(pending
      ? { status: "pending" as const, phase: "accepted" as const, dbPath: qualified.binding.dbPath }
      : { status: "failed" as const, reason: "launcher-restarted" }),
  };
  const state: ServiceState = { protocol: 4, activeVersion: baseline, update };
  const backup = NodePath.join(base, "runtime", "db-backup", previousId);
  await NodeFSP.mkdir(backup, { recursive: true, mode: 0o700 });
  await NodeFSP.copyFile(qualified.binding.dbPath, NodePath.join(backup, "database"));
  await NodeFSP.writeFile(
    NodePath.join(backup, "paired-binding.json"),
    JSON.stringify({ transactionId: previousId, qualified }),
    { mode: 0o600 },
  );
  await writeServiceState(NodePath.join(base, "runtime", "service-state.json"), state);
  return state;
}

it.each([
  "outcome",
  "cleanup",
  "written",
  "file-synced",
  "linked",
  "directory-synced",
  "native-state",
] as const)(
  "repeats the durable pre-trial settlement prefix after interruption at %s",
  async (phase) => {
    const interrupted = new Error(`synthetic interruption after ${phase}`);
    await runScenario(
      "commit",
      async (base, failure) => {
        assert.equal(failure, interrupted);
        const statePath = NodePath.join(base, "runtime", "service-state.json");
        const before = await readServiceState(statePath);
        assert.equal(before.update?.status, phase === "native-state" ? "failed" : "pending");
        const outcomePath = NodePath.join(
          base,
          "runtime",
          "jones-update-operations",
          `${previousId}.outcome.json`,
        );
        const outcomeBytes = await NodeFSP.readFile(outcomePath);
        assert.equal(JSON.parse(outcomeBytes.toString()).status, "failed");
        assert.notProperty(JSON.parse(outcomeBytes.toString()), "reason");
        await NodeAssert.rejects(
          new Launcher(base, before, { startupGateProtocol: 1 }).run(),
          /Active child exited unexpectedly/,
        );
        const after = await readServiceState(statePath);
        assert.equal(after.update?.status, "failed");
        assert.equal(after.activeVersion, baseline);
        assert.deepEqual(await NodeFSP.readFile(outcomePath), outcomeBytes);
        assert.equal(
          (await readUpdateOperationSettlement(base, previousId))?.basis,
          "launcher-recorded",
        );
        assert.isFalse(
          NodeFS.existsSync(NodePath.join(base, "runtime", "db-backup", previousId, "database")),
        );
        assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
      },
      undefined,
      {
        beforeLaunch: async (base, qualified) => {
          await seedSettlementFixture(base, qualified, true);
        },
        recoveryIO: {
          ...(phase === "outcome"
            ? {
                failedOutcome: async (...args: Parameters<typeof writeFailedUpdateOperation>) => {
                  await writeFailedUpdateOperation(...args);
                  throw interrupted;
                },
              }
            : {}),
          ...(phase === "cleanup"
            ? {
                cleanup: async (base, pending) => {
                  await removeQualifiedRecoveryPayload(
                    base,
                    pending.id,
                    pending.dbPath,
                    { transactionId: pending.id, qualified: pending.qualified },
                    "failed-before-trial",
                  );
                  throw interrupted;
                },
              }
            : {}),
          ...(["written", "file-synced", "linked", "directory-synced"].includes(phase)
            ? {
                settle: (base, receipt) =>
                  publishUpdateOperationSettlement(base, receipt, {
                    onPhase: async (current) => {
                      if (current === phase) throw interrupted;
                    },
                  }),
              }
            : {}),
          ...(phase === "native-state"
            ? {
                writeState: async (...args: Parameters<typeof writeServiceState>) => {
                  await writeServiceState(...args);
                  throw interrupted;
                },
              }
            : {}),
        },
      },
    );
  },
);

const reconciliationRequest = (requestId = "22345678-1234-4234-8234-123456789abc") => ({
  type: "reconcile-failed-update" as const,
  operationId: previousId,
  requestId,
});

it("keeps uncertain snapshot staging held before publishing pre-trial settlement", async () => {
  await runScenario(
    "commit",
    async (base, failure) => {
      assert.match(String(failure), /snapshot staging has unknown prior effects/);
      const state = await readServiceState(NodePath.join(base, "runtime", "service-state.json"));
      assert.equal(state.update?.status, "pending");
      assert.equal(state.activeVersion, baseline);
      assert.isUndefined(await readUpdateOperationSettlement(base, previousId));
      assert.equal(
        await NodeFSP.readFile(
          NodePath.join(base, "runtime", "db-backup", `${previousId}.staging`, "partial"),
          "utf8",
        ),
        "retained",
      );
      assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
    },
    undefined,
    {
      beforeLaunch: async (base, qualified) => {
        await seedSettlementFixture(base, qualified, true);
        const staging = NodePath.join(base, "runtime", "db-backup", `${previousId}.staging`);
        await NodeFSP.mkdir(staging, { mode: 0o700 });
        await NodeFSP.writeFile(NodePath.join(staging, "partial"), "retained");
      },
    },
  );
});

it("bounds per-child settlement replies without evicting an earlier request result", async () => {
  const requests = Array.from({ length: 257 }, () =>
    reconciliationRequest(NodeCrypto.randomUUID()),
  );
  requests.push(requests[0]!);
  await runScenario(
    "commit",
    async (base, failure) => {
      assert.match(String(failure), /Active child exited unexpectedly/);
      const { replies } = await readSettlementReplies(base);
      assert.equal(replies.length, 258);
      for (const reply of replies.slice(0, 256)) assert.equal(reply.reason, "not-found");
      assert.equal(replies[256].reason, "settlement-write-failed");
      assert.deepEqual(replies[257], replies[0]);
    },
    undefined,
    { settlementRequests: requests },
  );
});
const readSettlementReplies = async (base: string) =>
  JSON.parse(
    await NodeFSP.readFile(NodePath.join(base, "runtime", "settlement-replies.json"), "utf8"),
  );

it("serializes legacy reconciliation and returns the original reply for a repeated request ID", async () => {
  const first = reconciliationRequest();
  await runScenario(
    "commit",
    async (base, failure) => {
      assert.match(String(failure), /Active child exited unexpectedly/);
      const { context, replies } = await readSettlementReplies(base);
      assert.equal(context.updateSettlementProtocol, 1);
      assert.deepEqual(
        replies.map((reply: { result: string }) => reply.result),
        ["settled", "settled", "already-settled"],
      );
      assert.deepEqual(replies[0], replies[1]);
      assert.equal(
        (await readUpdateOperationSettlement(base, previousId))?.basis,
        "legacy-reconciled",
      );
      assert.isFalse(
        NodeFS.existsSync(NodePath.join(base, "runtime", "db-backup", previousId, "database")),
      );
      assert.equal(
        (await readServiceState(NodePath.join(base, "runtime", "service-state.json"))).update
          ?.status,
        "failed",
      );
    },
    undefined,
    {
      beforeLaunch: async (base, qualified) => {
        await seedSettlementFixture(base, qualified);
      },
      settlementRequests: [
        first,
        first,
        reconciliationRequest("32345678-1234-4234-8234-123456789abc"),
      ],
    },
  );
});

it.each([
  "not-failed",
  "not-found",
  "insufficient-evidence",
  "restore-pending",
  "active-changed",
  "trial-evidence-present",
  "retired-evidence-mismatch",
  "settlement-write-failed",
] as const)("returns %s without claiming a legacy operation was settled", async (reason) => {
  await runScenario(
    "commit",
    async (base, failure) => {
      assert.match(String(failure), /Active child exited unexpectedly/);
      const { replies } = await readSettlementReplies(base);
      assert.deepEqual(replies, [
        {
          type: "update-reconciled",
          operationId: previousId,
          requestId: reconciliationRequest().requestId,
          result: "refused",
          reason,
        },
      ]);
      assert.isUndefined(await readUpdateOperationSettlement(base, previousId));
      assert.equal(readMarker(NodePath.join(base, "userdata", "statev2.sqlite")), "before");
    },
    undefined,
    {
      settlementRequests: [
        reason === "retired-evidence-mismatch"
          ? {
              ...reconciliationRequest(),
              type: "record-retired-operation",
              retiredLocation: "../untrusted",
            }
          : reconciliationRequest(),
      ],
      settlementActiveDrift: reason === "active-changed",
      beforeLaunch: async (base, qualified) => {
        if (reason === "not-found" || reason === "retired-evidence-mismatch") return;
        const state = await seedSettlementFixture(base, qualified);
        const file = NodePath.join(base, "runtime", "service-state.json");
        if (reason === "not-failed")
          await writeServiceState(file, {
            ...state,
            update: { ...state.update!, status: "rolled-back" },
          });
        if (reason === "insufficient-evidence") {
          await archiveUpdateOperation(base, state.update);
          await writeServiceState(file, { protocol: 4, activeVersion: baseline });
        }
        if (reason === "restore-pending" || reason === "trial-evidence-present")
          await NodeFSP.writeFile(
            NodePath.join(
              base,
              "runtime",
              "db-backup",
              previousId,
              reason === "restore-pending" ? ".restore-pending" : "resume-dispatched.json",
            ),
            "",
          );
      },
      ...(reason === "settlement-write-failed"
        ? {
            recoveryIO: {
              settle: async () => {
                throw new Error("synthetic settlement EIO");
              },
            },
          }
        : {}),
    },
  );
});

it("retains a refusal for its request ID after evidence changes, then evaluates a new request", async () => {
  const first = reconciliationRequest();
  await runScenario(
    "commit",
    async (base) => {
      const { replies } = await readSettlementReplies(base);
      assert.deepEqual(replies[0], replies[1]);
      assert.equal(replies[0].reason, "restore-pending");
      assert.equal(replies[2].result, "settled");
    },
    undefined,
    {
      settlementRequests: [
        first,
        first,
        reconciliationRequest("32345678-1234-4234-8234-123456789abc"),
      ],
      clearRestoreAfterReply: true,
      beforeLaunch: async (base, qualified) => {
        await seedSettlementFixture(base, qualified);
        await NodeFSP.writeFile(
          NodePath.join(base, "runtime", "db-backup", previousId, ".restore-pending"),
          "",
        );
      },
    },
  );
});

it("records a retired-operation tombstone from exact retained copies and keeps its ID unavailable", async () => {
  const request = {
    ...reconciliationRequest(),
    type: "record-retired-operation" as const,
    retiredLocation: `retired/${previousId}`,
  };
  await runScenario(
    "commit",
    async (base) => {
      const { replies } = await readSettlementReplies(base);
      assert.deepEqual(
        replies.map((reply: { result: string }) => reply.result),
        ["settled", "settled", "already-settled", "refused"],
      );
      assert.equal(replies[3].reason, "retired-evidence-mismatch");
      assert.equal(
        (await readUpdateOperationSettlement(base, previousId))?.basis,
        "retired-exception",
      );
      const staged = JSON.parse(
        await NodeFSP.readFile(
          NodePath.join(base, "runtime", "retired", previousId, `${previousId}.json`),
          "utf8",
        ),
      );
      await NodeAssert.rejects(reserveUpdateOperation(base, previousId, staged.binding));
    },
    undefined,
    {
      settlementRequests: [
        request,
        request,
        { ...request, requestId: "32345678-1234-4234-8234-123456789abc" },
        {
          ...request,
          requestId: "42345678-1234-4234-8234-123456789abc",
          retiredLocation: "retired/different",
        },
      ],
      beforeLaunch: async (base, qualified) => {
        const state = await seedSettlementFixture(base, qualified);
        await archiveUpdateOperation(base, state.update);
        const retired = NodePath.join(base, "runtime", request.retiredLocation);
        await NodeFSP.mkdir(retired, { recursive: true, mode: 0o700 });
        for (const suffix of [".json", ".outcome.json"])
          await NodeFSP.rename(
            NodePath.join(base, "runtime", "jones-update-operations", previousId + suffix),
            NodePath.join(retired, previousId + suffix),
          );
        await writeServiceState(NodePath.join(base, "runtime", "service-state.json"), {
          protocol: 4,
          activeVersion: baseline,
        });
      },
    },
  );
});

it("does not clean a terminal generation during launcher startup", async () => {
  await runScenario(
    "commit",
    async (base) => {
      assert.equal(
        readMarker(NodePath.join(base, "runtime", "db-backup", previousId, "database")),
        "before",
      );
    },
    undefined,
    { previous: "committed", recoveryOnly: true },
  );
});
