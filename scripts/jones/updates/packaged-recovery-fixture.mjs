import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as NodeSqlite from "node:sqlite";
import {
  currentQualifiedRuntimeBinding,
  decodeQualifiedRuntimeReceipt,
  enrollQualifiedRuntime,
  qualifiedPayloadDigest,
  stageQualifiedRuntime,
} from "../../../apps/server/src/jones/cloud/qualifiedRuntime.ts";
import {
  operationBinding,
  reserveUpdateOperation,
  writeFailedUpdateOperation,
} from "../../../apps/server/src/jones/updates/launcherOperation.ts";

const refuse = (message) => {
  throw new Error("Packaged recovery fixture refused: " + message);
};
const hash = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");

async function physicalPrivateRoot(root) {
  if (
    !NodePath.isAbsolute(root) ||
    (await NodeFSP.realpath(root)) !== root ||
    !NodePath.basename(root).startsWith("jones-packaged-recovery-")
  )
    refuse("dedicated physical root required");
  const stat = await NodeFSP.lstat(root);
  if (!stat.isDirectory() || stat.uid !== process.getuid() || (stat.mode & 0o077) !== 0)
    refuse("private owned root required");
  if ((await NodeFSP.readdir(root)).length !== 0) refuse("fixture root is occupied");
  return { dev: stat.dev, ino: stat.ino };
}

async function boundPayload(input, root) {
  if (
    input === null ||
    typeof input !== "object" ||
    Object.keys(input).sort().join(",") !== "payloadDirectory,receipt"
  )
    refuse("payload input differs");
  const receipt = decodeQualifiedRuntimeReceipt(input.receipt);
  if (
    receipt === undefined ||
    receipt.platform !== "linux" ||
    // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone fixture binds its actual host without loading application services.
    receipt.architecture !== process.arch
  )
    refuse("payload receipt does not match Linux host");
  const directory = await NodeFSP.realpath(input.payloadDirectory);
  if (
    !NodePath.isAbsolute(input.payloadDirectory) ||
    directory !== input.payloadDirectory ||
    directory === root ||
    directory.startsWith(root + NodePath.sep) ||
    root.startsWith(directory + NodePath.sep)
  )
    refuse("payload must be a separate physical input");
  const digest = await qualifiedPayloadDigest(directory, receipt.platform, receipt.workflow, {
    runtimeVersion: receipt.version,
    eventLoop: "worker",
  });
  if (digest !== receipt.payloadSha256) refuse("payload digest differs");
  const executable = NodePath.join(directory, "t3");
  const stat = await NodeFSP.lstat(executable, { bigint: true });
  if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o111n) === 0n)
    refuse("payload executable is not a regular executable");
  return {
    directory,
    receipt,
    executable,
    executableSha256: hash(await NodeFSP.readFile(executable)),
    identity: Object.fromEntries(
      ["dev", "ino", "size", "mtimeNs", "ctimeNs"].map((key) => [key, String(stat[key])]),
    ),
  };
}

async function privateJson(filename, value) {
  const handle = await NodeFSP.open(filename, "wx", 0o600);
  try {
    await handle.writeFile(JSON.stringify(value) + "\n");
    await handle.sync();
  } finally {
    await handle.close();
  }
  const parent = await NodeFSP.open(NodePath.dirname(filename), "r");
  try {
    await parent.sync();
  } finally {
    await parent.close();
  }
}

function artifact(input) {
  const { protocol: _protocol, payloadSha256: _payloadSha256, ...metadata } = input.receipt;
  return { ...metadata, payloadDirectory: input.directory };
}

export async function seedPackagedRecoveryFixture({
  root,
  active,
  candidate,
  operationId,
  kind = "pending",
  recoveryPayload = true,
  archivedOutcome = false,
}) {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone fixture binds its actual host without loading application services.
  if (process.platform !== "linux") refuse("Linux fixture required");
  if (!/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(operationId))
    refuse("operation ID must be a fixed UUID v4");
  if (
    !["pending", "legacy-failed"].includes(kind) ||
    typeof recoveryPayload !== "boolean" ||
    typeof archivedOutcome !== "boolean" ||
    (kind === "pending" && archivedOutcome)
  )
    refuse("unsupported recovery fixture");
  const allocation = await physicalPrivateRoot(root);
  const previous = await boundPayload(active, root);
  const next = await boundPayload(candidate, root);
  if (
    previous.receipt.version === next.receipt.version ||
    next.receipt.installedSourceSha !== previous.receipt.sourceSha
  )
    refuse("active and candidate source/version binding differs");

  const fixtureId = NodeCrypto.randomUUID().replaceAll("-", "");
  const baseDir = NodePath.join(root, "home");
  await privateJson(NodePath.join(root, "fixture-owner.json"), {
    protocol: 1,
    purpose: "jones-packaged-recovery",
    fixtureId,
    executable: next.executable,
    identity: next.identity,
  });
  await NodeFSP.mkdir(NodePath.join(baseDir, "userdata"), { recursive: true, mode: 0o700 });
  const dbPath = NodePath.join(baseDir, "userdata", "statev2.sqlite");
  const database = new NodeSqlite.DatabaseSync(dbPath);
  try {
    database.exec(
      "CREATE TABLE fixture_marker(value TEXT); INSERT INTO fixture_marker VALUES ('before');",
    );
  } finally {
    database.close();
  }
  await NodeFSP.chmod(dbPath, 0o600);
  const environmentId = "packaged-recovery-" + fixtureId;
  await NodeFSP.writeFile(NodePath.join(baseDir, "userdata", "environment-id"), environmentId, {
    flag: "wx",
    mode: 0o600,
  });

  // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone fixture binds its actual host without loading application services.
  const host = { platform: "linux", architecture: process.arch };
  await enrollQualifiedRuntime({
    baseDir,
    host,
    artifact: artifact(previous),
    validate: async () => {},
  });
  const staged = await stageQualifiedRuntime({
    stagedHandle: NodeCrypto.randomUUID(),
    host,
    artifact: artifact(next),
    binding: await currentQualifiedRuntimeBinding(baseDir, previous.receipt.version, host),
    validate: async () => {},
    options: { eventLoop: "worker" },
  });
  if (staged.receipt.payloadSha256 !== next.receipt.payloadSha256) refuse("staged payload changed");
  await reserveUpdateOperation(baseDir, operationId, operationBinding(staged));
  const pending = {
    id: operationId,
    fromVersion: previous.receipt.version,
    targetVersion: next.receipt.version,
    qualified: staged,
    status: "pending",
    phase: "accepted",
    dbPath,
  };
  if (recoveryPayload) {
    const backup = NodePath.join(baseDir, "runtime", "db-backup", operationId);
    await NodeFSP.mkdir(backup, { recursive: true, mode: 0o700 });
    await NodeFSP.copyFile(dbPath, NodePath.join(backup, "database"));
    await NodeFSP.chmod(NodePath.join(backup, "database"), 0o600);
    await privateJson(NodePath.join(backup, "paired-binding.json"), {
      transactionId: operationId,
      qualified: staged,
    });
  }
  if (archivedOutcome) await writeFailedUpdateOperation(baseDir, pending);
  const update =
    kind === "pending"
      ? pending
      : {
          id: operationId,
          fromVersion: pending.fromVersion,
          targetVersion: pending.targetVersion,
          qualified: staged,
          status: "failed",
          reason: "launcher-restarted",
        };
  const state = { protocol: 4, activeVersion: previous.receipt.version, update };
  await privateJson(NodePath.join(baseDir, "runtime", "service-state.json"), state);
  const prepared = {
    schema: "jones.packaged-recovery-fixture/v1",
    purpose: "synthetic-recovery-inputs",
    root,
    allocation,
    fixtureId,
    baseDir,
    operationId,
    kind,
    recoveryPayload,
    archivedOutcome,
    launcher: {
      executable: next.executable,
      sha256: next.executableSha256,
      identity: next.identity,
    },
    inputs: {
      active: {
        payloadDirectory: previous.directory,
        receipt: previous.receipt,
        executableSha256: previous.executableSha256,
      },
      candidate: {
        payloadDirectory: next.directory,
        receipt: next.receipt,
        executableSha256: next.executableSha256,
      },
    },
    staged,
    state,
    evidence: {
      artifactExecuted: false,
      packagedAcceptance: false,
      stagingMeasuredInPackagedWorker: false,
    },
  };
  await privateJson(NodePath.join(root, "prepared-fixture.json"), prepared);
  return prepared;
}
