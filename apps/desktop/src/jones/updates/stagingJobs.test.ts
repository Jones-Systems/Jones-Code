// @effect-diagnostics nodeBuiltinImport:off - Synthetic staging roots under the user's cache.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  JONES_ACTIONS_RECEIPT_FILE,
  createJonesStageAttempt,
  publishJonesCompletedStage,
  type JonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { JonesBackgroundPreparation } from "./backgroundPreparation.ts";
import { stageJonesMacApp } from "./jonesMacStaging.ts";
import { reclaimJonesStagePayload } from "./stagingJobs.ts";

const faults = vi.hoisted(() => ({
  operation: "",
  path: "",
  gate: "",
  after: false,
  partial: false,
  foreignDevice: "",
  referenceDirectory: "",
  referenceName: "",
  referenceOrder: "",
  events: [] as string[],
}));

vi.mock("node:child_process", async (original) => ({
  ...(await original<typeof import("node:child_process")>()),
  execFile: vi.fn(),
}));
vi.mock("@t3tools/shared/jones/jonesActions", async (original) => ({
  ...(await original<typeof import("@t3tools/shared/jones/jonesActions")>()),
  // This suite proves cleanup and the next Mac attempt gate, not artifact qualification.
  validateJonesStagedArtifact: vi.fn(async () => undefined),
}));

vi.mock("node:fs/promises", async (original) => {
  const actual = await original<typeof import("node:fs/promises")>();
  const inject = async <T>(
    operation: string,
    path: unknown,
    action: () => Promise<T>,
  ): Promise<T> => {
    const name = String(path);
    faults.events.push(`${operation}:${name}`);
    const fail =
      faults.operation === operation &&
      faults.path === name &&
      (!faults.gate || faults.events.includes(faults.gate));
    if (fail && !faults.after) throw Object.assign(new Error("Injected EIO"), { code: "EIO" });
    const value = await action();
    if (fail) throw Object.assign(new Error("Injected EIO"), { code: "EIO" });
    return value;
  };
  return {
    ...actual,
    readdir: async (...args: Parameters<typeof actual.readdir>) => {
      const entries = await actual.readdir(...args);
      if (String(args[0]) === faults.referenceDirectory) {
        const rank = (entry: unknown) => {
          const name = String(
            typeof entry === "object" && entry !== null && "name" in entry ? entry.name : entry,
          );
          return name === faults.referenceName ? (faults.referenceOrder === "first" ? -1 : 1) : 0;
        };
        entries.sort((a, b) => rank(a) - rank(b));
      }
      return entries;
    },
    link: (...args: Parameters<typeof actual.link>) =>
      inject("link", args[1], () => actual.link(...args)),
    unlink: (...args: Parameters<typeof actual.unlink>) =>
      inject("unlink", args[0], () => actual.unlink(...args)),
    rmdir: (...args: Parameters<typeof actual.rmdir>) =>
      inject("rmdir", args[0], () => actual.rmdir(...args)),
    open: async (...args: Parameters<typeof actual.open>) => {
      const file = await actual.open(...args);
      const sync = file.sync.bind(file);
      file.sync = () => inject("sync", args[0], sync);
      return file;
    },
  };
});
vi.mock("node:fs", async (original) => {
  const actual = await original<typeof import("node:fs")>();
  const NodePath = await import("node:path");
  return {
    ...actual,
    promises: {
      ...actual.promises,
      lstat: async (...args: Parameters<typeof actual.promises.lstat>) => {
        const info = await actual.promises.lstat(...args);
        if (String(args[0]) === faults.foreignDevice)
          return Object.assign(info, { dev: Number(info.dev) + 1 });
        return info;
      },
      rm: async (...args: Parameters<typeof actual.promises.rm>) => {
        faults.events.push(`rm:${String(args[0])}`);
        if (faults.operation === "rm" && String(args[0]) === faults.path) {
          if (faults.partial) {
            // A pre-fix recursive attempt removal could already have lost completion.json.
            await actual.promises.unlink(
              NodePath.join(NodePath.dirname(String(args[0])), "completion.json"),
            );
            const names = await actual.promises.readdir(args[0]);
            if (names[0])
              await actual.promises.rm(NodePath.join(String(args[0]), names[0]), {
                recursive: true,
              });
          }
          if (faults.after) await actual.promises.rm(...args);
          throw Object.assign(new Error("Injected EIO"), { code: "EIO" });
        }
        return actual.promises.rm(...args);
      },
    },
  };
});

const resetFaults = () => {
  faults.operation = "";
  faults.path = "";
  faults.gate = "";
  faults.after = false;
  faults.partial = false;
  faults.foreignDevice = "";
  faults.referenceDirectory = "";
  faults.referenceName = "";
  faults.referenceOrder = "";
  faults.events.length = 0;
};

const replaced = "a".repeat(64);
const current = "b".repeat(64);

const stageReference = (handle: string) => ({
  schema: 1,
  source: "jones-actions",
  home: "/synthetic-home",
  profile: null,
  currentVersion: "0.1.0",
  installedSource: "c".repeat(40),
  active: null,
  artifactDirectory: "/synthetic-artifact",
  app: {
    handle,
    receiptPath: "/synthetic-receipt",
    appPath: "/synthetic-app",
    executablePath: "/synthetic-executable",
    version: "0.1.1",
    sourceSha: "d".repeat(40),
    sourceTree: "e".repeat(40),
    appDigest: "f".repeat(64),
    asarDigest: "f".repeat(64),
    executableDigest: "f".repeat(64),
  },
});

const fixtures = new Set<string>();
afterEach(async () => {
  resetFaults();
  vi.mocked(NodeChildProcess.execFile).mockReset();
  for (const root of fixtures) {
    await NodeFSP.rm(root, { recursive: true, force: true });
    await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
    fixtures.delete(root);
  }
});

async function updatesRoot(nativeLayout = false) {
  const parent = NodePath.join(NodeOS.homedir(), ".cache", "jones-updater-test-fixtures");
  await NodeFSP.mkdir(parent, { recursive: true, mode: 0o700 });
  const home = await NodeFSP.mkdtemp(NodePath.join(parent, "staging-jobs-"));
  fixtures.add(home);
  const root = NodePath.join(home, ...(nativeLayout ? ["runtime"] : []), "jones-updates");
  await NodeFSP.mkdir(NodePath.join(root, "artifacts"), { recursive: true });
  await NodeFSP.mkdir(NodePath.join(root, "apps"), { recursive: true });
  return root;
}

/** A completed attempt in each root, the way the client and app staging publish them. */
async function completedStage(root: string, handle: string) {
  const artifact = await createJonesStageAttempt(NodePath.join(root, "artifacts"), handle);
  await NodeFSP.writeFile(NodePath.join(artifact.directory, "qualified.dmg"), "dmg-bytes");
  await NodeFSP.writeFile(NodePath.join(artifact.directory, JONES_ACTIONS_RECEIPT_FILE), "{}");
  await publishJonesCompletedStage(
    NodePath.join(root, "artifacts"),
    handle,
    artifact.directory,
    JONES_ACTIONS_RECEIPT_FILE,
  );
  const app = await createJonesStageAttempt(NodePath.join(root, "apps"), handle);
  const contents = NodePath.join(app.directory, "Candidate.app", "Contents", "Resources");
  await NodeFSP.mkdir(contents, { recursive: true });
  await NodeFSP.writeFile(NodePath.join(contents, "app.asar"), "asar-bytes");
  await NodeFSP.symlink("Resources", NodePath.join(contents, "..", "Link"));
  await NodeFSP.writeFile(NodePath.join(app.directory, "mac-app-receipt.json"), "{}");
  await publishJonesCompletedStage(
    NodePath.join(root, "apps"),
    handle,
    app.directory,
    "mac-app-receipt.json",
  );
  return { artifactAttempt: artifact.attemptRoot, appAttempt: app.attemptRoot };
}

const exists = (path: string) =>
  NodeFSP.lstat(path).then(
    () => true,
    () => false,
  );

const nativeTransaction = "1".repeat(64);
const retiredPreparation = "2".repeat(64);
const withdrawnPreparation = "3".repeat(64);
const referenceBytes = (value: unknown) => JSON.stringify(value) + "\n";
const referenceDigest = (value: unknown) =>
  NodeCrypto.createHash("sha256").update(referenceBytes(value)).digest("hex");

function sortedRecord(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortedRecord);
  if (value !== null && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value)
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([key, item]) => [key, sortedRecord(item)]),
    );
  return value;
}

// Native helper preparation_claim/activate/preparation_command and server trialGate receipts.
// No native process runs: only production reclaim consumes these retained records.
function retainedReferences(root: string, phase: "resumed" | "rolled-back" = "resumed") {
  const home = NodePath.dirname(NodePath.dirname(root));
  const expected = {
    protocol: 1,
    owner: "desktop",
    generation: "bootstrap",
    transactionId: "bootstrap",
    home,
    databasePath: NodePath.join(home, "userdata", "statev2.sqlite"),
    profile: NodePath.join(home, "profile"),
    environmentId: "synthetic-environment",
    appPath: "/synthetic/Jones Code.app",
    executablePath: "/synthetic/Jones Code.app/Contents/MacOS/Jones Code",
    version: "0.1.0",
    sourceSha: "c".repeat(40),
    sourceTree: "d".repeat(40),
  };
  const staged = { ...stageReference(current).app, startupGateProtocol: 1 };
  const selectionPath = NodePath.join(root, "staging", `${"4".repeat(64)}.json`);
  const claim = (transactionId: string) => ({
    protocol: 1,
    preparationClaimProtocol: 3,
    transactionId,
    stagedHandle: current,
    selectionPath,
    selectionSha256: "5".repeat(64),
    home,
    databasePath: expected.databasePath,
    profile: expected.profile,
    environmentId: expected.environmentId,
  });
  const intent = (transactionId: string) => ({
    protocol: 1,
    transactionId,
    expected,
    staged,
    continuationReceipt: NodePath.join(root, "transactions", transactionId, "continuation.json"),
    processes: [
      { pid: 101, role: "main" },
      { pid: 102, role: "backend" },
    ],
    listener: "http://127.0.0.1:7777",
  });
  const attempt = (transactionId: string) => ({
    protocol: 1,
    transactionId,
    stagedHandle: current,
    selectionPath,
    selectionSha256: claim(transactionId).selectionSha256,
    expected,
  });
  const request = (transactionId: string) => ({
    protocol: 1,
    transactionId,
    stagedHandle: current,
    claimSha256: referenceDigest(claim(transactionId)),
    processes: [
      { pid: 101, startSec: 123, startUsec: 456, executable: expected.executablePath },
      { pid: 102, startSec: 124, startUsec: 456, executable: "/synthetic/server" },
    ],
  });
  const tx = `transactions/${nativeTransaction}`;
  const trial = {
    protocol: 1,
    startupGateProtocol: 1,
    transactionId: nativeTransaction,
    stagedHandle: current,
    home,
    databasePath: expected.databasePath,
    profile: expected.profile,
    environmentId: expected.environmentId,
    version: staged.version,
    sourceSha: staged.sourceSha,
    sourceTree: staged.sourceTree,
    listener: intent(nativeTransaction).listener,
  };
  const receipt = {
    ...trial,
    resumeHeld: true,
    backendProcess: { pid: 103, identity: "synthetic process birth" },
  };
  const records: Record<string, unknown> = {
    [`${tx}/prepare-intent.json`]: claim(nativeTransaction),
    [`${tx}/prepare-request.json`]: request(nativeTransaction),
    [`${tx}/prepare-dispatched.json`]: claim(nativeTransaction),
    [`${tx}/continuation.json`]: { ...claim(nativeTransaction), prepared: true },
    [`${tx}/activation-request.json`]: intent(nativeTransaction),
    [`${tx}/intent.json`]: intent(nativeTransaction),
    [`${tx}/journal.json`]: {
      intent: intent(nativeTransaction),
      phase,
      incomingBundle: `/synthetic/Jones Code.app.jones-incoming-${nativeTransaction}`,
      previousBundle: "/synthetic/Jones Code.app.jones-previous-bootstrap",
      pairedState: NodePath.join(root, tx, "previous-pair"),
      recovery: { method: "copy", bytes: 64, startedAt: 1, completedAt: 2 },
      storage: {
        stateBytes: 64,
        candidateBytes: 128,
        requiredBytes: 268435648,
        headroomBytesPerVolume: 268435456,
        volumes: [
          {
            path: NodePath.join(root, tx),
            device: "1",
            requiredBytes: 268435648,
            availableBytes: 536870912,
          },
        ],
      },
      recoveryReserves: [
        {
          path: NodePath.join(root, tx, "recovery-journal-reserve.bin"),
          bytes: 4194304,
          device: "1",
          inode: "2",
          purpose: "journal",
        },
        {
          path: NodePath.join(root, tx, "recovery-reserve.bin"),
          bytes: 264241152,
          device: "1",
          inode: "3",
          purpose: "recovery",
        },
      ],
      candidateWriter: {
        pid: 104,
        startSec: 125,
        startUsec: 456,
        executable: expected.executablePath,
      },
      backendWriter: { pid: 103, startSec: 126, startUsec: 456, executable: "/synthetic/server" },
      trialReceipt: receipt,
    },
    [`${tx}/trial-descriptor.json`]: {
      ...trial,
      trialReceiptPath: NodePath.join(root, tx, "trial-receipt.json"),
      commitGrantPath: NodePath.join(root, tx, "commit-grant.json"),
    },
    [`${tx}/trial-receipt.json`]: receipt,
  };
  if (phase === "resumed") {
    records[`${tx}/commit-grant.json`] = { ...trial, generation: nativeTransaction };
    records[`${tx}/resume-dispatched.json`] = receipt;
  } else {
    records[`${tx}/advanced-state-authority-guard.json`] = {
      protocol: 1,
      guard: { kind: "absent" },
    };
  }
  const olderTransaction = "0".repeat(64);
  const older = `transactions/${olderTransaction}`;
  const olderJournal = { intent: intent(olderTransaction), phase: "rolled-back" };
  const retirementPlan = {
    protocol: 1,
    transactionId: olderTransaction,
    activeBinding: expected,
    journalSha256: referenceDigest(olderJournal),
    targets: [],
  };
  const retirementSha = NodeCrypto.createHash("sha256")
    .update(JSON.stringify(sortedRecord(retirementPlan)))
    .digest("hex");
  records[`${older}/intent.json`] = intent(olderTransaction);
  records[`${older}/journal.json`] = olderJournal;
  records[`${older}/retirement-intent.json`] = {
    protocol: 1,
    planSha256: retirementSha,
    plan: retirementPlan,
  };
  records[`${older}/retirement-receipt.json`] = {
    protocol: 1,
    transactionId: olderTransaction,
    planSha256: retirementSha,
    status: "retired",
  };
  const attemptKey = NodeCrypto.createHash("sha256")
    .update(`${selectionPath}\n${claim(nativeTransaction).selectionSha256}`)
    .digest("hex");
  records[`attempt-selections/${attemptKey}.json`] = attempt(nativeTransaction);
  for (const transactionId of [retiredPreparation, withdrawnPreparation]) {
    const retired = `retired-preparations/${transactionId}`;
    const evidence = transactionId === retiredPreparation ? "backend-refused" : "withdrawn";
    const preparation: Record<string, unknown> = {
      "prepare-intent.json": claim(transactionId),
      "prepare-request.json": request(transactionId),
      "prepare-failure.json": { protocol: 1, reason: "Synthetic preparation refusal" },
    };
    if (evidence === "backend-refused")
      preparation["prepare-refused.json"] = {
        protocol: 1,
        transactionId,
        stagedHandle: current,
        claimSha256: referenceDigest(claim(transactionId)),
        reason: "Synthetic refusal",
      };
    else
      Object.assign(preparation, {
        "prepare-dispatched.json": claim(transactionId),
        "continuation.json": { ...claim(transactionId), prepared: true },
        "activation-request.json": intent(transactionId),
        "preparation-withdrawal.json": {
          protocol: 1,
          withdrawalProtocol: 1,
          transactionId,
          claimSha256: referenceDigest(claim(transactionId)),
          claim: claim(transactionId),
          expected,
          evidence: "withdrawal-protocol",
        },
      });
    const plan = {
      protocol: 1,
      transactionId,
      activeBinding: expected,
      records: Object.fromEntries(
        Object.entries(preparation).map(([name, value]) => [name, referenceDigest(value)]),
      ),
      attemptSelection: NodePath.join(root, "attempt-selections", `${attemptKey}.json`),
      attemptSha256: referenceDigest(attempt(transactionId)),
      retiredPath: NodePath.join(root, retired),
    };
    // retirement_digest uses sorted compact JSON; these synthetic fields are all ASCII.
    const planSha256 = NodeCrypto.createHash("sha256")
      .update(JSON.stringify(sortedRecord(plan)))
      .digest("hex");
    Object.assign(preparation, {
      "attempt-selection.json": attempt(transactionId),
      "preparation-retirement.json": { protocol: 1, planSha256, plan, evidence },
      "preparation-retirement-receipt.json": {
        protocol: 1,
        transactionId,
        planSha256,
        status: "retired",
        evidence,
      },
    });
    for (const [name, value] of Object.entries(preparation)) records[`${retired}/${name}`] = value;
  }
  // stagedSelection.selectionPath hashes the full binding in this order.
  const binding = {
    baseDir: home,
    dbPath: expected.databasePath,
    environmentId: expected.environmentId,
    activeVersion: "0.1.0-preview.20261026.1",
    activeSourceSha: expected.sourceSha,
  };
  const serverKey = NodeCrypto.createHash("sha256").update(JSON.stringify(binding)).digest("hex");
  records[`selections/${serverKey}.json`] = {
    protocol: 1,
    stagedHandle: "12345678-1234-4234-8234-123456789012",
    binding,
    receipt: {
      protocol: 1,
      repository: "Jones-Systems/Jones-Code",
      channel: "jones-main",
      platform: "darwin",
      architecture: "arm64",
      workflow: ".github/workflows/artifact-desktop-mac.yml",
      version: "0.1.1-preview.20261026.2",
      sourceSha: staged.sourceSha,
      sourceTree: staged.sourceTree,
      installedSourceSha: expected.sourceSha,
      archiveSha256: "d".repeat(64),
      payloadSha256: "e".repeat(64),
      artifactDigest: `sha256:${"f".repeat(64)}`,
      runId: 1,
      runAttempt: 1,
      artifactId: 1,
    },
  };
  return records;
}

// Historical preparation writers b5e78ed / 1aeec841 and their missing-handle trial family.
// expected describes the previous generation; only the new intent/stage/directory share T.
function legacyCommittedReferences(root: string, variant: "unversioned" | "claim1" | "none") {
  const home = NodePath.dirname(NodePath.dirname(root));
  const tx = `transactions/${current}`;
  const binding = {
    protocol: 1,
    transactionId: current,
    home,
    databasePath: NodePath.join(home, "userdata", "statev2.sqlite"),
    profile: NodePath.join(home, "profile"),
    environmentId: "synthetic-environment",
  };
  const expected = {
    ...binding,
    owner: "desktop",
    generation: "bootstrap",
    transactionId: "bootstrap",
    appPath: "/synthetic/Jones Code.app",
    executablePath: "/synthetic/Jones Code.app/Contents/MacOS/Jones Code",
    version: "0.1.0",
    sourceSha: "c".repeat(40),
    sourceTree: "d".repeat(40),
  };
  const staged = { ...stageReference(current).app, startupGateProtocol: 1 };
  const intent = {
    protocol: 1,
    transactionId: current,
    expected,
    staged,
    continuationReceipt: NodePath.join(root, tx, "continuation.json"),
    processes: [
      { pid: 101, identity: "synthetic main birth" },
      { pid: 102, identity: "synthetic backend birth" },
    ],
    listener: "http://127.0.0.1:7777",
  };
  const trial = {
    ...binding,
    startupGateProtocol: 1,
    version: staged.version,
    sourceSha: staged.sourceSha,
    sourceTree: staged.sourceTree,
    listener: intent.listener,
  };
  const receipt = {
    ...trial,
    resumeHeld: true,
    backendProcess: { pid: 103, identity: "synthetic trial birth" },
  };
  const records: Record<string, unknown> = {
    [`${tx}/intent.json`]: intent,
    [`${tx}/journal.json`]: {
      intent,
      phase: "resumed",
      incomingBundle: `/synthetic/Jones Code.app.jones-incoming-${current}`,
      previousBundle: "/synthetic/Jones Code.app.jones-previous-bootstrap",
      pairedState: NodePath.join(root, tx, "previous-pair"),
      candidateWriter: { pid: 101, identity: "synthetic main birth" },
      backendWriter: receipt.backendProcess,
      trialReceipt: receipt,
    },
    [`${tx}/trial-descriptor.json`]: {
      ...trial,
      trialReceiptPath: NodePath.join(root, tx, "trial-receipt.json"),
      commitGrantPath: NodePath.join(root, tx, "commit-grant.json"),
    },
    [`${tx}/commit-grant.json`]: { ...trial, generation: current },
    [`${tx}/trial-receipt.json`]: receipt,
    [`${tx}/resume-dispatched.json`]: receipt,
  };
  if (variant === "unversioned") {
    records[`${tx}/prepare-intent.json`] = { ...binding, prepared: true };
    records[`${tx}/continuation.json`] = { ...binding, prepared: true };
  } else if (variant === "claim1") {
    const claim = {
      ...binding,
      preparationClaimProtocol: 1,
      selectionPath: NodePath.join(root, "staging", `${"4".repeat(64)}.json`),
      selectionSha256: "5".repeat(64),
    };
    records[`${tx}/prepare-intent.json`] = claim;
    records[`${tx}/prepare-dispatched.json`] = claim;
    records[`${tx}/continuation.json`] = { ...claim, prepared: true };
  }
  return records;
}

async function writeReferences(root: string, records: Record<string, unknown>) {
  for (const [path, value] of Object.entries(records)) {
    const file = NodePath.join(root, path);
    await NodeFSP.mkdir(NodePath.dirname(file), { recursive: true });
    await NodeFSP.writeFile(file, referenceBytes(value), { mode: 0o600 });
  }
}

async function referenceSnapshot(root: string, records: Record<string, unknown>) {
  return Object.fromEntries(
    await Promise.all(
      Object.keys(records).map(async (path) => [
        path,
        await NodeFSP.readFile(NodePath.join(root, path), "utf8"),
      ]),
    ),
  ) as Record<string, string>;
}

async function expectHistoryUnchanged(root: string, bytes: Record<string, string>) {
  for (const [path, raw] of Object.entries(bytes))
    expect(await NodeFSP.readFile(NodePath.join(root, path), "utf8"), path).toBe(raw);
}

async function expectStageState(
  root: string,
  handle: string,
  stage: Awaited<ReturnType<typeof completedStage>>,
  present: boolean,
) {
  for (const path of [stage.artifactAttempt, stage.appAttempt])
    expect(await exists(path), path).toBe(present);
  for (const kind of ["artifacts", "apps"]) {
    expect(await exists(NodePath.join(root, kind, "completed", `${handle}.json`))).toBe(present);
    expect(await exists(NodePath.join(root, "stage-cleanup", `${kind}-${handle}.json`))).toBe(
      false,
    );
  }
}

describe("reclaimJonesStagePayload", () => {
  it.each([
    ["LEGACY-UNVERSIONED", "unversioned"],
    ["LEGACY-CLAIM1", "claim1"],
    ["LEGACY-NO-PREP", "none"],
  ] as const)("%s reclaims unrelated C with genuine committed history", async (_name, variant) => {
    const root = await updatesRoot(true);
    const old = await completedStage(root, replaced);
    const kept = await completedStage(root, current);
    const records = legacyCommittedReferences(root, variant);
    await writeReferences(root, records);
    const bytes = await referenceSnapshot(root, records);

    expect(await reclaimJonesStagePayload(root, replaced)).toMatchObject({ status: "reclaimed" });
    await expectStageState(root, replaced, old, false);
    await expectStageState(root, current, kept, true);
    await expectHistoryUnchanged(root, bytes);
    expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "absent" });
    await expectHistoryUnchanged(root, bytes);
  });

  it("legacy descriptor permits matching explicit companion handles and absent optional records", async () => {
    const root = await updatesRoot(true);
    const old = await completedStage(root, replaced);
    const kept = await completedStage(root, current);
    const records = legacyCommittedReferences(root, "none");
    const tx = `transactions/${current}`;
    records[`${tx}/trial-receipt.json`] = {
      ...(records[`${tx}/trial-receipt.json`] as object),
      stagedHandle: current,
    };
    delete records[`${tx}/intent.json`];
    delete records[`${tx}/commit-grant.json`];
    delete records[`${tx}/resume-dispatched.json`];
    await writeReferences(root, records);
    const bytes = await referenceSnapshot(root, records);
    expect(await reclaimJonesStagePayload(root, replaced)).toMatchObject({ status: "reclaimed" });
    await expectStageState(root, replaced, old, false);
    await expectStageState(root, current, kept, true);
    await expectHistoryUnchanged(root, bytes);
    expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "absent" });
    await expectHistoryUnchanged(root, bytes);
  });

  it.each([
    "partial-current-2",
    "partial-current-3",
    "absent-with-reserved-fields",
    "claim1-missing-selection-hash",
    "claim1-with-handle",
    "unversioned-with-selection",
    "unversioned-dispatched",
    "null-discriminator",
    "string-discriminator",
    "future-discriminator",
  ])("legacy discriminator guard holds %s without changing history", async (condition) => {
    const root = await updatesRoot(true);
    const old = await completedStage(root, replaced);
    const kept = await completedStage(root, current);
    const records = legacyCommittedReferences(root, "claim1");
    const tx = `transactions/${current}`;
    const claim = { ...(records[`${tx}/prepare-intent.json`] as Record<string, unknown>) };
    if (condition.startsWith("partial-current") || condition === "absent-with-reserved-fields") {
      claim.preparationClaimProtocol = condition === "partial-current-2" ? 2 : 3;
      claim.stagedHandle = current;
      if (condition === "partial-current-2") delete claim.stagedHandle;
      if (condition === "partial-current-3") delete claim.selectionPath;
      if (condition === "absent-with-reserved-fields") delete claim.preparationClaimProtocol;
    }
    if (condition === "claim1-missing-selection-hash") delete claim.selectionSha256;
    if (condition === "claim1-with-handle") claim.stagedHandle = current;
    if (condition.startsWith("unversioned")) {
      delete claim.preparationClaimProtocol;
      delete claim.selectionSha256;
      claim.prepared = true;
      if (condition === "unversioned-dispatched") delete claim.selectionPath;
    }
    if (condition === "null-discriminator") claim.preparationClaimProtocol = null;
    if (condition === "string-discriminator") claim.preparationClaimProtocol = "1";
    if (condition === "future-discriminator") claim.preparationClaimProtocol = 4;
    const name =
      condition === "unversioned-dispatched" ? "prepare-dispatched.json" : "prepare-intent.json";
    records[`${tx}/${name}`] = claim;
    await writeReferences(root, records);
    const bytes = await referenceSnapshot(root, records);
    await expect(reclaimJonesStagePayload(root, replaced)).rejects.toThrow();
    await expectStageState(root, replaced, old, true);
    await expectStageState(root, current, kept, true);
    await expectHistoryUnchanged(root, bytes);
  });

  it.each([
    ["modern-2", "first"],
    ["modern-2", "last"],
    ["modern-3", "first"],
    ["modern-3", "last"],
    ["other-handle", "first"],
    ["other-handle", "last"],
    ["null-handle", "last"],
    ["malformed-handle", "last"],
    ["explicit-descriptor", "last"],
  ])("legacy implicit-handle guard holds %s encountered %s", async (condition, order) => {
    const root = await updatesRoot(true);
    const old = await completedStage(root, replaced);
    const kept = await completedStage(root, current);
    const records = legacyCommittedReferences(root, "none");
    const tx = `transactions/${current}`;
    let name = "trial-receipt.json";
    if (condition.startsWith("modern")) {
      name = "prepare-intent.json";
      records[`${tx}/${name}`] = {
        ...(legacyCommittedReferences(root, "claim1")[`${tx}/${name}`] as object),
        preparationClaimProtocol: condition === "modern-2" ? 2 : 3,
        stagedHandle: current,
      };
    } else {
      if (condition === "explicit-descriptor") name = "trial-descriptor.json";
      records[`${tx}/${name}`] = {
        ...(records[`${tx}/${name}`] as object),
        stagedHandle:
          condition === "null-handle"
            ? null
            : condition === "malformed-handle"
              ? "invalid"
              : condition === "other-handle"
                ? "e".repeat(64)
                : current,
      };
    }
    await writeReferences(root, records);
    const bytes = await referenceSnapshot(root, records);
    faults.referenceDirectory = NodePath.join(root, tx);
    faults.referenceName = name;
    faults.referenceOrder = order;
    await expect(reclaimJonesStagePayload(root, replaced)).rejects.toThrow();
    await expectStageState(root, replaced, old, true);
    await expectStageState(root, current, kept, true);
    await expectHistoryUnchanged(root, bytes);
  });

  it.each([
    "missing-journal",
    "missing-descriptor",
    "resume-intent",
    "blocked",
    "rolled-back",
    "different-staged-handle",
    "different-transaction",
    "home-mismatch",
    "database-mismatch",
    "noncanonical-profile",
    "receipt-path-mismatch",
    "continuation-path-mismatch",
    "build-mismatch",
    "listener-mismatch",
    "grant-generation-mismatch",
    "claim-selection-mismatch",
    "mixed-legacy-claims",
    "retired-location",
  ])("legacy committed-context guard holds %s without changing history", async (condition) => {
    const root = await updatesRoot(true);
    const old = await completedStage(root, replaced);
    const kept = await completedStage(root, current);
    let records = structuredClone(legacyCommittedReferences(root, "claim1"));
    const tx = `transactions/${current}`;
    const record = (name: string) => records[`${tx}/${name}`] as Record<string, unknown>;
    const journal = record("journal.json");
    const intent = journal.intent as Record<string, unknown>;
    if (condition === "missing-journal") delete records[`${tx}/journal.json`];
    if (condition === "missing-descriptor") delete records[`${tx}/trial-descriptor.json`];
    if (["resume-intent", "blocked", "rolled-back"].includes(condition)) journal.phase = condition;
    if (condition === "different-staged-handle")
      (intent.staged as Record<string, unknown>).handle = "e".repeat(64);
    if (condition === "different-transaction") intent.transactionId = "e".repeat(64);
    if (condition === "home-mismatch") record("continuation.json").home = "/other/home";
    if (condition === "database-mismatch")
      (intent.expected as Record<string, unknown>).databasePath = "/other/state.sqlite";
    if (condition === "noncanonical-profile") {
      const profile = NodePath.dirname(NodePath.dirname(root)) + "/unused/../profile";
      (intent.expected as Record<string, unknown>).profile = profile;
      for (const value of Object.values(records)) {
        if ("profile" in (value as object)) (value as Record<string, unknown>).profile = profile;
      }
      (journal.trialReceipt as Record<string, unknown>).profile = profile;
    }
    if (condition === "receipt-path-mismatch")
      record("trial-descriptor.json").trialReceiptPath = NodePath.join(
        root,
        "transactions",
        nativeTransaction,
        "trial-receipt.json",
      );
    if (condition === "continuation-path-mismatch")
      intent.continuationReceipt = NodePath.join(
        root,
        "transactions",
        nativeTransaction,
        "continuation.json",
      );
    if (condition === "build-mismatch") record("trial-receipt.json").sourceSha = "e".repeat(40);
    if (condition === "listener-mismatch")
      record("trial-receipt.json").listener = "http://127.0.0.1:8888";
    if (condition === "grant-generation-mismatch")
      record("commit-grant.json").generation = nativeTransaction;
    if (condition === "claim-selection-mismatch")
      record("continuation.json").selectionSha256 = "6".repeat(64);
    if (condition === "mixed-legacy-claims") {
      records[`${tx}/continuation.json`] = legacyCommittedReferences(root, "unversioned")[
        `${tx}/continuation.json`
      ];
    }
    if (condition === "retired-location")
      records = Object.fromEntries(
        Object.entries(records).map(([path, value]) => [
          path.replace("transactions/", "retired-preparations/"),
          value,
        ]),
      );
    await writeReferences(root, records);
    const bytes = await referenceSnapshot(root, records);
    await expect(reclaimJonesStagePayload(root, replaced)).rejects.toThrow();
    await expectStageState(root, replaced, old, true);
    await expectStageState(root, current, kept, true);
    await expectHistoryUnchanged(root, bytes);
  });

  it.each([
    ["journal.json", "value"],
    ["journal.json", "key"],
    ["continuation.json", "value"],
    ["trial-receipt.json", "key"],
  ])("legacy whole-value scan retains escaped %s metadata %s", async (name, position) => {
    const root = await updatesRoot(true);
    const old = await completedStage(root, replaced);
    const kept = await completedStage(root, current);
    const records = legacyCommittedReferences(root, "unversioned");
    const path = `transactions/${current}/${name}`;
    records[path] = {
      ...(records[path] as object),
      ...(position === "key" ? { [replaced]: true } : { extraMetadata: replaced }),
    };
    await writeReferences(root, records);
    const escaped = referenceBytes(records[path]).replaceAll("a", "\\u0061");
    expect(escaped.includes(replaced)).toBe(false);
    await NodeFSP.writeFile(NodePath.join(root, path), escaped);
    const bytes = await referenceSnapshot(root, records);
    expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "referenced" });
    await expectStageState(root, replaced, old, true);
    await expectStageState(root, current, kept, true);
    await expectHistoryUnchanged(root, bytes);
  });

  it("legacy directory retains T itself and all history bytes", async () => {
    const root = await updatesRoot(true);
    const old = await completedStage(root, replaced);
    const kept = await completedStage(root, current);
    const records = legacyCommittedReferences(root, "none");
    await writeReferences(root, records);
    const bytes = await referenceSnapshot(root, records);
    expect(await reclaimJonesStagePayload(root, current)).toEqual({ status: "referenced" });
    await expectStageState(root, replaced, old, true);
    await expectStageState(root, current, kept, true);
    await expectHistoryUnchanged(root, bytes);
  });

  it("LEGACY-LEDGER-RECOVERY clears only after restored history permits both roots to finish", async () => {
    const root = await updatesRoot(true);
    const old = await completedStage(root, replaced);
    const kept = await completedStage(root, current);
    const records = legacyCommittedReferences(root, "unversioned");
    await writeReferences(root, records);
    const bytes = await referenceSnapshot(root, records);
    const journal = NodePath.join(root, "transactions", current, "journal.json");
    await NodeFSP.writeFile(journal, "{");
    const corruptBytes = await referenceSnapshot(root, records);
    let record = { enabled: false, unreclaimed: [replaced] };
    let now = 0;
    const clearedStates: boolean[][] = [];
    const terminalPaths = [
      old.artifactAttempt,
      old.appAttempt,
      ...["artifacts", "apps"].map((kind) =>
        NodePath.join(root, kind, "completed", `${replaced}.json`),
      ),
    ];
    const reclaim = vi.fn((handle: string) => reclaimJonesStagePayload(root, handle));
    const background = new JonesBackgroundPreparation({
      controller: {
        busy: null,
        latestAvailable: undefined,
        stagedBuild: undefined,
        preparationHold: undefined,
        check: async () => true,
        download: async () => ({ completed: false }),
        discard: async () => ({ accepted: false, completed: false }),
        supersede: async () => "refused",
        reclaimExclusive: async (reclaim) => {
          await reclaim();
          return true;
        },
        setPreparationStatus: async () => undefined,
      },
      store: {
        read: async () => record,
        write: async (next) => {
          if (next.unreclaimed.length === 0)
            clearedStates.push(await Promise.all(terminalPaths.map(exists)));
          record = { ...next, unreclaimed: [...next.unreclaimed] };
        },
      },
      now: () => now,
      reclaim,
    });
    try {
      await background.start();
      await background.idle();
      expect(reclaim).toHaveBeenCalledTimes(1);
      expect(record.unreclaimed).toEqual([replaced]);
      await expectStageState(root, replaced, old, true);
      await expectStageState(root, current, kept, true);
      await expectHistoryUnchanged(root, corruptBytes);
      now += 1;
      await background.tick();
      expect(reclaim).toHaveBeenCalledTimes(1);
      expect(record.unreclaimed).toEqual([replaced]);
      expect(clearedStates).toEqual([]);
      await expectStageState(root, replaced, old, true);
      await expectHistoryUnchanged(root, corruptBytes);
      await NodeFSP.writeFile(journal, bytes[`transactions/${current}/journal.json`]!);
      now += 60 * 60_000;
      await background.tick();
      expect(reclaim).toHaveBeenCalledTimes(2);
      expect(record.unreclaimed).toEqual([]);
      expect(clearedStates).toEqual([[false, false, false, false]]);
      await expectStageState(root, replaced, old, false);
      await expectStageState(root, current, kept, true);
      await expectHistoryUnchanged(root, bytes);
      expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "absent" });
      await expectHistoryUnchanged(root, bytes);
    } finally {
      background.close();
    }
  });

  it.each(["resumed", "rolled-back"] as const)(
    "DUR-A1-REG-1 permits routine reclaim with complete unrelated %s native, server and retired history",
    async (phase) => {
      const root = await updatesRoot(true);
      const old = await completedStage(root, replaced);
      const kept = await completedStage(root, current);
      const records = retainedReferences(root, phase);
      await writeReferences(root, records);

      expect(await reclaimJonesStagePayload(root, replaced)).toMatchObject({ status: "reclaimed" });
      for (const path of [old.artifactAttempt, old.appAttempt])
        expect(await exists(path)).toBe(false);
      for (const path of [kept.artifactAttempt, kept.appAttempt])
        expect(await exists(path)).toBe(true);
      for (const [path, value] of Object.entries(records))
        expect(await NodeFSP.readFile(NodePath.join(root, path), "utf8")).toBe(
          referenceBytes(value),
        );
      expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "absent" });
    },
  );

  it.each([
    [`transactions/${nativeTransaction}/intent.json`, "transactionId", "4".repeat(64)],
    [
      `transactions/${nativeTransaction}/prepare-intent.json`,
      "selectionPath",
      "/other/staging.json",
    ],
    [`transactions/${nativeTransaction}/continuation.json`, "prepared", false],
    [`transactions/${nativeTransaction}/prepare-request.json`, "processes", []],
    [
      `transactions/${nativeTransaction}/trial-descriptor.json`,
      "trialReceiptPath",
      "/other/receipt.json",
    ],
    [`transactions/${nativeTransaction}/commit-grant.json`, "generation", "4".repeat(64)],
    [`transactions/${nativeTransaction}/resume-dispatched.json`, "resumeHeld", false],
    [
      `retired-preparations/${withdrawnPreparation}/preparation-withdrawal.json`,
      "withdrawalProtocol",
      2,
    ],
    [
      `retired-preparations/${retiredPreparation}/preparation-retirement-receipt.json`,
      "status",
      "ready",
    ],
  ])("DUR-A1-REG-1 holds schema-invalid retained %s field %s", async (path, field, invalid) => {
    const root = await updatesRoot(true);
    const old = await completedStage(root, replaced);
    const records = retainedReferences(root);
    const relative = String(path);
    const value = { ...(records[relative] as object), [String(field)]: invalid };
    await writeReferences(root, { [relative]: value });
    await expect(reclaimJonesStagePayload(root, replaced)).rejects.toThrow();
    for (const kind of ["artifacts", "apps"]) {
      expect(await exists(NodePath.join(root, "stage-cleanup", `${kind}-${replaced}.json`))).toBe(
        false,
      );
      expect(await exists(NodePath.join(root, kind, "completed", `${replaced}.json`))).toBe(true);
    }
    for (const path of [old.artifactAttempt, old.appAttempt]) expect(await exists(path)).toBe(true);
  });

  it.each(["native", "server", "retired"])(
    "DUR-A1-REG-1 retains payloads for direct and escaped %s metadata before typed stripping",
    async (kind) => {
      const root = await updatesRoot(true);
      const old = await completedStage(root, replaced);
      const records = retainedReferences(root);
      const path =
        kind === "native"
          ? `transactions/${nativeTransaction}/intent.json`
          : kind === "retired"
            ? `retired-preparations/${retiredPreparation}/prepare-intent.json`
            : Object.keys(records).find((path) => path.startsWith("selections/"))!;
      const file = NodePath.join(root, path);
      await NodeFSP.mkdir(NodePath.dirname(file), { recursive: true });
      for (const position of ["direct", "escaped-value", "escaped-key"]) {
        const value = {
          ...(structuredClone(records[path]) as object),
          ...(position === "escaped-key" ? { [replaced]: true } : { extraMetadata: replaced }),
        };
        if (position === "direct" && kind === "native")
          (value as { staged?: { handle: string } }).staged!.handle = replaced;
        if (position === "direct" && kind === "retired")
          Object.assign(value, { stagedHandle: replaced });
        const raw =
          position === "direct"
            ? referenceBytes(value)
            : referenceBytes(value).replaceAll("a", "\\u0061");
        if (position !== "direct") expect(raw.includes(replaced)).toBe(false);
        await NodeFSP.writeFile(file, raw);
        expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "referenced" });
        for (const kind of ["artifacts", "apps"]) {
          expect(
            await exists(NodePath.join(root, "stage-cleanup", `${kind}-${replaced}.json`)),
          ).toBe(false);
          expect(await exists(NodePath.join(root, kind, "completed", `${replaced}.json`))).toBe(
            true,
          );
        }
        for (const path of [old.artifactAttempt, old.appAttempt])
          expect(await exists(path)).toBe(true);
      }
    },
  );

  it.each(["journal", "server", "retired-plan", "future-version", "future-file", "truncated"])(
    "DUR-A1-REG-1 holds ledger and backoff for %s evidence, then reclaims with supported history",
    async (fault) => {
      const root = await updatesRoot(true);
      const old = await completedStage(root, replaced);
      const records = retainedReferences(root);
      await writeReferences(root, records);
      const server = Object.keys(records).find((path) => path.startsWith("selections/"))!;
      const path =
        fault === "journal"
          ? `transactions/${nativeTransaction}/journal.json`
          : fault === "retired-plan"
            ? `retired-preparations/${retiredPreparation}/preparation-retirement.json`
            : fault === "future-version"
              ? `transactions/${nativeTransaction}/prepare-intent.json`
              : fault === "future-file"
                ? `transactions/${nativeTransaction}/future-intent.json`
                : server;
      const file = NodePath.join(root, path);
      const value = structuredClone(
        records[path] ?? records[`transactions/${nativeTransaction}/prepare-intent.json`],
      ) as Record<string, unknown>;
      if (fault === "journal") value.phase = 17;
      if (fault === "server") (value.receipt as Record<string, unknown>).runId = "invalid";
      if (fault === "retired-plan") (value.plan as Record<string, unknown>).records = [];
      if (fault === "future-version") value.protocol = 2;
      await NodeFSP.writeFile(
        file,
        fault === "truncated" ? referenceBytes(value).slice(0, -8) : referenceBytes(value),
      );
      let record = { enabled: false, unreclaimed: [replaced] };
      let now = 0;
      const reclaim = vi.fn((handle: string) => reclaimJonesStagePayload(root, handle));
      const background = new JonesBackgroundPreparation({
        controller: {
          busy: null,
          latestAvailable: undefined,
          stagedBuild: undefined,
          preparationHold: undefined,
          check: async () => true,
          download: async () => ({ completed: false }),
          discard: async () => ({ accepted: false, completed: false }),
          supersede: async () => "refused",
          reclaimExclusive: async (reclaim) => {
            await reclaim();
            return true;
          },
          setPreparationStatus: async () => undefined,
        },
        store: {
          read: async () => record,
          write: async (next) => {
            record = { ...next, unreclaimed: [...next.unreclaimed] };
          },
        },
        now: () => now,
        reclaim,
      });
      try {
        await background.start();
        await background.idle();
        expect(reclaim).toHaveBeenCalledTimes(1);
        expect(record.unreclaimed).toEqual([replaced]);
        for (const kind of ["artifacts", "apps"]) {
          expect(
            await exists(NodePath.join(root, "stage-cleanup", `${kind}-${replaced}.json`)),
          ).toBe(false);
          expect(await exists(NodePath.join(root, kind, "completed", `${replaced}.json`))).toBe(
            true,
          );
        }
        now += 1;
        await background.tick();
        expect(reclaim).toHaveBeenCalledTimes(1);
        now += 60 * 60_000;
        await background.tick();
        expect(reclaim).toHaveBeenCalledTimes(2);
        expect(record.unreclaimed).toEqual([replaced]);
        for (const path of [old.artifactAttempt, old.appAttempt])
          expect(await exists(path)).toBe(true);
        if (fault === "future-file") await NodeFSP.unlink(file);
        else await NodeFSP.writeFile(file, referenceBytes(records[path]));
        now += 60 * 60_000;
        await background.tick();
        expect(reclaim).toHaveBeenCalledTimes(3);
        expect(record.unreclaimed).toEqual([]);
        for (const path of [old.artifactAttempt, old.appAttempt])
          expect(await exists(path)).toBe(false);
      } finally {
        background.close();
      }
    },
  );

  it("removes only the replaced handle's indexed attempts", async () => {
    const root = await updatesRoot();
    const old = await completedStage(root, replaced);
    const kept = await completedStage(root, current);
    await NodeFSP.mkdir(NodePath.join(root, "staging"));
    await NodeFSP.writeFile(
      NodePath.join(root, "staging", "selection.json"),
      JSON.stringify(stageReference(current)),
    );

    const result = await reclaimJonesStagePayload(root, replaced);
    expect(result.status).toBe("reclaimed");
    // The payloads plus each attempt's linked completion record.
    expect(result.status === "reclaimed" && result.bytes).toBeGreaterThan(
      "dmg-bytes".length + "{}".length * 2 + "asar-bytes".length,
    );
    for (const path of [
      old.artifactAttempt,
      old.appAttempt,
      NodePath.join(root, "artifacts", "completed", `${replaced}.json`),
      NodePath.join(root, "apps", "completed", `${replaced}.json`),
    ])
      expect(await exists(path)).toBe(false);
    for (const path of [
      kept.artifactAttempt,
      kept.appAttempt,
      NodePath.join(root, "apps", "completed", `${current}.json`),
    ])
      expect(await exists(path)).toBe(true);
    expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "absent" });
  });

  it.each([
    ["a selection", "staging", "selection.json"],
    ["an attempt selection", "attempt-selections", "attempt.json"],
    ["a server runtime selection", "selections", "runtime.json"],
    ["a transaction record", "transactions", "txn-1/journal.json"],
    ["a retired preparation", "retired-preparations", "txn-2/prepare-intent.json"],
  ])("keeps a payload that %s still names", async (_label, directory, file) => {
    const root = await updatesRoot();
    const old = await completedStage(root, replaced);
    const record = NodePath.join(root, directory, file);
    await NodeFSP.mkdir(NodePath.dirname(record), { recursive: true });
    await NodeFSP.writeFile(record, JSON.stringify({ staged: { handle: replaced } }));

    expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "referenced" });
    expect(await exists(old.artifactAttempt)).toBe(true);
    expect(await exists(old.appAttempt)).toBe(true);
  });

  it.each([
    ["attempt-selections/attempt.json", "{"],
    ["staging/historical.json", '{"schema":1,'],
    ["selections/historical.json", "{"],
    ["attempt-selections/attempt.json", '{"protocol":1}'],
    ["staging/historical.json", JSON.stringify({ app: { handle: current } })],
    ["transactions/txn-1/journal.json", '{"unrecognized":true}'],
  ])(
    "holds an unreadable reference at %s (%s) before granting deletion authority",
    async (path, raw) => {
      const root = await updatesRoot();
      const old = await completedStage(root, replaced);
      const reference = NodePath.join(root, path);
      await NodeFSP.mkdir(NodePath.dirname(reference), { recursive: true });
      await NodeFSP.writeFile(reference, raw);

      await expect(reclaimJonesStagePayload(root, replaced)).rejects.toThrow();
      for (const kind of ["artifacts", "apps"]) {
        expect(await exists(NodePath.join(root, "stage-cleanup", `${kind}-${replaced}.json`))).toBe(
          false,
        );
        expect(await exists(NodePath.join(root, kind, "completed", `${replaced}.json`))).toBe(true);
      }
      for (const attempt of [old.artifactAttempt, old.appAttempt]) {
        expect(await exists(NodePath.join(attempt, replaced))).toBe(true);
        expect(await exists(NodePath.join(attempt, "completion.json"))).toBe(true);
      }
      expect(await NodeFSP.readFile(reference, "utf8")).toBe(raw);
    },
  );

  it.each(["value", "key"])(
    "keeps a payload named by an escaped reference %s",
    async (position) => {
      const root = await updatesRoot();
      const old = await completedStage(root, replaced);
      const reference = NodePath.join(root, "staging", "historical.json");
      await NodeFSP.mkdir(NodePath.dirname(reference));
      const value = position === "value" ? stageReference(replaced) : { [replaced]: true };
      const raw = JSON.stringify(value).replaceAll("a", "\\u0061");
      expect(raw.includes(replaced)).toBe(false);
      await NodeFSP.writeFile(reference, raw);

      expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "referenced" });
      for (const kind of ["artifacts", "apps"]) {
        expect(await exists(NodePath.join(root, "stage-cleanup", `${kind}-${replaced}.json`))).toBe(
          false,
        );
        expect(await exists(NodePath.join(root, kind, "completed", `${replaced}.json`))).toBe(true);
      }
      expect(await exists(NodePath.join(old.artifactAttempt, replaced))).toBe(true);
      expect(await exists(NodePath.join(old.appAttempt, replaced))).toBe(true);
    },
  );

  it("treats a transaction named for the handle as evidence", async () => {
    const root = await updatesRoot();
    await completedStage(root, replaced);
    await NodeFSP.mkdir(NodePath.join(root, "transactions", replaced), { recursive: true });
    expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "referenced" });
  });

  it("skips a transaction's recovery reserve but refuses an unbounded record", async () => {
    const root = await updatesRoot();
    await completedStage(root, replaced);
    const transaction = NodePath.join(root, "transactions", "txn-1");
    await NodeFSP.mkdir(transaction, { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(transaction, "reserve.bin"),
      Buffer.alloc(2 * 1024 * 1024),
    );
    for (const name of ["previous-pair", "advanced-state"]) {
      const state = NodePath.join(transaction, name);
      await NodeFSP.mkdir(state);
      await NodeFSP.writeFile(NodePath.join(state, "state.sqlite"), "non-JSON state copy");
    }
    expect(await reclaimJonesStagePayload(root, replaced)).toMatchObject({ status: "reclaimed" });

    await completedStage(root, current);
    await NodeFSP.mkdir(NodePath.join(root, "staging"), { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(root, "staging", "huge.json"),
      Buffer.alloc(2 * 1024 * 1024),
    );
    await expect(reclaimJonesStagePayload(root, current)).rejects.toThrow("bounded regular file");
  });

  it("refuses an invalid handle without reading any record", async () => {
    const root = await updatesRoot();
    await expect(reclaimJonesStagePayload(root, "../apps")).rejects.toThrow("Invalid");
  });

  it("holds both roots when a legacy directory has no receipt", async () => {
    const root = await updatesRoot();
    const legacy = NodePath.join(root, "apps", replaced);
    await NodeFSP.mkdir(NodePath.join(legacy, "Candidate.app"), { recursive: true });
    await NodeFSP.writeFile(NodePath.join(legacy, "mac-app-receipt.json"), "{}");
    const partial = NodePath.join(root, "artifacts", replaced);
    await NodeFSP.mkdir(partial);

    await expect(reclaimJonesStagePayload(root, replaced)).rejects.toMatchObject({
      code: "ENOENT",
    });
    expect(await exists(legacy)).toBe(true);
    expect(await exists(partial)).toBe(true);
  });
  it("reclaims a receipted exact-handle legacy stage", async () => {
    const root = await updatesRoot();
    const legacy = NodePath.join(root, "apps", replaced);
    await NodeFSP.mkdir(legacy);
    await NodeFSP.writeFile(NodePath.join(legacy, "mac-app-receipt.json"), "{}");
    expect(await reclaimJonesStagePayload(root, replaced)).toEqual({
      status: "reclaimed",
      bytes: 2,
    });
    expect(await exists(legacy)).toBe(false);
  });

  for (const kind of ["artifacts", "apps"] as const) {
    it.each([
      "marker-publish",
      "marker-sync",
      "index-unlink",
      "index-sync",
      "payload-before",
      "payload-partial",
      "payload-after",
      "completion-unlink",
      "attempt-rmdir",
      "parent-sync",
      "marker-retire",
    ])(
      `replays ${kind} %s interruption using durable external cleanup authority`,
      async (prefix) => {
        const root = await updatesRoot();
        const old = await completedStage(root, replaced);
        const kept = await completedStage(root, current);
        const attempt = kind === "apps" ? old.appAttempt : old.artifactAttempt;
        const marker = NodePath.join(root, "stage-cleanup", `${kind}-${replaced}.json`);
        const index = NodePath.join(root, kind, "completed", `${replaced}.json`);
        const operations: Record<string, [string, string, boolean]> = {
          "marker-publish": ["link", marker, true],
          "marker-sync": ["sync", NodePath.dirname(marker), false],
          "index-unlink": ["unlink", index, true],
          "index-sync": ["sync", NodePath.dirname(index), false],
          "payload-before": ["rm", NodePath.join(attempt, replaced), false],
          "payload-partial": ["rm", NodePath.join(attempt, replaced), false],
          "payload-after": ["rm", NodePath.join(attempt, replaced), true],
          "completion-unlink": ["unlink", NodePath.join(attempt, "completion.json"), true],
          "attempt-rmdir": ["rmdir", attempt, true],
          "parent-sync": ["sync", NodePath.dirname(attempt), false],
          "marker-retire": ["unlink", marker, true],
        };
        [faults.operation, faults.path, faults.after] = operations[prefix]!;
        faults.partial = prefix === "payload-partial";
        if (prefix === "marker-sync") faults.gate = `link:${marker}`;
        await expect(reclaimJonesStagePayload(root, replaced)).rejects.toMatchObject({
          code: "EIO",
        });
        if (!["marker-retire"].includes(prefix)) expect(await exists(marker)).toBe(true);
        if (prefix === "payload-partial")
          expect(await exists(NodePath.join(attempt, "completion.json"))).toBe(false);
        resetFaults();
        const result = await reclaimJonesStagePayload(root, replaced);
        expect(["absent", "reclaimed"]).toContain(result.status);
        expect(faults.events).toContain(`sync:${NodePath.join(root, "stage-cleanup")}`);
        if (["index-unlink", "index-sync"].includes(prefix))
          expect(faults.events).toContain(`sync:${NodePath.dirname(index)}`);
        if (["attempt-rmdir", "parent-sync"].includes(prefix))
          expect(faults.events).toContain(`sync:${NodePath.dirname(attempt)}`);
        for (const path of [old.artifactAttempt, old.appAttempt])
          expect(await exists(path)).toBe(false);
        expect(await exists(kept.artifactAttempt)).toBe(true);
        expect(await exists(kept.appAttempt)).toBe(true);
        expect(await NodeFSP.readdir(NodePath.join(root, "stage-cleanup"))).toEqual([]);
        // The exact unindexed attempt that blocked subsequent Mac staging is now gone.
        const next = await createJonesStageAttempt(NodePath.join(root, kind), "c".repeat(64));
        expect(await exists(next.directory)).toBe(true);
      },
    );
  }

  it("recovers the legacy index-unlinked prefix only from a surviving self-describing completion", async () => {
    const root = await updatesRoot();
    const old = await completedStage(root, replaced);
    for (const kind of ["artifacts", "apps"])
      await NodeFSP.unlink(NodePath.join(root, kind, "completed", `${replaced}.json`));
    expect(await reclaimJonesStagePayload(root, replaced)).toMatchObject({ status: "reclaimed" });
    expect(await exists(old.artifactAttempt)).toBe(false);
    expect(await exists(old.appAttempt)).toBe(false);
  });

  it.each([
    "mount",
    "payload-mount",
    "copier.json",
    "payload-copier",
    "corrupt-completion",
    "missing-completion",
    "symlink",
    "root-symlink",
    "cross-device",
    "unexplained-root",
  ])(
    "holds %s without changing unrelated paths or deleting activation evidence",
    async (condition) => {
      const root = await updatesRoot();
      const old = await completedStage(root, replaced);
      const kept = await completedStage(root, current);
      const receipt = NodePath.join(old.artifactAttempt, "completion.json");
      const payload = NodePath.join(old.artifactAttempt, replaced);
      if (condition === "mount") await NodeFSP.mkdir(NodePath.join(old.artifactAttempt, "mount"));
      if (condition === "payload-mount") await NodeFSP.mkdir(NodePath.join(payload, "mount"));
      if (condition === "payload-copier")
        await NodeFSP.writeFile(NodePath.join(payload, "copier.json"), "{}");
      if (condition === "copier.json")
        await NodeFSP.writeFile(NodePath.join(old.artifactAttempt, "copier.json"), "{}");
      if (condition === "corrupt-completion") await NodeFSP.writeFile(receipt, "{}");
      if (condition === "missing-completion") await NodeFSP.unlink(receipt);
      if (condition === "symlink")
        await NodeFSP.symlink(kept.artifactAttempt, NodePath.join(payload, "escape"));
      if (condition === "root-symlink") {
        const directory = NodePath.join(root, "artifacts");
        await NodeFSP.rename(directory, `${directory}-kept`);
        await NodeFSP.symlink(`${directory}-kept`, directory);
      }
      if (condition === "cross-device")
        faults.foreignDevice = NodePath.join(payload, "qualified.dmg");
      if (condition === "unexplained-root")
        await NodeFSP.mkdir(NodePath.join(root, "artifacts", "unknown"));
      const keptReceipt = await NodeFSP.readFile(
        NodePath.join(kept.artifactAttempt, "completion.json"),
        "utf8",
      );
      await expect(reclaimJonesStagePayload(root, replaced)).rejects.toThrow();
      expect(await exists(old.artifactAttempt)).toBe(true);
      expect(await exists(old.appAttempt)).toBe(true);
      expect(await exists(NodePath.join(root, "artifacts", "completed", `${replaced}.json`))).toBe(
        true,
      );
      expect(
        await NodeFSP.readFile(NodePath.join(kept.artifactAttempt, "completion.json"), "utf8"),
      ).toBe(keptReceipt);
    },
  );

  it.each([
    "corrupt-marker",
    "new-index",
    "new-unindexed-attempt",
    "changed-target",
    "new-reference",
  ])("keeps pinned partial cleanup held on %s", async (condition) => {
    const root = await updatesRoot();
    const old = await completedStage(root, replaced);
    faults.operation = "rm";
    faults.path = NodePath.join(old.artifactAttempt, replaced);
    await expect(reclaimJonesStagePayload(root, replaced)).rejects.toThrow("EIO");
    resetFaults();
    const marker = NodePath.join(root, "stage-cleanup", `artifacts-${replaced}.json`);
    if (condition === "corrupt-marker") await NodeFSP.writeFile(marker, "{}");
    if (condition === "new-index") {
      const next = await createJonesStageAttempt(NodePath.join(root, "artifacts"), replaced);
      await NodeFSP.writeFile(NodePath.join(next.directory, JONES_ACTIONS_RECEIPT_FILE), "{}");
      await publishJonesCompletedStage(
        NodePath.join(root, "artifacts"),
        replaced,
        next.directory,
        JONES_ACTIONS_RECEIPT_FILE,
      );
    }
    if (condition === "new-unindexed-attempt") {
      const next = await createJonesStageAttempt(NodePath.join(root, "artifacts"), replaced);
      await NodeFSP.writeFile(
        NodePath.join(next.attemptRoot, "completion.json"),
        JSON.stringify({ schema: 1, handle: replaced, directory: next.directory }),
      );
      await NodeFSP.writeFile(NodePath.join(next.directory, JONES_ACTIONS_RECEIPT_FILE), "{}");
    }
    if (condition === "changed-target") {
      await NodeFSP.rename(old.artifactAttempt, `${old.artifactAttempt}-kept`);
      await NodeFSP.mkdir(old.artifactAttempt);
    }
    if (condition === "new-reference") {
      await NodeFSP.mkdir(NodePath.join(root, "selections"));
      await NodeFSP.writeFile(
        NodePath.join(root, "selections", "live.json"),
        JSON.stringify({ handle: replaced }),
      );
      expect(await reclaimJonesStagePayload(root, replaced)).toEqual({ status: "referenced" });
    } else await expect(reclaimJonesStagePayload(root, replaced)).rejects.toThrow();
    expect(await exists(marker)).toBe(true);
    expect(await exists(old.artifactAttempt)).toBe(true);
    expect(await exists(old.appAttempt)).toBe(true);
  });

  it("unblocks the real Mac staging gate after replay of index-unlink EIO", async () => {
    const root = await updatesRoot();
    const old = await completedStage(root, replaced);
    const next = {
      candidate: { platform: "darwin" },
      payloadPath: NodePath.join(root, "fixture.dmg"),
      stagedHandle: "c".repeat(64),
    } as JonesStagedArtifact;
    faults.operation = "rm";
    faults.path = NodePath.join(old.appAttempt, replaced);
    await expect(reclaimJonesStagePayload(root, replaced)).rejects.toThrow("EIO");
    await expect(stageJonesMacApp(next, NodePath.join(root, "apps"), "darwin")).rejects.toThrow(
      "incomplete or unknown",
    );
    expect(NodeChildProcess.execFile).not.toHaveBeenCalled();
    resetFaults();
    await reclaimJonesStagePayload(root, replaced);
    vi.mocked(NodeChildProcess.execFile).mockImplementation(
      (_command, _args, _options, callback) => {
        if (typeof callback !== "function") throw new Error("Missing native callback.");
        callback(new Error("Synthetic native boundary"), "", "");
        return { exitCode: 1, signalCode: null } as ReturnType<typeof NodeChildProcess.execFile>;
      },
    );
    await expect(stageJonesMacApp(next, NodePath.join(root, "apps"), "darwin")).rejects.toThrow();
    expect(NodeChildProcess.execFile).toHaveBeenCalledWith(
      "/usr/bin/hdiutil",
      expect.arrayContaining(["attach"]),
      expect.any(Object),
      expect.any(Function),
    );
  });

  it("keeps the background ledger and backoff for an unreadable reference until it is removed", async () => {
    const root = await updatesRoot();
    const old = await completedStage(root, replaced);
    const reference = NodePath.join(root, "attempt-selections", "attempt.json");
    await NodeFSP.mkdir(NodePath.dirname(reference));
    await NodeFSP.writeFile(reference, "{");
    let record = { enabled: false, unreclaimed: [replaced] };
    let now = 0;
    const reclaim = vi.fn((handle: string) => reclaimJonesStagePayload(root, handle));
    const background = new JonesBackgroundPreparation({
      controller: {
        busy: null,
        latestAvailable: undefined,
        stagedBuild: undefined,
        preparationHold: undefined,
        check: async () => true,
        download: async () => ({ completed: false }),
        discard: async () => ({ accepted: false, completed: false }),
        supersede: async () => "refused",
        reclaimExclusive: async (reclaim) => {
          await reclaim();
          return true;
        },
        setPreparationStatus: async () => undefined,
      },
      store: {
        read: async () => record,
        write: async (next) => {
          record = { ...next, unreclaimed: [...next.unreclaimed] };
        },
      },
      now: () => now,
      reclaim,
    });
    try {
      await background.start();
      await background.idle();
      expect(reclaim).toHaveBeenCalledTimes(1);
      expect(record.unreclaimed).toEqual([replaced]);
      for (const kind of ["artifacts", "apps"]) {
        expect(await exists(NodePath.join(root, "stage-cleanup", `${kind}-${replaced}.json`))).toBe(
          false,
        );
        expect(await exists(NodePath.join(root, kind, "completed", `${replaced}.json`))).toBe(true);
      }
      expect(await exists(NodePath.join(old.artifactAttempt, replaced))).toBe(true);
      expect(await exists(NodePath.join(old.appAttempt, replaced))).toBe(true);
      now += 1;
      await background.tick();
      expect(reclaim).toHaveBeenCalledTimes(1);
      now += 60 * 60_000;
      await background.tick();
      expect(reclaim).toHaveBeenCalledTimes(2);
      expect(record.unreclaimed).toEqual([replaced]);
      await NodeFSP.unlink(reference);
      now += 60 * 60_000;
      await background.tick();
      expect(reclaim).toHaveBeenCalledTimes(3);
      expect(record.unreclaimed).toEqual([]);
      expect(await exists(old.artifactAttempt)).toBe(false);
      expect(await exists(old.appAttempt)).toBe(false);
    } finally {
      background.close();
    }
  });

  it("keeps the background ledger through real EIO retries and clears it only after both roots finish", async () => {
    const root = await updatesRoot();
    const old = await completedStage(root, replaced);
    let record = { enabled: false, unreclaimed: [replaced] };
    let now = 0;
    const background = new JonesBackgroundPreparation({
      controller: {
        busy: null,
        latestAvailable: undefined,
        stagedBuild: undefined,
        preparationHold: undefined,
        check: async () => true,
        download: async () => ({ completed: false }),
        discard: async () => ({ accepted: false, completed: false }),
        supersede: async () => "refused",
        reclaimExclusive: async (reclaim) => {
          await reclaim();
          return true;
        },
        setPreparationStatus: async () => undefined,
      },
      store: {
        read: async () => record,
        write: async (next) => {
          record = { ...next, unreclaimed: [...next.unreclaimed] };
        },
      },
      now: () => now,
      reclaim: (handle) => reclaimJonesStagePayload(root, handle),
    });
    faults.operation = "rm";
    faults.path = NodePath.join(old.appAttempt, replaced);
    try {
      await background.start();
      await background.idle();
      expect(record.unreclaimed).toEqual([replaced]);
      now += 60 * 60_000;
      await background.tick();
      expect(record.unreclaimed).toEqual([replaced]);
      expect(await exists(old.appAttempt)).toBe(true);
      resetFaults();
      now += 60 * 60_000;
      await background.tick();
      expect(record.unreclaimed).toEqual([]);
      expect(await exists(old.artifactAttempt)).toBe(false);
      expect(await exists(old.appAttempt)).toBe(false);
    } finally {
      background.close();
    }
  });
});
