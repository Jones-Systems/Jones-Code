import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import { packagedStartupNames, runOwnedChild, snapshotTree } from "./native-startup-fixture.mjs";
import { observerControl, protectedMutations, startObserver } from "./native-startup-observer.mjs";

import { preparePackagedDesktopHelperFault } from "./packaged-desktop-helper-fault.mjs";

function assertDarwin() {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone packaged fixture admits its actual native host without loading application services.
  NodeAssert.equal(process.platform, "darwin");
}

const packagedDesktopSessionScript = String.raw`
ObjC.import("CoreGraphics");
var raw = $.CGSessionCopyCurrentDictionary();
var session = raw ? ObjC.deepUnwrap(ObjC.castRefToObject(raw)) : null;
function booleanOrNull(value) { return typeof value === "boolean" ? value : null; }
JSON.stringify({
  schema: "jones-packaged-desktop-gui-session/v1",
  sessionAvailable: session !== null,
  onConsole: session ? booleanOrNull(session.kCGSSessionOnConsoleKey) : null,
  loginDone: session ? booleanOrNull(session.kCGSessionLoginDoneKey) : null,
  // An unlocked CGSession omits the lock key; a present malformed value remains unknown.
  screenLocked: session
    ? Object.prototype.hasOwnProperty.call(session, "CGSSessionScreenIsLocked")
      ? booleanOrNull(session.CGSSessionScreenIsLocked) : false
    : null,
  sessionUid: session && typeof session.kCGSSessionUserIDKey === "number"
    ? session.kCGSSessionUserIDKey : null
});
`;

export function validatePackagedDesktopGuiSession(receipt, { uid, lock = "unlocked" }) {
  NodeAssert.ok(["unlocked", "any"].includes(lock), "Unknown GUI lock requirement.");
  NodeAssert.equal(receipt.schema, "jones-packaged-desktop-gui-session/v1");
  NodeAssert.ok(Number.isSafeInteger(uid) && uid >= 0);
  NodeAssert.equal(receipt.sessionAvailable, true, "Packaged desktop GUI session is unavailable.");
  NodeAssert.equal(
    receipt.sessionUid,
    uid,
    "Packaged desktop GUI session belongs to another user.",
  );
  NodeAssert.equal(receipt.onConsole, true, "Packaged desktop GUI session is not on console.");
  NodeAssert.equal(receipt.loginDone, true, "Packaged desktop GUI login is incomplete.");
  NodeAssert.equal(
    typeof receipt.screenLocked,
    "boolean",
    "Packaged desktop GUI screen-lock state is unknown.",
  );
  if (lock === "unlocked")
    NodeAssert.equal(receipt.screenLocked, false, "Packaged desktop GUI screen is locked.");
  return Object.freeze({ ...receipt });
}

export async function assertPackagedDesktopGuiSession({ fixtureRoot, signal, lock = "unlocked" }) {
  assertDarwin();
  const result = await runOwnedChild(
    "/usr/bin/osascript",
    ["-l", "JavaScript", "-e", packagedDesktopSessionScript],
    {
      cwd: fixtureRoot,
      env: { PATH: "/usr/bin:/bin", HOME: fixtureRoot },
      signal,
      timeout: 5000,
    },
  );
  NodeAssert.equal(result.signal, null, result.stderr);
  NodeAssert.equal(result.code, 0, result.stderr || result.stdout);
  NodeAssert.ok(result.stdout.length <= 4096);
  return validatePackagedDesktopGuiSession(JSON.parse(result.stdout), {
    uid: process.getuid(),
    lock,
  });
}

const shaPattern = /^[0-9a-f]{64}$/;
const sourcePattern = /^[0-9a-f]{40}$/;
const sha = (value) => NodeCrypto.createHash("sha256").update(value).digest("hex");

async function fileHash(filename) {
  const handle = await NodeFSP.open(
    filename,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  );
  try {
    const before = await handle.stat({ bigint: true });
    NodeAssert.ok(
      before.isFile() && before.nlink === 1n && before.uid === BigInt(process.getuid()),
    );
    const hash = NodeCrypto.createHash("sha256");
    for await (const chunk of handle.createReadStream({ autoClose: false })) hash.update(chunk);
    const after = await handle.stat({ bigint: true });
    for (const key of ["dev", "ino", "size", "mode", "mtimeNs", "ctimeNs"])
      NodeAssert.equal(after[key], before[key], "Bundle file changed while hashing.");
    const current = await NodeFSP.lstat(filename, { bigint: true });
    NodeAssert.equal(current.dev, before.dev);
    NodeAssert.equal(current.ino, before.ino);
    return hash.digest("hex");
  } finally {
    await handle.close();
  }
}

async function confined(root, filename, directory = false) {
  NodeAssert.equal(NodePath.resolve(filename), filename);
  NodeAssert.ok(filename.startsWith(root + NodePath.sep) || filename === root);
  NodeAssert.equal(await NodeFSP.realpath(filename), filename);
  const info = await NodeFSP.lstat(filename);
  NodeAssert.equal(info.uid, process.getuid());
  NodeAssert.ok(directory ? info.isDirectory() : info.isFile());
  return info;
}

// Matches the native staged-app digest, including internal framework symlinks.
export async function fingerprintPackagedDesktopBundle(appPath) {
  NodeAssert.equal(await NodeFSP.realpath(appPath), appPath);
  NodeAssert.ok(appPath.endsWith(".app"));
  const files = [];
  async function visit(directory, depth) {
    NodeAssert.ok(depth <= 64);
    for (const entry of await NodeFSP.readdir(directory, { withFileTypes: true })) {
      const filename = NodePath.join(directory, entry.name);
      files.push(filename);
      NodeAssert.ok(files.length <= 200000);
      if (entry.isDirectory()) await visit(filename, depth + 1);
    }
  }
  await visit(appPath, 0);
  files.sort((a, b) =>
    Buffer.compare(
      Buffer.from(NodePath.relative(appPath, a)),
      Buffer.from(NodePath.relative(appPath, b)),
    ),
  );
  const hash = NodeCrypto.createHash("sha256");
  let bytes = 0;
  for (const filename of files) {
    const relative = NodePath.relative(appPath, filename).split(NodePath.sep).join("/");
    const info = await NodeFSP.lstat(filename);
    NodeAssert.equal(info.uid, process.getuid());
    let record;
    if (info.isSymbolicLink()) {
      const target = await NodeFSP.realpath(filename);
      NodeAssert.ok(target === appPath || target.startsWith(appPath + NodePath.sep));
      record = ["link", relative, await NodeFSP.readlink(filename)];
    } else if (info.isDirectory()) {
      record = ["directory", relative, info.mode & 0o777];
    } else {
      NodeAssert.ok(info.isFile() && info.nlink === 1);
      bytes += info.size;
      NodeAssert.ok(bytes <= 8 * 1024 ** 3);
      record = ["file", relative, info.mode & 0o777, await fileHash(filename)];
    }
    hash.update(JSON.stringify(record) + "\n");
  }
  return Object.freeze({ appDigest: hash.digest("hex"), files: files.length, bytes });
}

// Only the packed root package.json is read. No archive extraction or app execution.
// Format reference: electron/asar src/disk.ts and src/pickle.ts.
export async function readPackagedDesktopMetadata(asarPath) {
  const handle = await NodeFSP.open(
    asarPath,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  );
  try {
    const before = await handle.stat({ bigint: true });
    NodeAssert.ok(before.isFile() && before.nlink === 1n && before.size <= 8n * 1024n ** 3n);
    async function read(position, length) {
      const buffer = Buffer.alloc(length);
      const result = await handle.read(buffer, 0, length, position);
      NodeAssert.equal(result.bytesRead, length, "Truncated ASAR input.");
      return buffer;
    }
    const size = await read(0, 8);
    NodeAssert.equal(size.readUInt32LE(0), 4);
    const headerSize = size.readUInt32LE(4);
    NodeAssert.ok(
      headerSize >= 8 && headerSize <= 16 * 1024 ** 2 && headerSize + 8 <= Number(before.size),
    );
    const header = await read(8, headerSize);
    NodeAssert.equal(header.readUInt32LE(0), headerSize - 4);
    const jsonSize = header.readUInt32LE(4);
    NodeAssert.ok(jsonSize > 0 && jsonSize <= headerSize - 8 && headerSize - 8 - jsonSize < 4);
    const decoder = new NodeUtil.TextDecoder("utf-8", { fatal: true });
    const tree = JSON.parse(decoder.decode(header.subarray(8, 8 + jsonSize)));
    const entry = tree.files?.["package.json"];
    NodeAssert.ok(entry && !entry.link && !entry.files && !entry.unpacked);
    NodeAssert.ok(typeof entry.offset === "string" && /^(0|[1-9][0-9]*)$/.test(entry.offset));
    NodeAssert.ok(Number.isSafeInteger(entry.size) && entry.size > 0 && entry.size <= 256 * 1024);
    const offset = 8 + headerSize + Number(entry.offset);
    NodeAssert.ok(Number.isSafeInteger(offset) && offset + entry.size <= Number(before.size));
    const result = JSON.parse(decoder.decode(await read(offset, entry.size)));
    const after = await handle.stat({ bigint: true });
    for (const key of ["dev", "ino", "size", "mtimeNs", "ctimeNs"])
      NodeAssert.equal(after[key], before[key]);
    return result;
  } finally {
    await handle.close();
  }
}

export async function bindPackagedDesktopPair({ fixtureRoot, active, candidate }) {
  const root = await confined(fixtureRoot, fixtureRoot, true);
  NodeAssert.equal(root.mode & 0o077, 0, "The disposable fixture root must be private.");
  NodeAssert.notEqual(active.appPath, candidate.appPath);
  async function bind(input) {
    const appInfo = await confined(fixtureRoot, input.appPath, true);
    NodeAssert.ok(
      input.executablePath.startsWith(input.appPath + NodePath.sep + "Contents/MacOS/"),
    );
    const executableInfo = await confined(input.appPath, input.executablePath);
    NodeAssert.ok((executableInfo.mode & 0o111) !== 0);
    NodeAssert.match(input.sourceSha, sourcePattern);
    NodeAssert.match(input.sourceTree, sourcePattern);
    NodeAssert.match(input.appDigest, shaPattern);
    const asar = NodePath.join(input.appPath, "Contents/Resources/app.asar");
    await confined(input.appPath, asar);
    const metadata = await readPackagedDesktopMetadata(asar);
    NodeAssert.equal(metadata.jonesSource?.repository, "Jones-Systems/Jones-Code");
    NodeAssert.equal(metadata.jonesSource?.sha, input.sourceSha);
    NodeAssert.equal(metadata.jonesSource?.tree, input.sourceTree);
    NodeAssert.equal(metadata.version, input.version);
    NodeAssert.equal(metadata.startupGateProtocol, 1);
    packagedStartupNames(metadata, {
      executableName: NodePath.basename(input.executablePath),
      bundleIdentifier: input.bundleIdentifier,
      bundleName: NodePath.basename(input.appPath, ".app"),
    });
    const fingerprint = await fingerprintPackagedDesktopBundle(input.appPath);
    NodeAssert.equal(fingerprint.appDigest, input.appDigest);
    return Object.freeze({
      ...input,
      metadata,
      fingerprint,
      identity: Object.freeze({ dev: appInfo.dev, ino: appInfo.ino }),
    });
  }
  const boundActive = await bind(active);
  const boundCandidate = await bind(candidate);
  return Object.freeze({
    fixtureRoot,
    active: boundActive,
    candidate: boundCandidate,
    async assertUnchanged() {
      const now = await confined(fixtureRoot, fixtureRoot, true);
      NodeAssert.equal(now.dev, root.dev);
      NodeAssert.equal(now.ino, root.ino);
      for (const item of [boundActive, boundCandidate]) {
        const info = await confined(fixtureRoot, item.appPath, true);
        NodeAssert.deepEqual({ dev: info.dev, ino: info.ino }, item.identity);
        NodeAssert.equal(
          (await fingerprintPackagedDesktopBundle(item.appPath)).appDigest,
          item.appDigest,
        );
      }
    },
  });
}

export function validatePackagedDesktopDialog(receipt, { pid, executable, code, transactionId }) {
  NodeAssert.equal(receipt.protocol, 1);
  NodeAssert.equal(receipt.kind, "packaged-desktop-native-dialog");
  NodeAssert.equal(receipt.pid, pid);
  NodeAssert.equal(receipt.executable, executable);
  NodeAssert.ok(Number.isSafeInteger(receipt.startSec) && receipt.startSec > 0);
  NodeAssert.ok(
    Number.isSafeInteger(receipt.startUsec) &&
      receipt.startUsec >= 0 &&
      receipt.startUsec < 1000000,
  );
  NodeAssert.equal(receipt.code, code);
  NodeAssert.equal(receipt.transactionId, transactionId);
  NodeAssert.equal(receipt.title, "Jones Code could not start safely");
  NodeAssert.ok(
    ["AXDialog", "AXSystemDialog"].includes(receipt.subrole) || receipt.role === "AXDialog",
  );
  NodeAssert.ok(typeof receipt.text === "string" && receipt.text.length <= 65536);
  NodeAssert.ok(receipt.text.includes("Startup refused (" + code + ")."));
  if (transactionId !== null) NodeAssert.ok(receipt.text.includes("Transaction: " + transactionId));
  NodeAssert.equal(receipt.dismissed, true);
  return Object.freeze(receipt);
}

// Actual execution is Darwin-only and belongs to a separately admitted native case.
// Every fixture and process is retained for the caller's disposition on failure.
export async function exercisePackagedDesktopStartupRefusal({
  pair,
  home,
  profile,
  temporary,
  protectedPaths,
  dialogObserver,
  mutationObserver,
  code,
  transactionId = null,
  launch = "active",
  signal,
}) {
  assertDarwin();
  NodeAssert.match(code, /^[a-z0-9-]{1,80}$/);
  if (transactionId !== null) NodeAssert.match(transactionId, /^[a-f0-9]{64}$/);
  NodeAssert.ok(["active", "candidate"].includes(launch));
  NodeAssert.ok(protectedPaths.length > 0 && protectedPaths.length <= 32);
  for (const directory of [home, profile, temporary])
    await confined(pair.fixtureRoot, directory, true);
  for (const filename of protectedPaths) {
    NodeAssert.equal(NodePath.resolve(filename), filename);
    NodeAssert.ok(
      filename.startsWith(home + NodePath.sep) ||
        filename === profile ||
        filename.startsWith(profile + NodePath.sep),
    );
  }
  for (const observer of [dialogObserver, mutationObserver]) {
    await confined(pair.fixtureRoot, observer.executable);
    NodeAssert.match(observer.sha256, shaPattern);
    NodeAssert.equal(await fileHash(observer.executable), observer.sha256);
  }
  await pair.assertUnchanged();
  const before = await Promise.all(protectedPaths.map(snapshotTree));
  const env = {
    PATH: "/usr/bin:/bin",
    HOME: pair.fixtureRoot,
    CFFIXED_USER_HOME: pair.fixtureRoot,
    XDG_CONFIG_HOME: NodePath.join(pair.fixtureRoot, "config"),
    XDG_CACHE_HOME: NodePath.join(pair.fixtureRoot, "cache"),
    TMPDIR: temporary,
    TMP: temporary,
    TEMP: temporary,
    T3CODE_HOME: home,
    T3CODE_DESKTOP_USER_DATA_DIR: profile,
    NODE_DISABLE_COMPILE_CACHE: "1",
  };
  const sessionBefore = await assertPackagedDesktopGuiSession({
    fixtureRoot: pair.fixtureRoot,
    signal,
  });
  const observer = await startObserver(mutationObserver.executable, [home, profile], signal);
  let result;
  let dialog;
  let capturedPid;
  let primaryFailure;
  try {
    await observerControl(observer, [home, profile]);
    await observer.flush();
    const start = observer.events.length;
    const selected = pair[launch];
    result = await runOwnedChild(selected.executablePath, [], {
      cwd: pair.fixtureRoot,
      env,
      signal,
      timeout: 30000,
      async onSpawn(pid) {
        capturedPid = pid;
        const observed = await runOwnedChild(
          dialogObserver.executable,
          [String(pid), selected.executablePath, code, transactionId ?? "-", pair.fixtureRoot],
          { cwd: pair.fixtureRoot, env, signal, timeout: 20000 },
        );
        NodeAssert.equal(observed.code, 0, observed.stderr || observed.stdout);
        NodeAssert.equal(observed.signal, null);
        dialog = validatePackagedDesktopDialog(JSON.parse(observed.stdout), {
          pid,
          executable: selected.executablePath,
          code,
          transactionId,
        });
      },
    });
    NodeAssert.equal(result.code, 1, result.stderr);
    NodeAssert.equal(result.signal, null);
    NodeAssert.match(result.stderr, /Jones Code refused native startup:/);
    NodeAssert.ok(dialog);
    await observer.flush();
    NodeAssert.deepEqual(protectedMutations(observer.events.slice(start), protectedPaths), []);
    NodeAssert.deepEqual(await Promise.all(protectedPaths.map(snapshotTree)), before);
    await pair.assertUnchanged();
  } catch (cause) {
    primaryFailure = cause;
    throw cause;
  } finally {
    try {
      await observer.close();
    } catch (cause) {
      if (primaryFailure)
        throw new AggregateError(
          [primaryFailure, cause],
          "Startup refusal and observer closure failed.",
        );
      throw cause;
    }
  }
  return Object.freeze({
    protocol: 1,
    case: "packaged-desktop-startup-refusal",
    guiSession: Object.freeze({
      lockRequirement: "unlocked",
      before: sessionBefore,
      after: await assertPackagedDesktopGuiSession({ fixtureRoot: pair.fixtureRoot, signal }),
    }),
    pid: capturedPid,
    dialog,
    result,
    protectedStateUnchanged: true,
    appPairUnchanged: true,
    identityScope: "captured-process-group-only",
    externalDescendantSettlementRequired: true,
    packagedDesktopMatrixPassed: false,
  });
}

async function privateFixtureBytes(root, filename, maximum = 1024 * 1024) {
  await confined(root, filename);
  for (let current = NodePath.dirname(filename); ; current = NodePath.dirname(current)) {
    const info = await confined(root, current, true);
    NodeAssert.equal(info.mode & 0o077, 0);
    if (current === root) break;
  }
  const handle = await NodeFSP.open(
    filename,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  );
  try {
    const before = await handle.stat({ bigint: true });
    NodeAssert.ok(before.isFile() && before.nlink === 1n);
    NodeAssert.equal(before.mode & 0o077n, 0n);
    NodeAssert.ok(before.size <= BigInt(maximum));
    const bytes = await handle.readFile();
    const after = await handle.stat({ bigint: true });
    for (const key of ["dev", "ino", "size", "mtimeNs", "ctimeNs"])
      NodeAssert.equal(after[key], before[key]);
    NodeAssert.equal(bytes.length, Number(before.size));
    return bytes;
  } finally {
    await handle.close();
  }
}

async function requireAbsent(filename) {
  await NodeAssert.rejects(NodeFSP.lstat(filename), { code: "ENOENT" });
}

// Consumes the normal preparer's existing receipts; it never creates claims or state.
export async function bindPackagedDesktopPreparedIntent({
  pair,
  helper,
  manifest,
  intent,
  interruptedPhase = null,
}) {
  NodeAssert.ok(
    interruptedPhase === null ||
      ["intent-only", "intent", "preparing", "quiescent"].includes(interruptedPhase),
  );
  await pair.assertUnchanged();
  const manifestBytes = await privateFixtureBytes(pair.fixtureRoot, manifest, 65536);
  const intentBytes = await privateFixtureBytes(pair.fixtureRoot, intent, 65536);
  const active = JSON.parse(manifestBytes);
  const activation = JSON.parse(intentBytes);
  NodeAssert.equal(active.protocol, 1);
  NodeAssert.equal(active.owner, "desktop");
  NodeAssert.equal(activation.protocol, 1);
  NodeAssert.match(activation.transactionId, shaPattern);
  NodeAssert.deepEqual(activation.expected, active);
  for (const key of [
    "appPath",
    "executablePath",
    "sourceSha",
    "sourceTree",
    "version",
    "appDigest",
  ]) {
    NodeAssert.equal(active[key], pair.active[key]);
    NodeAssert.equal(activation.staged[key], pair.candidate[key]);
  }
  for (const name of ["home", "profile"]) {
    const info = await confined(pair.fixtureRoot, active[name], true);
    NodeAssert.equal(info.mode & 0o077, 0);
  }
  NodeAssert.ok(active.databasePath.startsWith(active.home + NodePath.sep));
  await privateFixtureBytes(pair.fixtureRoot, active.databasePath, 1024 * 1024);
  NodeAssert.equal(manifest, NodePath.join(active.home, "runtime/jones-active-install.json"));
  const transaction = NodePath.join(
    active.home,
    "runtime/jones-updates/transactions",
    activation.transactionId,
  );
  NodeAssert.equal(intent, NodePath.join(transaction, "intent.json"));
  NodeAssert.equal(NodePath.dirname(helper), NodePath.join(active.home, "runtime/jones-updates"));
  const helperBytes = await privateFixtureBytes(pair.fixtureRoot, helper, 2 * 1024 * 1024);
  NodeAssert.equal(NodePath.basename(helper), "activate-v1-" + sha(helperBytes) + ".py");
  const continuationBytes = await privateFixtureBytes(
    pair.fixtureRoot,
    activation.continuationReceipt,
    65536,
  );
  const continuation = JSON.parse(continuationBytes);
  NodeAssert.equal(continuation.protocol, 1);
  NodeAssert.ok([2, 3].includes(continuation.preparationClaimProtocol));
  NodeAssert.equal(continuation.prepared, true);
  NodeAssert.equal(continuation.transactionId, activation.transactionId);
  NodeAssert.equal(continuation.stagedHandle, activation.staged.handle);
  for (const key of ["home", "profile", "databasePath", "environmentId"])
    NodeAssert.equal(continuation[key], active[key]);
  const { prepared, ...claim } = continuation;
  NodeAssert.equal(prepared, true);
  for (const name of ["prepare-intent.json", "prepare-dispatched.json"])
    NodeAssert.deepEqual(
      JSON.parse(
        await privateFixtureBytes(pair.fixtureRoot, NodePath.join(transaction, name), 65536),
      ),
      claim,
    );
  const selectionBytes = await privateFixtureBytes(
    pair.fixtureRoot,
    continuation.selectionPath,
    131072,
  );
  NodeAssert.equal(sha(selectionBytes), continuation.selectionSha256);
  const selection = JSON.parse(selectionBytes);
  NodeAssert.deepEqual(selection.active, active);
  NodeAssert.deepEqual(selection.app, activation.staged);
  let initialJournalBytes = null;
  if (interruptedPhase === null || interruptedPhase === "intent-only") {
    await requireAbsent(NodePath.join(transaction, "journal.json"));
  } else {
    initialJournalBytes = await privateFixtureBytes(
      pair.fixtureRoot,
      NodePath.join(transaction, "journal.json"),
    );
    const journal = JSON.parse(initialJournalBytes);
    NodeAssert.equal(journal.phase, interruptedPhase);
    NodeAssert.deepEqual(journal.intent, activation);
    const allowed = new Set([
      "intent",
      "phase",
      "message",
      "storage",
      "recoveryReserves",
      "incomingBundle",
      "previousBundle",
    ]);
    NodeAssert.ok(Object.keys(journal).every((key) => allowed.has(key)));
  }
  for (const name of ["previous-pair/pair.json", "trial-descriptor.json", "resume-dispatched.json"])
    await requireAbsent(NodePath.join(transaction, name));
  return Object.freeze({
    active,
    activation,
    transaction,
    initialJournal: initialJournalBytes === null ? null : JSON.parse(initialJournalBytes),
    initialJournalSha256: initialJournalBytes === null ? null : sha(initialJournalBytes),
    manifestSha256: sha(manifestBytes),
    intentSha256: sha(intentBytes),
    helperSha256: sha(helperBytes),
    async assertInputsUnchanged() {
      NodeAssert.equal(
        sha(await privateFixtureBytes(pair.fixtureRoot, manifest, 65536)),
        sha(manifestBytes),
      );
      NodeAssert.equal(
        sha(await privateFixtureBytes(pair.fixtureRoot, intent, 65536)),
        sha(intentBytes),
      );
      NodeAssert.equal(
        sha(await privateFixtureBytes(pair.fixtureRoot, helper, 2 * 1024 * 1024)),
        sha(helperBytes),
      );
      NodeAssert.equal(
        sha(await privateFixtureBytes(pair.fixtureRoot, activation.continuationReceipt, 65536)),
        sha(continuationBytes),
      );
      NodeAssert.equal(
        sha(await privateFixtureBytes(pair.fixtureRoot, continuation.selectionPath, 131072)),
        sha(selectionBytes),
      );
      await pair.assertUnchanged();
    },
  });
}

export function validatePackagedDesktopPreEffect(journal, { activation, fault, expectedRelaunch }) {
  NodeAssert.ok(["started", "skipped"].includes(expectedRelaunch));
  NodeAssert.equal(journal.phase, "rolled-back");
  NodeAssert.deepEqual(journal.intent, activation);
  NodeAssert.equal(fault.transactionId, activation.transactionId);
  const boundary = {
    storage: ["intent", "storage"],
    reserve: ["preparing", "reserve"],
    "exit-wait": ["preparing", "stop-processes"],
    "before-pair": ["quiescent", "pair"],
  }[fault.point];
  NodeAssert.ok(boundary);
  NodeAssert.deepEqual([fault.phase, fault.step], boundary);
  NodeAssert.deepEqual([journal.preEffect?.phase, journal.preEffect?.step], boundary);
  NodeAssert.equal(journal.preEffect.reason, "preparation-failed");
  NodeAssert.equal(journal.preEffect.relaunch, expectedRelaunch);
  NodeAssert.equal(journal.preEffect.diagnostics?.errno, 5);
  NodeAssert.equal(
    journal.relaunch,
    undefined,
    "A separate failed reopen must not count as recovery.",
  );
  NodeAssert.equal(journal.phaseBeforeBlock, undefined);
  return journal;
}

// LaunchServices may reopen the previous app outside the helper's process group.
// The native caller must retain and settle those owned processes separately.
export async function exercisePackagedDesktopPreparationRefusal({
  pair,
  helper,
  manifest,
  intent,
  temporary,
  point,
  expectedRelaunch,
  signal,
}) {
  assertDarwin();
  NodeAssert.ok(["started", "skipped"].includes(expectedRelaunch));
  await confined(pair.fixtureRoot, temporary, true);
  const binding = await bindPackagedDesktopPreparedIntent({ pair, helper, manifest, intent });
  const sessionBefore = await assertPackagedDesktopGuiSession({
    fixtureRoot: pair.fixtureRoot,
    signal,
    lock: "any",
  });
  const fault = await preparePackagedDesktopHelperFault({
    fixtureRoot: pair.fixtureRoot,
    helper,
    manifest,
    intent,
    point,
    observeRelaunch: true,
  });
  await binding.assertInputsUnchanged();
  let pid;
  const execution = await runOwnedChild("/usr/bin/python3", fault.argv, {
    cwd: pair.fixtureRoot,
    env: {
      PATH: "/usr/bin:/bin",
      HOME: pair.fixtureRoot,
      CFFIXED_USER_HOME: pair.fixtureRoot,
      TMPDIR: temporary,
      TMP: temporary,
      TEMP: temporary,
      XDG_CONFIG_HOME: NodePath.join(pair.fixtureRoot, "config"),
      XDG_CACHE_HOME: NodePath.join(pair.fixtureRoot, "cache"),
      ...fault.environment,
    },
    signal,
    timeout: 30000,
    onSpawn(captured) {
      pid = captured;
    },
  });
  NodeAssert.equal(execution.signal, null, execution.stderr);
  NodeAssert.equal(execution.code, 0, execution.stderr);
  const receipt = await fault.readReceipt();
  NodeAssert.equal(receipt.pid, pid);
  const journalBytes = await privateFixtureBytes(
    pair.fixtureRoot,
    NodePath.join(binding.transaction, "journal.json"),
  );
  const journal = validatePackagedDesktopPreEffect(JSON.parse(journalBytes), {
    activation: binding.activation,
    fault: receipt,
    expectedRelaunch,
  });
  for (const name of [
    "previous-pair/pair.json",
    "trial-descriptor.json",
    "commit-grant.json",
    "resume-dispatched.json",
  ])
    await requireAbsent(NodePath.join(binding.transaction, name));
  await binding.assertInputsUnchanged();
  const beforeRelaunch = expectedRelaunch === "started" ? await fault.readRelaunchReceipt() : null;
  if (expectedRelaunch === "skipped") await fault.assertNoRelaunch();
  return Object.freeze({
    guiSession: Object.freeze({
      lockRequirement: "any",
      before: sessionBefore,
      after: await assertPackagedDesktopGuiSession({
        fixtureRoot: pair.fixtureRoot,
        signal,
        lock: "any",
      }),
    }),
    beforeRelaunch,
    point,
    transactionId: binding.activation.transactionId,
    execution,
    receipt,
    journal,
    journalSha256: sha(journalBytes),
    manifestSha256: binding.manifestSha256,
    intentSha256: binding.intentSha256,
    helperSha256: binding.helperSha256,
    identityScope: "captured-helper-process-group-only",
    externalDescendantSettlementRequired: true,
    packagedHelperProvenanceRequired: true,
    packagedDesktopMatrixPassed: false,
  });
}

export function validatePackagedDesktopMainWindow(receipt, { pid, executable }) {
  NodeAssert.equal(receipt.protocol, 1);
  NodeAssert.equal(receipt.kind, "packaged-desktop-main-window");
  NodeAssert.equal(receipt.pid, pid);
  NodeAssert.equal(receipt.executable, executable);
  NodeAssert.ok(Number.isSafeInteger(receipt.startSec) && receipt.startSec > 0);
  NodeAssert.ok(
    Number.isSafeInteger(receipt.startUsec) &&
      receipt.startUsec >= 0 &&
      receipt.startUsec < 1000000,
  );
  NodeAssert.equal(receipt.role, "AXWindow");
  NodeAssert.equal(receipt.subrole, "AXStandardWindow");
  NodeAssert.equal(receipt.main, true);
  NodeAssert.equal(receipt.minimized, false);
  NodeAssert.ok(typeof receipt.title === "string" && receipt.title.length <= 4096);
  NodeAssert.ok(Number.isFinite(receipt.width) && receipt.width >= 840 && receipt.width <= 32768);
  NodeAssert.ok(
    Number.isFinite(receipt.height) && receipt.height >= 620 && receipt.height <= 32768,
  );
  return Object.freeze(receipt);
}

export function validatePackagedDesktopStartupSettlement(
  journal,
  { activation, interruptedPhase },
) {
  NodeAssert.ok(["intent-only", "intent", "preparing", "quiescent"].includes(interruptedPhase));
  NodeAssert.equal(journal.phase, "rolled-back");
  NodeAssert.deepEqual(journal.intent, activation);
  NodeAssert.deepEqual(journal.legacySettlement, {
    basis: interruptedPhase === "intent-only" ? "unstarted-activation" : "interrupted-preparation",
    settledBy: "startup",
    evidence: {
      phase: interruptedPhase === "intent-only" ? "intent" : interruptedPhase,
      manifestUnchanged: true,
      pairAbsent: true,
      bundleUnswapped: true,
    },
  });
  const allowed = new Set([
    "intent",
    "phase",
    "message",
    "storage",
    "recoveryReserves",
    "incomingBundle",
    "previousBundle",
    "legacySettlement",
  ]);
  NodeAssert.ok(Object.keys(journal).every((key) => allowed.has(key)));
  return journal;
}

// Startup consumes an existing interrupted native preparation; no fixture state is invented.
// Native observers and escaped descendants still require the caller's retaining controller.
export async function exercisePackagedDesktopInterruptedStartup({
  pair,
  helper,
  manifest,
  intent,
  temporary,
  interruptedPhase,
  windowObserver,
  signal,
  lock = "unlocked",
}) {
  assertDarwin();
  NodeAssert.ok(["intent-only", "intent", "preparing", "quiescent"].includes(interruptedPhase));
  await confined(pair.fixtureRoot, temporary, true);
  await confined(pair.fixtureRoot, windowObserver.executable);
  NodeAssert.match(windowObserver.sha256, shaPattern);
  NodeAssert.equal(await fileHash(windowObserver.executable), windowObserver.sha256);
  const binding = await bindPackagedDesktopPreparedIntent({
    pair,
    helper,
    manifest,
    intent,
    interruptedPhase,
  });
  let capturedPid;
  let observation;
  let settlement;
  let journalSha256;
  let stopRequested = false;
  const sessionBefore = await assertPackagedDesktopGuiSession({
    fixtureRoot: pair.fixtureRoot,
    signal,
    lock,
  });
  const execution = await runOwnedChild(pair.active.executablePath, [], {
    cwd: pair.fixtureRoot,
    env: {
      PATH: "/usr/bin:/bin",
      HOME: pair.fixtureRoot,
      CFFIXED_USER_HOME: pair.fixtureRoot,
      TMPDIR: temporary,
      TMP: temporary,
      TEMP: temporary,
      XDG_CONFIG_HOME: NodePath.join(pair.fixtureRoot, "config"),
      XDG_CACHE_HOME: NodePath.join(pair.fixtureRoot, "cache"),
      T3CODE_HOME: binding.active.home,
      T3CODE_DESKTOP_USER_DATA_DIR: binding.active.profile,
      T3CODE_JONES_ACTIVE_MANIFEST: manifest,
      T3CODE_JONES_ACTIVE_GENERATION: binding.active.generation,
      NODE_DISABLE_COMPILE_CACHE: "1",
    },
    signal,
    timeout: 30000,
    async onSpawn(pid, { requestStop }) {
      capturedPid = pid;
      const observed = await runOwnedChild(
        windowObserver.executable,
        ["--main-window", String(pid), pair.active.executablePath, pair.fixtureRoot],
        {
          cwd: pair.fixtureRoot,
          env: { PATH: "/usr/bin:/bin", HOME: pair.fixtureRoot, TMPDIR: temporary },
          signal,
          timeout: 20000,
        },
      );
      NodeAssert.equal(observed.signal, null, observed.stderr);
      NodeAssert.equal(observed.code, 0, observed.stderr || observed.stdout);
      observation = validatePackagedDesktopMainWindow(JSON.parse(observed.stdout), {
        pid,
        executable: pair.active.executablePath,
      });
      const journalBytes = await privateFixtureBytes(
        pair.fixtureRoot,
        NodePath.join(binding.transaction, "journal.json"),
      );
      settlement = validatePackagedDesktopStartupSettlement(JSON.parse(journalBytes), {
        activation: binding.activation,
        interruptedPhase,
      });
      journalSha256 = sha(journalBytes);
      for (const name of [
        "previous-pair/pair.json",
        "trial-descriptor.json",
        "commit-grant.json",
        "resume-dispatched.json",
      ])
        await requireAbsent(NodePath.join(binding.transaction, name));
      await binding.assertInputsUnchanged();
      stopRequested = requestStop();
      NodeAssert.equal(
        stopRequested,
        true,
        "The captured app stopped before its verified fixture shutdown.",
      );
    },
  });
  NodeAssert.equal(stopRequested, true);
  NodeAssert.ok(
    execution.signal === "SIGTERM" || (execution.signal === null && execution.code === 0),
    execution.stderr,
  );
  NodeAssert.ok(!execution.stderr.includes("Jones Code refused native startup:"));
  await binding.assertInputsUnchanged();
  const finalJournal = await privateFixtureBytes(
    pair.fixtureRoot,
    NodePath.join(binding.transaction, "journal.json"),
  );
  NodeAssert.equal(sha(finalJournal), journalSha256);
  return Object.freeze({
    guiSession: Object.freeze({
      lockRequirement: lock,
      before: sessionBefore,
      after: await assertPackagedDesktopGuiSession({ fixtureRoot: pair.fixtureRoot, signal, lock }),
    }),
    interruptedPhase,
    transactionId: binding.activation.transactionId,
    capturedPid,
    observation,
    settlement,
    initialJournal: binding.initialJournal,
    initialJournalSha256: binding.initialJournalSha256,
    journalSha256,
    execution,
    stopRequested,
    identityScope: "captured-process-group-only",
    externalDescendantSettlementRequired: true,
    packagedHelperProvenanceRequired: true,
    packagedDesktopMatrixPassed: false,
  });
}

const packagedIdentityOwner = String.raw`
import os, sys, json, stat, hashlib, subprocess, select, resource, signal
root, helper, helper_sha = sys.argv[1:]
assert sys.platform == 'darwin'
assert os.path.realpath(root) == root and os.path.realpath(helper) == helper
assert os.stat(root).st_uid == os.getuid() and os.stat(root).st_mode & 0o077 == 0
python = os.path.realpath(sys.executable)
with open(python, 'rb') as stream: python_sha = hashlib.sha256(stream.read()).hexdigest()
resource.setrlimit(resource.RLIMIT_FSIZE, (8192, 8192))
def cancelled(signum, frame):
    raise InterruptedError('Owned identity fixture cancelled')
signal.signal(signal.SIGTERM, cancelled)
signal.signal(signal.SIGINT, cancelled)
signal.signal(signal.SIGALRM, cancelled)
signal.alarm(18)
def verify_helper():
    fd = os.open(helper, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        assert stat.S_ISREG(before.st_mode) and before.st_uid == os.getuid()
        assert before.st_nlink == 1 and before.st_mode & 0o077 == 0 and before.st_size <= 2 * 1024**2
        data = os.read(fd, before.st_size + 1)
        after = os.fstat(fd)
        assert len(data) == before.st_size and hashlib.sha256(data).hexdigest() == helper_sha
        for field in ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns'):
            assert getattr(before, field) == getattr(after, field)
    finally:
        os.close(fd)
def query(label, pid):
    verify_helper()
    argv = [python, '-I', '-S', '-B', helper, '--process-identity', str(pid)]
    paths = [os.path.join(root, label + suffix) for suffix in ('.stdout', '.stderr')]
    with open(paths[0], 'xb') as out, open(paths[1], 'xb') as err:
        os.chmod(paths[0], 0o600); os.chmod(paths[1], 0o600)
        result = subprocess.run(argv, stdin=subprocess.DEVNULL, stdout=out, stderr=err,
            cwd=root, close_fds=True, timeout=3, check=False)
    streams = []
    for path in paths:
        assert os.stat(path).st_size <= 8192
        with open(path, 'rb') as stream: streams.append(stream.read().decode('utf8', errors='strict'))
    return {'argv':argv, 'code':result.returncode, 'stdout':streams[0], 'stderr':streams[1]}
owner_image = query('owner-image', os.getpid())
assert owner_image['code'] == 0 and owner_image['stderr'] == ''
kernel_python = json.loads(owner_image['stdout'])['executable']
assert os.path.isabs(kernel_python) and os.path.realpath(kernel_python) == kernel_python
with open(kernel_python, 'rb') as stream: kernel_python_sha = hashlib.sha256(stream.read()).hexdigest()
read_fd, write_fd = os.pipe()
pid = os.fork()
if pid == 0:
    os.close(write_fd)
    try: result = 0 if os.read(read_fd, 1) == b'x' else 125
    except BaseException: result = 125
    os._exit(result)
os.close(read_fd)
queue = None
reaped = False
try:
    queue = select.kqueue()
    queue.control([select.kevent(pid, filter=select.KQ_FILTER_PROC,
        flags=select.KQ_EV_ADD | select.KQ_EV_ONESHOT,
        fflags=select.KQ_NOTE_EXIT)], 0, 0)
    first = query('live-first', pid)
    second = query('live-second', pid)
    os.write(write_fd, b'x')
    os.close(write_fd); write_fd = None
    events = queue.control(None, 1, 3)
    assert len(events) == 1
    event = events[0]
    assert event.ident == pid and event.filter == select.KQ_FILTER_PROC
    assert not event.flags & select.KQ_EV_ERROR and event.fflags & select.KQ_NOTE_EXIT
    # No wait/poll has reaped this direct child. NOTE_EXIT precedes zombie inspection.
    zombie = query('zombie', pid)
    waited_pid, status = os.waitpid(pid, 0)
    reaped = True
    assert waited_pid == pid and os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
    after_reap = query('reaped', pid)
    verify_helper()
    print(json.dumps({'protocol':1, 'case':'packaged-desktop-process-identity',
        'ownerPid':os.getpid(), 'capturedPid':pid, 'helperSha256':helper_sha,
        'python':{'executable':python, 'sha256':python_sha, 'kernelExecutable':kernel_python, 'kernelSha256':kernel_python_sha},
        'ownerIdentity':owner_image,
        'commands':[first, second, zombie, after_reap],
        'exitEvent':{'pid':event.ident,'filter':event.filter,'flags':event.flags,'fflags':event.fflags},
        'wait':{'pid':waited_pid,'rawStatus':status,'exitCode':os.WEXITSTATUS(status)}}))
finally:
    if write_fd is not None: os.close(write_fd)
    if not reaped:
        # The pipe releases only this forked child; no PID signal or process scan.
        os.waitpid(pid, 0)
    if queue is not None: queue.close()
    signal.alarm(0)
`;

// ESRCH is required here for the specific packaged L-f row; other native errors stay failures.
export function validatePackagedDesktopProcessIdentity(
  receipt,
  { ownerPid, helper, helperSha256 },
) {
  NodeAssert.equal(receipt.protocol, 1);
  NodeAssert.equal(receipt.case, "packaged-desktop-process-identity");
  NodeAssert.equal(receipt.ownerPid, ownerPid);
  NodeAssert.ok(
    Number.isSafeInteger(receipt.capturedPid) &&
      receipt.capturedPid > 0 &&
      receipt.capturedPid !== ownerPid,
  );
  NodeAssert.equal(receipt.helperSha256, helperSha256);
  NodeAssert.ok(NodePath.isAbsolute(receipt.python.executable));
  NodeAssert.match(receipt.python.sha256, shaPattern);
  NodeAssert.ok(NodePath.isAbsolute(receipt.python.kernelExecutable));
  NodeAssert.match(receipt.python.kernelSha256, shaPattern);
  const owner = receipt.ownerIdentity;
  NodeAssert.deepEqual(owner.argv, [
    receipt.python.executable,
    "-I",
    "-S",
    "-B",
    helper,
    "--process-identity",
    String(ownerPid),
  ]);
  NodeAssert.equal(owner.code, 0);
  NodeAssert.equal(owner.stderr, "");
  NodeAssert.ok(typeof owner.stdout === "string" && Buffer.byteLength(owner.stdout) <= 8192);
  const ownerImage = JSON.parse(owner.stdout);
  NodeAssert.deepEqual(Object.keys(ownerImage).sort(), [
    "executable",
    "pid",
    "startSec",
    "startUsec",
  ]);
  NodeAssert.equal(ownerImage.pid, ownerPid);
  NodeAssert.equal(ownerImage.executable, receipt.python.kernelExecutable);
  NodeAssert.ok(Number.isSafeInteger(ownerImage.startSec) && ownerImage.startSec > 0);
  NodeAssert.ok(
    Number.isSafeInteger(ownerImage.startUsec) &&
      ownerImage.startUsec >= 0 &&
      ownerImage.startUsec < 1000000,
  );
  NodeAssert.equal(receipt.commands.length, 4);
  const [first, second, zombie, reaped] = receipt.commands;
  for (const result of receipt.commands) {
    NodeAssert.deepEqual(result.argv, [
      receipt.python.executable,
      "-I",
      "-S",
      "-B",
      helper,
      "--process-identity",
      String(receipt.capturedPid),
    ]);
    NodeAssert.ok(typeof result.stdout === "string" && Buffer.byteLength(result.stdout) <= 8192);
    NodeAssert.ok(typeof result.stderr === "string" && Buffer.byteLength(result.stderr) <= 8192);
  }
  for (const result of [first, second]) {
    NodeAssert.equal(result.code, 0);
    NodeAssert.equal(result.stderr, "");
    const identity = JSON.parse(result.stdout);
    NodeAssert.deepEqual(Object.keys(identity).sort(), [
      "executable",
      "pid",
      "startSec",
      "startUsec",
    ]);
    NodeAssert.equal(identity.pid, receipt.capturedPid);
    NodeAssert.equal(identity.executable, receipt.python.kernelExecutable);
    NodeAssert.ok(Number.isSafeInteger(identity.startSec) && identity.startSec > 0);
    NodeAssert.ok(
      Number.isSafeInteger(identity.startUsec) &&
        identity.startUsec >= 0 &&
        identity.startUsec < 1000000,
    );
  }
  NodeAssert.deepEqual(JSON.parse(first.stdout), JSON.parse(second.stdout));
  NodeAssert.equal(receipt.exitEvent.pid, receipt.capturedPid);
  NodeAssert.equal(receipt.exitEvent.filter, -5);
  NodeAssert.equal(receipt.exitEvent.flags & 0x4000, 0);
  NodeAssert.notEqual(receipt.exitEvent.fflags & 0x80000000, 0);
  for (const result of [zombie, reaped]) {
    NodeAssert.equal(result.code, 1);
    NodeAssert.equal(result.stdout, "");
    const diagnostic = JSON.parse(result.stderr);
    NodeAssert.deepEqual(Object.keys(diagnostic).sort(), ["call", "errno", "error"]);
    NodeAssert.equal(diagnostic.error, "process-exited");
    NodeAssert.equal(diagnostic.errno, 3);
    NodeAssert.ok(["proc_pidinfo", "proc_pidpath"].includes(diagnostic.call));
  }
  NodeAssert.deepEqual(receipt.wait, { pid: receipt.capturedPid, rawStatus: 0, exitCode: 0 });
  return Object.freeze(receipt);
}

// Uses the unchanged, content-named packaged helper's existing read-only CLI.
// The creating native controller owns artifact provenance and any escaped descendants.
export async function exercisePackagedDesktopProcessIdentity({ pair, helper, temporary, signal }) {
  assertDarwin();
  await pair.assertUnchanged();
  await confined(pair.fixtureRoot, temporary, true);
  NodeAssert.equal((await NodeFSP.lstat(temporary)).mode & 0o077, 0);
  const helperBytes = await privateFixtureBytes(pair.fixtureRoot, helper, 2 * 1024 * 1024);
  const helperSha256 = sha(helperBytes);
  NodeAssert.equal(NodePath.basename(helper), "activate-v1-" + helperSha256 + ".py");
  const root = NodePath.join(temporary, "packaged-process-identity-" + NodeCrypto.randomUUID());
  await NodeFSP.mkdir(root, { mode: 0o700 });
  let ownerPid;
  const execution = await runOwnedChild(
    "/usr/bin/python3",
    ["-I", "-S", "-B", "-c", packagedIdentityOwner, root, helper, helperSha256],
    {
      cwd: root,
      signal,
      timeout: 25000,
      env: { PATH: "/usr/bin:/bin", HOME: pair.fixtureRoot, TMPDIR: root },
      onSpawn(pid) {
        ownerPid = pid;
      },
    },
  );
  NodeAssert.equal(execution.signal, null, execution.stderr);
  NodeAssert.equal(execution.code, 0, execution.stderr);
  NodeAssert.equal(execution.stderr, "");
  const observation = validatePackagedDesktopProcessIdentity(JSON.parse(execution.stdout), {
    ownerPid,
    helper,
    helperSha256,
  });
  NodeAssert.equal(
    sha(await privateFixtureBytes(pair.fixtureRoot, helper, 2 * 1024 * 1024)),
    helperSha256,
  );
  await pair.assertUnchanged();
  return Object.freeze({
    root,
    observation,
    execution,
    helperSha256,
    identityScope: "captured-process-group-only",
    externalDescendantSettlementRequired: true,
    packagedHelperProvenanceRequired: true,
    packagedDesktopMatrixPassed: false,
  });
}
