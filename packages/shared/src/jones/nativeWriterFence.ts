// @effect-diagnostics nodeBuiltinImport:off -- Synchronous native admission precedes the desktop and server runtimes.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import type * as NodeSqlite from "node:sqlite";
import * as NodeUtil from "node:util";

export type JonesNativeStartupRefusalCode =
  | "transaction-unsettled"
  | "transaction-settleable"
  | "unstarted-activation"
  | "trial-permit-mismatch"
  | "generation-changed"
  | "ownership-unknown"
  | "paths-differ"
  | "lease-lost"
  | "writer-mismatch"
  | "foreign-state";

export class JonesNativeStartupRefusal extends Error {
  override readonly name = "JonesNativeStartupRefusal";
  readonly code: JonesNativeStartupRefusalCode;
  readonly transactionId: string | undefined;
  constructor(
    code: JonesNativeStartupRefusalCode,
    message: string,
    transactionId?: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.code = code;
    this.transactionId = transactionId;
  }
}

export function asJonesNativeStartupRefusal(
  cause: unknown,
  fallback: JonesNativeStartupRefusalCode = "ownership-unknown",
  transactionId?: string,
): JonesNativeStartupRefusal {
  if (cause instanceof JonesNativeStartupRefusal) {
    if (cause.transactionId !== undefined || transactionId === undefined) return cause;
    return new JonesNativeStartupRefusal(cause.code, cause.message, transactionId, { cause });
  }
  return new JonesNativeStartupRefusal(
    fallback,
    cause instanceof Error ? cause.message : "Unknown admission failure",
    transactionId,
    { cause },
  );
}

type JsonRecord = Record<string, unknown>;
interface Lease {
  readonly database: NodeSqlite.DatabaseSync;
  readonly device: string;
  readonly inode: string;
  readonly scope: string;
}

const registryKey = Symbol.for("jones.native-writer-leases.v1");
const registry = globalThis as unknown as { [key: symbol]: Map<string, Lease> | undefined };
const leases = (registry[registryKey] ??= new Map());

function record(value: unknown): JsonRecord {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Native startup metadata is not an object; startup held.");
  return value as JsonRecord;
}

function exists(path: string): boolean {
  try {
    NodeFS.lstatSync(path);
    return true;
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw cause;
  }
}

function readJson(path: string): JsonRecord {
  const fd = NodeFS.openSync(path, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    const info = NodeFS.fstatSync(fd);
    if (!info.isFile() || info.uid !== process.getuid?.() || info.size > 1024 * 1024)
      throw new JonesNativeStartupRefusal(
        "ownership-unknown",
        "Native startup metadata is not a bounded regular file; startup held.",
      );
    const bytes = Buffer.alloc(info.size + 1);
    let length = 0;
    while (length < bytes.length) {
      const count = NodeFS.readSync(fd, bytes, length, bytes.length - length, null);
      if (count === 0) break;
      length += count;
    }
    if (length > info.size)
      throw new JonesNativeStartupRefusal(
        "ownership-unknown",
        "Native startup metadata exceeds its bound; startup held.",
      );
    return record(JSON.parse(bytes.subarray(0, length).toString("utf8")));
  } finally {
    NodeFS.closeSync(fd);
  }
}

function syncParent(path: string): void {
  const fd = NodeFS.openSync(NodePath.dirname(path), "r");
  try {
    NodeFS.fsyncSync(fd);
  } finally {
    NodeFS.closeSync(fd);
  }
}

function publishExclusive(path: string, value: JsonRecord): void {
  const scratch = `${path}.${NodeCrypto.randomUUID()}.pending`;
  const fd = NodeFS.openSync(scratch, "wx", 0o600);
  try {
    try {
      NodeFS.writeFileSync(fd, JSON.stringify(value) + "\n");
      NodeFS.fsyncSync(fd);
    } finally {
      NodeFS.closeSync(fd);
    }
    NodeFS.linkSync(scratch, path);
    syncParent(path);
  } finally {
    NodeFS.unlinkSync(scratch);
  }
}

function sqlite() {
  const builtin = process.getBuiltinModule?.("node:sqlite") as
    | typeof import("node:sqlite")
    | undefined;
  if (builtin === undefined)
    throw new Error("This native runtime cannot hold the required writer lease; startup held.");
  return builtin;
}

function leaseIdentity(path: string, scope: string) {
  // Do not open/read/hash the lease file outside SQLite. Closing any other fd for
  // this inode can release every POSIX SQLite lock held by the current process.
  const info = NodeFS.lstatSync(path, { bigint: true });
  const witness = readJson(`${path}.identity.json`);
  if (
    !info.isFile() ||
    info.isSymbolicLink() ||
    info.nlink !== 1n ||
    info.uid !== BigInt(process.getuid?.() ?? -1) ||
    (info.mode & 0o077n) !== 0n ||
    witness.protocol !== 1 ||
    witness.scope !== scope ||
    witness.device !== String(info.dev) ||
    witness.inode !== String(info.ino)
  )
    throw new JonesNativeStartupRefusal(
      "lease-lost",
      "The stable native lease inode changed; reconciliation is required.",
    );
  return { device: String(info.dev), inode: String(info.ino) };
}

function initializeLease(path: string, scope: string): void {
  if (exists(path)) return;
  if (exists(`${path}.identity.json`))
    throw new JonesNativeStartupRefusal(
      "lease-lost",
      "A native lease disappeared; reconciliation is required.",
    );
  NodeFS.mkdirSync(NodePath.dirname(path), { recursive: true, mode: 0o700 });
  const scratch = `${path}.${NodeCrypto.randomUUID()}.pending`;
  NodeFS.closeSync(NodeFS.openSync(scratch, "wx", 0o600));
  try {
    const database = new (sqlite().DatabaseSync)(scratch);
    try {
      database.exec("PRAGMA journal_mode = DELETE; PRAGMA synchronous = FULL;");
      database.exec("CREATE TABLE jones_native_writer_lease (protocol INTEGER, scope TEXT);");
      database.prepare("INSERT INTO jones_native_writer_lease VALUES (1, ?)").run(scope);
    } finally {
      database.close();
    }
    const fd = NodeFS.openSync(scratch, "r");
    try {
      NodeFS.fsyncSync(fd);
    } finally {
      NodeFS.closeSync(fd);
    }
    try {
      NodeFS.linkSync(scratch, path);
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException).code === "EEXIST") return;
      throw cause;
    }
    NodeFS.unlinkSync(scratch);
    syncParent(path);
    const info = NodeFS.lstatSync(path, { bigint: true });
    publishExclusive(`${path}.identity.json`, {
      protocol: 1,
      scope,
      device: String(info.dev),
      inode: String(info.ino),
    });
  } finally {
    if (exists(scratch)) NodeFS.unlinkSync(scratch);
  }
}

function holdLease(path: string, scope: string): void {
  initializeLease(path, scope);
  const identity = leaseIdentity(path, scope);
  const held = leases.get(path);
  if (held !== undefined) {
    if (held.scope !== scope || held.device !== identity.device || held.inode !== identity.inode)
      throw new JonesNativeStartupRefusal(
        "lease-lost",
        "A held native lease was replaced; startup held.",
      );
    return;
  }
  const database = new (sqlite().DatabaseSync)(path, { readOnly: true });
  try {
    if (database.prepare("PRAGMA journal_mode").get()?.journal_mode !== "delete")
      throw new JonesNativeStartupRefusal(
        "lease-lost",
        "Native writer leases require rollback journal mode; startup held.",
      );
    database.exec("PRAGMA busy_timeout = 0; BEGIN;");
    const rows = database.prepare("SELECT protocol, scope FROM jones_native_writer_lease").all();
    const current = leaseIdentity(path, scope);
    if (
      rows.length !== 1 ||
      rows[0]?.protocol !== 1 ||
      rows[0]?.scope !== scope ||
      current.device !== identity.device ||
      current.inode !== identity.inode
    )
      throw new JonesNativeStartupRefusal(
        "lease-lost",
        "Native writer lease identity is unknown; startup held.",
      );
    // Process lifetime is deliberate: failed SQL/profile shutdown must not
    // release this lease while another application-state connection survives.
    leases.set(path, { database, scope, ...identity });
  } catch (cause) {
    database.close();
    throw cause;
  }
}

function profileWriterLease(profile: string) {
  const profileHash = NodeCrypto.createHash("sha256").update(profile).digest("hex");
  return {
    path: NodePath.join(NodePath.dirname(profile), `.jones-profile-writer-${profileHash}.sqlite`),
    scope: `profile:${profile}`,
  };
}

export function nativeWriterLeasePaths(home: string, profile: string) {
  const canonicalHome = NodeFS.realpathSync(home);
  return [
    {
      path: NodePath.join(canonicalHome, "runtime", "jones-native-writer.sqlite"),
      scope: `home:${canonicalHome}`,
    },
    profileWriterLease(NodeFS.realpathSync(profile)),
  ].sort((left, right) => (left.path < right.path ? -1 : left.path > right.path ? 1 : 0));
}

function canonicalPotentialPath(path: string, symlinks = 0): string {
  if (symlinks > 40) throw new Error("Native state path contains a symlink loop.");
  let cursor = NodePath.resolve(path);
  const suffix: string[] = [];
  for (;;) {
    try {
      return NodePath.join(NodeFS.realpathSync(cursor), ...suffix);
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
    }
    try {
      if (NodeFS.lstatSync(cursor).isSymbolicLink()) {
        const target = NodePath.resolve(NodePath.dirname(cursor), NodeFS.readlinkSync(cursor));
        return NodePath.join(canonicalPotentialPath(target, symlinks + 1), ...suffix);
      }
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
    }
    const parent = NodePath.dirname(cursor);
    if (parent === cursor) throw new Error("Native state path cannot be resolved.");
    suffix.unshift(NodePath.basename(cursor));
    cursor = parent;
  }
}

function refuseForeignNativeState(home: string, databasePath: string, profile?: string): void {
  const homes = new Set([
    canonicalPotentialPath(home),
    NodePath.dirname(NodePath.dirname(canonicalPotentialPath(databasePath))),
  ]);
  for (const ownerHome of homes) {
    const runtime = NodePath.join(ownerHome, "runtime");
    const lease = NodePath.join(runtime, "jones-native-writer.sqlite");
    if (
      exists(NodePath.join(runtime, "jones-active-install.json")) ||
      exists(lease) ||
      exists(`${lease}.identity.json`)
    )
      throw new JonesNativeStartupRefusal(
        "foreign-state",
        "Native database ownership requires its bound home; startup held.",
      );
  }
  if (profile !== undefined) {
    // The profile lease is outside the renamed tree. Resolve a temporarily
    // absent leaf too, so rollback cannot be raced by a fresh-home launch.
    const lease = profileWriterLease(canonicalPotentialPath(profile)).path;
    if (exists(lease) || exists(`${lease}.identity.json`))
      throw new JonesNativeStartupRefusal(
        "foreign-state",
        "This profile belongs to a native install in another home; startup held.",
      );
  }
}

export interface JonesNativeWriterAdmission {
  readonly binding: Readonly<{
    home: string;
    databasePath: string;
    profile: string;
    environmentId: string;
    version: string;
    sourceSha: string;
    sourceTree: string;
    manifestSha256: string;
  }>;
  readonly assertCurrent: () => void;
}

const admittedWriters = new WeakSet<JonesNativeWriterAdmission>();
const desktopOwners = (registry[Symbol.for("jones.desktop-owner-leases.v1")] ??= new Map());

/** Retains one desktop owner independently of the shared application-writer leases. */
function inspectJonesDesktopOwner(
  admission: JonesNativeWriterAdmission,
): JonesNativeWriterAdmission {
  if (!admittedWriters.has(admission))
    throw new JonesNativeStartupRefusal(
      "ownership-unknown",
      "Desktop ownership requires retained native admission.",
    );
  admission.assertCurrent();
  const path = NodePath.join(admission.binding.home, "runtime", "jones-desktop-owner.sqlite");
  const scope = `desktop-owner:${admission.binding.home}`;
  initializeLease(path, scope);
  const identity = leaseIdentity(path, scope);
  let held = desktopOwners.get(path);
  if (held === undefined) {
    const database = new (sqlite().DatabaseSync)(path);
    try {
      database.exec("PRAGMA busy_timeout = 0; BEGIN EXCLUSIVE;");
      held = { database, scope, ...identity };
      desktopOwners.set(path, held);
    } catch (cause) {
      database.close();
      throw asJonesNativeStartupRefusal(cause, "lease-lost");
    }
  }
  const owner = held;
  return Object.freeze({
    binding: admission.binding,
    assertCurrent: () => {
      try {
        admission.assertCurrent();
        const current = leaseIdentity(path, scope);
        if (
          !owner.database.isOpen ||
          !owner.database.isTransaction ||
          current.device !== owner.device ||
          current.inode !== owner.inode
        )
          throw new JonesNativeStartupRefusal("lease-lost", "Desktop owner exclusion was lost.");
      } catch (cause) {
        throw asJonesNativeStartupRefusal(cause, "lease-lost");
      }
    },
  });
}

/** Mirrors C1's journal-only settlement admission; the helper revalidates under its activation lock. */
function isStartupSettleable(
  directory: string,
  transactionId: string,
  active: JsonRecord,
  journal?: JsonRecord,
): boolean {
  const root = NodePath.dirname(directory);
  const info = NodeFS.lstatSync(directory);
  if (
    NodeFS.lstatSync(root).isSymbolicLink() ||
    !info.isDirectory() ||
    info.isSymbolicLink() ||
    info.uid !== process.getuid?.() ||
    NodeFS.realpathSync(directory) !== directory
  )
    return false;
  if (exists(NodePath.join(root, "..", "..", "jones-maintenance"))) return false;
  const allowed = new Set([
    "intent.json",
    "journal.json",
    "prepare-intent.json",
    "prepare-dispatched.json",
    "continuation.json",
    "activation-request.json",
    "prepare-request.json",
    "prepare-failure.json",
    "recovery-reserve.bin",
    "recovery-journal-reserve.bin",
    "previous-pair",
  ]);
  if (NodeFS.readdirSync(directory).some((name) => !allowed.has(name))) return false;
  const intentPath = NodePath.join(directory, "intent.json");
  if (!exists(intentPath)) return false;
  const intent = readJson(intentPath);
  const keys = [
    "protocol",
    "transactionId",
    "expected",
    "staged",
    "continuationReceipt",
    "processes",
    "listener",
  ];
  if (
    Object.keys(intent).length !== keys.length ||
    keys.some((key) => !(key in intent)) ||
    intent.protocol !== 1 ||
    intent.transactionId !== transactionId ||
    intent.continuationReceipt !== NodePath.join(directory, "continuation.json") ||
    typeof intent.listener !== "string" ||
    !intent.listener.trim() ||
    intent.staged === null ||
    typeof intent.staged !== "object" ||
    Array.isArray(intent.staged) ||
    !Array.isArray(intent.processes) ||
    intent.processes.length === 0 ||
    intent.processes.length > 256 ||
    !intent.processes.every((value: unknown) => {
      const proof = record(value);
      return (
        Object.keys(proof).length === 2 &&
        Number.isInteger(proof.pid) &&
        Number(proof.pid) > 0 &&
        typeof proof.identity === "string" &&
        proof.identity.trim().length > 0
      );
    }) ||
    !NodeUtil.isDeepStrictEqual(active, intent.expected)
  )
    return false;
  if (journal !== undefined) {
    const fields = new Set([
      "intent",
      "phase",
      "message",
      "storage",
      "recoveryReserves",
      "incomingBundle",
      "previousBundle",
    ]);
    if (
      !["intent", "preparing", "quiescent"].includes(String(journal.phase)) ||
      !NodeUtil.isDeepStrictEqual(journal.intent, intent) ||
      Object.keys(journal).some((key) => !fields.has(key))
    )
      return false;
  }
  if (
    active.protocol !== 1 ||
    active.owner !== "desktop" ||
    typeof active.appPath !== "string" ||
    !NodePath.isAbsolute(active.appPath) ||
    typeof active.generation !== "string" ||
    !active.generation
  )
    return false;
  const pair = NodePath.join(directory, "previous-pair");
  if (
    (exists(pair) && NodeFS.lstatSync(pair).isSymbolicLink()) ||
    exists(NodePath.join(pair, "pair.json"))
  )
    return false;
  if (
    [
      "trial-descriptor.json",
      "trial-receipt.json",
      "commit-grant.json",
      "resume-dispatched.json",
      "advanced-state",
      "advanced-state-authority-guard.json",
    ].some((name) => exists(NodePath.join(directory, name)))
  )
    return false;
  const previous = active.appPath + ".jones-previous-" + active.generation;
  const incoming = active.appPath + ".jones-incoming-" + transactionId;
  return (
    !exists(previous) &&
    (journal !== undefined && "previousBundle" in journal ? journal.previousBundle : previous) ===
      previous &&
    (journal !== undefined && "incomingBundle" in journal ? journal.incomingBundle : incoming) ===
      incoming &&
    !["candidateWriter", "trialReceipt", "recovery", "preSwapPreparationRecovery"].some(
      (key) => journal !== undefined && key in journal,
    )
  );
}

/** Refuses unresolved native replacement before the caller opens application state. */
function inspectJonesNativeWriterFence(input: {
  readonly home: string;
  readonly databasePath: string;
  readonly profile?: string | undefined;
  readonly descriptorPath?: string | undefined;
  readonly version: string;
  readonly buildMetadata: unknown;
}): JonesNativeWriterAdmission | undefined {
  const manifestPath = NodePath.join(input.home, "runtime", "jones-active-install.json");
  if (!exists(manifestPath)) {
    if (input.descriptorPath !== undefined)
      throw new JonesNativeStartupRefusal(
        "trial-permit-mismatch",
        "Native trial manifest is missing; startup held.",
      );
    refuseForeignNativeState(input.home, input.databasePath, input.profile);
    return;
  }
  const before = readJson(manifestPath);
  if (before.protocol !== 1 || before.owner !== "desktop" || typeof before.profile !== "string")
    throw new JonesNativeStartupRefusal(
      "ownership-unknown",
      "Native install ownership is unknown; startup held.",
    );
  const home = NodeFS.realpathSync(input.home);
  const databasePath = NodeFS.realpathSync(input.databasePath);
  const profile = NodeFS.realpathSync(input.profile ?? before.profile);
  if (before.home !== home || before.databasePath !== databasePath || before.profile !== profile)
    throw new JonesNativeStartupRefusal(
      "paths-differ",
      "Native state paths differ from the active install; startup held.",
    );
  for (const lease of nativeWriterLeasePaths(home, profile)) holdLease(lease.path, lease.scope);
  // Shared leases precede this admission read. An activation published during
  // inspection must drain this process before it can obtain exclusive access.
  const active = readJson(manifestPath);
  if (JSON.stringify(active) !== JSON.stringify(before))
    throw new JonesNativeStartupRefusal(
      "generation-changed",
      "Native generation changed during startup; startup held.",
    );
  let authorized = active;
  let settleable: string | undefined;
  const transactions = NodePath.join(home, "runtime", "jones-updates", "transactions");
  if (exists(transactions)) {
    const transactionRoot = NodeFS.lstatSync(transactions);
    if (!transactionRoot.isDirectory() || transactionRoot.isSymbolicLink())
      throw new JonesNativeStartupRefusal(
        "ownership-unknown",
        "Native transaction root is unknown; startup held.",
      );
    const entries = NodeFS.readdirSync(transactions, { withFileTypes: true });
    if (entries.length > 1000)
      throw new JonesNativeStartupRefusal(
        "ownership-unknown",
        "Native transaction history exceeds its bound.",
      );
    for (const entry of entries) {
      if (!entry.isDirectory() || !/^[a-f0-9]{64}$/.test(entry.name))
        throw new JonesNativeStartupRefusal(
          "ownership-unknown",
          "Unknown native transaction requires reconciliation.",
        );
      try {
        const directory = NodePath.join(transactions, entry.name);
        const journalPath = NodePath.join(directory, "journal.json");
        if (!exists(journalPath)) {
          if (exists(NodePath.join(directory, "intent.json"))) {
            if (
              input.descriptorPath === undefined &&
              isStartupSettleable(directory, entry.name, active)
            ) {
              settleable ??= entry.name;
            } else {
              throw new JonesNativeStartupRefusal(
                "unstarted-activation",
                "Unstarted native activation holds new writers.",
                entry.name,
              );
            }
          } else if (NodeFS.readdirSync(directory).length !== 0) {
            throw new JonesNativeStartupRefusal(
              "ownership-unknown",
              "Unknown native transaction requires reconciliation.",
              entry.name,
            );
          }
          continue;
        }
        const journal = readJson(journalPath);
        const intent = record(journal.intent);
        if (intent.transactionId !== entry.name || intent.protocol !== 1)
          throw new JonesNativeStartupRefusal(
            "ownership-unknown",
            "Native journal binding is unknown; startup held.",
          );
        if (intent.kind === "cold-first-transition")
          throw new JonesNativeStartupRefusal(
            "transaction-unsettled",
            "Cold receiver requires reconciliation; startup held.",
          );
        if (
          ["kind", "maintenanceGeneration", "handoffSha256", "stateTransaction"].some(
            (key) => key in intent,
          )
        )
          throw new JonesNativeStartupRefusal(
            "ownership-unknown",
            "Unknown or mixed activation intent; startup held.",
          );
        if (journal.phase === "resumed" || journal.phase === "rolled-back") continue;
        if (
          input.descriptorPath === undefined &&
          isStartupSettleable(directory, entry.name, active, journal)
        ) {
          settleable ??= entry.name;
          continue;
        }
        if (!["trial", "validated", "committed", "resume-intent"].includes(String(journal.phase)))
          throw new JonesNativeStartupRefusal(
            "transaction-unsettled",
            "Native activation requires reconciliation before startup.",
            entry.name,
          );
        if (
          input.descriptorPath === undefined ||
          NodeFS.realpathSync(input.descriptorPath) !==
            NodePath.join(directory, "trial-descriptor.json")
        )
          throw new JonesNativeStartupRefusal(
            input.descriptorPath === undefined ? "transaction-unsettled" : "trial-permit-mismatch",
            "Native activation requires reconciliation before startup.",
            entry.name,
          );
        const descriptor = readJson(input.descriptorPath);
        const staged = record(intent.staged);
        if (
          descriptor.protocol !== 1 ||
          descriptor.startupGateProtocol !== 1 ||
          descriptor.transactionId !== entry.name ||
          descriptor.home !== home ||
          descriptor.stagedHandle !== staged.handle ||
          descriptor.databasePath !== databasePath ||
          descriptor.profile !== profile ||
          descriptor.environmentId !== active.environmentId ||
          ["version", "sourceSha", "sourceTree"].some((key) => descriptor[key] !== staged[key])
        )
          throw new JonesNativeStartupRefusal(
            "trial-permit-mismatch",
            "Native trial permit differs from the staged generation.",
          );
        authorized = descriptor;
      } catch (cause) {
        throw asJonesNativeStartupRefusal(cause, "ownership-unknown", entry.name);
      }
    }
  }
  const source = record(record(input.buildMetadata).jonesSource);
  if (
    source.repository !== "Jones-Systems/Jones-Code" ||
    source.sha !== authorized.sourceSha ||
    source.tree !== authorized.sourceTree ||
    input.version !== authorized.version
  )
    throw new JonesNativeStartupRefusal(
      "writer-mismatch",
      "The native writer does not match the admitted source generation.",
    );
  if (settleable !== undefined)
    throw new JonesNativeStartupRefusal(
      "transaction-settleable",
      "Interrupted native activation can be settled before startup.",
      settleable,
    );
  // Trial admission still serves its existing startup gate, but cannot issue ordinary desktop ownership.
  if (input.descriptorPath !== undefined || authorized !== active) return;
  if (typeof active.environmentId !== "string" || active.environmentId.length === 0)
    throw new JonesNativeStartupRefusal(
      "ownership-unknown",
      "Native environment identity is unavailable.",
    );
  const binding = Object.freeze({
    home,
    databasePath,
    profile,
    environmentId: active.environmentId,
    version: input.version,
    sourceSha: String(source.sha),
    sourceTree: String(source.tree),
    manifestSha256: NodeCrypto.createHash("sha256").update(JSON.stringify(active)).digest("hex"),
  });
  const admission: JonesNativeWriterAdmission = Object.freeze({
    binding,
    assertCurrent: () => {
      try {
        for (const lease of nativeWriterLeasePaths(home, profile)) {
          const held = leases.get(lease.path);
          const identity = leaseIdentity(lease.path, lease.scope);
          if (
            held === undefined ||
            !held.database.isOpen ||
            !held.database.isTransaction ||
            held.device !== identity.device ||
            held.inode !== identity.inode
          )
            throw new JonesNativeStartupRefusal(
              "lease-lost",
              "Native writer admission lost its held lease.",
            );
        }
        const current = holdJonesNativeWriterFence(input);
        if (current === undefined || JSON.stringify(current.binding) !== JSON.stringify(binding))
          throw new JonesNativeStartupRefusal(
            "generation-changed",
            "Native writer admission changed.",
          );
      } catch (cause) {
        throw asJonesNativeStartupRefusal(cause, "lease-lost");
      }
    },
  });
  admittedWriters.add(admission);
  return admission;
}

export function holdJonesNativeWriterFence(
  input: Parameters<typeof inspectJonesNativeWriterFence>[0],
): JonesNativeWriterAdmission | undefined {
  try {
    return inspectJonesNativeWriterFence(input);
  } catch (cause) {
    throw asJonesNativeStartupRefusal(cause);
  }
}

export function holdJonesDesktopOwner(
  admission: JonesNativeWriterAdmission,
): JonesNativeWriterAdmission {
  try {
    return inspectJonesDesktopOwner(admission);
  } catch (cause) {
    throw asJonesNativeStartupRefusal(cause, "lease-lost");
  }
}
