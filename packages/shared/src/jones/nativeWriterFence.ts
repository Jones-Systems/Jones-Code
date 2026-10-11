// @effect-diagnostics nodeBuiltinImport:off -- Synchronous native admission precedes the desktop and server runtimes.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import type { DatabaseSync } from "node:sqlite";

type JsonRecord = Record<string, unknown>;
interface Lease {
  readonly database: DatabaseSync;
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
  const info = NodeFS.lstatSync(path);
  if (!info.isFile() || info.isSymbolicLink() || info.size > 1024 * 1024)
    throw new Error("Native startup metadata is not a bounded regular file; startup held.");
  return record(JSON.parse(NodeFS.readFileSync(path, "utf8")));
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
    throw new Error("The stable native lease inode changed; reconciliation is required.");
  return { device: String(info.dev), inode: String(info.ino) };
}

function initializeLease(path: string, scope: string): void {
  if (exists(path)) return;
  if (exists(`${path}.identity.json`))
    throw new Error("A native lease disappeared; reconciliation is required.");
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
      throw new Error("A held native lease was replaced; startup held.");
    return;
  }
  const database = new (sqlite().DatabaseSync)(path, { readOnly: true });
  try {
    if (database.prepare("PRAGMA journal_mode").get()?.journal_mode !== "delete")
      throw new Error("Native writer leases require rollback journal mode; startup held.");
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
      throw new Error("Native writer lease identity is unknown; startup held.");
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
      throw new Error("Native database ownership requires its bound home; startup held.");
  }
  if (profile !== undefined) {
    // The profile lease is outside the renamed tree. Resolve a temporarily
    // absent leaf too, so rollback cannot be raced by a fresh-home launch.
    const lease = profileWriterLease(canonicalPotentialPath(profile)).path;
    if (exists(lease) || exists(`${lease}.identity.json`))
      throw new Error("This profile belongs to a native install in another home; startup held.");
  }
}

/** Refuses unresolved native replacement before the caller opens application state. */
export function holdJonesNativeWriterFence(input: {
  readonly home: string;
  readonly databasePath: string;
  readonly profile?: string | undefined;
  readonly descriptorPath?: string | undefined;
  readonly version: string;
  readonly buildMetadata: unknown;
}): void {
  const manifestPath = NodePath.join(input.home, "runtime", "jones-active-install.json");
  if (!exists(manifestPath)) {
    if (input.descriptorPath !== undefined)
      throw new Error("Native trial manifest is missing; startup held.");
    refuseForeignNativeState(input.home, input.databasePath, input.profile);
    return;
  }
  const before = readJson(manifestPath);
  if (before.protocol !== 1 || before.owner !== "desktop" || typeof before.profile !== "string")
    throw new Error("Native install ownership is unknown; startup held.");
  const home = NodeFS.realpathSync(input.home);
  const databasePath = NodeFS.realpathSync(input.databasePath);
  const profile = NodeFS.realpathSync(input.profile ?? before.profile);
  if (before.home !== home || before.databasePath !== databasePath || before.profile !== profile)
    throw new Error("Native state paths differ from the active install; startup held.");
  for (const lease of nativeWriterLeasePaths(home, profile)) holdLease(lease.path, lease.scope);
  // Shared leases precede this admission read. An activation published during
  // inspection must drain this process before it can obtain exclusive access.
  const active = readJson(manifestPath);
  if (JSON.stringify(active) !== JSON.stringify(before))
    throw new Error("Native generation changed during startup; startup held.");
  let authorized = active;
  const transactions = NodePath.join(home, "runtime", "jones-updates", "transactions");
  if (exists(transactions)) {
    const entries = NodeFS.readdirSync(transactions, { withFileTypes: true });
    if (entries.length > 1000) throw new Error("Native transaction history exceeds its bound.");
    for (const entry of entries) {
      if (!entry.isDirectory() || !/^[a-f0-9]{64}$/.test(entry.name))
        throw new Error("Unknown native transaction requires reconciliation.");
      const directory = NodePath.join(transactions, entry.name);
      const journalPath = NodePath.join(directory, "journal.json");
      if (!exists(journalPath)) {
        if (exists(NodePath.join(directory, "intent.json")))
          throw new Error("Unstarted native activation holds new writers.");
        continue;
      }
      const journal = readJson(journalPath);
      const intent = record(journal.intent);
      if (intent.transactionId !== entry.name || intent.protocol !== 1)
        throw new Error("Native journal binding is unknown; startup held.");
      if (journal.phase === "resumed" || journal.phase === "rolled-back") continue;
      if (
        input.descriptorPath === undefined ||
        NodeFS.realpathSync(input.descriptorPath) !==
          NodePath.join(directory, "trial-descriptor.json") ||
        !["trial", "validated", "committed", "resume-intent"].includes(String(journal.phase))
      )
        throw new Error("Native activation requires reconciliation before startup.");
      const descriptor = readJson(input.descriptorPath);
      const staged = record(intent.staged);
      if (
        descriptor.protocol !== 1 ||
        descriptor.startupGateProtocol !== 1 ||
        descriptor.transactionId !== entry.name ||
        descriptor.home !== home ||
        descriptor.databasePath !== databasePath ||
        descriptor.profile !== profile ||
        descriptor.environmentId !== active.environmentId ||
        ["version", "sourceSha", "sourceTree"].some((key) => descriptor[key] !== staged[key])
      )
        throw new Error("Native trial permit differs from the staged generation.");
      authorized = descriptor;
    }
  }
  const source = record(record(input.buildMetadata).jonesSource);
  if (
    source.repository !== "Jones-Systems/Jones-Code" ||
    source.sha !== authorized.sourceSha ||
    source.tree !== authorized.sourceTree ||
    input.version !== authorized.version
  )
    throw new Error("The native writer does not match the admitted source generation.");
}
