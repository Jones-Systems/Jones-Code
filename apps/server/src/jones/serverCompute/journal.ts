// @effect-diagnostics nodeBuiltinImport:off cryptoRandomUUID:off preferSchemaOverJson:off -- Durable bounded ownership metadata uses OS-exclusive files and explicit validation.
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { computeFailure } from "./failure.ts";
import { counter, identifier, keys, record } from "./protocol.ts";

type Claim = { version: 1; authorityId: string; parentPid: number; count: 2 | 3 };
type SlotRecord = {
  version: 1;
  authorityId: string;
  slotId: number;
  generation: number;
  state: "starting" | "registered" | "retired";
  pid?: number;
  nonce: string;
};
type Snapshot = { stat: NodeFS.Stats; text: string };
const sameInode = (a: NodeFS.Stats, b: NodeFS.Stats) => a.dev === b.dev && a.ino === b.ino;
const absent = (cause: unknown) => record(cause) && cause.code === "ENOENT";
/** ESRCH alone is absence. PID reuse/EPERM/unsupported probes close admission; never signal a recorded PID. */
function presence(pid: number): "present" | "absent" | "unknown" {
  try {
    process.kill(pid, 0);
    return "present";
  } catch (cause) {
    return record(cause) && cause.code === "ESRCH" ? "absent" : "unknown";
  }
}
export function proveAbsent(pid: number): boolean {
  return presence(pid) === "absent";
}
async function snapshot(path: string): Promise<Snapshot> {
  const file = await NodeFSP.open(path, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    const stat = await file.stat();
    if (
      !stat.isFile() ||
      stat.size > 16_384 ||
      stat.nlink !== 1 ||
      (stat.mode & 0o077) !== 0 ||
      stat.uid !== process.getuid?.()
    )
      throw computeFailure("retirement-unknown");
    const bytes = Buffer.alloc(16_385);
    const read = await file.read(bytes, 0, bytes.length, 0);
    if (read.bytesRead > 16_384) throw computeFailure("retirement-unknown");
    return { stat, text: bytes.subarray(0, read.bytesRead).toString("utf8") };
  } finally {
    await file.close();
  }
}
async function unchanged(path: string, original: Snapshot): Promise<void> {
  const now = await snapshot(path);
  if (!sameInode(now.stat, original.stat) || now.text !== original.text)
    throw computeFailure("retirement-unknown");
}
function claimValue(text: string): Claim {
  const value: unknown = JSON.parse(text);
  if (
    !record(value) ||
    !keys(value, ["version", "authorityId", "parentPid", "count"]) ||
    value.version !== 1 ||
    !identifier(value.authorityId) ||
    !counter(value.parentPid) ||
    (value.count !== 2 && value.count !== 3)
  )
    throw computeFailure("retirement-unknown");
  return value as Claim;
}
function slotValue(text: string, claim: Claim, slotId: number): SlotRecord {
  const value: unknown = JSON.parse(text);
  if (
    !record(value) ||
    !keys(value, ["version", "authorityId", "slotId", "generation", "state", "nonce"], ["pid"]) ||
    value.version !== 1 ||
    value.authorityId !== claim.authorityId ||
    value.slotId !== slotId ||
    !counter(value.generation) ||
    !identifier(value.nonce) ||
    !["starting", "registered", "retired"].includes(String(value.state)) ||
    (value.pid !== undefined && !counter(value.pid)) ||
    (value.state !== "starting" && value.pid === undefined)
  )
    throw computeFailure("retirement-unknown");
  return value as SlotRecord;
}
async function syncDir(directory: string): Promise<void> {
  const fd = await NodeFSP.open(
    directory,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_DIRECTORY | NodeFS.constants.O_NOFOLLOW,
  );
  try {
    await fd.sync();
  } finally {
    await fd.close();
  }
}
async function exclusive(path: string, value: unknown): Promise<Snapshot> {
  const fd = await NodeFSP.open(
    path,
    NodeFS.constants.O_CREAT |
      NodeFS.constants.O_EXCL |
      NodeFS.constants.O_WRONLY |
      NodeFS.constants.O_NOFOLLOW,
    0o600,
  );
  try {
    const text = JSON.stringify(value);
    await fd.writeFile(text);
    await fd.sync();
    return { stat: await fd.stat(), text };
  } finally {
    await fd.close();
  }
}

/** One owner mutates these files; a failed/partial mutation deliberately leaves admission closed. */
export class GenerationJournal {
  private readonly records = new Map<number, Snapshot>();
  private readonly directory: string;
  readonly claim: Claim;
  private readonly ownedClaim: Snapshot;
  private readonly directoryStat: NodeFS.Stats;
  private constructor(
    directory: string,
    claim: Claim,
    ownedClaim: Snapshot,
    directoryStat: NodeFS.Stats,
  ) {
    this.directory = directory;
    this.claim = claim;
    this.ownedClaim = ownedClaim;
    this.directoryStat = directoryStat;
  }

  static async acquire(
    directory: string,
    count: 2 | 3,
    platform: NodeJS.Platform = HostProcess.Platform.defaultValue(),
  ): Promise<GenerationJournal> {
    if (platform !== "darwin" && platform !== "linux") throw computeFailure("unavailable");
    // Refuse redirected ancestors as well as leaf records. The server supplies its canonical stateDir.
    const absolute = NodePath.resolve(directory),
      root = NodePath.parse(absolute).root;
    let ancestor = root;
    for (const segment of absolute.slice(root.length).split(NodePath.sep).slice(0, -1)) {
      ancestor = NodePath.join(ancestor, segment);
      const stat = await NodeFSP.lstat(ancestor);
      if (!stat.isDirectory() || stat.isSymbolicLink()) throw computeFailure("retirement-unknown");
    }
    // The server owns stateDir; refuse a redirected lifecycle directory. No recursive sweep/mkdir.
    try {
      await NodeFSP.mkdir(directory, { mode: 0o700 });
    } catch (cause) {
      if (!record(cause) || cause.code !== "EEXIST") throw cause;
    }
    const dir = await NodeFSP.lstat(directory);
    if (
      !dir.isDirectory() ||
      dir.isSymbolicLink() ||
      (dir.mode & 0o077) !== 0 ||
      dir.uid !== process.getuid?.()
    )
      throw computeFailure("retirement-unknown");
    const path = NodePath.join(directory, "pool.claim"),
      recovery = NodePath.join(directory, "recovery.claim");
    // All contenders, including fresh admission, serialize through this O_EXCL guard.
    // A crash leaving the guard is uncertain and cannot be silently repaired.
    const recoverySnapshot = await exclusive(recovery, {
      version: 1,
      owner: NodeCrypto.randomUUID(),
    });
    let completed = false,
      refusedLiveOwner = false;
    try {
      let old: Snapshot | undefined;
      try {
        old = await snapshot(path);
      } catch (cause) {
        if (!absent(cause)) throw cause;
      }
      const prior = old === undefined ? undefined : { snapshot: old, claim: claimValue(old.text) };
      if (prior) {
        const parent = presence(prior.claim.parentPid);
        if (parent !== "absent") {
          if (parent === "present") {
            await unchanged(path, prior.snapshot);
            refusedLiveOwner = true;
          }
          throw computeFailure("retirement-unknown");
        }
      }
      // A live owner may be between exclusive .next creation and rename. Refuse
      // that duplicate without retaining our guard; only recovery inspects tails.
      for (let id = 0; id < 3; id++) {
        try {
          await NodeFSP.lstat(NodePath.join(directory, `slot-${id}.next`));
          throw computeFailure("retirement-unknown");
        } catch (cause) {
          if (!absent(cause)) throw cause;
        }
      }
      if (prior) {
        const slots: Array<{ path: string; snapshot: Snapshot }> = [];
        for (let id = 0; id < 3; id++) {
          const slotPath = NodePath.join(directory, `slot-${id}.json`);
          let state: Snapshot;
          try {
            state = await snapshot(slotPath);
          } catch (cause) {
            if (absent(cause)) continue;
            throw cause;
          }
          if (id >= prior.claim.count) throw computeFailure("retirement-unknown");
          const slot = slotValue(state.text, prior.claim, id);
          if (slot.pid === undefined || !proveAbsent(slot.pid))
            throw computeFailure("retirement-unknown");
          slots.push({ path: slotPath, snapshot: state });
        }
        await unchanged(path, prior.snapshot);
        for (const slot of slots) await unchanged(slot.path, slot.snapshot);
        // No actor can acquire a new generation until the recovery guard is released.
        for (const slot of slots) await NodeFSP.unlink(slot.path);
        await unchanged(path, prior.snapshot);
        await NodeFSP.unlink(path);
        await syncDir(directory);
      } else {
        // Orphan records cannot be explained by this authority; never overwrite them.
        for (let id = 0; id < 3; id++) {
          try {
            await NodeFSP.lstat(NodePath.join(directory, `slot-${id}.json`));
            throw computeFailure("retirement-unknown");
          } catch (cause) {
            if (!absent(cause)) throw cause;
          }
        }
      }
      const claim: Claim = {
        version: 1,
        authorityId: NodeCrypto.randomUUID(),
        parentPid: process.pid,
        count,
      };
      await exclusive(path, claim);
      await syncDir(directory);
      const result = new GenerationJournal(directory, claim, await snapshot(path), dir);
      completed = true;
      return result;
    } finally {
      // A proved live-owner refusal changed no pool records. Unknown or interrupted recovery retains its guard.
      if (completed || refusedLiveOwner) {
        if (!sameInode(await NodeFSP.lstat(directory), dir))
          throw computeFailure("retirement-unknown");
        await unchanged(recovery, recoverySnapshot);
        await NodeFSP.unlink(recovery);
        await syncDir(directory);
      }
    }
  }
  private async verify(): Promise<void> {
    if (!sameInode(await NodeFSP.lstat(this.directory), this.directoryStat))
      throw computeFailure("retirement-unknown");
    await unchanged(NodePath.join(this.directory, "pool.claim"), this.ownedClaim);
  }
  async reserve(slotId: number, generation: number, nonce: string): Promise<void> {
    await this.write({
      version: 1,
      authorityId: this.claim.authorityId,
      slotId,
      generation,
      nonce,
      state: "starting",
    });
  }
  async registered(slotId: number, generation: number, nonce: string, pid: number): Promise<void> {
    await this.write({
      version: 1,
      authorityId: this.claim.authorityId,
      slotId,
      generation,
      nonce,
      pid,
      state: "registered",
    });
  }
  async retired(slotId: number, generation: number, nonce: string, pid: number): Promise<void> {
    await this.write({
      version: 1,
      authorityId: this.claim.authorityId,
      slotId,
      generation,
      nonce,
      pid,
      state: "retired",
    });
  }
  private async write(value: SlotRecord): Promise<void> {
    await this.verify();
    if (value.slotId < 0 || value.slotId >= this.claim.count || !counter(value.generation))
      throw computeFailure("protocol");
    const path = NodePath.join(this.directory, `slot-${value.slotId}.json`),
      prior = this.records.get(value.slotId);
    if (prior) {
      await unchanged(path, prior);
      const old = slotValue(prior.text, this.claim, value.slotId);
      const nextGeneration =
        value.generation === old.generation + 1 &&
        old.state === "retired" &&
        value.state === "starting";
      const sameGeneration =
        value.generation === old.generation &&
        value.nonce === old.nonce &&
        ((old.state === "starting" && value.state === "registered") ||
          ((old.state === "starting" || old.state === "registered") && value.state === "retired"));
      if (!nextGeneration && !sameGeneration) throw computeFailure("retirement-unknown");
      const temporary = NodePath.join(this.directory, `slot-${value.slotId}.next`);
      await exclusive(temporary, value);
      await unchanged(path, prior);
      await NodeFSP.rename(temporary, path);
    } else await exclusive(path, value);
    await syncDir(this.directory);
    this.records.set(value.slotId, await snapshot(path));
  }
  /** Call only after captured exit/close evidence for every spawned child. */
  async release(): Promise<void> {
    await this.verify();
    for (const [id, state] of this.records) {
      const slot = slotValue(state.text, this.claim, id);
      if (slot.state !== "retired") throw computeFailure("retirement-unknown");
      await unchanged(NodePath.join(this.directory, `slot-${id}.json`), state);
    }
    for (const id of this.records.keys())
      await NodeFSP.unlink(NodePath.join(this.directory, `slot-${id}.json`));
    await unchanged(NodePath.join(this.directory, "pool.claim"), this.ownedClaim);
    await NodeFSP.unlink(NodePath.join(this.directory, "pool.claim"));
    await syncDir(this.directory);
  }
}
