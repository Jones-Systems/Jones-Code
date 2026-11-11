// @effect-diagnostics nodeBuiltinImport:off globalDate:off -- Dependency-free native timestamps and producer-owned durable finalization records; this is not an assessment or retirement ledger.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import { attest, binding, type Attestation, type Binding } from "./OutputAttestation.ts";
import { participantGuard, reservationTransaction } from "./ParticipantGuard.ts";
import { canonical, exists, inside, openAbsolute, readBounded } from "./Filesystem.ts";
import {
  producerIdentity,
  observeProducerSelf,
  observeProducerSession,
  type ProducerIdentity,
  type HolderObservation,
} from "./ProducerSessionObservation.ts";

type Spawn = { id: string } & (
  | { state: "pending" | "none" | "boot_absent" }
  | { state: "session"; sid: number; discharged: boolean }
);
interface Holders {
  version: 1;
  self: ProducerIdentity;
  in_process: "running" | "settled";
  admission_closed: boolean;
  self_discharge: "settled" | "self_absent" | "different_boot" | null;
  spawns: Spawn[];
  overflow: boolean;
  overflow_discharge?: "different_boot";
}
interface Production {
  schema: "workspace-output-obligation/v1";
  id: string;
  producer: string;
  owner: string;
  worktree: string;
  binding: Binding;
  started_at: string;
  status: "active" | Attestation["status"];
  outcome: "unresolved" | "retained" | "disposed";
  proofStatus: "deferred" | "attested" | "unavailable";
  roots: string[];
  sidecars: string[];
  reasons: string[];
  finished_at?: string;
  rev?: number;
  holders?: Holders;
  resolution?: { version: 1; rev: number; at: string };
  supersedes?: { id: string; rev: number | null };
}
export interface ProductionSession {
  record: Production;
  path: string;
}
interface ProductionResolution {
  id: string;
  path: string;
  outcome: Production["outcome"];
  status: Production["status"];
  reasons: string[];
}
const NAME = /^[a-z0-9][a-z0-9._-]{0,23}-[a-f0-9]{32}\.json$/;
const TEMP = /^\.[a-z0-9][a-z0-9._-]{0,23}-[a-f0-9]{32}-[a-f0-9]{16}$/;
const dutyReasons = (record: Production) =>
  record.reasons.filter((reason) => reason !== "content_verification_deferred");
const queues = new Map<string, Promise<unknown>>();
const keysWithin = (value: object, keys: readonly string[]) =>
  Object.keys(value).every((key) => keys.includes(key));
const cursorName = (value: unknown): value is string =>
  typeof value === "string" && Buffer.byteLength(value) <= 1024 && !/[\/\0]/.test(value);

// Queue before SQLite's synchronous busy wait; retain exclusion through all I/O.
function guarded<T>(bound: Binding, action: () => Promise<T>): Promise<T> {
  const key = bound.git_common_dir;
  const prior = queues.get(key) ?? Promise.resolve();
  const result = prior
    .catch(() => {})
    .then(() =>
      reservationTransaction(bound, async (db) => {
        if (db.prepare("SELECT 1 FROM reservations WHERE admin_id=?").get(bound.admin_id))
          throw new Error("retiring");
        if (canonical(await binding(bound.path)) !== canonical(bound))
          throw new Error("identity_mismatch");
        return action();
      }),
    );
  queues.set(key, result);
  void result
    .finally(() => {
      if (queues.get(key) === result) queues.delete(key);
    })
    .catch(() => {});
  return result;
}
function validHolders(record: Production): boolean {
  const h = record.holders;
  if (!h || h.version !== 1 || !Number.isSafeInteger(record.rev) || record.rev! < 1) return false;
  if (
    !keysWithin(h, [
      "version",
      "self",
      "in_process",
      "admission_closed",
      "self_discharge",
      "spawns",
      "overflow",
      "overflow_discharge",
    ])
  )
    return false;
  const self = h.self;
  if (
    !self ||
    [self.host, self.boot, self.start, self.namespace].some((x) => typeof x !== "string" || !x) ||
    !keysWithin(self, ["host", "boot", "pid", "start", "namespace"]) ||
    !Number.isSafeInteger(self.pid) ||
    self.pid <= 0 ||
    !["running", "settled"].includes(h.in_process) ||
    typeof h.admission_closed !== "boolean" ||
    ![null, "settled", "self_absent", "different_boot"].includes(h.self_discharge) ||
    typeof h.overflow !== "boolean" ||
    !Array.isArray(h.spawns) ||
    (h.overflow_discharge !== undefined &&
      (h.overflow_discharge !== "different_boot" || h.self_discharge !== "different_boot"))
  )
    return false;
  const ids = new Set<string>();
  for (const spawn of h.spawns) {
    if (!spawn || !/^[a-f0-9]{32}$/.test(spawn.id) || ids.has(spawn.id)) return false;
    ids.add(spawn.id);
    if (spawn.state === "session") {
      if (
        !keysWithin(spawn, ["id", "state", "sid", "discharged"]) ||
        !Number.isSafeInteger(spawn.sid) ||
        spawn.sid <= 0 ||
        typeof spawn.discharged !== "boolean"
      )
        return false;
    } else if (
      !["pending", "none", "boot_absent"].includes(spawn.state) ||
      !keysWithin(spawn, ["id", "state"]) ||
      (spawn.state === "boot_absent" && h.self_discharge !== "different_boot")
    )
      return false;
  }
  if (h.self_discharge === "settled" && h.in_process !== "settled") return false;
  return Buffer.byteLength(canonical(record)) < 60000;
}
function allocationResolved(record: Production): boolean {
  const h = record.holders;
  return (
    validHolders(record) &&
    !!h &&
    h.admission_closed &&
    !!h.self_discharge &&
    !h.overflow &&
    h.spawns.every(
      (spawn) =>
        spawn.state === "none" ||
        spawn.state === "boot_absent" ||
        (spawn.state === "session" && spawn.discharged),
    ) &&
    record.resolution?.version === 1 &&
    record.resolution.rev === record.rev &&
    keysWithin(record.resolution, ["version", "rev", "at"]) &&
    typeof record.resolution.at === "string" &&
    Number.isFinite(Date.parse(record.resolution.at)) &&
    dutyReasons(record).length === 0 &&
    ["retained", "disposed"].includes(record.outcome) &&
    (record.status !== "active" || record.finished_at === undefined)
  );
}
function validate(record: Production, name: string, bound: Binding) {
  if (
    !record ||
    record.schema !== "workspace-output-obligation/v1" ||
    record.id + ".json" !== name ||
    !NAME.test(name) ||
    canonical(record.binding) !== canonical(bound) ||
    record.worktree !== bound.path ||
    typeof record.producer !== "string" ||
    !/^[a-z0-9][a-z0-9._-]{0,23}$/.test(record.producer) ||
    !record.id.startsWith(record.producer + "-") ||
    typeof record.owner !== "string" ||
    !record.owner ||
    typeof record.started_at !== "string" ||
    !Number.isFinite(Date.parse(record.started_at)) ||
    !["active", "succeeded", "failed", "cancelled"].includes(record.status) ||
    !["unresolved", "retained", "disposed"].includes(record.outcome) ||
    !["deferred", "attested", "unavailable"].includes(record.proofStatus) ||
    !Array.isArray(record.roots) ||
    record.roots.some(
      (root) =>
        typeof root !== "string" ||
        NodePath.resolve(root) !== root ||
        !inside(root, bound.path) ||
        root === bound.path ||
        inside(root, bound.git_dir),
    ) ||
    !Array.isArray(record.sidecars) ||
    record.sidecars.some((x) => typeof x !== "string") ||
    !Array.isArray(record.reasons) ||
    record.reasons.some((x) => typeof x !== "string")
  )
    throw new Error("identity_mismatch");
  return record;
}
async function load(path: string, bound: Binding) {
  return validate(
    JSON.parse((await readBounded(path, 65536)).toString()),
    NodePath.basename(path),
    bound,
  );
}
async function save(session: ProductionSession, expectedRev: number | null) {
  const record = session.record;
  if (canonical(await binding(record.worktree)) !== canonical(record.binding))
    throw new Error("identity_mismatch");
  if (expectedRev === null) {
    if (await exists(session.path)) throw new Error("revision_changed");
  } else if ((await load(session.path, record.binding)).rev !== expectedRev)
    throw new Error("revision_changed");
  const payload = canonical(record) + "\n";
  if (Buffer.byteLength(payload) >= 60000) throw new Error("holder_bound_exceeded");
  const directory = await openAbsolute(NodePath.dirname(session.path), true);
  const temporary = NodePath.join(
    NodePath.dirname(session.path),
    "." + record.id + "-" + NodeCrypto.randomBytes(8).toString("hex"),
  );
  try {
    const file = await NodeFSP.open(temporary, "wx", 0o600);
    try {
      await file.writeFile(payload);
      await file.sync();
    } finally {
      await file.close();
    }
    await NodeFSP.rename(temporary, session.path);
    await directory.sync();
  } finally {
    await NodeFSP.rm(temporary, { force: true });
    await directory.close();
  }
}
async function change(session: ProductionSession, edit: (record: Production) => void) {
  const current = await load(session.path, session.record.binding);
  if (!validHolders(current)) throw new Error("legacy_no_holder_identity");
  if (current.rev !== session.record.rev) throw new Error("revision_changed");
  const rev = current.rev!;
  edit(current);
  current.rev = rev + 1;
  delete current.resolution;
  await save({ path: session.path, record: current }, rev);
  session.record = current;
}
async function namesWithinCapacity(directory: string) {
  const names: string[] = [];
  const anchor = await openAbsolute(directory, true);
  try {
    const stream = await NodeFSP.opendir(directory);
    for await (const entry of stream) {
      names.push(entry.name);
      if (names.length > 10064)
        throw new Error(`capacity_unrecoverable; path=${directory}; count>=10065`);
    }
  } finally {
    await anchor.close();
  }
  return names.sort();
}
const sameKey = (a: Production, b: Production) =>
  canonical(a.binding) === canonical(b.binding) &&
  a.producer === b.producer &&
  canonical([...a.roots].sort()) === canonical([...b.roots].sort());
function prunable(record: Production) {
  if (record.status === "active") return allocationResolved(record);
  return (
    ["retained", "disposed"].includes(record.outcome) &&
    dutyReasons(record).length === 0 &&
    typeof record.finished_at === "string" &&
    Number.isFinite(Date.parse(record.finished_at)) &&
    ((record.holders === undefined &&
      record.rev === undefined &&
      record.resolution === undefined) ||
      allocationResolved(record))
  );
}
async function prune(successorPath: string, bound: Binding, count: number) {
  const successor = await load(successorPath, bound),
    link = successor.supersedes;
  if (!link || !NAME.test(link.id + ".json") || !validHolders(successor)) return false;
  const path = NodePath.join(NodePath.dirname(successorPath), link.id + ".json");
  if (path === successorPath || !(await exists(path))) return false;
  const prior = await load(path, bound);
  if (
    !sameKey(prior, successor) ||
    !prunable(prior) ||
    (prior.rev ?? null) !== link.rev ||
    Date.parse(successor.started_at) < Date.parse(prior.started_at)
  )
    return false;
  // A new active successor preserves the older reader's hold for this exact key.
  if (prior.status === "active" && !allocationResolved(prior)) return false;
  try {
    await NodeFSP.unlink(path);
    const directory = await openAbsolute(NodePath.dirname(path), true);
    try {
      await directory.sync();
    } finally {
      await directory.close();
    }
  } catch (cause) {
    throw new Error(`prune_not_confirmed; path=${path}; count=${count}`, { cause });
  }
  return true;
}
async function capacity(bound: Binding, key?: Pick<Production, "producer" | "roots">) {
  const directory = NodePath.join(bound.git_dir, "workspace-output-obligations");
  let names = await namesWithinCapacity(directory);
  for (const name of names.filter((name) => TEMP.test(name)))
    await NodeFSP.unlink(NodePath.join(directory, name));
  names = names.filter((name) => !TEMP.test(name));
  const cursorPath = NodePath.join(bound.git_dir, "workspace-output-obligations-cursor.json");
  let after = "";
  try {
    const cursor = JSON.parse((await readBounded(cursorPath, 4096)).toString());
    if (cursorName(cursor.after)) after = cursor.after;
  } catch {
    /* A corrupt advisory cursor restarts discovery; it authorizes nothing. */
  }
  const ordered = [
    ...names.filter((name) => name > after),
    ...names.filter((name) => name <= after),
  ];
  let candidate: ProductionSession | undefined;
  for (const name of ordered.slice(0, 16)) {
    after = name;
    if (!NAME.test(name)) continue;
    const path = NodePath.join(directory, name);
    let record: Production;
    try {
      record = await load(path, bound);
    } catch {
      continue;
    }
    if (record.supersedes && (await prune(path, bound, names.length)))
      names = await namesWithinCapacity(directory);
    if (
      !candidate &&
      key &&
      sameKey(record, { ...record, producer: key.producer, roots: key.roots }) &&
      prunable(record)
    )
      candidate = { path, record };
  }
  const cursorTemporary = cursorPath + "-" + NodeCrypto.randomBytes(8).toString("hex");
  try {
    const file = await NodeFSP.open(cursorTemporary, "wx", 0o600);
    try {
      await file.writeFile(canonical({ after: cursorName(after) ? after : "" }));
    } finally {
      await file.close();
    }
    await NodeFSP.rename(cursorTemporary, cursorPath);
  } finally {
    await NodeFSP.rm(cursorTemporary, { force: true });
  }
  if (names.length > 10000)
    throw new Error(`capacity_unrecoverable; path=${directory}; count=${names.length}`);
  return { names, candidate };
}
async function reconcile(session: ProductionSession): Promise<ProductionResolution> {
  if (!validHolders(session.record))
    return {
      id: session.record.id,
      path: session.path,
      status: session.record.status,
      outcome: session.record.outcome,
      reasons: ["legacy_no_holder_identity"],
    };
  if (allocationResolved(session.record))
    return {
      id: session.record.id,
      path: session.path,
      status: session.record.status,
      outcome: session.record.outcome,
      reasons: [],
    };
  if (!session.record.holders!.admission_closed)
    await change(session, (r) => {
      r.holders!.admission_closed = true;
    });
  const h = session.record.holders!;
  const observedSelf = await observeProducerSelf(h.self);
  const self: HolderObservation =
    observedSelf.reason === "different_boot"
      ? observedSelf
      : h.self_discharge
        ? { state: "absent", reason: h.self_discharge }
        : h.in_process === "settled"
          ? { state: "absent", reason: "settled" }
          : observedSelf;
  const blockers: string[] = [];
  if (self.state !== "absent") blockers.push(self.reason);
  const differentBoot = self.state === "absent" && self.reason === "different_boot";
  const discharged = new Set<number>();
  for (const spawn of h.spawns)
    if (spawn.state === "session" && !spawn.discharged) {
      const observation = differentBoot
        ? { state: "absent", reason: "different_boot" }
        : await observeProducerSession(h.self, spawn.sid);
      if (observation.state === "absent") discharged.add(spawn.sid);
      else blockers.push(observation.reason);
    }
  if (!differentBoot && (h.overflow || h.spawns.some((spawn) => spawn.state === "pending")))
    blockers.push("untracked_spawn_possible");
  const record = await load(session.path, session.record.binding);
  if (record.rev !== session.record.rev) throw new Error("revision_changed");
  const rev = record.rev!,
    holders = record.holders!;
  if (self.state === "absent")
    holders.self_discharge = differentBoot
      ? "different_boot"
      : h.in_process === "settled"
        ? "settled"
        : "self_absent";
  for (let i = 0; i < holders.spawns.length; i++) {
    const spawn = holders.spawns[i]!;
    if (spawn.state === "session" && (differentBoot || discharged.has(spawn.sid)))
      spawn.discharged = true;
    if (spawn.state === "pending" && differentBoot)
      holders.spawns[i] = { id: spawn.id, state: "boot_absent" };
  }
  if (differentBoot && holders.overflow) {
    holders.overflow = false;
    holders.overflow_discharge = "different_boot";
  }
  const complete =
    holders.self_discharge &&
    !holders.overflow &&
    holders.spawns.every(
      (spawn) =>
        spawn.state === "none" ||
        spawn.state === "boot_absent" ||
        (spawn.state === "session" && spawn.discharged),
    );
  const spawnedComplete =
    !holders.overflow &&
    holders.spawns.every(
      (spawn) =>
        spawn.state === "none" ||
        spawn.state === "boot_absent" ||
        (spawn.state === "session" && spawn.discharged),
    );
  record.reasons = record.reasons.filter((reason) =>
    reason === "producer_active"
      ? !complete
      : reason === "in_process_completion_not_observed"
        ? !holders.self_discharge
        : reason === "writer_completion_not_observed"
          ? !spawnedComplete
          : true,
  );
  if (!complete) {
    if (!holders.self_discharge) record.reasons.push("in_process_completion_not_observed");
    if (
      holders.overflow ||
      holders.spawns.some(
        (spawn) => spawn.state === "pending" || (spawn.state === "session" && !spawn.discharged),
      )
    )
      record.reasons.push("writer_completion_not_observed");
  }
  if (!record.roots.length && !record.reasons.includes("producer_outputs_unknown"))
    record.reasons.push("producer_outputs_unknown");
  record.rev = rev + 1;
  if (complete && dutyReasons(record).length === 0) {
    let remaining = false;
    for (const root of record.roots) if (await exists(root)) remaining = true;
    record.outcome = remaining ? "retained" : "disposed";
    if (
      remaining &&
      record.proofStatus === "deferred" &&
      !record.reasons.includes("content_verification_deferred")
    )
      record.reasons.push("content_verification_deferred");
    record.resolution = { version: 1, rev: record.rev, at: new Date().toISOString() };
  } else {
    record.outcome = "unresolved";
    delete record.resolution;
  }
  await save({ path: session.path, record }, rev);
  session.record = record;
  return {
    id: record.id,
    path: session.path,
    outcome: record.outcome,
    status: record.status,
    reasons: [...new Set([...blockers, ...dutyReasons(record)])],
  };
}
export async function resolveProductions(
  worktree: string,
  match?: { producer: string; roots: readonly string[] },
) {
  const bound = await binding(worktree);
  return guarded(bound, async () => {
    const directory = NodePath.join(bound.git_dir, "workspace-output-obligations");
    if (!(await exists(directory))) return [];
    const { names } = await capacity(bound);
    const results: ProductionResolution[] = [];
    for (const name of names) {
      if (!NAME.test(name)) throw new Error("observation_failed");
      if (match && !name.startsWith(match.producer + "-")) continue;
      const path = NodePath.join(directory, name),
        record = await load(path, bound);
      if (
        match &&
        canonical([...record.roots].sort()) !==
          canonical([...new Set(match.roots.map((root) => NodePath.resolve(root)))].sort())
      )
        continue;
      if (record.status !== "active" && record.outcome !== "unresolved") continue;
      results.push(await reconcile({ path, record }));
    }
    return results;
  });
}
export async function beginProduction(
  worktree: string,
  producer: string,
  roots: readonly string[],
): Promise<ProductionSession> {
  const bound = await binding(worktree);
  if (
    !/^[a-z0-9][a-z0-9._-]{0,23}$/.test(producer) ||
    roots.some(
      (root) =>
        !inside(NodePath.resolve(root), bound.path) ||
        NodePath.resolve(root) === bound.path ||
        inside(NodePath.resolve(root), bound.git_dir),
    )
  )
    throw new Error("invalid_producer");
  const self = await producerIdentity();
  return guarded(bound, async () => {
    const directory = NodePath.join(bound.git_dir, "workspace-output-obligations");
    await NodeFSP.mkdir(directory, { mode: 0o700 }).catch((cause) => {
      if (cause.code !== "EEXIST") throw cause;
    });
    const { names, candidate } = await capacity(bound, {
      producer,
      roots: roots.map((root) => NodePath.resolve(root)),
    });
    for (const name of names) {
      if (!NAME.test(name) || !name.startsWith(producer + "-")) continue;
      const path = NodePath.join(directory, name),
        record = await load(path, bound);
      if (
        canonical([...record.roots].sort()) ===
          canonical([...new Set(roots.map((root) => NodePath.resolve(root)))].sort()) &&
        (record.status === "active" || record.outcome === "unresolved")
      )
        await reconcile({ path, record });
    }
    if (names.length === 10000 && !candidate)
      throw new Error(
        `capacity_unrecoverable; path=${directory}; count=10000; no_supersession_candidate`,
      );
    const id = producer + "-" + NodeCrypto.randomBytes(16).toString("hex");
    const session: ProductionSession = {
      path: NodePath.join(directory, id + ".json"),
      record: {
        schema: "workspace-output-obligation/v1",
        id,
        producer,
        owner: producer,
        worktree: bound.path,
        binding: bound,
        started_at: new Date().toISOString(),
        status: "active",
        outcome: "unresolved",
        proofStatus: "deferred",
        roots: [...new Set(roots.map((root) => NodePath.resolve(root)))].sort(),
        sidecars: [],
        reasons: ["producer_active"],
        rev: 1,
        holders: {
          version: 1,
          self,
          in_process: "running",
          admission_closed: false,
          self_discharge: null,
          spawns: [],
          overflow: false,
        },
        ...(candidate
          ? { supersedes: { id: candidate.record.id, rev: candidate.record.rev ?? null } }
          : {}),
      },
    };
    await save(session, null);
    if (candidate) {
      const removed = await prune(session.path, bound, names.length + 1);
      if (!removed && names.length === 10000)
        throw new Error(
          `capacity_unrecoverable; path=${directory}; count=10001; successor=${session.path}`,
        );
    }
    return session;
  });
}
export async function productionSpawnIntent(session: ProductionSession): Promise<string | null> {
  let id: string | null = NodeCrypto.randomBytes(16).toString("hex");
  await guarded(session.record.binding, () =>
    change(session, (record) => {
      const h = record.holders!;
      if (h.admission_closed) throw new Error("spawn_admission_closed");
      if (h.overflow || Buffer.byteLength(canonical(record)) > 56000) {
        h.overflow = true;
        id = null;
      } else h.spawns.push({ id: id!, state: "pending" });
    }),
  );
  return id;
}
export async function productionSpawnResult(
  session: ProductionSession,
  intent: string | null,
  result: { kind: "none" } | { kind: "session"; pid: number; detached: boolean },
) {
  if (intent === null) return;
  await guarded(session.record.binding, () =>
    change(session, (record) => {
      const h = record.holders!,
        index = h.spawns.findIndex((spawn) => spawn.id === intent);
      if (h.admission_closed || index < 0 || h.spawns[index]!.state !== "pending")
        throw new Error("spawn_binding_uncertain");
      if (result.kind === "none") h.spawns[index] = { id: intent, state: "none" };
      else if (result.detached && Number.isSafeInteger(result.pid) && result.pid > 0)
        h.spawns[index] = { id: intent, state: "session", sid: result.pid, discharged: false };
    }),
  );
}
export async function finishProduction(
  session: ProductionSession,
  status: Attestation["status"] | undefined,
  extraReasons: readonly string[] = [],
  options: { verifyContent?: boolean; inProcessSettled?: boolean } = {},
) {
  if (session.record.status !== "active") throw new Error("producer_already_finalized");
  const sidecars: string[] = [],
    reasons = [...extraReasons];
  let proofStatus: Production["proofStatus"] = session.record.proofStatus;
  if (options.verifyContent && status) {
    for (const root of session.record.roots)
      if (await exists(root)) {
        try {
          const proof = await attest({
            worktree: session.record.worktree,
            root,
            producer: session.record.producer,
            status,
            expected: session.record.binding,
            participantGuard,
          });
          sidecars.push(proof.sidecar);
          if (proof.roots[0]?.verification !== "content") reasons.push("attestation_unverifiable");
        } catch {
          reasons.push("attestation_unverifiable");
        }
      }
    proofStatus = reasons.includes("attestation_unverifiable")
      ? "unavailable"
      : sidecars.length
        ? "attested"
        : "deferred";
  }
  return guarded(session.record.binding, async () => {
    await change(session, (record) => {
      if (status) {
        record.status = status;
        record.finished_at = new Date().toISOString();
      }
      record.sidecars = [...new Set([...record.sidecars, ...sidecars])];
      record.proofStatus = proofStatus;
      record.reasons = [...new Set([...record.reasons, ...reasons])];
      if (
        extraReasons.includes("writer_completion_not_observed") &&
        record.holders!.spawns.length === 0
      )
        record.holders!.overflow = true;
      // Existing finish callers assert completion. Interruptible adapters withhold
      // this assertion whenever an underlying write may still be in flight.
      if (
        options.inProcessSettled ??
        (status !== undefined && !extraReasons.includes("writer_completion_not_observed"))
      )
        record.holders!.in_process = "settled";
      record.holders!.admission_closed = true;
    });
    await reconcile(session);
    return session.record;
  });
}
export async function outputObligations(worktree: string) {
  const bound = await binding(worktree),
    directory = NodePath.join(bound.git_dir, "workspace-output-obligations");
  if (!(await exists(directory))) return [];
  const dir = await openAbsolute(directory, true);
  await dir.close();
  const names = await NodeFSP.readdir(directory);
  if (names.length > 10000) throw new Error("bound_exceeded");
  const results: Production[] = [];
  for (const name of names) {
    if (!NAME.test(name)) throw new Error("observation_failed");
    const record = await load(NodePath.join(directory, name), bound);
    if (
      record.outcome === "unresolved" ||
      (record.status === "active" && !allocationResolved(record))
    )
      results.push(record);
  }
  return results;
}
