// @effect-diagnostics nodeBuiltinImport:off -- C4 common-store protocol is shared with V3, independent of Jones' database.
import * as NodeProcess from "node:process";
import * as NodeOS from "node:os";
import * as NodeFSP from "node:fs/promises";
import * as NodeChildProcess from "node:child_process";
import { canonical, nativePlatform } from "./Filesystem.ts";
import { incarnation, type Binding } from "./OutputAttestation.ts";
import { reservationTransaction } from "./ParticipantGuard.ts";

export interface ReservationRecord {
  schema: "workspace-reservation/v1";
  admin_id: string;
  incarnation: string;
  executor: string;
  receipt: string;
  host: string;
  boot_id: string;
  pid: number;
  process_start: string;
}
const command = (file: string, args: string[]) =>
  new Promise<string>((resolve, reject) =>
    NodeChildProcess.execFile(
      file,
      args,
      {
        timeout: 2000,
        maxBuffer: 4096,
        encoding: "utf8",
        env: { ...NodeProcess.env, LC_ALL: "C" },
      },
      (error, out) => (error ? reject(error) : resolve(out.trim())),
    ),
  );
// Reservation recovery identifies the actual executor PID, independent of injected application runtime values.
export const nativePid = process.pid;
export async function holderIdentity() {
  const boot_id =
    nativePlatform === "linux"
      ? (await NodeFSP.readFile("/proc/sys/kernel/random/boot_id", "utf8")).trim()
      : nativePlatform === "darwin"
        ? await command("/usr/sbin/sysctl", ["-n", "kern.boottime"])
        : "";
  const process_start = await command("/bin/ps", ["-p", String(nativePid), "-o", "lstart="]);
  if (!boot_id || !process_start) throw new Error("holder_identity_unavailable");
  return { host: NodeOS.hostname(), boot_id, pid: nativePid, process_start };
}
// The start time of a live PID, or null only when ps completed and reported no such process.
function processStart(pid: number) {
  return new Promise<string | null>((resolve, reject) =>
    NodeChildProcess.execFile(
      "/bin/ps",
      ["-p", String(pid), "-o", "lstart="],
      {
        timeout: 2000,
        maxBuffer: 4096,
        encoding: "utf8",
        env: { ...NodeProcess.env, LC_ALL: "C" },
      },
      (error, out) => {
        if (!error) return resolve(out.trim() || null);
        const failure = error as { code?: unknown; killed?: unknown; signal?: unknown };
        if (failure.code === 1 && failure.killed !== true && !failure.signal && !out.trim())
          return resolve(null);
        reject(error);
      },
    ),
  );
}
// Absence is proven only on this host: a different boot, a dead or reused PID, or this very
// process when the caller already holds the target's in-process lease. Anything else is live.
export async function holderAbsent(prior: ReservationRecord, leaseHeld: boolean) {
  const current = await holderIdentity();
  if (prior.host !== current.host) return false;
  if (prior.boot_id !== current.boot_id) return true;
  if (prior.pid === current.pid && prior.process_start === current.process_start) return leaseHeld;
  const start = await processStart(prior.pid);
  return start === null || start !== prior.process_start;
}
export async function readReservation(bound: Binding) {
  return reservationTransaction(bound, async (db) => {
    const row = db
      .prepare("SELECT incarnation,record FROM reservations WHERE admin_id=?")
      .get(bound.admin_id);
    if (!row) return null;
    const prior = readRecord(row.record);
    if (prior.admin_id !== bound.admin_id || prior.incarnation !== row.incarnation)
      throw new Error("reservation_unverifiable");
    return prior;
  });
}
function readRecord(value: unknown): ReservationRecord {
  if (typeof value !== "string") throw new Error("reservation_unverifiable");
  const r = JSON.parse(value) as ReservationRecord;
  if (
    r.schema !== "workspace-reservation/v1" ||
    !Number.isSafeInteger(r.pid) ||
    r.pid <= 0 ||
    [r.admin_id, r.incarnation, r.executor, r.receipt, r.host, r.boot_id, r.process_start].some(
      (x) => typeof x !== "string" || !x,
    )
  )
    throw new Error("reservation_unverifiable");
  return r;
}
export async function acquireReservation(
  bound: Binding,
  record: ReservationRecord,
  recovery?: {
    holderAbsent: (prior: ReservationRecord) => Promise<boolean>;
    receiptReadback: (prior: ReservationRecord) => Promise<boolean>;
  },
) {
  if (record.admin_id !== bound.admin_id || record.incarnation !== incarnation(bound))
    throw new Error("identity_mismatch");
  readRecord(canonical(record));
  return reservationTransaction(bound, async (db) => {
    const row = db
      .prepare("SELECT incarnation,record FROM reservations WHERE admin_id=?")
      .get(bound.admin_id);
    if (row) {
      const prior = readRecord(row.record);
      if (prior.admin_id !== bound.admin_id || prior.incarnation !== row.incarnation)
        throw new Error("reservation_unverifiable");
      if (canonical(prior) === canonical(record)) return record;
      if (!recovery || !(await recovery.holderAbsent(prior))) throw new Error("retiring");
      if (!(await recovery.receiptReadback(prior)))
        throw new Error("reservation_recovery_required");
    }
    db.prepare("INSERT OR REPLACE INTO reservations VALUES (?,?,?)").run(
      bound.admin_id,
      record.incarnation,
      canonical(record),
    );
    return record;
  });
}
export async function releaseReservation(bound: Binding, record: ReservationRecord) {
  await reservationTransaction(bound, async (db) => {
    const row = db
      .prepare("SELECT incarnation,record FROM reservations WHERE admin_id=?")
      .get(bound.admin_id);
    if (
      !row ||
      row.incarnation !== record.incarnation ||
      canonical(readRecord(row.record)) !== canonical(record)
    )
      throw new Error("reservation_changed");
    db.prepare("DELETE FROM reservations WHERE admin_id=?").run(bound.admin_id);
  });
}
