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
