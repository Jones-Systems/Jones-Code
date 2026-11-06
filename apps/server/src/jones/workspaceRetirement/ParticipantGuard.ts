// @effect-diagnostics nodeBuiltinImport:off -- C4 SQLite adapter; lock order is common-store guard then the producer's publication.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { exists, openAbsolute } from "./Filesystem.ts";
import type { Binding } from "./OutputAttestation.ts";

export const RESERVATION_DDL =
  "CREATE TABLE reservations (admin_id TEXT PRIMARY KEY, incarnation TEXT NOT NULL, record TEXT NOT NULL)";
export async function reservationTransaction<T>(
  bound: Pick<Binding, "git_common_dir" | "admin_id">,
  action: (db: NodeSqlite.DatabaseSync) => Promise<T>,
  timeoutMs = 5000,
): Promise<T> {
  if (!(await exists(bound.git_common_dir))) throw new Error("reservation_store_absent");
  const common = await openAbsolute(bound.git_common_dir, true);
  await common.close();
  const parent = NodePath.join(bound.git_common_dir, "workspace-lifecycle");
  await NodeFSP.mkdir(parent, { mode: 0o700 }).catch((cause) => {
    if (cause.code !== "EEXIST") throw cause;
  });
  const directory = await openAbsolute(parent, true);
  await directory.close();
  const path = NodePath.join(parent, "reservations.sqlite3");
  if (!(await exists(path))) {
    const temp = await NodeFSP.mkdtemp(NodePath.join(parent, ".reservation-init-"));
    try {
      const staged = NodePath.join(temp, "store");
      const initial = new NodeSqlite.DatabaseSync(staged);
      try {
        initial.exec(RESERVATION_DDL + "; PRAGMA user_version=1;");
      } finally {
        initial.close();
      }
      await NodeFSP.link(staged, path).catch((cause) => {
        if (cause.code !== "EEXIST") throw cause;
      });
    } finally {
      await NodeFSP.rm(temp, { recursive: true, force: true });
    }
  }
  const file = await openAbsolute(path);
  try {
    const s = await file.stat();
    if (!s.isFile() || s.uid !== process.getuid?.()) throw new Error("reservation_unverifiable");
  } finally {
    await file.close();
  }
  const db = new NodeSqlite.DatabaseSync(path);
  try {
    db.exec(`PRAGMA busy_timeout=${Math.max(0, Math.trunc(timeoutMs))}; BEGIN IMMEDIATE`);
    if (
      db.prepare("PRAGMA user_version").get()?.user_version !== 1 ||
      db.prepare("SELECT sql FROM sqlite_master WHERE name='reservations'").get()?.sql !==
        RESERVATION_DDL
    )
      throw new Error("reservation_unverifiable");
    const result = await action(db);
    db.exec("COMMIT");
    return result;
  } finally {
    db.close();
  }
}

export async function participantGuard<T>(
  bound: Pick<Binding, "git_common_dir" | "admin_id">,
  action: () => Promise<T>,
  timeoutMs = 5000,
): Promise<T> {
  if (!(await exists(bound.git_common_dir))) return action();
  return reservationTransaction(bound, async (db) => {
    if (db.prepare("SELECT 1 FROM reservations WHERE admin_id=?").get(bound.admin_id))
      throw new Error("retiring");
    return action();
  }, timeoutMs);
}
