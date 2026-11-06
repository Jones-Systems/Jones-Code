// @effect-diagnostics nodeBuiltinImport:off -- C4 SQLite adapter; lock order is common-store guard then the producer's publication.
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { exists, openAbsolute } from "./Filesystem.ts";
import type { Binding } from "./OutputAttestation.ts";

export const RESERVATION_DDL = "CREATE TABLE reservations (admin_id TEXT PRIMARY KEY, incarnation TEXT NOT NULL, record TEXT NOT NULL)";
export async function participantGuard<T>(bound: Binding, action:()=>Promise<T>): Promise<T> {
  if (!await exists(bound.git_common_dir)) return action();
  const common = await openAbsolute(bound.git_common_dir,true);
  await common.close();
  const parent = Path.join(bound.git_common_dir,"workspace-lifecycle");
  await FS.mkdir(parent,{mode:0o700}).catch(cause=>{if(cause.code !== "EEXIST") throw cause;});
  const directory = await openAbsolute(parent,true);
  await directory.close();
  const path = Path.join(parent,"reservations.sqlite3");
  if (!await exists(path)) {
    const temp = await FS.mkdtemp(Path.join(parent,".reservation-init-"));
    try {
      const staged = Path.join(temp,"store");
      const initial = new DatabaseSync(staged);
      try { initial.exec(RESERVATION_DDL+"; PRAGMA user_version=1;"); } finally {initial.close();}
      await FS.link(staged,path).catch(cause=>{if(cause.code !== "EEXIST") throw cause;});
    } finally {await FS.rm(temp,{recursive:true,force:true});}
  }
  const file = await openAbsolute(path);
  try {const s = await file.stat(); if(!s.isFile() || s.uid !== process.getuid?.()) throw new Error("reservation_unverifiable");} finally {await file.close();}
  const db = new DatabaseSync(path);
  try {
    db.exec("PRAGMA busy_timeout=5000; BEGIN IMMEDIATE");
    if (db.prepare("PRAGMA user_version").get()?.user_version !== 1 || db.prepare("SELECT sql FROM sqlite_master WHERE name='reservations'").get()?.sql !== RESERVATION_DDL) throw new Error("reservation_unverifiable");
    if (db.prepare("SELECT 1 FROM reservations WHERE admin_id=?").get(bound.admin_id)) throw new Error("retiring");
    const result = await action();
    db.exec("COMMIT"); return result;
  } finally {db.close();}
}
