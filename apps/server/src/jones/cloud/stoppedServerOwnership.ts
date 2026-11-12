// @effect-diagnostics nodeBuiltinImport:off
// The standalone launcher shares the server's lifetime lock without loading Effect.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

const missing = (cause: unknown) => (cause as NodeJS.ErrnoException)?.code === "ENOENT";
const blocked = () =>
  new Error(
    "state-dir-owned: Another server may own this home. Stop that server, then retry Install; no state was restored.",
  );

/** Hold the same exclusive transaction as serverOwnership.ts; never unlink its lock file. */
export function acquireStoppedServerOwnership(baseDir: string): () => void {
  const userdata = NodePath.join(baseDir, "userdata");
  const directory = NodeFS.lstatSync(userdata);
  if (!directory.isDirectory() || directory.isSymbolicLink())
    throw new Error(
      "State ownership requires a real userdata directory. Restore its directory path, then retry Install.",
    );
  const lock = NodePath.join(userdata, "server-owner.sqlite");
  try {
    const stat = NodeFS.lstatSync(lock);
    if (!stat.isFile() || stat.isSymbolicLink())
      throw new Error(
        "The server ownership file is not a regular file. Reconcile it before retrying Install.",
      );
  } catch (cause) {
    if (!missing(cause)) throw cause;
  }
  const database = new NodeSqlite.DatabaseSync(lock);
  try {
    database.exec("PRAGMA busy_timeout = 0; BEGIN EXCLUSIVE;");
    // Pre-lock servers publish a runtime record. A known live legacy owner still blocks recovery.
    let runtime: unknown;
    try {
      runtime = JSON.parse(
        NodeFS.readFileSync(NodePath.join(userdata, "server-runtime.json"), "utf8"),
      );
    } catch (cause) {
      if (!missing(cause))
        throw new Error(
          "The server runtime record is unreadable. Stop the server and reconcile its record before retrying Install.",
          { cause },
        );
    }
    if (
      runtime &&
      typeof runtime === "object" &&
      "pid" in runtime &&
      !("ownerId" in runtime) &&
      typeof runtime.pid === "number" &&
      Number.isSafeInteger(runtime.pid) &&
      runtime.pid > 0
    ) {
      let live = true;
      try {
        process.kill(runtime.pid, 0);
      } catch (cause) {
        if ((cause as NodeJS.ErrnoException)?.code === "ESRCH") live = false;
      }
      if (live) throw blocked();
    }
  } catch (cause) {
    database.close();
    if (
      cause instanceof Error &&
      (("errcode" in cause && cause.errcode === 5) ||
        ("code" in cause && cause.code === "SQLITE_BUSY"))
    )
      throw blocked();
    throw cause;
  }
  return () => database.close();
}

export async function withStoppedServerOwnership<A>(
  baseDir: string,
  body: () => Promise<A>,
): Promise<A> {
  const release = acquireStoppedServerOwnership(baseDir);
  try {
    return await body();
  } finally {
    release();
  }
}
