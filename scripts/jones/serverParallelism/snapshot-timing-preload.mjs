import * as NodeSqlite from "node:sqlite";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeWorkerThreads from "node:worker_threads";

const path = process.env.JONES_SP_SNAPSHOT_SQL;
if (
  !NodeWorkerThreads.isMainThread ||
  !NodePath.isAbsolute(path ?? "") ||
  NodePath.basename(process.argv[1] ?? "") !== "snapshot.mjs"
) {
  throw new Error("Snapshot SQL trace requires the synthetic snapshot runner");
}
const sqlByStatement = new WeakMap();
const calls = [];
let overflow = false;

function measure(query, operation, action) {
  const startMs = performance.now();
  const startedCpu = process.threadCpuUsage();
  try {
    return action();
  } finally {
    const endMs = performance.now();
    const { user, system } = process.threadCpuUsage(startedCpu);
    if (endMs - startMs >= 0.5) {
      if (calls.length < 4096) {
        calls.push({ query, operation, startMs, endMs, cpuMs: (user + system) / 1000 });
      } else {
        overflow = true;
      }
    }
  }
}

function queryKind(sql) {
  if (sql.includes("AS forked_from_run_source_thread_id")) return "shell-thread-rows";
  if (sql.includes("AS snapshot_sequence")) return "snapshot-sequence";
  if (sql.includes("shell_item_count") || sql.includes("item_count")) return "item-count";
  if (sql.includes("orchestration_v2_projection_provider_threads")) return "provider-threads";
  if (/^\s*(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)\b/i.test(sql)) return "transaction";
  if (/^\s*SELECT\b/i.test(sql)) return "other-read";
  return "other";
}

const originalPrepare = NodeSqlite.DatabaseSync.prototype.prepare;
NodeSqlite.DatabaseSync.prototype.prepare = function (sql, ...args) {
  const kind = queryKind(sql);
  const statement = measure(kind, "prepare", () => originalPrepare.call(this, sql, ...args));
  sqlByStatement.set(statement, kind);
  return statement;
};

for (const operation of ["all", "get", "run"]) {
  const original = NodeSqlite.StatementSync.prototype[operation];
  NodeSqlite.StatementSync.prototype[operation] = function (...args) {
    return measure(sqlByStatement.get(this) ?? "unbound", operation, () =>
      original.apply(this, args),
    );
  };
}

process.once("exit", () => {
  NodeFS.writeFileSync(
    path,
    JSON.stringify({ source: process.env.JONES_SP_SOURCE_SHA, overflow, calls }),
    {
      flag: "wx",
      mode: 0o600,
    },
  );
});
