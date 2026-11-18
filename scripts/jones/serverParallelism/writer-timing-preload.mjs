// Loaded explicitly by the synthetic writer diagnostic. Never passed to the server or children.
import * as NodeDiagnosticsChannel from "node:diagnostics_channel";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeWorkerThreads from "node:worker_threads";

const fileKind = /^(baseline|combined)-[0-2]\.sqlite$/;
const maximumTransactions = 1500;
const maximumCalls = 64;

function statementKind(sql) {
  const operation = sql.trim();
  if (/^BEGIN IMMEDIATE$/i.test(operation)) return "begin";
  if (/^INSERT INTO synthetic_load\(/i.test(operation)) return "insert";
  if (/^COMMIT$/i.test(operation)) return "commit";
  if (/^ROLLBACK$/i.test(operation)) return "rollback";
  if (/^PRAGMA wal_checkpoint\(NOOP\)$/i.test(operation)) return "wal-frame-count";
  if (/^PRAGMA journal_size_limit$/i.test(operation)) return "wal-size-limit";
  return "other";
}

if (NodeWorkerThreads.isMainThread) {
  const output = process.env.JONES_SP_WRITER_TRACE;
  const entry = NodePath.basename(process.argv[1] ?? "");
  if (!NodePath.isAbsolute(output ?? "") || !["baseline.mjs", "qualify.mjs"].includes(entry))
    throw new Error("Writer timing requires a synthetic qualification entry point and trace path");

  const statements = new WeakMap();
  const transactions = [];
  let current;
  let incomplete = false;

  NodeDiagnosticsChannel.channel("jones:server-parallelism:writer-timing").subscribe(
    ({ databaseName, index, start, end }) => {
      if (
        !fileKind.test(databaseName) ||
        !Number.isSafeInteger(index) ||
        index < 0 ||
        index >= 500 ||
        transactions.length >= maximumTransactions
      ) {
        incomplete = true;
        return;
      }
      if (start !== undefined) {
        if (current !== undefined) incomplete = true;
        current = { databaseName, index, start, cpu: process.threadCpuUsage(), calls: [] };
      } else if (current?.databaseName === databaseName && current.index === index) {
        const { cpu, ...timing } = current;
        const { user, system } = process.threadCpuUsage(cpu);
        transactions.push({ ...timing, end, cpuMs: (user + system) / 1000 });
        current = undefined;
      } else incomplete = true;
    },
  );

  const measure = (metadata, operation, run) => {
    const timing = current;
    if (timing === undefined || timing.databaseName !== metadata?.databaseName) return run();
    if (timing.calls.length >= maximumCalls) {
      incomplete = true;
      return run();
    }
    const start = performance.now();
    const cpu = process.threadCpuUsage();
    let succeeded = false;
    try {
      const result = run();
      succeeded = true;
      return result;
    } finally {
      const end = performance.now();
      const { user, system } = process.threadCpuUsage(cpu);
      timing.calls.push({
        kind: metadata.kind,
        operation,
        start,
        end,
        cpuMs: (user + system) / 1000,
        succeeded,
      });
    }
  };

  const originalPrepare = NodeSqlite.DatabaseSync.prototype.prepare;
  NodeSqlite.DatabaseSync.prototype.prepare = function (sql, ...args) {
    const databaseName = NodePath.basename(this.location() ?? "");
    if (!fileKind.test(databaseName)) return originalPrepare.call(this, sql, ...args);
    const metadata = { databaseName, kind: statementKind(sql) };
    const statement = measure(metadata, "prepare", () => originalPrepare.call(this, sql, ...args));
    statements.set(statement, metadata);
    return statement;
  };

  for (const operation of ["run", "get", "all"]) {
    const original = NodeSqlite.StatementSync.prototype[operation];
    NodeSqlite.StatementSync.prototype[operation] = function (...args) {
      return measure(statements.get(this), operation, () => original.apply(this, args));
    };
  }

  process.once("exit", () => {
    NodeFS.writeFileSync(
      output,
      JSON.stringify({
        schema: "jones.server-parallelism.writer-timing/v1",
        source: process.env.JONES_SP_SOURCE_SHA,
        sourceTree: process.env.JONES_SP_SOURCE_TREE,
        node: process.versions.node,
        entry,
        complete:
          !incomplete && current === undefined && transactions.length === maximumTransactions,
        transactions,
      }) + "\n",
      { flag: "wx", mode: 0o600 },
    );
  });
}
