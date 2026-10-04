#!/usr/bin/env node

// @effect-diagnostics nodeBuiltinImport:off -- Read-only header qualification requires O_NOFOLLOW and the same native descriptor closed in finally.
import * as NodeFS from "node:fs";
import * as NodeProcess from "node:process";
import * as NodeSqlite from "node:sqlite";

import {
  validateSyntheticFixture,
  type ValidatedSyntheticFixture,
} from "../../../scripts/performance-staging/guard.mjs";
import {
  component,
  healthCountTables,
  type HealthSchemaProfile,
  type CountName,
  emptyHealthResults,
  envelopeReserveBytes,
  HealthInputError,
  isRecord,
  parentRuntime,
  parseHealthArguments,
  parseReceiptInput,
  workerInputBytes,
  type AllocationData,
  type Component,
  type ForeignKeysData,
  type HealthFixtureSummary,
  type HealthReason,
  type HealthResults,
  type HealthStatus,
  type HealthWorkerFrame,
  type HealthWorkerInput,
  type IntegrityData,
  type MetadataData,
  type SidecarSummary,
} from "./jones-sqlite-support.ts";

function decimal(value: NodeSqlite.SQLOutputValue | undefined): string {
  if (typeof value === "bigint" && value >= 0n) return value.toString();
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return String(value);
  throw new HealthInputError("sqlite_query_failed");
}

function schemaName(value: NodeSqlite.SQLOutputValue | undefined): string {
  if (typeof value !== "string" || value.includes("\0") || Buffer.byteLength(value) > 512)
    throw new HealthInputError("record_limit");
  return value;
}

function sidecars(fixture: ValidatedSyntheticFixture): readonly SidecarSummary[] {
  const kinds = ["main", "wal", "shm", "journal"] as const;
  return fixture.layout.map((entry, index) => ({
    kind: kinds[index]!,
    present: entry.present,
    bytes: entry.present ? String(entry.size) : null,
    sha256: entry.present
      ? (fixture.receipt.manifest.find((file) => file.relativePath === entry.relativePath)
          ?.sha256 ?? null)
      : null,
  }));
}

function fixtureSummary(fixture: ValidatedSyntheticFixture): HealthFixtureSummary {
  return {
    receiptSha256: fixture.receiptSha256,
    source: fixture.receipt.creationReceipt.binding,
    rootId: fixture.receipt.creationReceipt.rootId,
    producerStep: fixture.receipt.producerStep,
    closureId: fixture.receipt.closure.closureId,
    closedLayout: sidecars(fixture),
    verifiedFiles: String(fixture.verifiedFiles),
    verifiedBytes: String(fixture.verifiedBytes),
  };
}

function qualifiedReadonlyLayout(fixture: ValidatedSyntheticFixture): boolean {
  if (fixture.layout.slice(1).some((entry) => entry.present)) return false;
  const fd = NodeFS.openSync(
    fixture.canonicalPath,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  );
  try {
    const header = Buffer.alloc(100);
    const bytes = NodeFS.readSync(fd, header, 0, header.length, 0);
    // A WAL open may create or update sidecars even with readOnly. No immutable URI is assumed.
    return (
      bytes === header.length &&
      header.subarray(0, 16).equals(Buffer.from("SQLite format 3\0")) &&
      header[18] === 1 &&
      header[19] === 1
    );
  } finally {
    NodeFS.closeSync(fd);
  }
}

function scalar(
  database: NodeSqlite.DatabaseSync,
  query: string,
): NodeSqlite.SQLOutputValue | undefined {
  const statement = database.prepare(query);
  statement.setReadBigInts(true);
  const row = statement.get();
  return row ? Object.values(row)[0] : undefined;
}

function settings(database: NodeSqlite.DatabaseSync): MetadataData["settings"] {
  const journal = scalar(database, "PRAGMA journal_mode");
  const sync = scalar(database, "PRAGMA synchronous");
  const vacuum = scalar(database, "PRAGMA auto_vacuum");
  const encoding = scalar(database, "PRAGMA encoding");
  return {
    journalMode: typeof journal === "string" ? journal : null,
    synchronous: ["off", "normal", "full", "extra"][Number(sync)] ?? null,
    foreignKeys: scalar(database, "PRAGMA foreign_keys") === 1n,
    queryOnly: scalar(database, "PRAGMA query_only") === 1n,
    busyTimeoutMs: Number(scalar(database, "PRAGMA busy_timeout")),
    pageSizeBytes: decimal(scalar(database, "PRAGMA page_size")),
    pageCount: decimal(scalar(database, "PRAGMA page_count")),
    freelistCount: decimal(scalar(database, "PRAGMA freelist_count")),
    walAutoCheckpointPages: decimal(scalar(database, "PRAGMA wal_autocheckpoint")),
    autoVacuum: ["none", "full", "incremental"][Number(vacuum)] ?? null,
    encoding: typeof encoding === "string" ? encoding : null,
  };
}

function metadata(
  database: NodeSqlite.DatabaseSync,
  fixture: ValidatedSyntheticFixture,
  maxRecords: number,
): Component<MetadataData> {
  const rows: { name: string; type: string }[] = [];
  let truncated = false;
  for (const row of database
    .prepare("SELECT name, type FROM sqlite_schema ORDER BY type, name LIMIT ?")
    .iterate(maxRecords + 1)) {
    if (rows.length === maxRecords) {
      truncated = true;
      break;
    }
    rows.push({ name: schemaName(row.name), type: schemaName(row.type) });
  }
  return component(truncated ? "truncated" : "completed", truncated ? "record_limit" : null, {
    connectionSettingsOnly: true,
    settings: settings(database),
    schema: rows,
    sidecars: sidecars(fixture),
  });
}

function counts(
  database: NodeSqlite.DatabaseSync,
  profile: HealthSchemaProfile,
): HealthResults["counts"] {
  const rows: {
    name: CountName;
    status: "completed" | "unavailable" | "failed";
    count: string | null;
  }[] = [];
  const exists = database.prepare(
    "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ? LIMIT 1",
  );
  for (const [name, table] of healthCountTables(profile)) {
    if (!exists.get(table)) rows.push({ name, status: "unavailable", count: null });
    else {
      try {
        rows.push({
          name,
          status: "completed",
          count: decimal(scalar(database, `SELECT COUNT(*) FROM "${table}"`)),
        });
      } catch {
        rows.push({ name, status: "failed", count: null });
      }
    }
  }
  const failed = rows.some((row) => row.status === "failed");
  const missing = rows.some((row) => row.status === "unavailable");
  return component(
    failed ? "failed" : missing ? "unavailable" : "completed",
    failed ? "sqlite_query_failed" : missing ? "tables_missing" : null,
    rows,
  );
}

function allocation(
  database: NodeSqlite.DatabaseSync,
  maxRecords: number,
): Component<AllocationData> {
  if (!database.prepare("SELECT 1 FROM pragma_module_list WHERE name = 'dbstat' LIMIT 1").get())
    return component("unavailable", "dbstat_unavailable");
  const statement = database.prepare(
    "SELECT name, pageno AS pages, pgsize AS bytes, payload AS payload_bytes, unused AS unused_bytes FROM dbstat WHERE aggregate = TRUE ORDER BY name LIMIT ?",
  );
  statement.setReadBigInts(true);
  const rows: AllocationData["rows"][number][] = [];
  let truncated = false;
  for (const row of statement.iterate(maxRecords + 1)) {
    if (rows.length === maxRecords) {
      truncated = true;
      break;
    }
    rows.push({
      name: schemaName(row.name),
      pages: decimal(row.pages),
      bytes: decimal(row.bytes),
      payloadBytes: decimal(row.payload_bytes),
      unusedBytes: decimal(row.unused_bytes),
    });
  }
  return component(truncated ? "truncated" : "completed", truncated ? "record_limit" : null, {
    scope: "btree-pages-only",
    excludes: "freelist-and-other-non-btree-pages",
    rows,
  });
}

function integrity(
  database: NodeSqlite.DatabaseSync,
  maxDiagnostics: number,
): Component<IntegrityData> {
  const diagnostics: IntegrityData["diagnostics"][number][] = [];
  let violations = 0;
  let ok = false;
  let truncated = false;
  for (const row of database.prepare(`PRAGMA integrity_check(${maxDiagnostics + 1})`).iterate()) {
    const value = Object.values(row)[0];
    if (value === "ok") ok = true;
    else {
      violations++;
      if (diagnostics.length === maxDiagnostics) {
        truncated = true;
        break;
      }
      diagnostics.push({ code: "integrity_violation" });
    }
  }
  const passed = truncated ? null : ok && violations === 0;
  return component(
    truncated ? "truncated" : passed ? "completed" : "failed",
    truncated ? "diagnostic_limit" : passed ? null : "integrity_violations",
    { check: "full-integrity-check", passed, observedViolations: String(violations), diagnostics },
  );
}

function foreignKeys(
  database: NodeSqlite.DatabaseSync,
  maxDiagnostics: number,
): Component<ForeignKeysData> {
  const diagnostics: ForeignKeysData["diagnostics"][number][] = [];
  let violations = 0;
  let truncated = false;
  const statement = database.prepare("PRAGMA foreign_key_check");
  statement.setReadBigInts(true);
  for (const row of statement.iterate()) {
    violations++;
    if (diagnostics.length === maxDiagnostics) {
      truncated = true;
      break;
    }
    diagnostics.push({
      table: schemaName(row.table),
      parent: schemaName(row.parent),
      constraintIndex: decimal(row.fkid),
    });
  }
  return component(
    truncated ? "truncated" : violations === 0 ? "completed" : "failed",
    truncated ? "diagnostic_limit" : violations === 0 ? null : "foreign_key_violations",
    {
      passed: truncated ? null : violations === 0,
      observedViolations: String(violations),
      diagnostics,
    },
  );
}

function decodeInput(text: string | undefined): HealthWorkerInput {
  if (!text || Buffer.byteLength(text) > workerInputBytes)
    throw new HealthInputError("worker_input_too_large");
  const value: unknown = JSON.parse(text);
  if (
    !isRecord(value) ||
    value.schema !== "jones.sqlite-health-worker-input/v1" ||
    !isRecord(value.request) ||
    !isRecord(value.fixture) ||
    !isRecord(value.policy) ||
    !isRecord(value.request.limits)
  )
    throw new HealthInputError("invalid_receipt_input");
  const raw = value.request;
  const limits = value.request.limits;
  const args = [
    "--schema-profile",
    String(raw.schemaProfile ?? "legacy-v1"),
    "--fixture-root",
    String(raw.fixtureRoot),
    "--fixture-receipt-sha256",
    String(raw.fixtureReceiptSha256),
    "--fixture-binding-json",
    JSON.stringify(raw.fixtureBinding),
    "--deadline-ms",
    String(limits.deadlineMs),
    "--max-output-bytes",
    String(limits.maxOutputBytes),
    "--max-stderr-bytes",
    String(limits.maxStderrBytes),
    "--max-records",
    String(limits.maxRecords),
    "--max-diagnostics",
    String(limits.maxDiagnostics),
  ];
  if (!Array.isArray(raw.include)) throw new HealthInputError("invalid_arguments");
  if (raw.include.length) args.push("--include", raw.include.join(","));
  const request = parseHealthArguments(args);
  const fixture = parseReceiptInput(JSON.stringify(value.fixture.receipt), request.fixtureBinding);
  return {
    schema: "jones.sqlite-health-worker-input/v1",
    request,
    fixture,
    policy: value.policy as unknown as HealthWorkerInput["policy"],
  };
}

export async function runHealthWorker(
  input: HealthWorkerInput,
  emit: (frame: HealthWorkerFrame) => void,
): Promise<HealthStatus> {
  const results = emptyHealthResults(input.request.include);
  let status: HealthStatus = "completed";
  let reason: HealthReason | null = null;
  let fixture: ValidatedSyntheticFixture | null = null;
  let database: NodeSqlite.DatabaseSync | null = null;
  emit({ kind: "runtime", runtime: parentRuntime() });
  const result = <Name extends keyof HealthResults>(name: Name, value: HealthResults[Name]) => {
    Object.assign(results, { [name]: value });
    emit({ kind: "result", name, result: value });
    if (value.status === "failed") {
      status = "failed";
      reason = value.reason;
    } else if (value.status !== "completed" && value.status !== "omitted" && status !== "failed") {
      status = "partial";
      reason = value.reason;
    }
  };
  try {
    fixture = await validateSyntheticFixture({
      receipt: input.fixture.receipt,
      expectedReceiptSha256: input.request.fixtureReceiptSha256,
      expectedBinding: input.request.fixtureBinding,
      policy: input.policy,
    });
    if (fixture.receipt.creationReceipt.canonicalRootPath !== input.request.fixtureRoot)
      throw new HealthInputError("fixture_root_mismatch");
    emit({ kind: "fixture", fixture: fixtureSummary(fixture) });
    if (!qualifiedReadonlyLayout(fixture)) {
      result("readonly", component("unavailable", "readonly_layout_unqualified"));
      status = "refused";
      reason = "readonly_layout_unqualified";
    } else {
      try {
        database = new NodeSqlite.DatabaseSync(fixture.canonicalPath, {
          readOnly: true,
          allowExtension: false,
          timeout: 0,
          enableForeignKeyConstraints: true,
        });
      } catch {
        throw new HealthInputError("readonly_open_unavailable");
      }
      database.exec("PRAGMA query_only = ON");
      const version = scalar(database, "SELECT sqlite_version()");
      const sourceId = scalar(database, "SELECT sqlite_source_id()");
      emit({
        kind: "runtime",
        runtime: {
          ...parentRuntime(),
          sqliteVersion: typeof version === "string" ? version : null,
          sqliteSourceId: typeof sourceId === "string" ? sourceId : null,
        },
      });
      const check = <Name extends keyof HealthResults>(
        name: Name,
        run: () => HealthResults[Name],
      ) => {
        try {
          result(name, run());
        } catch (error) {
          if (error instanceof HealthInputError && error.reason === "output_limit") throw error;
          result(
            name,
            component<never>(
              error instanceof HealthInputError && error.reason === "record_limit"
                ? "truncated"
                : "failed",
              error instanceof HealthInputError ? error.reason : "sqlite_query_failed",
            ),
          );
        }
      };
      check("metadata", () => metadata(database!, fixture!, input.request.limits.maxRecords));
      if (input.request.include.includes("counts"))
        check("counts", () => counts(database!, input.request.schemaProfile));
      if (input.request.include.includes("allocation"))
        check("allocation", () => allocation(database!, input.request.limits.maxRecords));
      if (input.request.include.includes("integrity"))
        check("integrity", () => integrity(database!, input.request.limits.maxDiagnostics));
      if (input.request.include.includes("foreign-keys"))
        check("foreignKeys", () => foreignKeys(database!, input.request.limits.maxDiagnostics));
    }
  } catch (error) {
    reason =
      error instanceof HealthInputError
        ? error.reason
        : fixture
          ? "sqlite_query_failed"
          : "fixture_refused";
    status = reason === "output_limit" ? "partial" : fixture ? "refused" : "refused";
  } finally {
    if (database) {
      try {
        database.close();
      } catch {
        status = "failed";
        reason = "sqlite_close_failed";
      }
    }
    if (fixture && database) {
      try {
        const after = await validateSyntheticFixture({
          receipt: input.fixture.receipt,
          expectedReceiptSha256: input.request.fixtureReceiptSha256,
          expectedBinding: input.request.fixtureBinding,
          policy: input.policy,
        });
        result(
          "readonly",
          component("completed", null, {
            unchanged: true,
            beforeReceiptSha256: fixture.receiptSha256,
            afterReceiptSha256: after.receiptSha256,
          }),
        );
      } catch (error) {
        if (error instanceof HealthInputError && error.reason === "output_limit") {
          status = "partial";
          reason = "output_limit";
        } else {
          result(
            "readonly",
            component("unavailable", "fixture_changed", {
              unchanged: null,
              beforeReceiptSha256: fixture.receiptSha256,
              afterReceiptSha256: null,
            }),
          );
          if (status !== "failed") {
            status = "partial";
            reason = "fixture_changed";
          }
        }
      }
    }
  }
  emit({ kind: "complete", status, reason });
  return status;
}

if (import.meta.main) {
  let written = 0;
  let budget = 16 * 1024;
  const emit = (frame: HealthWorkerFrame) => {
    const text = `${JSON.stringify(frame)}\n`;
    const limit = frame.kind === "complete" ? budget : budget - 256;
    if (written + Buffer.byteLength(text) > limit) throw new HealthInputError("output_limit");
    written += Buffer.byteLength(text);
    NodeProcess.stdout.write(text);
  };
  try {
    if (NodeProcess.argv.length !== 4 || NodeProcess.argv[2] !== "health")
      throw new HealthInputError("invalid_arguments");
    const input = decodeInput(NodeProcess.argv[3]);
    budget = input.request.limits.maxOutputBytes - envelopeReserveBytes;
    const status = await runHealthWorker(input, emit);
    process.exitCode =
      status === "completed" ? 0 : status === "failed" ? 1 : status === "refused" ? 2 : 3;
  } catch (error) {
    const reason = error instanceof HealthInputError ? error.reason : "worker_failed";
    emit({ kind: "complete", status: reason === "output_limit" ? "partial" : "failed", reason });
    process.exitCode = reason === "output_limit" ? 3 : 1;
  }
}
