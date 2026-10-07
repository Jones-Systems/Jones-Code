// @effect-diagnostics nodeBuiltinImport:off -- Native CLI fixtures capture child PIDs, watch ready barriers and compare exact filesystem identities and bytes.
// @effect-diagnostics globalTimers:off -- The fixture creator bounds captured-PID termination/reap and clears its timers on close and finally.
// @effect-diagnostics globalConsole:off -- Failure diagnostics preserve the original test error and report bounded native stderr or the retained fixture path.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import * as NodeSqlite from "node:sqlite";
import * as NodeURL from "node:url";
import { describe, expect, it } from "@effect/vitest";

import {
  withClosedSyntheticFixture,
  fixtureCustodyReceipt,
  type ClosedFixtureConsumerOutcome,
  type ClosedSyntheticFixture,
  type SyntheticFixtureError,
} from "../../../../scripts/jones/performance/fixtures.mjs";
import {
  assertOwnedDatabase,
  createOwnedRoot,
  disposeOwnedRoot,
  observeSyntheticClose,
  sealSyntheticFixture,
  syntheticFixtureReceiptSha256,
  type OwnedChildReceipt,
  type OwnedRoot,
  type PerformanceBinding,
  type SyntheticFixtureReceipt,
} from "../../../../scripts/jones/performance/guard.mjs";
import { runOwnedChild } from "../../../../scripts/jones/performance/lifecycle.mjs";
import { currentDatabaseSource } from "../../../../scripts/jones/performance/sources.mjs";
import { runSqliteHealth, sqliteHealthConsumerOutcome } from "./sqliteHealth.ts";
import {
  countTables,
  v2CountTables,
  encodeHealthEnvelope,
  healthExitCode,
  healthPolicy,
  parseHealthArguments,
  parseReceiptInput,
  workerInputBytes,
  reapTimeoutMs,
  terminateGraceMs,
  workerPath,
  worktreePath,
  type HealthEnvelope,
} from "./sqliteHealth.support.ts";

const cliPath = NodeURL.fileURLToPath(new URL("./sqliteHealth.ts", import.meta.url));
const payloadSentinel = "synthetic-message-content-must-never-be-reported";
const producerStep = "spec.jones-performance-portfolio#task.d-health.001:readonly-test-fixture";

interface Fixture {
  readonly scratch: string;
  readonly owner: OwnedRoot;
  readonly root: string;
  readonly binding: PerformanceBinding;
  readonly receipt: SyntheticFixtureReceipt;
  readonly pin: string;
  readonly dbstat: boolean;
  readonly custody: { unknownConsumer: boolean };
}

async function withFixture(
  body: (fixture: Fixture) => Promise<void>,
  configure?: (database: NodeSqlite.DatabaseSync, databasePath: string) => void,
) {
  const scratch = NodeFS.mkdtempSync(NodePath.join(NodePath.dirname(worktreePath), ".health-test-"));
  const binding = {
    repository: "Jones-Systems/Jones-Code",
    sourceRevision: "73b1702f6d5bde5e16636951f637e5416e5a5a6d",
    taskRef: "spec.jones-performance-portfolio#task.d-health.001",
    runId: NodePath.basename(scratch),
  };
  let owner: OwnedRoot | undefined;
  let permit: ReturnType<typeof assertOwnedDatabase> | undefined;
  let database: NodeSqlite.DatabaseSync | undefined;
  const custody = { unknownConsumer: false };
  let closed = false;
  let failure: unknown;
  try {
    owner = createOwnedRoot({
      parentPath: scratch,
      childName: "fixture",
      binding,
      policy: healthPolicy(),
    });
    permit = assertOwnedDatabase(owner, {
      databaseRelativePath: "state.sqlite",
      access: "create",
    });
    database = new NodeSqlite.DatabaseSync(permit.canonicalPath);
    database.exec("PRAGMA journal_mode = DELETE");
    for (const [, table] of countTables) {
      database.exec(`CREATE TABLE "${table}" (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)`);
      database
        .prepare(`INSERT INTO "${table}" (id, payload) VALUES (?, ?)`)
        .run(1, payloadSentinel);
    }
    configure?.(database, permit.canonicalPath);
    const dbstat = !!database
      .prepare("SELECT 1 FROM pragma_module_list WHERE name = 'dbstat'")
      .get();
    const proof = await observeSyntheticClose(owner, {
      permit,
      producerStep,
      resource: database,
      close: (resource) => {
        resource.close();
        closed = true;
      },
    });
    const receipt = await sealSyntheticFixture(owner, {
      databaseRelativePath: "state.sqlite",
      producerStep,
      closedProof: proof,
    });
    await body({
      scratch,
      owner,
      root: owner.creationReceipt.canonicalRootPath,
      binding,
      receipt,
      pin: syntheticFixtureReceiptSha256(receipt),
      dbstat,
      custody,
    });
  } catch (error) {
    failure = error;
  } finally {
    if (database && owner && permit && !closed) {
      try {
        await observeSyntheticClose(owner, {
          permit,
          producerStep,
          resource: database,
          close: (resource) => {
            resource.close();
            closed = true;
          },
        });
      } catch (error) {
        failure ??= error;
      }
    }
    const cleanup = owner && !custody.unknownConsumer && (!database || closed)
      ? disposeOwnedRoot(owner)
      : null;
    if (owner && (!cleanup || cleanup.outcome !== "complete")) {
      const cleanupFailure = new Error(`fixture cleanup retained exact root ${scratch}`);
      if (failure) console.error(cleanupFailure.message);
      else failure = cleanupFailure;
    } else {
      try {
        NodeFS.rmSync(scratch, { recursive: true, force: false });
      } catch (error) {
        if (failure) console.error(`fixture scratch cleanup failed at ${scratch}`);
        else failure = error;
      }
    }
  }
  if (failure) throw failure;
}

function argumentsFor(fixture: Fixture, extra: readonly string[] = []) {
  return [
    "--fixture-root",
    fixture.root,
    "--fixture-receipt-sha256",
    fixture.pin,
    "--fixture-binding-json",
    JSON.stringify(fixture.binding),
    ...(extra.includes("--schema-profile") ? [] : ["--schema-profile", "legacy-v1"]),
    ...extra,
  ];
}

function snapshot(root: string) {
  return ["", "-wal", "-shm", "-journal"].map((suffix) => {
    const path = NodePath.join(root, `state.sqlite${suffix}`);
    if (!NodeFS.existsSync(path)) return { suffix, present: false };
    const stat = NodeFS.lstatSync(path, { bigint: true });
    return {
      suffix,
      present: true,
      device: stat.dev.toString(),
      inode: stat.ino.toString(),
      size: stat.size.toString(),
      mtime: stat.mtimeNs.toString(),
      ctime: stat.ctimeNs.toString(),
      sha256: NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(path)).digest("hex"),
    };
  });
}

async function cli(
  fixture: Fixture,
  args: readonly string[],
  input: string | Buffer,
  leaveStdinOpen = false,
) {
  const child = NodeChildProcess.spawn(NodeProcess.execPath, [cliPath, ...args], {
    cwd: worktreePath,
    env: { HOME: NodeOS.homedir(), NODE_NO_WARNINGS: "1" },
    shell: false,
    stdio: ["pipe", "pipe", "pipe"],
  });
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  let bytes = 0;
  let failure: unknown;
  let observedClose = false;
  let observedConsumerClose = false;
  let stopped = false;
  let grace: ReturnType<typeof setTimeout> | undefined;
  let reap: ReturnType<typeof setTimeout> | undefined;
  let finish: (value: {
    code: number | null;
    signal: NodeJS.Signals | null;
    closed: boolean;
  }) => void;
  const closed = new Promise<{
    code: number | null;
    signal: NodeJS.Signals | null;
    closed: boolean;
  }>((resolve) => {
    finish = resolve;
  });
  const stop = () => {
    if (observedClose || stopped) return;
    stopped = true;
    child.kill("SIGTERM");
    grace = setTimeout(() => {
      if (observedClose) return;
      child.kill("SIGKILL");
      reap = setTimeout(() => {
        if (observedClose) return;
        child.unref();
        child.stdout.destroy();
        child.stderr.destroy();
        finish({ code: null, signal: null, closed: false });
      }, reapTimeoutMs);
    }, terminateGraceMs);
  };
  const deadline = setTimeout(() => {
    failure ??= new Error("CLI test lifetime exceeded");
    stop();
  }, 15_000);
  const capture = (chunks: Buffer[], chunk: Buffer) => {
    bytes += chunk.length;
    if (bytes > 1024 * 1024) {
      failure ??= new Error("CLI test output exceeded its bound");
      stop();
      return;
    }
    chunks.push(Buffer.from(chunk));
  };
  child.stdout.on("data", (chunk: Buffer) => capture(stdout, chunk));
  child.stderr.on("data", (chunk: Buffer) => capture(stderr, chunk));
  child.stdin.on("error", () => {});
  child.once("error", (error) => {
    failure ??= error;
    stop();
  });
  child.once("close", (code, signal) => {
    observedClose = true;
    clearTimeout(deadline);
    clearTimeout(grace);
    clearTimeout(reap);
    finish({ code, signal, closed: true });
  });
  try {
    if (leaveStdinOpen) child.stdin.write(input);
    else child.stdin.end(input);
    const result = await closed;
    if (!result.closed) throw failure ?? new Error("CLI test child closure is unknown");
    const output = Buffer.concat(stdout).toString("utf8");
    const stderrBytes = Buffer.concat(stderr);
    const stderrText = stderrBytes.subarray(0, 16 * 1024).toString("utf8");
    const nativeDiagnostic = `CLI exit=${result.code ?? "null"} signal=${result.signal ?? "null"}; stderr=${stderrBytes.length} bytes (first 16 KiB): ${stderrText}`;
    if (failure) {
      console.error(nativeDiagnostic);
      throw failure;
    }
    if (!output.trim()) throw new Error(`CLI emitted no JSON report; ${nativeDiagnostic}`);
    let report: HealthEnvelope;
    try {
      report = JSON.parse(output) as HealthEnvelope;
    } catch {
      throw new Error(`CLI emitted an invalid JSON report; ${nativeDiagnostic}`);
    }
    observedConsumerClose = report.child.closed && report.child.reaped &&
      report.child.outcome !== "unknown" && report.cleanup.status === "completed" &&
      report.cleanup.supervisorRoot === null;
    expect(output.trim().split("\n")).toHaveLength(1);
    expect(Buffer.byteLength(output)).toBeLessThanOrEqual(report.limits.maxOutputBytes);
    return { ...result, output, report, stderr: stderrText };
  } finally {
    child.stdin.destroy();
    if (!observedClose) {
      stop();
      await closed;
    }
    clearTimeout(deadline);
    clearTimeout(grace);
    clearTimeout(reap);
    if (!observedClose || !observedConsumerClose) fixture.custody.unknownConsumer = true;
  }
}

function request(fixture: Fixture, extra: readonly string[] = []) {
  return parseHealthArguments(argumentsFor(fixture, extra));
}

function receiptInput(fixture: Fixture) {
  return parseReceiptInput(JSON.stringify(fixture.receipt), fixture.binding);
}

async function injectedWorker(
  fixture: Fixture,
  code: string,
  extra: readonly string[] = [],
  controller?: AbortController,
  onReady?: () => void,
) {
  let path: string | undefined;
  let ready = false;
  const report = await runSqliteHealth(request(fixture, extra), receiptInput(fixture), {
    ...(controller ? { signal: controller.signal } : {}),
    runChild: async (options) => {
      path = options.owner.creationReceipt.canonicalRootPath;
      const readyPath = NodePath.join(path, "ready");
      const watcher = NodeFS.watch(path, { persistent: false }, () => {
        if (!ready && NodeFS.existsSync(readyPath)) {
          ready = true;
          onReady?.();
        }
      });
      try {
        return await runOwnedChild({ ...options, args: ["--input-type=module", "-e", code] });
      } finally {
        watcher.close();
      }
    },
  });
  expect(ready).toBe(true);
  expect(report.child.pid).toBeTypeOf("number");
  expect(report.child.closed).toBe(true);
  expect(report.child.reaped).toBe(true);
  expect(report.cleanup.status).toBe("completed");
  expect(NodePath.dirname(path!)).toBe(NodePath.dirname(worktreePath));
  expect(NodeFS.existsSync(path!)).toBe(false);
  expect(NodeFS.existsSync(fixture.root)).toBe(true);
  return report;
}

const readyCode =
  "import{writeFileSync,renameSync}from'node:fs';writeFileSync('arming','ready');renameSync('arming','ready');";
const blockedCode = `${readyCode}process.on('SIGTERM',()=>{});Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0);`;

describe("jones-sqlite-health — closed fixture integration", () => {
  it("preserves the independent pin and report while requiring every closure and cleanup condition", async () => {
    await withFixture(async (fixture) => {
      const { report } = await cli(fixture, argumentsFor(fixture), JSON.stringify(fixture.receipt));
      expect(report.status).toBe("completed");
      for (const status of ["completed", "failed", "interrupted"] as const) {
        const value: HealthEnvelope = { ...report, status, fixture: null };
        const outcome = sqliteHealthConsumerOutcome(fixture.pin, value);
        expect(outcome).toEqual({
          schema: "jones-performance-fixture-consumer/v1",
          fixtureReceiptSha256: fixture.pin,
          disposition: "release",
          value,
        });
        expect(outcome.value).toBe(value);
      }
      const cases: readonly [string, HealthEnvelope][] = [
        ["unknown child outcome", { ...report, child: { ...report.child, outcome: "unknown" } }],
        ["unclosed child", { ...report, child: { ...report.child, closed: false } }],
        ["unreaped child", { ...report, child: { ...report.child, reaped: false } }],
        ["unknown cleanup", { ...report, cleanup: { ...report.cleanup, status: "unknown" } }],
        ["retained cleanup", { ...report, cleanup: { ...report.cleanup, status: "retained" } }],
        [
          "retained supervisor root despite completed cleanup",
          { ...report, cleanup: { ...report.cleanup, supervisorRoot: fixture.root } },
        ],
        [
          "not started with unknown cleanup",
          {
            ...report,
            child: { ...report.child, outcome: "not_started", pid: null },
            cleanup: { ...report.cleanup, status: "unknown" },
          },
        ],
        [
          "not started with a retained root",
          {
            ...report,
            child: { ...report.child, outcome: "not_started", pid: null },
            cleanup: { ...report.cleanup, supervisorRoot: fixture.root },
          },
        ],
      ];
      for (const [name, value] of cases) {
        const outcome = sqliteHealthConsumerOutcome(fixture.pin, value);
        expect(outcome.disposition, name).toBe("retain");
        expect(outcome.fixtureReceiptSha256, name).toBe(fixture.pin);
        expect(outcome.value, name).toBe(value);
      }
    });
  });

  it("uses the receiving V2 offline DELETE producer with separate custody and release pins and refuses a raw report", async () => {
    const databaseSource = currentDatabaseSource(worktreePath);
    const binding: PerformanceBinding = {
      repository: "Jones-Systems/Jones-Code",
      sourceRevision: databaseSource.sourceRevision,
      taskRef: "spec.jones-performance-portfolio#task.e-fixture.001",
      runId: NodeCrypto.randomUUID(),
    };
    const policy = healthPolicy();
    const owner = createOwnedRoot({
      parentPath: NodePath.dirname(worktreePath),
      childName: `.health-integration-test-${binding.runId}`,
      binding,
      policy,
    });
    const root = owner.creationReceipt.canonicalRootPath;
    const options = {
      parentPath: root,
      binding,
      policy,
      producer: "current-v2" as const,
      databaseSource,
      profile: "health-offline-delete" as const,
      recipe: { kind: "coherent-v2" as const, threads: 2, historyTurns: 3, payloadBytes: 256 },
    };
    const readHealth = (context: ClosedSyntheticFixture) =>
      runSqliteHealth(
        parseHealthArguments([
          "--fixture-root",
          context.fixture.receipt.creationReceipt.canonicalRootPath,
          "--fixture-receipt-sha256",
          context.fixture.receiptSha256,
          "--fixture-binding-json",
          JSON.stringify(binding),
          "--include",
          "counts,integrity,foreign-keys",
        ]),
        { receipt: context.fixture.receipt, expectedBinding: binding },
      );
    const consumerClosed = (report: HealthEnvelope) =>
      report.child.closed &&
      report.child.reaped &&
      report.child.outcome !== "unknown" &&
      report.cleanup.status === "completed" &&
      report.cleanup.supervisorRoot === null;
    let safeToClean = true;
    let failure: unknown;
    try {
      safeToClean = false;
      const accepted = await withClosedSyntheticFixture(
        { ...options, childName: "adapted" },
        async (context) =>
          sqliteHealthConsumerOutcome(context.receiptSha256, await readHealth(context)),
      );
      safeToClean =
        accepted.cleanup.outcome === "complete" &&
        accepted.childReceipt.closed &&
        accepted.childReceipt.reaped &&
        accepted.childReceipt.outcome !== "unknown" &&
        consumerClosed(accepted.value);
      expect(accepted.value.status).toBe("completed");
      const custodyPin = syntheticFixtureReceiptSha256(fixtureCustodyReceipt(accepted.receipt));
      expect(accepted.receipt.schema).toBe("jones-performance-fixture/v2");
      expect(accepted.receiptSha256).not.toBe(custodyPin);
      expect(accepted.value.fixture?.receiptSha256).toBe(custodyPin);
      expect(accepted.value.schemaProfile).toBe("orchestration-v2");
      expect(accepted.value.results.counts.data?.find((row) => row.name === "events")).toEqual({ name: "events", status: "completed", count: "21" });
      expect(accepted.value.results.counts.data?.find((row) => row.name === "commandReceipts")?.count).toBe("9");
      expect(accepted.value.results.counts.data?.find((row) => row.name === "threads")?.count).toBe("2");
      expect(accepted.value.results.counts.data?.find((row) => row.name === "messages")?.count).toBe("12");
      expect(accepted.value.results.counts.data?.find((row) => row.name === "runs")?.count).toBe("6");
      expect(accepted.capture.tables.orchestration_command_receipts?.count).toBe(9);
      expect(accepted.capture.tables.orchestration_v2_command_receipts?.count).toBe(0);
      expect(accepted.value.results.integrity.data?.passed).toBe(true);
      expect(accepted.value.results.foreignKeys.data?.passed).toBe(true);
      expect(accepted.value.results.readonly.data?.unchanged).toBe(true);
      expect(accepted.capture.profile?.kind).toBe("health-offline-delete");
      expect(accepted.capture.profile?.stage).toBe("sealed");
      expect(accepted.cleanup.outcome).toBe("complete");
      expect(NodeFS.existsSync(NodePath.join(root, "adapted"))).toBe(false);

      safeToClean = false;
      const raw: { context?: ClosedSyntheticFixture; report?: HealthEnvelope } = {};
      const rejected = await withClosedSyntheticFixture(
        { ...options, childName: "raw" },
        async (context) => {
          raw.context = context;
          raw.report = await readHealth(context);
          return raw.report as unknown as ClosedFixtureConsumerOutcome<HealthEnvelope>;
        },
      ).then(
        () => null,
        (error: unknown) => error,
      );
      if (!(rejected instanceof Error)) throw new Error("raw health report unexpectedly released");
      const evidence = (rejected as SyntheticFixtureError).evidence;
      // The original test owner cleans this refusal only after both captured lifetimes closed.
      safeToClean = Boolean(
        evidence?.cleanup.outcome === "retained" &&
        evidence.cleanup.reason === "invalid_consumer_outcome" &&
        raw.context &&
        raw.report &&
        evidence.childReceipt === raw.context.childReceipt &&
        evidence.receipt === raw.context.receipt &&
        raw.context.childReceipt.closed &&
        raw.context.childReceipt.reaped &&
        raw.context.childReceipt.outcome !== "unknown" &&
        raw.context.receipt.closure.completed &&
        consumerClosed(raw.report),
      );
      expect((rejected as SyntheticFixtureError).code).toBe("invalid_consumer_outcome");
      expect(evidence?.cleanup.outcome).toBe("retained");
      expect(NodeFS.existsSync(NodePath.join(root, "raw"))).toBe(true);
      expect(raw.report?.status).toBe("completed");
      expect(raw.report?.results.readonly.data?.unchanged).toBe(true);
      expect(evidence).not.toHaveProperty("value");
      expect(safeToClean).toBe(true);
    } catch (error) {
      failure = error;
    } finally {
      const cleanup = safeToClean ? disposeOwnedRoot(owner) : null;
      if (!cleanup || cleanup.outcome !== "complete") {
        const cleanupFailure = new Error(`profile test retained exact root ${root}`);
        if (failure) console.error(cleanupFailure.message);
        else failure = cleanupFailure;
      }
    }
    if (failure) throw failure;
    expect(NodeFS.existsSync(root)).toBe(false);
  }, 30_000);
});

describe("jones-sqlite-health — d-readonly", () => {
  it("attributes V2 counts to shared events and command receipts despite conflicting obsolete tables", async () => {
    await withFixture(async (fixture) => {
      const before = snapshot(fixture.root);
      const result = await cli(
        fixture,
        argumentsFor(fixture, ["--schema-profile", "orchestration-v2", "--include", "counts"]),
        JSON.stringify(fixture.receipt),
      );
      expect(result.report.schemaProfile).toBe("orchestration-v2");
      expect(result.report.results.counts.status).toBe("completed");
      expect(result.report.results.counts.data?.map((row) => [row.name, row.count])).toEqual(
        v2CountTables.map(([name]) => [name, "1"]),
      );
      expect(result.report.results.counts.data?.find((row) => row.name === "commandReceipts")).toEqual({ name: "commandReceipts", status: "completed", count: "1" });
      expect(result.report.results.counts.data?.some((row) => row.name === "activities")).toBe(false);
      expect(snapshot(fixture.root)).toEqual(before);
      expect(result.report.cleanup.status).toBe("completed");
      expect(result.report.child.reaped).toBe(true);
    }, (database) => {
      for (const [, table] of countTables) database.exec(`DROP TABLE "${table}"`);
      database.exec("CREATE TABLE orchestration_v2_events (id INTEGER PRIMARY KEY); INSERT INTO orchestration_v2_events VALUES (1),(2),(3)");
      database.exec("CREATE TABLE orchestration_v2_command_receipts (id INTEGER PRIMARY KEY); INSERT INTO orchestration_v2_command_receipts VALUES (1),(2),(3)");
      for (const [, table] of v2CountTables) {
        database.exec(`CREATE TABLE "${table}" (id INTEGER PRIMARY KEY)`);
        database.exec(`INSERT INTO "${table}" VALUES (1)`);
      }
    });
  });

  it("reports missing V2 projection tables while counting the shared authoritative events table", async () => {
    await withFixture(async (fixture) => {
      const result = await cli(
        fixture,
        argumentsFor(fixture, ["--schema-profile", "orchestration-v2", "--include", "counts"]),
        JSON.stringify(fixture.receipt),
      );
      expect(result.report.results.counts.reason).toBe("tables_missing");
      expect(result.report.results.counts.data?.find((row) => row.name === "events")).toEqual({ name: "events", status: "completed", count: "1" });
      expect(result.report.results.counts.data?.find((row) => row.name === "threads")?.status)
        .toBe("unavailable");
    });
  });

  it("runs the actual Node CLI with fixed metadata and leaves main/WAL/SHM/journal unchanged", async () => {
    await withFixture(async (fixture) => {
      const before = snapshot(fixture.root);
      const result = await cli(fixture, argumentsFor(fixture), JSON.stringify(fixture.receipt));
      expect(result.code).toBe(0);
      expect(result.report.status).toBe("completed");
      expect(result.report.results.metadata.status).toBe("completed");
      expect(result.report.results.metadata.data?.settings.queryOnly).toBe(true);
      expect(result.report.runtime.sqliteVersion).toMatch(/^\d+\.\d+\.\d+/);
      expect(result.report.runtime.sqliteSourceId).toBeTypeOf("string");
      expect(result.report.fixture?.receiptSha256).toBe(fixture.pin);
      expect(result.report.results.readonly.data?.unchanged).toBe(true);
      expect(result.report.results.counts.status).toBe("omitted");
      expect(result.report.results.integrity.status).toBe("omitted");
      expect(result.report.results.foreignKeys.status).toBe("omitted");
      expect(result.report.cleanup.fixtureOwnership).toBe("retained-input");
      expect(result.report.cleanup.status).toBe("completed");
      expect(result.report.child.reaped).toBe(true);
      expect(result.output).not.toContain(payloadSentinel);
      expect(snapshot(fixture.root)).toEqual(before);
      expect(NodeFS.existsSync(fixture.root)).toBe(true);
    });
  });

  it("runs only opted-in counts, allocation and separate full-integrity/FK checks", async () => {
    await withFixture(async (fixture) => {
      const before = snapshot(fixture.root);
      const result = await cli(
        fixture,
        argumentsFor(fixture, ["--include", "counts,allocation,integrity,foreign-keys"]),
        JSON.stringify(fixture.receipt),
      );
      expect(result.report.limits.deadlineMs).toBe(60_000);
      expect(result.report.results.counts.data?.map((row) => [row.name, row.count])).toEqual(
        countTables.map(([name]) => [name, "1"]),
      );
      expect(result.report.results.allocation.status).toBe(
        fixture.dbstat ? "completed" : "unavailable",
      );
      if (fixture.dbstat) {
        expect(result.report.results.allocation.data?.scope).toBe("btree-pages-only");
        expect(
          result.report.results.allocation.data?.rows.every((row) => /^\d+$/.test(row.bytes)),
        ).toBe(true);
      } else expect(result.report.results.allocation.reason).toBe("dbstat_unavailable");
      expect(result.report.results.integrity.data?.passed).toBe(true);
      expect(result.report.results.foreignKeys.data?.passed).toBe(true);
      expect(result.report.status).toBe(fixture.dbstat ? "completed" : "partial");
      expect(result.output).not.toContain(payloadSentinel);
      expect(snapshot(fixture.root)).toEqual(before);
    });
  });

  it("reports missing tables and bounded schema/allocation rows as incomplete", async () => {
    await withFixture(
      async (fixture) => {
        const result = await cli(
          fixture,
          argumentsFor(fixture, ["--include", "counts,allocation", "--max-records", "1"]),
          JSON.stringify(fixture.receipt),
        );
        expect(result.code).toBe(3);
        expect(result.report.results.metadata.status).toBe("truncated");
        expect(result.report.results.metadata.data?.schema).toHaveLength(1);
        expect(result.report.results.counts.status).toBe("unavailable");
        expect(result.report.results.counts.data?.find((row) => row.name === "leases")).toEqual({
          name: "leases",
          status: "unavailable",
          count: null,
        });
        expect(result.report.results.allocation.status).toBe(
          fixture.dbstat ? "truncated" : "unavailable",
        );
        expect(result.report.results.readonly.data?.unchanged).toBe(true);
      },
      (database) => database.exec("DROP TABLE worktree_ownership_leases"),
    );
  });

  it("does not turn full integrity success into an FK pass or a truncated FK pass", async () => {
    await withFixture(
      async (fixture) => {
        const full = await cli(
          fixture,
          argumentsFor(fixture, ["--include", "integrity,foreign-keys"]),
          JSON.stringify(fixture.receipt),
        );
        expect(full.code).toBe(1);
        expect(full.report.results.integrity.data?.passed).toBe(true);
        expect(full.report.results.foreignKeys.status).toBe("failed");
        expect(full.report.results.foreignKeys.data?.passed).toBe(false);
        const bounded = await cli(
          fixture,
          argumentsFor(fixture, ["--include", "foreign-keys", "--max-diagnostics", "1"]),
          JSON.stringify(fixture.receipt),
        );
        expect(bounded.code).toBe(3);
        expect(bounded.report.results.foreignKeys.status).toBe("truncated");
        expect(bounded.report.results.foreignKeys.data?.passed).toBeNull();
        expect(bounded.report.results.foreignKeys.data?.diagnostics).toHaveLength(1);
      },
      (database) =>
        database.exec(
          "PRAGMA foreign_keys = OFF; CREATE TABLE fk_parent(id INTEGER PRIMARY KEY); CREATE TABLE fk_child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES fk_parent(id)); INSERT INTO fk_child VALUES (1,99),(2,99)",
        ),
    );
  });

  it("refuses bad pins, missing or foreign bindings and alias/root mismatch before a database open", async () => {
    await withFixture(async (fixture) => {
      const before = snapshot(fixture.root);
      const badPin = argumentsFor(fixture);
      badPin[3] = "0".repeat(64);
      const pin = await cli(fixture, badPin, JSON.stringify(fixture.receipt));
      expect(pin.code).toBe(2);
      expect(pin.report.runtime.sqliteVersion).toBeNull();
      const missing = await cli(
        fixture,
        argumentsFor(fixture).slice(0, 4),
        JSON.stringify(fixture.receipt),
      );
      expect(missing.code).toBe(2);
      expect(missing.report.child.pid).toBeNull();
      const foreign = argumentsFor(fixture);
      foreign[5] = JSON.stringify({ ...fixture.binding, sourceRevision: "a".repeat(40) });
      expect((await cli(fixture, foreign, JSON.stringify(fixture.receipt))).code).toBe(2);
      const alias = NodePath.join(fixture.scratch, "alias");
      NodeFS.symlinkSync(fixture.root, alias);
      const mismatch = argumentsFor(fixture);
      mismatch[1] = alias;
      const result = await cli(fixture, mismatch, JSON.stringify(fixture.receipt));
      expect(result.report.reason).toBe("fixture_root_mismatch");
      expect(result.report.runtime.sqliteVersion).toBeNull();
      expect(snapshot(fixture.root)).toEqual(before);
    });
  });

  it("refuses a protected receipt location without opening it", async () => {
    await withFixture(async (fixture) => {
      const protectedRoot = NodePath.join(
        NodeOS.homedir(),
        ".t3",
        "health-synthetic-canary-never-open",
      );
      const receipt = {
        ...fixture.receipt,
        creationReceipt: {
          ...fixture.receipt.creationReceipt,
          canonicalRootPath: protectedRoot,
          canonicalParentPath: NodePath.dirname(protectedRoot),
        },
      };
      const args = argumentsFor(fixture);
      args[1] = protectedRoot;
      args[3] = syntheticFixtureReceiptSha256(receipt);
      const result = await cli(fixture, args, JSON.stringify(receipt));
      expect(result.code).toBe(2);
      expect(result.report.reason).toBe("fixture_refused");
      expect(result.report.runtime.sqliteVersion).toBeNull();
    });
  });

  it("refuses SQL, path overrides, repeated flags and unqualified WAL layouts", async () => {
    await withFixture(async (fixture) => {
      for (const extra of [
        ["--sql", "DELETE FROM orchestration_events"],
        ["--database", "state.sqlite"],
        ["--include", "counts,counts"],
        ["--fixture-root", fixture.root],
        ["--deadline-ms", "300001"],
      ]) {
        const result = await cli(
          fixture,
          argumentsFor(fixture, extra),
          JSON.stringify(fixture.receipt),
        );
        expect(result.code).toBe(2);
        expect(result.report.child.pid).toBeNull();
        expect(result.output).not.toContain("DELETE FROM");
      }
    });
    await withFixture(
      async (fixture) => {
        const before = snapshot(fixture.root);
        const result = await cli(fixture, argumentsFor(fixture), JSON.stringify(fixture.receipt));
        expect(result.code).toBe(2);
        expect(result.report.reason).toBe("readonly_layout_unqualified");
        expect(result.report.results.readonly.status).toBe("unavailable");
        expect(result.report.runtime.sqliteVersion).toBeNull();
        expect(snapshot(fixture.root)).toEqual(before);
      },
      (database) => database.exec("PRAGMA journal_mode = WAL"),
    );
  });

  it("refuses a sealed DELETE fixture with a sidecar before opening SQLite", async () => {
    await withFixture(async (fixture) => {
      const before = snapshot(fixture.root);
      expect(fixture.receipt.layout.some((entry) => entry.relativePath.endsWith("-shm") && entry.present)).toBe(true);
      const result = await cli(fixture, argumentsFor(fixture), JSON.stringify(fixture.receipt));
      expect(result.report.reason).toBe("readonly_layout_unqualified");
      expect(result.report.status).toBe("refused");
      expect(result.report.runtime.sqliteVersion).toBeNull();
      expect(snapshot(fixture.root)).toEqual(before);
    }, (_database, databasePath) => {
      NodeFS.writeFileSync(`${databasePath}-shm`, Buffer.alloc(64), { flag: "wx", mode: 0o600 });
    });
  });

  it("rejects empty, nonobject, invalid UTF-8, multiple-object and oversized stdin before retention/open", async () => {
    await withFixture(async (fixture) => {
      for (const input of [
        "",
        "[]",
        "{}{}",
        Buffer.from([0xff]),
        Buffer.alloc(workerInputBytes + 1, 65),
      ]) {
        const result = await cli(fixture, argumentsFor(fixture), input);
        expect(result.code).toBe(2);
        expect(result.report.child.pid).toBeNull();
      }
      const large = {
        ...fixture.receipt,
        padding: "a".repeat(
          workerInputBytes - Buffer.byteLength(JSON.stringify(fixture.receipt)) - 100,
        ),
      };
      expect(Buffer.byteLength(JSON.stringify(large))).toBeLessThan(workerInputBytes);
      const result = await cli(fixture, argumentsFor(fixture), JSON.stringify(large));
      expect(result.report.reason).toBe("worker_input_too_large");
      expect(result.report.child.pid).toBeNull();
    });
  });
});

describe("jones-sqlite-health — d-deadline", () => {
  it("ends a stdin stream that never reaches EOF within the total deadline", async () => {
    await withFixture(async (fixture) => {
      const result = await cli(
        fixture,
        argumentsFor(fixture, ["--deadline-ms", "1000"]),
        JSON.stringify(fixture.receipt),
        true,
      );
      expect(result.code).toBe(3);
      expect(result.report.status).toBe("interrupted");
      expect(result.report.reason).toBe("worker_deadline");
      expect(result.report.child.pid).toBeNull();
      expect(result.report.cleanup.status).toBe("completed");
    });
  });

  it("terminates and reaps a synchronously blocked leaf after its ready barrier", async () => {
    await withFixture(async (fixture) => {
      const before = snapshot(fixture.root);
      const report = await injectedWorker(fixture, blockedCode, ["--deadline-ms", "2000"]);
      expect(report.status).toBe("interrupted");
      expect(report.reason).toBe("worker_deadline");
      expect(report.child.outcome).toBe("timed_out");
      expect(report.child.escalated).toBe(true);
      expect(report.results.readonly.status).toBe("interrupted");
      expect(snapshot(fixture.root)).toEqual(before);
    });
  });

  it("cancels only the captured blocked leaf after a ready handshake and cleans its supervisor", async () => {
    await withFixture(async (fixture) => {
      const controller = new AbortController();
      const report = await injectedWorker(fixture, blockedCode, [], controller, () =>
        controller.abort(),
      );
      expect(report.status).toBe("interrupted");
      expect(report.reason).toBe("cancelled");
      expect(report.child.outcome).toBe("cancelled");
      expect(report.child.terminated).toBe(true);
      expect(report.child.escalated).toBe(true);
      const outcome = sqliteHealthConsumerOutcome(fixture.pin, report);
      expect(outcome.disposition).toBe("release");
      expect(outcome.value).toBe(report);
    });
  });

  it("enforces combined output before accumulation and preserves a bounded final envelope", async () => {
    await withFixture(async (fixture) => {
      const code = `${readyCode}process.stdout.write(Buffer.alloc(512*1024,65));process.stderr.write(Buffer.alloc(512*1024,66));Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0);`;
      const report = await injectedWorker(fixture, code, ["--max-output-bytes", "16384"]);
      expect(report.status).toBe("partial");
      expect(report.reason).toBe("output_limit");
      expect(report.child.outcome).toBe("output_limited");
      expect(Number(report.child.capturedBytes)).toBeLessThanOrEqual(
        report.limits.maxCapturedChildBytes,
      );
      expect(Number(report.child.observedBytes)).toBeGreaterThan(
        report.limits.maxCapturedChildBytes,
      );
      expect(report.child.truncated).toBe(true);
      const encoded = encodeHealthEnvelope(report);
      expect(Buffer.byteLength(encoded.text)).toBeLessThanOrEqual(16384);
      expect(encoded.text).not.toContain("AAAAAAAAAAAAAAAA");
    });
  });

  it("preserves a failed child exit while removing known closed scratch and omitting raw stderr", async () => {
    await withFixture(async (fixture) => {
      const report = await injectedWorker(
        fixture,
        `${readyCode}console.error('private synthetic exception payload');process.exitCode=7;`,
      );
      expect(report.status).toBe("failed");
      expect(report.child.exitCode).toBe(7);
      expect(report.child.outcome).toBe("failed");
      expect(JSON.stringify(report)).not.toContain("private synthetic exception payload");
      const outcome = sqliteHealthConsumerOutcome(fixture.pin, report);
      expect(outcome.disposition).toBe("release");
      expect(outcome.value).toBe(report);
    });
  });

  it("reports a post-capture stderr budget excess without claiming early termination", async () => {
    await withFixture(async (fixture) => {
      const report = await runSqliteHealth(
        request(fixture, ["--max-stderr-bytes", "1"]),
        receiptInput(fixture),
        {
          runChild: (options) =>
            runOwnedChild({
              ...options,
              args: [
                "--input-type=module",
                "-e",
                `import{runHealthWorker}from${JSON.stringify(NodeURL.pathToFileURL(workerPath).href)};const status=await runHealthWorker(JSON.parse(process.argv[1]),frame=>console.log(JSON.stringify(frame)));process.stderr.write('bounded synthetic diagnostic');process.exitCode=status==='completed'?0:1;`,
                options.args[2]!,
              ],
            }),
        },
      );
      expect(report.status).toBe("partial");
      expect(report.reason).toBe("stderr_budget_exceeded");
      expect(report.child.outcome).toBe("success");
      expect(report.child.truncated).toBe(false);
      expect(report.child.terminated).toBe(false);
      expect(report.cleanup.status).toBe("completed");
      expect(JSON.stringify(report)).not.toContain("bounded synthetic diagnostic");
    });
  });

  it("retains the exact supervisor when returned reap evidence is unknown", async () => {
    await withFixture(async (fixture) => {
      let owner: OwnedRoot | null = null;
      let nativeReceipt: OwnedChildReceipt | null = null;
      try {
        const report = await runSqliteHealth(request(fixture), receiptInput(fixture), {
          runChild: async (options) => {
            owner = options.owner;
            nativeReceipt = await runOwnedChild({
              ...options,
              args: ["--input-type=module", "-e", "process.exitCode=7"],
            });
            return { ...nativeReceipt, outcome: "unknown", closed: false, reaped: false };
          },
        });
        expect(report.cleanup.status).toBe("unknown");
        expect(healthExitCode(report)).toBe(4);
        expect(NodeFS.existsSync(report.cleanup.supervisorRoot!)).toBe(true);
      } finally {
        // This injected unknown observation has a separately captured genuine terminal receipt.
        if (owner && nativeReceipt)
          expect(disposeOwnedRoot(owner, { childReceipts: [nativeReceipt] }).outcome).toBe(
            "complete",
          );
      }
    });
  });
});
