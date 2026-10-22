// @effect-diagnostics nodeBuiltinImport:off globalDate:off globalTimers:off - Finite native qualification owns its sentinels and captured children; no server Effect runtime.
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";
import * as NodeURL from "node:url";
import { collectAdmission } from "./ios-admission.ts";
import { jobInputs } from "./ios-native-build.ts";
import { acceptCreatedDevice } from "./ios-pin-workflow.ts";
import { fileDigest, nativeEnvironment, object, ownedCommand } from "./xcuitest.ts";
import type { ChildReceipt } from "./xcuitest.ts";

export const SENTINEL_CASES = ["success", "failure", "startup-timeout", "cancellation"] as const;
export type SentinelCase = (typeof SENTINEL_CASES)[number];
export function caseArguments(kind: SentinelCase): string[] {
  if (kind === "failure") return ["--no-action", "true"];
  if (kind === "startup-timeout") return ["--fixture-readiness-timeout-ms", "1"];
  return [];
}
export function assertCase(
  kind: SentinelCase,
  runnerExit: number,
  receipt: Record<string, unknown>,
) {
  if (kind === "success") {
    NodeAssert.equal(runnerExit, 0);
    NodeAssert.equal(receipt.outcome, "pass");
    NodeAssert.equal(receipt.nativeClaim, true);
  } else {
    NodeAssert.notEqual(runnerExit, 0);
    NodeAssert.equal(receipt.outcome, "fail");
    NodeAssert.equal(receipt.nativeClaim, false);
    NodeAssert.equal(
      receipt.failureCode,
      {
        failure: "negative-control-oracle",
        "startup-timeout": "startup-timeout",
        cancellation: "cancelled",
      }[kind],
    );
  }
  NodeAssert.equal(receipt.retainScratch, false, "Runner cleanup unresolved");
}
export function testStarted(ndjson: string) {
  return ndjson
    .slice(0, ndjson.lastIndexOf("\n") + 1)
    .split("\n")
    .filter(Boolean)
    .some((line) => object(JSON.parse(line)).stage === "test-started");
}
export function groupsGone(children: unknown): boolean {
  NodeAssert.ok(Array.isArray(children) && children.length > 0, "Captured children required");
  return children.every((raw) => {
    const child = object(raw);
    if (!Number.isSafeInteger(child.pgid) || Number(child.pgid) <= 0 || child.groupGone !== true)
      return false;
    try {
      process.kill(-Number(child.pgid), 0);
      return false;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ESRCH") return true;
      throw error;
    }
  });
}
async function absent(path: string) {
  try {
    await NodeFSP.lstat(path);
    return false;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return true;
    throw error;
  }
}
async function portClosed(port: number) {
  NodeAssert.ok(Number.isInteger(port) && port > 0 && port < 65536, "Fixture port unavailable");
  return new Promise<boolean>((resolve, reject) => {
    const socket = NodeNet.connect({ host: "127.0.0.1", port });
    socket.once("connect", () => {
      socket.destroy();
      resolve(false);
    });
    socket.once("error", (error: NodeJS.ErrnoException) =>
      error.code === "ECONNREFUSED" ? resolve(true) : reject(error),
    );
    socket.setTimeout(2_000, () => {
      socket.destroy();
      reject(new Error("Fixture port state unknown"));
    });
  });
}

interface NativeOptions {
  source: string;
  sourceSha: string;
  root: string;
  output: string;
  serialSlot: string;
  tooling: Record<string, unknown>;
  buildRecord: string;
  children: ChildReceipt[];
  signal: AbortSignal;
}
export async function runJourney(
  options: NativeOptions,
  name: string,
  extra: string[],
  cancelAtTest = false,
) {
  const { source, sourceSha, root, output, tooling, children, signal } = options;
  const receiptPath = NodePath.join(output, `${name}.json`),
    progress = NodePath.join(root, `${name}.ndjson`);
  const admissionPath = NodePath.join(root, `${name}-admission.json`);
  const admission = await collectAdmission(sourceSha, options.serialSlot, root, children, signal);
  await NodeFSP.writeFile(admissionPath, JSON.stringify(admission), { flag: "wx", mode: 0o600 });
  const args = [
    NodePath.join(source, "scripts/jones/mobile/ios-pin-workflow.ts"),
    "--execute",
    "--source",
    source,
    "--source-sha",
    sourceSha,
    "--build-record",
    options.buildRecord,
    "--developer-dir",
    String(tooling.developerDir),
    "--runtime",
    String(tooling.runtime),
    "--device-type",
    String(tooling.deviceType),
    "--scratch-parent",
    root,
    "--receipt",
    receiptPath,
    "--admission",
    admissionPath,
    "--progress",
    progress,
    ...extra,
  ];
  const runner = ownedCommand(process.execPath, args, {
    cwd: source,
    env: { ...process.env },
    timeoutMs: 12 * 60_000,
    receipts: children,
  });
  void runner.result.catch(() => {});
  let cancellationSent = false;
  try {
    while (runner.child.exitCode === null && runner.child.signalCode === null) {
      const started = await NodeFSP.readFile(progress, "utf8").then(
        testStarted,
        (error: NodeJS.ErrnoException) => {
          if (error.code === "ENOENT") return false;
          throw error;
        },
      );
      if (!cancellationSent && (signal.aborted || (cancelAtTest && started))) {
        NodeAssert.ok(runner.child.pid, "Captured runner PID unavailable");
        runner.child.kill("SIGTERM");
        cancellationSent = true;
      }
      await NodeTimersPromises.setTimeout(50);
    }
  } catch (error) {
    runner.child.kill("SIGTERM");
    await runner.result;
    throw error;
  }
  const result = await runner.result;
  const receipt = object(JSON.parse(await NodeFSP.readFile(receiptPath, "utf8")));
  if (cancelAtTest) {
    NodeAssert.ok(cancellationSent, "Cancellation did not reach test-started");
    NodeAssert.ok(
      Array.isArray(receipt.children) &&
        receipt.children
          .map(object)
          .some(
            (child) =>
              Array.isArray(child.argv) &&
              child.argv.includes("test-without-building") &&
              Number.isSafeInteger(child.pid) &&
              Number(child.pid) > 0,
          ),
      "Captured Xcode child missing at cancellation",
    );
  }
  return { receipt, receiptPath, runnerExit: result.code };
}

export async function verifyJourney(
  receipt: Record<string, unknown>,
  devices: { udid: string; state: string }[],
) {
  const createdUdid = String(object(receipt.device).udid);
  NodeAssert.match(createdUdid, /^[0-9a-f-]{36}$/i);
  NodeAssert.ok(typeof receipt.scratch === "string", "Runner scratch unavailable");
  const evidence = {
    createdUdid,
    createdUdidAbsent: !devices.some((device) => device.udid === createdUdid),
    groupsGone: groupsGone(receipt.children),
    scratchAbsent: await absent(receipt.scratch),
    portClosed: await portClosed(Number(object(receipt.server).port)),
  };
  assertCleanupEvidence(evidence);
  return evidence;
}

export function assertCleanupEvidence(evidence: {
  createdUdidAbsent: boolean;
  groupsGone: boolean;
  scratchAbsent: boolean;
  portClosed: boolean;
}) {
  NodeAssert.ok(
    evidence.createdUdidAbsent &&
      evidence.groupsGone &&
      evidence.scratchAbsent &&
      evidence.portClosed,
    "Owned native cleanup unresolved",
  );
}

export function qualification(
  cases: { case: string; passed: boolean }[],
  sentinelsRemoved: boolean,
) {
  return sentinelsRemoved &&
    cases.length === SENTINEL_CASES.length &&
    cases.every((row, index) => row.case === SENTINEL_CASES[index] && row.passed)
    ? "established"
    : "not-established";
}

export async function sentinelMain(mode: string) {
  NodeAssert.ok(
    mode === "--qualify" || mode === "--control-pinned",
    "Use --qualify or --control-pinned",
  );
  const input = jobInputs(),
    children: ChildReceipt[] = [];
  const controller = new AbortController(),
    cancel = () => controller.abort();
  const tooling = object(
    JSON.parse(await NodeFSP.readFile(NodePath.join(input.output, "tooling.json"), "utf8")),
  );
  const buildRecord = NodePath.join(input.output, "build-record.json");
  const options = { ...input, tooling, buildRecord, children, signal: controller.signal };
  const env = nativeEnvironment(
    input.root,
    String(tooling.developerDir),
    process.env.PATH ?? "/usr/bin:/bin",
  );
  const read = async (exe: string, args: string[], teardown = false) => {
    const result = await ownedCommand(exe, args, {
      cwd: input.root,
      env,
      receipts: children,
      ...(teardown ? {} : { signal: controller.signal }),
    }).result;
    NodeAssert.equal(result.code, 0, "Sentinel command failed");
    return result.stdout.trim();
  };
  const devices = async (teardown = false) => {
    const response = object(
      JSON.parse(await read("/usr/bin/xcrun", ["simctl", "list", "devices", "--json"], teardown)),
    );
    return Object.values(object(response.devices)).flatMap((rows) => {
      NodeAssert.ok(Array.isArray(rows));
      return rows.map((raw) => {
        const row = object(raw);
        return { udid: String(row.udid), name: String(row.name), state: String(row.state) };
      });
    });
  };
  const cases: ({ case: string; passed: boolean } & Record<string, unknown>)[] = [];
  const result: Record<string, unknown> = {
    schema: mode === "--qualify" ? "jones-ios-cleanup-sentinel/1" : "jones-ios-control-pinned/1",
    sourceSha: input.sourceSha,
    buildRecordSha256: await fileDigest(buildRecord),
    tooling,
    cases,
    qualification: "not-established",
    sentinelsRemoved: false,
    cleanupKnown: false,
  };
  const resultPath = NodePath.join(
    input.output,
    mode === "--qualify" ? "sentinel.json" : "control-pinned-check.json",
  );
  await NodeFSP.writeFile(resultPath, JSON.stringify(result), { flag: "wx", mode: 0o600 });
  process.once("SIGINT", cancel);
  process.once("SIGTERM", cancel);
  let sentinelUdid: string | undefined, sleep: ReturnType<typeof ownedCommand> | undefined;
  let primary: unknown,
    unknownCreate = false;
  try {
    if (mode === "--control-pinned") {
      const sentinel = object(
        JSON.parse(await NodeFSP.readFile(NodePath.join(input.output, "sentinel.json"), "utf8")),
      );
      NodeAssert.equal(sentinel.qualification, "established", "Sentinel qualification required");
      NodeAssert.equal(sentinel.sourceSha, input.sourceSha);
      NodeAssert.equal(sentinel.buildRecordSha256, await fileDigest(buildRecord));
      const run = await runJourney(options, "control-pinned", ["--control-pinned", "true"]);
      assertCase("success", run.runnerExit, run.receipt);
      Object.assign(result, await verifyJourney(run.receipt, await devices()), {
        outcome: "pass",
        cleanupKnown: true,
      });
    } else {
      await collectAdmission(
        input.sourceSha,
        input.serialSlot,
        input.root,
        children,
        controller.signal,
      );
      const previous = await devices(),
        name = `jones-ios-sentinel-${process.pid}-${Date.now()}`;
      try {
        sentinelUdid = acceptCreatedDevice(
          await read("/usr/bin/xcrun", [
            "simctl",
            "create",
            name,
            String(tooling.deviceType),
            String(tooling.runtime),
          ]),
          previous,
          name,
        );
      } catch (error) {
        unknownCreate = true;
        const matching = (await devices(true)).filter(
          (item) => item.name === name && !previous.some((old) => old.udid === item.udid),
        );
        if (matching.length === 1) {
          sentinelUdid = acceptCreatedDevice(matching[0]!.udid, previous, name);
          unknownCreate = false;
        } else if (matching.length === 0) unknownCreate = false;
        throw error;
      }
      await read("/usr/bin/xcrun", ["simctl", "boot", sentinelUdid]);
      await read("/usr/bin/xcrun", ["simctl", "bootstatus", sentinelUdid, "-b"]);
      sleep = ownedCommand("/bin/sleep", ["3600"], {
        cwd: input.root,
        env,
        timeoutMs: 3_650_000,
        receipts: children,
      });
      void sleep.result.catch(() => {});
      NodeAssert.ok(sleep.child.pid, "Captured sentinel sleep unavailable");
      const pid = sleep.child.pid;
      const startTime = await read("/bin/ps", ["-p", String(pid), "-o", "lstart="]);
      NodeAssert.ok(startTime, "Sentinel sleep start time unavailable");
      for (const kind of SENTINEL_CASES) {
        controller.signal.throwIfAborted();
        NodeAssert.equal(
          (await devices()).find((item) => item.udid === sentinelUdid)?.state,
          "Booted",
        );
        const run = await runJourney(options, kind, caseArguments(kind), kind === "cancellation");
        const row: { case: string; passed: boolean } & Record<string, unknown> = {
          case: kind,
          passed: false,
          runnerExit: run.runnerExit,
          receiptSha256: await fileDigest(run.receiptPath),
          failureCode: run.receipt.failureCode,
        };
        cases.push(row);
        Object.assign(row, await verifyJourney(run.receipt, await devices()));
        const after = (await devices()).find((item) => item.udid === sentinelUdid)?.state;
        const aliveAfter =
          sleep.child.exitCode === null &&
          sleep.child.signalCode === null &&
          (await read("/bin/ps", ["-p", String(pid), "-o", "lstart="])) === startTime;
        Object.assign(row, {
          sentinelSim: { udid: sentinelUdid, before: "Booted", after },
          sentinelSleep: { pid, startTime, aliveAfter },
        });
        NodeAssert.ok(after === "Booted" && aliveAfter, "Unrelated sentinel did not survive");
        assertCase(kind, run.runnerExit, run.receipt);
        row.passed = true;
        await NodeFSP.writeFile(resultPath, JSON.stringify(result, null, 2), { mode: 0o600 });
      }
    }
  } catch (error) {
    primary = error;
    result.failure = error instanceof Error ? error.message : "Sentinel failure";
  } finally {
    let cleanupError: unknown;
    try {
      if (sleep) {
        await sleep.stop();
        await sleep.result.catch((error) => {
          if (!sleep?.receipt.groupGone) throw error;
        });
      }
    } catch (error) {
      cleanupError = error;
    }
    try {
      if (sentinelUdid) {
        const current = (await devices(true)).find((item) => item.udid === sentinelUdid);
        if (current && current.state !== "Shutdown")
          await read("/usr/bin/xcrun", ["simctl", "shutdown", sentinelUdid], true);
        if (current) await read("/usr/bin/xcrun", ["simctl", "delete", sentinelUdid], true);
        NodeAssert.ok(
          !(await devices(true)).some((item) => item.udid === sentinelUdid),
          "Owned sentinel Simulator remains",
        );
      }
      NodeAssert.ok(
        !cleanupError && !unknownCreate && children.every((child) => child.groupGone === true),
        "Sentinel cleanup unresolved",
      );
      result.sentinelsRemoved = true;
      if (mode === "--qualify")
        result.cleanupKnown =
          cases.length === 4 &&
          cases.every(
            (row) =>
              row.groupsGone === true &&
              row.scratchAbsent === true &&
              row.createdUdidAbsent === true &&
              row.portClosed === true,
          );
    } catch (error) {
      cleanupError = error;
      result.cleanupKnown = false;
      result.cleanupFailure = error instanceof Error ? error.message : "Cleanup unresolved";
    }
    result.children = children;
    if (mode === "--qualify")
      result.qualification = qualification(cases, result.sentinelsRemoved === true);
    process.removeListener("SIGINT", cancel);
    process.removeListener("SIGTERM", cancel);
    await NodeFSP.writeFile(resultPath, JSON.stringify(result, null, 2), { mode: 0o600 });
    if (primary || cleanupError)
      throw new AggregateError(
        [primary, cleanupError].filter(Boolean),
        "Native qualification failed; see redacted receipts",
      );
  }
  NodeAssert.ok(
    mode === "--control-pinned"
      ? result.outcome === "pass"
      : result.qualification === "established",
  );
  return result;
}

if (
  process.argv[1] &&
  NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href === import.meta.url
) {
  sentinelMain(process.argv[2] ?? "")
    .then(() => process.stdout.write("Native qualification receipts written\n"))
    .catch(() => {
      process.stderr.write("Native qualification failed; retain redacted evidence\n");
      process.exitCode = 1;
    });
}
