// @effect-diagnostics nodeBuiltinImport:off globalDate:off - Same-host macOS admission; sampling grants no execution authority.
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeURL from "node:url";
import { object, ownedCommand } from "./xcuitest.ts";
import type { ChildReceipt } from "./xcuitest.ts";

export function hostedClass(value: Record<string, unknown>, env = process.env): boolean {
  if (value.hostClass !== "github-hosted-ephemeral") {
    NodeAssert.ok(
      value.hostClass === undefined || value.hostClass === "laptop",
      "Unknown host class",
    );
    return false;
  }
  NodeAssert.ok(
    env.GITHUB_ACTIONS === "true" &&
      env.RUNNER_ENVIRONMENT === "github-hosted" &&
      env.RUNNER_OS === "macOS" &&
      env.ImageOS?.startsWith("macos26"),
    "Hosted class requires a disposable GitHub macOS26 runner",
  );
  NodeAssert.ok(
    env.GITHUB_RUN_ID &&
      env.GITHUB_RUN_ATTEMPT &&
      value.runId === env.GITHUB_RUN_ID &&
      value.runAttempt === env.GITHUB_RUN_ATTEMPT,
    "Hosted run identity mismatch",
  );
  NodeAssert.equal(
    value.serialSlot,
    `gh-${env.GITHUB_RUN_ID}-${env.GITHUB_RUN_ATTEMPT}`,
    "Hosted serial slot mismatch",
  );
  return true;
}

export function memoryAdmitted(pressure: unknown, swapBytesPerSecond: unknown): boolean {
  NodeAssert.ok(
    ["normal", "warning", "critical"].includes(String(pressure)),
    "Memory pressure unavailable",
  );
  NodeAssert.ok(
    typeof swapBytesPerSecond === "number" &&
      Number.isFinite(swapBytesPerSecond) &&
      swapBytesPerSecond >= 0,
    "Swap interval unavailable",
  );
  return pressure !== "critical" || swapBytesPerSecond <= 10 * 1024 ** 2;
}

export function assertResourceAdmission(
  value: unknown,
  sourceSha: string,
  now = Date.now(),
  env = process.env,
) {
  const admission = object(value);
  NodeAssert.equal(admission.sourceSha, sourceSha, "admission source mismatch");
  NodeAssert.equal(admission.hostname, NodeOS.hostname(), "admission host mismatch");
  NodeAssert.ok(
    typeof admission.serialSlot === "string" && admission.serialSlot.length > 0,
    "root serial slot required",
  );
  NodeAssert.ok(
    typeof admission.measuredAt === "number" &&
      now >= admission.measuredAt &&
      now - admission.measuredAt < 60_000,
    "fresh root admission required",
  );
  NodeAssert.ok(
    typeof admission.cpuUsed === "number" &&
      Number.isFinite(admission.cpuUsed) &&
      admission.cpuUsed >= 0 &&
      admission.cpuUsed < 85,
    "CPU admission held",
  );
  NodeAssert.ok(
    typeof admission.availableMemoryBytes === "number" &&
      Number.isFinite(admission.availableMemoryBytes) &&
      admission.availableMemoryBytes > 0,
    "memory evidence required",
  );
  NodeAssert.ok(
    memoryAdmitted(admission.memoryPressure, admission.swapBytesPerSecond),
    "memory pressure and swap held",
  );
  NodeAssert.ok(
    typeof admission.freeDiskBytes === "number" &&
      Number.isFinite(admission.freeDiskBytes) &&
      admission.freeDiskBytes >= 0,
    "disk measurement unavailable",
  );
  if (!hostedClass(admission, env))
    NodeAssert.ok(admission.freeDiskBytes >= 30 * 1024 ** 3, "30 GiB free disk required");
  return admission;
}

export async function admissionMain(args: string[]) {
  const flags = new Map<string, string>();
  for (let index = 0; index < args.length; index += 2) {
    const key = args[index]!,
      value = args[index + 1];
    NodeAssert.ok(
      ["--source-sha", "--serial-slot", "--scratch", "--output"].includes(key) &&
        value &&
        !flags.has(key),
      "Invalid admission arguments",
    );
    flags.set(key, value);
  }
  const required = (key: string) => {
    const value = flags.get(key);
    NodeAssert.ok(value, `${key} required`);
    return value;
  };
  const children: ChildReceipt[] = [];
  const controller = new AbortController(),
    cancel = () => controller.abort();
  process.once("SIGINT", cancel);
  process.once("SIGTERM", cancel);
  try {
    const value = await collectAdmission(
      required("--source-sha"),
      required("--serial-slot"),
      required("--scratch"),
      children,
      controller.signal,
    );
    await NodeFSP.writeFile(required("--output"), JSON.stringify({ ...value, children }, null, 2), {
      flag: "wx",
      mode: 0o600,
    });
  } finally {
    process.removeListener("SIGINT", cancel);
    process.removeListener("SIGTERM", cancel);
  }
}

if (
  process.argv[1] &&
  NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href === import.meta.url
) {
  admissionMain(process.argv.slice(2)).catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : "Admission unavailable"}\n`);
    process.exitCode = 1;
  });
}

export function vmSample(text: string) {
  const page = /page size of (\d+) bytes/.exec(text);
  NodeAssert.ok(page && Number(page[1]) > 0, "VM page size unavailable");
  const count = (label: string) => {
    const match = new RegExp(`^${label}:\\s+(\\d+)\\.\\s*$`, "m").exec(text);
    NodeAssert.ok(match, `VM ${label} unavailable`);
    return Number(match[1]);
  };
  const available = count("Pages free") + count("Pages inactive") + count("Pages speculative");
  return {
    pageBytes: Number(page[1]),
    swap: count("Swapins") + count("Swapouts"),
    availableMemoryBytes: available * Number(page[1]),
  };
}

export function cpuSample(top: string): number {
  const samples = [
    ...top.matchAll(/CPU usage:\s*([\d.]+)% user,\s*([\d.]+)% sys,\s*([\d.]+)% idle/g),
  ];
  NodeAssert.ok(samples.length >= 2, "Two top CPU samples required");
  const last = samples.at(-1)!;
  const used = Number(last[1]) + Number(last[2]);
  NodeAssert.ok(Number.isFinite(used) && used >= 0 && used <= 100, "CPU sample unavailable");
  return used;
}

export function swapRate(
  before: ReturnType<typeof vmSample>,
  after: ReturnType<typeof vmSample>,
  seconds: number,
) {
  NodeAssert.ok(
    before.pageBytes === after.pageBytes &&
      after.swap >= before.swap &&
      Number.isFinite(seconds) &&
      seconds > 0,
    "Swap counter/interval invalid",
  );
  return ((after.swap - before.swap) * before.pageBytes) / seconds;
}

export async function collectAdmission(
  sourceSha: string,
  serialSlot: string,
  scratch: string,
  children: ChildReceipt[],
  signal?: AbortSignal,
) {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Pre-install bootstrap must read the actual host without loading HostProcess workspace dependencies.
  NodeAssert.equal(process.platform, "darwin", "macOS required");
  NodeAssert.match(sourceSha, /^[0-9a-f]{40}$/);
  const read = async (exe: string, argv: string[]) => {
    const result = await ownedCommand(exe, argv, {
      cwd: scratch,
      env: { PATH: "/usr/bin:/bin:/usr/sbin:/sbin" },
      ...(signal === undefined ? {} : { signal }),
      timeoutMs: 10_000,
      receipts: children,
    }).result;
    NodeAssert.equal(result.code, 0, "Admission sample failed");
    return result.stdout.trim();
  };
  const before = vmSample(await read("/usr/bin/vm_stat", [])),
    start = NodePerfHooks.performance.now();
  const top = await read("/usr/bin/top", ["-l", "2", "-s", "1", "-n", "0"]);
  const after = vmSample(await read("/usr/bin/vm_stat", [])),
    seconds = (NodePerfHooks.performance.now() - start) / 1000;
  const rawPressure = await read("/usr/sbin/sysctl", ["-n", "kern.memorystatus_vm_pressure_level"]);
  const pressure = ({ "1": "normal", "2": "warning", "4": "critical" } as Record<string, string>)[
    rawPressure
  ];
  NodeAssert.ok(pressure, "Memory pressure unavailable");
  const disk = await NodeFSP.statfs(scratch);
  const hosted = process.env.RUNNER_ENVIRONMENT === "github-hosted";
  const admission = {
    schema: "jones-ios-admission/1",
    memoryRule: "codex.macos-admission/v1",
    hostClass: hosted ? "github-hosted-ephemeral" : "laptop",
    ...(hosted
      ? { runId: process.env.GITHUB_RUN_ID, runAttempt: process.env.GITHUB_RUN_ATTEMPT }
      : {}),
    sourceSha,
    hostname: NodeOS.hostname(),
    serialSlot,
    measuredAt: Date.now(),
    cpuUsed: cpuSample(top),
    availableMemoryBytes: after.availableMemoryBytes,
    memoryPressure: pressure,
    swapBytesPerSecond: swapRate(before, after, seconds),
    freeDiskBytes: disk.bavail * disk.bsize,
  };
  assertResourceAdmission(admission, sourceSha);
  return admission;
}
