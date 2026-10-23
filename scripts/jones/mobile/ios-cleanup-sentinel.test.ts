// @effect-diagnostics nodeBuiltinImport:off globalDate:off - Synthetic native orchestration owns bounded disposable fixtures; no Simulator or Xcode starts.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { fileDigest, object, ownedCommand as realOwnedCommand } from "./xcuitest.ts";
import type { ChildReceipt } from "./xcuitest.ts";
import type { JourneyAttempt } from "./ios-cleanup-sentinel.ts";
import { describe, expect, it } from "vite-plus/test";
import {
  assertCase,
  assertCleanupEvidence,
  caseArguments,
  qualification,
  SENTINEL_CASES,
  testStarted,
  sentinelMain,
} from "./ios-cleanup-sentinel.ts";

describe("native sentinel qualification decisions", () => {
  it("requires all four independent cleanup observations", () => {
    const evidence = {
      createdUdidAbsent: true,
      groupsGone: true,
      scratchAbsent: true,
      portClosed: true,
    };
    expect(() => assertCleanupEvidence(evidence)).not.toThrow();
    for (const key of Object.keys(evidence))
      expect(() => assertCleanupEvidence({ ...evidence, [key]: false })).toThrow(
        "cleanup unresolved",
      );
  });
  it("requires the intended native failure, not an unrelated startup failure", () => {
    expect(caseArguments("failure")).toEqual(["--no-action", "true"]);
    expect(caseArguments("startup-timeout")).toEqual(["--fixture-readiness-timeout-ms", "1"]);
    const failure = {
      outcome: "fail",
      nativeClaim: false,
      retainScratch: false,
      failureCode: "negative-control-oracle",
    };
    expect(() => assertCase("failure", 1, failure)).not.toThrow();
    expect(() => assertCase("failure", 1, { ...failure, failureCode: "other" })).toThrow();
    expect(() => assertCase("failure", 0, failure)).toThrow();
    expect(() =>
      assertCase("startup-timeout", 1, { ...failure, failureCode: "startup-timeout" }),
    ).not.toThrow();
    expect(() =>
      assertCase("cancellation", 1, { ...failure, failureCode: "cancelled" }),
    ).not.toThrow();
    expect(() =>
      assertCase("success", 0, { outcome: "pass", nativeClaim: true, retainScratch: true }),
    ).toThrow("cleanup");
  });
  it("cannot qualify missing cases or unresolved sentinel cleanup", () => {
    const cases = SENTINEL_CASES.map((kind) => ({ case: kind, passed: true }));
    expect(qualification(cases, true)).toBe("established");
    expect(qualification(cases, false)).toBe("not-established");
    expect(qualification(cases.slice(1), true)).toBe("not-established");
    expect(qualification([], true)).toBe("not-established");
    expect(qualification([...cases].reverse(), true)).toBe("not-established");
    expect(
      qualification(
        cases.map((row, index) => ({ ...row, passed: index !== 2 })),
        true,
      ),
    ).toBe("not-established");
  });
  it("waits for a complete test-started event, not device creation or a partial write", () => {
    expect(testStarted('{"stage":"device-created"}\n')).toBe(false);
    expect(testStarted('{"stage":"test-started"}')).toBe(false);
    expect(testStarted('{"stage":"device-booted"}\n{"stage":"test-started"}\n{"sta')).toBe(true);
  });
});

type HarnessConfig = {
  mode?: "--qualify" | "--control-pinned";
  sample?: (label: string, index: number, sample: Record<string, unknown>) => unknown;
  cancel?: "before" | "sample" | "wait" | "ready" | "running";
  target?: string;
  receipt?:
    | "unknown"
    | "missing"
    | "malformed"
    | "allocating"
    | "source"
    | "build"
    | "admission"
    | "retained"
    | "device"
    | "server"
    | "scratch"
    | "xcode";
  failure?: "spawn" | "progress" | "settlement" | "hash" | "stop";
  observation?: "device" | "groups" | "scratch" | "port" | "sentinel" | "create" | "parent";
  behavior?: "failed-success" | "successful-failure" | "wrong-code" | "early-cancellation";
  diagnosticPayload?: boolean;
  settlementFailure?: boolean;
  admissionWriteFailure?: boolean;
  preexistingReceipt?: boolean;
  prerequisite?: "missing" | "source" | "build";
};

async function withHarness(
  config: HarnessConfig,
  check: (h: Awaited<ReturnType<typeof harness>>) => Promise<void>,
) {
  const taskRoot = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-sentinel-"));
  try {
    await check(await harness(taskRoot, config));
  } finally {
    await NodeFSP.rm(taskRoot, { recursive: true, force: true });
    expect(NodeFS.existsSync(taskRoot)).toBe(false);
  }
}

async function harness(taskRoot: string, config: HarnessConfig) {
  const root = await NodeFSP.realpath(taskRoot).then((path) => NodePath.join(path, "build"));
  const output = NodePath.join(taskRoot, "evidence");
  await NodeFSP.mkdir(root);
  await NodeFSP.mkdir(output);
  const input = {
    source: taskRoot,
    sourceSha: "a".repeat(40),
    root,
    output,
    serialSlot: "synthetic-slot",
  };
  const build = { sourceSha: input.sourceSha, artifacts: { synthetic: true } };
  const tooling = {
    developerDir: "/synthetic/Xcode",
    runtime: "synthetic-runtime",
    deviceType: "synthetic-device",
  };
  const buildRecord = NodePath.join(output, "build-record.json");
  await NodeFSP.writeFile(buildRecord, JSON.stringify(build));
  await NodeFSP.writeFile(NodePath.join(output, "tooling.json"), JSON.stringify(tooling));
  if (config.mode === "--control-pinned")
    await NodeFSP.writeFile(
      NodePath.join(output, "sentinel.json"),
      JSON.stringify({
        qualification: config.prerequisite === "missing" ? "not-established" : "established",
        sourceSha: config.prerequisite === "source" ? "b".repeat(40) : input.sourceSha,
        buildRecordSha256:
          config.prerequisite === "build" ? "wrong" : await fileDigest(buildRecord),
      }),
    );
  const target =
    config.target ?? (config.mode === "--control-pinned" ? "control-pinned" : "success");
  if (config.admissionWriteFailure)
    await NodeFSP.writeFile(NodePath.join(output, `${target}-admission.json`), "preexisting");
  if (config.preexistingReceipt)
    await NodeFSP.writeFile(
      NodePath.join(output, `${target}.json`),
      JSON.stringify({ outcome: "pass", retainScratch: false }),
    );
  const controller = new AbortController();
  if (config.cancel === "before") controller.abort(new Error("owner cancelled"));
  const sentinelUdid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
  const journeyUdid = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
  let deviceRows: { udid: string; name: string; state: string }[] = [];
  const starts: string[] = [],
    stops: string[] = [],
    sampled: string[] = [];
  const readsAfterAbort: boolean[] = [];
  let clock = 100_000,
    boundary = 0,
    sampleIndex = 0,
    pid = 1000;
  const labels =
    config.mode === "--control-pinned" ? ["control-pinned"] : ["sentinel-setup", ...SENTINEL_CASES];
  const ownedCommand: typeof realOwnedCommand = (executable, argv, options) => {
    options.signal?.throwIfAborted();
    const receipt: ChildReceipt = {
      executable,
      argv: [...argv],
      spawnedAt: new Date(0).toISOString(),
      pid: ++pid,
      pgid: pid,
      groupGone: false,
    };
    options.receipts?.push(receipt);
    const journey = argv.includes("--receipt");
    const arg = (flag: string) => argv[argv.indexOf(flag) + 1]!;
    const name = journey ? NodePath.basename(arg("--receipt"), ".json") : executable;
    const affected = name === target;
    if (journey) {
      starts.push(name);
      expect(NodePath.dirname(arg("--admission"))).toBe(output);
      expect(JSON.parse(NodeFS.readFileSync(arg("--admission"), "utf8")).cpuUsed).toBeLessThan(85);
      if (affected && config.failure === "spawn") {
        receipt.groupGone = true;
        throw new Error("unknown spawn effect");
      }
    }
    let stdout = "",
      code = 0;
    const live =
      executable === "/bin/sleep" ||
      (journey &&
        ((name === "cancellation" && config.behavior !== "early-cancellation") ||
          (affected && (config.failure === "progress" || config.cancel === "running"))));
    const child = {
      pid,
      exitCode: live ? null : 0,
      signalCode: null as string | null,
      kill: () => {
        finish();
        return true;
      },
    };
    let resolve!: (value: {
      stdout: string;
      stderr: string;
      code: number;
      receipt: ChildReceipt;
    }) => void;
    let reject!: (reason: Error) => void;
    const result = new Promise<{
      stdout: string;
      stderr: string;
      code: number;
      receipt: ChildReceipt;
    }>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    const writeReceipt = () => {
      if (!journey) return;
      if (affected && config.receipt === "missing") return;
      let value: Record<string, unknown> = {
        schema: "jones-ios-workflow-receipt/2",
        sourceSha: input.sourceSha,
        build,
        admission: JSON.parse(NodeFS.readFileSync(arg("--admission"), "utf8")),
        outcome: code === 0 ? "pass" : "fail",
        nativeClaim: code === 0,
        retainScratch: false,
        failureCode:
          name === "failure"
            ? "negative-control-oracle"
            : name === "startup-timeout"
              ? "startup-timeout"
              : name === "cancellation"
                ? "cancelled"
                : "other",
        scratch: NodePath.join(root, `${name}-scratch`),
        device: { udid: journeyUdid },
        server: { port: 12345 },
        children: [{ pid: 1234, pgid: 1234, groupGone: true, argv: ["test-without-building"] }],
      };
      if (affected) {
        if (config.receipt === "unknown")
          Object.assign(value, {
            outcome: "unknown",
            nativeClaim: false,
            retainScratch: true,
            primary: "Owned command timed out; operation effect requires readback",
            primaryErrors: [
              "Owned command timed out; operation effect requires readback",
              "kill EPERM",
            ],
            cleanup: [
              {
                label: "owned-children-gone",
                status: "failed",
                error: "captured child/group cleanup unresolved",
              },
            ],
          });
        if (config.diagnosticPayload)
          Object.assign(value, {
            primary: `${taskRoot}/private-token ${"x".repeat(2000)}`,
            primaryErrors: Array.from(
              { length: 10 },
              () => `${taskRoot}/private-token ${"x".repeat(2000)}`,
            ),
            cleanup: Array.from({ length: 10 }, () => ({
              error: `${taskRoot}/private-token ${"x".repeat(2000)}`,
            })),
            stdout: "raw-private-output",
            argv: ["raw-private-argv"],
          });
        if (config.receipt === "source") value.sourceSha = "b".repeat(40);
        if (config.receipt === "build") value.build = { ...build, unrelated: true };
        if (config.receipt === "admission")
          value.admission = { ...object(value.admission), measuredAt: 1 };
        if (config.receipt === "retained") value.retainScratch = true;
        if (config.receipt === "device") delete value.device;
        if (config.receipt === "server") delete value.server;
        if (config.receipt === "scratch") delete value.scratch;
        if (config.receipt === "xcode")
          value.children = [{ pid: 1234, pgid: 1234, groupGone: true, argv: ["fixture"] }];
        if (config.receipt === "allocating")
          value = { outcome: "unknown", nativeClaim: false, stage: "allocating" };
        if (config.behavior === "wrong-code") value.failureCode = "other";
      }
      NodeFS.writeFileSync(
        arg("--receipt"),
        affected && config.receipt === "malformed" ? "{" : JSON.stringify(value),
        { mode: 0o600 },
      );
    };
    function finish() {
      child.exitCode = code;
      receipt.code = code;
      receipt.groupGone =
        !(affected && config.failure === "stop") && config.observation !== "parent";
      writeReceipt();
      if (
        affected &&
        (config.failure === "settlement" || config.failure === "stop" || config.settlementFailure)
      )
        reject(new Error("runner settlement failed"));
      else resolve({ stdout, stderr: "", code, receipt });
    }
    if (journey) {
      code = name === "success" || name === "control-pinned" ? 0 : 1;
      if (affected && config.behavior === "failed-success") code = 1;
      if (affected && config.behavior === "successful-failure") code = 0;
      if (affected && config.observation === "device")
        deviceRows.push({ udid: journeyUdid, name: "journey", state: "Shutdown" });
      if (live)
        NodeFS.writeFileSync(
          arg("--progress"),
          affected && config.failure === "progress" ? "{broken}\n" : '{"stage":"test-started"}\n',
        );
      if (affected && config.cancel === "running") controller.abort(new Error("owner cancelled"));
    } else if (argv[0] === "simctl") {
      if (argv[1] === "list") {
        if (controller.signal.aborted) readsAfterAbort.push(options.signal === undefined);
        stdout = JSON.stringify({ devices: { runtime: deviceRows } });
      }
      if (argv[1] === "create") {
        deviceRows.push({ udid: sentinelUdid, name: argv[2]!, state: "Shutdown" });
        stdout = sentinelUdid;
        if (config.observation === "create") {
          deviceRows.push({ udid: journeyUdid, name: argv[2]!, state: "Shutdown" });
          stdout = "invalid-created-device";
        }
      }
      if (argv[1] === "boot") deviceRows[0]!.state = "Booted";
      if (argv[1] === "shutdown") deviceRows[0]!.state = "Shutdown";
      if (argv[1] === "delete" && config.observation !== "sentinel")
        deviceRows = deviceRows.filter((row) => row.udid !== argv[2]);
    } else if (executable === "/bin/ps") stdout = "synthetic-start-time";
    if (!live) finish();
    return {
      child,
      receipt,
      result,
      stop: async () => {
        stops.push(name);
        finish();
        if (affected && config.failure === "stop") throw new Error("stop unresolved");
      },
    } as ReturnType<typeof realOwnedCommand>;
  };
  const dependencies: NonNullable<Parameters<typeof sentinelMain>[1]> = {
    jobInputs: () => input,
    controller: () => controller,
    ownedCommand,
    wait: async () => {},
    groupsGone: (children) =>
      Array.isArray(children) &&
      children.length > 0 &&
      children.every((child) => object(child).groupGone === true) &&
      config.observation !== "groups",
    absent: async (path) => {
      expect(path.startsWith(root + NodePath.sep)).toBe(true);
      return config.observation !== "scratch";
    },
    portClosed: async (port) => {
      expect(port).toBe(12345);
      return config.observation !== "port";
    },
    fileDigest: async (path) => {
      if (config.failure === "hash" && path === NodePath.join(output, `${target}.json`))
        throw new Error("receipt hash failed");
      return fileDigest(path);
    },
    admission: {
      env: {},
      monotonicNow: () => clock,
      now: () => {
        if (
          config.cancel === "ready" &&
          NodeFS.existsSync(NodePath.join(output, `${target}-admission.json`))
        )
          controller.abort(new Error("owner cancelled"));
        return clock;
      },
      wait: async (ms) => {
        clock += ms;
        if (config.cancel === "wait") controller.abort(new Error("owner cancelled"));
      },
      sample: async () => {
        const label = labels[boundary]!;
        sampled.push(label);
        const sample = {
          sourceSha: input.sourceSha,
          hostname: NodeOS.hostname(),
          serialSlot: input.serialSlot,
          measuredAt: clock,
          cpuUsed: 30,
          availableMemoryBytes: 1024 ** 3,
          memoryPressure: "normal",
          swapBytesPerSecond: 0,
          freeDiskBytes: 30 * 1024 ** 3,
        };
        if (config.cancel === "sample" && label === target)
          controller.abort(new Error("owner cancelled"));
        const value = await (config.sample?.(label, sampleIndex++, sample) ?? sample);
        if (Number(object(value).cpuUsed) < 85) {
          boundary++;
          sampleIndex = 0;
        }
        return value;
      },
    },
  };
  return {
    root,
    output,
    starts,
    stops,
    sampled,
    readsAfterAbort,
    clock: () => clock,
    events: async (label: string) =>
      (await NodeFSP.readFile(NodePath.join(output, `${label}-admission-events.jsonl`), "utf8"))
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line)),
    run: async () => {
      let error: unknown;
      try {
        await sentinelMain(config.mode ?? "--qualify", dependencies);
      } catch (cause) {
        error = cause;
      }
      const receipt = JSON.parse(
        await NodeFSP.readFile(
          NodePath.join(
            output,
            config.mode === "--control-pinned" ? "control-pinned-check.json" : "sentinel.json",
          ),
          "utf8",
        ),
      );
      return { error, receipt };
    },
  };
}

describe("sentinel orchestration admission and cleanup", () => {
  it("waits at every boundary, including exactly 85%, then runs each case once", async () => {
    await withHarness(
      { sample: (_label, index, sample) => ({ ...sample, cpuUsed: index === 0 ? 85 : 30 }) },
      async (h) => {
        const { error, receipt } = await h.run();
        expect(error).toBeUndefined();
        expect(receipt).toMatchObject({
          qualification: "established",
          cleanupKnown: true,
          sentinelsRemoved: true,
        });
        expect(h.starts).toEqual(SENTINEL_CASES);
        expect(receipt.journeys.every((j: JourneyAttempt) => j.state === "cleanup-proved")).toBe(
          true,
        );
        await NodeFSP.rm(h.root, { recursive: true });
        for (const label of ["sentinel-setup", ...SENTINEL_CASES]) {
          expect((await h.events(label)).map((event) => event.kind)).toEqual([
            "started",
            "sample",
            "held",
            "sample",
            "ready",
          ]);
          expect((await h.events(label)).at(-1)).toMatchObject({ elapsedMs: 20_000 });
        }
      },
    );
  });
  it.each(["sentinel-setup", "success", "failure"])(
    "bounds permanent busy at %s without launching that journey",
    async (target) => {
      await withHarness(
        { sample: (label, _index, sample) => ({ ...sample, cpuUsed: label === target ? 85 : 30 }) },
        async (h) => {
          const { error, receipt } = await h.run();
          expect(error).toBeInstanceOf(AggregateError);
          expect(receipt).toMatchObject({
            cleanupKnown: true,
            qualification: "not-established",
            failure: "CPU admission readiness deadline exceeded",
          });
          expect(h.starts).toEqual(target === "failure" ? ["success"] : []);
          expect(receipt.cases).toHaveLength(target === "failure" ? 1 : 0);
          expect(h.clock()).toBe(220_000);
          expect((await h.events(target)).at(-1)).toMatchObject({
            kind: "aborted",
            elapsedMs: 120_000,
          });
        },
      );
    },
  );
  it.each([
    ["stale", { measuredAt: 0 }],
    ["host", { hostname: "wrong-host" }],
    ["slot", { serialSlot: "wrong-slot" }],
    ["source", { sourceSha: "wrong-source" }],
    ["cpu", { cpuUsed: NaN }],
    ["memory", { availableMemoryBytes: 0 }],
    ["pressure", { memoryPressure: "critical", swapBytesPerSecond: 11 * 1024 ** 2 }],
    ["disk", { freeDiskBytes: 0 }],
  ] as const)("does not retry invalid %s admission", async (_name, invalid) => {
    await withHarness(
      {
        sample: (label, _index, sample) =>
          label === "success"
            ? { ...sample, ...invalid, cpuUsed: "cpu" === _name ? NaN : 90 }
            : sample,
      },
      async (h) => {
        const { error, receipt } = await h.run();
        expect(error).toBeDefined();
        expect(h.starts).toEqual([]);
        expect(h.sampled).toEqual(["sentinel-setup", "success"]);
        expect(receipt).toMatchObject({
          cleanupKnown: true,
          qualification: "not-established",
          cases: [],
        });
      },
    );
  });
  it.each(["collector", "write"])("stops on %s evidence failure", async (kind) => {
    await withHarness(
      {
        admissionWriteFailure: kind === "write",
        sample: (label, _index, sample) => {
          if (kind === "collector" && label === "success") throw new Error("collector failed");
          return sample;
        },
      },
      async (h) => {
        const { error, receipt } = await h.run();
        expect(error).toBeDefined();
        expect(h.starts).toEqual([]);
        expect(h.sampled).toEqual(["sentinel-setup", "success"]);
        expect(receipt.cleanupKnown).toBe(true);
      },
    );
  });
  it.each(["before", "sample", "wait", "ready"] as const)(
    "never launches after cancellation at %s",
    async (cancel) => {
      await withHarness(
        {
          cancel,
          sample: (label, _index, sample) => ({
            ...sample,
            cpuUsed: cancel === "wait" && label === "success" ? 90 : 30,
          }),
        },
        async (h) => {
          const { error, receipt } = await h.run();
          expect(error).toBeDefined();
          expect(h.starts).toEqual([]);
          expect(receipt).toMatchObject({
            cleanupKnown: true,
            qualification: "not-established",
            cases: [],
          });
        },
      );
    },
  );
  it.each([
    "missing",
    "malformed",
    "allocating",
    "source",
    "build",
    "admission",
    "retained",
    "device",
    "server",
    "scratch",
  ] as const)("cannot prove cleanup from a %s receipt", async (receipt) => {
    await withHarness({ receipt }, async (h) => {
      const { error, receipt: result } = await h.run();
      expect(error).toBeDefined();
      expect(h.stops).toContain("success");
      expect(result).toMatchObject({
        cleanupKnown: false,
        sentinelsRemoved: true,
        qualification: "not-established",
        cases: [],
      });
      expect(result.journeys[0].state).toBe("launch-unresolved");
    });
  });
  it.each(["spawn", "settlement", "stop"] as const)(
    "keeps %s effects unresolved",
    async (failure) => {
      await withHarness({ failure }, async (h) => {
        const { error, receipt } = await h.run();
        expect(error).toBeDefined();
        expect(receipt.cleanupKnown).toBe(false);
        expect(receipt.journeys[0].state).toBe("launch-unresolved");
        if (failure !== "spawn") expect(h.stops).toContain("success");
      });
    },
  );
  it.each(["progress", "hash"] as const)(
    "preserves a %s failure while reconciling current cleanup",
    async (failure) => {
      await withHarness({ failure }, async (h) => {
        const { error, receipt } = await h.run();
        expect(error).toBeInstanceOf(AggregateError);
        expect(receipt).toMatchObject({
          cleanupKnown: true,
          qualification: "not-established",
          cases: [],
        });
        expect(receipt.journeys[0].state).toBe("cleanup-proved");
        expect(h.stops).toContain("success");
        if (failure === "hash") expect(receipt.failure).toBe("receipt hash failed");
        else expect(receipt.failure).toMatch(/JSON|property/);
      });
    },
  );
  it("preserves the primary parse error when runner settlement also fails", async () => {
    await withHarness({ failure: "progress", settlementFailure: true }, async (h) => {
      const { receipt } = await h.run();
      expect(receipt.failure).toMatch(/JSON|property/);
      expect(receipt.cleanupKnown).toBe(false);
    });
  });
  it("keeps a cancelled admission after a clean case incomplete", async () => {
    await withHarness({ cancel: "sample", target: "failure" }, async (h) => {
      const { error, receipt } = await h.run();
      expect(error).toBeDefined();
      expect(h.starts).toEqual(["success"]);
      expect(receipt).toMatchObject({ cleanupKnown: true, qualification: "not-established" });
      expect(receipt.cases).toHaveLength(1);
    });
  });
  it("does not let one clean case hide a subsequent unresolved launch", async () => {
    await withHarness({ target: "failure", receipt: "missing" }, async (h) => {
      const { receipt } = await h.run();
      expect(h.starts).toEqual(["success", "failure"]);
      expect(receipt.cases).toHaveLength(1);
      expect(receipt.cleanupKnown).toBe(false);
      expect(receipt.journeys.map((j: JourneyAttempt) => j.state)).toEqual([
        "cleanup-proved",
        "launch-unresolved",
        "not-launched",
        "not-launched",
      ]);
    });
  });
  it("rejects a preexisting receipt before spawning", async () => {
    await withHarness({ preexistingReceipt: true }, async (h) => {
      const { receipt } = await h.run();
      expect(h.starts).toEqual([]);
      expect(receipt).toMatchObject({
        cleanupKnown: true,
        failure: "Journey receipt already exists",
        qualification: "not-established",
      });
    });
  });
  it("proves cleanup after owner cancellation using signal-independent teardown reads", async () => {
    await withHarness({ cancel: "running" }, async (h) => {
      const { error, receipt } = await h.run();
      expect(error).toBeDefined();
      expect(receipt).toMatchObject({
        cleanupKnown: true,
        qualification: "not-established",
        failure: "owner cancelled",
      });
      expect(h.readsAfterAbort.length).toBeGreaterThan(0);
      expect(h.readsAfterAbort.every(Boolean)).toBe(true);
      expect(h.stops).toContain("success");
    });
  });
  it.each(["device", "groups", "scratch", "port", "sentinel", "create", "parent"] as const)(
    "requires positive %s cleanup observations",
    async (observation) => {
      await withHarness({ observation }, async (h) => {
        const { error, receipt } = await h.run();
        expect(error).toBeDefined();
        expect(receipt.cleanupKnown).toBe(false);
        if (observation === "create") expect(h.starts).toEqual([]);
      });
    },
  );
  it.each([
    ["success", "failed-success"],
    ["failure", "successful-failure"],
    ["failure", "wrong-code"],
    ["cancellation", "early-cancellation"],
  ] as const)(
    "keeps unexpected %s/%s behavior failed even with cleanup proof",
    async (target, behavior) => {
      await withHarness({ target, behavior }, async (h) => {
        const { error, receipt } = await h.run();
        expect(error).toBeDefined();
        expect(receipt).toMatchObject({ cleanupKnown: true, qualification: "not-established" });
      });
    },
  );
  it("requires the captured Xcode child for intended cancellation", async () => {
    await withHarness({ target: "cancellation", receipt: "xcode" }, async (h) => {
      const { receipt } = await h.run();
      expect(receipt).toMatchObject({
        cleanupKnown: true,
        qualification: "not-established",
        failure: "Captured Xcode child missing at cancellation",
      });
    });
  });
  it.each(["pass", "busy", "unresolved", "behavior"])(
    "accounts independently for pinned control: %s",
    async (kind) => {
      await withHarness(
        {
          mode: "--control-pinned",
          ...(kind === "unresolved" ? { receipt: "missing" as const } : {}),
          ...(kind === "behavior" ? { behavior: "failed-success" as const } : {}),
          sample: (_label, index, sample) => ({
            ...sample,
            cpuUsed: kind === "busy" || index === 0 ? 85 : 30,
          }),
        },
        async (h) => {
          const { error, receipt } = await h.run();
          expect(h.starts).toEqual(kind === "busy" ? [] : ["control-pinned"]);
          expect(receipt.cleanupKnown).toBe(kind !== "unresolved");
          expect(receipt.journeys).toHaveLength(1);
          expect(receipt.qualification).toBe("not-established");
          if (kind === "pass") {
            expect(error).toBeUndefined();
            expect(receipt.outcome).toBe("pass");
          } else expect(error).toBeDefined();
        },
      );
    },
  );
  it.each(["missing", "source", "build"] as const)(
    "preserves the pinned-control %s prerequisite",
    async (prerequisite) => {
      await withHarness({ mode: "--control-pinned", prerequisite }, async (h) => {
        const { error, receipt } = await h.run();
        expect(error).toBeDefined();
        expect(h.starts).toEqual([]);
        expect(receipt.cleanupKnown).toBe(true);
      });
    },
  );
});

describe("preserved native failure diagnostics", () => {
  it("retains a bound unknown child failure separately from receipt validation", async () => {
    await withHarness({ receipt: "unknown" }, async (h) => {
      const { error, receipt } = await h.run();
      expect(error).toBeInstanceOf(AggregateError);
      expect(h.starts).toEqual(["success"]);
      expect(receipt).toMatchObject({
        cleanupKnown: false,
        qualification: "not-established",
        cases: [],
      });
      expect(receipt.journeys[0]).toMatchObject({
        state: "launch-unresolved",
        cleanupFailure: "Incomplete receipt",
        childFailure: {
          primary: "Owned command timed out; operation effect requires readback",
          primaryErrors: [
            "Owned command timed out; operation effect requires readback",
            "kill EPERM",
          ],
          cleanupErrors: ["captured child/group cleanup unresolved"],
        },
      });
    });
  });
  it("retains the original sentinel stop error alongside aggregate cleanup failure", async () => {
    await withHarness({ target: "/bin/sleep", failure: "stop" }, async (h) => {
      const { error, receipt } = await h.run();
      expect(h.starts).toEqual(SENTINEL_CASES);
      expect(error).toBeInstanceOf(AggregateError);
      expect(receipt).toMatchObject({
        cleanupKnown: false,
        sentinelsRemoved: false,
        qualification: "not-established",
        sentinelStopErrors: ["stop unresolved", "runner settlement failed"],
        cleanupFailure: "Sentinel cleanup unresolved",
      });
    });
  });
});

describe("bounded sentinel failure fields", () => {
  it("copies only bounded redacted causes from a bound receipt", async () => {
    await withHarness({ receipt: "unknown", diagnosticPayload: true }, async (h) => {
      const { receipt } = await h.run();
      const failure = receipt.journeys[0].childFailure;
      expect(Object.keys(failure).sort()).toEqual(["cleanupErrors", "primary", "primaryErrors"]);
      expect(failure.primaryErrors).toHaveLength(8);
      expect(failure.cleanupErrors).toHaveLength(8);
      for (const text of [failure.primary, ...failure.primaryErrors, ...failure.cleanupErrors]) {
        expect(text).toHaveLength(512);
        expect(Buffer.byteLength(text)).toBeLessThanOrEqual(2048);
        expect(text).toContain("[REDACTED]");
      }
      const serialized = JSON.stringify(failure);
      expect(serialized).not.toContain(NodePath.dirname(h.root));
      expect(serialized).not.toMatch(/raw-private|stdout|argv/);
    });
  });
  it.each(["source", "build", "admission"] as const)(
    "does not adopt diagnostics before %s binding",
    async (receipt) => {
      await withHarness({ receipt, diagnosticPayload: true }, async (h) => {
        const { receipt: result } = await h.run();
        expect(result.journeys[0].childFailure).toBeUndefined();
        expect(result.cleanupKnown).toBe(false);
        expect(h.starts).toEqual(["success"]);
      });
    },
  );
});
