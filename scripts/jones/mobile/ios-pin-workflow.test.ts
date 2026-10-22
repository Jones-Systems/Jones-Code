import { describe, expect, it } from "vite-plus/test";
import {
  acceptCreatedDevice,
  assertPersistence,
  assertTransition,
  blockers,
  driverArgv,
  driverEnvironment,
  FreshRefs,
  main,
  OwnedCleanup,
  parseThread,
  redact,
  runInjected,
} from "./ios-pin-workflow.ts";
import type { GateFacts, PairState, WorkflowIO } from "./ios-pin-workflow.ts";

// Synthetic CLI envelopes follow v0.21.25 src/commands/output/json.ts and CaptureSnapshotResult; no native capture.
const snapshot = (labels: string[], generation = 1) => ({
  success: true,
  data: {
    refsGeneration: generation,
    nodes: labels.map((label, index) => ({ ref: `e${index + 1}`, label, index })),
  },
});
const pair = (
  pinnedAt: string | null = null,
  sequence = 1,
  control: string | null = null,
): PairState => ({
  target: { id: "a", pinnedAt, sequence },
  control: { id: "b", pinnedAt: control, sequence },
});
const pinned = "2026-10-21T12:00:00.000Z";

function rehearsal(control: string | null = null) {
  let state = pair(null, 1, control);
  let menu = false;
  let generation = 0;
  const actions: readonly string[][] = [];
  const log = actions as string[][];
  const io: WorkflowIO = {
    snapshot: async () =>
      snapshot(menu ? [state.target.pinnedAt ? "Unpin" : "Pin"] : ["Target"], ++generation),
    action: async (argv) => {
      log.push([...argv]);
      if (argv[0] === "longpress") menu = true;
      else {
        state = pair(state.target.pinnedAt ? null : pinned, state.target.sequence + 1, control);
        menu = false;
      }
    },
    read: async () => structuredClone(state),
    relaunch: async () => {
      menu = false;
    },
  };
  return { io, actions };
}

describe("fresh snapshot refs", () => {
  it("matches exact labels and pins driver generation into consumed refs", () => {
    const refs = new FreshRefs();
    refs.observe(snapshot(["Target", "Target extra"], 7));
    expect(refs.consume(refs.select("Target"))).toBe("@e1~s7");
    expect(() => refs.select("Target")).toThrow("fresh snapshot required");
  });
  it("rejects missing, ambiguous, malformed and stale references", () => {
    const refs = new FreshRefs();
    refs.observe(snapshot(["Target"]));
    const old = refs.select("Target");
    refs.observe(snapshot(["Target"], 2));
    expect(() => refs.consume(old)).toThrow("stale snapshot ref");
    expect(() => refs.select("Other")).toThrow("matched 0 nodes");
    refs.observe(snapshot(["Target", "Target"], 3));
    expect(() => refs.select("Target")).toThrow("matched 2 nodes");
    expect(() => refs.observe({ success: true, data: { nodes: [] } })).toThrow(
      "generation unavailable",
    );
    expect(() => refs.select("Target")).toThrow("fresh snapshot required");
    expect(() =>
      refs.observe({ success: true, data: { refsGeneration: 1, nodes: [], truncated: true } }),
    ).toThrow("truncated");
    refs.observe({
      success: true,
      data: { refsGeneration: 1, nodes: [{ label: "Target", ref: "coordinates" }] },
    });
    expect(() => refs.select("Target")).toThrow("ref unavailable");
  });
  it("does not reuse a ref after relaunch", () => {
    const refs = new FreshRefs();
    refs.observe(snapshot(["Target"]));
    const old = refs.select("Target");
    refs.invalidate();
    expect(() => refs.consume(old)).toThrow("stale snapshot ref");
  });
});

describe("pin oracle", () => {
  it.each([null, pinned])("pins/unpins A while preserving control %s", async (control) => {
    const { io, actions } = rehearsal(control);
    const receipt = await runInjected(io, "Target");
    expect(receipt).toMatchObject({
      runner: "injected",
      tier: "B-rehearsal",
      nativeClaim: false,
      oracle: { uiAfterPinnedRelaunch: "Unpin", uiAfterUnpinnedRelaunch: "Pin" },
    });
    expect(receipt.oracle.pinned.control.pinnedAt).toBe(control);
    expect(receipt.oracle.unpinned.target.pinnedAt).toBeNull();
    expect(actions.map((args) => args[0])).toEqual([
      "longpress",
      "click",
      "longpress",
      "click",
      "longpress",
    ]);
  });
  it("fails when pin lands on the control thread", () => {
    const wrong = pair(null, 2, pinned);
    expect(() => assertTransition(pair(), wrong, true)).toThrow("control pin changed");
  });
  it("fails a non-advancing sequence", () => {
    expect(() => assertTransition(pair(), pair(pinned), true)).toThrow("sequence did not advance");
  });
  it("fails when A stays pinned after unpin", () => {
    expect(() => assertTransition(pair(pinned, 2), pair(pinned, 3), false)).toThrow(
      "target was not unpinned",
    );
  });
  it("fails server drift after relaunch", () => {
    expect(() => assertPersistence(pair(pinned, 2), pair(null, 3))).toThrow("relaunch changed");
  });
  it("allows unrelated sequence advancement but rejects identity drift or regression", () => {
    expect(() => assertPersistence(pair(pinned, 2), pair(pinned, 3))).not.toThrow();
    expect(() => assertPersistence(pair(pinned, 2), pair(pinned, 1))).toThrow("sequence regressed");
    const wrong = pair(pinned, 3);
    wrong.control.id = "another-thread";
    expect(() => assertPersistence(pair(pinned, 2), wrong)).toThrow("thread identity");
  });
  it("requires native menu persistence separately from server readback", async () => {
    const { io } = rehearsal();
    let relaunched = false;
    const originalRelaunch = io.relaunch;
    const originalSnapshot = io.snapshot;
    io.relaunch = async () => {
      relaunched = true;
      await originalRelaunch();
    };
    io.snapshot = async () => (relaunched ? snapshot(["Target", "Pin"], 5) : originalSnapshot());
    await expect(runInjected(io, "Target")).rejects.toThrow('selector "Unpin" matched 0 nodes');
  });
  it("rejects a malformed non-null pin timestamp", () => {
    expect(() =>
      parseThread(
        { snapshotSequence: 5, projection: { thread: { id: "a", pinnedAt: "garbage" } } },
        "a",
      ),
    ).toThrow("pinnedAt unavailable");
  });
  it("validates authenticated readback structure and identity", () => {
    expect(
      parseThread(
        { snapshotSequence: 5, projection: { thread: { id: "a", pinnedAt: null } } },
        "a",
      ),
    ).toEqual({ id: "a", pinnedAt: null, sequence: 5 });
    expect(() =>
      parseThread(
        { snapshotSequence: 5, projection: { thread: { id: "b", pinnedAt: null } } },
        "a",
      ),
    ).toThrow("identity mismatch");
    expect(() =>
      parseThread({ snapshotSequence: 5, projection: { thread: { id: "a" } } }, "a"),
    ).toThrow("pinnedAt unavailable");
  });
});

describe("native boundaries", () => {
  it("scopes driver commands and does not inherit daemon or provider secrets", () => {
    const target = { session: "owned-session", udid: "owned-udid", config: "/owned/config.json" };
    expect(driverArgv(target, ["longpress", "@e2~s5"])).toEqual([
      "longpress",
      "@e2~s5",
      "--config",
      "/owned/config.json",
      "--session",
      "owned-session",
      "--platform",
      "ios",
      "--udid",
      "owned-udid",
      "--json",
      "--response-level",
      "full",
    ]);
    const env = driverEnvironment("/owned", {
      PATH: "/bin",
      AGENT_DEVICE_DAEMON_BASE_URL: "foreign",
      AGENT_DEVICE_DAEMON_AUTH_TOKEN: "secret",
      AGENT_DEVICE_CONFIG: "foreign-config",
      OPENAI_API_KEY: "secret",
    });
    expect(env).toMatchObject({
      AGENT_DEVICE_HOME: "/owned/driver-home",
      AGENT_DEVICE_STATE_DIR: "/owned/driver-state",
      AGENT_DEVICE_IOS_RUNNER_DERIVED_PATH: "/owned/driver/.tmp/runner",
    });
    expect(env.AGENT_DEVICE_DAEMON_BASE_URL).toBeUndefined();
    expect(env.AGENT_DEVICE_DAEMON_AUTH_TOKEN).toBeUndefined();
    expect(env.AGENT_DEVICE_CONFIG).toBeUndefined();
    expect(env.OPENAI_API_KEY).toBeUndefined();
  });
  it("fails every unavailable gate and always holds the pinned pattern-kill driver", () => {
    expect(blockers({})).toEqual([
      "G1-canonical-admission-unavailable",
      "G2-source-app-identity",
      "G3-driver-source-identity",
      "G4-xctestdevices-shared-path",
      "G5-process-scan-partial",
      "G6-xcode-runtime",
      "G7-runner-output",
      "driver-pattern-kill-cleanup",
    ]);
  });
  it("a fully populated evidence record still cannot override the driver blocker", () => {
    const facts: GateFacts = {
      resourceDiagnostics: { cpuUsed: 20, cores: 10, freeBytes: 1_000_000, measuredAt: 100 },
      source: {
        head: "a".repeat(40),
        requested: "a".repeat(40),
        status: "",
        generatedIgnored: true,
      },
      app: {
        bundleId: "com.example.test",
        bundleSha256: "b".repeat(64),
        executableSha256: "c".repeat(64),
        recordedBundleSha256: "b".repeat(64),
        recordedExecutableSha256: "c".repeat(64),
        updatesEnabled: false,
        sourceSha: "a".repeat(40),
        buildArgv: [
          "xcodebuild",
          "-configuration",
          "Release",
          "CODE_SIGNING_ALLOWED=NO",
          "-derivedDataPath",
          "/owned/build",
        ],
      },
      driver: {
        version: "0.21.25",
        legacyRepairSha256: "61750fbc630752c7fe3339adc9d3e4df4701ff0fcfce6939e81036cacda15302",
      },
      host: {
        xctest: { kind: "directory" },
        foreignProcesses: [],
        firstLaunchStatus: 0,
        xcodeVersion: "Xcode 26.4\nBuild version 17E192",
      },
      runtime: { identifier: "runtime", isAvailable: true },
      runnerOutput: "owned-driver-copy/.tmp/runner",
    };
    expect(blockers(facts)).toEqual([
      "G1-canonical-admission-unavailable",
      "G5-process-scan-partial",
      "driver-pattern-kill-cleanup",
    ]);
    expect(blockers(facts)).toContain("G1-canonical-admission-unavailable");
    expect(blockers({ ...facts, app: { ...facts.app!, updatesEnabled: true } })).toContain(
      "G2-source-app-identity",
    );
    expect(blockers({ ...facts, host: { ...facts.host!, xctest: { kind: "symlink" } } })).toContain(
      "G4-xctestdevices-shared-path",
    );
    expect(
      blockers({ ...facts, host: { ...facts.host!, foreignProcesses: ["runner"] } }),
    ).toContain("G5-foreign-daemon");
  });
  it("plan mode requires no driver or files and reports the blocker", async () => {
    expect(await main(["--plan"])).toMatchObject({
      nativeEligible: false,
      blocker: "driver-pattern-kill-cleanup",
    });
  });
  it("accepts only a newly returned UDID and rejects name reuse", () => {
    const udid = "12345678-1234-1234-1234-123456789abc";
    expect(acceptCreatedDevice(udid, [], "owned")).toBe(udid);
    expect(() => acceptCreatedDevice(udid, [{ udid, name: "foreign" }], "owned")).toThrow(
      "already existed",
    );
    expect(() =>
      acceptCreatedDevice(udid, [{ udid: "different", name: "owned" }], "owned"),
    ).toThrow("already existed");
    expect(() => acceptCreatedDevice("", [], "owned")).toThrow("effect unknown");
  });
  it("redacts pairing secrets from output", () => {
    expect(redact("token=secret cookie=session=abc", ["secret", "session=abc"])).toBe(
      "token=[REDACTED] cookie=[REDACTED]",
    );
  });
});

describe("owned cleanup", () => {
  it("an injected pin interaction failure runs the owned teardown without masking the error", async () => {
    const cleanup = new OwnedCleanup();
    const calls: string[] = [];
    cleanup.add("owned-server", async () => {
      calls.push("server");
    });
    cleanup.add("returned-udid", async () => {
      calls.push("returned-udid");
    });
    cleanup.add("session", async () => {
      calls.push("session");
    });
    const { io } = rehearsal();
    io.action = async () => {
      throw new Error("pin interaction failed");
    };
    await expect(runInjected(io, "Target", cleanup)).rejects.toThrow("pin interaction failed");
    expect(calls).toEqual(["session", "returned-udid", "server"]);
  });
  it("cleanup failure prevents an injected success receipt", async () => {
    const cleanup = new OwnedCleanup();
    cleanup.add("session", async () => {
      throw new Error("unknown daemon");
    });
    await expect(runInjected(rehearsal().io, "Target", cleanup)).rejects.toThrow(
      "Injected cleanup unresolved",
    );
  });
  it.each([undefined, "pin failed"])(
    "cleans in reverse and preserves primary %s",
    async (primary) => {
      const cleanup = new OwnedCleanup();
      const calls: string[] = [];
      for (const label of ["scratch", "server", "created-device", "session"])
        cleanup.add(label, async () => {
          calls.push(label);
        });
      const result = await cleanup.finish(primary);
      expect(calls).toEqual(["session", "created-device", "server", "scratch"]);
      expect(result).toMatchObject({
        outcome: primary ? "fail" : "pass",
        primary: primary ?? null,
        retainScratch: false,
      });
    },
  );
  it("holds destructive cleanup after unknown teardown and keeps the original failure", async () => {
    const cleanup = new OwnedCleanup();
    const calls: string[] = [];
    cleanup.add(
      "scratch",
      async () => {
        calls.push("scratch");
      },
      true,
    );
    cleanup.add("server", async () => {
      calls.push("server");
    });
    cleanup.add(
      "created-device",
      async () => {
        calls.push("device");
      },
      true,
    );
    cleanup.add("session", async () => {
      throw new Error("daemon state unknown");
    });
    const result = await cleanup.finish("pin failed");
    expect(result).toMatchObject({
      outcome: "unknown",
      primary: "pin failed",
      retainScratch: true,
    });
    expect(calls).toEqual(["server"]);
    expect(result.cleanup.map((item) => item.status)).toEqual(["failed", "held", "passed", "held"]);
  });
  it("never cleans resources that were not registered by this run", async () => {
    const first = new OwnedCleanup(),
      second = new OwnedCleanup();
    const calls: string[] = [];
    first.add("first", async () => {
      calls.push("first");
    });
    second.add("second", async () => {
      calls.push("second");
    });
    await first.finish("create failed");
    expect(calls).toEqual(["first"]);
    await second.finish();
    expect(calls).toEqual(["first", "second"]);
    expect((await first.finish()).cleanup).toEqual([]);
  });
});
