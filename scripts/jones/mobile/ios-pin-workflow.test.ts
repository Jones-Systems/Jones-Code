import { describe, expect, it } from "vite-plus/test";
import * as NodeOS from "node:os";
import {
  acceptCreatedDevice,
  assertAdmission,
  assertPersistence,
  assertTransition,
  awaitTransition,
  main,
  nativeClaimAllowed,
  OwnedCleanup,
  parseThread,
  PHASES,
  PinPhases,
  redact,
  runInjected,
} from "./ios-pin-workflow.ts";
import type { PairState, WorkflowIO } from "./ios-pin-workflow.ts";

const pinned = "2026-10-21T12:00:00.000Z";
const pair = (
  pinnedAt: string | null = null,
  sequence = 1,
  control: string | null = null,
): PairState => ({
  target: { id: "a", pinnedAt, sequence },
  control: { id: "b", pinnedAt: control, sequence },
});
function rehearsal(control: string | null = null) {
  let state = pair(null, 1, control);
  const actions: string[] = [];
  const io: WorkflowIO = {
    pin: async () => {
      actions.push("pin");
      state = pair(pinned, 2, control);
    },
    unpin: async () => {
      actions.push("unpin");
      state = pair(null, 3, control);
    },
    menu: async (label) => {
      expect(label).toBe(state.target.pinnedAt ? "Unpin" : "Pin");
      actions.push(label);
    },
    read: async () => structuredClone(state),
    relaunch: async () => {
      actions.push("relaunch");
    },
  };
  return { io, actions };
}

describe("pin oracle", () => {
  it.each([null, pinned])("pins/unpins while preserving control %s", async (control) => {
    const { io, actions } = rehearsal(control);
    const receipt = await runInjected(io);
    expect(receipt).toMatchObject({
      runner: "injected",
      tier: "B-rehearsal",
      nativeClaim: false,
      oracle: { uiAfterPinnedRelaunch: "Unpin", uiAfterUnpinnedRelaunch: "Pin" },
    });
    expect(receipt.oracle.pinned.control.pinnedAt).toBe(control);
    expect(actions).toEqual(["pin", "relaunch", "Unpin", "unpin", "relaunch", "Pin"]);
  });
  it("no-action deliberately fails the same pin oracle", async () => {
    const { io, actions } = rehearsal();
    await expect(runInjected(io, true)).rejects.toThrow("Pin oracle did not converge");
    expect(actions).toEqual([]);
  });
  it("rejects mutation of the control, non-advancing sequence and failed unpin", () => {
    expect(() => assertTransition(pair(), pair(null, 2, pinned), true)).toThrow(
      "control pin changed",
    );
    expect(() => assertTransition(pair(), pair(pinned), true)).toThrow("sequence did not advance");
    expect(() => assertTransition(pair(pinned, 2), pair(pinned, 3), false)).toThrow(
      "target was not unpinned",
    );
  });
  it("rejects drift after relaunch but allows an unrelated sequence advance", () => {
    expect(() => assertPersistence(pair(pinned, 2), pair(null, 3))).toThrow("relaunch changed");
    expect(() => assertPersistence(pair(pinned, 2), pair(pinned, 3))).not.toThrow();
    expect(() => assertPersistence(pair(pinned, 2), pair(pinned, 1))).toThrow("sequence regressed");
  });
  it("requires native menu persistence separately from backend persistence", async () => {
    const { io } = rehearsal();
    io.menu = async () => {
      throw new Error("native menu missing");
    };
    await expect(runInjected(io)).rejects.toThrow("native menu missing");
  });
  it("waits only for readback convergence and does not repeat actions", async () => {
    let reads = 0;
    const after = await awaitTransition(
      async () => (++reads === 1 ? pair() : pair(pinned, 2)),
      pair(),
      true,
      { intervalMs: 1 },
    );
    expect(after.target.pinnedAt).toBe(pinned);
    expect(reads).toBe(2);
  });
  it("fails control drift immediately during convergence", async () => {
    await expect(awaitTransition(async () => pair(null, 2, pinned), pair(), true)).rejects.toThrow(
      "control pin changed",
    );
  });
  it("bounds absence and honors cancellation", async () => {
    await expect(
      awaitTransition(async () => pair(), pair(), true, { timeoutMs: 0 }),
    ).rejects.toThrow("did not converge");
    await expect(
      awaitTransition(async () => pair(), pair(), true, { signal: AbortSignal.abort() }),
    ).rejects.toThrow();
  });
  it("validates authenticated thread identity and timestamp", () => {
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
      parseThread(
        { snapshotSequence: 5, projection: { thread: { id: "a", pinnedAt: "garbage" } } },
        "a",
      ),
    ).toThrow("pinnedAt unavailable");
  });
});

describe("native phase handshake", () => {
  it("requires every phase in order and checks state across both relaunches", async () => {
    let state = pair();
    const phases = new PinPhases(async () => structuredClone(state));
    await phases.accept("initial");
    state = pair(pinned, 2);
    await phases.accept("pinned");
    await phases.accept("pinnedRelaunch");
    state = pair(null, 3);
    await phases.accept("unpinned");
    await phases.accept("unpinnedRelaunch");
    await phases.accept("complete");
    phases.assertComplete();
    expect(phases.observations.map((item) => item.phase)).toEqual(PHASES);
  });
  it("fails closed after a skipped/duplicate phase and cannot recover by replay", async () => {
    const phases = new PinPhases(async () => pair());
    await phases.accept("initial");
    await expect(phases.accept("complete")).rejects.toThrow("unexpected or duplicate");
    await expect(phases.accept("initial")).rejects.toThrow();
    expect(() => phases.assertComplete()).toThrow();
  });
  it("a zero test exit without the handshake is insufficient", () => {
    expect(() => new PinPhases(async () => pair()).assertComplete()).toThrow("journey incomplete");
  });
});

describe("admission and device identity", () => {
  it("never claims native qualification with a shortened fixture timeout", () => {
    expect(nativeClaimAllowed(30_000)).toBe(true);
    expect(nativeClaimAllowed(1)).toBe(false);
    expect(nativeClaimAllowed(29_999)).toBe(false);
  });
  const admission = () => ({
    sourceSha: "a".repeat(40),
    hostname: NodeOS.hostname(),
    serialSlot: "root-slot-1",
    measuredAt: 100,
    cpuUsed: 20,
    availableMemoryBytes: 8 * 1024 ** 3,
    memoryPressure: "normal",
    swapBytesPerSecond: 0,
    freeDiskBytes: 30 * 1024 ** 3,
  });
  it("requires a fresh same-host root slot and disk/CPU/memory evidence", () => {
    expect(() => assertAdmission(admission(), "a".repeat(40), 101)).not.toThrow();
    expect(() => assertAdmission(admission(), "a".repeat(40), 60_100)).toThrow(
      "fresh root admission",
    );
    expect(() => assertAdmission({ ...admission(), cpuUsed: 85 }, "a".repeat(40), 101)).toThrow(
      "CPU admission",
    );
    expect(() =>
      assertAdmission({ ...admission(), freeDiskBytes: 29 * 1024 ** 3 }, "a".repeat(40), 101),
    ).toThrow("30 GiB");
    expect(() =>
      assertAdmission({ ...admission(), hostname: "another-host" }, "a".repeat(40), 101),
    ).toThrow("host mismatch");
    expect(() => assertAdmission({ ...admission(), serialSlot: "" }, "a".repeat(40), 101)).toThrow(
      "root serial slot",
    );
  });
  it("plan mode needs no native tools and makes no qualification claim", async () => {
    expect(await main(["--plan"])).toMatchObject({
      adapter: "owned-xcodebuild-xcuitest",
      nativeClaim: false,
      qualification: "pending-native-and-sentinel-evidence",
    });
  });
  it("accepts only a newly returned UDID", () => {
    const udid = "12345678-1234-1234-1234-123456789abc";
    expect(acceptCreatedDevice(udid, [], "owned")).toBe(udid);
    expect(() => acceptCreatedDevice(udid, [{ udid, name: "foreign" }], "owned")).toThrow(
      "already existed",
    );
    expect(() => acceptCreatedDevice(udid, [{ udid: "other", name: "owned" }], "owned")).toThrow(
      "already existed",
    );
    expect(() => acceptCreatedDevice("", [], "owned")).toThrow("effect unknown");
  });
  it("redacts pairing secrets", () => {
    expect(redact("token=secret cookie=session=abc", ["secret", "session=abc"])).toBe(
      "token=[REDACTED] cookie=[REDACTED]",
    );
  });
});

describe("owned cleanup", () => {
  it.each([undefined, "pin failed"])(
    "cleans in reverse and preserves primary %s",
    async (primary) => {
      const cleanup = new OwnedCleanup(),
        calls: string[] = [];
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
  it("retains primary and cleanup failures and holds scratch after unknown teardown", async () => {
    const cleanup = new OwnedCleanup(),
      calls: string[] = [];
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
    cleanup.add("child", async () => {
      throw new Error("owned group unresolved");
    });
    const result = await cleanup.finish("pin failed");
    expect(result).toMatchObject({
      outcome: "unknown",
      primary: "pin failed",
      retainScratch: true,
    });
    expect(result.cleanup[0]).toMatchObject({ status: "failed", error: "owned group unresolved" });
    expect(calls).toEqual(["server"]);
  });
  it("cleanup failure prevents an injected success receipt", async () => {
    const cleanup = new OwnedCleanup();
    cleanup.add("child", async () => {
      throw new Error("unknown exit");
    });
    await expect(runInjected(rehearsal().io, false, cleanup)).rejects.toThrow("cleanup unresolved");
  });
  it("never cleans a sibling invocation", async () => {
    const first = new OwnedCleanup(),
      second = new OwnedCleanup(),
      calls: string[] = [];
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
