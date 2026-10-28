import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesOperationReceipt,
  JonesOperationStartInput,
} from "@t3tools/contracts/jones/jonesOperations";
import { createJonesActionJournal, JONES_ACTION_JOURNAL_KEY } from "./actionJournal.ts";

const environmentId = EnvironmentId.make("journal-host");
const otherEnvironment = EnvironmentId.make("unrelated-host");
const targetSource = "a".repeat(40),
  installed = "b".repeat(40);
const id = (n: number) => `${String(n).padStart(8, "0")}-1111-4111-8111-111111111111`;
const request: JonesOperationStartInput = {
  environmentId,
  operationId: id(1),
  expectedInstalledSource: installed,
  action: { kind: "download-exact", targetSource },
};
const receipt = (phase: JonesOperationReceipt["phase"] = "running"): JonesOperationReceipt => ({
  protocol: 1,
  ...request,
  phase,
  acceptedAt: "2026-10-27T04:00:00Z",
  updatedAt: "2026-10-27T04:00:01Z",
});
function disk() {
  const values = new Map<string, string>();
  return {
    values,
    getItem: (key: string) => values.get(key) ?? null,
    setItem: vi.fn((key: string, value: string) => {
      values.set(key, value);
    }),
  };
}

describe("durable client update duties", () => {
  it("serializes two runtimes against durable storage and refuses a second environment owner", async () => {
    const storage = disk();
    let lock: Promise<unknown> = Promise.resolve();
    const shared = {
      ...storage,
      withLock: <A>(work: () => Promise<A>) => {
        const result = lock.then(work);
        lock = result.catch(() => undefined);
        return result;
      },
    };
    const first = createJonesActionJournal(shared),
      second = createJonesActionJournal(shared);
    await Promise.all([first.load(), second.load()]);
    await first.reserve({
      id: id(1),
      environmentId,
      kind: "legacy",
      action: "download",
      targetSource,
    });
    await expect(
      second.reserve({
        id: id(2),
        environmentId,
        kind: "legacy",
        action: "download",
        targetSource,
      }),
    ).rejects.toThrow();
    const restored = createJonesActionJournal(storage);
    await restored.load();
    expect(restored.getSnapshot().duties.map((d) => d.id)).toEqual([id(1)]);
  });
  it("restores legacy uncertainty in a fresh runtime, and never treats source/revision or another receipt as proof", async () => {
    const storage = disk();
    const first = createJonesActionJournal(storage);
    await first.reserve({
      id: id(1),
      environmentId,
      kind: "legacy",
      action: "install",
      targetSource,
      stagedHandle: "stage",
      currentVersion: "old",
    });
    const restarted = createJonesActionJournal(storage);
    expect(restarted.getSnapshot().ready).toBe(false);
    await restarted.load();
    expect(restarted.getSnapshot().duties).toHaveLength(1);
    expect(await restarted.reconcileReceipt(receipt("stopped"))).toBe(false);
    await expect(
      restarted.reserve({
        id: id(2),
        environmentId,
        kind: "legacy",
        action: "download",
        targetSource,
      }),
    ).rejects.toThrow("earlier request");
    expect(restarted.getSnapshot().duties[0]?.id).toBe(id(1));
  });

  it("settles only an exactly bound operation and retains the unrelated environment", async () => {
    const storage = disk(),
      journal = createJonesActionJournal(storage);
    await journal.reserve({
      id: id(1),
      environmentId,
      kind: "operation",
      action: "download-exact",
      targetSource,
      operationId: id(1),
      request,
    });
    await journal.reserve({
      id: id(2),
      environmentId: otherEnvironment,
      kind: "legacy",
      action: "download",
      targetSource,
    });
    expect(await journal.reconcileReceipt({ ...receipt("stopped"), operationId: id(99) })).toBe(
      false,
    );
    expect(
      await journal.reconcileReceipt({ ...receipt("stopped"), environmentId: otherEnvironment }),
    ).toBe(false);
    expect(await journal.reconcileReceipt(receipt("stopped"))).toBe(true);
    const restarted = createJonesActionJournal(storage);
    await restarted.load();
    expect(restarted.getSnapshot().duties.map((d) => d.id)).toEqual([id(2)]);
    expect(restarted.getSnapshot().history[0]?.receipt?.phase).toBe("stopped");
  });

  it("holds Stop through missing/wrong IDs, held results, backwards receipts and a late start reply", async () => {
    const storage = disk(),
      journal = createJonesActionJournal(storage);
    await journal.reserve({
      id: id(3),
      environmentId,
      kind: "stop",
      action: "stop",
      targetSource,
      operationId: id(1),
      stopId: id(3),
      receipt: receipt(),
    });
    const stopped = {
      ...receipt("stopped"),
      stop: { stopId: id(3), requestedAt: "2026-10-27T04:00:02Z", result: "stopped" as const },
      updatedAt: "2026-10-27T04:00:04Z",
    };
    expect(await journal.reconcileReceipt(receipt("stopped"))).toBe(false);
    expect(
      await journal.reconcileReceipt({ ...stopped, stop: { ...stopped.stop, stopId: id(4) } }),
    ).toBe(false);
    expect(
      await journal.reconcileReceipt({
        ...stopped,
        phase: "held",
        stop: { ...stopped.stop, result: "held" },
      }),
    ).toBe(false);
    expect(await journal.reconcileReceipt({ ...stopped, updatedAt: "2026-10-27T04:00:01Z" })).toBe(
      false,
    );
    expect(await journal.reconcileReceipt(receipt())).toBe(false);
    const restarted = createJonesActionJournal(storage);
    await restarted.load();
    expect(restarted.getSnapshot().duties[0]?.stopId).toBe(id(3));
    expect(await restarted.reconcileReceipt(stopped)).toBe(true);
    expect(restarted.getSnapshot().duties).toEqual([]);
    expect(restarted.getSnapshot().history[0]?.receipt?.stop?.stopId).toBe(id(3));
  });

  it("holds a terminal operation without a Stop result, then accepts only the monotone exact Stop refinement", async () => {
    const journal = createJonesActionJournal(disk());
    await journal.reserve({
      id: id(3),
      environmentId,
      kind: "stop",
      action: "stop",
      targetSource,
      operationId: id(1),
      stopId: id(3),
      receipt: receipt(),
    });
    const partial = {
      ...receipt("stopped"),
      stop: { stopId: id(3), requestedAt: "2026-10-27T04:00:02Z" },
      updatedAt: "2026-10-27T04:00:03Z",
    };
    expect(await journal.reconcileReceipt(partial)).toBe(false);
    expect(journal.getSnapshot().duties).toHaveLength(1);
    expect(
      await journal.reconcileReceipt({
        ...partial,
        stop: { ...partial.stop, result: "stopped" },
        updatedAt: "2026-10-27T04:00:02Z",
      }),
    ).toBe(false);
    expect(
      await journal.reconcileReceipt({
        ...partial,
        stop: { ...partial.stop, result: "stopped" },
        updatedAt: "2026-10-27T04:00:04Z",
      }),
    ).toBe(true);
  });

  it.each(["read", "write", "corrupt", "capacity"])(
    "fails closed on %s without evicting unresolved duties",
    async (failure) => {
      const storage = disk();
      if (failure === "corrupt") storage.values.set(JONES_ACTION_JOURNAL_KEY, "{bad-json");
      const journal = createJonesActionJournal({
        getItem: (key) => {
          if (failure === "read") throw new Error("unavailable");
          return storage.getItem(key);
        },
        setItem: (key, value) => {
          if (failure === "write") throw new Error("unavailable");
          storage.setItem(key, value);
        },
      });
      if (failure === "capacity") {
        for (let n = 1; n <= 64; n++)
          await journal.reserve({
            id: id(n),
            environmentId: EnvironmentId.make(`capacity-${n}`),
            kind: "legacy",
            action: "download",
            targetSource,
          });
      }
      await expect(
        journal.reserve({
          id: id(99),
          environmentId,
          kind: "legacy",
          action: "download",
          targetSource,
        }),
      ).rejects.toThrow();
      if (failure === "capacity") expect(journal.getSnapshot().duties).toHaveLength(64);
      else expect(journal.getSnapshot().fault).toBeDefined();
    },
  );

  it("persists correlation without host diagnostic text or private outcome reasons", async () => {
    const storage = disk(),
      journal = createJonesActionJournal(storage);
    await journal.reserve({
      id: id(3),
      environmentId,
      kind: "stop",
      action: "stop",
      targetSource,
      operationId: id(1),
      stopId: id(3),
      receipt: {
        ...receipt(),
        message: "private host diagnostics",
        internalHostState: "private extra payload",
      } as JonesOperationReceipt,
    });
    expect(storage.values.get(JONES_ACTION_JOURNAL_KEY)).not.toContain("private host diagnostics");
    expect(storage.values.get(JONES_ACTION_JOURNAL_KEY)).not.toContain("private extra payload");
  });
});
