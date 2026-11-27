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

  it("retains bound admission and treats reordered journal fields as the same request under the lock", async () => {
    const storage = disk();
    const journal = createJonesActionJournal({ ...storage, withLock: (work) => work() });
    const admission = {
      kind: "follower",
      enrollmentId: id(2),
      directiveId: id(3),
      targetSource,
      targetTree: "d".repeat(40),
    } as const;
    const input = {
      ...request,
      correlation: { planId: id(4), deviceKey: environmentId },
      admission,
    };
    const duty = {
      id: id(1),
      environmentId,
      kind: "operation",
      action: "download-exact",
      targetSource,
      operationId: id(1),
      request: input,
    } as const;
    await journal.reserve(duty);
    const reordered = {
      targetTree: admission.targetTree,
      directiveId: admission.directiveId,
      targetSource,
      enrollmentId: admission.enrollmentId,
      kind: admission.kind,
    };
    const sidecar = "jones.bound-action-duties.v1";
    storage.values.set(
      sidecar,
      JSON.stringify([{ ...duty, request: { ...input, admission: reordered } }]),
    );
    expect(JSON.parse(storage.values.get(JONES_ACTION_JOURNAL_KEY)!)).toMatchObject({
      duties: [{ kind: "legacy", operationId: id(1) }],
    });
    const stopped = { ...receipt("stopped"), ...input, admission: reordered };
    expect(await journal.reconcileReceipt(receipt("stopped"), { ledgerRead: true })).toBe(false);
    expect(
      await journal.reconcileReceipt(
        {
          ...stopped,
          admission: { ...admission, directiveId: id(5) },
        },
        { ledgerRead: true },
      ),
    ).toBe(false);
    expect(await journal.reconcileReceipt(stopped)).toBe(true);
    const reloaded = createJonesActionJournal(storage);
    await reloaded.load();
    expect(reloaded.getSnapshot().history[0]).toMatchObject({
      request: { admission },
      receipt: { admission, phase: "stopped" },
    });
  });

  it("keeps bound Stops exact and preserves unrelated work for older readers", async () => {
    const storage = disk();
    const shared = { ...storage, withLock: <A>(work: () => Promise<A>) => work() };
    const current = createJonesActionJournal(shared);
    const bound = {
      ...receipt(),
      correlation: { planId: id(3), deviceKey: environmentId },
      admission: { kind: "ordinary" as const },
    };
    const other = { ...receipt(), environmentId: otherEnvironment, operationId: id(4) };
    const stopDuty = {
      id: id(2),
      kind: "stop",
      action: "stop",
      environmentId,
      targetSource,
      operationId: bound.operationId,
      stopId: id(2),
      receipt: bound,
    } as const;
    await current.reserve({
      ...stopDuty,
      id: id(5),
      stopId: id(5),
      operationId: other.operationId,
      environmentId: otherEnvironment,
      receipt: other,
    });
    await current.reserve(stopDuty);
    const key = "jones.bound-action-duties.v1";
    const bindings = storage.values.get(key)!;
    const raw = storage.values.get(JONES_ACTION_JOURNAL_KEY)!;
    expect(JSON.parse(raw).duties[1]).toEqual({
      id: id(2),
      kind: "legacy",
      action: "stop",
      environmentId,
      targetSource,
      operationId: bound.operationId,
      stopId: id(2),
    });
    expect(raw).not.toContain("admission");
    storage.values.delete(key);
    const held = createJonesActionJournal(shared);
    await held.load();
    expect(held.getSnapshot().duties).toMatchObject([
      { kind: "stop", receipt: other },
      { kind: "legacy", stopId: id(2) },
    ]);
    await expect(held.reserve({ ...stopDuty, id: id(6), stopId: id(6) })).rejects.toThrow(
      "earlier request",
    );
    storage.values.set(key, bindings);
    const restored = createJonesActionJournal(shared);
    await restored.load();
    const stopped = {
      ...bound,
      phase: "stopped",
      updatedAt: "2026-10-27T04:05:02Z",
      stop: { stopId: id(2), requestedAt: "2026-10-27T04:05:01Z", result: "stopped" },
    } as const;
    const { admission: _admission, ...unbound } = stopped;
    expect(await restored.reconcileReceipt(unbound, { ledgerRead: true })).toBe(false);
    expect(await restored.reconcileReceipt(stopped, { ledgerRead: true })).toBe(true);
    const reopened = createJonesActionJournal(shared);
    await reopened.load();
    expect(reopened.getSnapshot().duties.map((d) => d.id)).toEqual([id(5)]);
    expect(reopened.getSnapshot().history[0]).toMatchObject({ id: id(2), receipt: stopped });
    expect(storage.values.get(JONES_ACTION_JOURNAL_KEY)).not.toContain("admission");
  });

  it.each(["running", "held", "stopped", "unsupported"] as const)(
    "corrects an older archived unsupported reply with exact %s read evidence",
    async (outcome) => {
      const storage = disk();
      const provisional: JonesOperationReceipt = {
        ...request,
        protocol: 1,
        phase: "refused",
        code: "unsupported",
        updatedAt: "2026-10-27T04:05:00Z",
      };
      const duty = {
        id: id(1),
        environmentId,
        kind: "operation",
        action: "download-exact",
        targetSource,
        operationId: id(1),
        request,
      } as const;
      const waiting = createJonesActionJournal(storage);
      await waiting.reserve(duty);
      expect(await waiting.reconcileReceipt(provisional)).toBe(false);
      expect(waiting.getSnapshot().duties).toEqual([duty]);

      storage.values.set(
        JONES_ACTION_JOURNAL_KEY,
        JSON.stringify({
          schema: 1,
          duties: [],
          history: [{ ...duty, receipt: provisional }],
        }),
      );
      const archived = createJonesActionJournal(storage);
      await archived.load();
      const proof = outcome === "unsupported" ? provisional : receipt(outcome);
      await archived.reconcileReceipt(proof);
      await archived.reconcileReceipt({ ...proof, operationId: id(99) }, { ledgerRead: true });
      expect(archived.getSnapshot().history[0]?.receipt).toEqual(provisional);
      await archived.reconcileReceipt(proof, { ledgerRead: true });
      const restarted = createJonesActionJournal(storage);
      await restarted.load();
      if (outcome === "running" || outcome === "held") {
        expect(restarted.getSnapshot().history).toEqual([]);
        expect(restarted.getSnapshot().duties).toEqual([{ ...duty, receipt: proof }]);
        const terminal = { ...receipt("stopped"), updatedAt: "2026-10-27T04:06:00Z" };
        expect(await restarted.reconcileReceipt(terminal)).toBe(true);
        expect(restarted.getSnapshot().duties).toEqual([]);
        expect(restarted.getSnapshot().history[0]?.receipt).toEqual(terminal);
      } else {
        expect(restarted.getSnapshot().duties).toEqual([]);
        expect(restarted.getSnapshot().history).toEqual([
          {
            ...duty,
            receipt: proof,
            ...(outcome === "unsupported" ? { unsupportedReceiptVerified: true } : {}),
          },
        ]);
        if (outcome === "unsupported") {
          expect(JSON.parse(storage.values.get(JONES_ACTION_JOURNAL_KEY)!)).toEqual({
            schema: 1,
            duties: [],
            history: [{ ...duty, receipt: provisional }],
          });
          await restarted.reconcileReceipt(receipt("running"), { ledgerRead: true });
          expect(restarted.getSnapshot().history[0]?.receipt).toEqual(provisional);
          expect(restarted.getSnapshot().duties).toEqual([]);
        }
      }
    },
  );

  it("keeps an unrelated pending Stop readable when optional refusal proof is unavailable", async () => {
    const storage = disk();
    const journal = createJonesActionJournal(storage);
    const stoppedOperation = { ...receipt(), environmentId: otherEnvironment };
    const stopId = id(3);
    await journal.reserve({
      kind: "stop",
      id: stopId,
      stopId,
      operationId: request.operationId,
      environmentId: otherEnvironment,
      action: "stop",
      targetSource,
      receipt: stoppedOperation,
    });
    await journal.reserve({
      id: request.operationId,
      environmentId,
      kind: "operation",
      action: "download-exact",
      operationId: request.operationId,
      targetSource,
      request,
    });
    const unsupported: JonesOperationReceipt = {
      ...request,
      protocol: 1,
      phase: "refused",
      code: "unsupported",
      updatedAt: "2026-10-27T04:01:00Z",
    };
    expect(await journal.reconcileReceipt(unsupported, { ledgerRead: true })).toBe(true);
    const retained = JSON.parse(storage.values.get(JONES_ACTION_JOURNAL_KEY)!);
    expect(retained.duties.map((d: { id: string }) => d.id)).toEqual([stopId]);
    expect(retained.history[0]).not.toHaveProperty("unsupportedReceiptVerified");
    const lostProof = createJonesActionJournal({
      ...storage,
      getItem: (key) => {
        if (key !== JONES_ACTION_JOURNAL_KEY) throw new Error("Optional proof is unavailable");
        return storage.getItem(key);
      },
    });
    await lostProof.load();
    expect(lostProof.getSnapshot().duties[0]?.id).toBe(stopId);
    expect(lostProof.getSnapshot().history[0]?.unsupportedReceiptVerified).toBeUndefined();
    expect(
      await lostProof.reconcileReceipt(
        {
          ...stoppedOperation,
          phase: "stopped",
          updatedAt: "2026-10-27T04:02:00Z",
          stop: { stopId, requestedAt: "2026-10-27T04:01:00Z", result: "stopped" },
        },
        { ledgerRead: true },
      ),
    ).toBe(true);
    expect(lostProof.getSnapshot().duties).toEqual([]);
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
