import type { MobileThreadOrderSnapshot, MobileThreadOrderSource } from "../lib/threadOrderScope";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { Atom } from "effect/unstable/reactivity";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { createPendingThreadOrder } from "../features/threads/threadOrder";
import { makeThreadShellFixture } from "../test-fixtures";
import { appAtomRegistry } from "./atom-registry";
import {
  beginPendingThreadOrder,
  getPendingThreadOrder,
  pendingThreadOrderAtom,
} from "./thread-order";
import { environmentThreadShells } from "./threads";

vi.mock("./atom-registry", async () => {
  const { AtomRegistry } = await import("effect/unstable/reactivity");
  return { appAtomRegistry: AtomRegistry.make() };
});
vi.mock("./threads", async () => {
  const { Atom } = await import("effect/unstable/reactivity");
  return { environmentThreadShells: { threadShellsAtom: Atom.make([]).pipe(Atom.keepAlive) } };
});
vi.mock("./server", async () => {
  const { Atom } = await import("effect/unstable/reactivity");
  return { environmentServerConfigsAtom: Atom.make(new Map()).pipe(Atom.keepAlive) };
});
vi.mock("./use-thread-outbox", async () => {
  const { Atom } = await import("effect/unstable/reactivity");
  return { queuedThreadKeysAtom: Atom.make(new Set<string>()).pipe(Atom.keepAlive) };
});

// The mocked shell source is writable so tests can deliver canonical upserts.
const shellsAtom = environmentThreadShells.threadShellsAtom as Atom.Writable<
  readonly EnvironmentThreadShell[],
  readonly EnvironmentThreadShell[]
>;

function fixture() {
  // The shared section helper also reads lineage/settled/snooze fields, so
  // partial casts break when it grows — build complete shells instead.
  const rows = ["a", "b"].map((id, index) =>
    makeThreadShellFixture({
      id: ThreadId.make(id),
      environmentId: EnvironmentId.make("env"),
      createdAt: `2026-06-01T0${2 - index}:00:00.000Z`,
    }),
  );
  appAtomRegistry.set(shellsAtom, rows);
  const pending = createPendingThreadOrder({
    section: "active",
    ordered: rows,
    movedId: "env:b",
    direction: "up",
    assignments: [
      { id: "env:b", orderKey: "aa" },
      { id: "env:a", orderKey: "bb" },
    ],
  });
  const start = () => beginPendingThreadOrder(pending);
  const upsert = (id: string, key: string) => {
    const current = appAtomRegistry.get(shellsAtom);
    appAtomRegistry.set(
      shellsAtom,
      current.map((row) => (row.id === id ? { ...row, activeOrderKey: key } : row)),
    );
  };
  return { rows, start, upsert };
}

afterEach(() => appAtomRegistry.reset());

describe("shared mobile pending move", () => {
  it("blocks another pickup after receipts and clears on final canonical upsert", () => {
    const { start, upsert } = fixture();
    const move = start();
    move.complete();
    expect(getPendingThreadOrder()).not.toBeNull();
    upsert("b", "aa");
    expect(getPendingThreadOrder()).not.toBeNull();
    upsert("a", "bb");
    expect(getPendingThreadOrder()).toBeNull();
    expect(move.isPending()).toBe(false);
  });

  it("waits for receipts when shells arrive first", () => {
    const { start, upsert } = fixture();
    const move = start();
    upsert("b", "aa");
    upsert("a", "bb");
    expect(getPendingThreadOrder()).not.toBeNull();
    move.complete();
    expect(getPendingThreadOrder()).toBeNull();
  });

  it.each(["failure", "interruption"])("releases a %s without restoring old canonical keys", () => {
    const { start, upsert } = fixture();
    const move = start();
    upsert("b", "aa");
    move.cancel();
    expect(getPendingThreadOrder()).toBeNull();
    expect(appAtomRegistry.get(shellsAtom)[1]?.activeOrderKey).toBe("aa");
    const next = start();
    move.cancel();
    expect(next.isPending()).toBe(true);
    next.cancel();
  });

  it("stops remaining writes when a canonical membership change invalidates the move", () => {
    const { rows, start } = fixture();
    const move = start();
    appAtomRegistry.set(shellsAtom, rows.slice(1));
    expect(move.isPending()).toBe(false);
    expect(appAtomRegistry.get(pendingThreadOrderAtom)).toBeNull();
    move.complete();
    appAtomRegistry.set(shellsAtom, rows);
    expect(getPendingThreadOrder()).toBeNull();
  });
});

describe("workstream pending source", () => {
  function grouped() {
    const { rows, upsert } = fixture();
    const members = rows.map((row, index) => ({
      ...row,
      pinnedAt: index === 0 ? "2026-05-01T00:00:00Z" : null,
      pinOrderKey: index === 0 ? "zz" : null,
    }));
    appAtomRegistry.set(shellsAtom, members);
    let snapshot: MobileThreadOrderSnapshot | null = {
      enabled: true,
      revision: "ready",
      primaryGroupByThreadKey: new Map(
        members.map((row) => [JSON.stringify([row.environmentId, row.id]), "delivery"]),
      ),
    };
    const listeners = new Set<() => void>();
    const source: MobileThreadOrderSource = {
      read: () => snapshot,
      subscribe: (listener) => {
        listeners.add(listener);
        return () => {
          listeners.delete(listener);
        };
      },
    };
    const pending = createPendingThreadOrder({
      section: "active",
      scope: { kind: "workstream", groupKey: "delivery" },
      sourceRevision: "ready",
      ordered: members,
      movedId: "env:b",
      direction: "up",
      assignments: [
        { id: "env:b", orderKey: "aa" },
        { id: "env:a", orderKey: "bb" },
      ],
    });
    return {
      members,
      upsert,
      source,
      listeners,
      pending,
      invalidate: (unmounted: boolean) => {
        snapshot = unmounted ? null : { ...snapshot!, revision: "changed" };
        for (const listener of listeners) listener();
      },
    };
  }
  it("keeps grouped pins in the full active population through partial upserts", () => {
    const { members, source, listeners, pending, upsert } = grouped();
    const move = beginPendingThreadOrder(pending, source);
    expect(move.isPending()).toBe(true);
    appAtomRegistry.set(
      shellsAtom,
      members.map((row) => ({ ...row, pinnedAt: "2026-06-01T09:00:00Z", pinOrderKey: "xx" })),
    );
    expect(move.isPending()).toBe(true);
    move.complete();
    upsert("b", "aa");
    expect(move.isPending()).toBe(true);
    upsert("a", "bb");
    expect(move.isPending()).toBe(false);
    expect(listeners.size).toBe(0);
  });
  it.each([false, true])(
    "cancels and unsubscribes before another write on source invalidation (unmount=%s)",
    (unmounted) => {
      const { source, listeners, pending, invalidate } = grouped();
      const move = beginPendingThreadOrder(pending, source);
      invalidate(unmounted);
      expect(move.isPending()).toBe(false);
      expect(getPendingThreadOrder()).toBeNull();
      expect(listeners.size).toBe(0);
    },
  );
  it("unsubscribes when a failed or replaced operation cancels its hold", () => {
    const { source, listeners, pending } = grouped();
    const first = beginPendingThreadOrder(pending, source);
    const second = beginPendingThreadOrder(pending, source);
    expect(first.isPending()).toBe(false);
    expect(listeners.size).toBe(1);
    first.cancel();
    expect(second.isPending()).toBe(true);
    second.cancel();
    expect(listeners.size).toBe(0);
  });
});
