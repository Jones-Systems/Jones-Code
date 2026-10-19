import type { EnvironmentId } from "@t3tools/contracts";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import * as DateTime from "effect/DateTime";

import {
  createSettlementNotificationController,
  type SettlementNotificationStore,
} from "./controller.ts";
import {
  decodeSettlementBaseline,
  decodeSettlementInbox,
  emptyInbox,
  encodeSettlementBaseline,
  encodeSettlementInbox,
} from "./model.ts";
import { baseline, entry, env, otherEnv, shell, thread } from "./testFixtures.ts";

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function harness() {
  const data = {
    inbox: null as string | null,
    baselines: new Map<EnvironmentId, string>([[env, encodeSettlementBaseline(baseline())]]),
    writes: [] as Array<{ kind: "inbox" | "baseline"; json: string }>,
    failInbox: false,
    failBaseline: false,
  };
  let listener: ((json: string | null) => void) | undefined;
  const store = {
    loadInbox: vi.fn((): Promise<string | null> | string | null => data.inbox),
    saveInbox: vi.fn((json: string): Promise<void> | void => {
      if (data.failInbox) throw new Error("synthetic quota failure");
      data.writes.push({ kind: "inbox", json });
      data.inbox = json;
    }),
    loadBaseline: vi.fn(
      (id: EnvironmentId): Promise<string | null> | string | null => data.baselines.get(id) ?? null,
    ),
    saveBaseline: vi.fn((id: EnvironmentId, json: string): Promise<void> | void => {
      if (data.failBaseline) throw new Error("synthetic baseline failure");
      data.writes.push({ kind: "baseline", json });
      data.baselines.set(id, json);
    }),
    removeBaseline: vi.fn((id: EnvironmentId) => {
      data.baselines.delete(id);
    }),
    listBaselineEnvironmentIds: vi.fn(() => [...data.baselines.keys()]),
    subscribeExternalInbox: vi.fn((callback: (json: string | null) => void) => {
      listener = callback;
      return () => {
        listener = undefined;
      };
    }),
  } satisfies SettlementNotificationStore;
  return { data, store, external: (json: string | null) => listener?.(json) };
}

describe("settlement notification controller", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("hydrates once, queues only the latest observation and retains pending/acks received before loading", async () => {
    const h = harness();
    const held = deferred<string | null>();
    const loading = deferred<void>();
    h.store.loadInbox.mockImplementation(() => {
      loading.resolve();
      return held.promise;
    });
    const old = entry("old", 50);
    const dismissed = entry("dismissed", 60);
    const incoming = entry("external", 70, otherEnv);
    const controller = createSettlementNotificationController({ store: h.store, now: () => 1_000 });
    const hydration = controller.hydrate();
    expect(controller.hydrate()).toBe(hydration);
    await loading.promise;
    controller.observe(
      env,
      shell(2, [thread("thread-a", 200), thread("old"), thread("dismissed")]),
    );
    controller.observe(
      env,
      shell(3, [thread("thread-a", 300), thread("old"), thread("dismissed")]),
    );
    controller.dismiss([dismissed.id]);
    h.external(encodeSettlementInbox({ ...emptyInbox(), entries: [incoming] }));
    expect(controller.getSnapshot().persistence).toBe("loading");
    expect(h.store.saveBaseline).not.toHaveBeenCalled();
    held.resolve(encodeSettlementInbox({ ...emptyInbox(), entries: [old, dismissed] }));
    await hydration;
    await controller.flush();
    expect(controller.getSnapshot().visible.map((value) => value.id)).toEqual([
      entry("thread-a", 300).id,
      incoming.id,
      old.id,
    ]);
    expect(decodeSettlementInbox(h.data.inbox!).acknowledged).toContain(dismissed.id);
    expect(h.store.loadInbox).toHaveBeenCalledTimes(1);
    expect(h.store.subscribeExternalInbox).toHaveBeenCalledTimes(1);
  });

  it("gates new-environment comparisons on its async baseline load", async () => {
    const h = harness();
    h.data.baselines.clear();
    const controller = createSettlementNotificationController({ store: h.store });
    await controller.hydrate();
    const held = deferred<string | null>();
    h.store.loadBaseline.mockImplementation(() => held.promise);
    controller.observe(env, shell(2, [thread("thread-a", 200)]));
    expect(controller.getSnapshot().visible).toEqual([]);
    held.resolve(encodeSettlementBaseline(baseline()));
    await controller.flush();
    expect(controller.getSnapshot().visible.map((value) => value.id)).toEqual([entry().id]);
  });

  it("serializes coherent inbox-before-baseline writes while observations and dismissal race an async save", async () => {
    const h = harness();
    const controller = createSettlementNotificationController({ store: h.store, now: () => 1_000 });
    await controller.hydrate();
    const entered = deferred<void>();
    const release = deferred<void>();
    const saveInbox = h.store.saveInbox.getMockImplementation()!;
    h.store.saveInbox.mockImplementationOnce(async (json) => {
      entered.resolve();
      await release.promise;
      await saveInbox(json);
    });
    controller.observe(env, shell(2, [thread("thread-a", 200)]));
    await entered.promise;
    controller.observe(env, shell(3, [thread("thread-a", 300)]));
    controller.dismiss([entry().id]);
    expect(h.store.saveBaseline).not.toHaveBeenCalled();
    release.resolve();
    await controller.flush();
    expect(h.data.writes.map((write) => write.kind)).toEqual([
      "inbox",
      "baseline",
      "inbox",
      "baseline",
    ]);
    expect(decodeSettlementBaseline(h.data.writes[1]!.json).snapshotSequence).toBe(2);
    expect(decodeSettlementBaseline(h.data.writes[3]!.json).snapshotSequence).toBe(3);
    const firstInbox = decodeSettlementInbox(h.data.writes[0]!.json);
    expect(firstInbox.entries.map((value) => value.id)).toEqual([entry().id]);
    expect(controller.getSnapshot().visible.map((value) => value.id)).toEqual([
      entry("thread-a", 300).id,
    ]);
    expect(decodeSettlementInbox(h.data.inbox!).acknowledged).toContain(entry().id);
  });

  it("flush includes a baseline-only observation arriving during an async save", async () => {
    const h = harness();
    const controller = createSettlementNotificationController({ store: h.store });
    await controller.hydrate();
    const entered = deferred<void>();
    const release = deferred<void>();
    const saveBaseline = h.store.saveBaseline.getMockImplementation()!;
    h.store.saveBaseline.mockImplementationOnce(async (id, json) => {
      entered.resolve();
      await release.promise;
      await saveBaseline(id, json);
    });
    controller.observe(env, shell(2, [thread()]));
    const flushed = controller.flush();
    await entered.promise;
    controller.observe(env, shell(3, [thread()]));
    release.resolve();
    await flushed;
    expect(decodeSettlementBaseline(h.data.baselines.get(env)!).snapshotSequence).toBe(3);
    expect(h.store.saveBaseline).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(h.store.saveBaseline).toHaveBeenCalledTimes(2);
  });

  it("replays safely after a crash between the inbox and baseline saves, including dismissed events", async () => {
    const h = harness();
    h.data.failBaseline = true;
    const first = createSettlementNotificationController({ store: h.store, now: () => 1_000 });
    await first.hydrate();
    first.observe(env, shell(2, [thread("thread-a", 200)]));
    await first.flush();
    expect(decodeSettlementBaseline(h.data.baselines.get(env)!).snapshotSequence).toBe(1);
    expect(decodeSettlementInbox(h.data.inbox!).entries).toHaveLength(1);
    first.dismiss([entry().id]);
    await first.flush();
    h.data.failBaseline = false;
    const restarted = createSettlementNotificationController({ store: h.store, now: () => 2_000 });
    restarted.observe(env, shell(2, [thread("thread-a", 200)]));
    await restarted.flush();
    expect(restarted.getSnapshot().visible).toEqual([]);
    expect(decodeSettlementInbox(h.data.inbox!).entries).toHaveLength(1);
    expect(decodeSettlementInbox(h.data.inbox!).acknowledged).toEqual([entry().id]);
  });

  it("does not advance any baseline after a failed inbox save, and retries from memory", async () => {
    const h = harness();
    h.data.failInbox = true;
    const saveInbox = h.store.saveInbox.getMockImplementation()!;
    h.store.saveInbox.mockImplementation(async (json) => {
      await saveInbox(json);
    });
    const log = vi.fn();
    const controller = createSettlementNotificationController({ store: h.store, log });
    await controller.hydrate();
    controller.observe(env, shell(2, [thread("thread-a", 200)]));
    await expect(controller.flush()).resolves.toBeUndefined();
    expect(controller.getSnapshot().persistence).toBe("degraded");
    expect(controller.getSnapshot().visible).toHaveLength(1);
    expect(h.store.saveBaseline).not.toHaveBeenCalled();
    expect(log).toHaveBeenCalledWith("settlement-notifications.inbox-save-failed", undefined);
    h.data.failInbox = false;
    controller.observe(env, shell(3, [thread("thread-a", 200)]));
    await controller.flush();
    expect(controller.getSnapshot().persistence).toBe("ok");
    expect(decodeSettlementBaseline(h.data.baselines.get(env)!).snapshotSequence).toBe(3);
  });

  it.each([
    "not json",
    '{"version":2}',
    encodeSettlementBaseline(baseline({ environmentId: otherEnv })),
  ])("silently rebaselines corrupt or foreign baseline data: %s", async (bad) => {
    const h = harness();
    h.data.inbox = "broken inbox";
    h.data.baselines.set(env, bad);
    const controller = createSettlementNotificationController({ store: h.store });
    controller.observe(env, shell(2, [thread("thread-a", 200)]));
    await controller.flush();
    expect(controller.getSnapshot().visible).toEqual([]);
    expect(controller.getSnapshot().persistence).toBe("ok");
    expect(decodeSettlementBaseline(h.data.baselines.get(env)!).snapshotSequence).toBe(2);
    expect(decodeSettlementInbox(h.data.inbox!).entries).toEqual([]);
  });

  it("merges external tombstones monotonically and preserves unseen arrivals racing a click", async () => {
    const h = harness();
    const controller = createSettlementNotificationController({ store: h.store });
    await controller.hydrate();
    controller.observe(env, shell(2, [thread("thread-a", 200)]));
    const shown = controller.getSnapshot().visible.map((value) => value.id);
    h.external(encodeSettlementInbox({ ...emptyInbox(), entries: [entry("b", 300, otherEnv)] }));
    controller.dismiss(shown);
    h.external(encodeSettlementInbox({ ...emptyInbox(), entries: [entry()] }));
    await controller.flush();
    expect(controller.getSnapshot().visible.map((value) => value.threadId)).toEqual(["b"]);
    h.external(
      encodeSettlementInbox({ ...emptyInbox(), acknowledged: [entry("b", 300, otherEnv).id] }),
    );
    await controller.flush();
    expect(controller.getSnapshot().visible).toEqual([]);
    const restarted = createSettlementNotificationController({ store: h.store });
    await restarted.hydrate();
    expect(restarted.getSnapshot().visible).toEqual([]);
  });

  it("does not echo metadata-only storage changes between tabs with different shell views", async () => {
    const h = harness();
    const controller = createSettlementNotificationController({ store: h.store });
    await controller.hydrate();
    controller.observe(env, shell(2, [thread("thread-a", 200, { title: "Current title" })]));
    await controller.flush();
    const saves = h.store.saveInbox.mock.calls.length;
    h.external(
      encodeSettlementInbox({
        ...emptyInbox(),
        entries: [{ ...entry(), title: "Stale other tab" }],
      }),
    );
    await vi.advanceTimersByTimeAsync(1_000);
    expect(h.store.saveInbox).toHaveBeenCalledTimes(saves);
    expect(controller.getSnapshot().visible[0]?.title).toBe("Current title");
  });

  it("keeps snapshot references stable and subscriptions independent of baseline-only writes", async () => {
    const h = harness();
    const controller = createSettlementNotificationController({ store: h.store });
    await controller.hydrate();
    const listener = vi.fn();
    const unsubscribe = controller.subscribe(listener);
    const initial = controller.getSnapshot();
    controller.observe(env, shell(2, [thread()]));
    await controller.flush();
    expect(controller.getSnapshot()).toBe(initial);
    expect(listener).not.toHaveBeenCalled();
    controller.observe(env, shell(3, [thread("thread-a", 200)]));
    const arrival = controller.getSnapshot();
    controller.observe(env, shell(3, [thread("thread-a", 200)]));
    await controller.flush();
    expect(controller.getSnapshot()).toBe(arrival);
    expect(listener).toHaveBeenCalledTimes(1);
    unsubscribe();
    controller.dismiss([entry().id]);
    await controller.flush();
    expect(listener).toHaveBeenCalledTimes(1);
  });

  it("refreshes persisted titles and keeps gone entries and removed-environment baselines indefinitely", async () => {
    const h = harness();
    h.data.inbox = encodeSettlementInbox({ ...emptyInbox(), entries: [entry()] });
    const controller = createSettlementNotificationController({
      store: h.store,
      now: () => 100 * 86_400_000,
    });
    await controller.hydrate();
    controller.observe(env, shell(2, [thread("thread-a", 200, { title: "Renamed" })]));
    expect(controller.getSnapshot().visible[0]?.title).toBe("Renamed");
    controller.observe(
      env,
      shell(3, [thread("thread-a", 200, { deletedAt: DateTime.makeUnsafe(300) })]),
    );
    expect(controller.getSnapshot().visible[0]).toMatchObject({
      id: entry().id,
      title: "Renamed",
      threadGone: true,
    });
    controller.observe(env, shell(4, []));
    controller.noteCatalog([]);
    await controller.flush();
    expect(h.store.removeBaseline).not.toHaveBeenCalled();
    expect(h.data.baselines.has(env)).toBe(true);
    expect(controller.getSnapshot().visible).toHaveLength(1);
  });

  it("debounces baseline-only writes and flushes before the timer fires", async () => {
    const h = harness();
    const controller = createSettlementNotificationController({ store: h.store });
    await controller.hydrate();
    controller.observe(env, shell(2, [thread()]));
    await vi.advanceTimersByTimeAsync(999);
    expect(h.store.saveBaseline).not.toHaveBeenCalled();
    controller.observe(env, shell(3, [thread()]));
    await controller.flush();
    expect(h.store.saveBaseline).toHaveBeenCalledTimes(1);
    expect(decodeSettlementBaseline(h.data.baselines.get(env)!).snapshotSequence).toBe(3);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(h.store.saveBaseline).toHaveBeenCalledTimes(1);
  });
});
