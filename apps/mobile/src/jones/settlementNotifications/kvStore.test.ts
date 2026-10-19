import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { createSettlementKvStore } from "./kvStore";

function storageFixture() {
  const values = new Map<string, string>();
  let failure: Error | undefined;
  const check = () => {
    if (failure) throw failure;
  };
  const storage = {
    async getItem(key: string) {
      check();
      return values.get(key) ?? null;
    },
    async setItem(key: string, value: string) {
      check();
      values.set(key, value);
    },
    async removeItem(key: string) {
      check();
      values.delete(key);
    },
    async getAllKeys() {
      check();
      return [...values.keys()];
    },
  };
  return {
    values,
    storage,
    fail: (error?: Error) => {
      failure = error;
    },
  };
}

const environmentA = EnvironmentId.make("environment:a");
const environmentB = EnvironmentId.make("environment:b/日本語");

describe("mobile settlement SQLite adapter", () => {
  it("reports an absent inbox and baseline without inventing stored state", async () => {
    const store = createSettlementKvStore(storageFixture().storage);
    expect(await store.loadInbox()).toBeNull();
    expect(await store.loadBaseline(environmentA)).toBeNull();
    expect(await store.listBaselineEnvironmentIds()).toEqual([]);
  });

  it("retains exact inbox and separate environment baselines across adapter recreation", async () => {
    const fixture = storageFixture();
    const first = createSettlementKvStore(fixture.storage);
    const inbox = JSON.stringify({ version: 1, entries: [], acknowledged: ["a:1", "b:2"] });
    await first.saveInbox(inbox);
    await first.saveBaseline(environmentA, '{"snapshotSequence":10}');
    await first.saveBaseline(environmentB, '{"snapshotSequence":20}');

    const restarted = createSettlementKvStore(fixture.storage);
    expect(await restarted.loadInbox()).toBe(inbox);
    expect(await restarted.loadBaseline(environmentA)).toBe('{"snapshotSequence":10}');
    expect(await restarted.loadBaseline(environmentB)).toBe('{"snapshotSequence":20}');
    expect(await restarted.listBaselineEnvironmentIds()).toEqual([environmentA, environmentB]);
  });

  it("removes only the requested baseline and preserves pending and acknowledged data", async () => {
    const fixture = storageFixture();
    const store = createSettlementKvStore(fixture.storage);
    await store.saveInbox("pending-and-acknowledged");
    await store.saveBaseline(environmentA, "a");
    await store.saveBaseline(environmentB, "b");
    fixture.values.set("another-feature", "untouched");
    await store.removeBaseline(environmentA);
    expect(await store.loadBaseline(environmentA)).toBeNull();
    expect(await store.loadBaseline(environmentB)).toBe("b");
    expect(await store.loadInbox()).toBe("pending-and-acknowledged");
    expect(await store.listBaselineEnvironmentIds()).toEqual([environmentB]);
    expect(fixture.values.get("another-feature")).toBe("untouched");
  });

  it("passes corrupt JSON through for the shared controller to decode and recover", async () => {
    const store = createSettlementKvStore(storageFixture().storage);
    await store.saveInbox("{corrupt");
    await store.saveBaseline(environmentA, '{"version":999}');
    expect(await store.loadInbox()).toBe("{corrupt");
    expect(await store.loadBaseline(environmentA)).toBe('{"version":999}');
  });

  it("rejects SQLite failures so the controller can report degraded persistence and retry", async () => {
    const fixture = storageFixture();
    const store = createSettlementKvStore(fixture.storage);
    await store.saveInbox("original");
    await store.saveBaseline(environmentA, "original-baseline");
    const error = new Error("SQLite unavailable");
    fixture.fail(error);
    await expect(store.loadInbox()).rejects.toBe(error);
    await expect(store.loadBaseline(environmentA)).rejects.toBe(error);
    await expect(store.saveInbox("replacement")).rejects.toBe(error);
    await expect(store.saveBaseline(environmentA, "replacement")).rejects.toBe(error);
    await expect(store.removeBaseline(environmentA)).rejects.toBe(error);
    await expect(store.listBaselineEnvironmentIds()).rejects.toBe(error);
    fixture.fail();
    expect(await store.loadInbox()).toBe("original");
    expect(await store.loadBaseline(environmentA)).toBe("original-baseline");
    await store.saveInbox("replacement");
    expect(await store.loadInbox()).toBe("replacement");
  });
});
