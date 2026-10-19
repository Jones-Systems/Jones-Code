import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { SettlementEntry } from "@t3tools/client-runtime/jones/settlement-notifications";
import { createLocalStorageSettlementStore, SETTLEMENT_STORAGE_PREFIX } from "./localStorageStore";

class MemoryStorage implements Storage {
  private values = new Map<string, string>();
  get length() {
    return this.values.size;
  }
  clear() {
    this.values.clear();
  }
  getItem(key: string) {
    return this.values.get(key) ?? null;
  }
  key(index: number) {
    return [...this.values.keys()][index] ?? null;
  }
  removeItem(key: string) {
    this.values.delete(key);
  }
  setItem(key: string, value: string) {
    this.values.set(key, value);
  }
}
const events = new EventTarget() as unknown as Window;
const entry = (id: string): SettlementEntry => ({
  id: `env:${id}:1`,
  environmentId: EnvironmentId.make("env"),
  threadId: ThreadId.make(id),
  title: id,
  settledAtMs: 1,
  detectedAtMs: 2,
  threadGone: false,
});
const inbox = (entries: SettlementEntry[], acknowledged: string[] = []) =>
  JSON.stringify({ version: 1, entries, acknowledged });

describe("settlement localStorage persistence", () => {
  it("preserves another tab's addition and acknowledgement across stale saves and reopen", async () => {
    const storage = new MemoryStorage();
    const first = createLocalStorageSettlementStore(() => storage, events);
    const second = createLocalStorageSettlementStore(() => storage, events);
    await first.saveInbox(inbox([entry("a")]));
    const stale = await second.loadInbox();
    await first.saveInbox(inbox([entry("a"), entry("b")], [entry("a").id]));
    await second.saveInbox(stale!);
    await second.saveInbox(inbox([entry("a"), entry("c")]));
    const reopened = createLocalStorageSettlementStore(() => storage, events);
    const saved = JSON.parse((await reopened.loadInbox())!);
    expect(saved.acknowledged).toEqual([entry("a").id]);
    expect(saved.entries.map((row: SettlementEntry) => row.threadId).sort()).toEqual([
      "a",
      "b",
      "c",
    ]);
    expect(
      saved.entries
        .filter((row: SettlementEntry) => !saved.acknowledged.includes(row.id))
        .map((row: SettlementEntry) => row.threadId)
        .sort(),
    ).toEqual(["b", "c"]);
  });

  it("survives interleaving within saveInbox without overwriting unrelated records", async () => {
    const storage = new MemoryStorage();
    const a = createLocalStorageSettlementStore(() => storage, events);
    const b = createLocalStorageSettlementStore(() => storage, events);
    const set = storage.setItem.bind(storage);
    let interleave = true;
    storage.setItem = (key, value) => {
      if (interleave) {
        interleave = false;
        b.saveInbox(inbox([entry("b")], [entry("a").id]));
      }
      set(key, value);
    };
    await a.saveInbox(inbox([entry("a")]));
    const saved = JSON.parse((await b.loadInbox())!);
    expect(saved.entries).toHaveLength(2);
    expect(saved.acknowledged).toEqual([entry("a").id]);
  });

  it("ignores corrupt and future-version records without discarding valid records", async () => {
    const storage = new MemoryStorage();
    const store = createLocalStorageSettlementStore(() => storage, events);
    storage.setItem(`${SETTLEMENT_STORAGE_PREFIX}entry.bad`, "{");
    storage.setItem(
      `${SETTLEMENT_STORAGE_PREFIX}entry.future`,
      JSON.stringify({ version: 2, entry: entry("future") }),
    );
    storage.setItem(`${SETTLEMENT_STORAGE_PREFIX}inbox`, "{broken");
    await store.saveInbox(inbox([entry("valid")]));
    expect(JSON.parse((await store.loadInbox())!).entries).toEqual([entry("valid")]);
  });

  it("reports unavailable storage to the controller instead of claiming a successful save", () => {
    const store = createLocalStorageSettlementStore(() => {
      throw new Error("blocked");
    }, events);
    expect(() => store.loadInbox()).toThrow("blocked");
    expect(() => store.saveInbox(inbox([entry("a")]))).toThrow("blocked");
  });

  it("does not evict pending entries or dismissal tombstones", async () => {
    const storage = new MemoryStorage();
    const store = createLocalStorageSettlementStore(() => storage, events);
    const entries = Array.from({ length: 2100 }, (_, index) => entry(String(index)));
    await store.saveInbox(
      inbox(
        entries,
        entries.map((row) => row.id),
      ),
    );
    await store.saveInbox(inbox([]));
    const saved = JSON.parse((await store.loadInbox())!);
    expect(saved.entries).toHaveLength(2100);
    expect(saved.acknowledged).toHaveLength(2100);
  });
});
