import { describe, expect, it } from "@effect/vitest";

import { addArrivals, dismiss, mergeInbox, refreshThreads, visibleEntries } from "./inbox.ts";
import { decodeSettlementInbox, emptyInbox, encodeSettlementInbox } from "./model.ts";
import { entry, env, otherEnv } from "./testFixtures.ts";

const withEntries = (...entries: ReturnType<typeof entry>[]) => ({ ...emptyInbox(), entries });

describe("settlement inbox", () => {
  it("merges commutatively, associatively and idempotently with deterministic metadata conflicts", () => {
    const a = withEntries(entry());
    const b = dismiss(withEntries({ ...entry(), title: "Renamed", threadGone: true }, entry("b")), [
      entry("b").id,
    ]);
    const c = withEntries(entry("c"), { ...entry(), detectedAtMs: 999 });
    expect(mergeInbox(a, b)).toEqual(mergeInbox(b, a));
    expect(mergeInbox(mergeInbox(a, b), c)).toEqual(mergeInbox(a, mergeInbox(b, c)));
    expect(mergeInbox(mergeInbox(a, b), mergeInbox(a, b))).toEqual(mergeInbox(a, b));
  });

  it("dismisses exactly the shown event IDs while preserving racing arrivals", () => {
    const first = entry();
    const newer = entry("thread-a", 300);
    const another = entry("b", 400);
    const merged = mergeInbox(withEntries(first), withEntries(newer, another));
    expect(visibleEntries(dismiss(merged, [first.id]))).toEqual([another, newer]);
    expect(
      visibleEntries(mergeInbox(dismiss(withEntries(first), [first.id]), withEntries(first))),
    ).toEqual([]);
  });

  it("selects only the latest per environment/thread and never resurrects superseded events", () => {
    const first = entry();
    const newer = entry("thread-a", 300);
    const elsewhere = entry("thread-a", 250, otherEnv);
    const inbox = withEntries(first, newer, elsewhere);
    expect(visibleEntries(inbox)).toEqual([newer, elsewhere]);
    expect(visibleEntries(dismiss(inbox, [newer.id]))).toEqual([elsewhere]);
    expect(
      visibleEntries(addArrivals(dismiss(inbox, [newer.id]), [entry("thread-a", 400)], 500))[0]
        ?.settledAtMs,
    ).toBe(400);
  });

  it("refreshes titles, preserves identities and retains missing thread labels", () => {
    const first = entry();
    const gone = entry("gone");
    const other = entry("elsewhere", 200, otherEnv);
    const refreshed = refreshThreads(
      withEntries(first, gone, other),
      env,
      new Map([[first.threadId, "New title"]]),
      new Set(),
    );
    expect(refreshed.entries).toEqual([
      { ...first, title: "New title" },
      { ...gone, threadGone: true },
      other,
    ]);
    expect(
      refreshThreads(refreshed, env, new Map([[first.threadId, "New title"]]), new Set()),
    ).toBe(refreshed);
  });

  it("never evicts pending titles or acknowledgement tombstones by age/count", () => {
    const entries = Array.from({ length: 2_101 }, (_, index) => entry(`thread-${index}`, index));
    const inbox = withEntries(...entries);
    expect(visibleEntries(inbox)).toHaveLength(2_101);
    const dismissed = dismiss(
      inbox,
      entries.map((value) => value.id),
    );
    const roundtrip = decodeSettlementInbox(encodeSettlementInbox(dismissed));
    expect(roundtrip.acknowledged).toHaveLength(2_101);
    expect(roundtrip.entries).toHaveLength(2_101);
    expect(visibleEntries(mergeInbox(roundtrip, inbox))).toEqual([]);
  });

  it("rejects unknown versions, invalid identities and non-finite epochs", () => {
    expect(() => decodeSettlementInbox('{"version":2,"entries":[],"acknowledged":[]}')).toThrow();
    expect(() =>
      decodeSettlementInbox(JSON.stringify(withEntries({ ...entry(), id: "wrong" }))),
    ).toThrow();
    expect(() =>
      decodeSettlementInbox(
        JSON.stringify(withEntries({ ...entry(), settledAtMs: null as unknown as number })),
      ),
    ).toThrow();
  });
});
