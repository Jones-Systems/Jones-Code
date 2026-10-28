import { describe, expect, it } from "vite-plus/test";
import { CoherentSnapshots } from "./coherentSnapshot.ts";
const snapshot = { fingerprint: "complete-authoritative-state", content: { private: "synthetic" } };
const start = (
  cache: CoherentSnapshots<typeof snapshot>,
  partition = "owner/principal/home",
  query = "route/filters/limit",
) => cache.next(snapshot, partition, query, "original-inner-keyset-with-high-water", 0)!;
const resume = (cache: CoherentSnapshots<typeof snapshot>, token: string, now = 1) =>
  cache.resume(token, "owner/principal/home", "route/filters/limit", snapshot.fingerprint, now);

describe("bounded process-local coherent continuations", () => {
  it("retains an isolated original snapshot and idempotently reuses handles for retries", () => {
    const cache = new CoherentSnapshots<typeof snapshot>({ tokens: 2 });
    const value = structuredClone(snapshot);
    const token = cache.next(value, "owner/principal/home", "route/filters/limit", "next", 0)!;
    value.content.private = "changed caller object";
    const saved = resume(cache, token);
    expect(saved.lease.snapshot.content.private).toBe("synthetic");
    expect(
      cache.next(snapshot, "owner/principal/home", "route/filters/limit", "after", 1, saved),
    ).toMatch(/^[A-Za-z0-9_-]{43}$/);
    const second = cache.next(
      snapshot,
      "owner/principal/home",
      "route/filters/limit",
      "after",
      2,
      saved,
    );
    expect(
      cache.next(snapshot, "owner/principal/home", "route/filters/limit", "after", 3, saved),
    ).toBe(second);
    expect(
      cache.next(snapshot, "owner/principal/home", "route/filters/limit", null, 4, saved),
    ).toBeNull();
    expect(() =>
      cache.next(snapshot, "owner/principal/home", "route/filters/limit", "overflow", 5, saved),
    ).toThrow();
  });
  it("expires without sliding renewal and honestly loses handles on reconstruction", () => {
    const cache = new CoherentSnapshots<typeof snapshot>();
    const token = start(cache);
    const prior = resume(cache, token, 899_999);
    expect(prior.inner).toContain("high-water");
    expect(() =>
      cache.next(snapshot, "owner/principal/home", "route/filters/limit", null, 900_000, prior),
    ).toThrow();
    expect(() => resume(cache, token, 900_000)).toThrow();
    expect(() => resume(new CoherentSnapshots(), token)).toThrow();
  });
  it.each(["other-owner", "other-principal", "other-home", "other-generation"])(
    "cannot replay across %s partitions",
    (partition) => {
      const cache = new CoherentSnapshots<typeof snapshot>();
      const token = start(cache);
      expect(() =>
        cache.resume(token, partition, "route/filters/limit", snapshot.fingerprint, 1),
      ).toThrow();
      expect(resume(cache, token).lease.snapshot).toEqual(snapshot);
    },
  );
  it.each(["route", "filters", "limit", "inventory", "contract"])(
    "binds exact %s selection",
    (selection) => {
      const cache = new CoherentSnapshots<typeof snapshot>();
      const token = start(cache);
      expect(() =>
        cache.resume(token, "owner/principal/home", selection, snapshot.fingerprint, 1),
      ).toThrow();
    },
  );
  it("purges changed content only in the authenticated partition", () => {
    const cache = new CoherentSnapshots<typeof snapshot>();
    const token = start(cache);
    const other = start(cache, "other");
    expect(() =>
      cache.resume(token, "owner/principal/home", "route/filters/limit", "changed", 1),
    ).toThrow();
    expect(() => resume(cache, token)).toThrow();
    expect(
      cache.resume(other, "other", "route/filters/limit", snapshot.fingerprint, 1),
    ).toBeDefined();
    cache.purge("other");
    expect(() =>
      cache.resume(other, "other", "route/filters/limit", snapshot.fingerprint, 1),
    ).toThrow();
  });
  it("evicts oldest same-partition entries, without letting another caller evict protected content", () => {
    const cache = new CoherentSnapshots<typeof snapshot>({ entries: 1 });
    const first = start(cache);
    const next = start(cache);
    expect(() => resume(cache, first)).toThrow();
    expect(resume(cache, next)).toBeDefined();
    expect(() => start(cache, "other")).toThrow();
    expect(resume(cache, next)).toBeDefined();
  });
  it("bounds bytes, token material and configuration, rejecting malformed and unknown handles", () => {
    expect(() => start(new CoherentSnapshots({ bytes: 1 }))).toThrow();
    const cache = new CoherentSnapshots<typeof snapshot>();
    expect(() => cache.next(snapshot, "scope", "query", "a".repeat(2049), 0)).toThrow();
    for (const token of ["", "unknown", "a".repeat(43), "a".repeat(2049), "!"])
      expect(() => resume(cache, token)).toThrow();
    for (const limits of [
      { ttlMs: 900_001 },
      { tokens: 1001 },
      { entries: 17 },
      { bytes: 16 * 1024 * 1024 + 1 },
      { tokens: 0 },
    ])
      expect(() => new CoherentSnapshots(limits)).toThrow();
  });
});
