// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off -- Synthetic Node process, pipe and clock fixtures exercise the actual ownership boundary.
import { afterEach, expect, it, vi } from "vite-plus/test";
import { ComputeSupervisor, type JournalPort } from "./supervisor.ts";
import type { Bootstrap, Request, ScanStatus } from "./protocol.ts";
import type { ComputeChild, SlotResult } from "./processSlot.ts";
import { computeFailure } from "./failure.ts";

const pools: ComputeSupervisor[] = [];
afterEach(async () => { vi.useRealTimers(); for (const pool of pools.splice(0)) await pool.shutdown().catch(() => {}); });
const ready: ScanStatus = { scanning: false, warmupComplete: true };
function fixture(count: 2 | 3 = 2) {
  const children: Array<ComputeChild & { requests: Request[]; crash: () => void }> = [];
  const physical = new Map<number, Map<string, Request["token"]>>();
  const retired: number[] = [];
  const gates = new Map<string, Promise<void>>();
  let scan: ScanStatus = ready;
  const journal: JournalPort = { claim: { authorityId: "authority" }, reserve: async () => {}, registered: async () => {},
    retired: async (id) => { retired.push(id); }, release: async () => {} };
  const pool = new ComputeSupervisor({ count, build: "a".repeat(64), journal,
    start: async (bootstrap: Bootstrap, register, onExit) => {
      await register(100 + children.length);
      const entries = new Map<string, Request["token"]>(); physical.set(bootstrap.slotId, entries);
      const child = { pid: 100 + children.length, bootstrap, requests: [] as Request[], crash: () => onExit(computeFailure("worker-exit")),
        retire: async () => { entries.clear(); },
        request: async (request: Request): Promise<SlotResult> => {
          child.requests.push(request);
          if (request.operation === "open") {
            expect(entries.size).toBeLessThan(4); expect(entries.has(request.token.keyId)).toBe(false);
            entries.set(request.token.keyId, request.token);
          } else expect(entries.get(request.token.keyId)).toEqual(request.token);
          if (request.operation === "list") await gates.get(request.token.keyId);
          if (request.operation === "close") entries.delete(request.token.keyId);
          return { items: request.operation === "list" ? [{ path: `slot-${bootstrap.slotId}`, kind: "file" }] : [],
            final: { type: "done", ...(request.operation === "close" ? {} : { status: scan }),
              ...(["list", "search", "searchContents"].includes(request.operation) ? { truncated: false } : {}) } };
        } } satisfies ComputeChild;
      children.push(child); return child;
    } });
  pools.push(pool);
  const acquire = async (id: number) => { const h = pool.register({ cwd: `/synthetic/${id}`, variant: "paths" }); await pool.ensure(h); return h; };
  return { pool, children, acquire, gates, physical, retired, setScan: (value: ScanStatus) => { scan = value; } };
}

it.each([2, 3] as const)("dispatches operations to all %s distinct slots and skips busy keys", async (count) => {
  const f = fixture(count), handles = await Promise.all(Array.from({ length: count }, (_, id) => f.acquire(id)));
  const releases: Array<() => void> = [];
  for (const child of f.children) {
    const open = child.requests.find((r) => r.operation === "open")!;
    f.gates.set(open.token.keyId, new Promise<void>((resolve) => { releases.push(resolve); }));
  }
  const running = handles.map((h) => f.pool.run(h, "list", undefined));
  await new Promise<void>((resolve) => setImmediate(resolve));
  expect(f.children).toHaveLength(count);
  expect(f.children.every((c) => c.requests.some((r) => r.operation === "list"))).toBe(true);
  releases.forEach((release) => release());
  const results = await Promise.all(running);
  expect(new Set(results.map((r) => r.entries[0]!.path)).size).toBe(count);
});

it("rotates idle cohorts without invalidating a cached logical facade", async () => {
  const f = fixture(), handles = [];
  for (let id = 0; id < 9; id++) handles.push(await f.acquire(id));
  expect([...f.physical.values()].every((entries) => entries.size <= 4)).toBe(true);
  const old = f.children.flatMap((c) => c.requests).find((r) => r.operation === "open")!;
  await f.pool.run(handles[0]!, "list", undefined);
  const reopened = f.children.flatMap((c) => c.requests).filter((r) => r.operation === "open" && r.token.keyId === old.token.keyId);
  expect(reopened).toHaveLength(2);
  expect(reopened[1]!.token.incarnation).toBeGreaterThan(reopened[0]!.token.incarnation);
  f.pool.release(handles[0]!.handleId); f.pool.release(handles[0]!.handleId);
  await expect(f.pool.run(handles[0]!, "list", undefined)).rejects.toMatchObject({ reason: "unavailable" });
});

it("protects a running key and its queued jobs from eviction, and bounds per-key admission", async () => {
  const f = fixture(), handles = [];
  for (let id = 0; id < 8; id++) handles.push(await f.acquire(id));
  const token = f.children[0]!.requests.find((r) => r.operation === "open")!.token;
  let unblock!: () => void;
  f.gates.set(token.keyId, new Promise<void>((resolve) => { unblock = resolve; }));
  const running = f.pool.run(handles[0]!, "list", undefined);
  const queued = Array.from({ length: 8 }, () => f.pool.run(handles[0]!, "list", undefined));
  await expect(f.pool.run(handles[0]!, "list", undefined)).rejects.toMatchObject({ reason: "queue-full" });
  await f.acquire(9);
  expect(f.children[0]!.requests.some((r) => r.operation === "close" && r.token.keyId === token.keyId)).toBe(false);
  unblock(); await Promise.all([running, ...queued]);
});

it("fails already bound queued work on crash without replay, while fresh cached calls reopen after proof", async () => {
  const f = fixture(), handle = await f.acquire(1);
  const child = f.children[0]!, token = child.requests[0]!.token;
  let unblock!: () => void;
  f.gates.set(token.keyId, new Promise<void>((resolve) => { unblock = resolve; }));
  const running = f.pool.run(handle, "list", undefined);
  const queued = f.pool.run(handle, "list", undefined);
  const rejected = expect(queued).rejects.toMatchObject({ reason: "worker-exit" });
  child.crash(); unblock();
  await rejected; await expect(running).rejects.toMatchObject({ reason: "worker-exit" });
  // A fresh call may choose another proved slot immediately; no interrupted request is resent.
  f.gates.delete(token.keyId);
  await f.pool.run(handle, "list", undefined);
  expect(child.requests.filter((r) => r.operation === "list")).toHaveLength(1);
  expect(f.children.flatMap((c) => c.requests).filter((r) => r.operation === "list")).toHaveLength(2);
});

it("uses spare allocations after partial readiness without interpreting it as native completion", async () => {
  const f = fixture(); f.setScan({ scanning: true, warmupComplete: false });
  const first = await f.acquire(1);
  for (let id = 2; id <= 8; id++) await f.acquire(id);
  await f.pool.run(first, "refresh", undefined);
  expect(f.children).toHaveLength(2);
  expect(f.children.every((child) => child.requests.filter((r) => r.operation === "open").length === 4)).toBe(true);
});

it("limits logical handles and allows idempotent release", () => {
  const f = fixture();
  const handles = Array.from({ length: 256 }, (_, id) => f.pool.register({ cwd: `/synthetic/${id}`, variant: "paths" }));
  expect(() => f.pool.register({ cwd: "/overflow", variant: "content" })).toThrow();
  f.pool.release(handles[0]!.handleId); f.pool.release(handles[0]!.handleId);
  expect(() => f.pool.register({ cwd: "/after-release", variant: "content" })).not.toThrow();
});

it("removes exactly one queued cancellation without releasing the running key", async () => {
  const f = fixture(), handle = await f.acquire(1);
  const token = f.children[0]!.requests[0]!.token;
  let unblock!: () => void;
  f.gates.set(token.keyId, new Promise<void>((resolve) => { unblock = resolve; }));
  const running = f.pool.run(handle, "list", undefined);
  const controller = new AbortController();
  const cancelled = f.pool.run(handle, "search", { query: "cancelled", limit: 1 }, controller.signal);
  controller.abort(); await expect(cancelled).rejects.toMatchObject({ reason: "cancelled" });
  expect(f.children[0]!.requests.filter((r) => r.operation === "search")).toHaveLength(0);
  const fresh = f.pool.run(handle, "search", { query: "fresh", limit: 1 });
  unblock(); await running; await fresh;
  expect(f.children[0]!.requests.filter((r) => r.operation === "search")).toHaveLength(1);
});

it("expires queued work at its original deadline and never replays it when the key becomes free", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "performance"] });
  const f = fixture(), handle = await f.acquire(1), token = f.children[0]!.requests[0]!.token;
  let unblock!: () => void;
  f.gates.set(token.keyId, new Promise<void>((resolve) => { unblock = resolve; }));
  const running = f.pool.run(handle, "list", undefined);
  const expired = expect(f.pool.run(handle, "search", { query: "expire", limit: 1 })).rejects.toMatchObject({ reason: "queue-timeout" });
  await vi.advanceTimersByTimeAsync(10_001); await expired;
  unblock(); await running;
  expect(f.children[0]!.requests.filter((r) => r.operation === "search")).toHaveLength(0);
});

it("keeps all pool admission closed after an uncertain durable reservation", async () => {
  let starts = 0, released = false;
  const pool = new ComputeSupervisor({ count: 2, build: "a".repeat(64), journal: { claim: { authorityId: "authority" },
    reserve: async () => { throw new Error("fixture disk uncertainty"); }, registered: async () => {}, retired: async () => {},
    release: async () => { released = true; } }, start: async () => { starts++; throw new Error("must not spawn"); } });
  pools.push(pool);
  const handle = pool.register({ cwd: "/synthetic", variant: "paths" });
  await expect(pool.ensure(handle)).rejects.toMatchObject({ reason: "retirement-unknown" });
  expect(() => pool.register({ cwd: "/second", variant: "paths" })).toThrow();
  await expect(pool.shutdown()).rejects.toMatchObject({ reason: "retirement-unknown" });
  expect(starts).toBe(0); expect(released).toBe(false);
});

it("coalesces queued refreshes of one residency while preserving each caller's cancellation", async () => {
  const f = fixture(), handle = await f.acquire(1), token = f.children[0]!.requests[0]!.token;
  let unblock!: () => void;
  f.gates.set(token.keyId, new Promise<void>((resolve) => { unblock = resolve; }));
  const list = f.pool.run(handle, "list", undefined);
  const abort = new AbortController();
  const cancelled = f.pool.run(handle, "refresh", undefined, abort.signal);
  const first = f.pool.run(handle, "refresh", undefined), second = f.pool.run(handle, "refresh", undefined);
  abort.abort(); await expect(cancelled).rejects.toMatchObject({ reason: "cancelled" });
  unblock(); await Promise.all([list, first, second]);
  expect(f.children[0]!.requests.filter((r) => r.operation === "refresh")).toHaveLength(1);
});

it("bounds the global queue across independent keys while all execution slots are occupied", async () => {
  const f = fixture(), handles = await Promise.all([f.acquire(1), f.acquire(2)]);
  const releases: Array<() => void> = [];
  for (const child of f.children) {
    const token = child.requests[0]!.token;
    f.gates.set(token.keyId, new Promise<void>((resolve) => { releases.push(resolve); }));
  }
  const running = handles.map((h) => f.pool.run(h, "list", undefined));
  const controllers = Array.from({ length: 64 }, () => new AbortController());
  const queued = controllers.map((controller, index) => f.pool.ensure(f.pool.register({ cwd: `/queued/${index}`, variant: "paths" }), controller.signal));
  await expect(f.pool.ensure(f.pool.register({ cwd: "/queued/overflow", variant: "paths" }))).rejects.toMatchObject({ reason: "queue-full" });
  // Attach rejection observers before cancellation, and ensure no cancelled request ever reaches a child.
  const settled = Promise.allSettled(queued); controllers.forEach((controller) => controller.abort());
  expect((await settled).every((result) => result.status === "rejected")).toBe(true);
  releases.forEach((release) => release()); await Promise.all(running);
  expect(f.children.flatMap((child) => child.requests).filter((request) => request.operation === "open")).toHaveLength(2);
});
