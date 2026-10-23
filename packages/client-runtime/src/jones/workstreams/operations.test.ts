import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import {
  WORKSTREAM_OPERATIONS_CONTRACT,
  type WorkstreamOperationRequest,
  type WorkstreamSetupStatus,
} from "@t3tools/contracts/jones/workstreamOperations";
import {
  WorkstreamOperationsClient,
  supportsWorkstreamOperations,
  type OperationResult,
  type WorkstreamOperationsTransport,
  operationMayReplace,
  operationRegistryIdentity,
  WorkstreamOperationJoined,
} from "./operations.ts";
const request = (operation_id: string): WorkstreamOperationRequest => ({
  contract: WORKSTREAM_OPERATIONS_CONTRACT,
  operation_id,
  origin: { kind: "app" },
  kind: "create_workstream",
  registry: { registry_id: "registry-1", owner_id: "owner" },
  workstream: {
    name: "Alpha",
    lifecycle: "planned",
    progress: { state: "unknown" },
    sort_order: 0,
  },
});
const identity = {
  contract: WORKSTREAM_OPERATIONS_CONTRACT,
  operation_id: "operation-00000001",
  origin: { kind: "app" as const },
  kind: "create_workstream" as const,
  request_sha256: "a".repeat(64),
  children: [],
  observed: null,
  accepted_at: "2026-10-23T00:00:00Z",
  updated_at: "2026-10-23T00:00:00Z",
};
const committed: OperationResult = {
  ...identity,
  state: "committed",
  reason: null,
  detail: null,
  poll_after_ms: null,
  result: { workstream_id: "alpha", membership_id: null, registry_version: 2 },
};
const open: OperationResult = {
  ...identity,
  state: "reconciling",
  reason: "create_effect_unknown",
  detail: null,
  result: null,
  poll_after_ms: 1000,
};
const config: OperationResult = {
  ...identity,
  state: "unavailable",
  reason: "not_configured",
  detail: null,
  result: null,
  poll_after_ms: null,
};
function fixture() {
  let bytes: string | null = null;
  const storage = {
    load: vi.fn(async () => bytes),
    save: vi.fn(async (value: string) => {
      bytes = value;
    }),
  };
  const transport = {
    submit: vi.fn<WorkstreamOperationsTransport["submit"]>(async () => committed),
    read: vi.fn<WorkstreamOperationsTransport["read"]>(async () => ({ operation: committed })),
    setup: vi.fn<WorkstreamOperationsTransport["setup"]>(),
  };
  const id = vi.fn(async () => "operation-00000001");
  return { storage, transport, id, client: new WorkstreamOperationsClient(transport, storage, id) };
}
afterEach(() => vi.useRealTimers());
describe("Workstream operation recovery", () => {
  it("absent and unknown capabilities stay legacy", () => {
    expect(supportsWorkstreamOperations(undefined)).toBe(false);
    expect(supportsWorkstreamOperations({})).toBe(false);
    expect(supportsWorkstreamOperations({ workstreamOperations: { contract: "future/2" } })).toBe(
      false,
    );
    expect(
      supportsWorkstreamOperations({
        workstreamOperations: { contract: WORKSTREAM_OPERATIONS_CONTRACT },
      }),
    ).toBe(true);
  });
  it("persists before dispatch and coalesces a create double-submit", async () => {
    const f = fixture();
    f.transport.submit.mockImplementation(async () => {
      expect(f.storage.save).toHaveBeenCalled();
      return committed;
    });
    const first = f.client.run("create:Alpha", request);
    const second = f.client.run("create:Alpha", request);
    expect(second).toBe(first);
    await first;
    expect(f.transport.submit).toHaveBeenCalledTimes(1);
    expect(await f.client.run("create:Alpha", request)).toEqual(committed);
    expect(f.id).toHaveBeenCalledTimes(1);
  });
  it("reuses exact bytes after a lost reply and a client/server restart", async () => {
    const f = fixture();
    f.transport.submit.mockRejectedValueOnce(new Error("reply lost"));
    await expect(f.client.run("create:Alpha", request)).rejects.toThrow("reply lost");
    const restored = new WorkstreamOperationsClient(f.transport, f.storage, f.id);
    f.transport.read.mockResolvedValue({ operation: null });
    await restored.refresh("create:Alpha");
    expect(f.transport.submit.mock.calls[0]![0].request).toEqual(
      f.transport.submit.mock.calls[1]![0].request,
    );
    expect(f.id).toHaveBeenCalledTimes(1);
  });
  it("refreshes CONFIG failures by submitting the same ID rather than reading stale failure", async () => {
    const f = fixture();
    f.transport.submit.mockResolvedValueOnce(config).mockResolvedValueOnce(committed);
    expect(await f.client.run("create:Alpha", request)).toEqual(config);
    expect(operationMayReplace(config)).toBe(false);
    expect(await f.client.run("create:Alpha", request)).toEqual(committed);
    expect(f.transport.read).not.toHaveBeenCalled();
    expect(f.transport.submit.mock.calls[0]![0]).toEqual(f.transport.submit.mock.calls[1]![0]);
  });
  it("refuses a changed target while the earlier request is unsealed", async () => {
    const f = fixture();
    f.transport.submit.mockResolvedValue(config);
    await f.client.run("create:Alpha", request);
    await expect(
      f.client.run(
        "create:Alpha",
        (id) =>
          ({
            ...request(id),
            registry: { registry_id: "registry-2", owner_id: "owner" },
          }) as WorkstreamOperationRequest,
      ),
    ).rejects.toThrow("reconciled");
    expect(f.transport.submit).toHaveBeenCalledTimes(1);
  });
  it("allocates a new ID after contention", async () => {
    const f = fixture();
    f.transport.submit
      .mockResolvedValueOnce({ ...config, reason: "contention" })
      .mockResolvedValueOnce({ ...committed, operation_id: "operation-00000002" });
    f.id.mockResolvedValueOnce("operation-00000001").mockResolvedValueOnce("operation-00000002");
    await f.client.run("create:Alpha", request);
    await f.client.run("create:Alpha", request);
    expect(f.transport.submit.mock.calls.map(([value]) => value.request.operation_id)).toEqual([
      "operation-00000001",
      "operation-00000002",
    ]);
  });
  it("honors poll_after and the foreground budget", async () => {
    vi.useFakeTimers();
    const f = fixture();
    f.transport.submit.mockResolvedValue(open);
    expect(await f.client.run("create:Alpha", request, { waitBudgetMs: 500 })).toEqual(open);
    expect(f.transport.read).not.toHaveBeenCalled();
    const run = f.client.run("create:Alpha", request, { waitBudgetMs: 1000 });
    await vi.advanceTimersByTimeAsync(999);
    expect(f.transport.read).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(await run).toEqual(committed);
  });
  it("aborts polling timers without discarding the pending request", async () => {
    vi.useFakeTimers();
    const f = fixture();
    f.transport.submit.mockResolvedValue(open);
    const abort = new AbortController();
    const run = f.client.run("create:Alpha", request, { signal: abort.signal });
    const rejection = expect(run).rejects.toBeDefined();
    await vi.advanceTimersByTimeAsync(0);
    abort.abort();
    await rejection;
    expect(vi.getTimerCount()).toBe(0);
    expect(f.client.snapshot()[0]?.request?.operation_id).toBe(identity.operation_id);
  });
  it("joins a 409 operation without claiming the requested change succeeded", async () => {
    const f = fixture();
    f.transport.submit.mockRejectedValue({
      code: "thread_operation_in_progress",
      open_operation_id: identity.operation_id,
    });
    f.transport.read.mockResolvedValue({ operation: open });
    await expect(f.client.run("create:Alpha", request)).rejects.toBeInstanceOf(
      WorkstreamOperationJoined,
    );
    expect(
      f.client
        .snapshot()
        .some((entry) => entry.key.startsWith("joined:") && entry.request === null),
    ).toBe(true);
  });
  it("never fabricates request bytes for a discovered operation lost on restart", async () => {
    vi.useFakeTimers();
    const f = fixture();
    f.transport.read
      .mockResolvedValueOnce({ operations: [open] })
      .mockResolvedValue({ operation: null });
    const abort = new AbortController();
    const watch = f.client.watch(abort.signal).catch(() => undefined);
    await vi.advanceTimersByTimeAsync(1000);
    expect(f.transport.submit).not.toHaveBeenCalled();
    expect(f.id).not.toHaveBeenCalled();
    abort.abort();
    await watch;
    expect(vi.getTimerCount()).toBe(0);
  });
  it("reports ID conflict explicitly and retains the original request", async () => {
    const f = fixture();
    f.transport.submit.mockRejectedValue({ code: "operation_id_conflict" });
    await expect(f.client.run("create:Alpha", request)).rejects.toThrow("ID conflict");
    expect(f.client.snapshot()[0]?.request).toEqual(request(identity.operation_id));
  });
  it("does not dispatch until the exact request is durably saved, including after a storage failure", async () => {
    const f = fixture();
    f.storage.save.mockRejectedValueOnce(new Error("storage full"));
    await expect(f.client.run("create:Alpha", request)).rejects.toThrow("storage full");
    expect(f.transport.submit).not.toHaveBeenCalled();
    await f.client.run("create:Alpha", request);
    expect(f.storage.save).toHaveBeenCalledTimes(3);
    expect(f.id).toHaveBeenCalledTimes(1);
    expect(f.transport.submit).toHaveBeenCalledTimes(1);
  });
  it("shares one open-operation observer and keeps it alive until the final consumer unmounts", async () => {
    vi.useFakeTimers();
    const f = fixture();
    f.transport.read.mockResolvedValue({ operations: [] });
    const first = new AbortController();
    const second = new AbortController();
    const a = f.client.watch(first.signal).catch(() => undefined);
    const b = f.client.watch(second.signal).catch(() => undefined);
    await vi.advanceTimersByTimeAsync(0);
    expect(f.transport.read).toHaveBeenCalledTimes(1);
    first.abort();
    await a;
    expect(vi.getTimerCount()).toBe(1);
    second.abort();
    await b;
    await vi.advanceTimersByTimeAsync(0);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("observes a joined operation without resubmitting the blocked choice in the background", async () => {
    vi.useFakeTimers();
    const f = fixture();
    f.transport.submit.mockRejectedValue({
      code: "thread_operation_in_progress",
      open_operation_id: identity.operation_id,
    });
    f.transport.read.mockResolvedValue({ operation: open });
    await expect(f.client.run("create:Alpha", request)).rejects.toBeInstanceOf(
      WorkstreamOperationJoined,
    );
    const abort = new AbortController();
    const watch = f.client.watch(abort.signal).catch(() => undefined);
    await vi.advanceTimersByTimeAsync(4000);
    expect(f.transport.submit).toHaveBeenCalledTimes(1);
    abort.abort();
    await watch;
  });
});

describe("operation registry identity shared by web and mobile", () => {
  const registry_id = "8cdd84b6-81c5-4c88-9d13-5d324abaf662";
  const status = {
    registry: { state: "ready", registry_id, owner_id: "owner", can_write: true },
  } as WorkstreamSetupStatus;
  it("takes the signed UUID from setup, not an origin from the list binding", () => {
    expect(operationRegistryIdentity(status, "owner")).toEqual({ registry_id, owner_id: "owner" });
  });
  it.each(["not_configured", "unavailable"] as const)("refuses absent %s identity", (state) => {
    expect(() =>
      operationRegistryIdentity(
        { ...status, registry: { state, registry_id: null, owner_id: null, can_write: false } },
        "owner",
      ),
    ).toThrow("Check Workstreams setup");
  });
  it("refuses owner mismatch and a missing ready identity", () => {
    expect(() => operationRegistryIdentity(status, "other-owner")).toThrow("registry_mismatch");
    expect(() =>
      operationRegistryIdentity(
        { ...status, registry: { ...status.registry, registry_id: "" } } as WorkstreamSetupStatus,
        "owner",
      ),
    ).toThrow("identity unavailable");
  });
});
