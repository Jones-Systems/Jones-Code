import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { hostStatusConfigFromEnv, readHostStatus } from "./gateway.ts";

const guid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const now = 1_800_000_000_000;
const nodes = { nodes: [{ machine_guid: guid, state: "reachable", hw: { cpus: "8" } }] };
const data = (dimension: string, value: unknown, units: string, timestamp = now / 1000) => ({
  api: 3,
  db: { last_entry: timestamp },
  view: { units },
  result: { labels: ["time", dimension], data: [[timestamp, value]] },
});
const fixtureFetch = (
  load: unknown = data("load1", 12, "load"),
  memory: unknown = data("avail", 4096.5, "MiB"),
  info: unknown = nodes,
) =>
  vi.fn<typeof fetch>(async (input) => {
    const url = new URL(String(input));
    return Response.json(
      url.pathname.endsWith("nodes")
        ? info
        : url.searchParams.get("contexts") === "system.load"
          ? load
          : memory,
    );
  });
const config = { vps: { url: "https://netdata.example", token: "test-only-secret" } };

afterEach(() => vi.useRealTimers());

describe("Netdata host status", () => {
  it("always returns fixed ordered unavailable hosts when unconfigured without fetching", async () => {
    const fetcher = fixtureFetch();
    expect(await readHostStatus({}, fetcher)).toEqual({
      hosts: ["vps", "test", "mini", "home"].map((id) => ({
        id,
        status: "unavailable",
        reason: "not_configured",
      })),
    });
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("preserves load above CPU count, normalizes MiB, and reports source time", async () => {
    const fetcher = fixtureFetch(data("load1", 12, "load", now / 1000 - 2));
    const result = await readHostStatus(config, fetcher, () => now);
    expect(result.hosts[0]).toEqual({
      id: "vps",
      status: "available",
      load1: 12,
      logicalCpuCount: 8,
      availableMemoryBytes: 4096.5 * 1024 * 1024,
      sampledAt: "2027-01-15T07:59:58.000Z",
    });
    const calls = fetcher.mock.calls;
    expect(calls).toHaveLength(3);
    for (const [input, init] of calls) {
      expect(init?.redirect).toBe("error");
      expect(init?.headers).toMatchObject({ authorization: "Bearer test-only-secret" });
      const url = new URL(String(input));
      expect(url.origin).toBe("https://netdata.example");
      if (url.pathname.endsWith("data")) expect(url.searchParams.get("scope_nodes")).toBe(guid);
    }
    expect(JSON.stringify(result)).not.toContain("secret");
    expect(JSON.stringify(result)).not.toContain("netdata.example");
  });

  it.each([
    ["wrong units", data("load1", 1, "%")],
    ["negative", data("load1", -1, "load")],
    ["null", data("load1", null, "load")],
    ["string", data("load1", "1", "load")],
    ["wrong dimension", data("load5", 1, "load")],
    ["future", data("load1", 1, "load", now / 1000 + 6)],
    ["bad schema", { api: 3 }],
  ])("rejects %s", async (_label, payload) => {
    expect((await readHostStatus(config, fixtureFetch(payload), () => now)).hosts[0]).toEqual({
      id: "vps",
      status: "unavailable",
      reason: "invalid_response",
    });
  });

  it("rejects stale source data even with a current query bucket", async () => {
    const payload = data("load1", 1, "load");
    payload.db.last_entry -= 31;
    expect((await readHostStatus(config, fixtureFetch(payload), () => now)).hosts[0]).toMatchObject(
      { reason: "stale" },
    );
  });

  it("rejects missing or ambiguous CPU metadata and mismatched memory units", async () => {
    for (const info of [
      { nodes: [] },
      { nodes: [...nodes.nodes, ...nodes.nodes] },
      { nodes: [{ ...nodes.nodes[0], hw: { cpus: "0" } }] },
    ]) {
      expect(
        (await readHostStatus(config, fixtureFetch(undefined, undefined, info), () => now))
          .hosts[0],
      ).toMatchObject({ reason: "invalid_response" });
    }
    expect(
      (await readHostStatus(config, fixtureFetch(undefined, data("avail", 1, "MB")), () => now))
        .hosts[0],
    ).toMatchObject({ reason: "invalid_response" });
  });

  it("isolates upstream failure without returning raw errors", async () => {
    const good = fixtureFetch();
    const fetcher = vi.fn<typeof fetch>(async (input, init) => {
      if (new URL(String(input)).hostname === "failed.example")
        throw new Error("credential=private");
      return good(input, init);
    });
    const result = await readHostStatus(
      { ...config, test: { url: "https://failed.example" } },
      fetcher,
      () => now,
    );
    expect(result.hosts[0]?.status).toBe("available");
    expect(result.hosts[1]).toEqual({
      id: "test",
      status: "unavailable",
      reason: "upstream_unavailable",
    });
    expect(JSON.stringify(result)).not.toContain("private");
  });

  it("aborts a stalled upstream and clears its deadline", async () => {
    vi.useFakeTimers();
    const fetcher = vi.fn<typeof fetch>(
      (_input, init) =>
        new Promise((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => reject(new Error("aborted")), {
            once: true,
          });
        }),
    );
    const result = readHostStatus(config, fetcher, () => now);
    await vi.advanceTimersByTimeAsync(3000);
    expect((await result).hosts[0]).toMatchObject({ reason: "upstream_unavailable" });
    expect(vi.getTimerCount()).toBe(0);
  });

  it("rejects oversized or malformed JSON and non-success responses", async () => {
    for (const response of [
      new Response("x".repeat(256 * 1024 + 1)),
      new Response("not json"),
      new Response("private", { status: 401 }),
    ]) {
      const fetcher = vi.fn<typeof fetch>(async () => response);
      expect((await readHostStatus(config, fetcher, () => now)).hosts[0]?.status).toBe(
        "unavailable",
      );
    }
  });

  it("reads only fixed server configuration keys and rejects URL credentials", async () => {
    expect(
      hostStatusConfigFromEnv({
        T3CODE_NETDATA_VPS_URL: "https://example",
        T3CODE_NETDATA_OTHER_URL: "https://untrusted",
      }),
    ).toEqual({ vps: { url: "https://example" } });
    const fetcher = fixtureFetch();
    expect(
      (await readHostStatus({ vps: { url: "https://user:secret@example" } }, fetcher)).hosts[0],
    ).toMatchObject({ reason: "invalid_response" });
    expect(fetcher).not.toHaveBeenCalled();
  });
});
