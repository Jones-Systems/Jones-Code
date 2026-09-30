import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { hostStatusConfigFromEnv, readHostStatus } from "./gateway.ts";

const guid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const now = 1_800_000_000_000;
const nodes = {
  api: 2,
  nodes: [{ mg: guid, state: "reachable", hw: { cpus: "8", memory: "8589934592" } }],
};
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

  it("accepts the collector's canonical mg node identity", async () => {
    const info = {
      api: 2,
      nodes: [{ mg: guid, state: "reachable", hw: { cpus: "16", memory: "67429822464" } }],
    };
    const fetcher = fixtureFetch(undefined, undefined, info);
    const result = await readHostStatus({ vps: { ...config.vps, node: guid } }, fetcher, () => now);
    expect(result.hosts[0]).toMatchObject({
      status: "available",
      logicalCpuCount: 16,
      totalMemoryBytes: 67429822464,
    });
    for (const [input] of fetcher.mock.calls.slice(1)) {
      const url = new URL(String(input));
      expect(url.searchParams.get("scope_nodes")).toBe(guid);
      expect(url.searchParams.get("nodes")).toBe(guid);
    }
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
      totalMemoryBytes: 8589934592,
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

  it.each([undefined, "unknown", "0", "-1", "1.5", "8 GiB", "9007199254740992", 8589934592])(
    "rejects invalid total RAM metadata %s",
    async (memory) => {
      const info = { nodes: [{ ...nodes.nodes[0], hw: { cpus: "8", memory } }] };
      expect(
        (await readHostStatus(config, fixtureFetch(undefined, undefined, info), () => now))
          .hosts[0],
      ).toMatchObject({ reason: "invalid_response" });
    },
  );

  it("rejects available RAM above total and accepts their equality", async () => {
    expect(
      (await readHostStatus(config, fixtureFetch(undefined, data("avail", 8193, "MiB")), () => now))
        .hosts[0],
    ).toMatchObject({ reason: "invalid_response" });
    expect(
      (await readHostStatus(config, fixtureFetch(undefined, data("avail", 8192, "MiB")), () => now))
        .hosts[0],
    ).toMatchObject({
      status: "available",
      availableMemoryBytes: 8589934592,
      totalMemoryBytes: 8589934592,
    });
  });

  it("takes total RAM from the selected node on a multi-node parent", async () => {
    const info = {
      nodes: [
        {
          ...nodes.nodes[0],
          mg: "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee",
          hw: { cpus: "2", memory: "1024" },
        },
        ...nodes.nodes,
      ],
    };
    const result = await readHostStatus(
      { vps: { ...config.vps, node: guid } },
      fixtureFetch(undefined, undefined, info),
      () => now,
    );
    expect(result.hosts[0]).toMatchObject({
      status: "available",
      logicalCpuCount: 8,
      totalMemoryBytes: 8589934592,
    });
  });

  it("reads mapped hosts from one collector with distinct node-scoped metrics", async () => {
    const testGuid = "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee";
    const info = {
      api: 2,
      nodes: [
        ...nodes.nodes,
        { mg: testGuid, state: "reachable", hw: { cpus: "2", memory: "4294967296" } },
      ],
    };
    const shared = hostStatusConfigFromEnv({
      T3CODE_NETDATA_URL: "http://127.0.0.1:19999",
      T3CODE_NETDATA_VPS_NODE: guid,
      T3CODE_NETDATA_TEST_NODE: testGuid,
    });
    const fetcher = vi.fn<typeof fetch>(async (input) => {
      const url = new URL(String(input));
      if (url.pathname.endsWith("nodes")) return Response.json(info);
      const selected = url.searchParams.get("nodes");
      const isLoad = url.searchParams.get("contexts") === "system.load";
      return Response.json(
        isLoad
          ? data("load1", selected === guid ? 12 : 3, "load")
          : data("avail", selected === guid ? 4096 : 1024, "MiB"),
      );
    });
    const result = await readHostStatus(shared, fetcher, () => now);
    expect(result.hosts).toEqual([
      expect.objectContaining({ id: "vps", status: "available", load1: 12, logicalCpuCount: 8 }),
      expect.objectContaining({
        id: "test",
        status: "available",
        load1: 3,
        logicalCpuCount: 2,
        availableMemoryBytes: 1073741824,
        totalMemoryBytes: 4294967296,
      }),
      { id: "mini", status: "unavailable", reason: "not_configured" },
      { id: "home", status: "unavailable", reason: "not_configured" },
    ]);
    expect(fetcher).toHaveBeenCalledTimes(6);
    const queries = fetcher.mock.calls
      .map(([input]) => new URL(String(input)))
      .filter((url) => url.pathname.endsWith("data"));
    for (const selected of [guid, testGuid]) {
      const scoped = queries.filter((url) => url.searchParams.get("nodes") === selected);
      expect(scoped).toHaveLength(2);
      expect(scoped.map((url) => url.searchParams.get("contexts")).sort()).toEqual([
        "mem.available",
        "system.load",
      ]);
      for (const url of scoped) {
        expect(url.origin).toBe("http://127.0.0.1:19999");
        expect(Object.fromEntries(url.searchParams)).toEqual({
          scope_nodes: selected,
          nodes: selected,
          contexts: url.searchParams.get("contexts"),
          dimensions: url.searchParams.get("contexts") === "system.load" ? "load1" : "avail",
          after: "-5",
          points: "1",
          format: "json",
          options: "jsonwrap",
          group_by: "dimension",
        });
      }
    }
    expect(JSON.stringify(result)).not.toContain(guid);
  });

  it("leaves shared collector slots without an explicit node unconfigured", async () => {
    const shared = hostStatusConfigFromEnv({
      T3CODE_NETDATA_URL: "https://collector.example",
      T3CODE_NETDATA_TOKEN: "shared-test-token",
      T3CODE_NETDATA_VPS_NODE: " ",
      T3CODE_NETDATA_OTHER_NODE: guid,
    });
    expect(shared).toEqual({});
    const fetcher = fixtureFetch();
    expect((await readHostStatus(shared, fetcher, () => now)).hosts).toEqual(
      ["vps", "test", "mini", "home"].map((id) => ({
        id,
        status: "unavailable",
        reason: "not_configured",
      })),
    );
    expect(fetcher).not.toHaveBeenCalled();
  });

  it.each([
    ["unknown node", { nodes: nodes.nodes }, "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee"],
    ["duplicate identity", { nodes: [...nodes.nodes, ...nodes.nodes] }, guid],
    ["unselected parent", { nodes: [...nodes.nodes, ...nodes.nodes] }, undefined],
    [
      "conflicting identity aliases",
      { nodes: [{ ...nodes.nodes[0], machine_guid: "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee" }] },
      guid,
    ],
    ["malformed canonical identity", { nodes: [{ ...nodes.nodes[0], mg: "invalid" }] }, guid],
  ])("rejects %s without requesting metrics", async (_label, info, node) => {
    const fetcher = fixtureFetch(undefined, undefined, info);
    expect(
      (
        await readHostStatus(
          { vps: { ...config.vps, ...(node ? { node } : {}) } },
          fetcher,
          () => now,
        )
      ).hosts[0],
    ).toEqual({ id: "vps", status: "unavailable", reason: "invalid_response" });
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("accepts a legacy machine_guid and matching aliases without a node selector", async () => {
    for (const identity of [{ machine_guid: guid }, { mg: guid, machine_guid: guid }]) {
      const info = {
        nodes: [{ ...identity, state: "reachable", hw: { cpus: "8", memory: "8589934592" } }],
      };
      expect(
        (await readHostStatus(config, fixtureFetch(undefined, undefined, info), () => now))
          .hosts[0],
      ).toMatchObject({ status: "available", logicalCpuCount: 8 });
    }
  });

  it("binds shared and per-host tokens to their configured endpoints", async () => {
    const shared = hostStatusConfigFromEnv({
      T3CODE_NETDATA_URL: "https://collector.example",
      T3CODE_NETDATA_TOKEN: "shared-test-token",
      T3CODE_NETDATA_VPS_NODE: guid,
      T3CODE_NETDATA_TEST_URL: "https://override.example",
      T3CODE_NETDATA_MINI_NODE: guid,
      T3CODE_NETDATA_MINI_TOKEN: "mini-test-token",
      T3CODE_NETDATA_HOME_URL: "https://home.example",
      T3CODE_NETDATA_HOME_TOKEN: "home-test-token",
    });
    const fetcher = fixtureFetch();
    const result = await readHostStatus(shared, fetcher, () => now);
    expect(result.hosts.every((host) => host.status === "available")).toBe(true);
    expect(fetcher).toHaveBeenCalledTimes(12);
    const credentials = fetcher.mock.calls.map(([input, init]) => ({
      origin: new URL(String(input)).origin,
      authorization: new Headers(init?.headers).get("authorization"),
    }));
    expect(credentials.filter(({ origin }) => origin === "https://override.example")).toEqual(
      Array.from({ length: 3 }, () => ({
        origin: "https://override.example",
        authorization: null,
      })),
    );
    for (const authorization of ["Bearer shared-test-token", "Bearer mini-test-token"]) {
      expect(credentials.filter((call) => call.authorization === authorization)).toEqual(
        Array.from({ length: 3 }, () => ({ origin: "https://collector.example", authorization })),
      );
    }
    expect(credentials.filter(({ origin }) => origin === "https://home.example")).toEqual(
      Array.from({ length: 3 }, () => ({
        origin: "https://home.example",
        authorization: "Bearer home-test-token",
      })),
    );
    expect(JSON.stringify(result)).not.toContain("token");
    expect(JSON.stringify(result)).not.toContain("example");
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
