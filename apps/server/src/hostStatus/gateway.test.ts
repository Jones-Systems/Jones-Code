import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { HostStatus } from "@t3tools/contracts";
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
const cpu = (overrides: Record<string, unknown> = {}, timestamp = now / 1000) => {
  const values = {
    user: 5,
    nice: 1,
    system: 3,
    irq: 1,
    softirq: 2,
    guest: 3,
    guest_nice: 1,
    iowait: 10,
    steal: 4,
    ...overrides,
  };
  return {
    api: 3,
    db: { last_entry: timestamp },
    view: { units: "percentage" },
    result: {
      labels: ["time", ...Object.keys(values)],
      data: [[timestamp, ...Object.values(values)]],
    },
  };
};
const fixtureFetch = (
  cpuPayload: unknown = cpu(),
  memory: unknown = data("free", 4096.5, "MiB"),
  info: unknown = nodes,
) =>
  vi.fn<typeof fetch>(async (input) => {
    const url = new URL(String(input));
    return Response.json(
      url.pathname.endsWith("nodes")
        ? info
        : url.searchParams.get("contexts") === "system.cpu"
          ? cpuPayload
          : memory,
    );
  });
const config = { vps: { url: "https://netdata.example", token: "test-only-secret" } };

afterEach(() => vi.useRealTimers());

describe("Netdata host status", () => {
  it("enforces CPU percentage bounds and the occupied RAM wire contract", () => {
    const decode = Schema.decodeUnknownSync(HostStatus);
    const host = {
      id: "vps",
      status: "available",
      cpuUsagePercent: 40,
      occupiedMemoryBytes: 1024,
      totalMemoryBytes: 2048,
      logicalCpuCount: 8,
      sampledAt: "2027-01-15T08:00:00.000Z",
    };
    for (const cpuUsagePercent of [0, 100])
      expect(decode({ ...host, cpuUsagePercent })).toMatchObject({ cpuUsagePercent });
    for (const cpuUsagePercent of [-1, 101, NaN, Infinity, null, "40"])
      expect(() => decode({ ...host, cpuUsagePercent })).toThrow();
    for (const occupiedMemoryBytes of [-1, 1.5, null])
      expect(() => decode({ ...host, occupiedMemoryBytes })).toThrow();
    const { cpuUsagePercent: _cpu, occupiedMemoryBytes: _ram, ...legacy } = host;
    expect(() => decode({ ...legacy, load1: 1, availableMemoryBytes: 1024 })).toThrow();
  });

  it("reads available Linux RAM separately from free RAM and retains legacy occupied semantics", async () => {
    const fallback = fixtureFetch();
    const fetcher = vi.fn<typeof fetch>(async (input, init) =>
      new URL(String(input)).searchParams.get("contexts") === "mem.available"
        ? Response.json(data("avail", 6144.5, "MiB", now / 1000 - 4))
        : fallback(input, init),
    );
    expect((await readHostStatus(config, fetcher, () => now)).hosts[0]).toMatchObject({
      status: "available",
      availableMemoryBytes: 6144.5 * 1024 ** 2,
      occupiedMemoryBytes: 8589934592 - 4096.5 * 1024 ** 2,
      sampledAt: "2027-01-15T07:59:56.000Z",
    });
  });

  it("estimates Mac available RAM including speculative, inactive, and purgeable pages", async () => {
    const memory = data("free", 1024, "MiB");
    memory.result.labels.push(
      "speculative",
      "inactive",
      "purgeable",
      "active",
      "wired",
      "compressed",
    );
    memory.result.data[0]!.push(256, 2048, 512, 2048, 1024, 128);
    const macCpu = {
      ...cpu(),
      result: { labels: ["time", "user", "nice", "system"], data: [[now / 1000, 10, 0, 10]] },
    };
    expect(
      (await readHostStatus(config, fixtureFetch(macCpu, memory), () => now)).hosts[0],
    ).toMatchObject({
      status: "available",
      availableMemoryBytes: 3840 * 1024 ** 2,
      occupiedMemoryBytes: 7 * 1024 ** 3,
    });
  });

  it.each([data("avail", 0, "MiB"), data("avail", 8192, "MiB")])(
    "accepts zero and total available RAM",
    async (payload) => {
      const fallback = fixtureFetch();
      const fetcher: typeof fetch = async (input, init) =>
        new URL(String(input)).searchParams.get("contexts") === "mem.available"
          ? Response.json(payload)
          : fallback(input, init);
      expect((await readHostStatus(config, fetcher, () => now)).hosts[0]).toMatchObject({
        status: "available",
        availableMemoryBytes: Number(payload.result.data[0]![1]) * 1024 ** 2,
      });
    },
  );

  it.each([
    data("avail", null, "MiB"),
    data("avail", -1, "MiB"),
    data("avail", 8193, "MiB"),
    data("avail", 1, "MB"),
    data("free", 1, "MiB"),
    data("avail", 1, "MiB", now / 1000 - 31),
    { api: 3 },
  ])("keeps CPU usable without claiming unavailable or invalid available RAM", async (payload) => {
    const fallback = fixtureFetch();
    const fetcher: typeof fetch = async (input, init) =>
      new URL(String(input)).searchParams.get("contexts") === "mem.available"
        ? Response.json(payload)
        : fallback(input, init);
    const host = (await readHostStatus(config, fetcher, () => now)).hosts[0];
    expect(host).toMatchObject({ status: "available", cpuUsagePercent: 16 });
    expect(host).not.toHaveProperty("availableMemoryBytes");
  });

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

  it("sums busy CPU once, excludes idle/wait/steal, converts free RAM, and reports source time", async () => {
    const fetcher = fixtureFetch(cpu({}, now / 1000 - 2));
    const result = await readHostStatus(config, fetcher, () => now);
    expect(result.hosts[0]).toEqual({
      id: "vps",
      status: "available",
      cpuUsagePercent: 16,
      logicalCpuCount: 8,
      occupiedMemoryBytes: 8589934592 - 4096.5 * 1024 * 1024,
      totalMemoryBytes: 8589934592,
      sampledAt: "2027-01-15T07:59:58.000Z",
    });
    const calls = fetcher.mock.calls;
    expect(calls).toHaveLength(4);
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
    ["wrong units", { ...cpu(), view: { units: "load" } }],
    ["negative", cpu({ user: -1 })],
    ["null", cpu({ user: null })],
    ["string", cpu({ user: "1" })],
    ["over 100", cpu({ user: 101 })],
    ["busy sum over 100", cpu({ user: 99 })],
    ["wrong dimension", data("load5", 1, "%")],
    ["future", cpu({}, now / 1000 + 6)],
    ["bad schema", { api: 3 }],
  ])("rejects %s", async (_label, payload) => {
    expect((await readHostStatus(config, fixtureFetch(payload), () => now)).hosts[0]).toEqual({
      id: "vps",
      status: "unavailable",
      reason: "invalid_response",
    });
  });

  it("rejects stale source data even with a current query bucket", async () => {
    const payload = cpu();
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
      (await readHostStatus(config, fixtureFetch(undefined, data("free", 1, "MB")), () => now))
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

  it("rejects free RAM above total and accepts their equality", async () => {
    expect(
      (await readHostStatus(config, fixtureFetch(undefined, data("free", 8193, "MiB")), () => now))
        .hosts[0],
    ).toMatchObject({ reason: "invalid_response" });
    expect(
      (await readHostStatus(config, fixtureFetch(undefined, data("free", 8192, "MiB")), () => now))
        .hosts[0],
    ).toMatchObject({
      status: "available",
      occupiedMemoryBytes: 0,
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
      const isCpu = url.searchParams.get("contexts") === "system.cpu";
      return Response.json(
        isCpu
          ? cpu(selected === guid ? {} : { user: 0, nice: 0, system: 0, irq: 0 })
          : data("free", selected === guid ? 4096 : 1024, "MiB"),
      );
    });
    const result = await readHostStatus(shared, fetcher, () => now);
    expect(result.hosts).toEqual([
      expect.objectContaining({
        id: "vps",
        status: "available",
        cpuUsagePercent: 16,
        logicalCpuCount: 8,
      }),
      expect.objectContaining({
        id: "test",
        status: "available",
        cpuUsagePercent: 6,
        logicalCpuCount: 2,
        occupiedMemoryBytes: 3221225472,
        totalMemoryBytes: 4294967296,
      }),
      { id: "mini", status: "unavailable", reason: "not_configured" },
      { id: "home", status: "unavailable", reason: "not_configured" },
    ]);
    expect(fetcher).toHaveBeenCalledTimes(8);
    const queries = fetcher.mock.calls
      .map(([input]) => new URL(String(input)))
      .filter((url) => url.pathname.endsWith("data"));
    for (const selected of [guid, testGuid]) {
      const scoped = queries.filter((url) => url.searchParams.get("nodes") === selected);
      expect(scoped).toHaveLength(3);
      expect(scoped.map((url) => url.searchParams.get("contexts")).sort()).toEqual([
        "mem.available",
        "system.cpu",
        "system.ram",
      ]);
      for (const url of scoped) {
        expect(url.origin).toBe("http://127.0.0.1:19999");
        expect(Object.fromEntries(url.searchParams)).toEqual({
          scope_nodes: selected,
          nodes: selected,
          contexts: url.searchParams.get("contexts"),
          dimensions: url.searchParams.get("contexts") === "mem.available" ? "avail" : "*",
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

  it("accepts the captured Netdata 2.11.0 Linux CPU shape", async () => {
    const payload = {
      api: 3,
      db: { last_entry: now / 1000 },
      view: { units: "percentage" },
      result: {
        labels: [
          "time",
          "guest_nice",
          "guest",
          "steal",
          "softirq",
          "irq",
          "user",
          "system",
          "nice",
          "iowait",
        ],
        data: [[now / 1000, 0, 0, 0.0690339, 0.501911, 0, 10.5347086, 8.0013066, 0, 0.2448231]],
      },
    };
    const result = (await readHostStatus(config, fixtureFetch(payload), () => now)).hosts[0];
    expect(result).toMatchObject({ status: "available" });
    if (result?.status === "available") expect(result.cpuUsagePercent).toBeCloseTo(19.0379262);
  });

  it("accepts optional idle but excludes it and validates every CPU value", async () => {
    expect(
      (await readHostStatus(config, fixtureFetch(cpu({ idle: 70 })), () => now)).hosts[0],
    ).toMatchObject({ cpuUsagePercent: 16 });
    for (const overrides of [{ idle: null }, { iowait: null }, { steal: 101 }]) {
      expect(
        (await readHostStatus(config, fixtureFetch(cpu(overrides)), () => now)).hosts[0],
      ).toMatchObject({ reason: "invalid_response" });
    }
  });

  it("accepts complete Mac CPU dimensions in any order without core normalization", async () => {
    const payload = {
      api: 3,
      db: { last_entry: now / 1000 },
      view: { units: "percentage" },
      result: { labels: ["time", "system", "nice", "user"], data: [[now / 1000, 15, 5, 20]] },
    };
    expect((await readHostStatus(config, fixtureFetch(payload), () => now)).hosts[0]).toMatchObject(
      { status: "available", cpuUsagePercent: 40 },
    );
  });

  it("rejects incomplete, duplicate, and unknown CPU dimensions", async () => {
    for (const labels of [
      ["user", "system", "idle"],
      ["user", "nice", "system", "idle", "irq"],
      ["user", "nice", "system", "idle", "user"],
      ["user", "nice", "system", "idle", "unexpected"],
    ]) {
      const payload = {
        api: 3,
        db: { last_entry: now / 1000 },
        view: { units: "percentage" },
        result: { labels: ["time", ...labels], data: [[now / 1000, ...labels.map(() => 1)]] },
      };
      expect(
        (await readHostStatus(config, fixtureFetch(payload), () => now)).hosts[0],
      ).toMatchObject({ reason: "invalid_response" });
    }
  });

  it("rejects missing, null, negative, or wrongly labeled free RAM", async () => {
    for (const payload of [
      data("free", null, "MiB"),
      data("free", -1, "MiB"),
      data("avail", 1, "MiB"),
      { api: 3 },
    ]) {
      expect(
        (await readHostStatus(config, fixtureFetch(undefined, payload), () => now)).hosts[0],
      ).toMatchObject({ reason: "invalid_response" });
    }
  });

  it("uses the oldest CPU/RAM bucket or database time and rejects either stale source", async () => {
    const memory = data("free", 4096, "MiB", now / 1000 - 3);
    memory.db.last_entry = now / 1000 - 4;
    expect(
      (await readHostStatus(config, fixtureFetch(cpu({}, now / 1000 - 2), memory), () => now))
        .hosts[0],
    ).toMatchObject({ sampledAt: "2027-01-15T07:59:56.000Z" });
    for (const payload of [
      data("free", 1, "MiB", now / 1000 - 31),
      { ...data("free", 1, "MiB"), db: { last_entry: now / 1000 - 31 } },
    ]) {
      expect(
        (await readHostStatus(config, fixtureFetch(undefined, payload), () => now)).hosts[0],
      ).toMatchObject({ reason: "stale" });
    }
    expect(
      (await readHostStatus(config, fixtureFetch(cpu({}, now / 1000 - 31)), () => now)).hosts[0],
    ).toMatchObject({ reason: "stale" });
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
    expect(fetcher).toHaveBeenCalledTimes(16);
    const credentials = fetcher.mock.calls.map(([input, init]) => ({
      origin: new URL(String(input)).origin,
      authorization: new Headers(init?.headers).get("authorization"),
    }));
    expect(credentials.filter(({ origin }) => origin === "https://override.example")).toEqual(
      Array.from({ length: 4 }, () => ({
        origin: "https://override.example",
        authorization: null,
      })),
    );
    for (const authorization of ["Bearer shared-test-token", "Bearer mini-test-token"]) {
      expect(credentials.filter((call) => call.authorization === authorization)).toEqual(
        Array.from({ length: 4 }, () => ({ origin: "https://collector.example", authorization })),
      );
    }
    expect(credentials.filter(({ origin }) => origin === "https://home.example")).toEqual(
      Array.from({ length: 4 }, () => ({
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
