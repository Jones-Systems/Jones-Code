import * as DateTime from "effect/DateTime";
import type { HostStatus, HostStatusId, HostStatusSnapshot } from "@t3tools/contracts";

const HOST_IDS = ["vps", "test", "mini", "home"] as const;
const MAX_AGE_MS = 30_000;
const TIMEOUT_MS = 3_000;
const MAX_BODY_BYTES = 256 * 1024;

type HostConfig = { readonly url: string; readonly token?: string; readonly node?: string };
export type HostStatusConfig = Partial<Record<HostStatusId, HostConfig>>;

// Configure T3CODE_NETDATA_<VPS|TEST|MINI|HOME>_URL on the server only.
// Optional _TOKEN stays upstream; _NODE selects a machine GUID when a parent serves several nodes.
export function hostStatusConfigFromEnv(
  env: Readonly<Record<string, string | undefined>>,
): HostStatusConfig {
  return Object.fromEntries(
    HOST_IDS.flatMap((id) => {
      const prefix = `T3CODE_NETDATA_${id.toUpperCase()}`;
      const url = env[`${prefix}_URL`];
      return url ? [[id, { url, token: env[`${prefix}_TOKEN`], node: env[`${prefix}_NODE`] }]] : [];
    }),
  );
}

class InvalidResponse extends Error {}
class StaleResponse extends Error {}

function record(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new InvalidResponse();
  return value as Record<string, unknown>;
}

function finiteNonnegative(value: unknown): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0)
    throw new InvalidResponse();
  return value;
}

function nodeInfo(payload: unknown, selectedNode?: string) {
  const nodes = record(payload).nodes;
  if (!Array.isArray(nodes)) throw new InvalidResponse();
  const matching = nodes
    .map(record)
    .filter((node) => !selectedNode || node.machine_guid === selectedNode);
  if (matching.length !== 1) throw new InvalidResponse();
  const node = matching[0]!;
  if (node.state !== "reachable") throw new StaleResponse();
  if (typeof node.machine_guid !== "string" || !/^[a-f0-9-]{36}$/i.test(node.machine_guid))
    throw new InvalidResponse();
  const cpus = record(node.hw).cpus;
  if (typeof cpus !== "string" || !/^[1-9][0-9]*$/.test(cpus)) throw new InvalidResponse();
  const logicalCpuCount = Number(cpus);
  if (!Number.isSafeInteger(logicalCpuCount)) throw new InvalidResponse();
  return { guid: node.machine_guid, logicalCpuCount };
}

function metric(payload: unknown, dimension: string, units: string, now: number) {
  const root = record(payload);
  const view = record(root.view);
  const result = record(root.result);
  if (
    root.api !== 3 ||
    view.units !== units ||
    !Array.isArray(result.labels) ||
    result.labels.length !== 2 ||
    result.labels[0] !== "time" ||
    result.labels[1] !== dimension ||
    !Array.isArray(result.data) ||
    result.data.length !== 1
  )
    throw new InvalidResponse();
  const row: unknown = result.data[0];
  if (!Array.isArray(row) || row.length !== 2) throw new InvalidResponse();
  const sampledAt = finiteNonnegative(row[0]) * 1000;
  const lastEntry = finiteNonnegative(record(root.db).last_entry) * 1000;
  if (!Number.isSafeInteger(sampledAt) || sampledAt > now + 5_000 || lastEntry > now + 5_000)
    throw new InvalidResponse();
  if (now - sampledAt > MAX_AGE_MS || now - lastEntry > MAX_AGE_MS) throw new StaleResponse();
  return { value: finiteNonnegative(row[1]), sampledAt: Math.min(sampledAt, lastEntry) };
}

async function readJson(response: Response): Promise<unknown> {
  if (!response.ok) throw new Error("upstream");
  const reader = response.body?.getReader();
  if (!reader) throw new InvalidResponse();
  try {
    let size = 0;
    const chunks: Uint8Array[] = [];
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      size += next.value.byteLength;
      if (size > MAX_BODY_BYTES) throw new InvalidResponse();
      chunks.push(next.value);
    }
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    try {
      return JSON.parse(new TextDecoder().decode(bytes)) as unknown;
    } catch {
      throw new InvalidResponse();
    }
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

async function readHost(
  id: HostStatusId,
  config: HostConfig | undefined,
  fetcher: typeof fetch,
  now: () => number,
): Promise<HostStatus> {
  if (!config) return { id, status: "unavailable", reason: "not_configured" };
  const controller = new AbortController();
  // @effect-diagnostics-next-line globalTimers:off - native fetch deadline; cleared on every completion.
  const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const base = new URL(config.url);
    if (
      !["http:", "https:"].includes(base.protocol) ||
      base.username ||
      base.password ||
      base.search ||
      base.hash
    )
      throw new InvalidResponse();
    const request = async (path: string, params: Record<string, string> = {}) => {
      const url = new URL(`${base.pathname.replace(/\/$/, "")}/api/v3/${path}`, base);
      url.search = new URLSearchParams(params).toString();
      return readJson(
        await fetcher(url, {
          signal: controller.signal,
          redirect: "error",
          headers: {
            accept: "application/json",
            ...(config.token ? { authorization: `Bearer ${config.token}` } : {}),
          },
        }),
      );
    };
    const node = nodeInfo(await request("nodes"), config.node);
    const getMetric = (context: string, dimension: string) =>
      request("data", {
        scope_nodes: node.guid,
        nodes: node.guid,
        contexts: context,
        dimensions: dimension,
        after: "-5",
        points: "1",
        format: "json",
        options: "jsonwrap",
        group_by: "dimension",
      });
    const [loadPayload, memoryPayload] = await Promise.all([
      getMetric("system.load", "load1"),
      getMetric("mem.available", "avail"),
    ]);
    const at = now();
    const load = metric(loadPayload, "load1", "load", at);
    const memory = metric(memoryPayload, "avail", "MiB", at);
    const availableMemoryBytes = Math.round(memory.value * 1024 * 1024);
    if (!Number.isSafeInteger(availableMemoryBytes)) throw new InvalidResponse();
    return {
      id,
      status: "available",
      load1: load.value,
      logicalCpuCount: node.logicalCpuCount,
      availableMemoryBytes,
      sampledAt: DateTime.formatIso(
        DateTime.makeUnsafe(Math.min(load.sampledAt, memory.sampledAt)),
      ),
    };
  } catch (error) {
    return {
      id,
      status: "unavailable",
      reason:
        error instanceof StaleResponse
          ? "stale"
          : error instanceof InvalidResponse
            ? "invalid_response"
            : "upstream_unavailable",
    };
  } finally {
    clearTimeout(timeout);
    controller.abort();
  }
}

export async function readHostStatus(
  config: HostStatusConfig,
  fetcher: typeof fetch = fetch,
  now: () => number = Date.now,
): Promise<HostStatusSnapshot> {
  return { hosts: await Promise.all(HOST_IDS.map((id) => readHost(id, config[id], fetcher, now))) };
}
