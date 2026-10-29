import * as DateTime from "effect/DateTime";
import type { HostStatus, HostStatusId, HostStatusSnapshot } from "@t3tools/contracts";

const HOST_IDS = ["vps", "test", "mini", "home"] as const;
const MAX_AGE_MS = 30_000;
const TIMEOUT_MS = 3_000;
const MAX_BODY_BYTES = 256 * 1024;

type HostConfig = {
  readonly url: string;
  readonly token?: string;
  readonly node?: string;
  readonly storageMountPoint?: string;
};
export type HostStatusConfig = Partial<Record<HostStatusId, HostConfig>>;

// Shared collector slots require an explicit _NODE so one local node cannot fill every host.
// Tokens stay server-side and shared credentials never follow a per-host URL override.
export function hostStatusConfigFromEnv(
  env: Readonly<Record<string, string | undefined>>,
): HostStatusConfig {
  return Object.fromEntries(
    HOST_IDS.flatMap((id) => {
      const prefix = `T3CODE_NETDATA_${id.toUpperCase()}`;
      const overrideUrl = env[`${prefix}_URL`];
      const node = env[`${prefix}_NODE`]?.trim() || undefined;
      const url = overrideUrl || (node ? env.T3CODE_NETDATA_URL : undefined);
      const token = env[`${prefix}_TOKEN`] ?? (overrideUrl ? undefined : env.T3CODE_NETDATA_TOKEN);
      const storageMountPoint = env[`${prefix}_STORAGE_MOUNT_POINT`]?.trim() || undefined;
      return url ? [[id, { url, token, node, storageMountPoint }]] : [];
    }),
  );
}

class InvalidResponse extends Error {}
class StaleResponse extends Error {}
class CollectorDisconnected extends StaleResponse {}

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
    .map((value) => {
      const node = record(value);
      if (node.mg !== undefined && node.machine_guid !== undefined && node.mg !== node.machine_guid)
        throw new InvalidResponse();
      const guid = node.mg ?? node.machine_guid;
      if (typeof guid !== "string" || !/^[a-f0-9-]{36}$/i.test(guid)) throw new InvalidResponse();
      return { node, guid };
    })
    .filter(({ guid }) => !selectedNode || guid === selectedNode);
  if (matching.length !== 1) throw new InvalidResponse();
  const { node, guid } = matching[0]!;
  if (node.state !== "reachable") throw new CollectorDisconnected();
  const cpus = record(node.hw).cpus;
  if (typeof cpus !== "string" || !/^[1-9][0-9]*$/.test(cpus)) throw new InvalidResponse();
  const logicalCpuCount = Number(cpus);
  if (!Number.isSafeInteger(logicalCpuCount)) throw new InvalidResponse();
  // Netdata hw.memory is NETDATA_SYSTEM_TOTAL_RAM, expressed in bytes.
  const memory = record(node.hw).memory;
  if (typeof memory !== "string" || !/^[1-9][0-9]*$/.test(memory)) throw new InvalidResponse();
  const totalMemoryBytes = Number(memory);
  if (!Number.isSafeInteger(totalMemoryBytes)) throw new InvalidResponse();
  return { guid, logicalCpuCount, totalMemoryBytes };
}

function metric(payload: unknown, units: string, now: number, signedSwap = false) {
  const root = record(payload);
  const view = record(root.view);
  const result = record(root.result);
  if (
    root.api !== 3 ||
    view.units !== units ||
    !Array.isArray(result.labels) ||
    result.labels.length < 2 ||
    result.labels.length > 11 ||
    result.labels[0] !== "time" ||
    !Array.isArray(result.data) ||
    result.data.length !== 1
  )
    throw new InvalidResponse();
  const row: unknown = result.data[0];
  if (!Array.isArray(row) || row.length !== result.labels.length) throw new InvalidResponse();
  const sampledAt = finiteNonnegative(row[0]) * 1000;
  const lastEntry = finiteNonnegative(record(root.db).last_entry) * 1000;
  if (
    !Number.isSafeInteger(sampledAt) ||
    !Number.isSafeInteger(lastEntry) ||
    sampledAt > now + 5_000 ||
    lastEntry > now + 5_000
  )
    throw new InvalidResponse();
  if (now - sampledAt > MAX_AGE_MS || now - lastEntry > MAX_AGE_MS) throw new StaleResponse();
  const values = new Map<string, number>();
  for (let index = 1; index < result.labels.length; index++) {
    const label: unknown = result.labels[index];
    if (typeof label !== "string" || values.has(label)) throw new InvalidResponse();
    const value: unknown = row[index];
    if (signedSwap) {
      if (typeof value !== "number" || !Number.isFinite(value)) throw new InvalidResponse();
      values.set(label, value);
    } else {
      values.set(label, finiteNonnegative(value));
    }
  }
  return { values, sampledAt: Math.min(sampledAt, lastEntry) };
}

function cpuUsage(values: ReadonlyMap<string, number>) {
  const mac = ["user", "nice", "system"];
  const linux = [...mac, "irq", "softirq", "guest", "guest_nice", "iowait", "steal"];
  // Netdata hides idle by default. Require a complete collector shape, allowing explicit idle.
  const dimensions = [...values.keys()].filter((dimension) => dimension !== "idle");
  if (
    ![mac, linux].some(
      (expected) =>
        dimensions.length === expected.length &&
        expected.every((dimension) => values.has(dimension)),
    )
  )
    throw new InvalidResponse();
  if ([...values.values()].some((value) => value > 100)) throw new InvalidResponse();
  // Linux Netdata already subtracts guest from user/nice; add guest exactly once.
  const busy = dimensions
    .filter((dimension) => dimension !== "iowait" && dimension !== "steal")
    .reduce((total, dimension) => total + values.get(dimension)!, 0);
  if (busy > 100) throw new InvalidResponse();
  return busy;
}

function storageCapacity(payload: unknown, guid: string, mountPoint: string, now: number) {
  const root = record(payload);
  // Detailed API metadata proves the result comes from one filesystem on the selected node.
  const nodes = record(record(root.detailed).nodes);
  if (Object.keys(nodes).length !== 1 || !Object.hasOwn(nodes, guid)) throw new InvalidResponse();
  const contexts = record(record(nodes[guid]).contexts);
  if (Object.keys(contexts).length !== 1 || !Object.hasOwn(contexts, "disk.space"))
    throw new InvalidResponse();
  const instances = Object.values(record(record(contexts["disk.space"]).instances));
  if (instances.length !== 1) throw new InvalidResponse();
  const instance = record(instances[0]);
  if (record(instance.labels).mount_point !== mountPoint) throw new InvalidResponse();
  const dimensions = record(instance.dimensions);
  const expected = ["avail", "used", "reserved_for_root"];
  if (Object.keys(dimensions).length !== expected.length) throw new InvalidResponse();
  let sampledAt = now;
  for (const name of expected) {
    const dimension = record(dimensions[name]);
    if (dimension.qr !== 1) throw new InvalidResponse();
    const lastEntry = finiteNonnegative(dimension.le) * 1000;
    if (!Number.isSafeInteger(lastEntry) || lastEntry > now + 5_000) throw new InvalidResponse();
    if (now - lastEntry > MAX_AGE_MS) throw new StaleResponse();
    sampledAt = Math.min(sampledAt, lastEntry);
  }
  const storage = metric(payload, "GiB", now);
  const available = storage.values.get("avail");
  const used = storage.values.get("used");
  // Netdata's group_by=dimension uses the display name for reserved_for_root.
  const reserved = storage.values.get("reserved for root");
  if (
    storage.values.size !== 3 ||
    available === undefined ||
    used === undefined ||
    reserved === undefined
  )
    throw new InvalidResponse();
  const availableStorageBytes = Math.round(available * 1024 ** 3);
  const totalStorageBytes = Math.round((available + used + reserved) * 1024 ** 3);
  if (
    !Number.isSafeInteger(availableStorageBytes) ||
    !Number.isSafeInteger(totalStorageBytes) ||
    totalStorageBytes <= 0 ||
    availableStorageBytes > totalStorageBytes
  )
    throw new InvalidResponse();
  return {
    availableStorageBytes,
    totalStorageBytes,
    storageMountPoint: mountPoint,
    sampledAt: Math.min(sampledAt, storage.sampledAt),
  };
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
    const getMetric = (context: string, dimension: string, swapAverage = false) =>
      request("data", {
        scope_nodes: node.guid,
        nodes: node.guid,
        contexts: context,
        dimensions: dimension,
        after: swapAverage ? "-10" : "-5",
        points: "1",
        ...(swapAverage ? { time_group: "average" } : {}),
        format: "json",
        options: "jsonwrap",
        group_by: "dimension",
      });
    const cpuRequest = getMetric("system.cpu", "*");
    // Start optional storage as soon as CPU identifies the platform, alongside the RAM reads.
    // Every attempt shares the existing host deadline; unavailable storage cannot reject CPU.
    const storageRequest = cpuRequest
      .then(async (payload) => {
        const isMac = !metric(payload, "percentage", now()).values.has("irq");
        const mountPoints = config.storageMountPoint
          ? [config.storageMountPoint]
          : isMac
            ? ["/System/Volumes/Data", "/"]
            : ["/"];
        for (const mountPoint of mountPoints) {
          // Netdata label selectors are patterns. Reject syntax that could broaden a mount.
          if (!/^\/[a-zA-Z0-9_./-]*$/.test(mountPoint)) return undefined;
          try {
            return storageCapacity(
              await request("data", {
                scope_nodes: node.guid,
                nodes: node.guid,
                scope_contexts: "disk.space",
                contexts: "disk.space",
                scope_labels: `mount_point:${mountPoint}`,
                labels: `mount_point:${mountPoint}`,
                dimensions: "*",
                after: "-5",
                points: "1",
                format: "json",
                options: "jsonwrap,details",
                group_by: "dimension",
              }),
              node.guid,
              mountPoint,
              now(),
            );
          } catch {
            // A Mac without a valid Data chart can use its exact root chart, never other mounts.
            if (controller.signal.aborted) break;
          }
        }
        return undefined;
      })
      .catch(() => undefined);
    const [cpuPayload, memoryPayload] = await Promise.all([
      cpuRequest,
      getMetric("system.ram", "*"),
    ]);
    const at = now();
    const cpu = metric(cpuPayload, "percentage", at);
    const cpuUsagePercent = cpuUsage(cpu.values);
    const memory = metric(memoryPayload, "MiB", at);
    const freeMiB = memory.values.get("free");
    if (freeMiB === undefined) throw new InvalidResponse();
    const freeMemoryBytes = Math.round(freeMiB * 1024 * 1024);
    if (!Number.isSafeInteger(freeMemoryBytes) || freeMemoryBytes > node.totalMemoryBytes)
      throw new InvalidResponse();
    let availableMemoryBytes: number | undefined;
    let availableSampledAt = Math.min(cpu.sampledAt, memory.sampledAt);
    try {
      const isMac = !cpu.values.has("irq");
      const available = isMac
        ? memory
        : metric(await getMetric("mem.available", "avail"), "MiB", now());
      // Match Netdata's macOS estimate: chart free excludes speculative pages.
      const dimensions = isMac ? ["free", "speculative", "inactive", "purgeable"] : ["avail"];
      if (dimensions.some((dimension) => !available.values.has(dimension)))
        throw new InvalidResponse();
      const bytes = Math.round(
        dimensions.reduce((sum, dimension) => sum + available.values.get(dimension)!, 0) *
          1024 ** 2,
      );
      if (!Number.isSafeInteger(bytes) || bytes > node.totalMemoryBytes)
        throw new InvalidResponse();
      availableMemoryBytes = bytes;
      availableSampledAt = Math.min(availableSampledAt, available.sampledAt);
    } catch {
      // Older collectors can lack available memory; retain CPU without mislabeling free RAM.
    }
    let swapIoBytesPerSecond: number | undefined;
    if (id === "mini" || id === "home") {
      try {
        const swap = metric(await getMetric("mem.swapio", "*", true), "KiB/s", now(), true);
        const input = swap.values.get("in");
        const output = swap.values.get("out");
        if (
          swap.values.size !== 2 ||
          input === undefined ||
          output === undefined ||
          input < 0 ||
          output > 0
        )
          throw new InvalidResponse();
        // Netdata plots swap-out below zero; subtract it to combine both KiB/s directions.
        const bytesPerSecond = (input - output) * 1024;
        if (!Number.isFinite(bytesPerSecond)) throw new InvalidResponse();
        swapIoBytesPerSecond = bytesPerSecond;
        availableSampledAt = Math.min(availableSampledAt, swap.sampledAt);
      } catch {
        // Missing or invalid optional swap data must not turn an unknown rate into zero.
      }
    }
    const storage = await storageRequest;
    if (storage) availableSampledAt = Math.min(availableSampledAt, storage.sampledAt);
    return {
      id,
      status: "available",
      cpuUsagePercent,
      logicalCpuCount: node.logicalCpuCount,
      occupiedMemoryBytes: node.totalMemoryBytes - freeMemoryBytes,
      ...(availableMemoryBytes === undefined ? {} : { availableMemoryBytes }),
      ...(storage === undefined
        ? {}
        : {
            availableStorageBytes: storage.availableStorageBytes,
            totalStorageBytes: storage.totalStorageBytes,
            storageMountPoint: storage.storageMountPoint,
          }),
      ...(swapIoBytesPerSecond === undefined ? {} : { swapIoBytesPerSecond }),
      totalMemoryBytes: node.totalMemoryBytes,
      sampledAt: DateTime.formatIso(DateTime.makeUnsafe(availableSampledAt)),
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
      ...(error instanceof CollectorDisconnected
        ? { diagnostic: "collector_disconnected" as const }
        : error instanceof StaleResponse
          ? { diagnostic: "sample_expired" as const }
          : error instanceof InvalidResponse
            ? {}
            : { diagnostic: "upstream_failure" as const }),
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
