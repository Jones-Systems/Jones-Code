// @effect-diagnostics nodeBuiltinImport:off -- Synthetic qualification reads only task-captured workers.
import * as NodeChildProcess from "node:child_process";
import * as NodeUtil from "node:util";
import { sampleNativePools, type NativePoolObservation } from "./resourceSamples.ts";

const execute = NodeUtil.promisify(NodeChildProcess.execFile);
const decoder = new TextDecoder("utf-8", { fatal: true });
const method = "macos-proc-info" as const;
const belongsToNativePool = (name: string) =>
  name.startsWith("fff-bg-") || name.startsWith("fff-search-");

function unavailable(pid: number, raw = ""): NativePoolObservation {
  return {
    pid,
    method,
    status: "unavailable",
    threads: [],
    raw,
    error: "macos-census-unavailable",
  };
}

export function parseMacNativePoolCensus(pid: number, raw: string) {
  const invalid = () => ({ identity: null, observation: unavailable(pid, raw) });
  if (raw.length > 128 * 1024 || !raw.endsWith("\n")) return invalid();
  const [header, ...rows] = raw.slice(0, -1).split("\n");
  const start = /^([1-9][0-9]*)\t([1-9][0-9]*)\t([0-9]{1,6})$/.exec(header!);
  if (start?.[1] !== String(pid) || rows.length === 0 || rows.length >= 512) return invalid();
  const threads = [];
  const identifiers = new Set<string>();
  for (const row of rows) {
    const match = /^([1-9a-f][0-9a-f]{0,15})\t((?:[0-9a-f]{2}){0,63})$/.exec(row);
    if (!match || identifiers.has(match[1]!)) return invalid();
    const [, tid, bytes] = match;
    identifiers.add(tid!);
    let name: string;
    try {
      name = decoder.decode(Buffer.from(bytes!, "hex")) || "<unnamed>";
    } catch {
      return invalid();
    }
    if (/[\u0000-\u001f\u007f]/.test(name)) return invalid();
    threads.push({ tid: tid!, name });
  }
  return {
    identity: header!,
    observation: { pid, method, status: "complete", threads, raw } satisfies NativePoolObservation,
  };
}

const nativeNames = (observation: NativePoolObservation) =>
  observation.threads
    .map(({ name }) => name)
    .filter(belongsToNativePool)
    .sort();

export function makeMacNativePoolCensus(
  executable: string,
  reference = sampleNativePools,
  read = async (pid: number) =>
    (await execute(executable, [String(pid)], { timeout: 2_000, maxBuffer: 128 * 1024 })).stdout,
) {
  const observe = async (pid: number) => {
    try {
      return parseMacNativePoolCensus(pid, await read(pid));
    } catch {
      return { identity: null, observation: unavailable(pid) };
    }
  };
  const compare = async (pid: number) => {
    const before = await observe(pid);
    const referenceObservation = await reference(pid);
    const after = await observe(pid);
    const namedPools = nativeNames(referenceObservation);
    const censusPools = nativeNames(before.observation);
    // The reference uses kernel thread IDs and the census uses opaque handles.
    // Compare all native names and population sizes across the reference window.
    const verified =
      before.identity !== null &&
      before.identity === after.identity &&
      before.observation.raw === after.observation.raw &&
      referenceObservation.pid === pid &&
      referenceObservation.method === "macos-sample" &&
      referenceObservation.status === "complete" &&
      before.observation.threads.length === referenceObservation.threads.length &&
      namedPools.filter((name) => /^fff-bg-[0-9]+$/.test(name)).length >= 2 &&
      namedPools.some((name) => /^fff-search-[0-9]+$/.test(name)) &&
      censusPools.length === namedPools.length &&
      censusPools.every((name, index) => name === namedPools[index]);
    return {
      referenceObservation,
      receipt: {
        pid,
        verified,
        nativeNames: namedPools,
        threads: referenceObservation.threads.length,
        before: before.observation.raw,
        after: after.observation.raw,
      },
    };
  };
  let comparison: ReturnType<typeof compare> | undefined;
  let receipt: Awaited<ReturnType<typeof compare>>["receipt"] | undefined;
  return {
    async sample(pid: number) {
      if (!comparison) {
        comparison = compare(pid);
        const result = await comparison;
        receipt = result.receipt;
        return result.referenceObservation;
      }
      if (!(await comparison).receipt.verified) return reference(pid);
      const { observation } = await observe(pid);
      return observation.status === "complete" ? observation : reference(pid);
    },
    comparison: () => receipt ?? null,
  };
}
