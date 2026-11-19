// @effect-diagnostics nodeBuiltinImport:off -- Synthetic qualification reads only task-captured workers.
import * as NodeChildProcess from "node:child_process";
import * as NodeUtil from "node:util";
import {
  sampleNativePools,
  type MacNativePoolCensusFailure,
  type MacNativePoolCensusRejection,
  type NativePoolObservation,
} from "./resourceSamples.ts";

const execute = NodeUtil.promisify(NodeChildProcess.execFile);
const decoder = new TextDecoder("utf-8", { fatal: true });
const method = "macos-proc-info" as const;
const belongsToNativePool = (name: string) =>
  name.startsWith("fff-bg-") || name.startsWith("fff-search-");
const belongsToWatcher = (name: string) =>
  name === "fff-watcher-own" ||
  name === "notify-rs debouncer loop" ||
  name === "notify-rs fsevents loop";

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

function classifyReadFailure(cause: unknown): MacNativePoolCensusFailure {
  const error = cause as NodeChildProcess.ExecFileException | null | undefined;
  const code = error?.code;
  if (typeof code === "number" && Number.isInteger(code) && code >= 1 && code <= 255) {
    return { reason: "helper-exit", exitCode: code };
  }
  if (code === "ERR_CHILD_PROCESS_STDIO_MAXBUFFER") return { reason: "helper-output-limit" };
  // The execFile deadline is the only caller-initiated kill once maxBuffer is ruled out.
  if (error?.killed && error.signal === "SIGTERM") return { reason: "helper-timeout" };
  if (error?.signal) return { reason: "helper-signal" };
  return { reason: "helper-error" };
}

interface CensusRead {
  readonly identity: string | null;
  readonly observation: NativePoolObservation;
  readonly failure?: MacNativePoolCensusFailure;
}

export function parseMacNativePoolCensus(pid: number, raw: string): CensusRead {
  const invalid = (detail: MacNativePoolCensusRejection): CensusRead => ({
    identity: null,
    observation: unavailable(pid, raw),
    failure: { reason: "invalid-output", detail },
  });
  if (raw.length > 128 * 1024 || !raw.endsWith("\n")) return invalid("output-boundary");
  const [header, ...rows] = raw.slice(0, -1).split("\n");
  const start = /^([1-9][0-9]*)\t([1-9][0-9]*)\t([0-9]{1,6})$/.exec(header!);
  if (start?.[1] !== String(pid)) return invalid("process-identity");
  if (rows.length === 0 || rows.length >= 512) return invalid("thread-count");
  const threads = [];
  const identifiers = new Set<string>();
  for (const row of rows) {
    const match = /^([1-9a-f][0-9a-f]{0,15})\t((?:[0-9a-f]{2}){0,63})$/.exec(row);
    if (!match) return invalid("thread-row");
    const tid = match[1]!;
    if (identifiers.has(tid)) return invalid("duplicate-thread");
    identifiers.add(tid);
    let name: string;
    try {
      name = decoder.decode(Buffer.from(match[2]!, "hex")) || "<unnamed>";
    } catch {
      return invalid("name-encoding");
    }
    if (/[\u0000-\u001f\u007f]/.test(name)) return invalid("name-control");
    threads.push({ tid, name });
  }
  return {
    identity: header!,
    observation: { pid, method, status: "complete", threads, raw },
  };
}

const nativeNames = (observation: NativePoolObservation) =>
  observation.threads
    .map(({ name }) => name)
    .filter(belongsToNativePool)
    .sort();

function onlyWatchersStarted(before: NativePoolObservation, after: NativePoolObservation) {
  if (before.threads.length >= after.threads.length) return false;
  const originalNames = new Map(before.threads.map(({ tid, name }) => [tid, name]));
  let matched = 0;
  for (const { tid, name } of after.threads) {
    const original = originalNames.get(tid);
    if (original === name) matched++;
    else if (original !== undefined || !belongsToWatcher(name)) return false;
  }
  return matched === before.threads.length;
}

export function makeMacNativePoolCensus(
  executable: string,
  reference = sampleNativePools,
  read = async (pid: number) =>
    (await execute(executable, [String(pid)], { timeout: 2_000, maxBuffer: 128 * 1024 })).stdout,
) {
  const observe = async (pid: number): Promise<CensusRead> => {
    try {
      return parseMacNativePoolCensus(pid, await read(pid));
    } catch (cause) {
      return { identity: null, observation: unavailable(pid), failure: classifyReadFailure(cause) };
    }
  };
  const compare = async (pid: number, preceding?: CensusRead) => {
    const before = preceding ?? (await observe(pid));
    const referenceObservation = await reference(pid);
    const after = await observe(pid);
    const namedPools = nativeNames(referenceObservation);
    const censusPools = nativeNames(before.observation);
    const sameProcess = before.identity !== null && before.identity === after.identity;
    const unchanged = before.observation.raw === after.observation.raw;
    const watchersStarted = onlyWatchersStarted(before.observation, after.observation);
    const referenceBound =
      referenceObservation.pid === pid && referenceObservation.method === "macos-sample";
    const matchesReference =
      sameProcess &&
      referenceBound &&
      referenceObservation.status === "complete" &&
      namedPools.filter((name) => /^fff-bg-[0-9]+$/.test(name)).length >= 2 &&
      namedPools.some((name) => /^fff-search-[0-9]+$/.test(name)) &&
      censusPools.length === namedPools.length &&
      censusPools.every((name, index) => name === namedPools[index]);
    // Compare all native names and population sizes across the reference window.
    const verified =
      matchesReference &&
      unchanged &&
      before.observation.threads.length === referenceObservation.threads.length;
    const retryWatcherGrowth =
      matchesReference &&
      after.observation.threads.length === referenceObservation.threads.length &&
      watchersStarted;
    const retryUnavailableReference =
      sameProcess &&
      (unchanged || watchersStarted) &&
      referenceBound &&
      referenceObservation.status === "unavailable";
    return {
      after,
      referenceObservation,
      canRetry: retryWatcherGrowth || retryUnavailableReference,
      receipt: {
        pid,
        verified,
        nativeNames: namedPools,
        threads: referenceObservation.threads.length,
        referenceStatus: referenceObservation.status,
        ...(referenceObservation.error ? { referenceError: referenceObservation.error } : {}),
        before: before.observation.raw,
        after: after.observation.raw,
        ...(before.failure ? { beforeFailure: before.failure } : {}),
        ...(after.failure ? { afterFailure: after.failure } : {}),
      },
    };
  };
  const verify = async (pid: number) => {
    const initial = await compare(pid);
    if (!initial.canRetry) return initial;
    const final = await compare(pid, initial.after);
    // A failed read remains in generation evidence even if the retry admits later fast reads.
    const retainInitial =
      !final.receipt.verified || initial.referenceObservation.status === "unavailable";
    return {
      ...final,
      referenceObservation: retainInitial
        ? initial.referenceObservation
        : final.referenceObservation,
      receipt: {
        ...final.receipt,
        initial: { ...initial.receipt, reference: initial.referenceObservation.raw },
        ...(retainInitial ? { reference: final.referenceObservation.raw } : {}),
      },
    };
  };
  let comparison: ReturnType<typeof verify> | undefined;
  let receipt: Awaited<ReturnType<typeof verify>>["receipt"] | undefined;
  return {
    async sample(pid: number) {
      if (!comparison) {
        comparison = verify(pid);
        const result = await comparison;
        receipt = result.receipt;
        return result.referenceObservation;
      }
      if (!(await comparison).receipt.verified) return reference(pid);
      const { observation, failure } = await observe(pid);
      return failure ? { ...(await reference(pid)), fastCensusFailure: failure } : observation;
    },
    comparison: () => receipt ?? null,
  };
}
