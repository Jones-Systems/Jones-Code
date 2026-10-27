// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import {
  isPreparationJobId,
  readBoundedJson,
  writeDurableJson,
  type PreparationJob,
} from "./protocol.ts";

type Identity = { readonly pid: number; readonly processGroup: number };
type Launch = {
  readonly schema: 1;
  readonly jobId: string;
  readonly kind: PreparationJob["kind"];
  readonly state: "spawning" | "started" | "released" | "not-started";
  readonly worker?: Identity;
};
type Effects = {
  readonly schema: 1;
  readonly jobId: string;
  readonly pid: number;
  readonly phase: "transfer" | "native-effects" | "clean";
};
type PreparationRelease =
  | "released"
  | "not-started"
  | "unreaped"
  | "cleanup-unconfirmed"
  | "unknown";
const LIMIT = 4096;
const identity = (value: unknown): value is Identity => {
  if (typeof value !== "object" || value === null) return false;
  const r = value as Record<string, unknown>;
  return (
    typeof r.pid === "number" &&
    Number.isSafeInteger(r.pid) &&
    r.pid > 1 &&
    r.processGroup === r.pid
  );
};
const absent = (pid: number) => {
  try {
    process.kill(pid, 0);
    return false;
  } catch (cause) {
    return (cause as NodeJS.ErrnoException).code === "ESRCH";
  }
};

export async function preparationLifecycleJobIds(baseDir: string): Promise<ReadonlyArray<string>> {
  const directory = NodePath.join(baseDir, "runtime", "jones-preparation", "lifecycle");
  const names = await NodeFSP.readdir(directory).catch((cause: unknown) => {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw cause;
  });
  const jobs = await NodeFSP.readdir(
    NodePath.join(baseDir, "runtime", "jones-preparation", "jobs"),
  ).catch((cause: unknown) => {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw cause;
  });
  return [
    ...new Set([
      ...names
        .filter((name) => name.endsWith(".json") && isPreparationJobId(name.slice(0, -5)))
        .map((name) => name.slice(0, -5)),
      ...jobs.filter(isPreparationJobId),
    ]),
  ];
}

/** Lifecycle evidence outlives scratch deletion. Parent and worker have separate single-writer files. */
export function preparationLifecycle(baseDir: string, jobId: string) {
  if (!isPreparationJobId(jobId)) throw new Error("Invalid preparation lifecycle ID.");
  const directory = NodePath.join(baseDir, "runtime", "jones-preparation", "lifecycle");
  const launchFile = NodePath.join(directory, `${jobId}.json`);
  const effectFile = NodePath.join(directory, `${jobId}.effects.json`);
  const save = async (file: string, value: Launch | Effects) => {
    await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
    await writeDurableJson(file, value, LIMIT);
  };
  const launch = (value: Omit<Launch, "schema" | "jobId">) =>
    save(launchFile, { schema: 1, jobId, ...value });
  return {
    launch,
    reserve: async (kind: PreparationJob["kind"]) => {
      await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
      const handle = await NodeFSP.open(launchFile, "wx", 0o600);
      try {
        await handle.writeFile(
          `${JSON.stringify({ schema: 1, jobId, kind, state: "not-started" })}\n`,
        );
        await handle.sync();
      } finally {
        await handle.close();
      }
      const parent = await NodeFSP.open(directory, "r");
      try {
        await parent.sync();
      } finally {
        await parent.close();
      }
    },
    effects: (pid: number, phase: Effects["phase"]) =>
      save(effectFile, { schema: 1, jobId, pid, phase }),
    release: async (
      gone: (worker: Identity) => boolean = (worker) =>
        absent(worker.pid) && absent(-worker.processGroup),
    ): Promise<PreparationRelease> => {
      try {
        const raw = (await readBoundedJson(launchFile, LIMIT)) as Launch;
        if (
          raw.schema !== 1 ||
          raw.jobId !== jobId ||
          !["stage", "verify", "verify-runtime", "sweep"].includes(raw.kind)
        )
          return "unknown";
        if (raw.state === "not-started") return raw.state;
        if (raw.state === "released") return identity(raw.worker) ? "released" : "unknown";
        if (raw.state !== "spawning" && raw.state !== "started") return "unknown";
        const effects = (await readBoundedJson(effectFile, LIMIT).catch(() => undefined)) as
          | Effects
          | undefined;
        const boundEffects =
          effects?.schema === 1 &&
          effects.jobId === jobId &&
          identity({ pid: effects.pid, processGroup: effects.pid })
            ? effects
            : undefined;
        const worker = identity(raw.worker)
          ? raw.worker
          : boundEffects === undefined
            ? undefined
            : { pid: boundEffects.pid, processGroup: boundEffects.pid };
        if (worker === undefined || (boundEffects !== undefined && boundEffects.pid !== worker.pid))
          return "unknown";
        if (!gone(worker)) return "unreaped";
        if (
          raw.kind === "stage" &&
          (boundEffects === undefined || !["transfer", "clean"].includes(boundEffects.phase))
        )
          return "cleanup-unconfirmed";
        // Only the parent/job-lock owner calls release after the entire captured group is absent.
        await launch({ kind: raw.kind, state: "released", worker });
        return "released";
      } catch {
        return "unknown";
      }
    },
  };
}
