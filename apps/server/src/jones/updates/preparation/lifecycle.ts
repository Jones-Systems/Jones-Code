// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
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

/** Only immutable release evidence may bypass the job lock. Never cache an unresolved read. */
export async function readTerminalRelease(baseDir: string, jobId: string): Promise<boolean> {
  if (!isPreparationJobId(jobId)) return false;
  const file = NodePath.join(baseDir, "runtime", "jones-preparation", "lifecycle", `${jobId}.json`);
  try {
    // NONBLOCK also prevents a substituted FIFO from blocking before fstat rejects it.
    const fd = await NodeFSP.open(
      file,
      NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
    );
    try {
      const stat = await fd.stat();
      if (
        !stat.isFile() ||
        (stat.mode & 0o022) !== 0 ||
        stat.size > LIMIT ||
        (typeof process.getuid === "function" && stat.uid !== process.getuid())
      )
        return false;
      const bytes = Buffer.alloc(LIMIT + 1);
      let length = 0;
      while (length < bytes.length) {
        const read = await fd.read(bytes, length, bytes.length - length, length);
        if (read.bytesRead === 0) break;
        length += read.bytesRead;
      }
      if (length > LIMIT) return false;
      const raw: unknown = JSON.parse(bytes.subarray(0, length).toString("utf8"));
      if (typeof raw !== "object" || raw === null) return false;
      const r = raw as Record<string, unknown>;
      return (
        r.schema === 1 &&
        r.jobId === jobId &&
        ["stage", "verify", "verify-runtime", "sweep"].includes(r.kind as string) &&
        r.state === "released" &&
        identity(r.worker)
      );
    } finally {
      await fd.close();
    }
  } catch {
    return false;
  }
}

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
  // Order the asynchronous identity write before release, including a very short-lived child.
  let tail: Promise<unknown> = Promise.resolve();
  const ordered = <A>(operation: () => Promise<A>): Promise<A> => {
    const next = tail.then(operation);
    tail = next.catch(() => undefined);
    return next;
  };
  return {
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
      // This capability exists only after the exclusive reservation is durable. Its caller owns
      // the job lock throughout launch and release; a released ID can never be reserved again.
      const launch = (state: "spawning" | "started" | "not-started", worker?: Identity) =>
        ordered(async () => {
          const current = (await readBoundedJson(launchFile, LIMIT)) as Launch;
          if (
            current.schema !== 1 ||
            current.jobId !== jobId ||
            current.kind !== kind ||
            !["not-started", "spawning", "started"].includes(current.state)
          )
            throw new Error("Preparation launch evidence cannot be replaced.");
          await save(launchFile, {
            schema: 1,
            jobId,
            kind,
            state,
            ...(worker === undefined ? {} : { worker }),
          });
        });
      return {
        spawning: () => launch("spawning"),
        started: (worker: Identity) => launch("started", worker),
        notStarted: () => launch("not-started"),
      };
    },
    effects: (pid: number, phase: Effects["phase"]) =>
      save(effectFile, { schema: 1, jobId, pid, phase }),
    release: (
      gone: (worker: Identity) => boolean = (worker) =>
        absent(worker.pid) && absent(-worker.processGroup),
    ): Promise<PreparationRelease> =>
      ordered(async () => {
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
          await save(launchFile, { schema: 1, jobId, kind: raw.kind, state: "released", worker });
          return "released";
        } catch {
          return "unknown";
        }
      }),
  };
}
