// @effect-diagnostics nodeBuiltinImport:off -- Owns only captured synthetic collector processes and readiness observations.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";
import { monotonicSeconds, runBoundCollector } from "./collector.ts";

export async function closeOwnedFixtureGroups(owner: string, directory: string, assertions: {
  verify: (fields: string[], startTicks: string, metadata: { uid: number }, command: string, directory: string, pgid: number) => void;
  closed: (exists: (pgid: number) => boolean, pgid: number) => void;
}) {
  const trace = await NodeFSP.readFile(NodePath.join(owner, "trace.fixture"), "utf8").catch(
    (cause: unknown) => {
      if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return "";
      throw cause;
    },
  );
  const groups = new Map<number, string>();
  for (const line of trace.trim().split("\n").filter(Boolean)) {
    const entry = JSON.parse(line);
    if (entry.event === "probe" || entry.event === "usage_start")
      groups.set(entry.pgid, entry.start_ticks);
  }
  const exists = (pgid: number) => {
    try {
      process.kill(-pgid, 0);
      return true;
    } catch (cause) {
      if (cause instanceof Error && "code" in cause && cause.code === "ESRCH") return false;
      throw cause;
    }
  };
  for (const [pgid, startTicks] of groups) {
    if (!exists(pgid)) continue;
    const verify = async () => {
      const processDirectory = `/proc/${pgid}`;
      const evidence = await Promise.all([
        NodeFSP.readFile(`${processDirectory}/stat`, "utf8"),
        NodeFSP.readFile(`${processDirectory}/cmdline`, "utf8"),
        NodeFSP.stat(processDirectory),
      ]).catch((cause: unknown) => {
        if (cause instanceof Error && "code" in cause && cause.code === "ENOENT" && !exists(pgid))
          return null;
        throw cause;
      });
      if (evidence === null) return false;
      const [stat, command, metadata] = evidence;
      const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
      assertions.verify(fields, startTicks, metadata, command, directory, pgid);
      return true;
    };
    if (!(await verify())) continue;
    process.kill(-pgid, "SIGTERM");
    const grace = monotonicSeconds() + 1.5;
    while (exists(pgid) && monotonicSeconds() < grace) await NodeTimersPromises.setTimeout(25);
    if (exists(pgid) && (await verify())) process.kill(-pgid, "SIGKILL");
    const reap = monotonicSeconds() + 1;
    while (exists(pgid) && monotonicSeconds() < reap) await NodeTimersPromises.setTimeout(25);
    assertions.closed(exists, pgid);
  }
}

export async function observePublishedOperation(scratch: string, childPid: number, first: Promise<string>, nativeDeadline: number) {
  const publicationWait = new AbortController();
  let publication: Promise<unknown> | undefined;
  try {
          publication = (async () => {
            while (!publicationWait.signal.aborted) {
              const operation = JSON.parse(
                await NodeFSP.readFile(NodePath.join(scratch, "state/malcolm.json"), "utf8"),
              ).operation;
              if (
                operation?.pid === childPid &&
                operation.pgid === childPid &&
                operation.account === "malcolm"
              )
                return operation;
              await NodeTimersPromises.setTimeout(20, undefined, {
                signal: publicationWait.signal,
              });
            }
            throw new Error("fixture operation publication wait cancelled");
          })();
          return await Promise.race([
            publication,
            first.then(() => {
              throw new Error("collector finished before child operation was published");
            }),
            NodeTimersPromises.setTimeout(
              Math.max(0, (nativeDeadline - monotonicSeconds()) * 1000),
              undefined,
              { signal: publicationWait.signal },
            ).then(() => {
              throw new Error("child operation was not published before the collector deadline");
            }),
          ]);
  } finally {
    publicationWait.abort();
    await publication?.catch(() => {});
  }
}

// The delay consumes the caller's existing absolute budget before native startup.
export async function startDelayedCollector(...args: Parameters<typeof runBoundCollector>) {
  await NodeTimersPromises.setTimeout(100);
  return runBoundCollector(...args);
}

export function ownProbeReceipt(scratch: string) {
  const receiptWait = new AbortController();
  let watcher: ReturnType<typeof NodeFS.watch> | undefined;
  const observed = new Promise<boolean>((resolve) => {
    watcher = NodeFS.watch(scratch, (_event, file) => {
      if (file === "probe-ready.fixture") {
        watcher!.close();
        resolve(true);
      }
    });
  });
  return {
    observe: () => Promise.race([
      observed,
      NodeTimersPromises.setTimeout(2_000, false, { signal: receiptWait.signal }),
    ]),
    close() { receiptWait.abort(); watcher?.close(); },
  };
}
