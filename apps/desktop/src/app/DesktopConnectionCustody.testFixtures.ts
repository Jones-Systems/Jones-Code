// @effect-diagnostics nodeBuiltinImport:off - Owns captured synthetic children and retained custody evidence.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

export function startChild(directory: string) {
  const child = NodeChildProcess.fork(
    NodeURL.fileURLToPath(
      new URL("./DesktopConnectionCompatibilityHintStore.fixture.ts", import.meta.url),
    ),
    [directory],
    {
      execArgv: ["--experimental-strip-types"],
      stdio: ["ignore", "ignore", "inherit", "ipc"],
    },
  );
  return ownChild(child, directory);
}

type FixtureCustody = {
  status: "unresolved";
  directory: string;
  pid: number | null;
  executable: string;
  arguments: string[];
  capturedAt: string;
  closeObserved: false;
  exitCode: number | null;
  signalCode: NodeJS.Signals | null;
  deadlineMs: 5000;
  errors: string[];
};

export class FixtureCustodyError extends Error {
  readonly custody: FixtureCustody;
  readonly evidencePath: string;

  constructor(custody: FixtureCustody, evidencePath: string) {
    super(`Could not close owned child; custody retained at ${evidencePath}`);
    this.custody = custody;
    this.evidencePath = evidencePath;
  }
}

export function ownChild(child: NodeChildProcess.ChildProcess, directory: string) {
  const identity = {
    directory,
    pid: child.pid ?? null,
    executable: child.spawnfile,
    arguments: [...child.spawnargs],
    // @effect-diagnostics-next-line globalDate:off -- Capture native child custody wall time outside an Effect runtime; the controlled fixture uses the same fake clock.
    capturedAt: new Date().toISOString(),
  };
  const errors: string[] = [];
  const remember = (error: unknown) => {
    if (errors.length < 16) errors.push(String(error).slice(0, 1024));
  };
  let id = 0;
  const pending = new Map<
    number,
    {
      resolve: (value: unknown) => void;
      reject: (error: Error) => void;
      timer: ReturnType<typeof setTimeout>;
    }
  >();
  let readyResolve: () => void;
  let readyReject: (error: Error) => void;
  const ready = new Promise<void>((resolve, reject) => {
    readyResolve = resolve;
    readyReject = reject;
  });
  // @effect-diagnostics-next-line globalTimers:off -- This native IPC readiness deadline shares the fixture's fake timers and is cleared on readiness, failure or exit.
  const readyTimer = setTimeout(() => readyReject(new Error("Fixture readiness timed out")), 8_000);
  let closeObserved = false;
  const closed = new Promise<void>((resolve) =>
    child.once("close", () => {
      closeObserved = true;
      resolve();
    }),
  );
  void ready.catch(() => undefined);
  child.on(
    "message",
    (message: { ready?: boolean; id?: number; ok?: boolean; result?: unknown; error?: string }) => {
      if (message.ready) {
        clearTimeout(readyTimer);
        readyResolve();
        return;
      }
      const call = pending.get(message.id!);
      if (!call) return;
      clearTimeout(call.timer);
      pending.delete(message.id!);
      if (message.ok) call.resolve(message.result);
      else call.reject(new Error(message.error));
    },
  );
  const fail = (error: Error) => {
    remember(error);
    clearTimeout(readyTimer);
    readyReject(error);
    for (const call of pending.values()) {
      clearTimeout(call.timer);
      call.reject(error);
    }
    pending.clear();
  };
  // Keep the listener after a custody timeout: kill/disconnect can report later errors.
  child.on("error", fail);
  child.once("exit", (code, signal) => fail(new Error(`Fixture exited: ${code}/${signal}`)));
  let stopping: Promise<{ status: "closed" }> | undefined;
  const stop = async (): Promise<{ status: "closed" }> => {
    const channel = child.channel;
    try {
      // Node 24's parent-initiated IPC disconnect can omit the child's close event.
      // Keep our read side open: the child drains requests, disconnects, then exits.
      if (child.connected)
        child.send({ operation: "shutdown" }, (error) => {
          if (error) remember(error);
        });
    } catch (error) {
      remember(error);
    }
    // @effect-diagnostics-next-line globalTimers:off -- Escalation belongs to the captured native child; controlled custody tests advance this same timer.
    const timer = setTimeout(() => {
      if (closeObserved || child.exitCode !== null || child.signalCode !== null) return;
      try {
        if (!child.kill("SIGKILL")) remember(new Error("Owned child SIGKILL was not delivered"));
      } catch (error) {
        remember(error);
      }
    }, 1_000);
    let deadline: ReturnType<typeof setTimeout> | undefined;
    try {
      const outcome = await Promise.race([
        closed.then(() => "closed" as const),
        new Promise<"deadline">((resolve) => {
          // @effect-diagnostics-next-line globalTimers:off -- Bound native close observation without treating deadline expiry as reaping or granting scratch cleanup.
          deadline = setTimeout(() => resolve("deadline"), 5_000);
        }),
      ]);
      if (outcome === "closed") return { status: "closed" };
      fail(new Error("Owned child close was not observed before the five-second deadline"));
      try {
        if (child.connected) child.disconnect();
      } catch (error) {
        remember(error);
      }
      // This fork has no owned stdio pipes (ignore/ignore/inherit/ipc). Release only
      // its process and IPC refs; neither action proves exit or permits scratch removal.
      for (const handle of new Set([child, channel, child.channel])) {
        try {
          handle?.unref();
        } catch (error) {
          remember(error);
        }
      }
      const custody: FixtureCustody = {
        ...identity,
        status: "unresolved",
        closeObserved: false,
        exitCode: child.exitCode,
        signalCode: child.signalCode,
        deadlineMs: 5000,
        errors: [...errors],
      };
      const evidencePath = NodePath.join(
        directory,
        `child-${identity.pid ?? "unspawned"}-custody.json`,
      );
      const unresolved = new FixtureCustodyError(custody, evidencePath);
      try {
        await NodeFSP.writeFile(evidencePath, JSON.stringify(custody, null, 2) + "\n", {
          flag: "wx",
          mode: 0o600,
        });
      } catch (error) {
        throw new AggregateError([unresolved, error], "Could not persist unresolved child custody");
      }
      throw unresolved;
    } finally {
      clearTimeout(timer);
      clearTimeout(deadline);
    }
  };
  return {
    ready,
    call: (command: object) =>
      new Promise<unknown>((resolve, reject) => {
        const current = ++id;
        // @effect-diagnostics-next-line globalTimers:off -- Native IPC calls use the fixture's controllable timer queue and clear each deadline on settlement.
        const timer = setTimeout(() => {
          pending.delete(current);
          reject(new Error("Fixture operation timed out"));
        }, 8_000);
        pending.set(current, { resolve, reject, timer });
        try {
          child.send({ ...command, id: current }, (error) => {
            if (error) fail(error);
          });
        } catch (error) {
          fail(error instanceof Error ? error : new Error(String(error)));
        }
      }),
    stop: () => (stopping ??= stop()),
  };
}
