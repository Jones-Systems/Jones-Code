// @effect-diagnostics nodeBuiltinImport:off - Owns synthetic SQLite files and captured cross-process fixture children.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as SqlClient from "effect/sql/SqlClient";
import * as Store from "./DesktopConnectionCompatibilityHintStore.ts";

const environmentId = EnvironmentId.make("hint-environment");
const protection = {
  isEncryptionAvailable: Effect.succeed(true),
  encryptString: (value: string) => Effect.succeed(new TextEncoder().encode(`fixture:${value}`)),
  decryptString: (value: Uint8Array) => Effect.succeed(new TextDecoder().decode(value).slice(8)),
};
const withStore = <A, E, R>(
  use: (
    store: Effect.Success<ReturnType<typeof Store.make>>,
    directory: string,
  ) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "connection-hints-" });
    const store = yield* Store.make({ directory, protection });
    return yield* use(store, directory);
  }).pipe(Effect.provide(NodeServices.layer), Effect.scoped);

describe("isolated desktop compatibility hints", () => {
  it.effect(
    "persists exact bindings, clears only the addressed binding, and never writes the catalog",
    () =>
      withStore((store, directory) =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const catalog = NodePath.join(directory, "connection-catalog.json");
          yield* fs.writeFileString(catalog, "saved-off-and-credentials");
          expect(yield* store.set(environmentId, "route-a", "outdated-update")).toBe(true);
          expect(yield* store.set(environmentId, "route-b", "outdated-update")).toBe(true);
          yield* store.set(environmentId, "route-a", null);
          expect(yield* store.read).toEqual([
            { environmentId, routeBinding: "route-b", kind: "outdated-update" },
          ]);
          expect(yield* fs.readFileString(catalog)).toBe("saved-off-and-credentials");
          const reopened = yield* Store.make({ directory, protection });
          expect(yield* reopened.read).toEqual(yield* store.read);
        }),
      ),
  );
  it.effect("bounds new keys but permits replacement and clear at capacity", () =>
    withStore((store) =>
      Effect.gen(function* () {
        for (let index = 0; index < 128; index++) {
          expect(yield* store.set(environmentId, `route-${index}`, "outdated-update")).toBe(true);
        }
        expect(yield* store.set(environmentId, "overflow", "outdated-update")).toBe(false);
        expect(yield* store.set(environmentId, "route-0", "outdated-update")).toBe(true);
        yield* store.set(environmentId, "route-0", null);
        expect(yield* store.set(environmentId, "overflow", "outdated-update")).toBe(true);
      }),
    ),
  );
  it.effect("does not create storage on an absent read or unavailable encryption", () =>
    withStore((store, directory) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        expect(yield* store.read).toEqual([]);
        const unavailable = yield* Store.make({
          directory,
          protection: { ...protection, isEncryptionAvailable: Effect.succeed(false) },
        });
        expect(yield* unavailable.set(environmentId, "route-a", "outdated-update")).toBe(false);
        expect(yield* fs.readDirectory(directory)).toEqual([]);
      }),
    ),
  );
  it.effect("rolls back a rejected transaction and ignores a malformed encrypted record", () =>
    withStore((store, directory) =>
      Effect.gen(function* () {
        yield* store.set(environmentId, "retained", "outdated-update");
        const filename = NodePath.join(directory, "connection-compatibility-hints-v1.sqlite");
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`CREATE TRIGGER reject_hint BEFORE INSERT ON compatibility_hints BEGIN SELECT RAISE(ABORT, 'fixture rejection'); END`;
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })), Effect.scoped);
        expect(
          (yield* Effect.result(store.set(environmentId, "rejected", "outdated-update")))._tag,
        ).toBe("Failure");
        expect(yield* store.read).toEqual([
          { environmentId, routeBinding: "retained", kind: "outdated-update" },
        ]);
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`UPDATE compatibility_hints SET encrypted_hint = 'not-base64'`;
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })), Effect.scoped);
        expect(yield* store.read).toEqual([]);
      }),
    ),
  );
  it.effect("preserves corrupt isolated bytes without attempting repair", () =>
    withStore((store, directory) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const file = NodePath.join(directory, "connection-compatibility-hints-v1.sqlite");
        yield* fs.writeFileString(file, "malformed");
        expect((yield* Effect.result(store.read))._tag).toBe("Failure");
        expect(yield* fs.readFileString(file)).toBe("malformed");
      }),
    ),
  );
});

it("serializes exact record operations across independent processes", async () => {
  const directory = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.tmpdir(), "connection-hints-process-"),
  );
  const children: ReturnType<typeof startChild>[] = [];
  let failure: unknown;
  try {
    const first = startChild(directory);
    children.push(first);
    const second = startChild(directory);
    children.push(second);
    await Promise.all(children.map((child) => child.ready));
    const writes = await Promise.all([
      first.call({
        operation: "set",
        environmentId,
        routeBinding: "route-a",
        kind: "outdated-update",
      }),
      second.call({
        operation: "set",
        environmentId,
        routeBinding: "route-b",
        kind: "outdated-update",
      }),
    ]);
    expect(writes).toEqual([true, true]);
    expect(await first.call({ operation: "read" })).toHaveLength(2);
    expect(
      await first.call({ operation: "set", environmentId, routeBinding: "route-a", kind: null }),
    ).toBe(true);
    expect(await second.call({ operation: "read" })).toEqual([
      { environmentId, routeBinding: "route-b", kind: "outdated-update" },
    ]);
    expect(await NodeFSP.readdir(directory)).not.toContain("connection-catalog.json");
  } catch (error) {
    failure = error;
  } finally {
    const stopped = await Promise.allSettled(children.map((child) => child.stop()));
    const unresolved = stopped.filter((result) => result.status === "rejected");
    if (unresolved.length > 0)
      throw new AggregateError(
        [...(failure === undefined ? [] : [failure]), ...unresolved.map((result) => result.reason)],
        `Child custody unresolved; retained ${directory}`,
      );
    try {
      await NodeFSP.rm(directory, { recursive: true });
    } catch (error) {
      throw new AggregateError(
        [...(failure === undefined ? [] : [failure]), error],
        `Could not remove owned fixture ${directory}`,
      );
    }
  }
  if (failure !== undefined) throw failure;
}, 60_000);

function startChild(directory: string) {
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

class FixtureCustodyError extends Error {
  constructor(
    readonly custody: FixtureCustody,
    readonly evidencePath: string,
  ) {
    super(`Could not close owned child; custody retained at ${evidencePath}`);
  }
}

function ownChild(child: NodeChildProcess.ChildProcess, directory: string) {
  const identity = {
    directory,
    pid: child.pid ?? null,
    executable: child.spawnfile,
    arguments: [...child.spawnargs],
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

it("requests child-side shutdown and requires close after exit before releasing custody", async () => {
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "hint-close-control-"));
  // No OS process exists; events below exercise the owner's actual shutdown state machine.
  const child = new NodeChildProcess.ChildProcess();
  let owner: ReturnType<typeof ownChild> | undefined;
  let failure: unknown;
  try {
    const send = vi.fn(() => true);
    const disconnect = vi.fn();
    const kill = vi.fn(() => true);
    Object.defineProperties(child, {
      pid: { value: 4243 },
      connected: { value: true },
      spawnfile: { value: "controlled-fixture" },
      spawnargs: { value: ["controlled-fixture", directory] },
      send: { value: send },
      disconnect: { value: disconnect },
      kill: { value: kill },
    });
    owner = ownChild(child, directory);
    child.emit("message", { ready: true });
    await owner.ready;
    let finished = false;
    const stopping = owner.stop().then((result) => {
      finished = true;
      return result;
    });
    expect(send).toHaveBeenCalledExactlyOnceWith({ operation: "shutdown" }, expect.any(Function));
    expect(disconnect).not.toHaveBeenCalled();
    child.emit("disconnect");
    child.emit("exit", 0, null);
    // Drain the complete promise chain before testing that exit alone cannot settle stop.
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(finished).toBe(false);
    child.emit("close", 0, null);
    expect(await stopping).toEqual({ status: "closed" });
    expect(kill).not.toHaveBeenCalled();
    expect(disconnect).not.toHaveBeenCalled();
    expect(await NodeFSP.readdir(directory)).toEqual([]);
  } catch (error) {
    failure = error;
  } finally {
    const cleanupErrors: unknown[] = [];
    try {
      child.emit("close", 0, null);
      await owner?.stop();
    } catch (error) {
      cleanupErrors.push(error);
    }
    child.unref();
    child.removeAllListeners();
    try {
      await NodeFSP.rm(directory, { recursive: true });
    } catch (error) {
      cleanupErrors.push(error);
    }
    if (cleanupErrors.length > 0)
      throw new AggregateError(
        [...(failure === undefined ? [] : [failure]), ...cleanupErrors],
        `Could not release controlled fixture ${directory}`,
      );
  }
  if (failure !== undefined) throw failure;
  await expect(NodeFSP.stat(directory)).rejects.toMatchObject({ code: "ENOENT" });
});

it.each(["declined", "throws"] as const)(
  "retains custody and releases owned refs when signaling %s and close never arrives",
  async (signalOutcome) => {
    const directory = await NodeFSP.mkdtemp(
      NodePath.join(NodeOS.tmpdir(), "hint-custody-control-"),
    );
    const cleanup: (() => void)[] = [];
    let failure: unknown;
    try {
      // An unspawned handle has no OS process to abandon; every effect below is controlled.
      const child = new NodeChildProcess.ChildProcess();
      cleanup.push(
        () => child.unref(),
        () => child.removeAllListeners(),
      );
      const channel = { unref: vi.fn() };
      const disconnect = vi.fn(() => {
        throw new Error("controlled disconnect refusal");
      });
      const send = vi.fn(() => true);
      Object.defineProperties(child, {
        pid: { value: 4242 },
        connected: { value: true },
        channel: { value: channel },
        spawnfile: { value: "controlled-fixture" },
        spawnargs: { value: ["controlled-fixture", directory] },
        // IPC methods exist only after Node's setupChannel; this handle was never spawned.
        disconnect: { value: disconnect },
        send: { value: send },
      });
      const kill = vi.spyOn(child, "kill").mockImplementation(() => {
        if (signalOutcome === "throws") throw new Error("controlled signal refusal");
        return false;
      });
      cleanup.push(() => kill.mockRestore());
      const unref = vi.spyOn(child, "unref").mockImplementation(() => undefined);
      cleanup.push(() => unref.mockRestore());
      vi.useFakeTimers();
      cleanup.push(() => {
        vi.clearAllTimers();
        vi.useRealTimers();
      });
      const owner = ownChild(child, directory);
      const result = owner.stop().then(
        () => undefined,
        (error: unknown) => error,
      );
      expect(send).toHaveBeenCalledWith({ operation: "shutdown" }, expect.any(Function));
      expect(disconnect).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(999);
      expect(kill).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      expect(kill).toHaveBeenCalledExactlyOnceWith("SIGKILL");
      expect(unref).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(4_000);
      const error = await result;
      expect(error).toBeInstanceOf(FixtureCustodyError);
      if (!(error instanceof FixtureCustodyError)) throw error;
      expect(error.custody).toMatchObject({
        status: "unresolved",
        directory,
        pid: 4242,
        executable: "controlled-fixture",
        arguments: ["controlled-fixture", directory],
        closeObserved: false,
        exitCode: null,
        signalCode: null,
        deadlineMs: 5000,
      });
      expect(disconnect).toHaveBeenCalledExactlyOnceWith();
      expect(error.custody.errors).toContain("Error: controlled disconnect refusal");
      expect(error.custody.errors).toContain(
        signalOutcome === "throws"
          ? "Error: controlled signal refusal"
          : "Error: Owned child SIGKILL was not delivered",
      );
      expect(JSON.parse(await NodeFSP.readFile(error.evidencePath, "utf8"))).toEqual(error.custody);
      expect(unref).toHaveBeenCalledExactlyOnceWith();
      expect(channel.unref).toHaveBeenCalledExactlyOnceWith();
      expect(() => child.emit("error", new Error("late signal failure"))).not.toThrow();
      expect(() => child.emit("error", new Error("second late signal failure"))).not.toThrow();
      expect(vi.getTimerCount()).toBe(0);
      await expect(owner.stop()).rejects.toBe(error);
    } catch (error) {
      failure = error;
    } finally {
      const cleanupErrors: unknown[] = [];
      for (const release of cleanup.reverse()) {
        try {
          release();
        } catch (error) {
          cleanupErrors.push(error);
        }
      }
      try {
        await NodeFSP.rm(directory, { recursive: true });
      } catch (error) {
        cleanupErrors.push(error);
      }
      if (cleanupErrors.length > 0)
        throw new AggregateError(
          [failure, ...cleanupErrors],
          `Could not release controlled fixture ${directory}`,
        );
    }
    if (failure !== undefined) throw failure;
    await expect(NodeFSP.stat(directory)).rejects.toMatchObject({ code: "ENOENT" });
  },
);
