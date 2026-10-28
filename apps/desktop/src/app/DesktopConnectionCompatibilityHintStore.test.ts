// @effect-diagnostics nodeBuiltinImport:off - Owns synthetic SQLite files and captured cross-process fixture children.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { afterEach, vi } from "vite-plus/test";
import {
  startChild,
  ownChild,
  FixtureCustodyError,
} from "./DesktopConnectionCustody.testFixtures.ts";

import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as SqlClient from "effect/sql/SqlClient";
import * as Store from "./DesktopConnectionCompatibilityHintStore.ts";

afterEach(() => vi.useRealTimers());

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
});

it("requests child-side shutdown and requires close after exit before releasing custody", async () => {
  vi.useFakeTimers();
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
    await vi.advanceTimersByTimeAsync(0);
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
