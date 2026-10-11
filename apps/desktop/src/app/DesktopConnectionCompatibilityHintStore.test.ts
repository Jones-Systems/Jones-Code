// @effect-diagnostics nodeBuiltinImport:off - Owns synthetic SQLite files and captured cross-process fixture children.
import * as NodeChildProcess from "node:child_process";
import * as NodeFs from "node:fs/promises";
import * as NodeOs from "node:os";
import * as NodePath from "node:path";
import * as NodeUrl from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
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
  const directory = await NodeFs.mkdtemp(
    NodePath.join(NodeOs.tmpdir(), "connection-hints-process-"),
  );
  const children: ReturnType<typeof startChild>[] = [];
  let failure: unknown;
  try {
    const first = startChild(directory);
    children.push(first);
    const second = startChild(directory);
    children.push(second);
    await Promise.all(children.map((child) => child.ready));
    await Promise.all([
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
    expect(await first.call({ operation: "read" })).toHaveLength(2);
    await first.call({ operation: "set", environmentId, routeBinding: "route-a", kind: null });
    expect(await second.call({ operation: "read" })).toEqual([
      { environmentId, routeBinding: "route-b", kind: "outdated-update" },
    ]);
    expect(await NodeFs.readdir(directory)).not.toContain("connection-catalog.json");
  } catch (error) {
    failure = error;
  } finally {
    const stopped = await Promise.allSettled(children.map((child) => child.stop()));
    const unresolved = stopped.find((result) => result.status === "rejected");
    if (unresolved)
      throw new AggregateError(
        [failure, unresolved.reason],
        `Child custody unresolved; retained ${directory}`,
      );
    try {
      await NodeFs.rm(directory, { recursive: true });
    } catch (error) {
      throw new AggregateError([failure, error], `Could not remove owned fixture ${directory}`);
    }
  }
  if (failure !== undefined) throw failure;
}, 60_000);

function startChild(directory: string) {
  const child = NodeChildProcess.fork(
    NodeUrl.fileURLToPath(
      new URL("./DesktopConnectionCompatibilityHintStore.fixture.ts", import.meta.url),
    ),
    [directory],
    {
      execArgv: ["--experimental-strip-types"],
      stdio: ["ignore", "ignore", "inherit", "ipc"],
    },
  );
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
  const closed = new Promise<void>((resolve) => child.once("close", () => resolve()));
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
    clearTimeout(readyTimer);
    readyReject(error);
    for (const call of pending.values()) {
      clearTimeout(call.timer);
      call.reject(error);
    }
    pending.clear();
  };
  child.once("error", fail);
  child.once("exit", (code, signal) => fail(new Error(`Fixture exited: ${code}/${signal}`)));
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
        child.send({ ...command, id: current }, (error) => {
          if (error) fail(error);
        });
      }),
    stop: async () => {
      if (child.pid === undefined) return;
      if (child.connected) child.disconnect();
      const timer = setTimeout(() => {
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      }, 1_000);
      let deadline: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          closed,
          new Promise<never>((_, reject) => {
            deadline = setTimeout(() => reject(new Error("Could not close owned child")), 5_000);
          }),
        ]);
      } finally {
        clearTimeout(timer);
        clearTimeout(deadline);
      }
    },
  };
}
