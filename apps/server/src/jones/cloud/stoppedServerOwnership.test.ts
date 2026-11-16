// @effect-diagnostics nodeBuiltinImport:off
import { expect, it } from "vite-plus/test";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import {
  acquireStoppedServerOwnership,
  withStoppedServerOwnership,
} from "./stoppedServerOwnership.ts";

async function fixture(body: (base: string) => Promise<void>) {
  const base = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-state-ownership-test-"));
  try {
    await NodeFSP.mkdir(NodePath.join(base, "userdata"), { mode: 0o700 });
    await body(base);
  } finally {
    await NodeFSP.rm(base, { recursive: true, force: true });
    await expect(NodeFSP.lstat(base)).rejects.toMatchObject({ code: "ENOENT" });
  }
}

it("refuses state changes while the server lifetime transaction owns the home", () =>
  fixture(async (base) => {
    const path = NodePath.join(base, "userdata", "server-owner.sqlite");
    const server = new NodeSqlite.DatabaseSync(path);
    server.exec("BEGIN EXCLUSIVE;");
    const sentinel = NodePath.join(base, "userdata", "sentinel");
    await NodeFSP.writeFile(sentinel, "before");
    try {
      await expect(
        withStoppedServerOwnership(base, async () => {
          await NodeFSP.writeFile(sentinel, "restored");
        }),
      ).rejects.toThrow(/state-dir-owned/);
      expect(await NodeFSP.readFile(sentinel, "utf8")).toBe("before");
    } finally {
      server.close();
    }
    await withStoppedServerOwnership(base, async () => {
      await NodeFSP.writeFile(sentinel, "restored");
    });
    expect(await NodeFSP.readFile(sentinel, "utf8")).toBe("restored");
  }));

it.each([false, true])(
  "releases ownership after failure=%s without unlinking the shared lock",
  (failed) =>
    fixture(async (base) => {
      const path = NodePath.join(base, "userdata", "server-owner.sqlite");
      const release = acquireStoppedServerOwnership(base);
      const original = await NodeFSP.stat(path);
      expect(() => acquireStoppedServerOwnership(base)).toThrow(/state-dir-owned/);
      release();
      const operation = withStoppedServerOwnership(base, async () => {
        if (failed) throw new Error("fixture body failed");
      });
      if (failed) await expect(operation).rejects.toThrow("fixture body failed");
      else await operation;
      const released = acquireStoppedServerOwnership(base);
      released();
      expect((await NodeFSP.stat(path)).ino).toBe(original.ino);
    }),
);

it("keeps a live legacy owner and an unreadable legacy runtime record closed to recovery", () =>
  fixture(async (base) => {
    const record = NodePath.join(base, "userdata", "server-runtime.json");
    await NodeFSP.writeFile(record, JSON.stringify({ pid: process.pid }));
    expect(() => acquireStoppedServerOwnership(base)).toThrow(/state-dir-owned/);
    await NodeFSP.writeFile(record, "{incomplete");
    expect(() => acquireStoppedServerOwnership(base)).toThrow(/runtime record is unreadable/);
    await NodeFSP.writeFile(
      record,
      JSON.stringify({ pid: process.pid, ownerId: "lock-owning-server" }),
    );
    const release = acquireStoppedServerOwnership(base);
    release();
  }));
