// @effect-diagnostics nodeBuiltinImport:off
import { assert, it } from "@effect/vitest";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  linuxQualifiedQuiescenceAdapter,
  nativeQualifiedQuiescenceAdapter,
  parseDarwinWriterOutput,
  proveQualifiedStateQuiescence,
  QualifiedQuiescenceError,
  type QualifiedQuiescenceAdapter,
} from "./qualifiedQuiescence.ts";

async function fixture(body: (baseDir: string, procRoot: string) => Promise<void>) {
  const base = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-quiescence-test-"));
  try {
    await NodeFSP.mkdir(NodePath.join(base, "userdata"));
    await NodeFSP.writeFile(NodePath.join(base, "userdata", "state.sqlite"), "synthetic database");
    await NodeFSP.writeFile(NodePath.join(base, "userdata", "settings.json"), "unchanged settings");
    const procRoot = NodePath.join(base, "proc");
    const pidDir = NodePath.join(procRoot, "123");
    await NodeFSP.mkdir(NodePath.join(pidDir, "fd"), { recursive: true });
    await NodeFSP.mkdir(NodePath.join(pidDir, "fdinfo"));
    const uid = (await NodeFSP.stat(base)).uid;
    await NodeFSP.writeFile(
      NodePath.join(pidDir, "status"),
      `Uid:\t${uid}\t${uid}\t${uid}\t${uid}\n`,
    );
    await NodeFSP.writeFile(
      NodePath.join(pidDir, "stat"),
      `123 (synthetic worker) S ${Array(18).fill("0").join(" ")} 777`,
    );
    await NodeFSP.writeFile(NodePath.join(pidDir, "maps"), "");
    await body(base, procRoot);
  } finally {
    await NodeFSP.rm(base, { recursive: true, force: true });
  }
}
const blocked = (reason: QualifiedQuiescenceError["reason"]) => (cause: unknown) =>
  cause instanceof QualifiedQuiescenceError && cause.reason === reason;

it("detects a same-user writable alias of the exact database without changing state", async () => {
  await fixture(async (baseDir, procRoot) => {
    const database = NodePath.join(baseDir, "userdata", "state.sqlite");
    await NodeFSP.link(database, NodePath.join(procRoot, "123", "fd", "5"));
    await NodeFSP.writeFile(NodePath.join(procRoot, "123", "fdinfo", "5"), "flags:\t0100002\n");
    await NodeAssert.rejects(
      proveQualifiedStateQuiescence({
        baseDir,
        adapter: linuxQualifiedQuiescenceAdapter({ procRoot }),
      }),
      blocked("writer-active"),
    );
    assert.equal(await NodeFSP.readFile(database, "utf8"), "synthetic database");
    assert.equal(
      await NodeFSP.readFile(NodePath.join(baseDir, "userdata", "settings.json"), "utf8"),
      "unchanged settings",
    );
  });
});

it("accepts readonly descriptors and excludes only the explicitly owned candidate writer", async () => {
  await fixture(async (baseDir, procRoot) => {
    await NodeFSP.link(
      NodePath.join(baseDir, "userdata", "state.sqlite"),
      NodePath.join(procRoot, "123", "fd", "5"),
    );
    const fdinfo = NodePath.join(procRoot, "123", "fdinfo", "5");
    await NodeFSP.writeFile(fdinfo, "flags:\t0100000\n");
    const adapter = linuxQualifiedQuiescenceAdapter({ procRoot });
    await proveQualifiedStateQuiescence({ baseDir, adapter });
    await NodeFSP.writeFile(fdinfo, "flags:\t0100002\n");
    await proveQualifiedStateQuiescence({ baseDir, adapter, allowedProcessIds: [123] });
    await NodeAssert.rejects(
      proveQualifiedStateQuiescence({ baseDir, adapter }),
      blocked("writer-active"),
    );
  });
});

it("detects a writable shared mapping after the writer closes its descriptor", async () => {
  await fixture(async (baseDir, procRoot) => {
    await NodeFSP.writeFile(
      NodePath.join(procRoot, "123", "maps"),
      "1234-5678 rw-s 00000000 00:00 2 /synthetic/state.sqlite\n",
    );
    const uid = (await NodeFSP.stat(baseDir)).uid;
    const writers = await linuxQualifiedQuiescenceAdapter({ procRoot }).scan({
      uid,
      files: [{ path: "/synthetic/state.sqlite", device: 0n, inode: 2n }],
      allowedProcessIds: [],
    });
    assert.deepEqual(writers, [{ pid: 123, path: "/synthetic/state.sqlite" }]);
  });
});

it("refuses an incomplete same-user descriptor observation and bounded process overflow", async () => {
  await fixture(async (baseDir, procRoot) => {
    await NodeFSP.link(
      NodePath.join(baseDir, "userdata", "state.sqlite"),
      NodePath.join(procRoot, "123", "fd", "5"),
    );
    await NodeAssert.rejects(
      proveQualifiedStateQuiescence({
        baseDir,
        adapter: linuxQualifiedQuiescenceAdapter({ procRoot }),
      }),
      blocked("unavailable"),
    );
    await NodeAssert.rejects(
      proveQualifiedStateQuiescence({
        baseDir,
        adapter: linuxQualifiedQuiescenceAdapter({ procRoot, maximumProcesses: 0 }),
      }),
      blocked("proof-limit"),
    );
  });
});

it("refuses state changed during observation and unavailable native platforms", async () => {
  await fixture(async (baseDir) => {
    const adapter: QualifiedQuiescenceAdapter = {
      scan: async () => {
        await NodeFSP.writeFile(
          NodePath.join(baseDir, "userdata", "keybindings.json"),
          "new writer",
        );
        return [];
      },
    };
    await NodeAssert.rejects(
      proveQualifiedStateQuiescence({ baseDir, adapter }),
      blocked("state-identity-changed"),
    );
    await NodeAssert.rejects(
      proveQualifiedStateQuiescence({
        baseDir,
        adapter: nativeQualifiedQuiescenceAdapter("unsupported"),
      }),
      blocked("unavailable"),
    );
  });
});

it("interprets bounded Darwin exact-file writer output without trusting unknown names", () => {
  const input = {
    uid: 501,
    files: [{ path: "/home/user/userdata/state.sqlite", device: 1n, inode: 2n }],
    allowedProcessIds: [],
  };
  assert.deepEqual(
    parseDarwinWriterOutput("p123\nf5\nar\nn/home/user/userdata/state.sqlite\n", input),
    [],
  );
  assert.deepEqual(
    parseDarwinWriterOutput("p123\nf5\nau\nn/home/user/userdata/state.sqlite\n", input),
    [{ pid: 123, path: input.files[0]!.path }],
  );
  NodeAssert.throws(
    () => parseDarwinWriterOutput("p123\nf5\nau\nn/unexpected/state.sqlite\n", input),
    blocked("unavailable"),
  );
});

it("observes the selected V2 database and sidecars while preserving legacy state", async () => {
  await fixture(async (baseDir) => {
    const databasePath = NodePath.join(baseDir, "userdata", "statev2.sqlite");
    await NodeFSP.writeFile(databasePath, "selected synthetic database");
    await NodeFSP.writeFile(`${databasePath}-wal`, "selected synthetic WAL");
    await NodeFSP.writeFile(`${databasePath}-shm`, "selected synthetic shared memory");
    let observed: string[] = [];
    await proveQualifiedStateQuiescence({
      baseDir,
      databasePath,
      adapter: {
        scan: async (input) => {
          observed = input.files.map((file) => file.path);
          return [];
        },
      },
    });
    assert.includeMembers(observed, [databasePath, `${databasePath}-wal`, `${databasePath}-shm`]);
    assert.notInclude(observed, NodePath.join(baseDir, "userdata", "state.sqlite"));
    assert.equal(
      await NodeFSP.readFile(NodePath.join(baseDir, "userdata", "state.sqlite"), "utf8"),
      "synthetic database",
    );
    await NodeAssert.rejects(
      proveQualifiedStateQuiescence({
        baseDir,
        databasePath,
        adapter: { scan: async () => [{ pid: 123, path: `${databasePath}-wal` }] },
      }),
      blocked("writer-active"),
    );
  });
});
