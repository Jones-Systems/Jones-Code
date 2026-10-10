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
  parseDarwinWriterResult,
  proveQualifiedStateQuiescence,
  QualifiedQuiescenceError,
  type QualifiedQuiescenceAdapter,
} from "./qualifiedQuiescence.ts";

async function fixture(body: (baseDir: string, procRoot: string) => Promise<void>) {
  const base = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-quiescence-test-"));
  try {
    await NodeFSP.mkdir(NodePath.join(base, "userdata"));
    await NodeFSP.writeFile(
      NodePath.join(base, "userdata", "statev2.sqlite"),
      "synthetic database",
    );
    await NodeFSP.writeFile(NodePath.join(base, "userdata", "settings.json"), "unchanged settings");
    const procRoot = NodePath.join(base, "proc");
    const pidDir = NodePath.join(procRoot, "123");
    await NodeFSP.mkdir(NodePath.join(pidDir, "fd"), { recursive: true });
    await NodeFSP.mkdir(NodePath.join(pidDir, "fdinfo"));
    await NodeFSP.symlink("/synthetic/runtime/versions/current/t3", NodePath.join(pidDir, "exe"));
    await NodeFSP.writeFile(NodePath.join(pidDir, "cmdline"), "/synthetic/t3\0serve\0");
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
    await NodeAssert.rejects(NodeFSP.lstat(base), { code: "ENOENT" });
  }
}
const blocked = (reason: QualifiedQuiescenceError["reason"]) => (cause: unknown) =>
  cause instanceof QualifiedQuiescenceError && cause.reason === reason;

it("detects a same-user writable alias of the exact database without changing state", async () => {
  await fixture(async (baseDir, procRoot) => {
    const database = NodePath.join(baseDir, "userdata", "statev2.sqlite");
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

it("detects a Python sqlite writer holding the exact database inode open", async () => {
  await fixture(async (baseDir, procRoot) => {
    const pidDir = NodePath.join(procRoot, "123");
    await NodeFSP.unlink(NodePath.join(pidDir, "exe"));
    await NodeFSP.symlink("/usr/bin/python3", NodePath.join(pidDir, "exe"));
    await NodeFSP.writeFile(NodePath.join(pidDir, "cmdline"), "python3\0sqlite-tool.py\0");
    await NodeFSP.link(
      NodePath.join(baseDir, "userdata", "statev2.sqlite"),
      NodePath.join(pidDir, "fd", "5"),
    );
    await NodeFSP.writeFile(NodePath.join(pidDir, "fdinfo", "5"), "flags:\t0100002\n");
    await NodeAssert.rejects(
      proveQualifiedStateQuiescence({
        baseDir,
        adapter: linuxQualifiedQuiescenceAdapter({ procRoot }),
      }),
      blocked("writer-active"),
    );
  });
});

it("ignores unrelated process counts and an elapsed whole-host scan deadline", async () => {
  await fixture(async (baseDir, procRoot) => {
    let clock = 0;
    await proveQualifiedStateQuiescence({
      baseDir,
      adapter: linuxQualifiedQuiescenceAdapter({
        procRoot,
        maximumProcesses: 0,
        now: () => (clock += 10_000),
      }),
    });
  });
});

it("ignores unrelated same-user descriptors and their process birth during observation", async () => {
  await fixture(async (baseDir, procRoot) => {
    const unrelated = NodePath.join(procRoot, "124");
    await NodeFSP.mkdir(NodePath.join(unrelated, "fd"), { recursive: true });
    await NodeFSP.symlink("/usr/bin/python3", NodePath.join(unrelated, "exe"));
    for (const name of ["status", "stat", "maps"]) {
      await NodeFSP.copyFile(NodePath.join(procRoot, "123", name), NodePath.join(unrelated, name));
    }
    await NodeFSP.writeFile(NodePath.join(unrelated, "fd", "5"), "unrelated inode");
    let discoveries = 0;
    await proveQualifiedStateQuiescence({
      baseDir,
      adapter: linuxQualifiedQuiescenceAdapter({
        procRoot,
        processNames: async () => (++discoveries === 1 ? ["123"] : ["123", "124"]),
      }),
    });
    assert.equal(discoveries, 2);
  });
});

it("accepts readonly descriptors and excludes only the explicitly owned candidate writer", async () => {
  await fixture(async (baseDir, procRoot) => {
    await NodeFSP.link(
      NodePath.join(baseDir, "userdata", "statev2.sqlite"),
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

it("detects a Python shared-map writer after it closes its descriptor", async () => {
  await fixture(async (baseDir, procRoot) => {
    await NodeFSP.unlink(NodePath.join(procRoot, "123", "exe"));
    await NodeFSP.symlink("/usr/bin/python3", NodePath.join(procRoot, "123", "exe"));
    await NodeFSP.writeFile(NodePath.join(procRoot, "123", "cmdline"), "python3\0sqlite-tool.py\0");
    await NodeFSP.writeFile(
      NodePath.join(procRoot, "123", "maps"),
      "1234-5678 rw-s 00000000 00:00 2 /synthetic/statev2.sqlite\n",
    );
    const uid = (await NodeFSP.stat(baseDir)).uid;
    const writers = await linuxQualifiedQuiescenceAdapter({ procRoot }).scan({
      uid,
      files: [{ path: "/synthetic/statev2.sqlite", device: 0n, inode: 2n }],
      allowedProcessIds: [],
    });
    assert.deepEqual(writers, [{ pid: 123, path: "/synthetic/statev2.sqlite" }]);
  });
});

it("refuses incomplete selected-inode flags but ignores aggregate descriptor limits", async () => {
  await fixture(async (baseDir, procRoot) => {
    await NodeFSP.link(
      NodePath.join(baseDir, "userdata", "statev2.sqlite"),
      NodePath.join(procRoot, "123", "fd", "5"),
    );
    await NodeAssert.rejects(
      proveQualifiedStateQuiescence({
        baseDir,
        adapter: linuxQualifiedQuiescenceAdapter({ procRoot }),
      }),
      blocked("unavailable"),
    );
    await NodeFSP.writeFile(NodePath.join(procRoot, "123", "fdinfo", "5"), "flags:\t0100000\n");
    await proveQualifiedStateQuiescence({
      baseDir,
      adapter: linuxQualifiedQuiescenceAdapter({ procRoot, maximumDescriptors: 0 }),
    });
  });
});

it("detects a newly discovered Node runtime holding the exact settings inode open for writing", async () => {
  await fixture(async (baseDir, procRoot) => {
    const pidDir = NodePath.join(procRoot, "123");
    await NodeFSP.unlink(NodePath.join(pidDir, "exe"));
    await NodeFSP.symlink("/usr/bin/node", NodePath.join(pidDir, "exe"));
    await NodeFSP.symlink("/synthetic/repo/apps/server", NodePath.join(pidDir, "cwd"));
    await NodeFSP.writeFile(NodePath.join(pidDir, "cmdline"), "node\0dist/bin.mjs\0serve\0");
    await NodeFSP.link(
      NodePath.join(baseDir, "userdata", "settings.json"),
      NodePath.join(pidDir, "fd", "5"),
    );
    await NodeFSP.writeFile(NodePath.join(pidDir, "fdinfo", "5"), "flags:\t0100001\n");
    let discoveries = 0;
    await NodeAssert.rejects(
      proveQualifiedStateQuiescence({
        baseDir,
        adapter: linuxQualifiedQuiescenceAdapter({
          procRoot,
          processNames: async () => (++discoveries === 1 ? [] : ["123"]),
        }),
      }),
      blocked("writer-active"),
    );
  });
});

it("keeps a runtime with unavailable process evidence blocked", async () => {
  await fixture(async (baseDir, procRoot) => {
    await NodeFSP.unlink(NodePath.join(procRoot, "123", "maps"));
    await NodeAssert.rejects(
      proveQualifiedStateQuiescence({
        baseDir,
        adapter: linuxQualifiedQuiescenceAdapter({ procRoot }),
      }),
      blocked("unavailable"),
    );
  });
});

it("needs no executable or command-line identity for processes without selected inodes", async () => {
  await fixture(async (baseDir, procRoot) => {
    const directory = NodePath.join(procRoot, "124");
    await NodeFSP.mkdir(NodePath.join(directory, "fd"), { recursive: true });
    for (const name of ["status", "stat", "maps"]) {
      await NodeFSP.copyFile(NodePath.join(procRoot, "123", name), NodePath.join(directory, name));
    }
    await proveQualifiedStateQuiescence({
      baseDir,
      adapter: linuxQualifiedQuiescenceAdapter({ procRoot }),
    });
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
    files: [{ path: "/home/user/userdata/statev2.sqlite", device: 1n, inode: 2n }],
    allowedProcessIds: [],
  };
  assert.deepEqual(
    parseDarwinWriterOutput("p123\nf5\nar\nn/home/user/userdata/statev2.sqlite\n", input),
    [],
  );
  assert.deepEqual(
    parseDarwinWriterOutput("p123\nf5\nau\nn/home/user/userdata/statev2.sqlite\n", input),
    [{ pid: 123, path: input.files[0]!.path }],
  );
  NodeAssert.throws(
    () => parseDarwinWriterOutput("p123\nf5\nau\nn/unexpected/statev2.sqlite\n", input),
    blocked("unavailable"),
  );
});

it("accepts Darwin unrelated filesystem warnings and retains real diagnostic uncertainty", () => {
  const input = {
    uid: 501,
    files: [{ path: "/home/user/userdata/statev2.sqlite", device: 1n, inode: 2n }],
    allowedProcessIds: [],
  };
  const warning =
    "lsof: WARNING: can't stat() fuse file system /Volumes/unrelated\n      Output information may be incomplete.\n";
  assert.deepEqual(
    parseDarwinWriterResult(
      { code: 0, stdout: "p123\nf5\nar\nn/home/user/userdata/statev2.sqlite\n", stderr: warning },
      input,
    ),
    [],
  );
  assert.deepEqual(parseDarwinWriterResult({ code: 1, stdout: "", stderr: warning }, input), []);
  NodeAssert.throws(
    () =>
      parseDarwinWriterResult({ code: 0, stdout: "", stderr: "lsof: permission denied\n" }, input),
    blocked("unavailable"),
  );
  NodeAssert.throws(
    () =>
      parseDarwinWriterResult(
        {
          code: 1,
          stdout: "",
          stderr: "lsof: WARNING: can't stat() file system /home/user/userdata/statev2.sqlite\n",
        },
        input,
      ),
    blocked("unavailable"),
  );
  NodeAssert.throws(
    () =>
      parseDarwinWriterResult(
        {
          code: 1,
          stdout: "",
          stderr: "lsof: WARNING: can't stat() fuse file system /home/user\n",
        },
        input,
      ),
    blocked("unavailable"),
  );
});
