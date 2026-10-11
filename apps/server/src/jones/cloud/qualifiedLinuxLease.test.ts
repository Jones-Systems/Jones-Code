// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off
import { assert, it } from "@effect/vitest";
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { observeLinuxStateLeases, parseLinuxLeaseResult } from "./qualifiedLinuxLease.ts";
import { linuxQualifiedQuiescenceAdapter } from "./qualifiedQuiescence.ts";

// oxlint-disable-next-line t3code/no-global-process-runtime -- Kernel lease evidence exists only on a native Linux host.
const linux = NodeOS.platform() === "linux";

async function fixture(
  body: (files: { path: string; device: bigint; inode: bigint }[]) => Promise<void>,
) {
  const base = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-lease-test-"));
  try {
    const files = [];
    for (const name of ["statev2.sqlite", "settings.json"]) {
      const path = NodePath.join(base, name);
      await NodeFSP.writeFile(path, `synthetic ${name}`);
      const stat = await NodeFSP.stat(path, { bigint: true });
      files.push({ path, device: stat.dev, inode: stat.ino });
    }
    await body(files);
  } finally {
    await NodeFSP.rm(base, { recursive: true, force: true });
    await NodeAssert.rejects(NodeFSP.lstat(base), { code: "ENOENT" });
  }
}

it("accepts only a complete, ordered lease probe result", () => {
  const result = (stdout: string, extra: object = {}) =>
    parseLinuxLeaseResult({ code: 0, signal: null, stdout, stderr: "", ...extra }, 3);
  assert.deepEqual(result("complete\n"), []);
  assert.deepEqual(result("busy 0\nbusy 2\ncomplete\n"), [0, 2]);
  for (const incomplete of [
    "",
    "busy 0\n",
    "complete",
    "complete\nbusy 0\n",
    "busy 3\ncomplete\n",
    "busy 1\nbusy 1\ncomplete\n",
    "busy 2\nbusy 0\ncomplete\n",
    "busy -1\ncomplete\n",
    "lease 0\ncomplete\n",
  ])
    assert.equal(result(incomplete), undefined, JSON.stringify(incomplete));
  assert.equal(result("complete\n", { code: 4 }), undefined);
  assert.equal(result("complete\n", { code: null, signal: "SIGKILL" }), undefined);
  assert.equal(result("complete\n", { stderr: "Useless use of fcntl\n" }), undefined);
});

it("reports an unavailable lease program without inventing a proof", async () => {
  await fixture(async (files) => {
    assert.deepEqual(await observeLinuxStateLeases(files, { perl: "/nonexistent/jones-perl" }), {
      status: "unavailable",
    });
  });
});

it.skipIf(!linux)("proves exact inodes idle and keeps readers from blocking", async () => {
  await fixture(async (files) => {
    assert.deepEqual(await observeLinuxStateLeases(files), { status: "complete", busy: [] });
    const reader = await NodeFSP.open(files[0]!.path, "r");
    try {
      assert.deepEqual(await observeLinuxStateLeases(files), { status: "complete", busy: [] });
    } finally {
      await reader.close();
    }
    assert.equal(await NodeFSP.readFile(files[1]!.path, "utf8"), "synthetic settings.json");
  });
});

it.skipIf(!linux)("detects a write-open description held by any process", async () => {
  await fixture(async (files) => {
    const writer = await NodeFSP.open(files[1]!.path, "r+");
    try {
      assert.deepEqual(await observeLinuxStateLeases(files), { status: "complete", busy: [1] });
    } finally {
      await writer.close();
    }
    assert.deepEqual(await observeLinuxStateLeases(files), { status: "complete", busy: [] });
  });
});

it.skipIf(!linux)("detects an open SQLite connection on the exact database inode", async () => {
  await fixture(async (files) => {
    await NodeFSP.rm(files[0]!.path);
    const database = new NodeSqlite.DatabaseSync(files[0]!.path);
    try {
      database.exec("CREATE TABLE marker(value TEXT)");
      const stat = await NodeFSP.stat(files[0]!.path, { bigint: true });
      const selected = [{ ...files[0]!, device: stat.dev, inode: stat.ino }, files[1]!];
      assert.deepEqual(await observeLinuxStateLeases(selected), { status: "complete", busy: [0] });
    } finally {
      database.close();
    }
  });
});

// A controlled writer process: "hidden" marks itself non-dumpable as sshd
// sessions, the user manager and ssh-agent do, so the service user cannot read
// its /proc descriptors; "mapping" keeps only a writable shared mapping after
// closing the descriptor that created it.
const PYTHON_WRITER = `import ctypes, os, sys
libc = ctypes.CDLL(None, use_errno=True)
path, mode = sys.argv[1], sys.argv[2]
if mode == "hidden" and libc.prctl(4, 0, 0, 0, 0) != 0:
    sys.exit(3)
fd = os.open(path, os.O_RDWR)
if mode == "mapping":
    libc.mmap.restype = ctypes.c_void_p
    libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long]
    address = libc.mmap(None, os.fstat(fd).st_size, 3, 1, fd, 0)
    if address is None or address == ctypes.c_void_p(-1).value:
        sys.exit(4)
    os.close(fd)
print("ready", flush=True)
sys.stdin.read()
`;

async function withWriter(
  path: string,
  mode: "hidden" | "mapping",
  body: (pid: number) => Promise<void>,
) {
  const child = NodeChildProcess.spawn(
    "/usr/bin/python3",
    ["-I", "-c", PYTHON_WRITER, path, mode],
    {
      stdio: ["pipe", "pipe", "inherit"],
    },
  );
  const exited = new Promise<void>((resolve) => child.once("close", () => resolve()));
  try {
    await new Promise<void>((resolve, reject) => {
      child.stdout.once("data", (data: Buffer) =>
        data.toString("utf8").startsWith("ready") ? resolve() : reject(new Error("writer")),
      );
      child.once("error", reject);
      child.once("exit", (code) => reject(new Error(`synthetic writer exited ${String(code)}`)));
    });
    await body(child.pid!);
  } finally {
    // Only the captured child is stopped.
    child.stdin.end();
    const timer = setTimeout(() => child.kill("SIGKILL"), 5_000);
    await exited;
    clearTimeout(timer);
  }
}

it.skipIf(!linux)(
  "proves a non-dumpable writer that same-user descriptor observation cannot read",
  async () => {
    await fixture(async (files) => {
      await withWriter(files[0]!.path, "hidden", async (pid) => {
        await NodeAssert.rejects(NodeFSP.readdir(`/proc/${pid}/fd`), { code: "EACCES" });
        assert.deepEqual(await observeLinuxStateLeases(files), { status: "complete", busy: [0] });
        // The live incident: the same-user descriptor scan cannot complete.
        await NodeAssert.rejects(
          linuxQualifiedQuiescenceAdapter().scan({
            files,
            uid: (await NodeFSP.stat(files[0]!.path)).uid,
            allowedProcessIds: [],
          }),
          { code: "EACCES" },
        );
      });
      assert.deepEqual(await observeLinuxStateLeases(files), { status: "complete", busy: [] });
    });
  },
);

it.skipIf(!linux)("proves a writable shared mapping after its descriptor closed", async () => {
  await fixture(async (files) => {
    await withWriter(files[1]!.path, "mapping", async (pid) => {
      const descriptors = await NodeFSP.readdir(`/proc/${pid}/fd`);
      for (const fd of descriptors) {
        const stat = await NodeFSP.stat(`/proc/${pid}/fd/${fd}`, { bigint: true }).catch(
          () => undefined,
        );
        assert.notEqual(stat?.ino, files[1]!.inode, "the mapping must outlive its descriptor");
      }
      const maps = await NodeFSP.readFile(`/proc/${pid}/maps`, "utf8");
      assert.match(maps, new RegExp(`^\\S+ rw-s \\S+ \\S+ ${files[1]!.inode} `, "m"));
      assert.deepEqual(await observeLinuxStateLeases(files), { status: "complete", busy: [1] });
    });
    assert.deepEqual(await observeLinuxStateLeases(files), { status: "complete", busy: [] });
  });
});

it.skipIf(!linux)("refuses a replaced or redirected selected inode", async () => {
  await fixture(async (files) => {
    assert.deepEqual(
      await observeLinuxStateLeases([{ ...files[0]!, inode: files[0]!.inode + 1n }]),
      { status: "identity-changed" },
    );
    const link = `${files[1]!.path}.link`;
    await NodeFSP.symlink(files[1]!.path, link);
    assert.deepEqual(await observeLinuxStateLeases([{ ...files[1]!, path: link }]), {
      status: "identity-changed",
    });
  });
});
