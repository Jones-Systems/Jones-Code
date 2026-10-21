// These synthetic recovery fixtures check native file identities and durability operations.
// @effect-diagnostics nodeBuiltinImport:off
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as NodeChildProcess from "node:child_process";
import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  copyQualifiedBackupFile,
  preflightQualifiedBackup,
  readQualifiedBackupReceipt,
  retainQualifiedAdvancedState,
  restoreQualifiedBackupFile,
  writeQualifiedBackupReceipt,
  type QualifiedBackupAdapter,
} from "./qualifiedBackup.ts";

vi.mock("node:os", async (importOriginal) => ({
  ...(await importOriginal<typeof import("node:os")>()),
}));
vi.mock("node:child_process", async (importOriginal) => ({
  ...(await importOriginal<typeof import("node:child_process")>()),
}));
vi.mock("node:fs/promises", async (importOriginal) => ({
  ...(await importOriginal<typeof import("node:fs/promises")>()),
}));

async function fixture(body: (base: string, database: string) => Promise<void>) {
  const base = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-backup-test-"));
  try {
    await NodeFSP.mkdir(NodePath.join(base, "userdata"), { mode: 0o700 });
    await NodeFSP.mkdir(NodePath.join(base, "runtime"), { mode: 0o700 });
    const database = NodePath.join(base, "userdata", "statev2.sqlite");
    await NodeFSP.writeFile(database, "before-database", { mode: 0o600 });
    await body(base, database);
  } finally {
    await NodeFSP.rm(base, { recursive: true, force: true });
  }
}

const unsupported = () =>
  Object.assign(new Error("synthetic clone unsupported"), { code: "ENOTSUP" });

it("forces clone for the database and records copy fallback only on an unsupported filesystem", async () => {
  await fixture(async (base, database) => {
    const flags: number[] = [];
    const clone: QualifiedBackupAdapter = {
      availableBytes: async () => 2 * 1024 * 1024,
      copyFile: async (source, target, flag) => {
        flags.push(flag);
        await NodeFSP.copyFile(source, target, NodeFS.constants.COPYFILE_EXCL);
      },
    };
    const cloned = NodePath.join(base, "cloned");
    assert.deepEqual(await copyQualifiedBackupFile(database, cloned, clone), {
      method: "clone",
      bytes: 15,
    });
    assert.equal(
      flags[0]! & NodeFS.constants.COPYFILE_FICLONE_FORCE,
      NodeFS.constants.COPYFILE_FICLONE_FORCE,
    );
    assert.equal(await NodeFSP.readFile(cloned, "utf8"), "before-database");
    const copied = NodePath.join(base, "copied");
    const fallback: QualifiedBackupAdapter = {
      ...clone,
      copyFile: async (source, target, flag) => {
        flags.push(flag);
        if (flag & NodeFS.constants.COPYFILE_FICLONE_FORCE) throw unsupported();
        await NodeFSP.copyFile(source, target, flag);
      },
    };
    assert.deepEqual(await copyQualifiedBackupFile(database, copied, fallback), {
      method: "copy",
      bytes: 15,
    });
    assert.equal(await NodeFSP.readFile(copied, "utf8"), "before-database");
    assert.equal(flags.at(-1), NodeFS.constants.COPYFILE_EXCL);
  });
});

it("admits a low-space clone and refuses insufficient copy capacity before touching database state", async () => {
  await fixture(async (base, database) => {
    await NodeFSP.truncate(database, 32 * 1024 * 1024);
    const before = await NodeFSP.stat(database);
    const clone: QualifiedBackupAdapter = {
      availableBytes: async () => 2 * 1024 * 1024,
      copyFile: async (source, target, flag) => {
        assert.notEqual(source, database);
        assert.equal(
          flag & NodeFS.constants.COPYFILE_FICLONE_FORCE,
          NodeFS.constants.COPYFILE_FICLONE_FORCE,
        );
        await NodeFSP.copyFile(source, target, NodeFS.constants.COPYFILE_EXCL);
      },
    };
    assert.deepEqual(await preflightQualifiedBackup(base, database, clone), {
      method: "clone",
      bytes: before.size,
    });
    await NodeAssert.rejects(
      preflightQualifiedBackup(base, database, {
        ...clone,
        copyFile: async () => {
          throw unsupported();
        },
      }),
      /recovery-capacity/,
    );
    const after = await NodeFSP.stat(database);
    assert.equal(after.ino, before.ino);
    assert.equal(after.mtimeMs, before.mtimeMs);
    assert.deepEqual(await NodeFSP.readdir(NodePath.join(base, "runtime", "db-backup")), []);
  });
});

it("preserves unexpected clone failures rather than silently copying", async () => {
  await fixture(async (base, database) => {
    const permission = Object.assign(new Error("synthetic permission"), { code: "EACCES" });
    let copies = 0;
    await NodeAssert.rejects(
      copyQualifiedBackupFile(database, NodePath.join(base, "target"), {
        availableBytes: async () => 100 * 1024 * 1024,
        copyFile: async () => {
          copies += 1;
          throw permission;
        },
      }),
      (cause) => cause === permission,
    );
    assert.equal(copies, 1);
  });
});

it("resumes journaled advanced-state renames without copying or capturing restored files again", async () => {
  await fixture(async (base, database) => {
    const backup = NodePath.join(base, "runtime", "db-backup", "test-update");
    const advanced = NodePath.join(backup, "advanced-state");
    await NodeFSP.mkdir(advanced, { recursive: true, mode: 0o700 });
    const settings = NodePath.join(base, "userdata", "settings.json");
    await NodeFSP.writeFile(database, "failed-trial-database");
    await NodeFSP.writeFile(`${database}-wal`, "failed-trial-wal");
    await NodeFSP.writeFile(settings, "failed-trial-settings");
    await NodeFSP.writeFile(NodePath.join(backup, "database"), "previous-database");
    const names = ["database", "database-wal", "settings.json"];
    const sources = [database, `${database}-wal`, settings];
    const entries = await Promise.all(
      sources.map(async (source, index) => {
        const stat = await NodeFSP.stat(source, { bigint: true });
        return { name: names[index], device: stat.dev.toString(), inode: stat.ino.toString() };
      }),
    );
    await NodeFSP.writeFile(
      NodePath.join(advanced, "rename-journal.json"),
      JSON.stringify(entries),
    );
    await NodeFSP.rename(database, NodePath.join(advanced, "database"));
    await NodeFSP.copyFile(NodePath.join(backup, "database"), database);
    await retainQualifiedAdvancedState(base, database, backup);
    for (const [index, name] of names.entries()) {
      const stat = await NodeFSP.stat(NodePath.join(advanced, name), { bigint: true });
      assert.equal(stat.ino.toString(), entries[index]!.inode);
    }
    assert.equal(await NodeFSP.readFile(database, "utf8"), "previous-database");
    assert.equal(
      await NodeFSP.readFile(NodePath.join(advanced, "database"), "utf8"),
      "failed-trial-database",
    );
    assert.deepEqual(
      JSON.parse(await NodeFSP.readFile(NodePath.join(advanced, "paired-settings.json"), "utf8")),
      ["settings.json"],
    );
    await retainQualifiedAdvancedState(base, database, backup);
    await restoreQualifiedBackupFile(NodePath.join(backup, "database"), database);
    assert.equal(await NodeFSP.readFile(database, "utf8"), "previous-database");
    assert.equal(
      await NodeFSP.readFile(NodePath.join(backup, "database"), "utf8"),
      "previous-database",
    );
    assert.deepEqual(
      (await NodeFSP.readdir(NodePath.dirname(database))).filter((name) =>
        name.includes(".restore-"),
      ),
      [],
    );
  });
});

it("holds unknown advanced-state effects and publishes a validated timing receipt", async () => {
  await fixture(async (base, database) => {
    const backup = NodePath.join(base, "runtime", "db-backup", "test-update");
    await NodeFSP.mkdir(NodePath.join(backup, "advanced-state"), { recursive: true });
    await NodeFSP.writeFile(NodePath.join(backup, "advanced-state", "database"), "unknown-owner");
    await NodeAssert.rejects(
      retainQualifiedAdvancedState(base, database, backup),
      /unknown prior effects/,
    );
    assert.equal(await NodeFSP.readFile(database, "utf8"), "before-database");
    const receipt = {
      protocol: 1 as const,
      updateId: "test-update",
      method: "clone" as const,
      bytes: 15,
      startedAt: "2026-10-10T12:00:00.000Z",
      completedAt: "2026-10-10T12:00:00.015Z",
      durationMs: 15,
    };
    await writeQualifiedBackupReceipt(backup, receipt);
    assert.deepEqual(await readQualifiedBackupReceipt(base, "test-update"), receipt);
    assert.equal(await readQualifiedBackupReceipt(base, "missing-update"), undefined);
    await NodeAssert.rejects(readQualifiedBackupReceipt(base, "../outside"), /identity/);
  });
});

it("finishes an interrupted completed restore buffer by rename and cleans only an owned partial buffer", async () => {
  await fixture(async (base, database) => {
    const backup = NodePath.join(base, "runtime", "db-backup", "test-update");
    await NodeFSP.mkdir(backup, { recursive: true, mode: 0o700 });
    const source = NodePath.join(backup, "database");
    await NodeFSP.writeFile(source, "previous-database");
    const identity = NodeCrypto.createHash("sha256").update(source).digest("hex").slice(0, 16);
    const temporary = `${database}.restore-${identity}`;
    const intent = NodePath.join(backup, ".database-restore.json");
    await NodeFSP.writeFile(intent, JSON.stringify({ source, target: database, temporary }));
    await NodeFSP.writeFile(temporary, "previous-database");
    const buffer = await NodeFSP.stat(temporary, { bigint: true });
    await NodeFSP.writeFile(
      `${intent}.complete`,
      JSON.stringify({ device: buffer.dev.toString(), inode: buffer.ino.toString() }),
    );
    await restoreQualifiedBackupFile(source, database);
    assert.equal((await NodeFSP.stat(database, { bigint: true })).ino, buffer.ino);
    await restoreQualifiedBackupFile(source, database);
    assert.equal((await NodeFSP.stat(database, { bigint: true })).ino, buffer.ino);
    const settingsSource = NodePath.join(backup, "settings.json");
    const settingsTarget = NodePath.join(base, "userdata", "settings.json");
    await NodeFSP.writeFile(settingsSource, "previous-settings");
    const settingsIdentity = NodeCrypto.createHash("sha256")
      .update(settingsSource)
      .digest("hex")
      .slice(0, 16);
    const settingsBuffer = `${settingsTarget}.restore-${settingsIdentity}`;
    await NodeFSP.writeFile(
      NodePath.join(backup, ".settings.json-restore.json"),
      JSON.stringify({ source: settingsSource, target: settingsTarget, temporary: settingsBuffer }),
    );
    await NodeFSP.writeFile(settingsBuffer, "partial-copy");
    await restoreQualifiedBackupFile(settingsSource, settingsTarget);
    assert.equal(await NodeFSP.readFile(settingsTarget, "utf8"), "previous-settings");
    await NodeAssert.rejects(NodeFSP.access(settingsBuffer));
    assert.equal(await NodeFSP.readFile(source, "utf8"), "previous-database");
  });
});

it("preserves a restore buffer whose durable ownership is unknown", async () => {
  await fixture(async (base, database) => {
    const backup = NodePath.join(base, "runtime", "db-backup", "test-update");
    await NodeFSP.mkdir(backup, { recursive: true });
    const source = NodePath.join(backup, "database");
    await NodeFSP.writeFile(source, "previous-database");
    const identity = NodeCrypto.createHash("sha256").update(source).digest("hex").slice(0, 16);
    const temporary = `${database}.restore-${identity}`;
    await NodeFSP.writeFile(temporary, "unowned-copy");
    await NodeAssert.rejects(restoreQualifiedBackupFile(source, database), /unknown prior effects/);
    assert.equal(await NodeFSP.readFile(temporary, "utf8"), "unowned-copy");
    assert.equal(await NodeFSP.readFile(database, "utf8"), "before-database");
  });
});

// oxlint-disable-next-line t3code/no-global-process-runtime -- This integration fixture requires the actual macOS syscall.
it.runIf(NodeOS.platform() === "darwin")(
  "uses native clones for both preflight and copying and preserves an existing destination",
  async () => {
    await fixture(async (base, database) => {
      assert.equal((await preflightQualifiedBackup(base, database)).method, "clone");
      const target = NodePath.join(base, "native-clone");
      assert.deepEqual(await copyQualifiedBackupFile(database, target), {
        method: "clone",
        bytes: 15,
      });
      assert.equal(await NodeFSP.readFile(target, "utf8"), "before-database");
      assert.notEqual((await NodeFSP.stat(target)).ino, (await NodeFSP.stat(database)).ino);
      await NodeFSP.writeFile(target, "existing-sentinel");
      const before = await NodeFSP.stat(target);
      await NodeAssert.rejects(copyQualifiedBackupFile(database, target), { code: "EEXIST" });
      assert.equal((await NodeFSP.stat(target)).ino, before.ino);
      assert.equal(await NodeFSP.readFile(target, "utf8"), "existing-sentinel");
      assert.equal(await NodeFSP.readFile(database, "utf8"), "before-database");
      assert.deepEqual(
        (await NodeFSP.readdir(base)).filter((name) => name.startsWith(".qualified-clone-")),
        [],
      );
    });
  },
);

async function withDarwinCloneProcess(
  run: (source: string, target: string) => Promise<{ stdout: string; cause?: Error }>,
  body: () => Promise<void>,
) {
  const platform = vi.spyOn(NodeOS, "platform").mockReturnValue("darwin");
  const process = vi.spyOn(NodeChildProcess, "execFile").mockImplementation((...args) => {
    const [executable, argv, options, callback] = args;
    assert.equal(executable, "/usr/bin/osascript");
    assert.isArray(argv);
    const command = argv as readonly string[];
    assert.deepEqual(command.slice(0, 3), ["-l", "JavaScript", "-e"]);
    assert.include(command[3]!, 'ObjC.bindFunction("clonefile"');
    assert.notInclude(command[3]!, "Application(");
    assert.deepEqual(options, {
      encoding: "utf8",
      timeout: 30_000,
      killSignal: "SIGKILL",
      maxBuffer: 4096,
    });
    const complete = callback as (cause: Error | null, stdout: string, stderr: string) => void;
    void run(command[4]!, command[5]!).then(
      ({ stdout, cause }) => complete(cause ?? null, stdout, ""),
      (cause: Error) => complete(cause, "", ""),
    );
    return {} as NodeChildProcess.ChildProcess;
  });
  try {
    await body();
  } finally {
    process.mockRestore();
    platform.mockRestore();
  }
}

it("normalizes Darwin unsupported errno and checks capacity before any full copy", async () => {
  await fixture(async (base, database) => {
    const free = await NodeFSP.statfs(base, { bigint: true });
    const target = NodePath.join(base, "capacity-target");
    const capacity = vi.spyOn(NodeFSP, "statfs").mockResolvedValue({ ...free, bavail: 0n });
    try {
      for (const errno of [18, 22, 45, 78, 102]) {
        await withDarwinCloneProcess(
          async () => ({ stdout: `${errno}\n` }),
          async () => {
            await NodeAssert.rejects(
              copyQualifiedBackupFile(database, target),
              /recovery-capacity/,
            );
          },
        );
        await NodeAssert.rejects(NodeFSP.lstat(target), { code: "ENOENT" });
      }
    } finally {
      capacity.mockRestore();
    }
    await NodeFSP.writeFile(database, "small-copy");
    await withDarwinCloneProcess(
      async () => ({ stdout: "45\n" }),
      async () => {
        assert.deepEqual(await copyQualifiedBackupFile(database, target), {
          method: "copy",
          bytes: 10,
        });
      },
    );
    assert.equal(await NodeFSP.readFile(target, "utf8"), "small-copy");
    assert.deepEqual(
      (await NodeFSP.readdir(base)).filter((name) => name.startsWith(".qualified-clone-")),
      [],
    );
  });
});

it("cleans owned clone output on malformed or failed subprocess responses without publishing or falling back", async () => {
  await fixture(async (base, database) => {
    const target = NodePath.join(base, "unpublished");
    for (const response of [
      { stdout: "unexpected output" },
      {
        stdout: "0\n",
        cause: Object.assign(new Error("synthetic timeout"), { killed: true, signal: "SIGKILL" }),
      },
      {
        stdout: "",
        cause: Object.assign(new Error("synthetic missing bridge"), { code: "ENOENT" }),
      },
    ]) {
      await withDarwinCloneProcess(
        async (_source, temporary) => {
          await NodeFSP.writeFile(temporary, "owned-clone-output", { flag: "wx" });
          return response;
        },
        async () => {
          await NodeAssert.rejects(
            copyQualifiedBackupFile(database, target),
            /Native recovery clone/,
          );
        },
      );
      await NodeAssert.rejects(NodeFSP.lstat(target), { code: "ENOENT" });
      assert.deepEqual(
        (await NodeFSP.readdir(base)).filter((name) => name.startsWith(".qualified-clone-")),
        [],
      );
    }
  });
});

it("preserves a racing destination and fails closed on native permission and space errors", async () => {
  await fixture(async (base, database) => {
    const target = NodePath.join(base, "racing-target");
    await withDarwinCloneProcess(
      async (_source, temporary) => {
        await NodeFSP.writeFile(temporary, "clone-output", { flag: "wx" });
        await NodeFSP.writeFile(target, "race-sentinel", { flag: "wx" });
        return { stdout: "0\n" };
      },
      async () => {
        await NodeAssert.rejects(copyQualifiedBackupFile(database, target), { code: "EEXIST" });
      },
    );
    assert.equal(await NodeFSP.readFile(target, "utf8"), "race-sentinel");
    for (const [errno, code] of [
      [13, "EACCES"],
      [17, "EEXIST"],
      [28, "ENOSPC"],
      [999, "UNKNOWN"],
    ] as const) {
      await withDarwinCloneProcess(
        async () => ({ stdout: `${errno}\n` }),
        async () => {
          await NodeAssert.rejects(
            copyQualifiedBackupFile(database, NodePath.join(base, "no-copy")),
            { code },
          );
        },
      );
      await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(base, "no-copy")), { code: "ENOENT" });
    }
    assert.deepEqual(
      (await NodeFSP.readdir(base)).filter((name) => name.startsWith(".qualified-clone-")),
      [],
    );
  });
});

it("retains the Node forced-clone primitive on non-Darwin hosts", async () => {
  await fixture(async (base, database) => {
    const originalCopy = NodeFSP.copyFile;
    const platform = vi.spyOn(NodeOS, "platform").mockReturnValue("linux");
    const native = vi.spyOn(NodeChildProcess, "execFile");
    const copy = vi.spyOn(NodeFSP, "copyFile").mockImplementation((source, target, flags) => {
      assert.equal(flags, NodeFS.constants.COPYFILE_EXCL | NodeFS.constants.COPYFILE_FICLONE_FORCE);
      return originalCopy(source, target, NodeFS.constants.COPYFILE_EXCL);
    });
    try {
      assert.equal(
        (await copyQualifiedBackupFile(database, NodePath.join(base, "linux-target"))).method,
        "clone",
      );
      assert.equal(copy.mock.calls.length, 1);
      assert.equal(native.mock.calls.length, 0);
    } finally {
      copy.mockRestore();
      native.mockRestore();
      platform.mockRestore();
    }
  });
});

it("copies a Linux fallback in concurrent chunks and removes a partial copy after a failed write", async () => {
  await fixture(async (base, database) => {
    const chunk = 8 * 1024 * 1024;
    const contents = NodeCrypto.randomBytes(2 * chunk + chunk / 2 + 3);
    await NodeFSP.writeFile(database, contents);
    await NodeFSP.chmod(database, 0o640);
    const empty = NodePath.join(base, "userdata", "empty");
    await NodeFSP.writeFile(empty, "");
    const failed = NodePath.join(base, "failed");
    const space = Object.assign(new Error("synthetic full disk"), { code: "ENOSPC" });
    let failedWrites = 0;
    const originalOpen = NodeFSP.open;
    const platform = vi.spyOn(NodeOS, "platform").mockReturnValue("linux");
    const clone = vi
      .spyOn(NodeFSP, "copyFile")
      .mockImplementation(async (_source, _target, flags) => {
        assert.equal(
          flags,
          NodeFS.constants.COPYFILE_EXCL | NodeFS.constants.COPYFILE_FICLONE_FORCE,
        );
        throw unsupported();
      });
    // The second chunk's write to the "failed" target reports a full disk.
    const open = vi.spyOn(NodeFSP, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      if (args[0] !== failed) return handle;
      return new Proxy(handle, {
        get(target, property) {
          if (property === "write")
            return async (buffer: Buffer, offset: number, length: number, position: number) => {
              if (position !== chunk) return target.write(buffer, offset, length, position);
              failedWrites += 1;
              throw space;
            };
          const value = Reflect.get(target, property, target);
          return typeof value === "function" ? value.bind(target) : value;
        },
      });
    });
    try {
      const copied = NodePath.join(base, "copied");
      assert.deepEqual(await copyQualifiedBackupFile(database, copied), {
        method: "copy",
        bytes: contents.length,
      });
      assert.isTrue((await NodeFSP.readFile(copied)).equals(contents));
      assert.equal((await NodeFSP.stat(copied)).mode & 0o777, 0o640);
      assert.equal(clone.mock.calls.length, 1);
      await copyQualifiedBackupFile(empty, NodePath.join(base, "empty-copy"));
      assert.equal((await NodeFSP.stat(NodePath.join(base, "empty-copy"))).size, 0);

      await NodeAssert.rejects(
        copyQualifiedBackupFile(database, failed),
        (cause) => cause === space,
      );
      assert.equal(failedWrites, 1);
      await NodeAssert.rejects(NodeFSP.lstat(failed), { code: "ENOENT" });
    } finally {
      open.mockRestore();
      clone.mockRestore();
      platform.mockRestore();
    }
  });
});

function cpuSnapshot(busy: number, idle: number, count = 4): NodeOS.CpuInfo[] {
  return Array.from({ length: count }, () => ({
    model: "synthetic CPU",
    speed: 1000,
    times: { user: busy, nice: 0, sys: 0, irq: 0, idle },
  }));
}

async function checkLinuxCopyConcurrency(
  expected: number,
  parallelism: () => number,
  snapshots: () => NodeOS.CpuInfo[],
  chunks = 6,
) {
  await fixture(async (base, database) => {
    const chunk = 8 * 1024 * 1024;
    await NodeFSP.truncate(database, chunks * chunk);
    const destination = NodePath.join(base, "cpu-selected-copy");
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let active = 0;
    let maximum = 0;
    const originalOpen = NodeFSP.open;
    const platform = vi.spyOn(NodeOS, "platform").mockReturnValue("linux");
    const capacity = vi.spyOn(NodeOS, "availableParallelism").mockImplementation(parallelism);
    const cpus = vi.spyOn(NodeOS, "cpus").mockImplementation(snapshots);
    const clone = vi.spyOn(NodeFSP, "copyFile").mockRejectedValue(unsupported());
    const open = vi.spyOn(NodeFSP, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      if (args[0] !== database) return handle;
      return new Proxy(handle, {
        get(target, property) {
          if (property === "read")
            return async (buffer: Buffer, offset: number, length: number, position: number) => {
              active += 1;
              maximum = Math.max(maximum, active);
              entered.resolve();
              try {
                await release.promise;
                return await target.read(buffer, offset, length, position);
              } finally {
                active -= 1;
              }
            };
          const value = Reflect.get(target, property, target);
          return typeof value === "function" ? value.bind(target) : value;
        },
      });
    });
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const copying = copyQualifiedBackupFile(database, destination);
    try {
      await entered.promise;
      assert.equal(active, 1, "baseline starts before CPU sampling completes");
      await vi.advanceTimersByTimeAsync(100);
      assert.equal(active, expected, "CPU evidence selects concurrent positioned reads");
      release.resolve();
      assert.deepEqual(await copying, { method: "copy", bytes: chunks * chunk });
      assert.equal(maximum, expected);
      assert.equal(vi.getTimerCount(), 0);
    } finally {
      release.resolve();
      await copying.catch(() => {});
      vi.useRealTimers();
      open.mockRestore();
      clone.mockRestore();
      cpus.mockRestore();
      capacity.mockRestore();
      platform.mockRestore();
    }
  });
}

it("keeps a saturated Linux fallback on its baseline copy loop", async () => {
  let samples = 0;
  await checkLinuxCopyConcurrency(
    1,
    () => 4,
    () => cpuSnapshot(samples++ * 100, 0),
  );
});

it.each([
  { name: "idle host", available: 8, busy: 0, idle: 100, expected: 4 },
  { name: "two-core capacity hint", available: 2, busy: 0, idle: 100, expected: 2 },
  { name: "one-core capacity hint", available: 1, busy: 0, idle: 100, expected: 1 },
  { name: "one spare core", available: 4, busy: 75, idle: 25, expected: 2 },
  { name: "less than one spare core", available: 4, busy: 90, idle: 10, expected: 1 },
  { name: "zero capacity hint", available: 0, busy: 0, idle: 100, expected: 4 },
  { name: "nonfinite capacity hint", available: NaN, busy: 0, idle: 100, expected: 4 },
  { name: "fractional capacity hint", available: 1.5, busy: 0, idle: 100, expected: 4 },
])("selects Linux copy concurrency for $name", async ({ available, busy, idle, expected }) => {
  let samples = 0;
  await checkLinuxCopyConcurrency(
    expected,
    () => available,
    () => {
      if (available === 1) {
        samples += 1;
        throw new Error("one-core copies must not sample");
      }
      return samples++ === 0 ? cpuSnapshot(0, 0) : cpuSnapshot(busy, idle);
    },
  );
  assert.equal(samples, available === 1 ? 0 : 2);
});

it.each([
  { name: "empty", after: [] },
  { name: "mismatched", after: cpuSnapshot(100, 200, 3) },
  { name: "reset", after: cpuSnapshot(0, 200) },
  { name: "nonfinite", after: cpuSnapshot(Infinity, 200) },
  { name: "zero total delta", after: cpuSnapshot(100, 100) },
])("keeps aggressive Linux copy capacity with $name CPU evidence", async ({ after }) => {
  let samples = 0;
  await checkLinuxCopyConcurrency(
    4,
    () => 4,
    () => (samples++ === 0 ? cpuSnapshot(100, 100) : after),
  );
});

it.each([0, 1])(
  "keeps aggressive Linux copy capacity when CPU snapshot %i throws",
  async (failsAt) => {
    let samples = 0;
    await checkLinuxCopyConcurrency(
      4,
      () => 4,
      () => {
        if (samples++ === failsAt) throw new Error("synthetic CPU lookup failure");
        return cpuSnapshot(0, 0);
      },
    );
  },
);

it("keeps the four-loop ceiling when the capacity hint throws", async () => {
  let samples = 0;
  await checkLinuxCopyConcurrency(
    4,
    () => {
      throw new Error("synthetic capacity lookup failure");
    },
    () => cpuSnapshot(0, samples++ * 100),
  );
});

it("counts nice, system and interrupt CPU time as busy", async () => {
  let samples = 0;
  await checkLinuxCopyConcurrency(
    1,
    () => 4,
    () => {
      const value = samples++ * 100;
      return cpuSnapshot(0, 0).map((cpu) => ({
        ...cpu,
        times: { user: 0, nice: value, sys: value, irq: value, idle: 0 },
      }));
    },
  );
});

it.each([false, true])(
  "cancels Linux sampling when the baseline settles early (failure=%s)",
  async (fails) => {
    await fixture(async (base, database) => {
      await NodeFSP.truncate(database, 3 * 8 * 1024 * 1024);
      const destination = NodePath.join(base, "early-copy");
      const failure = Object.assign(new Error("synthetic early write failure"), { code: "ENOSPC" });
      const originalOpen = NodeFSP.open;
      const platform = vi.spyOn(NodeOS, "platform").mockReturnValue("linux");
      const capacity = vi.spyOn(NodeOS, "availableParallelism").mockReturnValue(4);
      const cpus = vi.spyOn(NodeOS, "cpus").mockReturnValue(cpuSnapshot(0, 0));
      const clone = vi.spyOn(NodeFSP, "copyFile").mockRejectedValue(unsupported());
      const open = vi.spyOn(NodeFSP, "open").mockImplementation(async (...args) => {
        const handle = await originalOpen(...args);
        if (!fails || args[0] !== destination) return handle;
        return new Proxy(handle, {
          get(target, property) {
            if (property === "write")
              return async () => {
                throw failure;
              };
            const value = Reflect.get(target, property, target);
            return typeof value === "function" ? value.bind(target) : value;
          },
        });
      });
      vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
      try {
        const copying = copyQualifiedBackupFile(database, destination);
        if (fails) {
          await NodeAssert.rejects(copying, (cause) => cause === failure);
          await NodeAssert.rejects(NodeFSP.lstat(destination), { code: "ENOENT" });
        } else {
          assert.equal((await copying).bytes, 3 * 8 * 1024 * 1024);
        }
        assert.equal(cpus.mock.calls.length, 1);
        assert.equal(vi.getTimerCount(), 0);
        await vi.advanceTimersByTimeAsync(100);
        assert.equal(cpus.mock.calls.length, 1);
      } finally {
        vi.useRealTimers();
        open.mockRestore();
        clone.mockRestore();
        cpus.mockRestore();
        capacity.mockRestore();
        platform.mockRestore();
      }
    });
  },
);

it("bypasses CPU sampling for empty, single-chunk and cloned Linux files", async () => {
  await fixture(async (base, database) => {
    const originalCopy = NodeFSP.copyFile;
    const platform = vi.spyOn(NodeOS, "platform").mockReturnValue("linux");
    const cpus = vi.spyOn(NodeOS, "cpus");
    const clone = vi.spyOn(NodeFSP, "copyFile").mockRejectedValue(unsupported());
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      for (const size of [0, 1, 8 * 1024 * 1024]) {
        await NodeFSP.truncate(database, size);
        assert.deepEqual(
          await copyQualifiedBackupFile(database, NodePath.join(base, `small-${size}`)),
          {
            method: "copy",
            bytes: size,
          },
        );
      }
      await NodeFSP.truncate(database, 2 * 8 * 1024 * 1024);
      clone.mockImplementation((source, target) =>
        originalCopy(source, target, NodeFS.constants.COPYFILE_EXCL),
      );
      assert.equal(
        (await copyQualifiedBackupFile(database, NodePath.join(base, "cloned-large"))).method,
        "clone",
      );
      assert.equal(cpus.mock.calls.length, 0);
      assert.equal(vi.getTimerCount(), 0);
    } finally {
      vi.useRealTimers();
      clone.mockRestore();
      cpus.mockRestore();
      platform.mockRestore();
    }
  });
});

it("caps idle Linux copy workers at the file's chunk count", async () => {
  let samples = 0;
  await checkLinuxCopyConcurrency(
    2,
    () => 4,
    () => cpuSnapshot(0, samples++ * 100),
    2,
  );
});

it("settles every started Linux copy loop before closing and removing a failed output", async () => {
  await fixture(async (base, database) => {
    const chunk = 8 * 1024 * 1024;
    await NodeFSP.truncate(database, 6 * chunk);
    const destination = NodePath.join(base, "settled-failure");
    const baselineEntered = Promise.withResolvers<void>();
    const baselineRelease = Promise.withResolvers<void>();
    const failedWrite = Promise.withResolvers<void>();
    const failure = Object.assign(new Error("synthetic concurrent disk full"), { code: "ENOSPC" });
    const originalOpen = NodeFSP.open;
    let samples = 0;
    let reads = 0;
    let closes = 0;
    let active = 0;
    const platform = vi.spyOn(NodeOS, "platform").mockReturnValue("linux");
    const capacity = vi.spyOn(NodeOS, "availableParallelism").mockReturnValue(4);
    const cpus = vi.spyOn(NodeOS, "cpus").mockImplementation(() => cpuSnapshot(0, samples++ * 100));
    const clone = vi.spyOn(NodeFSP, "copyFile").mockRejectedValue(unsupported());
    const open = vi.spyOn(NodeFSP, "open").mockImplementation(async (...args) => {
      const handle = await originalOpen(...args);
      return new Proxy(handle, {
        get(target, property) {
          if (property === "read")
            return async (buffer: Buffer, offset: number, length: number, position: number) => {
              reads += 1;
              active += 1;
              try {
                if (position === 0) {
                  baselineEntered.resolve();
                  await baselineRelease.promise;
                }
                return await target.read(buffer, offset, length, position);
              } finally {
                active -= 1;
              }
            };
          if (property === "write")
            return async (buffer: Buffer, offset: number, length: number, position: number) => {
              if (position !== 0) {
                failedWrite.resolve();
                throw failure;
              }
              return target.write(buffer, offset, length, position);
            };
          if (property === "close")
            return async () => {
              assert.equal(active, 0, "handles remain open until every read settles");
              closes += 1;
              await target.close();
            };
          const value = Reflect.get(target, property, target);
          return typeof value === "function" ? value.bind(target) : value;
        },
      });
    });
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const copying = copyQualifiedBackupFile(database, destination);
    const rejected = NodeAssert.rejects(copying, (cause) => cause === failure);
    try {
      await baselineEntered.promise;
      await vi.advanceTimersByTimeAsync(100);
      await failedWrite.promise;
      assert.equal(reads, 4);
      assert.equal(closes, 0);
      await NodeFSP.access(destination);
      baselineRelease.resolve();
      await rejected;
      assert.equal(closes, 2);
      assert.equal(reads, 4, "a failed loop prevents claiming additional chunks");
      await NodeAssert.rejects(NodeFSP.lstat(destination), { code: "ENOENT" });
      assert.equal(vi.getTimerCount(), 0);
    } finally {
      baselineRelease.resolve();
      await copying.catch(() => {});
      await rejected.catch(() => {});
      vi.useRealTimers();
      open.mockRestore();
      clone.mockRestore();
      cpus.mockRestore();
      capacity.mockRestore();
      platform.mockRestore();
    }
  });
});

it("reserves the exact copy rollback buffer and trial headroom before stopping", async () => {
  await fixture(async (base, database) => {
    await NodeFSP.writeFile(`${database}-wal`, "wal");
    await NodeFSP.writeFile(NodePath.join(base, "userdata", "settings.json"), "{}");
    const bytes = 20;
    const required = 2 * bytes + Math.ceil(0.05 * bytes) + 1024 * 1024;
    const adapter: QualifiedBackupAdapter = {
      copyFile: async () => {
        throw unsupported();
      },
      availableBytes: async () => required - 1,
    };
    await NodeAssert.rejects(
      preflightQualifiedBackup(base, database, adapter),
      /recovery-capacity:/,
    );
    assert.deepEqual(
      await preflightQualifiedBackup(base, database, {
        ...adapter,
        availableBytes: async () => required,
      }),
      { method: "copy", bytes },
    );
    assert.deepEqual(await NodeFSP.readdir(NodePath.join(base, "runtime", "db-backup")), []);
  });
});

it("reads protocol-one receipts with or without optional copy and sync durations", async () => {
  await fixture(async (base) => {
    const legacy = {
      protocol: 1 as const,
      updateId: "legacy",
      method: "copy" as const,
      bytes: 15,
      startedAt: "2026-10-10T12:00:00.000Z",
      completedAt: "2026-10-10T12:00:00.015Z",
      durationMs: 15,
    };
    for (const receipt of [legacy, { ...legacy, updateId: "timed", copyMs: 8, syncMs: 4 }]) {
      const directory = NodePath.join(base, "runtime", "db-backup", receipt.updateId);
      await NodeFSP.mkdir(directory, { recursive: true });
      await writeQualifiedBackupReceipt(directory, receipt);
      assert.deepEqual(await readQualifiedBackupReceipt(base, receipt.updateId), receipt);
    }
    const receiptPath = NodePath.join(base, "runtime", "db-backup", "timed", "backup-receipt.json");
    for (const durations of [{ copyMs: -1 }, { syncMs: "bad" }, { copyMs: null }]) {
      await NodeFSP.writeFile(
        receiptPath,
        JSON.stringify({ ...legacy, updateId: "timed", ...durations }),
      );
      await NodeAssert.rejects(readQualifiedBackupReceipt(base, "timed"), /receipt is invalid/);
    }
  });
});
