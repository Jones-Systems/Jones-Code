// These synthetic recovery fixtures check native file identities and durability operations.
// @effect-diagnostics nodeBuiltinImport:off
import { assert, it } from "@effect/vitest";
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
