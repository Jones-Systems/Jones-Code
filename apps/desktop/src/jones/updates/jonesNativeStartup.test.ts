// @effect-diagnostics nodeBuiltinImport:off -- Runs the pinned Electron executable as Node with fixture-only SQLite state.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "vite-plus/test";
import desktopPackage from "../../../package.json" with { type: "json" };

it("the pinned Electron runtime supports the same SQLite reader/exclusive locking protocol as Python", async () => {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-electron-lease-"));
  const lease = NodePath.join(root, "lease.sqlite");
  try {
    const require = NodeModule.createRequire(import.meta.url);
    const electron = require("electron") as string;
    const python = String.raw`
import sqlite3, sys
connection = sqlite3.connect(sys.argv[1], timeout=0, isolation_level=None)
try:
    connection.execute('BEGIN EXCLUSIVE')
except sqlite3.OperationalError as error:
    assert 'locked' in str(error), error
    assert sys.argv[2] == 'held'
else:
    assert sys.argv[2] == 'free'
finally: connection.close()
`;
    const script = `
      if (process.versions.electron !== ${JSON.stringify(desktopPackage.dependencies.electron)})
        throw new Error('Unexpected Electron runtime');
      const { DatabaseSync } = process.getBuiltinModule('node:sqlite');
      const database = new DatabaseSync(${JSON.stringify(lease)});
      database.exec('PRAGMA journal_mode=DELETE; CREATE TABLE lease (identity TEXT); INSERT INTO lease VALUES (\'fixture\'); BEGIN;');
      database.prepare('SELECT identity FROM lease').get();
      require('node:child_process').execFileSync('python3', ['-c', ${JSON.stringify(python)}, ${JSON.stringify(lease)}, 'held']);
      database.close();
    `;
    const result = NodeChildProcess.spawnSync(electron, ["-e", script], {
      env: { ...process.env, ELECTRON_RUN_AS_NODE: "1" },
      encoding: "utf8",
      timeout: 15000,
      maxBuffer: 1024 * 1024,
    });
    expect(result.status, result.stderr).toBe(0);
    NodeChildProcess.execFileSync("python3", ["-c", python, lease, "free"], { timeout: 5000 });
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
    await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
  }
});
