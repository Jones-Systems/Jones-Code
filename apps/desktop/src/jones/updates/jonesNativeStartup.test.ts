// @effect-diagnostics nodeBuiltinImport:off -- Runs the pinned Electron executable as Node with fixture-only SQLite state.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "vite-plus/test";
import desktopPackage from "../../../package.json" with { type: "json" };
import { holdJonesDesktopNativeWriterFence } from "./jonesNativeStartup.ts";
import { resolveJonesColdBackendPermit } from "./jonesColdBackendCustody.ts";

it("keeps warm startup unchanged and refuses cold startup before profile access", () => {
  const input = {
    platform: "linux",
    env: {},
    homeDirectory: "/must-not-access/home",
    appDataDirectory: "/must-not-access/profile",
    version: "fixture",
    metadata: { name: "t3code" },
  };
  holdJonesDesktopNativeWriterFence(input);
  expect(resolveJonesColdBackendPermit()).toBeUndefined();
  expect(() =>
    holdJonesDesktopNativeWriterFence({
      ...input,
      env: { T3CODE_JONES_COLD_CHANNEL_FD: "8" },
    }),
  ).toThrow("Cold custody binding or lifetime requires reconciliation.");
  expect(resolveJonesColdBackendPermit).toThrow();
  expect(() => holdJonesDesktopNativeWriterFence(input)).toThrow();
});

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
      database.exec("PRAGMA journal_mode=DELETE; CREATE TABLE lease (identity TEXT); INSERT INTO lease VALUES ('fixture'); BEGIN;");
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

it("admits secondary forwarding before claiming the established primary owner", async () => {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-secondary-owner-"));
  try {
    const home = await NodeFSP.realpath(root);
    const profile = NodePath.join(home, "profile");
    const databasePath = NodePath.join(home, "userdata", "statev2.sqlite");
    await NodeFSP.mkdir(profile);
    await NodeFSP.mkdir(NodePath.dirname(databasePath));
    await NodeFSP.mkdir(NodePath.join(home, "runtime"));
    await NodeFSP.writeFile(databasePath, "opaque state");
    const metadata = {
      name: "t3code",
      jonesSource: {
        repository: "Jones-Systems/Jones-Code",
        sha: "a".repeat(40),
        tree: "b".repeat(40),
      },
    };
    await NodeFSP.writeFile(
      NodePath.join(home, "runtime", "jones-active-install.json"),
      JSON.stringify({
        protocol: 1,
        owner: "desktop",
        home,
        databasePath,
        profile,
        environmentId: "fixture",
        version: "fixture",
        sourceSha: metadata.jonesSource.sha,
        sourceTree: metadata.jonesSource.tree,
      }),
      { mode: 0o600 },
    );
    const input = {
      platform: "darwin",
      env: { T3CODE_HOME: home, T3CODE_DESKTOP_USER_DATA_DIR: profile },
      homeDirectory: home,
      appDataDirectory: home,
      version: "fixture",
      metadata,
    };
    const moduleUrl = new URL("./jonesNativeStartup.ts", import.meta.url).href;
    const secondary =
      "const m = await import(" +
      JSON.stringify(moduleUrl) +
      "); m.holdJonesDesktopNativeWriterFence(" +
      JSON.stringify(input) +
      "); process.stdout.write('forwarding-admitted');";
    const script = [
      "const m = await import(" + JSON.stringify(moduleUrl) + ");",
      "const cp = await import('node:child_process');",
      "m.holdJonesDesktopNativeWriterFence(" + JSON.stringify(input) + ");",
      "const owner = m.currentJonesDesktopOwner()(); owner.assertCurrent();",
      "const secondary = cp.spawnSync(process.execPath, ['--input-type=module', '-e', " +
        JSON.stringify(secondary) +
        "], { encoding: 'utf8', timeout: 10000 });",
      "if (secondary.status !== 0 || secondary.stdout !== 'forwarding-admitted') throw new Error(secondary.stderr);",
      "const contender = cp.spawnSync(process.execPath, ['--input-type=module', '-e', " +
        JSON.stringify(secondary + " m.currentJonesDesktopOwner()();") +
        "], { encoding: 'utf8', timeout: 10000 });",
      "if (contender.status === 0) throw new Error('Competing primary acquired ownership');",
      "owner.assertCurrent();",
    ].join("\n");
    const result = NodeChildProcess.spawnSync(
      process.execPath,
      ["--input-type=module", "-e", script],
      { encoding: "utf8", timeout: 25000, maxBuffer: 1024 * 1024 },
    );
    expect(result.status, result.stderr).toBe(0);
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
    await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
  }
});
