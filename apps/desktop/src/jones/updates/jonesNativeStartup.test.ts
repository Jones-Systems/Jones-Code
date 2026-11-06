// @effect-diagnostics nodeBuiltinImport:off -- Runs the pinned Electron executable as Node with fixture-only SQLite state.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "vite-plus/test";
import desktopPackage from "../../../package.json" with { type: "json" };
import {
  holdJonesDesktopNativeWriterFence,
  recoverJonesNativeStartup,
  settleInterruptedJonesActivation,
  reportJonesNativeStartupRefusal,
} from "./jonesNativeStartup.ts";
import { JonesNativeStartupRefusal } from "@t3tools/shared/jones/nativeWriterFence";
import { jonesNativeHelperSource } from "./jonesNativeHelperSource.ts";
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

it("rechecks exactly once after settlement and never repeats another refusal", () => {
  const transactionId = "c".repeat(64);
  let admissions = 0;
  const settled: string[] = [];
  const admit = () => {
    admissions++;
    if (admissions === 1)
      throw new JonesNativeStartupRefusal("transaction-settleable", "interrupted", transactionId);
    return undefined;
  };
  expect(recoverJonesNativeStartup(admit, (id) => settled.push(id))).toBeUndefined();
  expect(admissions).toBe(2);
  expect(settled).toEqual([transactionId]);
  admissions = 0;
  expect(() =>
    recoverJonesNativeStartup(
      () => {
        admissions++;
        throw new JonesNativeStartupRefusal(
          "transaction-settleable",
          "still interrupted",
          transactionId,
        );
      },
      (id) => settled.push(id),
    ),
  ).toThrow("still interrupted");
  expect(admissions).toBe(2);
  expect(settled).toEqual([transactionId, transactionId]);
});

const refusalCodes = [
  "transaction-unsettled",
  "transaction-settleable",
  "unstarted-activation",
  "trial-permit-mismatch",
  "generation-changed",
  "ownership-unknown",
  "paths-differ",
  "lease-lost",
  "writer-mismatch",
  "foreign-state",
] as const;

it.each(refusalCodes)("reports %s before stderr and exit without opening state", (code) => {
  const transactionId = "c".repeat(64);
  const calls: string[] = [];
  const failure = new JonesNativeStartupRefusal(
    code,
    "native failure\n" + "x".repeat(2000),
    transactionId,
  );
  reportJonesNativeStartupRefusal(failure, {
    showErrorBox: (title, content) => {
      expect(title).not.toBe("");
      expect(content).toContain(code);
      expect(content).toContain(transactionId);
      expect(content).toContain("docs/user/updating.md#if-an-update-fails");
      calls.push("dialog");
    },
    writeStderr: (line) => {
      expect(line).toMatch(/^Jones Code refused native startup: /);
      expect(line.length).toBeLessThan(600);
      expect(line.split("\n")).toHaveLength(2);
      calls.push("stderr");
    },
    exit: (status) => {
      expect(status).toBe(1);
      calls.push("exit");
    },
  });
  expect(calls).toEqual(["dialog", "stderr", "exit"]);
  if (code !== "transaction-settleable") {
    let attempts = 0;
    expect(() =>
      recoverJonesNativeStartup(
        () => {
          throw failure;
        },
        () => {
          attempts++;
        },
      ),
    ).toThrow(failure);
    expect(attempts).toBe(0);
  }
});

it("exits even if both refusal output channels fail", () => {
  const calls: string[] = [];
  reportJonesNativeStartupRefusal(new Error("unknown"), {
    showErrorBox: () => {
      calls.push("dialog");
      throw new Error("unavailable dialog");
    },
    writeStderr: () => {
      calls.push("stderr");
      throw new Error("closed stderr");
    },
    exit: (code) => {
      expect(code).toBe(1);
      calls.push("exit");
    },
  });
  expect(calls).toEqual(["dialog", "stderr", "exit"]);
});

async function settlementFixture(body: (home: string) => Promise<void>) {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-startup-settle-"));
  try {
    const home = await NodeFSP.realpath(root);
    await NodeFSP.mkdir(NodePath.join(home, "runtime", "jones-updates"), {
      recursive: true,
      mode: 0o700,
    });
    await body(home);
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
    await expect(NodeFSP.lstat(root)).rejects.toMatchObject({ code: "ENOENT" });
  }
}

it("materializes the current embedded helper exclusively and bounds its exact invocation", async () =>
  settlementFixture(async (home) => {
    const transactionId = "c".repeat(64);
    const env = { FIXTURE: "yes" };
    const hash = NodeCrypto.createHash("sha256").update(jonesNativeHelperSource).digest("hex");
    const helper = NodePath.join(home, "runtime", "jones-updates", "activate-v1-" + hash + ".py");
    let attempts = 0;
    const run = (
      command: string,
      args: readonly string[],
      options: NodeChildProcess.SpawnSyncOptionsWithStringEncoding,
    ) => {
      attempts++;
      expect(command).toBe("/usr/bin/python3");
      expect(args).toEqual([
        helper,
        "--manifest",
        NodePath.join(home, "runtime", "jones-active-install.json"),
        "--settle-interrupted-activation",
        transactionId,
      ]);
      expect(options).toMatchObject({ env, encoding: "utf8", timeout: 10000, maxBuffer: 65536 });
      return { status: 0, signal: null, stdout: '{"protocol":1,"status":"settled"}' };
    };
    settleInterruptedJonesActivation({ home, transactionId, env }, run);
    expect(await NodeFSP.readFile(helper, "utf8")).toBe(jonesNativeHelperSource);
    const inode = (await NodeFSP.lstat(helper)).ino;
    expect((await NodeFSP.lstat(helper)).mode & 0o777).toBe(0o700);
    settleInterruptedJonesActivation({ home, transactionId, env }, run);
    expect((await NodeFSP.lstat(helper)).ino).toBe(inode);
    expect(attempts).toBe(2);
    await NodeFSP.writeFile(helper, "foreign helper");
    expect(() => settleInterruptedJonesActivation({ home, transactionId, env }, run)).toThrow(
      "differs",
    );
    expect(await NodeFSP.readFile(helper, "utf8")).toBe("foreign helper");
    expect(attempts).toBe(2);
    await NodeFSP.unlink(helper);
    await NodeFSP.symlink(NodePath.join(home, "absent"), helper);
    expect(() => settleInterruptedJonesActivation({ home, transactionId, env }, run)).toThrow();
    expect((await NodeFSP.lstat(helper)).isSymbolicLink()).toBe(true);
    expect(attempts).toBe(2);
  }));

it("refuses busy, uncertain, malformed and unsuccessful helper evidence without another fence check", async () =>
  settlementFixture(async (home) => {
    const transactionId = "c".repeat(64);
    const results = [
      {
        status: 0,
        signal: null,
        stdout: '{"protocol":1,"status":"refused","reason":"activation-running"}',
      },
      { status: 0, signal: null, stdout: '{"protocol":1,"status":"uncertain"}' },
      { status: 0, signal: null, stdout: '{"protocol":2,"status":"settled"}' },
      { status: 0, signal: null, stdout: '{"protocol":1,"status":"settled","extra":true}' },
      { status: 0, signal: null, stdout: '{"protocol":1,"status":"settled","reason":3}' },
      { status: 0, signal: null, stdout: "not JSON" },
      { status: 1, signal: null, stdout: '{"protocol":1,"status":"settled"}' },
      { status: null, signal: "SIGTERM" as const, stdout: "", error: new Error("timeout") },
      { status: null, signal: null, stdout: "", error: new Error("spawn failed") },
    ];
    for (const result of results) {
      let admissions = 0,
        attempts = 0;
      expect(() =>
        recoverJonesNativeStartup(
          () => {
            admissions++;
            throw new JonesNativeStartupRefusal(
              "transaction-settleable",
              "interrupted",
              transactionId,
            );
          },
          (id) =>
            settleInterruptedJonesActivation({ home, transactionId: id, env: {} }, () => {
              attempts++;
              return result;
            }),
        ),
      ).toThrow();
      expect(admissions).toBe(1);
      expect(attempts).toBe(1);
    }
    for (const env of [
      { T3CODE_JONES_TRIAL_DESCRIPTOR: "trial" },
      { T3CODE_JONES_COLD_CHANNEL_FD: "8" },
    ]) {
      let attempts = 0;
      expect(() =>
        settleInterruptedJonesActivation({ home, transactionId, env }, () => {
          attempts++;
          return results[0]!;
        }),
      ).toThrow("ownership");
      expect(attempts).toBe(0);
    }
  }));
