// @effect-diagnostics nodeBuiltinImport:off -- Each child owns its process-lifetime lease and exits before fixture removal.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "vite-plus/test";
import { nativeWriterLeasePaths } from "./nativeWriterFence.ts";

const moduleUrl = new URL("./nativeWriterFence.ts", import.meta.url).href;
const inspectLock = String.raw`
import sqlite3, sys
connection = sqlite3.connect(sys.argv[1], timeout=0, isolation_level=None)
try:
    connection.execute('BEGIN EXCLUSIVE')
except sqlite3.OperationalError as error:
    assert 'locked' in str(error), error
    assert sys.argv[2] == 'held'
else:
    assert sys.argv[2] == 'free'
finally:
    connection.close()
`;

async function fixture(body: (root: string, input: Record<string, unknown>) => Promise<void>) {
  const temporary = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-native-fence-"));
  const root = await NodeFSP.realpath(temporary);
  try {
    const profile = NodePath.join(root, "profile");
    const databasePath = NodePath.join(root, "userdata", "statev2.sqlite");
    await NodeFSP.mkdir(profile);
    await NodeFSP.mkdir(NodePath.dirname(databasePath));
    await NodeFSP.mkdir(NodePath.join(root, "runtime"));
    await NodeFSP.writeFile(databasePath, "opaque application state");
    const active = {
      protocol: 1, owner: "desktop", generation: "previous", transactionId: "bootstrap",
      home: root, databasePath, profile, environmentId: "fixture", version: "fixture-version",
      sourceSha: "a".repeat(40), sourceTree: "b".repeat(40),
    };
    await NodeFSP.writeFile(
      NodePath.join(root, "runtime", "jones-active-install.json"),
      JSON.stringify(active),
      { mode: 0o600 },
    );
    await body(root, {
      ...active,
      buildMetadata: { jonesSource: { repository: "Jones-Systems/Jones-Code", sha: active.sourceSha, tree: active.sourceTree } },
    });
  } finally {
    await NodeFSP.rm(temporary, { recursive: true, force: true });
    await expect(NodeFSP.lstat(temporary)).rejects.toMatchObject({ code: "ENOENT" });
  }
}

function run(input: Record<string, unknown>, after = "", before = "") {
  return NodeChildProcess.spawnSync(process.execPath, [
    "--input-type=module", "-e",
    `const module = await import(${JSON.stringify(moduleUrl)});
     ${before}
     const input = ${JSON.stringify(input)};
     module.holdJonesNativeWriterFence(input);
     ${after}`,
  ], { encoding: "utf8", timeout: 10000, maxBuffer: 1024 * 1024 });
}

it("holds real Python writers until process exit and shares one connection across duplicate imports", async () =>
  fixture(async (root, input) => {
    const probe = `
      const childProcess = await import('node:child_process');
      const duplicate = await import(${JSON.stringify(moduleUrl + "?second-bundle")});
      duplicate.holdJonesNativeWriterFence(input);
      for (const lease of module.nativeWriterLeasePaths(input.home, input.profile))
        childProcess.execFileSync('python3', ['-c', ${JSON.stringify(inspectLock)}, lease.path, 'held']);
    `;
    const result = run(input, probe);
    expect(result.status, result.stderr).toBe(0);
    for (const lease of nativeWriterLeasePaths(root, String(input.profile))) {
      NodeChildProcess.execFileSync("python3", ["-c", inspectLock, lease.path, "free"], { timeout: 5000 });
    }
  }));

it("publishes one stable lease identity when two first starts race", async () =>
  fixture(async (root, input) => {
    const children: NodeChildProcess.ChildProcess[] = [];
    try {
      const starts = [0, 1].map(() => {
        const child = NodeChildProcess.spawn(process.execPath, [
          "--input-type=module", "-e",
          `const module = await import(${JSON.stringify(moduleUrl)});
           process.stdout.write('ready');
           await new Promise(resolve => process.stdin.once('data', resolve));
           process.stdin.destroy();
           module.holdJonesNativeWriterFence(${JSON.stringify(input)});`,
        ], { stdio: ["pipe", "pipe", "pipe"], timeout: 10000 });
        children.push(child);
        const ready = new Promise<void>((resolve, reject) => {
          child.stdout.once("data", () => resolve());
          child.once("error", reject);
          child.once("exit", () => reject(new Error("Lease contender exited before admission.")));
        });
        const exited = new Promise<number | null>((resolve, reject) => {
          child.once("error", reject);
          child.once("exit", (code) => resolve(code));
        });
        return { child, ready, exited };
      });
      await Promise.all(starts.map(({ ready }) => ready));
      for (const { child } of starts) child.stdin.end("start");
      const outcomes = await Promise.all(starts.map(({ exited }) => exited));
      // A contender that observes publication before its witness may fail closed.
      expect(outcomes).toContain(0);
      expect(run(input).status).toBe(0);
      for (const lease of nativeWriterLeasePaths(root, String(input.profile))) {
        const info = await NodeFSP.lstat(lease.path, { bigint: true });
        const witness = JSON.parse(await NodeFSP.readFile(`${lease.path}.identity.json`, "utf8"));
        expect(witness.inode).toBe(String(info.ino));
        expect(info.nlink).toBe(1n);
        expect((await NodeFSP.readdir(NodePath.dirname(lease.path))).filter((name) => name.endsWith(".pending"))).toEqual([]);
      }
    } finally {
      await Promise.all(children.map(async (child) => {
        if (child.exitCode !== null || child.signalCode !== null) return;
        const closed = new Promise<void>((resolve) => child.once("close", () => resolve()));
        child.kill("SIGKILL");
        await closed;
      }));
    }
  }));

it.each([
  "shared-profile", "aliased-profile", "missing-profile", "dangling-profile-alias",
  "aliased-userdata", "missing-database-alias", "database-symlink", "missing-owner-manifest",
])("refuses %s from another home while Python owns native exclusive leases", async (fault) =>
  fixture(async (root, input) => {
    expect(run(input).status).toBe(0);
    const leases = nativeWriterLeasePaths(root, String(input.profile));
    const foreignHome = NodePath.join(root, "alternate-home");
    await NodeFSP.mkdir(foreignHome);
    let databasePath = NodePath.join(foreignHome, "userdata", "statev2.sqlite");
    let profile: string | undefined = String(input.profile);
    if (fault === "aliased-profile" || fault === "dangling-profile-alias") {
      profile = NodePath.join(foreignHome, "profile-alias");
      await NodeFSP.symlink(String(input.profile), profile, "dir");
    }
    if (fault === "missing-profile" || fault === "dangling-profile-alias")
      await NodeFSP.rename(String(input.profile), `${input.profile}.retained`);
    if (fault === "aliased-userdata" || fault === "missing-database-alias") {
      profile = undefined;
      await NodeFSP.symlink(NodePath.dirname(String(input.databasePath)), NodePath.dirname(databasePath), "dir");
      if (fault === "missing-database-alias")
        await NodeFSP.rename(String(input.databasePath), `${input.databasePath}.retained`);
    }
    if (fault === "database-symlink") {
      profile = undefined;
      await NodeFSP.mkdir(NodePath.dirname(databasePath));
      await NodeFSP.symlink(String(input.databasePath), databasePath);
    }
    if (fault === "missing-owner-manifest") {
      profile = undefined;
      databasePath = String(input.databasePath);
      await NodeFSP.unlink(NodePath.join(root, "runtime", "jones-active-install.json"));
    }
    const marker = NodePath.join(root, "caller-write");
    const foreign = { ...input, home: foreignHome, databasePath, profile };
    const script = `
      const module = await import(${JSON.stringify(moduleUrl)});
      module.holdJonesNativeWriterFence(${JSON.stringify(foreign)});
      const fs = await import('node:fs'); fs.writeFileSync(${JSON.stringify(marker)}, 'unsafe');
    `;
    const exclusiveProbe = String.raw`
import json, sqlite3, subprocess, sys
connections = []
try:
    for path in json.loads(sys.argv[1]):
        connection = sqlite3.connect(path, timeout=0, isolation_level=None)
        connections.append(connection)
        connection.execute('BEGIN EXCLUSIVE')
    result = subprocess.run(json.loads(sys.argv[2]), capture_output=True, text=True, timeout=10)
    assert result.returncode != 0, result.stdout
    assert 'startup held' in result.stderr, result.stderr
finally:
    for connection in connections: connection.close()
`;
    NodeChildProcess.execFileSync("python3", [
      "-c", exclusiveProbe, JSON.stringify(leases.map((lease) => lease.path)),
      JSON.stringify([process.execPath, "--input-type=module", "-e", script]),
    ], { timeout: 15000 });
    expect(NodeFS.existsSync(marker)).toBe(false);
  }));

it.each(["intent", "quiescent", "swapped", "rollback-intent", "blocked"])(
  "denies ordinary startup before any caller write during %s",
  async (phase) => fixture(async (root, input) => {
    const handle = "f".repeat(64);
    const directory = NodePath.join(root, "runtime", "jones-updates", "transactions", handle);
    await NodeFSP.mkdir(directory, { recursive: true });
    await NodeFSP.writeFile(NodePath.join(directory, "journal.json"), JSON.stringify({
      phase, intent: { protocol: 1, transactionId: handle },
    }));
    const marker = NodePath.join(root, "profile", "caller-write");
    const result = run(input, `const fs = await import('node:fs'); fs.writeFileSync(${JSON.stringify(marker)}, 'unsafe');`);
    expect(result.status).not.toBe(0);
    expect(NodeFS.existsSync(marker)).toBe(false);
  }),
);

it("admits only the exact source-bound trial permit before application state opens", async () =>
  fixture(async (root, input) => {
    const handle = "f".repeat(64);
    const directory = NodePath.join(root, "runtime", "jones-updates", "transactions", handle);
    await NodeFSP.mkdir(directory, { recursive: true });
    const descriptorPath = NodePath.join(directory, "trial-descriptor.json");
    const staged = { handle: "a".repeat(64), version: "candidate", sourceSha: "c".repeat(40), sourceTree: "d".repeat(40) };
    await NodeFSP.writeFile(descriptorPath, JSON.stringify({
      ...input, ...staged, stagedHandle: staged.handle, protocol: 1, startupGateProtocol: 1, transactionId: handle,
    }));
    await NodeFSP.writeFile(NodePath.join(directory, "journal.json"), JSON.stringify({
      phase: "trial", intent: { protocol: 1, transactionId: handle, staged },
    }));
    const candidate = {
      ...input, descriptorPath, version: staged.version,
      buildMetadata: { jonesSource: { repository: "Jones-Systems/Jones-Code", sha: staged.sourceSha, tree: staged.sourceTree } },
    };
    const allowed = run(candidate);
    expect(allowed.status, allowed.stderr).toBe(0);
    expect(run({ ...candidate, descriptorPath: undefined }).status).not.toBe(0);
    expect(run({ ...candidate, buildMetadata: input.buildMetadata }).status).not.toBe(0);
    const descriptor = JSON.parse(await NodeFSP.readFile(descriptorPath, "utf8"));
    delete descriptor.stagedHandle;
    await NodeFSP.writeFile(descriptorPath, JSON.stringify(descriptor));
    expect(run(candidate).status).not.toBe(0);
  }));

it("admits a legacy committed generation with an inherited equal-ID descriptor", async () =>
  fixture(async (root, input) => {
    const transactionId = "f".repeat(64);
    const directory = NodePath.join(root, "runtime", "jones-updates", "transactions", transactionId);
    await NodeFSP.mkdir(directory, { recursive: true });
    const descriptorPath = NodePath.join(directory, "trial-descriptor.json");
    await NodeFSP.writeFile(descriptorPath, JSON.stringify({ ...input, transactionId }));
    await NodeFSP.writeFile(NodePath.join(directory, "journal.json"), JSON.stringify({
      phase: "resumed", intent: { protocol: 1, transactionId, staged: { handle: transactionId } },
    }));
    await NodeFSP.writeFile(NodePath.join(root, "runtime", "jones-active-install.json"), JSON.stringify({
      ...input, transactionId, generation: transactionId,
    }));
    const result = run({ ...input, descriptorPath });
    expect(result.status, result.stderr).toBe(0);
  }));
it.each(["wal", "replaced", "missing-witness"])("refuses a %s lease without repairing its inode", async (fault) =>
  fixture(async (root, input) => {
    expect(run(input).status).toBe(0);
    const lease = nativeWriterLeasePaths(root, String(input.profile))[0]!;
    if (fault === "wal") {
      NodeChildProcess.execFileSync("python3", ["-c", "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); c.execute('PRAGMA journal_mode=WAL'); c.close()", lease.path], { timeout: 5000 });
    } else if (fault === "replaced") {
      await NodeFSP.rename(lease.path, `${lease.path}.retained`);
      await NodeFSP.copyFile(`${lease.path}.retained`, lease.path);
    } else {
      await NodeFSP.unlink(`${lease.path}.identity.json`);
    }
    const inode = (await NodeFSP.lstat(lease.path)).ino;
    expect(run(input).status).not.toBe(0);
    expect((await NodeFSP.lstat(lease.path)).ino).toBe(inode);
  }));

it("loads SQLite only for native ownership and denies a qualified runtime without it", async () =>
  fixture(async (root, input) => {
    expect(run(input, "", "process.getBuiltinModule = undefined;").status).not.toBe(0);
    await NodeFSP.unlink(NodePath.join(root, "runtime", "jones-active-install.json"));
    const ordinary = run({ ...input, buildMetadata: {} }, "", "process.getBuiltinModule = undefined;");
    expect(ordinary.status, ordinary.stderr).toBe(0);
    expect(run({ ...input, descriptorPath: "missing" }).status).not.toBe(0);
  }));

