import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

const outstandingGroups = new Set();

export function packagedStartupNames(metadata, { executableName, bundleIdentifier, bundleName }) {
  NodeAssert.equal(metadata.main, "apps/desktop/dist-electron/boot.cjs");
  const names = new Set([metadata.name, executableName, bundleIdentifier, bundleName]);
  if (metadata.productName !== undefined) names.add(metadata.productName);
  for (const name of names)
    NodeAssert.ok(
      typeof name === "string" &&
        name.length > 0 &&
        NodePath.basename(name) === name &&
        name !== "." &&
        name !== "..",
    );
  return [...names];
}

export function assertFixtureProcessesStopped() {
  NodeAssert.equal(
    outstandingGroups.size,
    0,
    "An owned process group is unresolved; fixture cleanup is held.",
  );
}

function groupExists(group) {
  try {
    process.kill(-group, 0);
    return true;
  } catch (error) {
    if (error.code === "ESRCH") return false;
    throw error;
  }
}

async function drainGroup(group) {
  for (const [signal, allowance] of [
    ["SIGTERM", 1000],
    ["SIGKILL", 4000],
  ]) {
    if (!groupExists(group)) {
      outstandingGroups.delete(group);
      return;
    }
    try {
      process.kill(-group, signal);
    } catch (error) {
      if (error.code !== "ESRCH") throw error;
    }
    const deadline = Date.now() + allowance;
    while (Date.now() < deadline) {
      if (!groupExists(group)) {
        outstandingGroups.delete(group);
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }
  if (!groupExists(group)) {
    outstandingGroups.delete(group);
    return;
  }
  throw new Error("Owned process group did not drain; fixture cleanup is held.");
}

export class FixtureOwnership {
  #paths = [];

  async #remember(filename, info) {
    this.#paths.push({
      filename,
      device: info.dev,
      inode: info.ino,
      directory: info.isDirectory(),
    });
  }

  async mkdir(filename) {
    await NodeFSP.mkdir(filename, { mode: 0o700 });
    await this.#remember(filename, await NodeFSP.lstat(filename, { bigint: true }));
  }

  async temporary(prefix) {
    const filename = await NodeFSP.mkdtemp(prefix);
    await this.#remember(filename, await NodeFSP.lstat(filename, { bigint: true }));
    return filename;
  }

  async file(filename, contents) {
    const handle = await NodeFSP.open(filename, "wx", 0o600);
    try {
      await this.#remember(filename, await handle.stat({ bigint: true }));
      await handle.writeFile(contents);
    } finally {
      await handle.close();
    }
  }

  async cleanup() {
    assertFixtureProcessesStopped();
    const failures = [];
    const preserved = [];
    for (const receipt of [...this.#paths].reverse()) {
      try {
        NodeAssert.ok(
          !preserved.some((filename) => filename.startsWith(receipt.filename + NodePath.sep)),
          `A descendant's ownership changed; preserving ${receipt.filename}`,
        );
        let current;
        try {
          current = await NodeFSP.lstat(receipt.filename, { bigint: true });
        } catch (error) {
          if (error.code === "ENOENT") continue;
          throw error;
        }
        NodeAssert.ok(
          !current.isSymbolicLink() &&
            current.dev === receipt.device &&
            current.ino === receipt.inode &&
            current.isDirectory() === receipt.directory,
          `Fixture ownership changed; preserving ${receipt.filename}`,
        );
        await NodeFSP.rm(receipt.filename, { recursive: receipt.directory, force: false });
      } catch (error) {
        failures.push(error);
        preserved.push(receipt.filename);
      }
    }
    if (failures.length)
      throw new AggregateError(
        failures,
        "Some fixture ownership could not be verified; paths were preserved.",
      );
  }
}

export function cleanLaunchEnvironment(environment, temporary) {
  const env = { ...environment };
  for (const key of Object.keys(env)) {
    if (
      /^(ELECTRON_|CHROME_|CHROMIUM_|T3CODE_|JONES_|VITE_|NODE_OPTIONS$|NODE_COMPILE_CACHE$|NODE_DISABLE_COMPILE_CACHE$|CFFIXED_USER_HOME$)/.test(
        key,
      )
    )
      delete env[key];
  }
  return {
    ...env,
    TMPDIR: temporary,
    TMP: temporary,
    TEMP: temporary,
    NODE_DISABLE_COMPILE_CACHE: "1",
  };
}

// Only captured child process groups are signalled. A leader's close is followed
// by group absence, including on timeout, output overflow, and cancellation.
export function runOwnedChild(command, args, { signal, timeout = 15000, ...options } = {}) {
  return new Promise((resolve, reject) => {
    const child = NodeChildProcess.spawn(command, args, {
      ...options,
      detached: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
    if (child.pid !== undefined) outstandingGroups.add(child.pid);
    let failure;
    let stdout = "";
    let stderr = "";
    const stop = (error) => {
      failure ??= error;
      if (child.pid !== undefined) {
        try {
          process.kill(-child.pid, "SIGKILL");
        } catch (cause) {
          if (cause.code !== "ESRCH") failure = cause;
        }
      }
    };
    const abort = () => stop(new Error("Native startup qualification cancelled."));
    const timer = setTimeout(
      () => stop(new Error("Native startup qualification child timed out.")),
      timeout,
    );
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) abort();
    child.once("error", (error) => {
      failure = error;
    });
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
      if (stdout.length > 1024 * 1024) stop(new Error("Native startup stdout exceeded its bound."));
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
      if (stderr.length > 1024 * 1024) stop(new Error("Native startup stderr exceeded its bound."));
    });
    child.once("close", (code, exitSignal) => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      const drained = child.pid === undefined ? Promise.resolve() : drainGroup(child.pid);
      void drained.then(() => {
        if (failure) reject(failure);
        else resolve({ code, signal: exitSignal, stdout, stderr });
      }, reject);
    });
  });
}

export async function createNativeState(home, profile, metadata, ownership) {
  await ownership.mkdir(home);
  await ownership.mkdir(profile);
  await NodeFSP.mkdir(NodePath.join(home, "userdata"));
  await NodeFSP.mkdir(NodePath.join(home, "runtime"));
  const databasePath = NodePath.join(home, "userdata", "statev2.sqlite");
  await NodeFSP.writeFile(databasePath, "synthetic application database; must never open", {
    flag: "wx",
  });
  await NodeFSP.writeFile(NodePath.join(profile, "protected-sentinel"), "unchanged", {
    flag: "wx",
  });
  const active = {
    protocol: 1,
    owner: "desktop",
    generation: "previous",
    transactionId: "bootstrap",
    home: await NodeFSP.realpath(home),
    profile: await NodeFSP.realpath(profile),
    databasePath: await NodeFSP.realpath(databasePath),
    environmentId: "native-startup-qualification",
    version: metadata.version,
    sourceSha: metadata.jonesSource.sha,
    sourceTree: metadata.jonesSource.tree,
  };
  await NodeFSP.writeFile(
    NodePath.join(home, "runtime", "jones-active-install.json"),
    JSON.stringify(active),
    { flag: "wx", mode: 0o600 },
  );
  const transaction = NodePath.join(
    home,
    "runtime",
    "jones-updates",
    "transactions",
    "f".repeat(64),
  );
  await NodeFSP.mkdir(NodePath.join(home, "runtime", "jones-updates"));
  await NodeFSP.mkdir(NodePath.dirname(transaction));
  await NodeFSP.mkdir(transaction);
  await NodeFSP.writeFile(NodePath.join(transaction, "intent.json"), "{}", { flag: "wx" });
  return active;
}

export function leasePaths(active) {
  return [
    NodePath.join(active.home, "runtime", "jones-native-writer.sqlite"),
    NodePath.join(
      NodePath.dirname(active.profile),
      `.jones-profile-writer-${NodeCrypto.createHash("sha256").update(active.profile).digest("hex")}.sqlite`,
    ),
  ].sort();
}

export async function initializeFixtureLeases(active, ownership) {
  for (const filename of leasePaths(active)) {
    await ownership.file(filename, "");
    const scope = filename.startsWith(active.home + NodePath.sep)
      ? `home:${active.home}`
      : `profile:${active.profile}`;
    const connection = new NodeSqlite.DatabaseSync(filename);
    try {
      connection.exec(
        "PRAGMA journal_mode=DELETE; CREATE TABLE jones_native_writer_lease (protocol INTEGER, scope TEXT);",
      );
      connection.prepare("INSERT INTO jones_native_writer_lease VALUES (1, ?)").run(scope);
    } finally {
      connection.close();
    }
    const info = await NodeFSP.stat(filename, { bigint: true });
    await ownership.file(
      `${filename}.identity.json`,
      JSON.stringify({ protocol: 1, scope, device: String(info.dev), inode: String(info.ino) }),
    );
  }
}

export async function withExclusiveLeases(paths, body) {
  const connections = [];
  try {
    for (const filename of paths) {
      // Never hash or read these inodes while this process holds SQLite locks.
      const connection = new NodeSqlite.DatabaseSync(filename);
      connections.push(connection);
      connection.exec("PRAGMA busy_timeout = 0; BEGIN EXCLUSIVE;");
    }
    return await body();
  } finally {
    for (const connection of connections.reverse()) connection.close();
  }
}

export function assertStartupRefused(result, reason) {
  NodeAssert.equal(result.signal, null, result.stderr);
  NodeAssert.equal(result.code, 1, result.stderr);
  NodeAssert.match(result.stderr, /Jones Code refused native startup:/);
  NodeAssert.match(result.stderr, reason);
}

export async function snapshotTree(root) {
  let info;
  try {
    info = await NodeFSP.lstat(root);
  } catch (error) {
    if (error.code === "ENOENT") return null;
    throw error;
  }
  NodeAssert.ok(!info.isSymbolicLink(), `Qualification cannot follow a protected symlink: ${root}`);
  if (info.isDirectory()) {
    const children = {};
    for (const name of (await NodeFSP.readdir(root)).sort())
      children[name] = await snapshotTree(NodePath.join(root, name));
    return { mode: info.mode, children };
  }
  NodeAssert.ok(info.isFile());
  NodeAssert.ok(info.size <= 1024 * 1024);
  return {
    mode: info.mode,
    size: info.size,
    mtimeMs: info.mtimeMs,
    sha256: NodeCrypto.createHash("sha256")
      .update(await NodeFSP.readFile(root))
      .digest("hex"),
  };
}
