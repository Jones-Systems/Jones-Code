import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

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

// Only captured child process groups are signalled. Close precedes scratch cleanup,
// including timeout, output overflow, and cancellation paths.
export function runOwnedChild(command, args, { signal, timeout = 15000, ...options } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      ...options,
      detached: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
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
      if (failure) reject(failure);
      else resolve({ code, signal: exitSignal, stdout, stderr });
    });
  });
}

export async function createNativeState(home, profile, metadata) {
  await fs.mkdir(path.join(home, "userdata"), { recursive: true });
  await fs.mkdir(path.join(home, "runtime"), { recursive: true });
  await fs.mkdir(profile, { recursive: true });
  const databasePath = path.join(home, "userdata", "statev2.sqlite");
  await fs.writeFile(databasePath, "synthetic application database; must never open");
  await fs.writeFile(path.join(profile, "protected-sentinel"), "unchanged");
  const active = {
    protocol: 1,
    owner: "desktop",
    generation: "previous",
    transactionId: "bootstrap",
    home: await fs.realpath(home),
    profile: await fs.realpath(profile),
    databasePath: await fs.realpath(databasePath),
    environmentId: "native-startup-qualification",
    version: metadata.version,
    sourceSha: metadata.jonesSource.sha,
    sourceTree: metadata.jonesSource.tree,
  };
  await fs.writeFile(
    path.join(home, "runtime", "jones-active-install.json"),
    JSON.stringify(active),
    { mode: 0o600 },
  );
  const transaction = path.join(home, "runtime", "jones-updates", "transactions", "f".repeat(64));
  await fs.mkdir(transaction, { recursive: true });
  await fs.writeFile(path.join(transaction, "intent.json"), "{}");
  return active;
}

export function leasePaths(active) {
  return [
    path.join(active.home, "runtime", "jones-native-writer.sqlite"),
    path.join(
      path.dirname(active.profile),
      `.jones-profile-writer-${createHash("sha256").update(active.profile).digest("hex")}.sqlite`,
    ),
  ].sort();
}

export async function initializeFixtureLeases(active) {
  for (const filename of leasePaths(active)) {
    await fs.writeFile(filename, "", { flag: "wx", mode: 0o600 });
    const scope = filename.startsWith(active.home + path.sep)
      ? `home:${active.home}`
      : `profile:${active.profile}`;
    const connection = new DatabaseSync(filename);
    try {
      connection.exec(
        "PRAGMA journal_mode=DELETE; CREATE TABLE jones_native_writer_lease (protocol INTEGER, scope TEXT);",
      );
      connection.prepare("INSERT INTO jones_native_writer_lease VALUES (1, ?)").run(scope);
    } finally {
      connection.close();
    }
    const info = await fs.stat(filename, { bigint: true });
    await fs.writeFile(
      `${filename}.identity.json`,
      JSON.stringify({ protocol: 1, scope, device: String(info.dev), inode: String(info.ino) }),
      { flag: "wx", mode: 0o600 },
    );
  }
}

export async function withExclusiveLeases(paths, body) {
  const connections = [];
  try {
    for (const filename of paths) {
      // Never hash or read these inodes while this process holds SQLite locks.
      const connection = new DatabaseSync(filename);
      connections.push(connection);
      connection.exec("PRAGMA busy_timeout = 0; BEGIN EXCLUSIVE;");
    }
    return await body();
  } finally {
    for (const connection of connections.reverse()) connection.close();
  }
}

export function assertStartupRefused(result, reason) {
  assert.equal(result.signal, null, result.stderr);
  assert.equal(result.code, 1, result.stderr);
  assert.match(result.stderr, /Jones Code refused native startup:/);
  assert.match(result.stderr, reason);
}

export async function snapshotTree(root) {
  let info;
  try {
    info = await fs.lstat(root);
  } catch (error) {
    if (error.code === "ENOENT") return null;
    throw error;
  }
  assert.ok(!info.isSymbolicLink(), `Qualification cannot follow a protected symlink: ${root}`);
  if (info.isDirectory()) {
    const children = {};
    for (const name of (await fs.readdir(root)).sort())
      children[name] = await snapshotTree(path.join(root, name));
    return { mode: info.mode, children };
  }
  assert.ok(info.isFile());
  assert.ok(info.size <= 1024 * 1024);
  return {
    mode: info.mode,
    size: info.size,
    mtimeMs: info.mtimeMs,
    sha256: createHash("sha256")
      .update(await fs.readFile(root))
      .digest("hex"),
  };
}
