import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeEvents from "node:events";

async function processIdentity(pid) {
  if (!pid) return null;
  try {
    const value = await NodeFSP.readFile(`/proc/${pid}/stat`, "utf8");
    return value.slice(value.lastIndexOf(")") + 2).split(" ")[19];
  } catch (error) {
    if (["ENOENT", "ESRCH"].includes(error.code)) return null;
    throw error;
  }
}

export const stateParent = () =>
  NodePath.join(
    process.env.XDG_STATE_HOME || NodePath.join(NodeOS.homedir(), ".local/state"),
    "jones-code-ui-evidence",
  );

export async function allocateRun(parent = stateParent()) {
  const scratchParent = NodePath.join(parent, "scratch");
  const registryParent = NodePath.join(parent, "registry");
  await NodeFSP.mkdir(scratchParent, { recursive: true, mode: 0o700 });
  await NodeFSP.mkdir(registryParent, { recursive: true, mode: 0o700 });
  const id = NodeCrypto.randomUUID();
  const root = await NodeFSP.mkdtemp(NodePath.join(scratchParent, `${id}-`));
  await NodeFSP.chmod(root, 0o700);
  const registry = NodePath.join(registryParent, `${id}.json`);
  const run = { id, root, registry, parent, pid: null };
  await NodeFSP.writeFile(registry, JSON.stringify(run), { mode: 0o600, flag: "wx" });
  return run;
}

export async function removeRun(run, remove = NodeFSP.rm) {
  const expectedParent = await NodeFSP.realpath(NodePath.join(run.parent, "scratch"));
  if (
    NodePath.dirname(run.root) !== expectedParent ||
    !NodePath.basename(run.root).startsWith(`${run.id}-`)
  ) {
    throw new Error("Scratch ownership could not be established");
  }
  const stat = await NodeFSP.lstat(run.root).catch((error) => {
    if (error.code === "ENOENT") return null;
    throw error;
  });
  if (stat?.isSymbolicLink()) throw new Error("Scratch root became a symlink");
  if (stat) await remove(run.root, { recursive: true, force: false });
  if (
    await NodeFSP.lstat(run.root).catch((error) => {
      if (error.code === "ENOENT") return null;
      throw error;
    })
  )
    throw new Error("Scratch removal did not complete");
  await NodeFSP.writeFile(
    run.registry,
    JSON.stringify({ ...run, cleanup: "complete", pid: null }),
    {
      mode: 0o600,
    },
  );
  return { outcome: "complete", rootAbsent: true };
}

export async function cleanupRegistered(id, parent = stateParent()) {
  if (!/^[a-f0-9-]{36}$/.test(id || "")) throw new Error("Invalid run ID");
  const registry = NodePath.join(parent, "registry", `${id}.json`);
  const run = JSON.parse(await NodeFSP.readFile(registry, "utf8"));
  if (run.id !== id || run.parent !== parent || run.registry !== registry)
    throw new Error("Registry identity mismatch");
  if (run.pid) {
    try {
      process.kill(run.pid, 0);
      throw new Error("Recorded process still exists; cleanup rejected");
    } catch (error) {
      if (error.code !== "ESRCH") throw error;
    }
  }
  return removeRun(run);
}

// Wait for the retained namespace handle to exit before deleting its writable tree.
export async function superviseRun({ run, start, signals = process, remove = removeRun }) {
  let child;
  let cancelled;
  let code = 2;
  let error;
  const forward = (signal) => {
    cancelled = signal;
    if (child && child.exitCode === null && child.signalCode === null) {
      child.kill(signal);
      escalation ??= setTimeout(() => {
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      }, 5000);
    }
  };
  const interrupt = () => forward("SIGINT");
  const terminate = () => forward("SIGTERM");
  signals.on("SIGINT", interrupt);
  signals.on("SIGTERM", terminate);
  let cleanup;
  let escalation;
  let startTicks = null;
  try {
    if (cancelled) throw new Error("Cancelled before spawn");
    child = await start();
    const closed = new Promise((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (exitCode, signal) => resolve({ exitCode, signal }));
    });
    run.pid = child.pid;
    startTicks = await processIdentity(child.pid);
    run.startTicks = startTicks;
    await NodeFSP.writeFile(run.registry, JSON.stringify(run), { mode: 0o600 });
    if (cancelled) forward(cancelled);
    const result = await closed;
    code = cancelled ? (cancelled === "SIGINT" ? 130 : 143) : (result.exitCode ?? 1);
  } catch (caught) {
    error = caught.message;
    if (child?.pid && child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
      await NodeEvents.once(child, "close").catch(() => {});
    }
    code = cancelled ? (cancelled === "SIGINT" ? 130 : 143) : 2;
  } finally {
    clearTimeout(escalation);
    try {
      cleanup = await remove(run);
    } catch (caught) {
      cleanup = { outcome: "unknown", error: caught.message };
      if (code === 0) code = 1;
    }
    signals.off("SIGINT", interrupt);
    signals.off("SIGTERM", terminate);
  }
  const currentTicks = await processIdentity(child?.pid);
  const exited =
    !currentTicks || (startTicks && currentTicks !== startTicks)
      ? true
      : child?.pid
        ? "unknown"
        : true;
  return {
    code,
    signal: cancelled ?? null,
    cleanup,
    error,
    pid: child?.pid ?? null,
    namespaceProcess: { pid: child?.pid ?? null, startTicks, exited },
  };
}
