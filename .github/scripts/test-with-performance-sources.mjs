import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import {
  assertQualificationDatabaseSource,
  sourceParentEnvironment,
  qualificationDatabaseSource,
  qualificationSourcePins,
} from "../../scripts/performance-staging/sources.mjs";

function groupExists(pid) {
  try {
    process.kill(-pid, 0);
    return true;
  } catch (error) {
    if (error.code === "ESRCH") return false;
    throw error;
  }
}

function signalGroup(pid, signal) {
  try {
    process.kill(-pid, signal);
  } catch (error) {
    if (error.code !== "ESRCH") throw error;
  }
}

export async function withOwnedSourceParent(workspace, signal, use) {
  const parent = NodeFS.realpathSync(NodePath.dirname(workspace));
  const root = await NodeFSP.mkdtemp(NodePath.join(parent, ".performance-sources-"));
  const identity = await NodeFSP.lstat(root, { bigint: true });
  const children = new Set();
  let unknownClose = false;
  let failure;
  let value;
  const run = async (command, args, cwd, env) => {
    signal.throwIfAborted();
    const child = NodeChildProcess.spawn(command, args, {
      cwd,
      env,
      stdio: "inherit",
      detached: true,
    });
    children.add(child);
    let killTimer;
    const stop = () => {
      if (!child.pid || !groupExists(child.pid)) return;
      signalGroup(child.pid, "SIGTERM");
      killTimer ??= setTimeout(() => {
        if (groupExists(child.pid)) signalGroup(child.pid, "SIGKILL");
      }, 2000);
    };
    signal.addEventListener("abort", stop, { once: true });
    if (signal.aborted) stop();
    let spawnError;
    const closed = await new Promise((resolve) => {
      child.once("error", (error) => {
        spawnError = error;
      });
      child.once("close", (code, childSignal) => resolve({ code, signal: childSignal }));
    });
    signal.removeEventListener("abort", stop);
    clearTimeout(killTimer);
    children.delete(child);
    if (child.pid && groupExists(child.pid)) {
      stop();
      const deadline = Date.now() + 2000;
      while (groupExists(child.pid) && Date.now() < deadline)
        await new Promise((resolve) => setTimeout(resolve, 20));
      if (groupExists(child.pid)) {
        signalGroup(child.pid, "SIGKILL");
        const reapDeadline = Date.now() + 2000;
        while (groupExists(child.pid) && Date.now() < reapDeadline)
          await new Promise((resolve) => setTimeout(resolve, 20));
      }
      unknownClose ||= groupExists(child.pid);
    }
    clearTimeout(killTimer);
    if (spawnError) throw spawnError;
    if (closed.code !== 0 || signal.aborted)
      throw Object.assign(new Error(`${command} failed (${closed.code ?? closed.signal})`), {
        exitCode: closed.code > 0 ? closed.code : 1,
      });
    if (unknownClose) throw new Error("source consumer process group closure is unproved");
  };
  try {
    value = await use({ root, run });
  } catch (error) {
    failure = error;
  }
  try {
    const current = await NodeFSP.lstat(root, { bigint: true });
    if (
      children.size ||
      unknownClose ||
      current.isSymbolicLink() ||
      current.dev !== identity.dev ||
      current.ino !== identity.ino
    )
      throw new Error(`Source checkouts retained at ${root}; ownership or closure is unproved`);
    // SIGKILL or host loss cannot run this cleanup; hosted-runner disposal is the fallback.
    await NodeFSP.rm(root, { recursive: true });
    if (NodeFS.existsSync(root)) throw new Error(`Source checkout cleanup failed at ${root}`);
  } catch (cleanupError) {
    console.error(cleanupError.message);
    failure ??= cleanupError;
  }
  if (failure) throw failure;
  return value;
}

async function main() {
  const [mode, shard] = process.argv.slice(2);
  if (
    process.argv.length !== (mode === "server" ? 4 : 3) ||
    (mode !== "packages" && mode !== "server") ||
    (mode === "server" && !/^[1-3]\/3$/.test(shard))
  )
    throw new Error("expected packages or server <1-3>/3");
  if (process.env.GITHUB_ACTIONS !== "true" || !process.env.GITHUB_WORKSPACE)
    throw new Error("source preparation requires the hosted CI workspace");
  const cancellation = new AbortController();
  const abort = () => cancellation.abort();
  process.once("SIGINT", abort);
  process.once("SIGTERM", abort);
  try {
    await withOwnedSourceParent(
      process.env.GITHUB_WORKSPACE,
      cancellation.signal,
      async ({ root, run }) => {
        const home = NodePath.join(root, "home");
        const temporary = NodePath.join(root, "tmp");
        await NodeFSP.mkdir(home);
        await NodeFSP.mkdir(temporary);
        const sourceEnvironment = {
          PATH: process.env.PATH,
          HOME: home,
          TMPDIR: temporary,
          LANG: "C.UTF-8",
          TZ: "UTC",
          CI: "true",
          GIT_CONFIG_NOSYSTEM: "1",
          GIT_CONFIG_GLOBAL: "/dev/null",
          GIT_TERMINAL_PROMPT: "0",
        };
        const environment = { ...process.env, [sourceParentEnvironment]: root };
        const sources = [];
        const verifySource = (source) => {
          assertQualificationDatabaseSource(source, environment);
          const status = NodeChildProcess.execFileSync(
            "git",
            [
              "-c",
              "core.fsmonitor=false",
              "-C",
              source.worktreePath,
              "status",
              "--porcelain",
              "--untracked-files=normal",
            ],
            { encoding: "utf8", maxBuffer: 256 * 1024, env: sourceEnvironment },
          );
          if (status !== "")
            throw new Error("prepared source contains changed or untracked inputs");
        };
        for (const pin of qualificationSourcePins) {
          const source = qualificationDatabaseSource(pin.sourceRevision, environment);
          sources.push(source);
          await NodeFSP.mkdir(source.worktreePath);
          await run("git", ["init", "--quiet"], source.worktreePath, sourceEnvironment);
          await run(
            "git",
            [
              "fetch",
              "--quiet",
              "--depth=1",
              "https://github.com/Jones-Systems/Jones-Code.git",
              pin.sourceRevision,
            ],
            source.worktreePath,
            sourceEnvironment,
          );
          await run(
            "git",
            ["checkout", "--quiet", "-b", "performance-source", "FETCH_HEAD"],
            source.worktreePath,
            sourceEnvironment,
          );
          verifySource(source);
          await run("vp", ["install", "--frozen-lockfile"], source.worktreePath, sourceEnvironment);
          verifySource(source);
        }
        let consumerFailure;
        try {
          if (mode === "server") {
            await run(
              "vp",
              ["run", "--filter", "t3", "test", "--shard", shard],
              process.env.GITHUB_WORKSPACE,
              environment,
            );
          } else {
            await run(
              "vp",
              [
                "run",
                "--parallel",
                "--concurrency-limit",
                "4",
                "--filter",
                "!t3",
                "--filter",
                "!@t3tools/monorepo",
                "--filter",
                "!@t3tools/client-runtime",
                "test",
              ],
              process.env.GITHUB_WORKSPACE,
              environment,
            );
            await run(
              "vp",
              ["run", "--filter", "@t3tools/client-runtime", "test"],
              process.env.GITHUB_WORKSPACE,
              environment,
            );
          }
        } catch (error) {
          consumerFailure = error;
        } finally {
          for (const source of sources) {
            try {
              verifySource(source);
            } catch (error) {
              console.error(error.message);
              consumerFailure ??= error;
            }
          }
        }
        if (consumerFailure) throw consumerFailure;
      },
    );
  } finally {
    process.removeListener("SIGINT", abort);
    process.removeListener("SIGTERM", abort);
  }
}

if (
  process.argv[1] &&
  NodePath.resolve(process.argv[1]) === NodeURL.fileURLToPath(import.meta.url)
) {
  main().catch((error) => {
    console.error(error.message);
    process.exitCode = error.exitCode ?? 1;
  });
}
