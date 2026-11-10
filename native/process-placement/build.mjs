import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeURL from "node:url";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";

const hash = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
export async function buildPlacement({
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Plain MJS build boundary supports Node 22 without TS loading; tests inject this input.
  platform = NodeOS.platform(),
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Native compiler architecture default stays in the plain MJS boundary; tests inject it.
  architecture = NodeOS.arch(),
  source = NodeURL.fileURLToPath(new URL("./placement.c", import.meta.url)),
  outputDir = NodeURL.fileURLToPath(new URL("../../apps/server/dist/native", import.meta.url)),
  compiler = "cc",
  compilerArgs = [],
  signal,
  removeScratch = NodeFS.rmSync,
  complete = async (input) => {
    const { completeProducer } = await import("../../scripts/jones/direct-producers/completion.ts");
    completeProducer(input);
  },
} = {}) {
  if (platform !== "linux") return;
  const sourceBytes = NodeFS.readFileSync(source);
  NodeFS.mkdirSync(outputDir, { recursive: true });
  const staging = NodeFS.mkdtempSync(NodePath.join(outputDir, ".placement-build-"));
  const name = `process-placement-linux-${architecture}`;
  let primaryFailure;
  let outcome = "failed";
  try {
    const sourceCopy = NodePath.join(staging, "placement.c");
    const candidate = NodePath.join(staging, name);
    NodeFS.writeFileSync(sourceCopy, sourceBytes);
    const status = await new Promise((accept, reject) => {
      const child = NodeChildProcess.spawn(
        compiler,
        [
          ...compilerArgs,
          "-O2",
          "-std=c11",
          "-Wall",
          "-Wextra",
          "-Werror",
          sourceCopy,
          "-o",
          candidate,
        ],
        {
          stdio: "inherit",
          env: { ...process.env, TMPDIR: staging },
        },
      );
      const abort = () => child.kill("SIGTERM");
      signal?.addEventListener("abort", abort, { once: true });
      if (signal?.aborted) abort();
      let spawnError;
      child.once("error", (cause) => {
        spawnError = cause;
      });
      child.once("close", (code, terminated) => {
        signal?.removeEventListener("abort", abort);
        if (signal?.aborted) outcome = "cancelled";
        if (spawnError) {
          reject(spawnError);
          return;
        }
        accept(signal?.aborted ? 143 : (code ?? (terminated ? 143 : 1)));
      });
    });
    if (status !== 0)
      throw Object.assign(new Error(`Placement compiler failed (${status})`), { exitCode: status });
    NodeFS.chmodSync(candidate, 0o755);
    const manifest = `${JSON.stringify(
      {
        version: 1,
        architecture,
        sourceSha256: hash(sourceBytes),
        helperSha256: hash(NodeFS.readFileSync(candidate)),
      },
      null,
      2,
    )}\n`;
    NodeFS.writeFileSync(`${candidate}.json`, manifest);
    NodeFS.renameSync(candidate, NodePath.join(outputDir, name));
    NodeFS.renameSync(`${candidate}.json`, NodePath.join(outputDir, `${name}.json`));
    outcome = "succeeded";
  } catch (cause) {
    primaryFailure = cause;
    throw cause;
  } finally {
    try {
      removeScratch(staging, { recursive: true, force: true });
    } catch (cleanupError) {
      outcome = primaryFailure === undefined ? "failed" : outcome;
      if (primaryFailure === undefined) throw cleanupError;
      primaryFailure.cleanupError = cleanupError;
      console.error(`Placement scratch cleanup failed: ${cleanupError.message}`);
    } finally {
      try {
        await complete({
          producer: "jones-process-placement",
          worktree: NodeURL.fileURLToPath(new URL("../../", import.meta.url)),
          status: outcome,
          roots: [
            NodePath.resolve(outputDir, name),
            NodePath.resolve(outputDir, `${name}.json`),
            NodePath.resolve(staging),
          ],
        });
      } catch {
        console.error(`[jones-process-placement] completion metadata unavailable; outcome=${outcome}.`);
      }
    }
  }
}

if (
  process.argv[1] !== undefined &&
  NodePath.resolve(process.argv[1]) === NodeURL.fileURLToPath(import.meta.url)
) {
  const cancellation = new AbortController();
  const stop = () => cancellation.abort();
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
  try {
    await buildPlacement({ signal: cancellation.signal });
  } catch (cause) {
    console.error(cause.message);
    process.exitCode = cause.exitCode ?? 1;
  } finally {
    process.removeListener("SIGTERM", stop);
    process.removeListener("SIGINT", stop);
  }
}
