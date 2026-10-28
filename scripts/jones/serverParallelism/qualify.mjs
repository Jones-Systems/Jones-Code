// CI/qualified-host entry. No installed server or provider environment is used.
import { readFile, writeFile, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir, availableParallelism } from "node:os";
import { join, isAbsolute } from "node:path";
import { runNativeQualification } from "../../../apps/server/src/jones/serverParallelism/nativeQualification.ts";
import { minimalWorkerEnvironment } from "../../../apps/server/src/jones/serverCompute/workerCommand.ts";
import { qualificationDecision, requiredNativePredicates } from "./qualificationResult.mjs";
const [manifestPath, output, profile, baselinePath] = process.argv.slice(2);
if (!manifestPath || !output || !isAbsolute(output) || !["laptop", "vps"].includes(profile))
  throw new Error(
    "Usage: qualify.mjs <manifest.json> <absolute report.json> <laptop|vps> [baseline.json]",
  );
const required = JSON.parse(
  await readFile(new URL("../../../package.json", import.meta.url), "utf8"),
).engines.node;
if (process.versions.node !== required) throw new Error(`Qualification requires Node ${required}`);
const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
const count = profile === "vps" ? 3 : 2;
if (
  manifest.schema !== "jones.server-parallelism.command/v1" ||
  !/^[a-f0-9]{64}$/.test(manifest.build) ||
  !isAbsolute(manifest.file) ||
  !Array.isArray(manifest.args) ||
  !manifest.args.every((arg) => typeof arg === "string") ||
  !["source", "bundle", "electron", "sea"].includes(manifest.runtimeForm) ||
  availableParallelism() < count
)
  throw new Error("Unqualified runtime command/host");
const command = {
  file: manifest.file,
  args: manifest.args,
  build: manifest.build,
  env: minimalWorkerEnvironment(
    process.env,
    manifest.runtimeForm === "sea",
    manifest.runtimeForm === "electron",
  ),
};
const scratch = await realpath(await mkdtemp(join(tmpdir(), "jones-sp-qualification-")));
let cleanupProved = false;
const controller = new AbortController();
const stop = () => controller.abort();
process.once("SIGTERM", stop);
process.once("SIGINT", stop);
try {
  const baseline = baselinePath ? JSON.parse(await readFile(baselinePath, "utf8")) : undefined;
  const usable =
    baseline?.schema === "jones.server-parallelism.baseline/v1" &&
    baseline.profile === profile &&
    typeof process.env.JONES_SP_SOURCE_SHA === "string" &&
    /^[a-f0-9]{40}$/.test(process.env.JONES_SP_SOURCE_SHA) &&
    baseline.source === process.env.JONES_SP_SOURCE_SHA &&
    baseline.outcome === "passed" &&
    baseline.cleanupProved === true &&
    baseline.windows?.length >= 3 &&
    baseline.acquisitions >= 500 &&
    baseline.files === 512 &&
    [baseline.writer?.p95, baseline.writer?.p99, baseline.loop?.p99].every(
      (value) => Number.isFinite(value) && value > 0,
    );
  const report = await runNativeQualification({
    count,
    command,
    directory: join(scratch, "fixture"),
    signal: controller.signal,
    onShutdown: () => {
      cleanupProved = true;
    },
    ...(usable
      ? {
          baselineWriterP99Ms: baseline.writer.p99,
          baselineWriterP95Ms: baseline.writer.p95,
          baselineLoopP99Ms: baseline.loop.p99,
          baselineChurn: baseline.churnWindows,
        }
      : {}),
  });
  const decision = qualificationDecision(report.predicates, requiredNativePredicates);
  await writeFile(
    output,
    JSON.stringify(
      {
        ...report,
        ...decision,
        source: process.env.JONES_SP_SOURCE_SHA ?? null,
        sourceTree: process.env.JONES_SP_SOURCE_TREE ?? null,
        runtimeForm: manifest.runtimeForm,
        profile,
        commandManifest: manifestPath,
        baseline: baselinePath ?? null,
        baselineUsable: usable,
        scratch,
        executed: true,
        cleanupProved,
        nativePhaseCompleteness: "unavailable",
      },
      null,
      2,
    ) + "\n",
    { flag: "wx", mode: 0o600 },
  );
  if (!decision.passed) process.exitCode = 1;
} catch (cause) {
  await writeFile(
    output,
    JSON.stringify(
      {
        executed: true,
        outcome: "failed",
        build: command.build,
        source: process.env.JONES_SP_SOURCE_SHA ?? null,
        runtimeForm: manifest.runtimeForm,
        profile,
        scratch,
        cleanupProved,
        error: String(cause),
      },
      null,
      2,
    ) + "\n",
    { flag: "wx", mode: 0o600 },
  );
  process.exitCode = 1;
} finally {
  process.removeListener("SIGTERM", stop);
  process.removeListener("SIGINT", stop);
  if (cleanupProved) await rm(scratch, { recursive: true });
}
