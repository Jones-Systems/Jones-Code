// Owner-invoked only: never imports into the installed server. Output is real
// measured data or a failure, and the caller retains admission/cleanup ownership.
import { readFile, writeFile, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir, availableParallelism } from "node:os";
import { join, isAbsolute } from "node:path";
import { runNativeQualification } from "../../../apps/server/src/jones/serverParallelism/nativeQualification.ts";
import { minimalWorkerEnvironment } from "../../../apps/server/src/jones/serverCompute/workerCommand.ts";
const [manifestPath, output, profile, baselinePath] = process.argv.slice(2);
if (!manifestPath || !output || !isAbsolute(output) || !["laptop", "vps"].includes(profile))
  throw new Error(
    "Usage: qualify.mjs <command-manifest.json> <absolute report.json> <laptop|vps> [baseline-report.json]",
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
  !["source", "bundle", "electron", "sea"].includes(manifest.runtimeForm) ||
  availableParallelism() < count
)
  throw new Error("Unqualified runtime command/host");
// Never inherit manifest-supplied credential/provider environment. Heap settings
// and Electron Node mode use the exact production minimum environment policy.
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
const baseline = baselinePath ? JSON.parse(await readFile(baselinePath, "utf8")) : undefined;
let cleanupProved = false;
try {
  const report = await runNativeQualification({
    count,
    command,
    directory: join(scratch, "fixture"),
    ...(baseline ? { baselineWriterP99Ms: baseline.windows?.[0]?.writer?.p99 } : {}),
  });
  cleanupProved = report.shutdownProved;
  await writeFile(
    output,
    JSON.stringify(
      {
        ...report,
        runtimeForm: manifest.runtimeForm,
        profile,
        commandManifest: manifestPath,
        baseline: baselinePath ?? null,
        scratch,
        executed: true,
        nativePhaseCompleteness: "unavailable",
      },
      null,
      2,
    ) + "\n",
    { flag: "wx", mode: 0o600 },
  );
  if (
    !report.predicates.productiveOverlap ||
    !report.predicates.normalTargets ||
    report.predicates.writerTarget === false
  )
    process.exitCode = 1;
} catch (cause) {
  await writeFile(
    output,
    JSON.stringify(
      {
        executed: true,
        outcome: "failed",
        build: command.build,
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
  if (cleanupProved) await rm(scratch, { recursive: true });
}
