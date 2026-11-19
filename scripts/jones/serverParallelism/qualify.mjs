// CI/qualified-host entry. No installed server or provider environment is used.
import { qualificationFixtureId } from "../../../apps/server/src/jones/serverParallelism/qualificationFixture.ts";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { runNativeQualification } from "../../../apps/server/src/jones/serverParallelism/nativeQualification.ts";
import { minimalWorkerEnvironment } from "../../../apps/server/src/jones/serverCompute/workerCommand.ts";
import { qualificationDecision, requiredNativePredicates } from "./qualificationResult.mjs";
import { startMacResourceSampler } from "./macos-resource-sampler.mjs";
import { startLinuxResourceObserver } from "./linux-resource-observer.mjs";
const [manifestPath, output, profile, baselinePath, observerArm] = process.argv.slice(2);
if (
  !manifestPath ||
  !output ||
  !NodePath.isAbsolute(output) ||
  !["laptop", "vps"].includes(profile)
)
  throw new Error(
    "Usage: qualify.mjs <manifest.json> <absolute report.json> <laptop|vps> [baseline.json]",
  );
const required = JSON.parse(
  await NodeFSP.readFile(new URL("../../../package.json", import.meta.url), "utf8"),
).engines.node;
if (process.versions.node !== required) throw new Error(`Qualification requires Node ${required}`);
if (
  observerArm !== undefined &&
  (!["inline", "isolated"].includes(observerArm) ||
    !baselinePath ||
    profile !== "vps" ||
    Effect.runSync(HostProcess.Platform) !== "linux" ||
    process.env.GITHUB_EVENT_NAME !== "workflow_dispatch" ||
    process.env.JONES_SP_SCOPE !== "observer-isolation")
) {
  throw new Error("The Linux observer comparison requires its manual diagnostic scope");
}
const manifest = JSON.parse(await NodeFSP.readFile(manifestPath, "utf8"));
const count = profile === "vps" ? 3 : 2;
if (
  manifest.schema !== "jones.server-parallelism.command/v1" ||
  !/^[a-f0-9]{64}$/.test(manifest.build) ||
  !NodePath.isAbsolute(manifest.file) ||
  !Array.isArray(manifest.args) ||
  !manifest.args.every((arg) => typeof arg === "string") ||
  !["source", "bundle", "electron", "sea"].includes(manifest.runtimeForm) ||
  NodeOS.availableParallelism() < count
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
async function runnerMacHelper(name) {
  if (Effect.runSync(HostProcess.Platform) !== "darwin")
    throw new Error("The resource helper requires macOS");
  const executable = NodePath.join(NodeOS.tmpdir(), name);
  const stat = await NodeFSP.lstat(executable);
  if (
    !stat.isFile() ||
    stat.uid !== Effect.runSync(HostProcess.UserId) ||
    (stat.mode & 0o022) !== 0
  )
    throw new Error("The resource helper must be a runner-owned regular file");
  return executable;
}
const macNativePoolCensus =
  process.env.JONES_SP_MAC_NATIVE_POOL_CENSUS === "1"
    ? await runnerMacHelper("jones-macos-native-pool-census")
    : undefined;
const scratch = await NodeFSP.realpath(
  await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-sp-qualification-")),
);
let cleanupProved = false;
let resourceSampler;
async function retireSampler() {
  if (!resourceSampler) return undefined;
  const exit = await resourceSampler.close();
  cleanupProved &&= exit.capturedExit;
  return { ...resourceSampler.observer(), exit };
}
const controller = new AbortController();
const stop = () => controller.abort();
process.once("SIGTERM", stop);
process.once("SIGINT", stop);
try {
  const baseline = baselinePath
    ? JSON.parse(await NodeFSP.readFile(baselinePath, "utf8"))
    : undefined;
  const usable =
    baseline?.schema === "jones.server-parallelism.baseline/v1" &&
    baseline.fixtureId === qualificationFixtureId &&
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
  if (observerArm === "isolated") {
    resourceSampler = await startLinuxResourceObserver();
  } else if (process.env.JONES_SP_MAC_RESOURCE_SAMPLER === "1") {
    resourceSampler = await startMacResourceSampler(
      await runnerMacHelper("jones-macos-resource-sampler"),
    );
  }
  const report = await runNativeQualification({
    count,
    command,
    ...(macNativePoolCensus ? { macNativePoolCensus } : {}),
    ...(resourceSampler ? { sample: resourceSampler.sample } : {}),
    directory: NodePath.join(scratch, "fixture"),
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
  const resourceObserver = await retireSampler();
  const predicates = resourceObserver
    ? {
        ...report.predicates,
        resourceSamplerClosed:
          resourceObserver.exit.capturedExit && resourceObserver.exit.code === 0,
      }
    : report.predicates;
  const decision = qualificationDecision(predicates, requiredNativePredicates);
  const observationRecorded =
    observerArm !== undefined &&
    usable &&
    cleanupProved &&
    report.windows.length === 3 &&
    report.churnWindows.length === 2 &&
    predicates.resourcesComplete === true &&
    predicates.observationComplete === true &&
    predicates.generationEvidenceComplete === true &&
    (observerArm === "inline" ||
      (predicates.resourceSamplerClosed === true &&
        resourceObserver.samples === report.resources.length));
  await NodeFSP.writeFile(
    output,
    JSON.stringify(
      {
        ...report,
        ...decision,
        predicates,
        ...(observerArm !== undefined
          ? {
              eligible: false,
              passed: false,
              outcome: observationRecorded ? "recorded" : "incomplete",
              observerArm,
              qualification: decision,
            }
          : {}),
        ...(resourceObserver ? { resourceObserver } : {}),
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
  if (!(observerArm === undefined ? decision.passed : observationRecorded)) process.exitCode = 1;
} catch (cause) {
  const resourceObserver = await retireSampler();
  await NodeFSP.writeFile(
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
        ...(observerArm !== undefined ? { eligible: false, observerArm } : {}),
        ...(resourceObserver ? { resourceObserver } : {}),
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
  if (cleanupProved) await NodeFSP.rm(scratch, { recursive: true });
}
