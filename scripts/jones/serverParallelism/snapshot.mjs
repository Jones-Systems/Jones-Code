import { writeFile, readFile } from "node:fs/promises";
import { isAbsolute } from "node:path";
import { measureSnapshotPipeline } from "../../../apps/server/src/jones/serverParallelism/snapshotQualification.ts";
const [output, count = "2000"] = process.argv.slice(2);
if (!output || !isAbsolute(output)) throw new Error("snapshot.mjs <absolute report.json> [thread-count]");
const required = JSON.parse(await readFile(new URL("../../../package.json", import.meta.url), "utf8")).engines.node;
if (process.versions.node !== required) throw new Error(`Node ${required} required`);
let report;
try {
  const measurement = await measureSnapshotPipeline(Number(count));
  const complete = measurement.samples.length === 3 && measurement.samples.every((sample) =>
    [sample.captureMs, sample.decodeMs, sample.schemaProjectionMs, sample.encodeMs, sample.loopMaxMs].every(Number.isFinite));
  const trigger = measurement.contiguousEncodeTrigger || measurement.decodeProjectionAttributionRequired;
  const loopTarget = complete && measurement.samples.every((sample) => sample.loopMaxMs <= 100 && sample.loopP99Ms <= 50);
  const activationDecision = !complete ? "unknown" : trigger ? "activate" : loopTarget ? "not-activated" : "unknown";
  report = { ...measurement, activationDecision, source: process.env.JONES_SP_SOURCE_SHA ?? null,
    threadCount: Number(count), outcome: activationDecision === "not-activated" ? "passed" : activationDecision === "activate" ? "failed" : "unknown",
    rootAction: activationDecision === "activate" ? "Implement SP-SNAPSHOT and requalify this exact candidate" :
      activationDecision === "unknown" ? "Attribute loop delay before deciding activation" : null,
    limitation: "Actual synthetic pipeline on this source/host; combined representative workload attribution remains separate" };
} catch (cause) { report = { outcome: "failed", activationDecision: "unknown", error: String(cause) }; }
await writeFile(output, JSON.stringify(report, null, 2) + "\n", { flag: "wx", mode: 0o600 });
if (report.outcome !== "passed") process.exitCode = 1;
