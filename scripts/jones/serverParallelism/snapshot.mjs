import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { measureSnapshotPipeline } from "../../../apps/server/src/jones/serverParallelism/snapshotQualification.ts";
import { snapshotDecision } from "./snapshotDecision.mjs";
const [output, count = "2000"] = process.argv.slice(2);
if (!output || !NodePath.isAbsolute(output))
  throw new Error("snapshot.mjs <absolute report.json> [thread-count]");
const required = JSON.parse(
  await NodeFSP.readFile(new URL("../../../package.json", import.meta.url), "utf8"),
).engines.node;
if (process.versions.node !== required) throw new Error(`Node ${required} required`);
let report;
try {
  const measurement = await measureSnapshotPipeline(Number(count));
  const activationDecision = snapshotDecision(measurement);
  report = {
    ...measurement,
    activationDecision,
    source: process.env.JONES_SP_SOURCE_SHA ?? null,
    threadCount: Number(count),
    outcome:
      activationDecision === "not-activated"
        ? "passed"
        : activationDecision === "activate"
          ? "failed"
          : "unknown",
    rootAction:
      activationDecision === "activate"
        ? "Implement SP-SNAPSHOT and requalify this exact candidate"
        : activationDecision === "unknown"
          ? "Attribute loop delay before deciding activation"
          : null,
    limitation:
      "Actual synthetic pipeline on this source/host; combined representative workload attribution remains separate",
  };
} catch (cause) {
  report = { outcome: "failed", activationDecision: "unknown", error: String(cause) };
}
await NodeFSP.writeFile(output, JSON.stringify(report, null, 2) + "\n", {
  flag: "wx",
  mode: 0o600,
});
if (report.outcome !== "passed") process.exitCode = 1;
