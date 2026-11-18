import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { measureSnapshotPipeline } from "../../../apps/server/src/jones/serverParallelism/snapshotQualification.ts";
import { snapshotDecision, snapshotPhaseLoopBounds } from "./snapshotDecision.mjs";
const [output, count = "2000"] = process.argv.slice(2);
if (!output || !NodePath.isAbsolute(output))
  throw new Error("snapshot.mjs <absolute report.json> [thread-count]");
const required = JSON.parse(
  await NodeFSP.readFile(new URL("../../../package.json", import.meta.url), "utf8"),
).engines.node;
if (process.versions.node !== required) throw new Error(`Node ${required} required`);
const rootActions = {
  "incomplete-measurement": "Obtain complete timing and event-loop samples for all three windows",
  "inconsistent-threshold-flags": "Reconcile the reported threshold flags with the timed windows",
  "synchronous-encode": "Implement SP-SNAPSHOT and requalify this exact candidate",
  "decode-or-projection-attribution":
    "Measure contiguous blocking in the flagged decode or projection windows",
  "loop-target-exceeded": "Attribute the event-loop target breaches in the flagged windows",
  "elapsed-phases-bounded": null,
  "within-thresholds": null,
};
let report;
try {
  const measurement = await measureSnapshotPipeline(Number(count));
  const decision = snapshotDecision(measurement);
  const { activationDecision, activationReason } = decision;
  report = {
    ...measurement,
    ...decision,
    phaseLoopBounds: snapshotPhaseLoopBounds(measurement),
    source: process.env.JONES_SP_SOURCE_SHA ?? null,
    threadCount: Number(count),
    outcome:
      activationDecision === "not-activated"
        ? "passed"
        : activationDecision === "activate"
          ? "failed"
          : "unknown",
    rootAction: rootActions[activationReason],
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
