import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import {
  coldMaintenanceCases,
  readColdMaintenanceSources,
  runColdMaintenanceFixture,
} from "./legacy-cold-maintenance-fixture.mjs";

// oxlint-disable-next-line t3code/no-global-process-runtime -- These standalone tests qualify the actual native identity ABI on an owned synthetic family.
const supportedNative = process.platform === "darwin" && process.arch === "arm64";

NodeTest.test("cold fixture composes the checked standalone source", async () => {
  const sources = await readColdMaintenanceSources();
  NodeAssert.ok(sources.maintenance.endsWith("    sys.exit(maintenance_main())\n"));
  NodeAssert.match(sources.nativeSourceSha256, /^[a-f0-9]{64}$/);
});

for (const scenario of coldMaintenanceCases) {
  NodeTest.test(
    `pre-state native maintenance: ${scenario}`,
    { skip: !supportedNative, timeout: 120000 },
    async (context) => {
      const report = await runColdMaintenanceFixture(scenario, { signal: context.signal });
      NodeAssert.equal(report.qualification, "synthetic-pre-state-same-kernel");
      NodeAssert.equal(report.stateAdmissionAllowed, false);
    },
  );
}
