import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import {
  coldMaintenanceCases,
  coldOwnerCases,
  coldPreparationCases,
  readColdMaintenanceSources,
  runColdMaintenanceFixture,
  runColdOwnerFixture,
  runColdPreparationFixture,
} from "./legacy-cold-maintenance-fixture.mjs";

// oxlint-disable-next-line t3code/no-global-process-runtime -- These standalone tests qualify the actual native identity ABI on an owned synthetic family.
const supportedNative = process.platform === "darwin" && process.arch === "arm64";

NodeTest.test("cold fixture composes the checked standalone source", async () => {
  const sources = await readColdMaintenanceSources();
  NodeAssert.ok(sources.maintenance.endsWith("    sys.exit(maintenance_main())\n"));
  NodeAssert.match(sources.nativeSourceSha256, /^[a-f0-9]{64}$/);
});

for (const scenario of coldOwnerCases) {
  NodeTest.test(`owned mechanical cold owner: ${scenario}`, { timeout: 30000 }, async (context) => {
    const report = await runColdOwnerFixture(scenario, { signal: context.signal });
    NodeAssert.deepEqual(report, {
      case: scenario,
      passed: true,
      qualification: "owned-mechanical-cold-owner",
      stateAdmissionAllowed: false,
      nativeColdOwnerQualified: false,
    });
  });
}

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

for (const scenario of coldPreparationCases) {
  NodeTest.test(`owned captured preparation: ${scenario}`, { timeout: 30000 }, async (context) => {
    NodeAssert.deepEqual(await runColdPreparationFixture(scenario, { signal: context.signal }), {
      case: scenario,
      passed: true,
      qualification: "owned-mechanical-captured-preparation",
      stateAdmissionAllowed: false,
      nativeColdOwnerQualified: false,
      sqliteQualified: false,
    });
  });
}
