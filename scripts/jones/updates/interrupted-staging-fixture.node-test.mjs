import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeTest from "node:test";
import {
  installedProducer,
  packagedRows,
  runSyntheticTransferRegression,
} from "./interrupted-staging-fixture.mjs";

NodeTest.test(
  "synthetic interrupted transfer proves group absence, cleans scratch and restages",
  { timeout: 120_000 },
  async () => {
    const result = await runSyntheticTransferRegression();
    NodeAssert.equal(result.status, "passed");
    NodeAssert.equal(result.packagedProof, false);
    NodeAssert.equal(result.groupAbsent, true);
    NodeAssert.equal(result.scratchCleaned, true);
    NodeAssert.equal(result.restaged, true);
    NodeAssert.notEqual(result.installedBinding, installedProducer);
  },
);

NodeTest.test(
  "unavailable packaged rows never count synthetic execution as installed producer proof",
  () => {
    const rows = packagedRows();
    NodeAssert.equal(rows.length, 3);
    for (const row of rows) {
      NodeAssert.equal(row.status, "unavailable");
      NodeAssert.equal(row.packagedProof, false);
      NodeAssert.equal(row.installedProducer, "47cad979dcb769e511581e6de9e1aa5be99fd461");
      NodeAssert.match(
        row.prerequisites.join(" "),
        /source\/tree.*archive.*payload.*milestone.*recovery-code/,
      );
    }
  },
);

NodeTest.test(
  "advisory arm64 fixture has packaging-path, label, manual and nightly coverage",
  async () => {
    const workflow = await NodeFSP.readFile(
      new URL("../../../.github/workflows/jones-update-qualification.yml", import.meta.url),
      "utf8",
    );
    NodeAssert.match(workflow, /workflow_dispatch:/);
    NodeAssert.match(workflow, /schedule:\n\s+- cron:/);
    NodeAssert.match(workflow, /types: \[opened, synchronize, reopened, labeled\]/);
    NodeAssert.match(
      workflow,
      /contains\(github.event.pull_request.labels\.\*\.name, 'qualify-interrupted-staging'\)/,
    );
    NodeAssert.match(workflow, /scripts\/jones\/updates\/\*\*/);
    NodeAssert.match(workflow, /test "\$\(uname -m\)" = arm64/);
    NodeAssert.match(workflow, /node scripts\/jones\/updates\/interrupted-staging-fixture\.mjs/);
    NodeAssert.match(workflow, /nativeEffectsRecovery\.test\.ts/);
  },
);
