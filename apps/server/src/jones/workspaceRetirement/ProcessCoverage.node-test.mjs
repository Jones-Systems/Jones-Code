import { installEffectSeam } from "./EffectSeam.node-test-support.mjs";
installEffectSeam();
const { test } = await import("node:test");
const { processCoverageCases } = await import("./ProcessCoverage.testkit.ts");
for (const [name, run] of Object.entries(processCoverageCases)) test(name, run);
