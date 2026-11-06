import { installEffectSeam } from "./EffectSeam.node-test-support.mjs";
installEffectSeam();
const { test } = await import("node:test");
const { retirementCases } = await import("./Retirement.testkit.ts");
for (const [name, run] of Object.entries(retirementCases)) test(name, run);
