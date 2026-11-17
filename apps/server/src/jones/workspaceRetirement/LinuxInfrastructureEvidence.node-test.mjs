import { installEffectSeam } from "./EffectSeam.node-test-support.mjs";
installEffectSeam();
const { test } = await import("node:test");
const { infrastructureCases } = await import("./LinuxInfrastructureEvidence.testkit.ts");
for (const [name, run] of Object.entries(infrastructureCases)) test(name, run);
