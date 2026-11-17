import { installEffectSeam } from "./EffectSeam.node-test-support.mjs";
installEffectSeam();
const { test } = await import("node:test");
const { presenceCases } = await import("./TargetPresence.testkit.ts");
for (const [name, run] of Object.entries(presenceCases)) test(name, run);
