import { installEffectSeam } from "./EffectSeam.node-test-support.mjs";
installEffectSeam();
const { test } = await import("node:test");
const { preserveCases } = await import("./PreserveBranch.testkit.ts");
for (const [name, run] of Object.entries(preserveCases)) test(name, run);
