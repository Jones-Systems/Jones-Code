import { installEffectSeam } from "./EffectSeam.node-test-support.mjs";
installEffectSeam();
const { test } = await import("node:test");
const { nativeObservationCases } = await import("./NativeObservation.testkit.ts");
for (const [name, run] of Object.entries(nativeObservationCases)) test(name, run);
