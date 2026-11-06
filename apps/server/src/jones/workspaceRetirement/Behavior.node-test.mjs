// Test-only clock adapter; native filesystem, Git and proof behavior is unchanged.
import { installEffectSeam } from "./EffectSeam.node-test-support.mjs";
installEffectSeam();
import * as NodeTest from "node:test";
const { behaviorCases } = await import("./Behavior.testkit.ts");
for (const [name, run] of Object.entries(behaviorCases)) NodeTest.test(name, run);
