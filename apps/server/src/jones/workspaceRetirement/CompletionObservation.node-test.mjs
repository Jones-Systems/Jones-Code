import { installEffectSeam } from "./EffectSeam.node-test-support.mjs";
installEffectSeam();
import * as NodeTest from "node:test";
const { completionCases } = await import("./CompletionObservation.testkit.ts");
for (const [name, run] of Object.entries(completionCases)) NodeTest.test(name, run);
