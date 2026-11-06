// Only the explicit-proof timestamp formatter is substituted; Git, descriptors and proof algorithms execute production code.
import { installEffectSeam } from "./EffectSeam.node-test-support.mjs";
installEffectSeam();
import { test } from "node:test";
import { behaviorCases } from "./Behavior.testkit.ts";
for (const [name, run] of Object.entries(behaviorCases)) test(name, run);
