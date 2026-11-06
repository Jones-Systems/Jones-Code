import { test } from "node:test";
import { behaviorCases } from "./Behavior.testkit.ts";
for (const [name, run] of Object.entries(behaviorCases)) test(name, run);
