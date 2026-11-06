import * as NodeTest from "node:test";
import { behaviorCases } from "./Behavior.testkit.ts";
for (const [name, run] of Object.entries(behaviorCases)) NodeTest.test(name, run);
