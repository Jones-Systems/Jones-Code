import { test } from "node:test";
import { scriptObservationCases } from "./ProjectScriptObservation.testkit.ts";
for (const [name, run] of Object.entries(scriptObservationCases)) test(name, run);
