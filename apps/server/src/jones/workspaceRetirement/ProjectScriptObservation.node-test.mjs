import * as NodeTest from "node:test";
import { scriptObservationCases } from "./ProjectScriptObservation.testkit.ts";
for (const [name, run] of Object.entries(scriptObservationCases)) NodeTest.test(name, run);
