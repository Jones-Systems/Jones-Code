import { it } from "vite-plus/test";
import { scriptObservationCases } from "./ProjectScriptObservation.testkit.ts";
for (const [name, run] of Object.entries(scriptObservationCases)) it(name, run);
