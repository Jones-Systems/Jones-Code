import { it } from "vite-plus/test";
import { scriptObservationCases } from "./ProjectScriptObservation.testkit.ts";
it.each(Object.entries(scriptObservationCases))("%s", (_name, run) => run());
