import { it } from "vite-plus/test";
import { qualificationCases } from "./Qualification.testkit.ts";
it.each(Object.entries(qualificationCases))("%s", (_name, run) => run());
