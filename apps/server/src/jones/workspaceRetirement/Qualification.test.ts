import { it } from "vite-plus/test";
import { qualificationCases, reftableAvailable, reftableCases } from "./Qualification.testkit.ts";
it.each(Object.entries(qualificationCases))("%s", (_name, run) => run());
it.skipIf(!reftableAvailable).each(Object.entries(reftableCases))("%s", (_name, run) => run());
