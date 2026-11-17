import { it } from "vite-plus/test";
import {
  qualificationCases,
  recoverabilityCases,
  reftableAvailable,
  reftableCases,
} from "./Qualification.testkit.ts";
it.each(Object.entries(qualificationCases))("%s", (_name, run) => run());
it.each(Object.entries(recoverabilityCases))("%s", (_name, run) => run());
it.skipIf(!reftableAvailable).each(Object.entries(reftableCases))("%s", (_name, run) => run());
