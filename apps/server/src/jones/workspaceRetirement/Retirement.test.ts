import { it } from "vite-plus/test";
import { retirementCases } from "./Retirement.testkit.ts";
it.each(Object.entries(retirementCases))("%s", (_name, run) => run());
