import { it } from "vite-plus/test";
import { retirementCases } from "./Retirement.testkit.ts";
for (const [name, run] of Object.entries(retirementCases)) it(name,run);
