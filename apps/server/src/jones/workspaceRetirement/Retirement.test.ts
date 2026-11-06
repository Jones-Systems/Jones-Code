import { test } from "vitest";
import { retirementCases } from "./Retirement.testkit.ts";
for (const [name, run] of Object.entries(retirementCases)) test(name,run);
