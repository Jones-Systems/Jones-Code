import { test } from "node:test";
import { runStatusCases } from "./runStatus.testkit.ts";

for (const [name, run] of Object.entries(runStatusCases)) test(name, run);
