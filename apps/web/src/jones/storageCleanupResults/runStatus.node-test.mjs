import * as NodeTest from "node:test";
import { runStatusCases } from "./runStatus.testkit.ts";

for (const [name, run] of Object.entries(runStatusCases)) NodeTest.test(name, run);
