import * as NodeTest from "node:test";
import { retirementCopyCases } from "./retirementCopy.testkit.ts";

for (const [name, run] of Object.entries(retirementCopyCases)) NodeTest.test(name, run);
