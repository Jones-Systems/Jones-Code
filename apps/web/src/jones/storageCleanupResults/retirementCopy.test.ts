import { it } from "vite-plus/test";
import { retirementCopyCases } from "./retirementCopy.testkit.ts";

it.each(Object.entries(retirementCopyCases))("%s", (_name, run) => run());
