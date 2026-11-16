import { it } from "vite-plus/test";
import { processCoverageCases } from "./ProcessCoverage.testkit.ts";
it.each(Object.entries(processCoverageCases))("%s", (_name, run) => run());
