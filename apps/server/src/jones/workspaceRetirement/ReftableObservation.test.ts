import { it } from "vite-plus/test";
import { reftableReaderCases } from "./ReftableObservation.testkit.ts";
it.each(Object.entries(reftableReaderCases))("%s", (_name, run) => run());
