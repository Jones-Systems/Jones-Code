import { it } from "vite-plus/test";
import { completionCases } from "./CompletionObservation.testkit.ts";
it.each(Object.entries(completionCases))("%s", (_name, run) => run());
