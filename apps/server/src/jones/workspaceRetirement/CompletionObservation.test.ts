import {it} from "vitest";
import {completionCases} from "./CompletionObservation.testkit.ts";
for(const [name,run] of Object.entries(completionCases)) it(name,run,30000);
