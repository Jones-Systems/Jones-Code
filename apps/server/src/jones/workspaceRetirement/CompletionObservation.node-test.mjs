import {test} from "node:test";
import {completionCases} from "./CompletionObservation.testkit.ts";
for(const [name,run] of Object.entries(completionCases)) test(name,run);
