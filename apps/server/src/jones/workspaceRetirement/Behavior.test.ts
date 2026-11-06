import {it} from "vitest";
import {behaviorCases} from "./Behavior.testkit.ts";
for(const [name,run] of Object.entries(behaviorCases)) it(name,run,30000);
