import {it} from "vitest";
import {sourceCases} from "./SourceBroker.testkit.ts";
for (const [name, run] of Object.entries(sourceCases)) it(name, run);
