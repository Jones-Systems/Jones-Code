import { test } from "node:test";
import { sourceCases } from "./SourceBroker.testkit.ts";
for (const [name, run] of Object.entries(sourceCases)) test(name, run);
