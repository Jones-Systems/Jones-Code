import * as NodeTest from "node:test";
import { sourceCases } from "./SourceBroker.testkit.ts";
for (const [name, run] of Object.entries(sourceCases)) NodeTest.test(name, run);
