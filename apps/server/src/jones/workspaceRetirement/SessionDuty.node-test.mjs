// Real Effect scheduling and in-memory SQLite; no sequential interpreter.
import * as NodeTest from "node:test";
import { sessionDutyCases } from "./SessionDuty.testkit.ts";
for (const [name, run] of Object.entries(sessionDutyCases)) NodeTest.test(name, run);
