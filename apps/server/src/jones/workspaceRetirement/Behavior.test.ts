import { it } from "vite-plus/test";
import { behaviorCases } from "./Behavior.testkit.ts";
it.each(Object.entries(behaviorCases))("%s", (_name, run) => run());
