import { it } from "vite-plus/test";
import { presenceCases } from "./TargetPresence.testkit.ts";
it.each(Object.entries(presenceCases))("%s", (_name, run) => run());
