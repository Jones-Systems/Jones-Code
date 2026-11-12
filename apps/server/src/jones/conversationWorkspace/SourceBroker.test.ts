import { it } from "vite-plus/test";
import { sourceCases } from "./SourceBroker.testkit.ts";
it.each(Object.entries(sourceCases))("%s", (_name, run) => run());
