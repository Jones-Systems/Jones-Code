import { it } from "vite-plus/test";
import { runStatusCases } from "./runStatus.testkit.ts";

it.each(Object.entries(runStatusCases))("%s", (_name, run) => run());
