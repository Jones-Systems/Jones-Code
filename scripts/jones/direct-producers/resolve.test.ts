import { it } from "vite-plus/test";
import { resolveCases } from "./finite-production.testkit.ts";

it.each(Object.entries(resolveCases))("%s", (_name, run) => run());
