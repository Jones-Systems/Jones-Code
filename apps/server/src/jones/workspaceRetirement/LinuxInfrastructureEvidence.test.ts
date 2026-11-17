import { test } from "vite-plus/test";
import { infrastructureCases } from "./LinuxInfrastructureEvidence.testkit.ts";
test.each(Object.entries(infrastructureCases))("%s", (_name, run) => run());
