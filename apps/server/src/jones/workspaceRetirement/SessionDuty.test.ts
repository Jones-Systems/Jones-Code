import { test } from "vite-plus/test";
import { sessionDutyCases } from "./SessionDuty.testkit.ts";
test.each(Object.entries(sessionDutyCases))("%s", async (_name, run) => {
  await run();
});
