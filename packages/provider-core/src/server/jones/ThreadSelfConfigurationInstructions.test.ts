import { describe, expect, it } from "vite-plus/test";
import { THREAD_SELF_CONFIGURATION_INSTRUCTIONS } from "./ThreadSelfConfigurationInstructions.ts";
describe("safe thread configuration guidance", () => {
  it("distinguishes saved settings from captured runs and observations", () => {
    for (const text of [
      "t3_thread_self_configure",
      "t3_thread_configure without modelSelection",
      "Do not pass modelSelection to t3_thread_configure",
      "execution precondition",
      "subsequently admitted",
      "already captured",
      "unknown or unavailable",
      "finish the current run",
      "unknown timing field",
    ])
      expect(THREAD_SELF_CONFIGURATION_INSTRUCTIONS).toContain(text);
  });
});
