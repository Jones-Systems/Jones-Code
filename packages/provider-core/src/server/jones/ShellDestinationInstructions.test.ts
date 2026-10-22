import { describe, expect, it } from "vite-plus/test";
import {
  JONES_SHELL_LANGUAGE,
  parseShellDestinationFence,
} from "@t3tools/contracts/jones/shellDestinationFence";

import {
  T3_CODE_ORCHESTRATION_INSTRUCTIONS,
  t3AcpPromptWithInstructions,
  t3OrchestrationPromptForFirstRun,
  t3OrchestrationSystemPrompt,
} from "../orchestrationInstructions.ts";
import { JONES_SHELL_DESTINATION_INSTRUCTIONS } from "./ShellDestinationInstructions.ts";

describe("shell destination instructions", () => {
  it("offers a dedicated-language example whose metadata follows the shared grammar", () => {
    const example = JONES_SHELL_DESTINATION_INSTRUCTIONS.match(/```([^\n]+)\npwd\n```/);
    expect(example).not.toBeNull();
    const opening = example?.[1] ?? "";
    expect(opening.startsWith(`${JONES_SHELL_LANGUAGE} `)).toBe(true);
    expect(parseShellDestinationFence(opening.slice(JONES_SHELL_LANGUAGE.length + 1))).toEqual({
      _tag: "Valid",
      environmentId: "REPLACE_WITH_DISCOVERED_ENVIRONMENT_ID",
      label: "Destination machine",
      title: "Check working directory",
    });
    expect(JONES_SHELL_DESTINATION_INSTRUCTIONS).toContain("placeholder, not a real environment ID");
    expect(JONES_SHELL_DESTINATION_INSTRUCTIONS).toContain("`environment=<exact ID>` exactly once");
  });

  it("teaches discovery and fails safely when a destination cannot be established", () => {
    for (const requirement of [
      "`get_invocation_context`",
      "`t3_environment_links`",
      "Never guess IDs, use an ID prefix, or resolve a target from a label",
      "Labels are display hints only",
      "Never put target metadata on a `bash`, `sh`, or plain `shell` fence",
      "older clients can execute those commands on the source environment",
      "If the target ID is unknown, use a `text` fence",
      "one interactive command line",
      "show an explicit `cd`",
      "connection to the destination and `terminal:operate` permission there",
      "Do not use a command block to bypass approval",
      "Native terminal prompts remain owner interaction",
      "emitting a block gives the agent no permission",
      "Source delivery does not install this feature",
    ]) {
      expect(JONES_SHELL_DESTINATION_INSTRUCTIONS).toContain(requirement);
    }
  });

  it("reaches shared developer, system, first-run fallback, and ACP prompt consumers", () => {
    const prompt = "Inspect the project.";
    const guidance = JONES_SHELL_DESTINATION_INSTRUCTIONS.trim();
    for (const instructions of [
      T3_CODE_ORCHESTRATION_INSTRUCTIONS,
      t3OrchestrationSystemPrompt(true),
      t3OrchestrationPromptForFirstRun({ prompt, runOrdinal: 1, hasT3Mcp: true }),
      t3AcpPromptWithInstructions({
        prompt,
        state: { interactionMode: "default", hasT3Mcp: true },
      }),
      t3AcpPromptWithInstructions({
        prompt,
        state: { interactionMode: "plan", hasT3Mcp: true },
      }),
    ]) {
      expect(instructions).toContain(guidance);
      expect(instructions?.split(guidance)).toHaveLength(2);
    }
  });

  it("retains existing MCP and repeat-injection gates", () => {
    const prompt = "Continue.";
    expect(t3OrchestrationSystemPrompt(false)).toBeUndefined();
    expect(t3OrchestrationPromptForFirstRun({ prompt, runOrdinal: 1, hasT3Mcp: false })).toBe(prompt);
    expect(t3OrchestrationPromptForFirstRun({ prompt, runOrdinal: 2, hasT3Mcp: true })).toBe(prompt);
    expect(
      t3AcpPromptWithInstructions({
        prompt,
        state: { interactionMode: "default", hasT3Mcp: false },
      }),
    ).not.toContain(JONES_SHELL_DESTINATION_INSTRUCTIONS.trim());
    const state = { interactionMode: "default", hasT3Mcp: true } as const;
    expect(t3AcpPromptWithInstructions({ prompt, state, previousState: state })).toBe(prompt);
  });
});
