import { assert, describe, it } from "@effect/vitest";
import { OrchestratorMcpDelegateTaskInput } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import {
  T3_CODE_ORCHESTRATION_INSTRUCTIONS,
  t3AcpPromptWithInstructions,
  t3OrchestrationPromptForFirstRun,
  t3OrchestrationSystemPrompt,
} from "./orchestrationInstructions.ts";

describe("T3 orchestration provider instructions", () => {
  it("distinguishes delegated subagents from ordinary top-level threads", () => {
    assert.include(T3_CODE_ORCHESTRATION_INSTRUCTIONS, "Use `delegate_task`");
    assert.include(T3_CODE_ORCHESTRATION_INSTRUCTIONS, "ordinary top-level T3 conversations");
    assert.include(T3_CODE_ORCHESTRATION_INSTRUCTIONS, "Never use them merely");
    assert.include(T3_CODE_ORCHESTRATION_INSTRUCTIONS, "cross-provider");
    assert.include(T3_CODE_ORCHESTRATION_INSTRUCTIONS, "call `delegate_task` again");
    assert.include(
      T3_CODE_ORCHESTRATION_INSTRUCTIONS,
      "Do not use `t3_thread_send` on `childThreadId`",
    );
  });

  it("routes separate-session delegation through delegate_task with valid workspace examples", () => {
    const instructions = T3_CODE_ORCHESTRATION_INSTRUCTIONS;
    assert.include(instructions, "runs in its own provider session and backing thread");
    assert.include(instructions, "including same-provider work such as Claude to Claude");
    assert.include(instructions, "asked for a separate session");
    assert.include(
      instructions,
      "when you are a Claude agent asked to make subagents, or the child should be a Claude agent, create app-owned `delegate_task` children with a Claude target even when a native Claude subagent tool supports the chosen model",
    );
    assert.include(instructions, "For other providers, prefer native subagent tools");
    assert.include(instructions, "Only when the user asks for a standalone top-level thread");
    assert.notInclude(instructions, "For independent implementation or a PR stack");
    // The delegated examples must be inputs delegate_task actually accepts.
    const examples = [
      ...instructions.matchAll(
        /^- (?:New worktree|Existing worktree|Another project): `(\{"task".*\})`$/gm,
      ),
    ];
    assert.lengthOf(examples, 3);
    const decode = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskInput);
    const decoded = examples.map(([, json]) => decode(JSON.parse(json ?? "")));
    assert.deepEqual(
      decoded.map((input) => input.workspaceStrategy?.type),
      ["worktree", "existing_worktree", "worktree"],
    );
    // Only the cross-project example names a project, and it carries its own workspace.
    assert.deepEqual(
      decoded.map((input) => input.projectId !== undefined),
      [false, false, true],
    );
    assert.include(instructions, "Another project requires `workspaceStrategy`");
  });

  it("documents structured schedules instead of JSON strings", () => {
    assert.include(T3_CODE_ORCHESTRATION_INSTRUCTIONS, "structured object, never as JSON text");
    assert.include(T3_CODE_ORCHESTRATION_INSTRUCTIONS, '"everyMs":3600000');
    assert.include(T3_CODE_ORCHESTRATION_INSTRUCTIONS, "bindToCurrentThread=false");
  });

  it("injects prompt fallback only for an MCP-enabled first run", () => {
    const prompt = "Inspect the repository.";
    const injected = t3OrchestrationPromptForFirstRun({
      prompt,
      runOrdinal: 1,
      hasT3Mcp: true,
    });

    assert.include(injected, "<t3_code_orchestration_instructions>");
    assert.include(injected, `<user_request>\n${prompt}\n</user_request>`);
    assert.equal(
      t3OrchestrationPromptForFirstRun({ prompt, runOrdinal: 2, hasT3Mcp: true }),
      prompt,
    );
    assert.equal(
      t3OrchestrationPromptForFirstRun({ prompt, runOrdinal: 1, hasT3Mcp: false }),
      prompt,
    );
  });

  it("only exposes the system prompt when the T3 MCP server is attached", () => {
    assert.equal(t3OrchestrationSystemPrompt(false), undefined);
    assert.equal(t3OrchestrationSystemPrompt(true), T3_CODE_ORCHESTRATION_INSTRUCTIONS);
  });

  it("gives ACP sessions provider-neutral mode, browser, and orchestration guidance", () => {
    const injected = t3AcpPromptWithInstructions({
      prompt: "Inspect the repository.",
      state: { interactionMode: "default", hasT3Mcp: true },
    });

    assert.include(injected, "Jones Code interaction mode: Default");
    assert.include(injected, "Jones Code collaborative browser");
    assert.include(injected, "Jones Code orchestration");
    assert.include(injected, "<user_request>\nInspect the repository.\n</user_request>");
  });

  it("reinjects ACP guidance only when mode or tool availability changes", () => {
    const prompt = "Continue.";
    const defaultState = { interactionMode: "default", hasT3Mcp: true } as const;

    assert.equal(
      t3AcpPromptWithInstructions({ prompt, state: defaultState, previousState: defaultState }),
      prompt,
    );
    assert.include(
      t3AcpPromptWithInstructions({
        prompt,
        state: { ...defaultState, interactionMode: "plan" },
        previousState: defaultState,
      }),
      "Jones Code interaction mode: Plan",
    );
    const withoutMcp = t3AcpPromptWithInstructions({
      prompt,
      state: { interactionMode: "default", hasT3Mcp: false },
    });
    assert.include(withoutMcp, "Jones Code interaction mode: Default");
    assert.notInclude(withoutMcp, "Jones Code collaborative browser");
    assert.notInclude(withoutMcp, "Jones Code orchestration");
  });
});
