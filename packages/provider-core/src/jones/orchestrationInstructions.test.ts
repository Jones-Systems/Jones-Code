import { assert, it } from "@effect/vitest";
import { OrchestratorMcpDelegateTaskInput } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import {
  T3_CODE_ORCHESTRATION_INSTRUCTIONS,
  T3_CODE_ORCHESTRATION_SECTIONS,
  t3AcpPromptWithInstructions,
} from "../server/orchestrationInstructions.ts";

it("routes actual owner actions to durable open input without plan mode or duplicate permissions", () => {
  for (const text of [
    "t3_request_user_response",
    "up to five brief Markdown questions",
    "put involved context in chat first",
    "Free text is always available",
    "omit choices unless real alternatives exist",
    "Only claim it was created after acceptance",
    "Do not create a request for finished results",
    "never enter app/provider Plan mode",
    "Native permission prompts remain authoritative",
    "Codex may use native asynchronous user input",
  ]) {
    assert.include(T3_CODE_ORCHESTRATION_INSTRUCTIONS, text);
  }
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
  assert.include(
    instructions,
    "It lists a short set of models per provider; pass `search` to find others.",
  );
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

it("keeps ordinary thread navigation separate from delegated task ownership", () => {
  const instructions = T3_CODE_ORCHESTRATION_INSTRUCTIONS;
  assert.include(
    instructions,
    "it appears under Delegated tasks in this thread's Linked section and is managed with `task_status` and `task_cancel`",
  );
  assert.include(
    instructions,
    "create ordinary top-level T3 conversations with no parent link: they are not subagents, `task_status`/`task_cancel` cannot manage them, and this thread gains only a navigation-only Linked entry.",
  );
  assert.include(
    instructions,
    "Use them only when the user explicitly asks for standalone separate/new/top-level threads or conversations.",
  );
  assert.notInclude(instructions, "they never appear in this thread's Lineage");
});

it("distinguishes independent review from a guarded original-session follow-up", () => {
  const instructions = T3_CODE_ORCHESTRATION_INSTRUCTIONS;
  assert.include(instructions, "For an independent T3 delegated review round");
  assert.include(instructions, "use `t3_task_continue` with the source `taskId`");
  assert.include(instructions, "It resumes the original provider session or refuses");
  assert.include(instructions, "Acceptance is not proof of continuation");
  assert.include(instructions, "Repeat the same request and key to read current evidence");
  assert.include(instructions, "Do not use `t3_thread_send` on `childThreadId`");
  assert.notInclude(instructions, "For every T3 delegated review round");
});

it("carries handoff provenance and requires receiver acceptance before taking ownership", () => {
  const instructions = T3_CODE_ORCHESTRATION_INSTRUCTIONS;
  assert.include(
    instructions,
    "To continue or hand off work in another thread, use `t3_thread_handoff`",
  );
  assert.include(
    instructions,
    "origin title, full thread ID, environment name and ID, return route, goal, scope, status and sources",
  );
  assert.include(
    instructions,
    "Ownership stays offered until the receiving thread accepts with `t3_thread_link` action=accept",
  );
  assert.include(
    instructions,
    "when you receive one, accept it when you take the work on and report back by its return route",
  );
});

it("retains Jones identity and whole upstream-sized instruction segments", () => {
  const injected = t3AcpPromptWithInstructions({
    prompt: "Inspect the repository.",
    state: { interactionMode: "default", hasT3Mcp: true },
  });
  assert.include(injected, "Jones Code interaction mode: Default");
  assert.include(injected, "Jones Code collaborative browser");
  assert.include(injected, "Jones Code orchestration");
  for (const section of T3_CODE_ORCHESTRATION_SECTIONS)
    assert.isBelow(Buffer.byteLength(section), 4000);
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

it("keeps compatible upstream delivery and cancellation rules", () => {
  for (const text of [
    "Follow a running delegate's `next` instruction",
    "Start no subagents unless your task says to",
    '`delivery: "file"`',
    "cancel it and start no replacement",
    "Use a PNG file and its path",
  ])
    assert.include(T3_CODE_ORCHESTRATION_INSTRUCTIONS, text);
});
