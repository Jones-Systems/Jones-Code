import { assert, describe, it } from "@effect/vitest";

import { buildRuntimeInstructions } from "./runtimeInstructions.ts";

describe("buildRuntimeInstructions", () => {
  it("keeps runtime labels and file citation guidance with an identified source-less build", () => {
    const instructions = buildRuntimeInstructions({
      harness: " \n Codex ",
      model: " test-model ",
      modelName: " Test\n Model ",
      reasoningEffort: " high ",
      appIdentity: {
        productId: "jones-code",
        productName: "Jones Code",
        version: "0.1.0-test",
        source: null,
      },
      pullRequestLinking: false,
    });
    assert.include(
      instructions,
      "through the Codex harness, as Test Model (model slug: test-model) with high reasoning effort",
    );
    assert.include(instructions, "Build source commit is unavailable");
    assert.notInclude(instructions, "App identity and build source are unavailable");
    assert.include(instructions, "[http.ts:42](/abs/path/http.ts:42)");
    assert.notInclude(instructions, "<pull_request_linking>");
  });

  it("leaves PR linking to a session that already carries it, and keeps runtime identity", () => {
    const instructions = buildRuntimeInstructions({
      harness: "Antigravity",
      model: "gemini-3.8-flash",
      pullRequestLinking: false,
    });

    assert.include(instructions, "through the Antigravity harness, as gemini-3.8-flash.");
    assert.notInclude(instructions, "<pull_request_linking>");
  });

  it("keeps absent identity unknown and describes the supplied Jones build", () => {
    const unknown = buildRuntimeInstructions({ harness: "Codex" });
    assert.include(unknown, "App identity and build source are unavailable");
    assert.notInclude(unknown, "running in T3 Code");
    const identified = buildRuntimeInstructions({
      harness: "Codex",
      appIdentity: {
        productId: "jones-code",
        productName: "Jones Code",
        version: "0.1.0-test",
        source: {
          repository: "Jones-Systems/Jones-Code",
          sha: "a".repeat(40),
          tree: "b".repeat(40),
        },
      },
    });
    assert.include(identified, "running in Jones Code through the Codex harness");
    assert.include(identified, "commit " + "a".repeat(40));
    assert.include(identified, "tree " + "b".repeat(40));
    assert.include(identified, "a connected desktop or mobile client may have a different version");
  });

  it("links PRs the agent creates, takes over, or pushes to, and no others", () => {
    const instructions = buildRuntimeInstructions({ harness: "Codex" });

    assert.include(
      instructions,
      "As your next call, use link_pull_request for each PR you create, take over, or push to.",
    );
    assert.include(instructions, "every stack layer");
    assert.include(instructions, "Skip read-only and background PRs.");
    assert.include(instructions, "Use full URLs and report failures.");
    assert.notInclude(instructions, "git log -1");
    assert.notInclude(instructions, "work on");
    assert.include(instructions, "list_thread_pull_requests");
    assert.include(instructions, "find and link missing work PRs");
  });

  it("waits on a PR only with the watch, never with a poll loop", () => {
    const instructions = buildRuntimeInstructions({ harness: "Codex" });

    assert.notInclude(instructions, "Creation, pending checks, or a merge ask alone is not enough");
    assert.include(instructions, "Links and watches persist across pushes.");
    // The wake message itself carries the do-not-re-watch rule.
    assert.notInclude(instructions, "On a wake");
    assert.include(
      instructions,
      "Call watch_pull_request when asked to monitor or babysit, or when assigned work still needs checks or reviews.",
    );
    assert.notInclude(instructions, "merge it now");
    assert.include(instructions, "Rearm if new work needs observation.");
    assert.include(instructions, "Recheck on a watch event, owner request, or new PR action.");
    assert.include(
      instructions,
      "An automatic goal turn alone needs no PR check or status update.",
    );
    assert.include(instructions, "then watch and end your turn");
    assert.include(instructions, "never watch, even if the brief says babysit");
    assert.include(
      instructions,
      "After PR work, a child's last line is: PR <full URL>, head <SHA>, checks and reviews <state>.",
    );
    assert.include(
      instructions,
      "For PR waits, no sleep, loops, gh --watch, Monitor or ScheduleWakeup.",
    );
    assert.include(instructions, "call unwatch_pull_request");
  });
});
