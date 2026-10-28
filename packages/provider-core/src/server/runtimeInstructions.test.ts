import { assert, describe, it } from "@effect/vitest";

import { buildRuntimeInstructions } from "./runtimeInstructions.ts";

describe("buildRuntimeInstructions", () => {
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

    assert.include(instructions, "Link each PR you create, take over, or push to");
    assert.include(instructions, "one link_pull_request per PR");
    assert.include(instructions, "Skip PRs you only read or review");
    assert.include(instructions, "If linking fails, say so.");
    assert.notInclude(instructions, "git log -1");
    assert.notInclude(instructions, "work on");
    assert.include(instructions, "list_thread_pull_requests");
    assert.include(instructions, "link any PR from your work that is missing");
  });

  it("waits on a PR only with the watch, never with a poll loop", () => {
    const instructions = buildRuntimeInstructions({ harness: "Codex" });

    assert.notInclude(instructions, "Creation, pending checks, or a merge ask alone is not enough");
    assert.include(instructions, "Links and watches persist across pushes.");
    // The wake message itself carries the do-not-re-watch rule.
    assert.notInclude(instructions, "On a wake");
    assert.include(
      instructions,
      "Call watch_pull_request when asked to monitor or babysit, or when work you were asked to do, such as a merge, still waits on checks or reviews, even after an earlier unwatch",
    );
    assert.notInclude(instructions, "merge it now");
    assert.include(instructions, "Otherwise act now or hand back.");
    assert.include(instructions, "then watch and end your turn");
    assert.include(instructions, "never watch, even if the brief says babysit");
    assert.include(
      instructions,
      "After PR work, a child's last line is: PR <full URL>, head <SHA>, checks and reviews <state>.",
    );
    assert.include(instructions, "Never wait with sleep");
    assert.include(instructions, "call unwatch_pull_request");
  });
});
