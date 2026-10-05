import { describe, expect, it } from "vitest";
import * as Schema from "effect/Schema";

import { OrchestrationV2Command } from "./orchestrationV2.ts";

const decode = Schema.decodeUnknownSync(OrchestrationV2Command);
const encode = Schema.encodeSync(OrchestrationV2Command);
const command = {
  type: "thread.delete",
  commandId: "command:removal-consent",
  threadId: "thread:removal-consent",
};
const consent = {
  projectId: "project:removal-consent",
  path: "/fixture/child-worktree",
  branch: "work/child",
  force: true,
};

describe("thread deletion worktree consent", () => {
  it("preserves old command bytes when consent is absent", () => {
    expect(encode(decode(command))).toEqual(command);
  });

  it("retains the exact explicit project, path, branch and force consent", () => {
    const requested = { ...command, worktreeRemoval: consent };
    expect(encode(decode(requested))).toEqual(requested);
  });

  it("rejects hidden overrides before the default decoder can strip them", () => {
    for (const override of [{ ownerThreadId: "foreign" }, { canonicalPath: "/foreign" }]) {
      expect(() => decode({ ...command, worktreeRemoval: { ...consent, ...override } })).toThrow();
    }
  });

  it("requires explicit true force consent and a captured branch", () => {
    for (const force of [undefined, false, "true"]) {
      expect(() => decode({ ...command, worktreeRemoval: { ...consent, force } })).toThrow();
    }
    expect(() =>
      decode({ ...command, worktreeRemoval: { ...consent, branch: undefined } }),
    ).toThrow();
    const detached = { ...command, worktreeRemoval: { ...consent, branch: null } };
    expect(encode(decode(detached))).toEqual(detached);
  });
});
