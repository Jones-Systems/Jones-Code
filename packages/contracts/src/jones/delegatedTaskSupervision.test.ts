import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { OrchestratorMcpDelegateTaskInput } from "../orchestratorMcp.ts";

const decode = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskInput);
const encode = Schema.encodeSync(OrchestratorMcpDelegateTaskInput);

describe("delegate_task supervision opt-in", () => {
  it("does not add a key when decoding legacy requests", () => {
    const legacy = { task: "Review the diff", runtimeMode: "approval-required" };
    expect(decode(legacy)).toEqual(legacy);
    expect(Object.hasOwn(decode(legacy), "supervisionRequestedByUser")).toBe(false);
  });

  it.each([true, false])("round-trips an explicit boolean %s", (value) => {
    const input = { task: "Review the diff", supervisionRequestedByUser: value };
    expect(encode(decode(input))).toEqual(input);
  });

  it.each(["true", 1, null, {}])("rejects a non-boolean %s", (value) => {
    expect(() => decode({ task: "Review the diff", supervisionRequestedByUser: value })).toThrow();
  });
});
