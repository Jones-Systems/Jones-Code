import { assert, describe, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { ThreadMetadataMcpUpdateInput } from "../threadMetadataMcp.ts";

const decodeUpdate = Schema.decodeUnknownSync(ThreadMetadataMcpUpdateInput);

describe("Jones thread-message metadata actions", () => {
  it("decodes block and allow with only the action", () => {
    for (const action of ["block_thread_messages", "allow_thread_messages"] as const) {
      assert.deepEqual(decodeUpdate({ action }), { action });
    }
  });

  it("rejects unrelated action data for block and allow", () => {
    for (const action of ["block_thread_messages", "allow_thread_messages"] as const) {
      assert.throws(() => decodeUpdate({ action, title: "Not allowed" }));
    }
  });
});
