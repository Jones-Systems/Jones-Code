import { expect, it } from "vite-plus/test";
import { lookupReceipt } from "./state.ts";
import { pending } from "../store/sql.testkit.ts";

it("never represents an unsupported legacy value as a native receipt or absence", () => {
  const context = { owner_id: "owner:test", server_generation: 1, registry_version: 0 };
  const base = {
    context,
    native: null,
    archive: null,
    open: null,
    completeness: "complete" as const,
  };
  const legacy = { ...pending("command:valid-0000").receipt, command_id: "command:0" };
  expect(() => lookupReceipt("command:0", { ...base, archive: legacy })).toThrow();
  expect(() => lookupReceipt("command:0", { ...base, open: legacy })).toThrow();
  expect(lookupReceipt("command:missing", base).kind).toBe("proven_absent");
  expect(lookupReceipt("command:missing", { ...base, completeness: "import_incomplete" })).toEqual({
    kind: "unknown",
    reason: "import_incomplete",
  });
});
