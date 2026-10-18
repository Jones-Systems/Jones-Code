import { describe, it } from "vite-plus/test";
import { conversationStoreCases } from "./Store.cases.ts";

describe("conversation library store", () => {
  it.each(conversationStoreCases)("$name", ({ run }) => run());
});
