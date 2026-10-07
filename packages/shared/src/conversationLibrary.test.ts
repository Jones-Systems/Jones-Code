import { describe, it } from "vite-plus/test";
import { conversationLibraryCases } from "./conversationLibrary.cases.ts";

describe("conversation library", () => {
  it.each(conversationLibraryCases)("$name", ({ run }) => run());
});
