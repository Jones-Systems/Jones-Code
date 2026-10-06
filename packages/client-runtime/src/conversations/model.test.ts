import { describe, it } from "vite-plus/test";
import { conversationLibraryReaderCases } from "./model.cases.ts";

describe("conversation library reader", () => {
  it.each(conversationLibraryReaderCases)("$name", ({ run }) => run());
});
