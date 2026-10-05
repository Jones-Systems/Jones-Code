import { describe, it } from "vite-plus/test";
import { conversationLibraryReaderCases } from "./model.cases.ts";

describe("conversation library reader", () => {
  for (const test of conversationLibraryReaderCases) it(test.name, test.run);
});
