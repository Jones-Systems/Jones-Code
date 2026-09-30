import { describe, it } from "vite-plus/test";
import { conversationLibraryCases } from "./conversationLibrary.cases.ts";

describe("conversation library", () => {
  for (const test of conversationLibraryCases) it(test.name, test.run);
});
