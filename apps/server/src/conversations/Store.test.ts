import { describe, it } from "vite-plus/test";
import { conversationStoreCases } from "./Store.cases.ts";

describe("conversation library store", () => {
  for (const test of conversationStoreCases) it(test.name, test.run);
});
