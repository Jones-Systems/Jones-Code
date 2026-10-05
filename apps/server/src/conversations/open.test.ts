import { describe, it } from "vite-plus/test";
import { libraryOpenCases } from "./open.cases.ts";

describe("conversation library file ownership", () => {
  for (const test of libraryOpenCases) it(test.name, test.run);
});
