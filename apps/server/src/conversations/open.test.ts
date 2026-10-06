import { describe, it } from "vite-plus/test";
import { libraryOpenCases } from "./open.cases.ts";

describe("conversation library file ownership", () => {
  it.each(libraryOpenCases)("$name", ({ run }) => run());
});
