import * as Assert from "node:assert/strict";
import * as FS from "node:fs";
import * as Path from "node:path";
import * as Test from "node:test";
import { currentQualificationModules, runCurrentQualification } from "./current-qualification.mjs";

Test.test("current V2 qualification keeps initialization and storage coverage distinct from historical fork007", () => {
  Assert.equal(currentQualificationModules.length, 4);
  Assert.ok(currentQualificationModules.includes("apps/server/src/persistence/initializeV2Database.test.ts"));
  Assert.ok(currentQualificationModules.every((file) => !file.includes("007_Jones")));
});

Test.test("qualifies an explicitly bound clean V2 candidate with synthetic databases", {
  skip: process.env.JONES_CURRENT_QUALIFICATION_REQUEST ? false : "explicit candidate/runtime binding required",
}, async (test) => {
  const path = process.env.JONES_CURRENT_QUALIFICATION_REQUEST;
  Assert.ok(Path.isAbsolute(path));
  const info = FS.lstatSync(path);
  Assert.ok(info.isFile() && !info.isSymbolicLink() && info.size <= 16384);
  const request = JSON.parse(FS.readFileSync(path, "utf8"));
  const result = await runCurrentQualification({ ...request, signal: test.signal });
  test.diagnostic(JSON.stringify(result));
  Assert.equal(result.outcome, "passed");
});
