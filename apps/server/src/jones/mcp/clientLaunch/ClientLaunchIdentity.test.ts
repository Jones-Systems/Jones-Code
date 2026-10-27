import { expect, it } from "@effect/vitest";
import { ProjectId } from "@t3tools/contracts";
import {
  canonicalRequestJson,
  clientLaunchDigest,
  clientLaunchIds,
} from "./ClientLaunchIdentity.ts";

it("uses RFC 8785 ordering, numbers and escaping", () => {
  expect(
    canonicalRequestJson({
      numbers: [Number("333333333.33333329"), 1e30, 4.5, 2e-3, 1e-27],
      string: '€$\u000f\nA\'B"\\"/',
      literals: [null, true, false],
    }),
  ).toBe(
    '{"literals":[null,true,false],"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27],"string":"€$\\u000f\\nA\'B\\\"\\\\\\\"/"}',
  );
  expect(canonicalRequestJson({ "€": 1, "\r": 2, "😀": 3, "1": 4, ö: 5, "\u0080": 6, דּ: 7 })).toBe(
    '{"\\r":2,"1":4,"\u0080":6,"ö":5,"€":1,"😀":3,"דּ":7}',
  );
  for (const value of [NaN, Infinity, "\ud800", { a: undefined }])
    expect(() => canonicalRequestJson(value)).toThrow();
});
it("isolates namespaces without exposing them and excludes resolved defaults", () => {
  const first = clientLaunchIds("synthetic-private-namespace", "request");
  expect(first.commandId).toMatch(/^mcp-launch:v1:[0-9a-f]{64}$/);
  expect(JSON.stringify(first)).not.toContain("synthetic-private-namespace");
  expect(first.threadId).toBe(first.messageId);
  expect(first).not.toEqual(clientLaunchIds("other", "request"));
  const project = ProjectId.make("project");
  expect(clientLaunchDigest({ clientRequestId: "golden", title: "hi" }, project)).toBe(
    "121b672d3bb6456c7b9e494c2b246db3aa0f133ccaccb56e1d0b43ea6f20670d",
  );
  expect(clientLaunchDigest({ clientRequestId: "a", title: " hi " }, project)).toBe(
    clientLaunchDigest(
      { clientRequestId: "b", title: "hi", workspaceStrategy: { type: "root" } },
      project,
    ),
  );
  expect(clientLaunchDigest({ clientRequestId: "a", title: "hi", message: "" }, project)).not.toBe(
    clientLaunchDigest({ clientRequestId: "a", title: "hi" }, project),
  );
});
