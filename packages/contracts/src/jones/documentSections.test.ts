import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  DocumentDecisionReference,
  DocumentSectionsInput,
  DocumentSectionsSelector,
} from "./documentSections.ts";

const selector = {
  repositoryIdentity: "github:Jones-Systems/Jones-Code",
  commitOid: "a".repeat(40),
  documentPath: "docs/decisions.md",
  documentSha256: "b".repeat(64),
  sectionIds: ["decision-1"],
};

describe("document extraction wire bounds", () => {
  it("accepts an explicit thread target and never a filesystem root or caller authority", () => {
    const decode = Schema.decodeUnknownSync(DocumentSectionsInput);
    expect(decode(selector)).toEqual(selector);
    expect(decode({ ...selector, threadId: "owner-target" }).threadId).toBe("owner-target");
    for (const extra of [
      { repository: "/private" },
      { workspaceRoot: "/private" },
      { callerKind: "owner" },
    ])
      expect(() => decode({ ...selector, ...extra })).toThrow();
  });
  it("requires exact digests, literal normalized Markdown paths, and ordered distinct IDs", () => {
    const decode = Schema.decodeUnknownSync(DocumentSectionsSelector);
    for (const patch of [
      { commitOid: "a".repeat(39) },
      { commitOid: "A".repeat(40) },
      { commitOid: `${selector.commitOid}\n` },
      { documentSha256: "b".repeat(63) },
      { repositoryIdentity: "https://github.com/a/b" },
      { documentPath: "../a.md" },
      { documentPath: "a/./b.md" },
      { documentPath: "a//b.md" },
      { documentPath: "/a.md" },
      { documentPath: "C:/a.md" },
      { documentPath: "a\\b.md" },
      { documentPath: "a\0.md" },
      { documentPath: "a.txt" },
      { documentPath: "a.md\n" },
      { documentPath: "\ud800.md" },
      { documentPath: `${"é".repeat(1023)}.md` },
      { sectionIds: [] },
      { sectionIds: ["id", "id"] },
      { sectionIds: ["id\n"] },
      { sectionIds: Array.from({ length: 33 }, (_, i) => `id${i}`) },
    ])
      expect(() => decode({ ...selector, ...patch })).toThrow();
    expect(decode({ ...selector, documentPath: "docs/[literal]*?.MARKDOWN" }).documentPath).toBe(
      "docs/[literal]*?.MARKDOWN",
    );
  });
  it("keeps compact references nonauthorizing with UTF-8 byte bounds", () => {
    const decode = Schema.decodeUnknownSync(DocumentDecisionReference);
    const reference = {
      ...selector,
      summary: "é".repeat(256),
      schema: "jones.document-decision-ref/v1",
      authorityEffect: "none",
    };
    expect(decode(reference).summary).toHaveLength(256);
    for (const patch of [
      { summary: "é".repeat(257) },
      { authorityEffect: "allow" },
      { documentPath: `${"x".repeat(1800)}.md` },
      { authority: "owner" },
    ])
      expect(() => decode({ ...reference, ...patch })).toThrow();
  });
});
