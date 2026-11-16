import { describe, expect, it, vi } from "vite-plus/test";
import * as documentMarkdownAst from "@t3tools/shared/jones/documentMarkdownAst";
import type { DocumentSectionsSelector } from "../../../../../packages/contracts/src/jones/documentSections.ts";
import {
  canonicalJson,
  decodeDecisionReference,
  encodeDecisionReference,
  extractDocumentSections,
  sha256,
  validateSelector,
  DocumentSectionRefusal,
} from "./pure.ts";

const blobOid = "b".repeat(40);
const base: DocumentSectionsSelector = {
  repositoryIdentity: "github:Jones-Systems/Jones-Code",
  commitOid: "a".repeat(40),
  documentPath: "docs/decisions.md",
  documentSha256: "0".repeat(64),
  sectionIds: ["one"],
};
const extract = (text: string | Uint8Array, ids: readonly string[] = ["one"]) => {
  const bytes = typeof text === "string" ? Buffer.from(text) : text;
  return extractDocumentSections(
    { ...base, sectionIds: ids, documentSha256: sha256(bytes) },
    blobOid,
    bytes,
  );
};

describe("exact Markdown sections", () => {
  it("preserves original mixed CR/LF bytes, inclusive line bounds, nesting, and caller order", () => {
    const result = extract(
      "Preface\r\n## [one] First ###\r\nα\r### [two] Child\nbody\r\n## Unlabelled\nstop",
      ["two", "one"],
    );
    expect(
      result.sections.map((section) => [
        section.id,
        section.level,
        section.title,
        section.startLine,
        section.endLine,
        section.text,
      ]),
    ).toEqual([
      ["two", 3, "Child", 4, 5, "### [two] Child\nbody\r\n"],
      ["one", 2, "First", 2, 5, "## [one] First ###\r\nα\r### [two] Child\nbody\r\n"],
    ]);
    for (const section of result.sections)
      expect(section.sha256).toBe(sha256(Buffer.from(section.text)));
    expect(result.document.sizeBytes).toBe(
      Buffer.byteLength(
        "Preface\r\n## [one] First ###\r\nα\r### [two] Child\nbody\r\n## Unlabelled\nstop",
      ),
    );
    expect(result.authorityEffect).toBe("none");
    expect(result.document.blobOid).toBe(blobOid);
  });
  it("ignores fenced headings, allows longer closing fences, and terminates at headings without IDs", () => {
    const text =
      "```md\n# [one] Decoy\n````\n~~~\n# [one] Decoy\n~~~\n## [one] Real\n````\n# Decoy\n```\n`````\nkeep\n## Boundary\n";
    const section = extract(text).sections[0]!;
    expect(section.title).toBe("Real");
    expect(section.startLine).toBe(7);
    expect(section.endLine).toBe(12);
    expect(section.text).toBe("## [one] Real\n````\n# Decoy\n```\n`````\nkeep\n");
    expect(extract("```invalid`info\n## [one] Still a heading").sections[0]!.startLine).toBe(2);
  });
  it.each([
    { name: "dash", opening: ["- ```md"], indent: "  ", closing: "```" },
    { name: "asterisk", opening: ["* ~~~md"], indent: "  ", closing: "~~~~" },
    { name: "plus", opening: ["+ ```md"], indent: "  ", closing: "```" },
    { name: "ordered period", opening: ["12. ```md"], indent: "    ", closing: "```" },
    { name: "ordered parenthesis", opening: ["3) ~~~md"], indent: "   ", closing: "~~~" },
    { name: "nested list", opening: ["- parent", "  - ```md"], indent: "    ", closing: "```" },
    { name: "same-line nesting", opening: ["- - ```md"], indent: "    ", closing: "```" },
    {
      name: "nested continuation",
      opening: ["- parent", "  1. child", "     ```md"],
      indent: "     ",
      closing: "```",
    },
  ])(
    "ignores headings in $name fences and resumes after the close with exact bytes and lines",
    ({ opening, indent, closing }) => {
      const before =
        [
          "## [one] Container",
          ...opening,
          `${indent}## [example] Not a section`,
          `${indent}## [real] Also not a section`,
          `${indent}${closing}`,
        ].join("\r\n") + "\r\n";
      const real = "## [real] Real section\rbody\n";
      const text = before + real + "## Boundary\n";
      const result = extract(text, ["real", "one"]);
      expect(
        result.sections.map((section) => [
          section.id,
          section.title,
          section.startLine,
          section.endLine,
          section.text,
        ]),
      ).toEqual([
        ["real", "Real section", opening.length + 5, opening.length + 6, real],
        ["one", "Container", 1, opening.length + 4, before],
      ]);
      expect(result.sections.map((section) => section.sha256)).toEqual([
        sha256(Buffer.from(real)),
        sha256(Buffer.from(before)),
      ]);
      expect(result.document.sha256).toBe(sha256(Buffer.from(text)));
      expect(() => extract(text, ["example"])).toThrow(
        new DocumentSectionRefusal("missing_section"),
      );
    },
  );
  it.each([
    { name: "bare dash", items: ["-"], indent: "  " },
    { name: "bare asterisk", items: ["*"], indent: "  " },
    { name: "bare plus", items: ["+"], indent: "  " },
    { name: "bare ordered period", items: ["12."], indent: "    " },
    { name: "bare ordered parenthesis", items: ["3)"], indent: "   " },
    { name: "unordered trailing spaces", items: ["-     "], indent: "  " },
    { name: "ordered trailing spaces", items: ["12.     "], indent: "    " },
    { name: "nested empty item", items: ["- parent", "", "  -"], indent: "    " },
    { name: "nested empty parents", items: ["-", "  1.   "], indent: "     " },
    { name: "same-line empty nesting", items: ["- -"], indent: "    " },
  ])(
    "retains $name indentation for following-line fences, closes or dedents, and preserves bytes",
    ({ items, indent }) => {
      for (const closed of [false, true]) {
        const before =
          [
            "## [one] Container",
            ...items,
            `${indent}\`\`\`md`,
            `${indent}## [example] Decoy`,
            ...(closed ? [`${indent}\`\`\``] : []),
          ].join("\r\n") + "\r\n";
        const real = "## [real] Real section\rbody\n";
        const text = before + real + "## Boundary\n";
        const boundaryLine = items.length + 3 + (closed ? 1 : 0);
        const result = extract(text, ["real", "one"]);
        expect(
          result.sections.map((section) => [
            section.id,
            section.title,
            section.startLine,
            section.endLine,
            section.text,
          ]),
        ).toEqual([
          ["real", "Real section", boundaryLine + 1, boundaryLine + 2, real],
          ["one", "Container", 1, boundaryLine, before],
        ]);
        expect(result.sections.map((section) => section.sha256)).toEqual([
          sha256(Buffer.from(real)),
          sha256(Buffer.from(before)),
        ]);
        expect(result.document.sha256).toBe(sha256(Buffer.from(text)));
        expect(() => extract(text, ["example"])).toThrow(
          new DocumentSectionRefusal("missing_section"),
        );
      }
    },
  );
  it("gives a Setext underline precedence over an empty item in the reported fence trace", () => {
    const text =
      "Title\n-\n  ```md\n  ## [example] Decoy\n## [code] Still code\n```\n## [real] Real section\nbody";
    const result = extract(text, ["real"]);
    expect(result.sections[0]).toMatchObject({
      id: "real",
      title: "Real section",
      startLine: 7,
      endLine: 8,
      text: "## [real] Real section\nbody",
      sha256: sha256(Buffer.from("## [real] Real section\nbody")),
    });
    expect(result.document.sha256).toBe(sha256(Buffer.from(text)));
    for (const id of ["example", "code"])
      expect(() => extract(text, [id])).toThrow(new DocumentSectionRefusal("missing_section"));
  });
  it.each([
    {
      source: "- ```md\n  ## [example] Not a section\n  ```\n## [real] Real section\nbody",
      startLine: 4,
    },
    { source: "-\n  ```md\n  ## [example] Decoy\n## [real] Real section\nbody", startLine: 4 },
  ])(
    "returns the real section in the original list-fence trace at line $startLine",
    ({ source, startLine }) => {
      const real = "## [real] Real section\nbody";
      expect(extract(source, ["real"]).sections[0]).toMatchObject({
        id: "real",
        title: "Real section",
        startLine,
        endLine: startLine + 1,
        text: real,
        sha256: sha256(Buffer.from(real)),
      });
      expect(() => extract(source, ["example"])).toThrow(
        new DocumentSectionRefusal("missing_section"),
      );
    },
  );
  it("keeps Setext titles outside the selectable and boundary index", () => {
    const text = "## [one] Keep\n[decoy]\n---\nmore\n## Boundary\n";
    expect(extract(text).sections[0]).toMatchObject({
      startLine: 1,
      endLine: 4,
      text: "## [one] Keep\n[decoy]\n---\nmore\n",
    });
    expect(() => extract(text, ["decoy"])).toThrow(new DocumentSectionRefusal("missing_section"));
  });
  it("uses raw ATX syntax and titles without selecting HTML, code, entities or escaped markers", () => {
    const preface =
      "<div>\n## [html] Hidden\n</div>\n\n    ## [code] Hidden\n\n\\## [escaped] Hidden\n\n" +
      "## \\[bracket] Hidden\n## [ent&#105;ty] Hidden\n";
    const real = "## [one] Raw &amp; \\*title\\* ###\r\nbody\n";
    const result = extract(preface + real);
    expect(result.sections[0]).toMatchObject({
      title: "Raw &amp; \\*title\\*",
      text: real,
      sha256: sha256(Buffer.from(real)),
    });
    for (const id of ["html", "code", "escaped", "bracket", "entity"])
      expect(() => extract(preface + real, [id])).toThrow(
        new DocumentSectionRefusal("missing_section"),
      );
  });
  it("traverses containers while retaining the original physical ATX eligibility rule", () => {
    const section = "  ## [one] Eligible\n  body\n> ## [quoted] Hidden\n\n- ## [marker] Hidden\n\n";
    const text = "- item\n" + section + "## Boundary\n";
    expect(extract(text).sections[0]).toMatchObject({ startLine: 2, endLine: 7, text: section });
    for (const id of ["quoted", "marker"])
      expect(() => extract(text, [id])).toThrow(new DocumentSectionRefusal("missing_section"));
  });
  it("preserves mixed endings and UTF-16 parser coordinates after BOM, astral Unicode and tabbed containers", () => {
    const preface = "\ufeff😀\r\n-\t```md\r\t## [decoy] Hidden\n\t```\r\n";
    const real = "##\t[one] Real 😀\rbody";
    const result = extract(preface + real);
    expect(result.sections[0]).toMatchObject({
      title: "Real 😀",
      startLine: 5,
      endLine: 6,
      text: real,
      sha256: sha256(Buffer.from(real)),
    });
    expect(result.document.sha256).toBe(sha256(Buffer.from(preface + real)));
    expect(() => extract(preface + real, ["decoy"])).toThrow(
      new DocumentSectionRefusal("missing_section"),
    );
  });
  it("refuses parser errors and inconsistent heading positions locally without a scanner fallback", () => {
    const source = "## [one] Real";
    const parser = vi.spyOn(documentMarkdownAst, "markdownHeadingPositions");
    try {
      parser.mockImplementationOnce(() => {
        throw new Error("Parser failed.");
      });
      expect(() => extract(source)).toThrow(new DocumentSectionRefusal("blob_refused"));
      parser.mockReturnValueOnce([{ depth: 2 }] as unknown as ReturnType<
        typeof documentMarkdownAst.markdownHeadingPositions
      >);
      expect(() => extract(source)).toThrow(new DocumentSectionRefusal("blob_refused"));
      for (const invalid of [
        {
          depth: 3 as const,
          start: { line: 1, offset: 0 },
          end: { line: 1, offset: source.length },
        },
        {
          depth: 2 as const,
          start: { line: 1, offset: 1 },
          end: { line: 1, offset: source.length },
        },
        {
          depth: 2 as const,
          start: { line: 1, offset: 0 },
          end: { line: 1, offset: source.length - 1 },
        },
        {
          depth: 2 as const,
          start: { line: 0, offset: 0 },
          end: { line: 1, offset: source.length },
        },
      ]) {
        parser.mockReturnValueOnce([invalid]);
        expect(() => extract(source)).toThrow(new DocumentSectionRefusal("blob_refused"));
      }
    } finally {
      parser.mockRestore();
    }
  });
  it.each([
    { name: "trailing spaces", prefix: ["Title", "-   "], fenceIndent: "  ", container: "" },
    {
      name: "multiline paragraph",
      prefix: ["Title", "continued", "-"],
      fenceIndent: "  ",
      container: "",
    },
    { name: "list paragraph", prefix: ["- Title", "  -"], fenceIndent: "    ", container: "  " },
  ])(
    "keeps $name Setext syntax in its existing container without exposing fenced headings",
    ({ prefix, fenceIndent, container }) => {
      const before =
        [
          ...prefix,
          `${fenceIndent}\`\`\`md`,
          `${fenceIndent}## [example] Decoy`,
          `${container}## [code] Still code`,
          `${container}\`\`\``,
        ].join("\r\n") + "\r\n";
      const real = "## [real] Real section\rbody\n";
      const text = before + real;
      const result = extract(text, ["real"]);
      expect(result.sections[0]).toMatchObject({
        title: "Real section",
        startLine: prefix.length + 5,
        endLine: prefix.length + 6,
        text: real,
        sha256: sha256(Buffer.from(real)),
      });
      expect(result.document.sha256).toBe(sha256(Buffer.from(text)));
      for (const id of ["example", "code"])
        expect(() => extract(text, [id])).toThrow(new DocumentSectionRefusal("missing_section"));
    },
  );
  it.each([
    { name: "paragraph separated by a blank", previous: ["Title", ""] },
    { name: "ATX heading", previous: ["# Heading"] },
    { name: "thematic break", previous: ["***"] },
    { name: "closed code fence", previous: ["```md", "text", "```"] },
  ])(
    "treats a dash after $name as an empty item rather than a Setext underline",
    ({ previous }) => {
      const before = [...previous, "-", "  ```md", "  ## [example] Decoy"].join("\r\n") + "\r\n";
      const real = "## [real] Real section\rbody\n";
      const text = before + real;
      const result = extract(text, ["real"]);
      expect(result.sections[0]).toMatchObject({
        title: "Real section",
        startLine: previous.length + 4,
        endLine: previous.length + 5,
        text: real,
        sha256: sha256(Buffer.from(real)),
      });
      expect(result.document.sha256).toBe(sha256(Buffer.from(text)));
      expect(() => extract(text, ["example"])).toThrow(
        new DocumentSectionRefusal("missing_section"),
      );
    },
  );
  it.each(["", "\r\n"])(
    "preserves the parser's fence after indented code and a dash, separator %j",
    (separator) => {
      const before = `    code\r\n${separator}-\r\n  \`\`\`md\r\n  ## [example] Decoy\r\n## [still-code] Still code\rbody\n  \`\`\`\n`;
      const real = "## [real] Real section\rbody\n";
      const text = before + real;
      for (const id of ["example", "still-code"])
        expect(() => extract(text, [id])).toThrow(new DocumentSectionRefusal("missing_section"));
      expect(extract(text, ["real"]).sections[0]).toMatchObject({
        title: "Real section",
        startLine: separator === "" ? 8 : 9,
        endLine: separator === "" ? 9 : 10,
        text: real,
        sha256: sha256(Buffer.from(real)),
      });
      expect(extract(text, ["real"]).document.sha256).toBe(sha256(Buffer.from(text)));
    },
  );
  it("ends an unclosed list fence on dedent without treating indented code as a list opener", () => {
    expect(extract("- ```md\n  ## [one] Decoy\n\n## [one] Real\nbody\n").sections[0]).toMatchObject(
      { title: "Real", startLine: 4, endLine: 5, text: "## [one] Real\nbody\n" },
    );
    expect(extract("    - ```md\n## [one] Real\n").sections[0]).toMatchObject({
      title: "Real",
      startLine: 2,
      endLine: 2,
      text: "## [one] Real\n",
    });
  });
  it("refuses the entire result when any requested ID is missing or repeated", () => {
    expect(() => extract("## [one] A\n", ["one", "missing"])).toThrow(
      new DocumentSectionRefusal("missing_section"),
    );
    expect(() => extract("## [one] A\n## [one] B\n")).toThrow(
      new DocumentSectionRefusal("ambiguous_section"),
    );
    expect(() => extract("    ## [one] Indented code\n")).toThrow(
      new DocumentSectionRefusal("missing_section"),
    );
  });
  it("hashes original bytes, rejects replacement decoding, and retains a UTF-8 BOM", () => {
    const bytes = Buffer.from("## [one] Header\r\nbody\r\n");
    expect(() =>
      extractDocumentSections(
        { ...base, documentSha256: sha256(Buffer.from(bytes.toString().replaceAll("\r\n", "\n"))) },
        blobOid,
        bytes,
      ),
    ).toThrow(new DocumentSectionRefusal("digest_mismatch"));
    expect(() =>
      extract(Buffer.concat([Buffer.from("## [one] A\n"), Buffer.from([0xc3, 0x28])])),
    ).toThrow(new DocumentSectionRefusal("invalid_utf8"));
    const bom = extract("\ufeffpreface\n## [one] H\nbody");
    expect(bom.document.sha256).toBe(sha256(Buffer.from("\ufeffpreface\n## [one] H\nbody")));
    expect(bom.sections[0]!.text).toBe("## [one] H\nbody");
  });
  it("retains physical ATX eligibility for a BOM-prefixed first line while extracting later headings", () => {
    const first = "## [first] First😀\r\n";
    const later = "## [one] Later\ufeff😀\rbody";
    const source = "\ufeff" + first + later;
    const result = extract(source);
    expect(result.sections[0]).toMatchObject({
      title: "Later\ufeff😀",
      startLine: 2,
      endLine: 3,
      text: later,
      sha256: sha256(Buffer.from(later)),
    });
    expect(result.document.sha256).toBe(sha256(Buffer.from(source)));
    expect(result.document.sizeBytes).toBe(Buffer.byteLength(source));
    expect(() => extract(source, ["first"])).toThrow(new DocumentSectionRefusal("missing_section"));
    expect(extract(first + later, ["first"]).sections[0]).toMatchObject({
      startLine: 1,
      endLine: 1,
      text: first,
    });
  });
  it("bounds full serialized responses, including JSON escaping and overlapping sections", () => {
    expect(() => extract(`## [one] H\n${"x".repeat(1024 * 1024)}`)).toThrow(
      new DocumentSectionRefusal("document_too_large"),
    );
    expect(() => extract(`## [one] H\n${"\0".repeat(12_000)}`)).toThrow(
      new DocumentSectionRefusal("response_too_large"),
    );
    const text = `## [one] H\n### [two] H\n${"x".repeat(33_000)}`;
    expect(() => extract(text, ["one", "two"])).toThrow(
      new DocumentSectionRefusal("response_too_large"),
    );
    expect(Buffer.byteLength(canonicalJson(extract("## [one] H\n")))).toBeLessThan(64 * 1024);
  });
});

describe("bounded nonauthorizing decision references", () => {
  it("round-trips canonical selectors without granting authority or requiring extraction", () => {
    const encoded = encodeDecisionReference("Short summary", base);
    const decoded = decodeDecisionReference(encoded);
    expect(decoded).toEqual({
      ...base,
      summary: "Short summary",
      schema: "jones.document-decision-ref/v1",
      authorityEffect: "none",
    });
    expect(encodeDecisionReference(decoded.summary, base)).toBe(encoded);
    expect(decodeDecisionReference(Buffer.from(encoded))).toEqual(decoded);
    expect(decodeDecisionReference(JSON.stringify(decoded, null, 2))).toEqual(decoded);
  });
  it("rejects duplicate/escaped keys, malformed text, added authority, and byte overflow", () => {
    const encoded = encodeDecisionReference("Summary", base);
    for (const input of [
      encoded.replace('"summary":"Summary"', '"summary":"Old","summ\\u0061ry":"Summary"'),
      encoded.replace('"none"', '"owner"'),
      encoded.replace('"Summary"', '"\\ud800"'),
      encoded.replace('"Summary"', '"' + "é".repeat(257) + '"'),
      encoded.replace('"summary":', '"root":"/private","summary":'),
      "ordinary decision text",
      " ".repeat(64 * 1024 + 1),
      Buffer.from([0xff]),
    ])
      expect(() => decodeDecisionReference(input)).toThrow(
        new DocumentSectionRefusal("invalid_reference"),
      );
    expect(() =>
      encodeDecisionReference("x", { ...base, documentPath: `${"x".repeat(1800)}.md` }),
    ).toThrow();
    expect(() => validateSelector({ ...base, sectionIds: ["one", "one"] })).toThrow();
    expect(() => validateSelector({ ...base, repository: "/private" })).toThrow();
  });
});
