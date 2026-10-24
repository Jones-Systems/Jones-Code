import { describe, expect, it } from "vite-plus/test";

import { JONES_SHELL_LANGUAGE, parseShellDestinationFence } from "./shellDestinationFence.ts";

describe("shell destination fence metadata", () => {
  it("preserves exact IDs without assuming a UUID, personal ID, or prefix", () => {
    expect(JONES_SHELL_LANGUAGE).toBe("jones-shell");
    for (const environmentId of ["fixture", "fixture.long", "ENV:α/42", "a".repeat(1024)]) {
      expect(parseShellDestinationFence(`environment=${environmentId}`)).toEqual({
        _tag: "Valid",
        environmentId,
      });
    }
    expect(parseShellDestinationFence('environment="synthetic environment"')).toEqual({
      _tag: "Valid",
      environmentId: "synthetic environment",
    });
  });

  it("keeps labels and titles as display hints regardless of option order", () => {
    expect(
      parseShellDestinationFence(
        `title='Check project.v2' label="Renée's 開発.box" environment=fixture.destination`,
      ),
    ).toEqual({
      _tag: "Valid",
      environmentId: "fixture.destination",
      label: "Renée's 開発.box",
      title: "Check project.v2",
    });
    expect(parseShellDestinationFence("environment=fixture label=other.destination")).toEqual({
      _tag: "Valid",
      environmentId: "fixture",
      label: "other.destination",
    });
  });

  it("decodes only explicit quote and backslash escapes", () => {
    expect(
      parseShellDestinationFence(
        String.raw`environment=fixture label='Renée\'s box' title="Say \"hello\" in C:\\work"`,
      ),
    ).toEqual({
      _tag: "Valid",
      environmentId: "fixture",
      label: "Renée's box",
      title: 'Say "hello" in C:\\work',
    });
  });

  it.each([
    undefined,
    null,
    "",
    "   ",
    "label=fixture",
    "title=fixture",
    "environment=",
    'environment=""',
    'environment=" "',
    'environment=" fixture"',
    'environment="fixture "',
    "environment=fixture environment=fixture",
    "environment=fixture label=one label=two",
    "environment=fixture title=one title=two",
    "environment=fixture host=other",
    "environmentId=fixture",
    "environment-extra=fixture",
    "Environment=fixture",
    "environment =fixture",
    "environment= fixture",
    "environment=fixture trailing",
    'environment="fixture"label=box',
    'environment="fixture',
    "environment='fixture",
    "environment=fixture label=Renée's.box",
    'environment=fixture label=""',
    "environment=fixture title=",
    String.raw`environment=fixture label="bad\nvalue"`,
    "environment=fixture=extra",
    "environment=fixture\nlabel=box",
    "environment=fixture\tlabel=box",
    "environment=fixture\u0000",
    'environment=fixture title="hidden\u202evalue"',
    'environment=fixture label="line\u2028break"',
    `environment=${"x".repeat(1025)}`,
    `environment=fixture label="${"x".repeat(4096)}"`,
  ])("rejects missing, ambiguous, or malformed metadata: %s", (meta) => {
    const result = parseShellDestinationFence(meta);
    expect(result._tag).toBe("Invalid");
    if (result._tag === "Invalid") {
      expect(result.reason.length).toBeGreaterThan(0);
      expect(result.reason).not.toContain("hidden");
    }
  });
});
