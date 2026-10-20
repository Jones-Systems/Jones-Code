import { expect, it } from "vite-plus/test";
import { projectComposerContextForProvider } from "../composerContextReferences.ts";

it("projects thread identity with conditional local-only reading and no transcript", () => {
  const projected = projectComposerContextForProvider({
    text: "Compare [Reference](t3-context://v1/thread/thread_reference)",
    records: [
      {
        version: 1,
        kind: "thread",
        contextId: "thread_reference",
        label: "Reference",
        title: "Source title",
        environmentId: "foreign",
        threadId: "reference",
      },
    ] as never,
  });
  expect(projected).toContain("title: Source title");
  expect(projected).toContain("environmentId: foreign");
  expect(projected).toContain("threadId: reference");
  expect(projected).toContain("with no history included");
  expect(projected).toContain("Compare its environmentId with your current environment");
  expect(projected).toContain(
    "Only for the same environment, read its history with t3_thread_read",
  );
  expect(projected).toContain("do not guess its history or automatically export it");
  expect(projected).toContain("Do not message or change it unless asked");
});
