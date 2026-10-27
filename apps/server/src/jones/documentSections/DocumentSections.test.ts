import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { AuthorizedRepository } from "./AuthorizedRepository.ts";
import * as DocumentSections from "./DocumentSections.ts";
import { withRepository } from "./repository.testkit.ts";

it("extracts through the independent service with no continuity record or provider", async () => {
  await withRepository("## [one] Synthetic decision\r\nUntrusted content\r\n", async ({ root, selector }) => {
    const result = await Effect.runPromise(Effect.gen(function* () {
      const service = yield* DocumentSections.DocumentSections;
      return yield* service.extract(AuthorizedRepository.fromAuthorizedWorkspace(root), selector);
    }).pipe(Effect.provide(DocumentSections.layer)));
    expect(result.sections[0]!.text).toBe("## [one] Synthetic decision\r\nUntrusted content\r\n");
    expect(result.authorityEffect).toBe("none");
  });
});
it("returns typed sanitized refusals for original-byte digest mismatch and invalid UTF-8", async () => {
  for (const [text, expected] of [["## [one] H\n", "digest_mismatch"],
    [Buffer.from([0xff]), "invalid_utf8"]] as const) {
    await withRepository(text, async ({ root, selector }) => {
      const error = await Effect.runPromise(Effect.gen(function* () {
        const service = yield* DocumentSections.DocumentSections;
        return yield* service.extract(AuthorizedRepository.fromAuthorizedWorkspace(root), {
          ...selector, documentSha256: expected === "digest_mismatch" ? "0".repeat(64) : selector.documentSha256,
        }).pipe(Effect.flip);
      }).pipe(Effect.provide(DocumentSections.layer)));
      expect(error._tag).toBe("DocumentSectionError"); expect(error.code).toBe(expected);
      expect(error.message).not.toContain(root); expect(error.message).not.toContain("github.com");
    });
  }
});
it("refuses a forged repository handle before invoking Git", async () => {
  const error = await Effect.runPromise(Effect.gen(function* () {
    const service = yield* DocumentSections.DocumentSections;
    return yield* service.extract({ root: "/private" } as unknown as AuthorizedRepository, {
      repositoryIdentity: "github:Synthetic/Repo", commitOid: "a".repeat(40), documentPath: "a.md",
      documentSha256: "b".repeat(64), sectionIds: ["one"],
    }).pipe(Effect.flip);
  }).pipe(Effect.provide(DocumentSections.layer)));
  expect(error.code).toBe("repository_refused"); expect(error.message).not.toContain("/private");
});
