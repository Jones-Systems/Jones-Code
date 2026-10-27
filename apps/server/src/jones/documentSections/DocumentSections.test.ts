import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { AuthorizedRepository } from "./AuthorizedRepository.ts";
import * as DocumentSections from "./DocumentSections.ts";
import { withRepositoryEffect } from "./repository.testkit.ts";

it.effect("extracts through the independent service with no continuity record or provider", () =>
  withRepositoryEffect(
    "## [one] Synthetic decision\r\nUntrusted content\r\n",
    ({ root, selector }) =>
      Effect.gen(function* () {
        const service = yield* DocumentSections.DocumentSections;
        const result = yield* service.extract(
          AuthorizedRepository.fromAuthorizedWorkspace(root),
          selector,
        );
        expect(result.sections[0]!.text).toBe(
          "## [one] Synthetic decision\r\nUntrusted content\r\n",
        );
        expect(result.authorityEffect).toBe("none");
      }),
  ).pipe(Effect.provide(DocumentSections.layer)),
);
it.effect(
  "returns typed sanitized refusals for original-byte digest mismatch and invalid UTF-8",
  () =>
    Effect.gen(function* () {
      for (const [text, expected] of [
        ["## [one] H\n", "digest_mismatch"],
        [Buffer.from([0xff]), "invalid_utf8"],
      ] as const) {
        yield* withRepositoryEffect(text, ({ root, selector }) =>
          Effect.gen(function* () {
            const service = yield* DocumentSections.DocumentSections;
            const error = yield* service
              .extract(AuthorizedRepository.fromAuthorizedWorkspace(root), {
                ...selector,
                documentSha256:
                  expected === "digest_mismatch" ? "0".repeat(64) : selector.documentSha256,
              })
              .pipe(Effect.flip);
            expect(error._tag).toBe("DocumentSectionError");
            expect(error.code).toBe(expected);
            expect(error.message).not.toContain(root);
            expect(error.message).not.toContain("github.com");
          }),
        );
      }
    }).pipe(Effect.provide(DocumentSections.layer)),
);
it.effect("refuses a forged repository handle before invoking Git", () =>
  Effect.gen(function* () {
    const service = yield* DocumentSections.DocumentSections;
    const error = yield* service
      .extract({ root: "/private" } as unknown as AuthorizedRepository, {
        repositoryIdentity: "github:Synthetic/Repo",
        commitOid: "a".repeat(40),
        documentPath: "a.md",
        documentSha256: "b".repeat(64),
        sectionIds: ["one"],
      })
      .pipe(Effect.flip);
    expect(error.code).toBe("repository_refused");
    expect(error.message).not.toContain("/private");
  }).pipe(Effect.provide(DocumentSections.layer)),
);
