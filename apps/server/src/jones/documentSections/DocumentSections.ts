import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import {
  DocumentSectionErrorCode,
  type DocumentSectionsResult,
  type DocumentSectionsSelector,
} from "@t3tools/contracts";
import { AuthorizedRepository } from "./AuthorizedRepository.ts";
import { DocumentSectionRefusal, extractDocumentSections, validateSelector } from "./pure.ts";
import { readCommittedDocument } from "./rawGit.ts";

export class DocumentSectionError extends Schema.TaggedError<DocumentSectionError>()(
  "DocumentSectionError", { code: DocumentSectionErrorCode },
) {
  override get message(): string { return `Document section extraction refused (${this.code}).`; }
}

export class DocumentSections extends Context.Service<
  DocumentSections,
  { readonly extract: (
    repository: AuthorizedRepository, selector: DocumentSectionsSelector,
  ) => Effect.Effect<DocumentSectionsResult, DocumentSectionError> }
>()("jones/documentSections/DocumentSections") {}

const safeError = (error: unknown) => new DocumentSectionError({
  code: error instanceof DocumentSectionRefusal ? error.code : "git_failed",
});

const extract: DocumentSections["Service"]["extract"] = Effect.fn("DocumentSections.extract")(
  function* (repository, selector) {
    const checked = yield* Effect.try({ try: () => validateSelector(selector), catch: safeError });
    const root = yield* Effect.try({
      try: () => AuthorizedRepository.rootOf(repository),
      catch: () => new DocumentSectionError({ code: "repository_refused" }),
    });
    return yield* Effect.acquireUseRelease(
      Effect.sync(() => {
        const controller = new AbortController();
        const promise = readCommittedDocument(root, checked, controller.signal);
        // Register rejection immediately, even if interrupted before use begins.
        const settled = promise.then(() => undefined, () => undefined);
        return { controller, promise, settled };
      }),
      ({ promise }) => Effect.tryPromise({ try: () => promise, catch: safeError }).pipe(
        Effect.flatMap(({ blobOid, bytes }) => Effect.try({
          try: () => extractDocumentSections(checked, blobOid, bytes), catch: safeError,
        })),
      ),
      ({ controller, settled }) => Effect.promise(async () => {
        controller.abort();
        // Await close/reap or an explicit bounded cleanup refusal.
        await settled;
      }),
    );
  },
);

export const layer = Layer.succeed(DocumentSections, DocumentSections.of({ extract }));
