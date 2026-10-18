import { PositiveInt, TrimmedNonEmptyString } from "@t3tools/contracts";
import { decodeJsonResult } from "@t3tools/shared/schemaJson";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as GitHubRest from "../server/GitHubRest.ts";
import * as GitHubRequestPolicy from "../server/GitHubRequestPolicy.ts";
import type { NormalizedGitHubPullRequestRecord } from "../server/gitHubPullRequests.ts";
import type { GitHubRepositoryLocator } from "../server/gitHubRepositoryResolution.ts";

const decodeGitHubPullRequestList = decodeJsonResult(Schema.Array(Schema.Unknown));
const RestRepository = Schema.NullOr(
  Schema.Struct({
    full_name: Schema.String,
    owner: Schema.Struct({ login: Schema.String }),
  }),
);
const decodeRestEntry = Schema.decodeUnknownExit(
  Schema.Struct({
    number: PositiveInt,
    title: TrimmedNonEmptyString,
    html_url: TrimmedNonEmptyString,
    state: Schema.Literals(["open", "closed"]),
    draft: Schema.Boolean,
    closed_at: Schema.NullOr(Schema.String),
    merged_at: Schema.NullOr(Schema.String),
    updated_at: Schema.DateTimeUtcFromString,
    base: Schema.Struct({ ref: TrimmedNonEmptyString, repo: RestRepository }),
    head: Schema.Struct({
      ref: TrimmedNonEmptyString,
      sha: Schema.optional(Schema.NullOr(Schema.String)),
      repo: RestRepository,
      user: Schema.NullOr(Schema.Struct({ login: Schema.String })),
    }),
  }),
);

/** Raw row count owns pagination: skipped malformed rows must not end a full page. */
function decodeGitHubRestPullRequestPage(raw: string) {
  return Result.map(decodeGitHubPullRequestList(raw), (entries) => ({
    rawCount: entries.length,
    pullRequests: entries.flatMap((entry): NormalizedGitHubPullRequestRecord[] => {
      const decoded = decodeRestEntry(entry);
      if (!Exit.isSuccess(decoded)) return [];
      const row = decoded.value;
      const headRepositoryNameWithOwner = row.head.repo?.full_name;
      const headRepositoryOwnerLogin = row.head.repo?.owner.login ?? row.head.user?.login;
      return [
        {
          number: row.number,
          title: row.title,
          url: row.html_url,
          baseRefName: row.base.ref,
          headRefName: row.head.ref,
          ...(row.head.sha?.trim() ? { headSha: row.head.sha.trim() } : {}),
          state: row.merged_at ? "merged" : row.state,
          ...(row.draft ? { isDraft: true } : {}),
          closedAt: row.closed_at,
          mergedAt: row.merged_at,
          updatedAt: Option.some(row.updated_at),
          ...(row.head.repo && row.base.repo
            ? {
                isCrossRepository:
                  row.head.repo.full_name.toLowerCase() !== row.base.repo.full_name.toLowerCase(),
              }
            : {}),
          ...(headRepositoryNameWithOwner ? { headRepositoryNameWithOwner } : {}),
          ...(headRepositoryOwnerLogin ? { headRepositoryOwnerLogin } : {}),
        },
      ];
    }),
  }));
}

class QualifiedHeadDecodeError extends Schema.TaggedError<QualifiedHeadDecodeError>()(
  "QualifiedHeadDecodeError",
  { cause: Schema.Defect() },
) {}
class QualifiedHeadPageLimitError extends Schema.TaggedError<QualifiedHeadPageLimitError>()(
  "QualifiedHeadPageLimitError",
  {},
) {}

export const listQualifiedHeadPullRequests = Effect.fn("listQualifiedHeadPullRequests")(
  function* (input: {
    readonly locator: GitHubRepositoryLocator;
    readonly headSelector: string;
    readonly state: "open" | "closed" | "merged" | "all";
    readonly limit: number;
  }) {
    const api = yield* GitHubRest.GitHubRest;
    const credential = yield* api.credential(input.locator.host);
    return yield* Effect.gen(function* () {
      const rows: NormalizedGitHubPullRequestRecord[] = [];
      // Fail at the bound rather than report partial discovery as exhaustive.
      for (let page = 1; page <= 10; page++) {
        const query = new URLSearchParams({
          head: input.headSelector,
          state: input.state === "merged" ? "closed" : input.state,
          sort: "created",
          direction: "desc",
          per_page: "100",
          page: String(page),
        });
        const response = yield* api.rest({
          host: input.locator.host,
          operation: "listPullRequestsByHead",
          path: `repos/${encodeURIComponent(input.locator.owner)}/${encodeURIComponent(input.locator.name)}/pulls?${query}`,
          // This path must respect pauses even under an ambient interactive request.
          allowReserve: false,
        });
        if (response.truncated || response.invalidUtf8) {
          return yield* new QualifiedHeadDecodeError({
            cause: new Error("Incomplete REST response"),
          });
        }
        const decoded = decodeGitHubRestPullRequestPage(response.body);
        if (Result.isFailure(decoded)) {
          return yield* new QualifiedHeadDecodeError({ cause: decoded.failure });
        }
        rows.push(
          ...decoded.success.pullRequests.filter(
            (row) => input.state === "all" || row.state === input.state,
          ),
        );
        if (rows.length >= input.limit || decoded.success.rawCount < 100) {
          return rows.slice(0, input.limit);
        }
      }
      return yield* new QualifiedHeadPageLimitError({});
    }).pipe(
      Effect.provideService(GitHubRequestPolicy.PinnedGitHubCredential, {
        host: GitHubRequestPolicy.normalizeHost(input.locator.host),
        token: credential.token,
        credentialFingerprint: credential.fingerprint,
      }),
    );
  },
);
