import type { WorkstreamPrLocator, WorkstreamPrObservation } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { ProviderError } from "./localAttestation.ts";

type PrObservation = typeof WorkstreamPrObservation.Type;
export type PrSnapshot = NonNullable<PrObservation["last_success"]>;
export interface LinkedPrObservationRequest {
  readonly source_instance_id: string;
  readonly locator: WorkstreamPrLocator;
  readonly previous_success: PrSnapshot | null;
  readonly etag: string | null;
}
export interface LinkedPrObservationResult {
  readonly outcome: PrObservation["outcome"];
  readonly observed_at: string;
  readonly retry_after_seconds: number | null;
  readonly last_success: PrSnapshot | null;
  readonly etag: string | null;
}
export interface GitHubPrHttpRequest {
  readonly url: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly signal: AbortSignal;
  readonly redirect: "error";
}
export interface GitHubPrHttpResponse {
  readonly status: number;
  readonly headers: Readonly<Record<string, string | undefined>>;
  readonly body: Uint8Array | AsyncIterable<Uint8Array>;
}
export class GitHubPrTransport extends Context.Service<
  GitHubPrTransport,
  {
    readonly get: (
      input: GitHubPrHttpRequest,
    ) => Effect.Effect<GitHubPrHttpResponse, ProviderError>;
  }
>()("t3/jones/workstreams/native/providers/githubPrObserver/GitHubPrTransport") {}

export class GitHubPrAccess extends Context.Service<
  GitHubPrAccess,
  {
    readonly source_instance_id: string;
    readonly authorization_header: string;
    readonly user_agent: string;
  }
>()("t3/jones/workstreams/native/providers/githubPrObserver/GitHubPrAccess") {}

export class LinkedPrObserver extends Context.Service<
  LinkedPrObserver,
  {
    readonly observe: (
      input: LinkedPrObservationRequest,
    ) => Effect.Effect<LinkedPrObservationResult, ProviderError>;
  }
>()("t3/jones/workstreams/native/providers/githubPrObserver/LinkedPrObserver") {}

export const GITHUB_PR_OBSERVATION_TIMEOUT_MS = 15_000;
export const GITHUB_PR_OBSERVATION_BODY_LIMIT_BYTES = 262_144;
export const GITHUB_PR_ETAG_LIMIT_BYTES = 512;

const OWNER_PATTERN = /^[a-z0-9](?:[a-z0-9-]{0,37}[a-z0-9])?$/;
const REPOSITORY_PATTERN = /^[a-z0-9._-]{1,100}$/;
const UTC_TIMESTAMP_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$/;

function validUtcTimestamp(value: unknown): value is string {
  if (typeof value !== "string" || !UTC_TIMESTAMP_PATTERN.test(value)) return false;
  const parsed = Date.parse(value);
  return (
    Number.isFinite(parsed) &&
    DateTime.formatIso(DateTime.makeUnsafe(parsed)).slice(0, 19) === value.slice(0, 19)
  );
}

function validHeaderValue(value: string, maximumBytes: number): boolean {
  return !/[\r\n]/.test(value) && Buffer.byteLength(value, "utf8") <= maximumBytes;
}

function responseHeader(
  headers: Readonly<Record<string, string | undefined>>,
  name: string,
): string | null {
  const expected = name.toLowerCase();
  for (const [candidate, value] of Object.entries(headers)) {
    if (candidate.toLowerCase() === expected && value !== undefined) return value;
  }
  return null;
}

function retainedResult(
  input: LinkedPrObservationRequest,
  outcome: PrObservation["outcome"],
  observedAt: string,
  retryAfterSeconds: number | null,
): LinkedPrObservationResult {
  return {
    outcome,
    observed_at: observedAt,
    retry_after_seconds: retryAfterSeconds,
    last_success: input.previous_success,
    etag: input.etag,
  };
}

function parseRetryDelay(response: GitHubPrHttpResponse, now: string): number {
  const retryAfter = responseHeader(response.headers, "retry-after");
  let delay: number | null = null;
  if (retryAfter !== null && /^\d+$/.test(retryAfter.trim())) {
    delay = Number(retryAfter.trim());
  } else if (retryAfter !== null) {
    const retryAt = Date.parse(retryAfter);
    if (Number.isFinite(retryAt)) delay = Math.ceil((retryAt - Date.parse(now)) / 1_000);
  }
  if (delay === null) {
    const reset = responseHeader(response.headers, "x-ratelimit-reset");
    if (reset !== null && /^\d+$/.test(reset.trim())) {
      delay = Math.ceil(Number(reset.trim()) - Date.parse(now) / 1_000);
    }
  }
  if (delay === null || !Number.isSafeInteger(delay) || delay < 1) return 60;
  return Math.min(delay, 3_600);
}

function isRateLimited(response: GitHubPrHttpResponse): boolean {
  if (response.status === 429) return true;
  if (response.status !== 403) return false;
  return (
    responseHeader(response.headers, "x-ratelimit-remaining")?.trim() === "0" ||
    responseHeader(response.headers, "retry-after") !== null
  );
}

function parseSnapshot(
  body: Uint8Array,
  owner: string,
  repository: string,
  number: number,
  observedAt: string,
): PrSnapshot | null {
  let decoded: string;
  let value: unknown;
  try {
    decoded = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(body);
    value = JSON.parse(decoded) as unknown;
  } catch {
    return null;
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) return null;
  const pull = value as Record<string, unknown>;
  const base = pull.base;
  if (base === null || typeof base !== "object" || Array.isArray(base)) return null;
  const repo = (base as Record<string, unknown>).repo;
  if (repo === null || typeof repo !== "object" || Array.isArray(repo)) return null;
  const fullName = (repo as Record<string, unknown>).full_name;
  if (typeof fullName !== "string" || fullName.toLowerCase() !== `${owner}/${repository}`)
    return null;
  if (
    pull.number !== number ||
    (pull.state !== "open" && pull.state !== "closed") ||
    typeof pull.draft !== "boolean"
  )
    return null;
  if (typeof pull.merged !== "boolean" || !validUtcTimestamp(pull.updated_at)) return null;
  if (pull.merged && !validUtcTimestamp(pull.merged_at)) return null;
  if (!pull.merged && pull.merged_at !== null) return null;
  return {
    state: pull.merged ? "merged" : pull.state,
    draft: pull.draft,
    observed_at: observedAt,
    provider_updated_at: pull.updated_at,
  };
}

async function readBoundedBody(
  body: GitHubPrHttpResponse["body"],
  controller: AbortController,
): Promise<Uint8Array | null> {
  if (body instanceof Uint8Array) {
    if (body.byteLength > GITHUB_PR_OBSERVATION_BODY_LIMIT_BYTES) {
      controller.abort();
      return null;
    }
    return body;
  }
  if (body === null || typeof body !== "object" || !(Symbol.asyncIterator in body)) return null;
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    for await (const chunk of body) {
      if (
        !(chunk instanceof Uint8Array) ||
        length + chunk.byteLength > GITHUB_PR_OBSERVATION_BODY_LIMIT_BYTES
      ) {
        controller.abort();
        return null;
      }
      chunks.push(chunk);
      length += chunk.byteLength;
    }
  } catch {
    return null;
  }
  const joined = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    joined.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return joined;
}

const make = Effect.gen(function* () {
  const access = yield* GitHubPrAccess;
  const transport = yield* GitHubPrTransport;
  return LinkedPrObserver.of({
    observe: Effect.fn("LinkedPrObserver.observe")(function* (input) {
      if (
        !access.source_instance_id ||
        !access.authorization_header ||
        !access.user_agent ||
        !validHeaderValue(access.authorization_header, 8_192) ||
        !validHeaderValue(access.user_agent, 512)
      )
        return yield* new ProviderError({ code: "provider_unconfigured" });
      const now = () => DateTime.now.pipe(Effect.map(DateTime.formatIso));
      if (input.source_instance_id !== access.source_instance_id)
        return retainedResult(input, "inaccessible", yield* now(), null);
      const { locator } = input;
      if (
        locator.host !== "github.com" ||
        !OWNER_PATTERN.test(locator.repository_owner) ||
        !REPOSITORY_PATTERN.test(locator.repository_name) ||
        [".", ".."].includes(locator.repository_name) ||
        !Number.isSafeInteger(locator.number) ||
        locator.number < 1 ||
        locator.number > 2_147_483_647 ||
        (input.etag !== null &&
          (!input.etag || !validHeaderValue(input.etag, GITHUB_PR_ETAG_LIMIT_BYTES)))
      )
        return retainedResult(input, "invalid_response", yield* now(), null);
      const headers: Record<string, string> = {
        accept: "application/vnd.github+json",
        authorization: access.authorization_header,
        "user-agent": access.user_agent,
        "x-github-api-version": "2022-11-28",
      };
      if (input.etag !== null) headers["if-none-match"] = input.etag;
      const fetched = yield* Effect.acquireUseRelease(
        Effect.sync(() => new AbortController()),
        (controller) =>
          Effect.gen(function* () {
            const response = yield* transport.get({
              url: `https://api.github.com/repos/${locator.repository_owner}/${locator.repository_name}/pulls/${locator.number}`,
              headers,
              signal: controller.signal,
              redirect: "error",
            });
            const body = yield* Effect.tryPromise({
              try: () => readBoundedBody(response.body, controller),
              catch: () => new ProviderError({ code: "provider_unavailable" }),
            });
            return { response, body };
          }).pipe(
            Effect.timeoutOption(GITHUB_PR_OBSERVATION_TIMEOUT_MS),
            Effect.catchTags({ WorkstreamsProviderError: () => Effect.succeed(Option.none()) }),
          ),
        (controller) => Effect.sync(() => controller.abort()),
      );
      const observedAt = yield* now();
      if (Option.isNone(fetched)) return retainedResult(input, "unavailable", observedAt, null);
      const { response, body } = fetched.value;
      if (!Number.isSafeInteger(response.status) || body === null)
        return retainedResult(input, "invalid_response", observedAt, null);
      if (isRateLimited(response))
        return retainedResult(
          input,
          "rate_limited",
          observedAt,
          parseRetryDelay(response, observedAt),
        );
      if ([401, 403, 404].includes(response.status))
        return retainedResult(input, "inaccessible", observedAt, null);
      if (response.status >= 500 && response.status <= 599)
        return retainedResult(input, "unavailable", observedAt, null);
      const etag = responseHeader(response.headers, "etag");
      if (etag !== null && (!etag || !validHeaderValue(etag, GITHUB_PR_ETAG_LIMIT_BYTES)))
        return retainedResult(input, "invalid_response", observedAt, null);
      if (response.status === 304) {
        if (input.previous_success === null)
          return retainedResult(input, "invalid_response", observedAt, null);
        return {
          outcome: "not_modified",
          observed_at: observedAt,
          retry_after_seconds: null,
          last_success: { ...input.previous_success, observed_at: observedAt },
          etag: etag ?? input.etag,
        };
      }
      if (response.status !== 200)
        return retainedResult(input, "invalid_response", observedAt, null);
      const snapshot = parseSnapshot(
        body,
        locator.repository_owner,
        locator.repository_name,
        locator.number,
        observedAt,
      );
      if (snapshot === null) return retainedResult(input, "invalid_response", observedAt, null);
      return {
        outcome: "observed",
        observed_at: observedAt,
        retry_after_seconds: null,
        last_success: snapshot,
        etag,
      };
    }),
  });
});

export const layer = Layer.effect(LinkedPrObserver, make);
export const unconfiguredLayer = Layer.succeed(LinkedPrObserver, {
  observe: () => Effect.fail(new ProviderError({ code: "provider_unconfigured" })),
});
