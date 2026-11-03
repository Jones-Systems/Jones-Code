import { EnvironmentAuthInvalidError } from "@t3tools/contracts";
import type { PreparedConnection } from "../../connection/model.ts";
import type { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import type { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../../state/environmentHttpAuth.ts";
import {
  NativeReadError,
  nativeReadRequest,
  validateNativeReadResult,
  validateNativeReadScope,
  type NativeReadScope,
  type NativeReadSha256,
} from "../../state/workstreams/nativeRead.ts";
import {
  validateNativeReadExport,
  type NativeCompletedExport,
} from "../../state/workstreams/nativeExport.ts";
import { WorkstreamsHomeReadResultRequest } from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import { WorkstreamsNativeReadExportQuery } from "@t3tools/contracts/jones/workstreamsNativeExport";
import * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

export interface EnvironmentNativeWorkstreamsInput {
  readonly prepared: PreparedConnection;
  readonly signer: Option.Option<ManagedRelayDpopSigner["Service"]>;
  readonly remoteAuthorization?: Option.Option<RemoteEnvironmentAuthorization["Service"]>;
  /** The application's current authenticated binding, never inferred from a read response. */
  readonly scope: NativeReadScope;
  readonly sha256?: NativeReadSha256;
  readonly timeoutMs?: number;
}
export interface EnvironmentNativeCompletedExport {
  readonly bytes: Uint8Array;
  readonly completion: NativeCompletedExport;
}
const validation = <A>(run: (signal: AbortSignal) => Promise<A>) =>
  Effect.tryPromise({
    try: run,
    catch: (cause) =>
      cause instanceof NativeReadError
        ? cause
        : new NativeReadError("invalid_response", "Invalid native Workstreams response."),
  });
const copyRequest = Schema.decodeUnknownEffect(WorkstreamsHomeReadResultRequest);
const requestJson = Schema.fromJsonString(WorkstreamsHomeReadResultRequest);
const decodeRequestJson = Schema.decodeEffect(requestJson);
const encodeRequestJson = Schema.encodeEffect(requestJson);
const decodeCredentialError = Schema.decodeUnknownEffect(EnvironmentAuthInvalidError);
const copyQuery = Schema.decodeUnknownEffect(WorkstreamsNativeReadExportQuery);
const scopeFor = (input: EnvironmentNativeWorkstreamsInput) =>
  Effect.try({
    try: () => validateNativeReadScope(input.scope),
    catch: () =>
      new NativeReadError(
        "scope_changed",
        "Refresh native Workstreams authorization before reading.",
      ),
  });

/** One native request through the existing HTTP credential owner; no legacy or transport retry. */
export const fetchEnvironmentNativeWorkstreamRead = Effect.fn(
  "clientRuntime.jones.workstreams.fetchEnvironmentNativeWorkstreamRead",
)(function* (
  input: EnvironmentNativeWorkstreamsInput & {
    readonly request: WorkstreamsHomeReadResultRequest;
  },
) {
  const scope = yield* scopeFor(input);
  const validated = yield* copyRequest(input.request, { onExcessProperty: "error" });
  // Retain the validated request before credential renewal yields to another application task.
  const request = yield* decodeRequestJson(yield* encodeRequestJson(validated));
  const result = yield* executeAuthenticatedEnvironmentHttpRequest({
    prepared: input.prepared,
    signer: input.signer,
    ...(input.remoteAuthorization === undefined
      ? {}
      : { remoteAuthorization: input.remoteAuthorization }),
    method: "POST",
    group: "workstreams",
    url: (urls) => urls.nativeRead(),
    timeoutMs: input.timeoutMs ?? 20_000,
    request: ({ client, headers }) => client.nativeRead({ headers, payload: request }),
  });
  return yield* validation(() => validateNativeReadResult(result, request, scope, input.sha256));
});

/**
 * The wire export carries no grant revision. Fresh counts bracket the transfer under the same
 * application scope; only complete original bytes and a current final binding can be released.
 * The application still owns scope/session replacement and cancels the enclosing Effect lifetime.
 */
export const fetchEnvironmentNativeWorkstreamExport = Effect.fn(
  "clientRuntime.jones.workstreams.fetchEnvironmentNativeWorkstreamExport",
)(function* (
  input: EnvironmentNativeWorkstreamsInput & {
    readonly sha256: NativeReadSha256;
    readonly pageSize?: number;
  },
) {
  const scope = yield* scopeFor(input);
  const query = yield* copyQuery(
    input.pageSize === undefined ? {} : { page_size: String(input.pageSize) },
    { onExcessProperty: "error" },
  );
  const current = () =>
    fetchEnvironmentNativeWorkstreamRead({
      ...input,
      scope,
      request: nativeReadRequest({ route: "counts", params: {} }),
    }).pipe(
      Effect.flatMap((result) =>
        result.envelope.coverage.registry === "complete" &&
        result.envelope.provenance.source === "live" &&
        !result.envelope.provenance.stale
          ? Effect.succeed(result)
          : Effect.fail(new NativeReadError("incomplete", "Native home coverage is incomplete.")),
      ),
    );
  yield* current();
  const bytes = yield* executeAuthenticatedEnvironmentHttpRequest({
    prepared: input.prepared,
    signer: input.signer,
    ...(input.remoteAuthorization === undefined
      ? {}
      : { remoteAuthorization: input.remoteAuthorization }),
    method: "GET",
    group: "workstreams",
    url: (urls) => urls.nativeExport({ query }),
    timeoutMs: input.timeoutMs ?? 20_000,
    request: ({ client, headers }) =>
      client.nativeExport({ headers, query, responseMode: "response-only" }).pipe(
        Effect.flatMap((response) =>
          Effect.gen(function* () {
            if (response.status === 401) {
              const rejected = yield* decodeCredentialError(yield* response.json);
              return yield* Effect.fail(rejected);
            }
            if (
              response.status !== 200 ||
              response.headers["content-type"]?.split(";")[0]?.trim().toLowerCase() !==
                "application/x-ndjson"
            )
              return yield* Effect.fail(
                new NativeReadError("invalid_response", "Invalid native export transport."),
              );
            let length = 0;
            const chunks = yield* response.stream.pipe(
              Stream.mapEffect((chunk) => {
                length += chunk.length;
                return length > 16 * 1024 * 1024 + 1024
                  ? Effect.fail(new NativeReadError("bounds", "Native export byte bound exceeded."))
                  : Effect.succeed(new Uint8Array(chunk));
              }),
              Stream.runCollect,
            );
            const original = new Uint8Array(length);
            let offset = 0;
            for (const chunk of chunks) {
              original.set(chunk, offset);
              offset += chunk.length;
            }
            return original;
          }),
        ),
      ),
  });
  const completion = yield* validation((signal) =>
    validateNativeReadExport(
      (async function* () {
        yield bytes;
      })(),
      { scope, sha256: input.sha256, signal },
    ),
  );
  yield* current();
  return { bytes, completion } satisfies EnvironmentNativeCompletedExport;
});
