import { canonicalWorkstreamsSnapshotJson } from "@t3tools/contracts/jones/workstreamsSnapshot";
// @effect-diagnostics nodeBuiltinImport:off -- Hash original export bytes without writing plaintext or creating keys.
import * as NodeCrypto from "node:crypto";
import {
  WORKSTREAM_CONTRACT_HEADER_VERSION,
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
} from "@t3tools/contracts";
import {
  WORKSTREAMS_NATIVE_CONTRACT,
  WORKSTREAMS_NATIVE_READ_CONTRACT,
} from "@t3tools/contracts/jones/workstreamsNative";
import {
  WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
  WorkstreamsHomeReadResult,
} from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import { validateNativeReadExport } from "../../../../../../../packages/client-runtime/src/state/workstreams/nativeExport.ts";
import type { NativeReadScope } from "../../../../../../../packages/client-runtime/src/state/workstreams/nativeRead.ts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Stdio from "effect/Stdio";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import { Command, Flag } from "effect/cli";
import * as Cli from "../runtime/peerSetupCli.ts";

export class NativeExportCliError extends Schema.TaggedError<NativeExportCliError>()(
  "NativeExportCliError",
  {
    code: Schema.Literals([
      "invalid_request",
      "unavailable",
      "unsupported",
      "forbidden",
      "invalid_response",
      "output_failed",
    ]),
  },
) {
  override get message(): string {
    return "Native Workstream export failed (" + this.code + "). No completed export is available.";
  }
}
const decodeCounts = Schema.decodeUnknownEffect(Schema.fromJsonString(WorkstreamsHomeReadResult));
const countsRequest = JSON.stringify({
  contract: WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
  request: { route: "counts", params: {} },
});
const collectBytes = <E>(stream: Stream.Stream<Uint8Array, E>, limit: number) =>
  Stream.runFoldEffect(
    stream,
    () => ({ bytes: 0, chunks: [] as Uint8Array[] }),
    (state, chunk) => {
      if (state.bytes + chunk.length > limit || state.chunks.length >= 65536)
        return Effect.fail(new NativeExportCliError({ code: "invalid_response" }));
      state.chunks.push(chunk.slice());
      state.bytes += chunk.length;
      return Effect.succeed(state);
    },
  );
const responseFailure = (status: number) =>
  new NativeExportCliError({
    code:
      status === 404 || status === 405
        ? "unsupported"
        : status === 401 || status === 403
          ? "forbidden"
          : "unavailable",
  });
/** Explicit local transport: no URL comes from CLI flags and redirects are never followed.
 * Authenticated counts pin only this export's validation context; this is neither capability
 * advertisement nor a proof that could authorize a server operation. */
export const runNativeReadExportCli = Effect.fn("NativeExportCli.run")(function* (
  origin: string,
  headers: { readonly authorization: string },
  pageSize = 100,
) {
  const url = yield* Effect.try({
    try: () => new URL(origin),
    catch: () => new NativeExportCliError({ code: "invalid_request" }),
  });
  if (
    url.protocol !== "http:" ||
    !["127.0.0.1", "[::1]"].includes(url.hostname) ||
    url.origin !== origin ||
    url.username ||
    url.password ||
    !url.port ||
    !Number.isSafeInteger(pageSize) ||
    pageSize < 1 ||
    pageSize > 100
  )
    return yield* new NativeExportCliError({ code: "invalid_request" });
  const http = yield* HttpClient.HttpClient;
  const observe = Effect.gen(function* () {
    const response = yield* http
      .execute(
        HttpClientRequest.post(origin + "/api/workstreams/native/v1/read").pipe(
          HttpClientRequest.setHeader("authorization", headers.authorization),
          HttpClientRequest.bodyText(countsRequest, "application/json"),
        ),
      )
      .pipe(Effect.mapError(() => new NativeExportCliError({ code: "unavailable" })));
    if (response.status !== 200) return yield* responseFailure(response.status);
    const bytes = yield* collectBytes(response.stream, 1024 * 1024).pipe(
      Effect.mapError(() => new NativeExportCliError({ code: "invalid_response" })),
    );
    const text = yield* Effect.try({
      try: () => new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(bytes.chunks)),
      catch: () => new NativeExportCliError({ code: "invalid_response" }),
    });
    const result = yield* decodeCounts(text).pipe(
      Effect.mapError(() => new NativeExportCliError({ code: "invalid_response" })),
    );
    if (
      result.route !== "counts" ||
      result.envelope.provenance.authority !== "native-home" ||
      result.envelope.provenance.source !== "live" ||
      result.envelope.provenance.stale ||
      result.envelope.coverage.registry !== "complete" ||
      result.envelope.provenance.served_by_environment_id !==
        result.envelope.provenance.home_environment_id ||
      result.value.context.owner_id !== result.envelope.registry.owner_id ||
      result.value.context.server_generation !== result.envelope.registry.server_generation ||
      result.value.context.registry_version !== result.envelope.registry.registry_version ||
      result.value.observed_at !== result.envelope.provenance.observed_at
    )
      return yield* new NativeExportCliError({ code: "invalid_response" });
    const registry = result.envelope.registry;
    return {
      capability: {
        contract: WORKSTREAMS_NATIVE_CONTRACT,
        mode: "native-home",
        registry: {
          registry_instance_id: registry.registry_instance_id,
          owner_id: registry.owner_id,
          canonical_origin: registry.canonical_origin,
          home_environment_id: result.envelope.provenance.home_environment_id,
          server_generation: registry.server_generation,
        },
        reads: [WORKSTREAMS_NATIVE_READ_CONTRACT],
        placement_proofs: ["workstreams-t3-placement/1.0.0"],
      },
      binding: {
        registryId: registry.canonical_origin,
        ownerId: registry.owner_id,
        principalId: result.value.principal_id,
        authorizationRevision: result.authorization_revision,
        serverGeneration: registry.server_generation,
        registryVersion: registry.registry_version,
        permissions: ["workstreams:read"],
        contractVersion: WORKSTREAM_CONTRACT_HEADER_VERSION,
        contractManifest: WORKSTREAM_CONTRACT_MANIFEST_SHA256,
      },
    } satisfies NativeReadScope;
  });
  const scope = yield* observe;
  const response = yield* http
    .execute(
      HttpClientRequest.get(
        origin + "/api/workstreams/native/v1/export?page_size=" + pageSize,
      ).pipe(HttpClientRequest.setHeader("authorization", headers.authorization)),
    )
    .pipe(Effect.mapError(() => new NativeExportCliError({ code: "unavailable" })));
  if (response.status !== 200) return yield* responseFailure(response.status);
  if (response.headers["content-type"]?.split(";")[0]?.trim() !== "application/x-ndjson")
    return yield* new NativeExportCliError({ code: "invalid_response" });
  const bytes = yield* collectBytes(response.stream, 16 * 1024 * 1024 + 1024).pipe(
    Effect.mapError(() => new NativeExportCliError({ code: "invalid_response" })),
  );
  yield* Effect.tryPromise({
    try: (signal) =>
      validateNativeReadExport(
        (async function* () {
          yield* bytes.chunks;
        })(),
        {
          scope,
          signal,
          sha256: async (raw) => NodeCrypto.createHash("sha256").update(raw).digest("hex"),
        },
      ),
    catch: () => new NativeExportCliError({ code: "invalid_response" }),
  });
  const current = yield* observe;
  if (canonicalWorkstreamsSnapshotJson(scope) !== canonicalWorkstreamsSnapshotJson(current))
    return yield* new NativeExportCliError({ code: "invalid_response" });
  const stdio = yield* Stdio.Stdio;
  // Keep original bytes and add no newline, banner, reserialization or success suffix.
  yield* Stream.run(Stream.fromIterable(bytes.chunks), stdio.stdout()).pipe(
    Effect.mapError(() => new NativeExportCliError({ code: "output_failed" })),
  );
});
const runLive = Effect.fn("NativeExportCli.live")((home: string, pageSize: number) =>
  Cli.withSelectedCliSessions(home, "read-export", (origin, sessions) =>
    Effect.acquireUseRelease(
      sessions.issue(),
      (issued) =>
        runNativeReadExportCli(origin, { authorization: "Bearer " + issued.token }, pageSize),
      (issued) => sessions.revoke(issued.sessionId).pipe(Effect.orDie),
    ),
  ).pipe(
    Effect.provide(FetchHttpClient.layer),
    Effect.provideService(FetchHttpClient.RequestInit, { redirect: "error" }),
    Effect.scoped,
  ),
);
export const makeNativeExportCliCommand = <E, R>(
  run: (home: string, pageSize: number) => Effect.Effect<void, E, R>,
) =>
  Command.make("export", {
    home: Flag.String("home").pipe(
      Flag.withDescription("Existing local server home; the command never starts a server."),
    ),
    pageSize: Flag.Int("page-size").pipe(
      Flag.withSchema(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
      Flag.withDefault(100),
    ),
  }).pipe(
    Command.withDescription("Export a complete native-home Workstream snapshot as JSONL."),
    Command.withHandler((flags) => run(flags.home, flags.pageSize)),
  );
export const command = makeNativeExportCliCommand(runLive);
