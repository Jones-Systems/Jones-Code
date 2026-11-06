import {
  WORKSTREAMS_T3_PROVIDER_PROTOCOL,
  WORKSTREAMS_T3_PROVIDER_V2_PROTOCOL,
  WorkstreamsNativeContextResponseV2,
  WorkstreamsNativeAttestationResponseV2,
  WorkstreamsNativeSettlementResponseV2,
  WORKSTREAMS_T3_PROVIDER_MAX_REQUEST_BYTES,
  WORKSTREAMS_T3_PROVIDER_MAX_RESPONSE_BYTES,
  WORKSTREAMS_T3_PROVIDER_MAX_JSON_DEPTH,
  WORKSTREAMS_T3_PROVIDER_METADATA_TIMEOUT_MS,
  WORKSTREAMS_T3_PROVIDER_SETTLEMENT_TIMEOUT_MS,
  WorkstreamsNativeContextRequest,
  WorkstreamsNativeContextResponse,
  WorkstreamsNativeAttestationRequest,
  WorkstreamsNativeAttestationResponse,
  WorkstreamsNativeSettlementRequest,
  WorkstreamsNativeSettlementResponse,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { NativeProviderEnrollmentBinding, type NativeProviderEnrollment } from "./enrollment.ts";
import { sha256Bytes, type WorkstreamsNativeProvider } from "./service.ts";

export type NativeProviderOperation =
  | "context"
  | "attestations"
  | "settlements"
  | "settlements/lookup";
class NativeProviderBodyError extends Schema.TaggedError<NativeProviderBodyError>()(
  "NativeProviderBodyError",
  {},
) {}
const jsonCodec = Schema.fromJsonString(Schema.Json);

const withinDepth = (value: Schema.Json): boolean => {
  const pending = [{ value, depth: 0 }];
  while (pending.length > 0) {
    const current = pending.pop()!;
    if (current.depth > WORKSTREAMS_T3_PROVIDER_MAX_JSON_DEPTH) return false;
    if (current.value !== null && typeof current.value === "object") {
      for (const child of Object.values(current.value))
        pending.push({ value: child, depth: current.depth + 1 });
    }
  }
  return true;
};

const readBody = <E>(body: Stream.Stream<Uint8Array, E>) =>
  Stream.runFoldEffect(
    body,
    () => ({ chunks: [] as Uint8Array[], size: 0 }),
    (state, chunk) => {
      if (state.size + chunk.byteLength > WORKSTREAMS_T3_PROVIDER_MAX_REQUEST_BYTES)
        return Effect.fail(new NativeProviderBodyError());
      state.chunks.push(chunk);
      state.size += chunk.byteLength;
      return Effect.succeed(state);
    },
  ).pipe(
    Effect.map((state) => {
      const bytes = new Uint8Array(state.size);
      let offset = 0;
      for (const chunk of state.chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }
      return bytes;
    }),
    Effect.mapError(() => new NativeProviderBodyError()),
  );

const decodeBody = Effect.fn("NativeProviderHttp.decodeBody")(function* <E>(
  body: Stream.Stream<Uint8Array, E>,
  context: boolean,
) {
  const bytes = yield* readBody(body);
  if (context) {
    if (bytes.byteLength !== 0) return yield* new NativeProviderBodyError();
    return { value: {} as Schema.Json, requestBytesSha256: sha256Bytes(bytes) };
  }
  const text = yield* Effect.try({
    try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
    catch: () => new NativeProviderBodyError(),
  });
  const value = yield* Schema.decodeEffect(jsonCodec)(text).pipe(
    Effect.mapError(() => new NativeProviderBodyError()),
  );
  if (!withinDepth(value)) return yield* new NativeProviderBodyError();
  return { value, requestBytesSha256: sha256Bytes(bytes) };
});

const render = Effect.fn("NativeProviderHttp.render")(function* (
  profile: "v1" | "v2",
  operation: NativeProviderOperation,
  response:
    | WorkstreamsNativeContextResponse
    | WorkstreamsNativeAttestationResponse
    | WorkstreamsNativeSettlementResponse
    | WorkstreamsNativeContextResponseV2
    | WorkstreamsNativeAttestationResponseV2
    | WorkstreamsNativeSettlementResponseV2,
) {
  const schema =
    profile === "v2"
      ? operation === "context"
        ? WorkstreamsNativeContextResponseV2
        : operation === "attestations"
          ? WorkstreamsNativeAttestationResponseV2
          : WorkstreamsNativeSettlementResponseV2
      : operation === "context"
        ? WorkstreamsNativeContextResponse
        : operation === "attestations"
          ? WorkstreamsNativeAttestationResponse
          : WorkstreamsNativeSettlementResponse;
  const json = yield* Schema.encodeUnknownEffect(Schema.fromJsonString(schema))(response).pipe(
    Effect.option,
  );
  if (
    Option.isNone(json) ||
    new TextEncoder().encode(json.value).byteLength > WORKSTREAMS_T3_PROVIDER_MAX_RESPONSE_BYTES
  )
    return HttpServerResponse.text(
      `{"protocol":"${profile === "v2" ? WORKSTREAMS_T3_PROVIDER_V2_PROTOCOL : WORKSTREAMS_T3_PROVIDER_PROTOCOL}","state":"rejected","reason":"invalid_request"}`,
      { status: 500, contentType: "application/json", headers: { "cache-control": "no-store" } },
    );
  const status =
    response.state === "rejected"
      ? response.reason === "unauthorized"
        ? 401
        : response.reason === "forbidden"
          ? 403
          : response.reason === "idempotency_conflict"
            ? 409
            : 400
      : 200;
  return HttpServerResponse.text(json.value, {
    status,
    contentType: "application/json",
    headers: { "cache-control": "no-store" },
  });
});

export const createNativeProviderHandlers = (
  provider: WorkstreamsNativeProvider<"v1" | "v2">,
  enrollments: NativeProviderEnrollment["Service"],
) => {
  const protocol =
    provider.profile === "v2"
      ? WORKSTREAMS_T3_PROVIDER_V2_PROTOCOL
      : WORKSTREAMS_T3_PROVIDER_PROTOCOL;
  const invalid = { protocol, state: "rejected", reason: "invalid_request" } as const;
  const renderResponse = (
    operation: NativeProviderOperation,
    response: Parameters<typeof render>[2],
  ) => render(provider.profile, operation, response);
  const handle = Effect.fn("NativeProviderHttp.handle")(function* <E>(
    operation: NativeProviderOperation,
    authenticatedSessionId: string | null,
    body: Stream.Stream<Uint8Array, E>,
  ) {
    if (authenticatedSessionId === null)
      return yield* renderResponse(operation, {
        protocol,
        state: "rejected",
        reason: "unauthorized",
      });
    const enrolled = yield* enrollments
      .getBySessionId(authenticatedSessionId)
      .pipe(Effect.option, Effect.timeoutOption(WORKSTREAMS_T3_PROVIDER_METADATA_TIMEOUT_MS));
    if (
      Option.isNone(enrolled) ||
      Option.isNone(enrolled.value) ||
      Option.isNone(enrolled.value.value)
    )
      return yield* renderResponse(operation, {
        protocol,
        state: "rejected",
        reason: "forbidden",
      });
    const binding = yield* Schema.decodeEffect(NativeProviderEnrollmentBinding)(
      enrolled.value.value.value,
    ).pipe(Effect.option);
    if (Option.isNone(binding) || binding.value.session_id !== authenticatedSessionId)
      return yield* renderResponse(operation, {
        protocol,
        state: "rejected",
        reason: "caller_mismatch",
      });
    const parsed = yield* decodeBody(body, operation === "context").pipe(
      Effect.option,
      Effect.timeoutOption(WORKSTREAMS_T3_PROVIDER_METADATA_TIMEOUT_MS),
    );
    if (Option.isNone(parsed) || Option.isNone(parsed.value))
      return yield* renderResponse(operation, invalid);
    const input = parsed.value.value;
    if (operation === "context") {
      const request = yield* Schema.decodeUnknownEffect(WorkstreamsNativeContextRequest)(
        input.value,
      ).pipe(Effect.option);
      if (Option.isNone(request)) return yield* renderResponse(operation, invalid);
      const result = yield* provider
        .context(binding.value)
        .pipe(Effect.timeoutOption(WORKSTREAMS_T3_PROVIDER_METADATA_TIMEOUT_MS));
      return yield* renderResponse(
        operation,
        Option.getOrElse(result, () => ({
          protocol,
          state: "unavailable" as const,
          reason: "authority_unavailable" as const,
        })),
      );
    }
    if (operation === "attestations") {
      const request = yield* Schema.decodeUnknownEffect(WorkstreamsNativeAttestationRequest)(
        input.value,
      ).pipe(Effect.option);
      if (Option.isNone(request)) return yield* renderResponse(operation, invalid);
      const result = yield* provider
        .attest(binding.value, request.value)
        .pipe(Effect.timeoutOption(WORKSTREAMS_T3_PROVIDER_METADATA_TIMEOUT_MS));
      return yield* renderResponse(
        operation,
        Option.getOrElse(result, () => ({
          protocol,
          state: "unknown" as const,
          reason: "observation_unavailable" as const,
        })),
      );
    }
    const request = yield* Schema.decodeUnknownEffect(WorkstreamsNativeSettlementRequest)(
      input.value,
    ).pipe(Effect.option);
    if (Option.isNone(request)) return yield* renderResponse(operation, invalid);
    const action = operation === "settlements" ? provider.settle : provider.lookup;
    const result = yield* action(binding.value, request.value, input.requestBytesSha256).pipe(
      Effect.timeoutOption(
        operation === "settlements"
          ? WORKSTREAMS_T3_PROVIDER_SETTLEMENT_TIMEOUT_MS
          : WORKSTREAMS_T3_PROVIDER_METADATA_TIMEOUT_MS,
      ),
    );
    return yield* renderResponse(
      operation,
      Option.getOrElse(result, () => ({
        protocol,
        state: "unknown" as const,
        request: request.value,
        reason: "provider_unavailable" as const,
      })),
    );
  });
  return { handle };
};
