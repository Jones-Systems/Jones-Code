import {
  WORKSTREAMS_T3_PROVIDER_PROTOCOL,
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
const invalid: Extract<WorkstreamsNativeContextResponse, { state: "rejected" }> = {
  protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
  state: "rejected",
  reason: "invalid_request",
};
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
  const value = yield* Schema.decodeUnknownEffect(jsonCodec)(text).pipe(
    Effect.mapError(() => new NativeProviderBodyError()),
  );
  if (!withinDepth(value)) return yield* new NativeProviderBodyError();
  return { value, requestBytesSha256: sha256Bytes(bytes) };
});

const render = Effect.fn("NativeProviderHttp.render")(function* (
  operation: NativeProviderOperation,
  response:
    | WorkstreamsNativeContextResponse
    | WorkstreamsNativeAttestationResponse
    | WorkstreamsNativeSettlementResponse,
) {
  const schema =
    operation === "context"
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
      '{"protocol":"workstreams-t3-provider/1.0.0","state":"rejected","reason":"invalid_request"}',
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
  provider: WorkstreamsNativeProvider,
  enrollments: NativeProviderEnrollment["Service"],
) => {
  const handle = Effect.fn("NativeProviderHttp.handle")(function* <E>(
    operation: NativeProviderOperation,
    authenticatedSessionId: string | null,
    body: Stream.Stream<Uint8Array, E>,
  ) {
    if (authenticatedSessionId === null)
      return yield* render(operation, {
        protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
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
      return yield* render(operation, {
        protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
        state: "rejected",
        reason: "forbidden",
      });
    const binding = yield* Schema.decodeUnknownEffect(NativeProviderEnrollmentBinding)(
      enrolled.value.value.value,
    ).pipe(Effect.option);
    if (Option.isNone(binding) || binding.value.session_id !== authenticatedSessionId)
      return yield* render(operation, {
        protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
        state: "rejected",
        reason: "caller_mismatch",
      });
    const parsed = yield* decodeBody(body, operation === "context").pipe(
      Effect.option,
      Effect.timeoutOption(WORKSTREAMS_T3_PROVIDER_METADATA_TIMEOUT_MS),
    );
    if (Option.isNone(parsed) || Option.isNone(parsed.value))
      return yield* render(operation, invalid);
    const input = parsed.value.value;
    if (operation === "context") {
      const request = yield* Schema.decodeUnknownEffect(WorkstreamsNativeContextRequest)(
        input.value,
      ).pipe(Effect.option);
      if (Option.isNone(request)) return yield* render(operation, invalid);
      const result = yield* provider
        .context(binding.value)
        .pipe(Effect.timeoutOption(WORKSTREAMS_T3_PROVIDER_METADATA_TIMEOUT_MS));
      return yield* render(
        operation,
        Option.getOrElse(result, () => ({
          protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
          state: "unavailable" as const,
          reason: "authority_unavailable" as const,
        })),
      );
    }
    if (operation === "attestations") {
      const request = yield* Schema.decodeUnknownEffect(WorkstreamsNativeAttestationRequest)(
        input.value,
      ).pipe(Effect.option);
      if (Option.isNone(request)) return yield* render(operation, invalid);
      const result = yield* provider
        .attest(binding.value, request.value)
        .pipe(Effect.timeoutOption(WORKSTREAMS_T3_PROVIDER_METADATA_TIMEOUT_MS));
      return yield* render(
        operation,
        Option.getOrElse(result, () => ({
          protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
          state: "unknown" as const,
          reason: "observation_unavailable" as const,
        })),
      );
    }
    const request = yield* Schema.decodeUnknownEffect(WorkstreamsNativeSettlementRequest)(
      input.value,
    ).pipe(Effect.option);
    if (Option.isNone(request)) return yield* render(operation, invalid);
    const action = operation === "settlements" ? provider.settle : provider.lookup;
    const result = yield* action(binding.value, request.value, input.requestBytesSha256).pipe(
      Effect.timeoutOption(
        operation === "settlements"
          ? WORKSTREAMS_T3_PROVIDER_SETTLEMENT_TIMEOUT_MS
          : WORKSTREAMS_T3_PROVIDER_METADATA_TIMEOUT_MS,
      ),
    );
    return yield* render(
      operation,
      Option.getOrElse(result, () => ({
        protocol: WORKSTREAMS_T3_PROVIDER_PROTOCOL,
        state: "unknown" as const,
        request: request.value,
        reason: "provider_unavailable" as const,
      })),
    );
  });
  return { handle };
};
