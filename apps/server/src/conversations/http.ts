import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import type { EnvironmentConversationLibraryError } from "@t3tools/contracts";
import {
  LIBRARY_MAX_REQUEST_BYTES,
  LibraryRequestSchema,
  type LibraryErrorCode,
  type LibraryRequest,
} from "@t3tools/contracts/conversationLibrary";
import { libraryRequestMutates } from "@t3tools/shared/conversationLibrary";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";

import { requireEnvironmentScope } from "../auth/http.ts";
import * as ConversationLibrary from "./Service.ts";

const LIBRARY_ERROR_MESSAGE: Record<LibraryErrorCode, string> = {
  invalid: "The conversation library request is invalid.",
  "too-large": "The conversation library request exceeds its size limit.",
  "not-found": "The requested conversation library record was not found.",
  conflict: "The conversation library changed. Reload before retrying.",
  unsupported: "The conversation library format is not supported.",
  storage: "The conversation library could not be accessed.",
  forbidden: "The conversation library operation is not allowed.",
};
const decodeLibraryRequestJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(LibraryRequestSchema),
);

function conversationLibraryHttpError(
  code: LibraryErrorCode,
  traceId: string,
): EnvironmentConversationLibraryError {
  return {
    kind: "error",
    code,
    message: LIBRARY_ERROR_MESSAGE[code],
    traceId,
  } as EnvironmentConversationLibraryError;
}

function readRequestBody(request: HttpServerRequest.HttpServerRequest, traceId: string) {
  const contentType = request.headers["content-type"]?.split(";", 1)[0]?.trim().toLowerCase();
  if (contentType !== "application/json") {
    return Effect.fail(conversationLibraryHttpError("invalid", traceId));
  }

  const chunks: Uint8Array[] = [];
  let receivedBytes = 0;
  let tooLarge = false;
  let failed = false;
  return request.stream.pipe(
    Stream.takeWhile((chunk) => {
      receivedBytes += chunk.byteLength;
      if (receivedBytes > LIBRARY_MAX_REQUEST_BYTES) {
        tooLarge = true;
        return false;
      }
      chunks.push(chunk);
      return true;
    }),
    Stream.runDrain,
    Effect.catch(() => {
      failed = true;
      return Effect.void;
    }),
    Effect.flatMap(() => {
      if (tooLarge) return Effect.fail(conversationLibraryHttpError("too-large", traceId));
      if (failed) return Effect.fail(conversationLibraryHttpError("invalid", traceId));
      const bytes = new Uint8Array(receivedBytes);
      let offset = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }
      return Effect.succeed(bytes);
    }),
  );
}

function decodeLibraryRequest(bytes: Uint8Array, traceId: string) {
  return Effect.try({
    try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
    catch: () => conversationLibraryHttpError("invalid", traceId),
  }).pipe(
    Effect.flatMap((json) =>
      decodeLibraryRequestJson(json).pipe(
        Effect.mapError(() => conversationLibraryHttpError("invalid", traceId)),
      ),
    ),
  );
}

export const conversationLibraryHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "conversationLibrary",
  (handlers) =>
    handlers.handleRaw(
      "conversationLibrary",
      Effect.fn("environment.conversationLibrary")(function* ({ request }) {
        const principal = yield* requireEnvironmentScope(AuthOrchestrationReadScope);
        const traceId = yield* Effect.currentParentSpan.pipe(
          Effect.map((span) => span.traceId),
          Effect.orElseSucceed(() => "unavailable"),
        );
        const bytes = yield* readRequestBody(request, traceId);
        const libraryRequest: LibraryRequest = yield* decodeLibraryRequest(bytes, traceId);
        const mutates = libraryRequestMutates(libraryRequest);
        if (mutates) yield* requireEnvironmentScope(AuthOrchestrationOperateScope);

        const library = yield* ConversationLibrary.ConversationLibrary;
        return yield* library
          .execute(libraryRequest, principal.scopes.has(AuthOrchestrationOperateScope))
          .pipe(Effect.mapError((cause) => conversationLibraryHttpError(cause.code, traceId)));
      }),
    ),
).pipe(Layer.provide(ConversationLibrary.layer));
