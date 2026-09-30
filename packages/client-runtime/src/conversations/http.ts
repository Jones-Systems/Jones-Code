import {
  EnvironmentConversationLibraryErrorSchema,
  type EnvironmentConversationLibraryError,
} from "@t3tools/contracts";
import type { LibraryReply, LibraryRequest } from "@t3tools/contracts/conversationLibrary";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";

import { RemoteEnvironmentAuthorization } from "../authorization/service.ts";
import type { PreparedConnection } from "../connection/model.ts";
import { ManagedRelayDpopSigner } from "../relay/managedRelay.ts";
import {
  makeEnvironmentHttpApiUrlBuilder,
  type RemoteEnvironmentRequestError,
} from "../rpc/http.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../state/environmentHttpAuth.ts";

const DEFAULT_CONVERSATION_LIBRARY_TIMEOUT_MS = 30_000;
const isConversationLibraryError = Schema.is(EnvironmentConversationLibraryErrorSchema);

export type ConversationLibraryRequestError =
  | RemoteEnvironmentRequestError
  | EnvironmentConversationLibraryError;

export const fetchEnvironmentConversationLibraryRequest = Effect.fn(
  "clientRuntime.conversations.fetchEnvironmentConversationLibraryRequest",
)(function* (input: {
  readonly prepared: PreparedConnection;
  readonly request: LibraryRequest;
  readonly signer: Option.Option<ManagedRelayDpopSigner["Service"]>;
  readonly remoteAuthorization?: Option.Option<RemoteEnvironmentAuthorization["Service"]>;
  readonly timeoutMs?: number;
}) {
  const remoteAuthorization =
    input.remoteAuthorization ?? (yield* Effect.serviceOption(RemoteEnvironmentAuthorization));
  return yield* executeAuthenticatedEnvironmentHttpRequest({
    ...input,
    remoteAuthorization,
    group: "conversationLibrary",
    method: "POST",
    url: (httpBaseUrl) =>
      makeEnvironmentHttpApiUrlBuilder(httpBaseUrl).conversationLibrary.conversationLibrary(),
    timeoutMs: input.timeoutMs ?? DEFAULT_CONVERSATION_LIBRARY_TIMEOUT_MS,
    request: ({ client, headers }) => {
      switch (input.request.kind) {
        case "hello":
          return client.conversationLibrary({ payload: input.request, headers });
        case "accounts":
          return client.conversationLibrary({ payload: input.request, headers });
        case "createAccount":
          return client.conversationLibrary({ payload: input.request, headers });
        case "preview":
          return client.conversationLibrary({ payload: input.request, headers });
        case "import":
          return client.conversationLibrary({ payload: input.request, headers });
        case "list":
          return client.conversationLibrary({ payload: input.request, headers });
        case "detail":
          return client.conversationLibrary({ payload: input.request, headers });
        case "update":
          return client.conversationLibrary({ payload: input.request, headers });
        case "selectSnapshot":
          return client.conversationLibrary({ payload: input.request, headers });
        case "remove":
          return client.conversationLibrary({ payload: input.request, headers });
      }
    },
  }).pipe(
    Effect.mapError((error) =>
      error._tag === "RemoteEnvironmentAuthFetchError" && isConversationLibraryError(error.cause)
        ? error.cause
        : error,
    ),
  );
});

export class ConversationLibraryRequester extends Context.Service<
  ConversationLibraryRequester,
  {
    readonly request: (
      prepared: PreparedConnection,
      request: LibraryRequest,
    ) => Effect.Effect<LibraryReply, ConversationLibraryRequestError>;
  }
>()("@t3tools/client-runtime/conversations/http/ConversationLibraryRequester") {}

export const conversationLibraryRequesterLayer: Layer.Layer<
  ConversationLibraryRequester,
  never,
  HttpClient.HttpClient
> = Layer.effect(
  ConversationLibraryRequester,
  Effect.gen(function* () {
    const httpClient = yield* HttpClient.HttpClient;
    const signer = yield* Effect.serviceOption(ManagedRelayDpopSigner);
    const remoteAuthorization = yield* Effect.serviceOption(RemoteEnvironmentAuthorization);
    return ConversationLibraryRequester.of({
      request: (prepared, request) =>
        fetchEnvironmentConversationLibraryRequest({
          prepared,
          request,
          signer,
          remoteAuthorization,
        }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient)),
    });
  }),
);
