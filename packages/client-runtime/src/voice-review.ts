import {
  VoiceReviewError,
  type VoiceReviewAction,
  type VoiceReviewMutationPayload,
  VoiceReviewEditSavePayload,
  VoiceReviewEditCancelPayload,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { RemoteEnvironmentAuthorization } from "./authorization/service.ts";
import type { PreparedConnection } from "./connection/model.ts";
import type { ManagedRelayDpopSigner } from "./relay/managedRelay.ts";
import { makeEnvironmentHttpApiUrlBuilder } from "./rpc/http.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "./state/environmentHttpAuth.ts";

export interface VoiceReviewClientOptions {
  readonly prepared: PreparedConnection;
  readonly signer: Option.Option<ManagedRelayDpopSigner["Service"]>;
  readonly remoteAuthorization?: Option.Option<RemoteEnvironmentAuthorization["Service"]>;
  readonly timeoutMs?: number;
}
const isReviewError = Schema.is(VoiceReviewError);
const isWrappedFetchError = Schema.is(
  Schema.Struct({
    _tag: Schema.Literal("RemoteEnvironmentAuthFetchError"),
    cause: Schema.Unknown,
  }),
);
const decodeEditSave = Schema.decodeUnknownEffect(VoiceReviewEditSavePayload);
const decodeEditCancel = Schema.decodeUnknownEffect(VoiceReviewEditCancelPayload);
const unwrapReviewError = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.mapError((error) => {
      if (isReviewError(error)) return error;
      if (isWrappedFetchError(error) && isReviewError(error.cause)) return error.cause;
      return error;
    }),
  );
const authorization = (input: VoiceReviewClientOptions) =>
  input.remoteAuthorization === undefined
    ? Effect.serviceOption(RemoteEnvironmentAuthorization)
    : Effect.succeed(input.remoteAuthorization);

export const fetchVoiceReviewDrafts = Effect.fn("clientRuntime.voiceReview.list")(function* (
  input: VoiceReviewClientOptions & {
    readonly scope?: "pending" | "recent";
    readonly limit?: number;
  },
) {
  const query = { scope: input.scope ?? "pending", limit: input.limit ?? 50 };
  return yield* unwrapReviewError(
    executeAuthenticatedEnvironmentHttpRequest({
      ...input,
      remoteAuthorization: yield* authorization(input),
      group: "voiceReview",
      method: "GET",
      timeoutMs: input.timeoutMs ?? 15_000,
      url: (base) => makeEnvironmentHttpApiUrlBuilder(base).voiceReview.list({ query }),
      request: ({ client, headers }) => client.list({ query, headers }),
    }),
  );
});

export const fetchVoiceReviewDraft = Effect.fn("clientRuntime.voiceReview.get")(function* (
  input: VoiceReviewClientOptions & { readonly id: string },
) {
  const params = { id: input.id };
  return yield* unwrapReviewError(
    executeAuthenticatedEnvironmentHttpRequest({
      ...input,
      remoteAuthorization: yield* authorization(input),
      group: "voiceReview",
      method: "GET",
      timeoutMs: input.timeoutMs ?? 15_000,
      url: (base) => makeEnvironmentHttpApiUrlBuilder(base).voiceReview.get({ params }),
      request: ({ client, headers }) => client.get({ params, headers }),
    }),
  );
});

export const mutateVoiceReviewDraft = Effect.fn("clientRuntime.voiceReview.mutate")(function* (
  input: VoiceReviewClientOptions & {
    readonly id: string;
    readonly action: VoiceReviewAction;
    readonly payload: VoiceReviewMutationPayload;
  },
) {
  const params = { id: input.id };
  const endpoint = {
    pause: "pause",
    play: "play",
    "edit-begin": "editBegin",
    "edit-save": "editSave",
    "edit-cancel": "editCancel",
    "send-now": "sendNow",
    delete: "delete",
  } as const;
  return yield* unwrapReviewError(
    executeAuthenticatedEnvironmentHttpRequest({
      ...input,
      remoteAuthorization: yield* authorization(input),
      group: "voiceReview",
      method: "POST",
      timeoutMs: input.timeoutMs ?? 25_000,
      url: (base) =>
        makeEnvironmentHttpApiUrlBuilder(base).voiceReview[endpoint[input.action]]({ params }),
      request: ({ client, headers }) => {
        const args = { params, headers, payload: input.payload };
        switch (input.action) {
          case "edit-save":
            return Effect.flatMap(decodeEditSave(input.payload), (payload) =>
              client.editSave({ ...args, payload }),
            );
          case "edit-cancel":
            return Effect.flatMap(decodeEditCancel(input.payload), (payload) =>
              client.editCancel({ ...args, payload }),
            );
          default:
            return client[endpoint[input.action]](args);
        }
      },
    }),
  );
});
