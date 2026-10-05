import type { LibraryReply, LibraryRequest } from "@t3tools/contracts/conversationLibrary";
import {
  ConversationLibraryError,
  libraryRequestMutates,
} from "@t3tools/shared/conversationLibrary";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as ServerConfig from "../config.ts";
import { openConversationLibrary } from "./open.ts";

export class ConversationLibrary extends Context.Service<
  ConversationLibrary,
  {
    readonly execute: (
      request: LibraryRequest,
      canWrite: boolean,
    ) => Effect.Effect<LibraryReply, ConversationLibraryError>;
  }
>()("t3/conversations/ConversationLibrary") {}

const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  return ConversationLibrary.of({
    execute: (request, canWrite) =>
      Effect.gen(function* () {
        const mutates = libraryRequestMutates(request);
        if (mutates && !canWrite) {
          return yield* Effect.fail(
            new ConversationLibraryError(
              "forbidden",
              "The conversation library operation is not allowed.",
            ),
          );
        }
        const now = yield* Clock.currentTimeMillis;
        return yield* Effect.acquireUseRelease(
          Effect.tryPromise({
            try: () => openConversationLibrary(config.stateDir, mutates, now),
            catch: (cause) =>
              cause instanceof ConversationLibraryError
                ? cause
                : new ConversationLibraryError(
                    "storage",
                    "The conversation library could not be opened.",
                  ),
          }),
          (store) =>
            Effect.try({
              try: () => store.execute(request, canWrite),
              catch: (cause) =>
                cause instanceof ConversationLibraryError
                  ? cause
                  : new ConversationLibraryError(
                      "storage",
                      "The conversation library could not be read or changed.",
                    ),
            }),
          (store) => Effect.sync(() => store.close()),
        );
      }),
  });
});

export const layer = Layer.effect(ConversationLibrary, make);
