import { EnvironmentId } from "@t3tools/contracts";
import type { LibraryReply, LibraryRequest } from "@t3tools/contracts/conversationLibrary";
import { fetchEnvironmentConversationLibraryRequest } from "@t3tools/client-runtime/conversations";
import { EnvironmentSupervisor } from "@t3tools/client-runtime/connection";
import * as ManagedRelay from "@t3tools/client-runtime/relay";
import { createEnvironmentCommand } from "@t3tools/client-runtime/state/runtime";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";

import { connectionAtomRuntime } from "../connection/runtime";

class ConversationLibraryConnectionNotReadyError extends Data.TaggedError(
  "ConversationLibraryConnectionNotReadyError",
)<{ readonly message: string }> {}

/**
 * Run the conversation-library HTTP adapter inside the selected environment's
 * supervisor. This keeps requests on the current prepared credential and
 * supports cookie, bearer, and relay DPoP authentication through the shared
 * runtime HTTP client.
 */
export const requestConversationLibrary = createEnvironmentCommand(connectionAtomRuntime, {
  label: "web-conversation-library:request",
  execute: (request: LibraryRequest, _registry, environmentId) =>
    Effect.gen(function* () {
      const supervisor = yield* EnvironmentSupervisor;
      const prepared = yield* SubscriptionRef.get(supervisor.prepared);
      if (Option.isNone(prepared)) {
        return yield* new ConversationLibraryConnectionNotReadyError({
          message: "The environment HTTP connection is not ready.",
        });
      }
      if (prepared.value.environmentId !== EnvironmentId.make(environmentId)) {
        return yield* new ConversationLibraryConnectionNotReadyError({
          message: "The environment connection changed. Refresh before trying again.",
        });
      }
      const signer = yield* Effect.serviceOption(ManagedRelay.ManagedRelay.ManagedRelayDpopSigner);
      return yield* fetchEnvironmentConversationLibraryRequest({
        prepared: prepared.value,
        request,
        signer,
      });
    }),
});

export function conversationLibraryReplyMatchesRequest(
  request: LibraryRequest,
  reply: LibraryReply,
): boolean {
  switch (request.kind) {
    case "hello":
      return reply.kind === "hello";
    case "accounts":
      return reply.kind === "accounts";
    case "createAccount":
      return reply.kind === "account";
    case "preview":
      return reply.kind === "preview";
    case "import":
      return reply.kind === "imported";
    case "list":
      return reply.kind === "list";
    case "detail":
      return reply.kind === "detail";
    case "update":
      return reply.kind === "updated";
    case "selectSnapshot":
      return reply.kind === "updated";
    case "remove":
      return reply.kind === "removed";
  }
}

export function conversationLibraryErrorMessage(cause: unknown): string {
  const error = cause instanceof Error ? cause : null;
  const code = error && "code" in error ? String(error.code) : null;
  if (code === "unsupported") {
    return "This environment server does not support the Conversation Library.";
  }
  if (code === "forbidden") {
    return "This account does not have permission to use the Conversation Library.";
  }
  if (code === "conflict") {
    return "The library changed while this request was running. Refresh to reconcile the current state.";
  }
  return error?.message.trim() || "The Conversation Library request failed.";
}
