import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as ConversationPolicy from "../conversationWorkspace/ConversationPolicy.ts";
import { RuntimeUseError, runtimeUseError } from "./RuntimeUseProtocol.ts";

const decodeBinding = Schema.decodeUnknownEffect(
  Schema.fromJsonString(ConversationPolicy.BindingRecord),
);

/** A conversation's sparse binding supplies its physical target. It cannot
 * substitute for an assigned checkout or change an existing admission. Reads
 * may run inside the caller's writer transaction; physical checks stay outside. */
export const readRuntimeUseTarget = Effect.fn("RuntimeUseConversation.readTarget")(
  function* (input: {
    readonly threadId: string;
    readonly projectId: string;
    readonly worktreePath: string | null;
    readonly projectRoot: string;
    readonly cwd: string | null;
  }) {
    const ordinary = input.worktreePath ?? input.projectRoot;
    if (input.cwd === null || input.cwd === ordinary) return { path: ordinary, conversation: null };
    const policy = yield* Effect.serviceOption(ConversationPolicy.ConversationPolicy);
    if (input.worktreePath !== null || Option.isNone(policy) || !policy.value.ownsPath(input.cwd))
      return yield* new RuntimeUseError({ reason: "runtime_use_scope_changed" });
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{ readonly binding: string }>`SELECT binding
    FROM jones_conversation_bindings WHERE thread_id=${input.threadId}`;
    if (rows.length !== 1)
      return yield* new RuntimeUseError({ reason: "runtime_use_conversation_missing" });
    const binding = yield* decodeBinding(rows[0]!.binding, { onExcessProperty: "error" });
    if (
      binding.threadId !== input.threadId ||
      binding.projectId !== input.projectId ||
      binding.path !== input.cwd ||
      binding.state !== "conversation"
    )
      return yield* new RuntimeUseError({ reason: "runtime_use_conversation_changed" });
    return { path: binding.path, conversation: binding };
  },
  Effect.mapError(runtimeUseError),
);

export const revalidateRuntimeUseConversation = Effect.fn(
  "RuntimeUseConversation.revalidatePhysical",
)(function* (binding: ConversationPolicy.Binding | null) {
  if (binding === null) return;
  const policy = yield* Effect.serviceOption(ConversationPolicy.ConversationPolicy);
  if (Option.isNone(policy) || !(yield* policy.value.acceptsCwd(binding.threadId, binding.path)))
    return yield* new RuntimeUseError({ reason: "runtime_use_conversation_unavailable" });
}, Effect.mapError(runtimeUseError));

export const readRuntimeUseSessionTarget = Effect.fn("RuntimeUseConversation.readSessionTarget")(
  function* (input: {
    readonly threadId: string;
    readonly projectId: string;
    readonly worktreePath: string | null;
    readonly projectRoot: string;
  }) {
    const policy = yield* Effect.serviceOption(ConversationPolicy.ConversationPolicy);
    const binding =
      input.worktreePath === null && Option.isSome(policy)
        ? yield* policy.value.lookup(input.threadId)
        : null;
    return yield* readRuntimeUseTarget({
      ...input,
      cwd: binding?.state === "conversation" ? binding.path : null,
    });
  },
);
