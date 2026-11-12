import type { CommandId } from "@t3tools/contracts";
import type { ThreadTitleVerdict } from "@t3tools/provider-core/server/jones/ThreadTitleAssessment";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type { ThreadTitleStateError } from "./store.ts";

// Only the short-lived title worker supplies this; no public command/schema changes.
export const ThreadTitleAssessment = Context.Reference<
  { readonly requestId: CommandId; readonly verdict: ThreadTitleVerdict } | undefined
>("jones/threadTitle/Assessment", { defaultValue: () => undefined });

export interface ThreadTitleCommitContext {
  readonly accept: Effect.Effect<void, ThreadTitleStateError>;
}
