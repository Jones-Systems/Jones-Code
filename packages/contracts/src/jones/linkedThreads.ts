import * as Schema from "effect/Schema";
import { EnvironmentId, ThreadId, ProjectId, RunId } from "../baseSchemas.ts";
import { ModelSelection } from "../modelSelection.ts";
import {
  OrchestrationV2ThreadLaunchWorkspaceStrategy,
  OrchestrationV2RunStatus,
} from "../orchestrationV2.ts";
import { RuntimeMode, ProviderInteractionMode } from "../providerPolicy.ts";
import {
  OrchestratorMcpClientRequestId,
  OrchestratorMcpEnvironmentTarget,
} from "../orchestratorMcp.ts";

const text = (max: number) => Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(max));
export const ThreadEndpointRef = Schema.Struct({
  environmentId: EnvironmentId,
  threadId: ThreadId,
});
export type ThreadEndpointRef = typeof ThreadEndpointRef.Type;
export const LinkedThreadId = Schema.String.check(Schema.isPattern(/^tl1:[0-9a-f]{64}$/));
export type LinkedThreadId = typeof LinkedThreadId.Type;
export const LinkedThreadKind = Schema.Literals(["launch", "continuation", "peer"]);
export type LinkedThreadKind = typeof LinkedThreadKind.Type;
export const LinkedThreadBasis = Schema.Literals(["launch", "handoff", "manual", "recovered"]);
export type LinkedThreadBasis = typeof LinkedThreadBasis.Type;
export const LinkedThreadAssertedBy = Schema.Literals([
  "user",
  "agent",
  "client",
  "linked_environment",
  "system",
]);
export type LinkedThreadAssertedBy = typeof LinkedThreadAssertedBy.Type;
export const LinkedThreadReverseState = Schema.Literals([
  "local",
  "recorded",
  "unavailable",
  "unknown",
]);
export type LinkedThreadReverseState = typeof LinkedThreadReverseState.Type;
export const LinkedThreadAcceptance = Schema.Literals(["offered", "accepted"]);
export type LinkedThreadAcceptance = typeof LinkedThreadAcceptance.Type;
export const LinkedThreadSourceRef = Schema.Struct({
  kind: Schema.Literals(["thread", "pull_request", "commit", "file", "url", "artifact", "other"]),
  ref: text(512),
  label: Schema.optionalKey(text(128)),
});
export type LinkedThreadSourceRef = typeof LinkedThreadSourceRef.Type;
export const LinkedThreadHandoff = Schema.Struct({
  schemaVersion: Schema.Literal("t3.thread-handoff/v1"),
  origin: Schema.Struct({
    ...ThreadEndpointRef.fields,
    environmentLabel: text(256),
    title: text(512),
  }),
  destination: Schema.NullOr(
    Schema.Struct({
      environmentId: EnvironmentId,
      environmentLabel: text(256),
      threadId: Schema.NullOr(ThreadId),
      title: Schema.NullOr(text(512)),
    }),
  ),
  returnRoute: Schema.Struct({
    kind: Schema.Literals(["same_environment", "linked_environment", "manual_relay"]),
    ...ThreadEndpointRef.fields,
  }),
  goal: text(4000),
  scope: text(4000),
  status: text(2000),
  sourceRefs: Schema.Array(LinkedThreadSourceRef).check(Schema.isMaxLength(16)),
  offeredAt: Schema.String,
});
export type LinkedThreadHandoff = typeof LinkedThreadHandoff.Type;
export const LinkedThreadLink = Schema.Struct({
  linkId: LinkedThreadId,
  kind: LinkedThreadKind,
  from: ThreadEndpointRef,
  to: ThreadEndpointRef,
  basis: LinkedThreadBasis,
  assertedBy: LinkedThreadAssertedBy,
  reverse: LinkedThreadReverseState,
  acceptance: Schema.NullOr(LinkedThreadAcceptance),
  handoff: Schema.NullOr(LinkedThreadHandoff),
  remote: Schema.NullOr(
    Schema.Struct({
      environmentLabel: Schema.NullOr(Schema.String),
      title: Schema.NullOr(Schema.String),
    }),
  ),
  createdAt: Schema.String,
  updatedAt: Schema.String,
  acceptedAt: Schema.NullOr(Schema.String),
});
export type LinkedThreadLink = typeof LinkedThreadLink.Type;
export const LinkedThreadsListInput = Schema.Struct({
  thread: ThreadEndpointRef,
  includeSiblings: Schema.optionalKey(Schema.Boolean),
  limit: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 200 }))),
});
export type LinkedThreadsListInput = typeof LinkedThreadsListInput.Type;
export const LinkedThreadsListResult = Schema.Struct({
  environmentId: EnvironmentId,
  links: Schema.Array(LinkedThreadLink),
  siblings: Schema.Array(Schema.Struct({ via: ThreadEndpointRef, link: LinkedThreadLink })),
  truncated: Schema.Boolean,
});
export type LinkedThreadsListResult = typeof LinkedThreadsListResult.Type;
export const ReverseClaim = Schema.Literals(["recorded", "unavailable", "unknown"]);
export const LinkThreadsInput = Schema.Struct({
  kind: Schema.Literals(["continuation", "peer"]),
  from: ThreadEndpointRef,
  to: ThreadEndpointRef,
  reverse: Schema.optionalKey(ReverseClaim),
  remote: Schema.optionalKey(
    Schema.Struct({
      environmentLabel: Schema.optionalKey(text(256)),
      title: Schema.optionalKey(text(512)),
    }),
  ),
});
export type LinkThreadsInput = typeof LinkThreadsInput.Type;
export const UnlinkThreadsInput = Schema.Struct({
  linkId: LinkedThreadId,
  reverse: Schema.optionalKey(ReverseClaim),
});
export type UnlinkThreadsInput = typeof UnlinkThreadsInput.Type;
export const AcceptThreadHandoffInput = Schema.Struct({
  from: ThreadEndpointRef,
  to: ThreadEndpointRef,
  linkId: Schema.optionalKey(LinkedThreadId),
  reverse: Schema.optionalKey(ReverseClaim),
});
export type AcceptThreadHandoffInput = typeof AcceptThreadHandoffInput.Type;
export const LinkedThreadMutationResult = Schema.Struct({
  link: LinkedThreadLink,
  outcome: Schema.Literals(["recorded", "unchanged", "removed", "accepted"]),
});
export type LinkedThreadMutationResult = typeof LinkedThreadMutationResult.Type;
export class LinkedThreadsError extends Schema.TaggedError<LinkedThreadsError>()(
  "LinkedThreadsError",
  {
    reason: Schema.Literals([
      "self_link",
      "cycle",
      "endpoint_not_found",
      "endpoint_not_local",
      "kind_not_allowed",
      "not_receiver",
      "forbidden",
      "not_found",
      "too_large",
    ]),
    message: Schema.String,
  },
) {}
export const LinkedThreadOriginLink = Schema.Struct({
  linkId: LinkedThreadId,
  state: Schema.Literals(["recorded", "not_recorded"]),
  reverse: Schema.optionalKey(LinkedThreadReverseState),
});
export type LinkedThreadOriginLink = typeof LinkedThreadOriginLink.Type;
export const ThreadLinksToolInput = Schema.Struct({
  environmentId: OrchestratorMcpEnvironmentTarget,
  threadId: Schema.optional(ThreadId),
  threadEnvironmentId: Schema.optional(EnvironmentId),
  includeSiblings: Schema.optional(Schema.Boolean),
  limit: LinkedThreadsListInput.fields.limit,
});
const ToolEndpoint = Schema.Struct({
  environmentId: Schema.optional(EnvironmentId),
  threadId: ThreadId,
});
export const ThreadLinkToolInput = Schema.Struct({
  action: Schema.Literals(["link", "unlink", "accept", "recover"]),
  basis: Schema.optionalKey(Schema.Literal("handoff")),
  kind: Schema.optional(Schema.Literals(["continuation", "peer"])),
  from: Schema.optional(ToolEndpoint),
  to: Schema.optional(ToolEndpoint),
  linkId: Schema.optional(LinkedThreadId),
  threadId: Schema.optional(ThreadId),
});
export type ThreadLinkToolInput = typeof ThreadLinkToolInput.Type;
export const LinkedThreadMirror = Schema.Struct({
  state: LinkedThreadReverseState,
  environmentId: Schema.NullOr(EnvironmentId),
  detail: Schema.optionalKey(Schema.String),
});
export const ThreadLinkToolResult = Schema.Struct({
  action: ThreadLinkToolInput.fields.action,
  outcome: Schema.Literals([
    "recorded",
    "unchanged",
    "removed",
    "accepted",
    "already_recorded",
    "previously_removed",
    "no_exact_evidence",
  ]),
  link: Schema.NullOr(LinkedThreadLink),
  mirror: LinkedThreadMirror,
  evidence: Schema.optionalKey(Schema.Literals(["launch_first_message", "created_thread_record"])),
});
export const ThreadHandoffToolInput = Schema.Struct({
  to: Schema.Struct({
    type: Schema.Literals(["existing", "new", "portable"]),
    environmentId: Schema.optional(EnvironmentId),
    threadId: Schema.optional(ThreadId),
    projectId: Schema.optional(ProjectId),
    title: Schema.optional(text(512)),
    modelSelection: Schema.optional(ModelSelection),
    modes: Schema.optional(
      Schema.Struct({
        runtimeMode: Schema.optional(RuntimeMode),
        interactionMode: Schema.optional(ProviderInteractionMode),
      }),
    ),
    workspaceStrategy: Schema.optional(OrchestrationV2ThreadLaunchWorkspaceStrategy),
    scratch: Schema.optional(Schema.Boolean),
  }),
  goal: text(4000),
  scope: text(4000),
  status: text(2000),
  sourceRefs: Schema.optional(Schema.Array(LinkedThreadSourceRef).check(Schema.isMaxLength(16))),
  message: Schema.optional(Schema.String.check(Schema.isMaxLength(20000))),
  clientRequestId: OrchestratorMcpClientRequestId,
});
export type ThreadHandoffToolInput = typeof ThreadHandoffToolInput.Type;
export const ThreadHandoffToolResult = Schema.Struct({
  handoff: LinkedThreadHandoff,
  handoffText: Schema.String,
  destination: Schema.NullOr(ThreadEndpointRef),
  link: Schema.NullOr(LinkedThreadLink),
  mirror: LinkedThreadMirror,
  delivery: Schema.Literals(["accepted", "queued", "rejected", "unknown", "not_sent"]),
  launch: Schema.optionalKey(
    Schema.Struct({
      runId: Schema.NullOr(RunId),
      status: Schema.NullOr(OrchestrationV2RunStatus),
      replayed: Schema.optionalKey(Schema.Boolean),
    }),
  ),
  detail: Schema.optionalKey(Schema.String),
});
export type ThreadHandoffToolResult = typeof ThreadHandoffToolResult.Type;
