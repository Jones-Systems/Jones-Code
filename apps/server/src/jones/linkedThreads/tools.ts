import { OrchestratorMcpFailure } from "@t3tools/contracts";
import {
  ThreadLinksToolInput,
  LinkedThreadsListResult,
  ThreadLinkToolInput,
  ThreadLinkToolResult,
  ThreadHandoffToolInput,
  ThreadHandoffToolResult,
  type ThreadEndpointRef,
} from "@t3tools/contracts/jones/linkedThreads";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { Tool } from "effect/ai";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import * as Access from "../../mcp/McpToolAccess.ts";
import { readCaller } from "../../mcp/threadAccess.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Orchestrator from "../../mcp/OrchestratorMcpService.ts";
import * as PeerForwarding from "../../peer/PeerForwarding.ts";
import * as Links from "./LinkedThreads.ts";
import * as Handoff from "./handoff.ts";
import { actorFromCaller } from "./authorization.ts";
import { mirrorLink } from "./mirror.ts";
import { sameEndpoint } from "./identity.ts";

const dependencies = [
  Invocation.McpInvocationContext,
  Threads.ThreadManagementService,
  Orchestrator.OrchestratorMcpService,
  PeerForwarding.PeerForwarding,
];
const shared = { failure: OrchestratorMcpFailure, failureMode: "return" as const, dependencies };
export const ThreadLinksTool = Tool.make("t3_thread_links", {
  ...shared,
  parameters: ThreadLinksToolInput,
  success: LinkedThreadsListResult,
  description:
    "List navigation-only links and sibling launches for a thread. Omit threadId for the caller; environmentId routes the query and threadEnvironmentId scopes its subject. Links grant no permissions, visibility, lineage or task management.",
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.Destructive, false);
export const ThreadLinkTool = Tool.make("t3_thread_link", {
  ...shared,
  parameters: ThreadLinkToolInput,
  success: ThreadLinkToolResult,
  description:
    "Link threads as continuation or peer, unlink by linkId, accept a continuation as its receiving thread, or recover one local thread from exact creation evidence. Link needs kind/from/to; accept needs from and defaults to this receiver. Cross-environment copies report mirror state; unavailable or unknown never proves a reverse grant. Ownership remains offered until the receiver accepts. No permissions or task management change.",
})
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.Destructive, false);
export const ThreadHandoffTool = Tool.make("t3_thread_handoff", {
  ...shared,
  parameters: ThreadHandoffToolInput,
  success: ThreadHandoffToolResult,
  description:
    "Continue work in a new or existing ordinary thread, or render portable handoff text. Thread callers only. Pass goal, scope, status, sources and a required stable clientRequestId. New launches and existing-thread sends use existing access checks. Delivery does not transfer ownership: only the receiver accepts with t3_thread_link action=accept. The text includes exact origin and environment IDs and an honest return route. Retry unchanged input with the same key after an unknown result.",
})
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.Destructive, false);
const unavailable = () =>
  new OrchestratorMcpFailure({
    code: "capability_denied",
    message: "Linked threads are unavailable in this server embedding.",
  });
const service = Effect.serviceOption(Links.LinkedThreads).pipe(
  Effect.flatMap((value) =>
    Option.isSome(value) ? Effect.succeed(value.value) : Effect.fail(unavailable()),
  ),
);
const invalid = (message: string) =>
  new OrchestratorMcpFailure({ code: "invalid_request", message });
const mcpFailure = (error: import("@t3tools/contracts").LinkedThreadsError) =>
  new OrchestratorMcpFailure({
    code:
      error.reason === "forbidden" || error.reason === "not_receiver"
        ? "capability_denied"
        : error.reason === "endpoint_not_found" || error.reason === "not_found"
          ? "thread_not_found"
          : "invalid_request",
    message: error.message,
  });

export const threadLinksHandler = Access.reads((input: typeof ThreadLinksToolInput.Type) =>
  Effect.gen(function* () {
    const caller = yield* readCaller();
    const links = yield* service;
    const threadId = input.threadId ?? caller.scope.thread?.threadId;
    if (threadId === undefined)
      return yield* invalid("Pass threadId when there is no calling thread.");
    return yield* links
      .list(actorFromCaller(caller), {
        thread: {
          environmentId: input.threadEnvironmentId ?? caller.scope.environmentId,
          threadId,
        },
        ...(input.includeSiblings === undefined ? {} : { includeSiblings: input.includeSiblings }),
        ...(input.limit === undefined ? {} : { limit: input.limit }),
      })
      .pipe(Effect.mapError(mcpFailure));
  }),
);
export const threadLinkHandler = Access.writesThreads(
  (input: typeof ThreadLinkToolInput.Type) => [input.threadId],
  (input) =>
    Effect.gen(function* () {
      const caller = yield* readCaller();
      const scope = caller.scope;
      const actor = actorFromCaller(caller);
      const links = yield* service;
      const forwarding = yield* PeerForwarding.PeerForwarding;
      const endpoint = (value: NonNullable<typeof input.from>): ThreadEndpointRef => ({
        environmentId: value.environmentId ?? scope.environmentId,
        threadId: value.threadId,
      });
      if (input.action === "recover") {
        if (input.threadId === undefined) return yield* invalid("Recover requires threadId.");
        const recovered = yield* links
          .recover(actor, { threadId: input.threadId })
          .pipe(Effect.mapError(mcpFailure));
        return {
          action: input.action,
          ...recovered,
          mirror: { state: "local" as const, environmentId: null },
        };
      }
      let result: import("@t3tools/contracts").LinkedThreadMutationResult;
      if (input.action === "link") {
        if (input.kind === undefined || input.from === undefined || input.to === undefined)
          return yield* invalid("Link requires kind, from and to.");
        if (
          input.basis === "handoff" &&
          (input.kind !== "continuation" ||
            actor.type !== "linked" ||
            actor.origin === null ||
            !sameEndpoint(actor.origin, endpoint(input.from)))
        )
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "A mirrored handoff requires its authenticated origin thread.",
          });
        const remoteTitle = caller.scope.client?.remoteOrigin?.thread?.title;
        result = yield* links
          .link(actor, {
            kind: input.kind,
            from: endpoint(input.from),
            to: endpoint(input.to),
            basis: input.basis ?? "manual",
            ...(actor.type === "linked"
              ? {
                  remote: {
                    environmentLabel: actor.label,
                    ...(remoteTitle === undefined || remoteTitle === ""
                      ? {}
                      : { title: remoteTitle }),
                  },
                }
              : {}),
          })
          .pipe(Effect.mapError(mcpFailure));
      } else if (input.action === "unlink") {
        if (input.linkId === undefined) return yield* invalid("Unlink requires linkId.");
        result = yield* links
          .unlink(actor, { linkId: input.linkId })
          .pipe(Effect.mapError(mcpFailure));
      } else {
        const to =
          input.to ??
          (scope.thread === undefined ? undefined : { threadId: scope.thread.threadId });
        if (input.from === undefined || to === undefined)
          return yield* invalid("Accept requires from and a receiving thread.");
        result = yield* links
          .accept(actor, {
            from: endpoint(input.from),
            to: endpoint(to),
            ...(input.linkId === undefined ? {} : { linkId: input.linkId }),
          })
          .pipe(Effect.mapError(mcpFailure));
      }
      const outcome = result.outcome;
      const mirrorInput: typeof ThreadLinkToolInput.Type = {
        action: input.action,
        kind: result.link.kind === "launch" ? undefined : result.link.kind,
        from: result.link.from,
        to: result.link.to,
        linkId: result.link.linkId,
      };
      const mirror = yield* mirrorLink(scope, result.link, mirrorInput, forwarding);
      if (mirror.state !== "local" && actor.type !== "linked") {
        if (input.action === "unlink")
          result = yield* links
            .unlink(actor, { linkId: result.link.linkId, reverse: mirror.state })
            .pipe(Effect.mapError(mcpFailure));
        else if (input.action === "accept")
          result = yield* links
            .accept(actor, {
              from: result.link.from,
              to: result.link.to,
              linkId: result.link.linkId,
              reverse: mirror.state,
            })
            .pipe(Effect.mapError(mcpFailure));
        else if (result.link.kind !== "launch")
          result = yield* links
            .link(actor, {
              kind: result.link.kind,
              from: result.link.from,
              to: result.link.to,
              basis: "manual",
              reverse: mirror.state,
            })
            .pipe(Effect.mapError(mcpFailure));
      }
      return { action: input.action, ...result, outcome, mirror };
    }),
);
export const threadHandoffHandler = Access.startsOrContinuesThreads(
  (input: ThreadHandoffToolInput) => input.to.modes ?? {},
  (input, gate) =>
    Effect.gen(function* () {
      const handoff = yield* Effect.serviceOption(Handoff.ThreadHandoff);
      if (Option.isNone(handoff)) return yield* unavailable();
      return yield* handoff.value.execute(input, gate);
    }),
  "stamped",
);
