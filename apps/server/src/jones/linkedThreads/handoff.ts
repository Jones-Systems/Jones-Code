import {
  OrchestratorMcpFailure,
  OrchestratorMcpThreadSendInput,
  OrchestratorMcpThreadSendResult,
} from "@t3tools/contracts";
import {
  type ThreadHandoffToolInput,
  type ThreadHandoffToolResult,
  type LinkedThreadHandoff,
  type LinkedThreadLink,
  type ThreadEndpointRef,
} from "@t3tools/contracts/jones/linkedThreads";
import { renderThreadHandoff } from "@t3tools/shared/jones/linkedThreads/handoffText";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { Tool } from "effect/ai";
import * as Environment from "../../environment/ServerEnvironment.ts";
import type { ThreadStartGate } from "../../mcp/McpToolAccess.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import { assertSameLink } from "../../mcp/linkOrigin.ts";
import { handoffRequestKey } from "./identity.ts";
import { assertTargetWithinLimits, readCaller } from "../../mcp/threadAccess.ts";
import * as Launch from "./launchTransport.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import { ProjectToolkit } from "../../mcp/toolkits/project/tools.ts";
import * as Orchestrator from "../../mcp/OrchestratorMcpService.ts";
import * as PeerLinks from "../../peer/PeerLinks.ts";
import * as PeerForwarding from "../../peer/PeerForwarding.ts";
import * as Links from "./LinkedThreads.ts";
import { actorFromCaller } from "./authorization.ts";
import { mirrorLink, mirrorFailureState } from "./mirror.ts";

const SendTool = Tool.make("t3_thread_send", {
  parameters: OrchestratorMcpThreadSendInput,
  success: OrchestratorMcpThreadSendResult,
  failure: OrchestratorMcpFailure,
});
export class ThreadHandoff extends Context.Service<
  ThreadHandoff,
  {
    readonly execute: (
      input: ThreadHandoffToolInput,
      gate: ThreadStartGate,
    ) => Effect.Effect<
      ThreadHandoffToolResult,
      OrchestratorMcpFailure,
      Invocation.McpInvocationContext | Orchestrator.OrchestratorMcpService
    >;
  }
>()("t3/jones/linkedThreads/handoff/ThreadHandoff") {}
const make = Effect.gen(function* () {
  const links = yield* Links.LinkedThreads;
  const environment = yield* Environment.ServerEnvironment;
  const registry = yield* PeerLinks.PeerLinks;
  const forwarding = yield* PeerForwarding.PeerForwarding;
  const launches = yield* Launch.HandoffLaunch;
  const threads = yield* Threads.ThreadManagementService;
  const execute = Effect.fn("ThreadHandoff.execute")(function* (
    input: ThreadHandoffToolInput,
    gate: ThreadStartGate,
  ): Effect.fn.Return<
    ThreadHandoffToolResult,
    OrchestratorMcpFailure,
    Invocation.McpInvocationContext | Orchestrator.OrchestratorMcpService
  > {
    const scope = yield* Invocation.McpInvocationContext;
    const orchestrator = yield* Orchestrator.OrchestratorMcpService;
    const caller = yield* readCaller().pipe(
      Effect.provideService(Threads.ThreadManagementService, threads),
    );
    if (caller.caller === undefined || scope.thread === undefined)
      return yield* new OrchestratorMcpFailure({
        code: "target_required",
        message: "Thread handoff requires a calling thread.",
      });
    const actor = actorFromCaller(caller);
    const here = scope.environmentId;
    const destinationEnvironmentId = input.to.environmentId ?? here;
    const remote = destinationEnvironmentId !== here;
    const descriptor = yield* environment.getDescriptor;
    const peer = remote
      ? yield* registry
          .get(destinationEnvironmentId)
          .pipe(Effect.orElseSucceed(() => Option.none()))
      : Option.none();
    const label = remote
      ? Option.isSome(peer)
        ? peer.value.label
        : "unknown environment"
      : descriptor.label;
    if (input.to.type === "existing" && input.to.threadId === undefined)
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "An existing destination requires threadId.",
      });
    let destination: ThreadEndpointRef | null =
      input.to.type === "existing" && input.to.threadId !== undefined
        ? { environmentId: destinationEnvironmentId, threadId: input.to.threadId }
        : null;
    if (destination?.environmentId === here && destination.threadId === caller.caller.id)
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "A thread cannot hand off to itself.",
      });
    let title = input.to.title ?? null;
    let destinationModes = caller.limits;
    if (destination !== null && !remote) {
      const shell = yield* caller.threads.getThreadShell(destination.threadId).pipe(
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "thread_not_found",
              message: "The receiving thread was not found.",
            }),
        ),
      );
      if (shell === null || shell.deletedAt !== null)
        return yield* new OrchestratorMcpFailure({
          code: "thread_not_found",
          message: "The receiving thread was not found.",
        });
      yield* assertSameLink(caller, shell);
      yield* assertTargetWithinLimits(caller.limits, shell);
      title = shell.title;
      destinationModes = shell;
    }
    let handoff: LinkedThreadHandoff = {
      schemaVersion: "t3.thread-handoff/v1",
      origin: {
        environmentId: here,
        environmentLabel: descriptor.label.slice(0, 256),
        threadId: caller.caller.id,
        title: (caller.caller.title || "Untitled thread").slice(0, 512),
      },
      destination:
        input.to.type === "portable"
          ? null
          : {
              environmentId: destinationEnvironmentId,
              environmentLabel: label.slice(0, 256),
              threadId: destination?.threadId ?? null,
              title,
            },
      returnRoute: {
        kind: input.to.type === "portable" || remote ? "linked_environment" : "same_environment",
        environmentId: here,
        threadId: caller.caller.id,
      },
      goal: input.goal,
      scope: input.scope,
      status: input.status,
      sourceRefs: input.sourceRefs ?? [],
      offeredAt: DateTime.formatIso(yield* DateTime.now),
    };
    const initialText = renderThreadHandoff({ handoff, linkId: null, message: input.message });
    if (input.to.type === "portable")
      return {
        handoff,
        handoffText: initialText,
        destination: null,
        link: null,
        mirror: { state: "local", environmentId: null },
        delivery: "not_sent",
      };
    let delivery: ThreadHandoffToolResult["delivery"] = "unknown";
    let launch: ThreadHandoffToolResult["launch"];
    let detail: string | undefined;
    if (input.to.type === "new") {
      const request = {
        title: input.to.title ?? `Continuation: ${caller.caller.title}`.slice(0, 512),
        projectId: input.to.projectId,
        modelSelection: input.to.modelSelection,
        runtimeMode: input.to.modes?.runtimeMode,
        interactionMode: input.to.modes?.interactionMode,
        workspaceStrategy: input.to.workspaceStrategy,
        scratch: input.to.scratch,
        message: initialText,
        clientRequestId: handoffRequestKey(input.clientRequestId),
      };
      const result = yield* (
        remote
          ? forwarding.call(
              scope,
              ProjectToolkit.tools.t3_thread_launch,
              destinationEnvironmentId,
              request,
            )
          : launches.launch(request, gate)
      ).pipe(Effect.result);
      if (result._tag === "Failure")
        return {
          handoff,
          handoffText: initialText,
          destination: null,
          link: null,
          mirror: {
            state: remote ? mirrorFailureState(result.failure) : "local",
            environmentId: remote ? destinationEnvironmentId : null,
          },
          delivery: mirrorFailureState(result.failure) === "unavailable" ? "rejected" : "unknown",
          detail:
            "The launch did not return a confirmed destination. Retry only with this same key and request.",
        };
      destination = { environmentId: destinationEnvironmentId, threadId: result.success.threadId };
      const initial =
        "initialMessage" in result.success ? result.success.initialMessage : undefined;
      delivery =
        initial === "rejected"
          ? "rejected"
          : initial === "accepted"
            ? "accepted"
            : result.success.runId !== null
              ? result.success.status === "queued"
                ? "queued"
                : "accepted"
              : "unknown";
      launch = {
        runId: result.success.runId,
        status: result.success.status,
        ...("replayed" in result.success && result.success.replayed !== undefined
          ? { replayed: result.success.replayed }
          : {}),
      };
      handoff = {
        ...handoff,
        destination: {
          environmentId: destinationEnvironmentId,
          environmentLabel: label,
          threadId: destination.threadId,
          title: request.title,
        },
      };
    } else if (destination !== null) {
      const request = {
        threadId: destination.threadId,
        message: initialText,
        clientRequestId: handoffRequestKey(input.clientRequestId, true),
      };
      const result = yield* (
        remote
          ? forwarding.call(scope, SendTool, destinationEnvironmentId, request)
          : gate.continueWithin(destinationModes, orchestrator.sendToThread(scope, request))
      ).pipe(Effect.result);
      if (result._tag === "Failure") {
        delivery = mirrorFailureState(result.failure) === "unavailable" ? "rejected" : "unknown";
        detail = "The receiver did not confirm message acceptance.";
      } else delivery = result.success.delivery === "queued" ? "queued" : "accepted";
      if (delivery === "rejected")
        return {
          handoff,
          handoffText: initialText,
          destination,
          link: null,
          mirror: {
            state: remote ? "unavailable" : "local",
            environmentId: remote ? destinationEnvironmentId : null,
          },
          delivery,
          ...(detail === undefined ? {} : { detail }),
        };
    }
    if (destination === null)
      return yield* new OrchestratorMcpFailure({
        code: "orchestration_error",
        message: "The handoff destination is unknown.",
      });
    const recorded = yield* links
      .link(actor, {
        kind: "continuation",
        from: { environmentId: here, threadId: caller.caller.id },
        to: destination,
        basis: "handoff",
        handoff,
        ...(remote
          ? { remote: { environmentLabel: label, ...(title === null ? {} : { title }) } }
          : {}),
      })
      .pipe(Effect.result);
    if (recorded._tag === "Failure")
      return {
        handoff,
        handoffText: initialText,
        destination,
        link: null,
        mirror: {
          state: remote ? "unknown" : "local",
          environmentId: remote ? destinationEnvironmentId : null,
        },
        delivery,
        ...(launch === undefined ? {} : { launch }),
        detail: `Delivery was ${delivery}; the continuation was not recorded: ${recorded.failure.message}`,
      };
    let link: LinkedThreadLink = recorded.success.link;
    const mirror = yield* mirrorLink(
      scope,
      link,
      { action: "link", kind: "continuation", from: link.from, to: link.to },
      forwarding,
    );
    if (mirror.state !== "local")
      link = (yield* links
        .link(actor, {
          kind: "continuation",
          from: link.from,
          to: link.to,
          basis: "handoff",
          reverse: mirror.state,
          handoff,
        })
        .pipe(
          Effect.mapError(
            (error) =>
              new OrchestratorMcpFailure({ code: "orchestration_error", message: error.message }),
          ),
        )).link;
    return {
      handoff,
      handoffText: renderThreadHandoff({ handoff, linkId: link.linkId, message: input.message }),
      destination,
      link,
      mirror,
      delivery,
      ...(launch === undefined ? {} : { launch }),
      ...(detail === undefined ? {} : { detail }),
    };
  });
  return { execute };
});
export const layer = Layer.effect(ThreadHandoff, make);
