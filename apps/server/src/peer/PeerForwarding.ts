import * as SqlClient from "effect/sql/SqlClient";
import {
  makeDependencyStore,
  type Dependency,
} from "../jones/threadResultDependencies/DependencyStore.ts";
import { makeJonesTablePresence } from "../jones/persistence/JonesTablePresence.ts";
import * as DependencyChanges from "../jones/threadResultDependencies/DependencyChanges.ts";
import { OrchestratorMcpCapabilitiesResult, ThreadRequestObservation } from "@t3tools/contracts";
import * as LinkedThreads from "../jones/linkedThreads/LinkedThreads.ts";
import { actorFromCaller } from "../jones/linkedThreads/authorization.ts";
import { ThreadId, LinkedThreadOriginLink } from "@t3tools/contracts";
import * as Option from "effect/Option";
import {
  type EnvironmentId,
  OrchestratorMcpFailure,
  type OrchestratorMcpEnvironmentLinksResult,
  OrchestratorMcpThreadInterruptInput,
  OrchestratorMcpThreadInterruptResult,
  OrchestratorMcpThreadReadInput,
  OrchestratorMcpThreadReadResult,
  OrchestratorMcpThreadWaitInput,
  OrchestratorMcpThreadWaitResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Tool from "effect/ai/Tool";

import { assertNotLinked } from "../mcp/linkOrigin.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { readCaller, unavailable } from "../mcp/threadAccess.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as PeerLinks from "./PeerLinks.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";
import type { LinkedMessageOrigin } from "../jones/peerTransport/LinkedMessageOrigin.ts";
import * as RequestContextSender from "../jones/peerTransport/RequestContextSender.ts";
import * as ProjectService from "../project/ProjectService.ts";

/**
 * A peer's `/mcp` holds a wait open for its whole timeout without sending
 * anything, and T3 Connect's edge drops a request idle for about 100 s, so a
 * forwarded wait goes out in pieces no longer than this.
 */
export const FORWARDED_WAIT_CHUNK_MS = 50_000;
const DEFAULT_WAIT_TIMEOUT_MS = 10 * 60 * 1_000;
const MAX_WAIT_TIMEOUT_MS = 60 * 60 * 1_000;
const encodeDependencyRequest = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

type Scope = McpInvocationContext.McpInvocationScope;

/**
 * Runs T3 MCP tools in a linked environment for an agent here. The tool's own
 * declaration checks the agent here first; the linked environment then holds
 * the call to the link's access, narrowed to the agent's modes, which every
 * forwarded call carries.
 */
export class PeerForwarding extends Context.Service<
  PeerForwarding,
  {
    /**
     * Runs `tool` in the linked environment `environmentId` with `params`. A
     * `t3_thread_wait` goes in pieces short enough to survive any route.
     */
    readonly call: <T extends Tool.Any>(
      scope: Scope,
      tool: T,
      environmentId: EnvironmentId,
      params: Tool.Parameters<T>,
    ) => Effect.Effect<Tool.Success<T>, OrchestratorMcpFailure>;
    /** `t3_thread_read` there. */
    readonly readThread: (
      scope: Scope,
      environmentId: EnvironmentId,
      input: OrchestratorMcpThreadReadInput,
    ) => Effect.Effect<OrchestratorMcpThreadReadResult, OrchestratorMcpFailure>;
    /** `t3_thread_interrupt` there. */
    readonly interruptThread: (
      scope: Scope,
      environmentId: EnvironmentId,
      input: OrchestratorMcpThreadInterruptInput,
    ) => Effect.Effect<OrchestratorMcpThreadInterruptResult, OrchestratorMcpFailure>;
    /** `t3_thread_wait` there, in pieces short enough to survive any route. */
    readonly waitForThread: (
      scope: Scope,
      environmentId: EnvironmentId,
      input: OrchestratorMcpThreadWaitInput,
    ) => Effect.Effect<OrchestratorMcpThreadWaitResult, OrchestratorMcpFailure>;
    /** The environments this one links to, as an agent sees them. */
    readonly links: (
      scope: Scope,
    ) => Effect.Effect<OrchestratorMcpEnvironmentLinksResult, OrchestratorMcpFailure>;
  }
>()("t3/peer/PeerForwarding") {}

/** The linked environment a call names, or nothing when it names this one. */
export const remoteTarget = (scope: Scope, environmentId: EnvironmentId | undefined) =>
  environmentId === scope.environmentId ? undefined : environmentId;

const make = Effect.gen(function* () {
  const linkedThreads = yield* Effect.serviceOption(LinkedThreads.LinkedThreads);
  const peers = yield* PeerMcpClient.PeerMcpClient;
  const links = yield* PeerLinks.PeerLinks;
  const crypto = yield* Crypto.Crypto;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const dependencySql = yield* Effect.serviceOption(SqlClient.SqlClient);
  const dependencyChanges = yield* Effect.serviceOption(DependencyChanges.DependencyChanges);
  const dependencies = Option.isSome(dependencySql)
    ? makeDependencyStore(dependencySql.value, makeJonesTablePresence(dependencySql.value))
    : undefined;
  const publishDependencyChanges = (threadId: ThreadId) =>
    dependencies === undefined || Option.isNone(dependencyChanges)
      ? Effect.void
      : Effect.gen(function* () {
          const sequence = yield* dependencies.sequence;
          yield* dependencyChanges.value.publish({ threadIds: [threadId], sequence });
        }).pipe(Effect.orDie);
  const projects = yield* ProjectService.ProjectService;

  /**
   * Forward the caller's own limits because the peer sees only the link credential; refuse local authority first.
   * Work originating through a link must not use this environment's own links: the peer would see our session
   * instead of the originating link and could not preserve its fence.
   */
  const forwardingCaller = (scope: Scope) =>
    readCaller().pipe(
      Effect.tap((caller) => assertNotLinked(caller, "use this environment's links")),
      Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
      Effect.provideService(ThreadManagement.ThreadManagementService, threads),
    );

  /**
   * Every caller here reaches the peer as the one link session, so a retry
   * key is scoped to its caller before it leaves: two agents here reusing a
   * key never collide there, and one agent retrying gets its first result.
   */
  const scopedRequestId = (namespace: string, clientRequestId: string) =>
    crypto.digest("SHA-256", new TextEncoder().encode(`${namespace}\n${clientRequestId}`)).pipe(
      Effect.map((digest) => `fwd:${Hex.encode(digest)}`),
      Effect.orDie,
    );

  /** Tool and contract schemas are plain data: they encode and decode without services. */
  const forward = Effect.fn("PeerForwarding.forward")(function* <A>(
    scope: Scope,
    toolName: string,
    parameters: Schema.Top,
    success: Schema.Top,
    environmentId: EnvironmentId,
    params: unknown,
    readOnly: boolean,
  ) {
    const waiterGeneration =
      scope.thread &&
      dependencies &&
      (yield* dependencies.available.pipe(Effect.mapError(unavailable)))
        ? yield* dependencies
            .waiterGeneration(scope.thread.threadId)
            .pipe(Effect.mapError(unavailable))
        : 0;
    const caller = yield* forwardingCaller(scope);
    const limits = caller.limits;
    const encoded = (yield* Schema.encodeUnknownEffect(parameters as Schema.Encoder<unknown>)(
      params,
    ).pipe(Effect.mapError(unavailable))) as Record<string, unknown>;
    const { environmentId: _target, ...args } = encoded;
    // A pending upload lives here, so it cannot go with the call.
    if (Array.isArray(args.attachments) && args.attachments.length > 0) {
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "A launch in a linked environment cannot carry attachments yet.",
      });
    }
    if (typeof args.clientRequestId === "string") {
      args.clientRequestId = yield* scopedRequestId(scope.requestNamespace, args.clientRequestId);
    }
    let origin: LinkedMessageOrigin | undefined;
    let tracked: Dependency | undefined;
    if (toolName === "t3_thread_send") {
      const observed = yield* peers.call({
        environmentId,
        tool: "t3_thread_read",
        arguments: { threadId: args.threadId, limit: 1, maxCharsPerItem: 1 },
        limits,
        success: OrchestratorMcpThreadReadResult,
      });
      const policy = observed.remoteMessagePolicy;
      if (
        policy?.schemaVersion !== "t3.linked-message-policy/v1" ||
        policy.authenticatedOrigin !== true ||
        policy.incomingAcceptance !== true
      ) {
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message:
            "This linked environment does not support authenticated incoming messages. Update it before sending; reading and Stop remain available.",
        });
      }
      if (args.resultRequest !== undefined)
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "Result descriptors are generated by the sending server, not callers.",
        });
      if (args.awaitResult === true) {
        if (
          !dependencies ||
          Option.isNone(dependencySql) ||
          Option.isNone(dependencyChanges) ||
          !scope.thread ||
          !caller.caller ||
          !(yield* dependencies.available.pipe(Effect.mapError(unavailable)))
        )
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message:
              "Durable result requests require an originating thread and dependency storage.",
          });
        const capabilities = yield* peers.call({
          environmentId,
          tool: "orchestrator_capabilities",
          arguments: {},
          limits,
          success: OrchestratorMcpCapabilitiesResult,
        });
        if (capabilities.features.threadResultRequestVersion !== "t3.thread-result-request/v1")
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message:
              "The linked receiver does not support correlated result requests. Nothing was sent.",
          });
        const records = yield* threads
          .getThreadRecords(caller.caller.id, ["runs", "subagents"])
          .pipe(Effect.mapError(unavailable));
        if (
          records.subagents.some(
            (task) =>
              task.remoteChild?.environmentId === environmentId &&
              task.remoteChild.threadId === args.threadId,
          )
        )
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "An app-owned delegated child already tracks its result.",
          });
        if (typeof args.clientRequestId !== "string")
          args.clientRequestId = yield* scopedRequestId(
            scope.requestNamespace,
            yield* crypto.randomUUIDv4().pipe(Effect.mapError(unavailable)),
          );
        const dependencyId = `dependency:${args.clientRequestId}`;
        // Register before committing intent, so a concurrent waiter event cannot
        // mistake this live send for a restart gap and fence it. The request scope
        // always clears this marker, including interruption before peers.call.
        yield* Effect.acquireRelease(
          Effect.sync(() => dependencyChanges.value.beginSend(dependencyId)),
          () =>
            Effect.sync(() => dependencyChanges.value.endSend(dependencyId)).pipe(
              Effect.andThen(publishDependencyChanges(caller.caller!.id)),
            ),
        );
        tracked = yield* dependencySql.value
          .withTransaction(
            dependencies.prepare({
              dependencyId,
              operationId: dependencyId,
              waiterThreadId: caller.caller.id,
              waiterRunId:
                records.runs
                  .filter(ThreadManagement.isActiveRun)
                  .toSorted((a, b) => b.ordinal - a.ordinal)[0]?.id ?? null,
              targetEnvironmentId: environmentId,
              targetThreadId: ThreadId.make(String(args.threadId)),
              targetLabel: observed.thread.title,
              scope: {
                environmentId: scope.environmentId,
                requestNamespace: scope.requestNamespace,
                thread: scope.thread,
              },
              forwardedRequestKey: String(args.clientRequestId),
              waiterGeneration,
              requestDigest: Hex.encode(
                yield* crypto
                  .digest("SHA-256", new TextEncoder().encode(encodeDependencyRequest(args)))
                  .pipe(Effect.orDie),
              ),
            }),
          )
          .pipe(
            Effect.mapError((error) =>
              Schema.is(OrchestratorMcpFailure)(error) ? error : unavailable(),
            ),
          );
        if (tracked.state === "released") {
          return yield* new OrchestratorMcpFailure({
            code: "request_conflict",
            message: "This result dependency was released; it cannot be sent again.",
          });
        }
        delete args.awaitResult;
        args.resultRequest = {
          schemaVersion: "t3.thread-result-request/v1",
          operationId: tracked.operationId,
        };
      }
      origin = {
        schemaVersion: "t3.linked-message-origin/v1",
        environmentId: scope.environmentId,
        ...(caller.caller === undefined
          ? {}
          : {
              thread: {
                threadId: caller.caller.id,
                title: (caller.caller.title ?? "").slice(0, 512),
              },
            }),
      };
    }
    if (
      toolName === "t3_thread_launch" ||
      toolName === "t3_thread_link" ||
      toolName === "t3_thread_request_observe"
    )
      origin = {
        schemaVersion: "t3.linked-message-origin/v1",
        environmentId: scope.environmentId,
        ...(caller.caller === undefined
          ? {}
          : {
              thread: {
                threadId: caller.caller.id,
                title: (caller.caller.title ?? "").slice(0, 512),
              },
            }),
      };
    const context = yield* RequestContextSender.contextFor({
      scope,
      caller,
      toolName,
      args,
      readOnly,
    }).pipe(Effect.provideService(ProjectService.ProjectService, projects));
    const result = yield* peers
      .call({
        environmentId,
        tool: toolName,
        arguments: args,
        limits,
        ...(origin === undefined ? {} : { origin }),
        ...(context === undefined ? {} : { context }),
        success: success as Schema.Decoder<A>,
      })
      .pipe(
        Effect.tap(() =>
          !tracked || !dependencies || Option.isNone(dependencySql)
            ? Effect.void
            : Effect.gen(function* () {
                const observed = yield* peers.call({
                  environmentId,
                  tool: "t3_thread_request_observe",
                  arguments: { operationId: tracked!.operationId, timeoutMs: 0 },
                  limits,
                  ...(origin ? { origin } : {}),
                  success: ThreadRequestObservation,
                });
                yield* dependencySql.value
                  .withTransaction(
                    Effect.gen(function* () {
                      yield* dependencies.observe(
                        tracked!.dependencyId,
                        tracked!.generation,
                        observed.observation,
                        observed.request ?? undefined,
                      );
                      if (observed.outcome !== null)
                        yield* dependencies.resolve(
                          tracked!.dependencyId,
                          tracked!.generation,
                          observed.outcome,
                          observed.result ?? "The request ended without an explicit answer.",
                        );
                      tracked = (yield* dependencies.get(tracked!.dependencyId))!;
                    }),
                  )
                  .pipe(Effect.mapError(unavailable));
              }).pipe(Effect.ignore),
        ),
        Effect.catch((error) =>
          Effect.gen(function* () {
            if (!tracked || !dependencies || Option.isNone(dependencySql)) return yield* error;
            const definite = !["orchestration_error", "remote_admission_pending"].includes(
              error.code,
            );
            yield* dependencySql.value
              .withTransaction(
                definite
                  ? dependencies.resolve(
                      tracked.dependencyId,
                      tracked.generation,
                      "not_sent",
                      "The linked environment refused the send before acceptance.",
                      true,
                    )
                  : dependencies.observe(
                      tracked.dependencyId,
                      tracked.generation,
                      error.code === "remote_admission_pending"
                        ? "admission_held"
                        : "delivery_unknown",
                    ),
              )
              .pipe(Effect.mapError(unavailable));
            return yield* new OrchestratorMcpFailure({
              code: error.code,
              message: error.message,
              ...(error.remoteAdmission === undefined
                ? {}
                : { remoteAdmission: error.remoteAdmission }),
              dependency: {
                dependencyId: tracked.dependencyId,
                requestId: tracked.requestId,
                state: definite
                  ? "released"
                  : error.code === "remote_admission_pending"
                    ? "awaiting"
                    : tracked.state,
              },
            });
          }),
        ),
      );
    if (tracked && result !== null && typeof result === "object") {
      return {
        ...result,
        dependency: {
          dependencyId: tracked.dependencyId,
          requestId: tracked.requestId,
          state: tracked.state,
        },
      } as A;
    }
    if (
      toolName === "t3_thread_launch" &&
      caller.caller !== undefined &&
      args.delegatedFrom === undefined &&
      Option.isSome(linkedThreads) &&
      result !== null &&
      typeof result === "object" &&
      "threadId" in result &&
      typeof result.threadId === "string"
    ) {
      const peer = yield* links.get(environmentId).pipe(Effect.orElseSucceed(() => Option.none()));
      const reported =
        "originLink" in result && Schema.is(LinkedThreadOriginLink)(result.originLink)
          ? result.originLink
          : undefined;
      const originLink = yield* linkedThreads.value.recordRemoteLaunch({
        from: { environmentId: scope.environmentId, threadId: caller.caller.id },
        to: { environmentId, threadId: ThreadId.make(result.threadId) },
        actor: actorFromCaller(caller),
        ...(reported === undefined ? {} : { reported }),
        title: typeof args.title === "string" ? args.title : null,
        environmentLabel: Option.isSome(peer) ? peer.value.label : null,
      });
      return { ...result, originLink } as A;
    }
    return result;
  }, Effect.scoped);

  const readThread: PeerForwarding["Service"]["readThread"] = (scope, environmentId, input) =>
    forward<OrchestratorMcpThreadReadResult>(
      scope,
      "t3_thread_read",
      OrchestratorMcpThreadReadInput,
      OrchestratorMcpThreadReadResult,
      environmentId,
      input,
      true,
    );

  const interruptThread: PeerForwarding["Service"]["interruptThread"] = (
    scope,
    environmentId,
    input,
  ) =>
    forward<OrchestratorMcpThreadInterruptResult>(
      scope,
      "t3_thread_interrupt",
      OrchestratorMcpThreadInterruptInput,
      OrchestratorMcpThreadInterruptResult,
      environmentId,
      input,
      false,
    );

  const waitForThread: PeerForwarding["Service"]["waitForThread"] = Effect.fn(
    "PeerForwarding.waitForThread",
  )(function* (scope, environmentId, input) {
    let remaining = Math.min(
      MAX_WAIT_TIMEOUT_MS,
      Math.max(1, input.timeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS),
    );
    let runId = input.runId;
    while (true) {
      const chunk = Math.min(remaining, FORWARDED_WAIT_CHUNK_MS);
      const result = yield* forward<OrchestratorMcpThreadWaitResult>(
        scope,
        "t3_thread_wait",
        OrchestratorMcpThreadWaitInput,
        OrchestratorMcpThreadWaitResult,
        environmentId,
        {
          ...input,
          threadId: input.threadId,
          ...(runId === undefined ? {} : { runId }),
          timeoutMs: chunk,
          ...(input.untilDelegatedWorkSettled === undefined
            ? {}
            : { untilDelegatedWorkSettled: input.untilDelegatedWorkSettled }),
        },
        true,
      );
      remaining -= chunk;
      if (!result.timedOut || remaining <= 0) return result;
      // Later pieces wait for the run the first one picked, not a newer one.
      runId = result.runId ?? undefined;
    }
  });

  const call: PeerForwarding["Service"]["call"] = (scope, tool, environmentId, params) =>
    tool.name === "t3_thread_wait"
      ? (waitForThread(
          scope,
          environmentId,
          params as OrchestratorMcpThreadWaitInput,
        ) as Effect.Effect<Tool.Success<typeof tool>, OrchestratorMcpFailure>)
      : forward(
          scope,
          tool.name,
          tool.parametersSchema,
          tool.successSchema,
          environmentId,
          params,
          Context.get(tool.annotations, Tool.Readonly),
        );

  const listLinks: PeerForwarding["Service"]["links"] = Effect.fn("PeerForwarding.listLinks")(
    function* (scope) {
      yield* forwardingCaller(scope);
      const listed = yield* links.list.pipe(
        Effect.mapError(
          (error) =>
            new OrchestratorMcpFailure({ code: "orchestration_error", message: error.message }),
        ),
      );
      return {
        environmentId: scope.environmentId,
        links: listed.map((link) => ({
          environmentId: link.environmentId,
          label: link.label,
          status: link.status,
          access: link.access,
          expiresAt: DateTime.formatIso(link.expiresAt),
          lastError: link.lastError,
        })),
      };
    },
  );

  return PeerForwarding.of({
    call,
    readThread,
    interruptThread,
    waitForThread,
    links: listLinks,
  });
});

export const layer = Layer.effect(PeerForwarding, make);
