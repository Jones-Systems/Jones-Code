// Agent steps in a linked environment: the same three operations, as that environment's T3 tools.
import {
  EnvironmentId,
  type OrchestratorMcpFailure,
  OrchestratorMcpThreadInterruptResult,
  OrchestratorMcpThreadReadResult,
  OrchestratorMcpThreadSendInput,
  OrchestratorMcpThreadSendReceiptLookupResult,
  OrchestratorMcpThreadSendResult,
  OrchestratorMcpThreadWaitResult,
  ThreadId,
} from "@t3tools/contracts";
import {
  fileBasename,
  isWindowsDrivePath,
  normalizeProjectPathForComparison,
} from "@t3tools/shared/path";
import * as Digest from "effect/crypto/Digest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import type { LinkedMessageOrigin } from "../jones/peerTransport/LinkedMessageOrigin.ts";
import { canonicalJson } from "../jones/remoteDelegation/shared.ts";
import { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as PeerMcpClient from "../peer/PeerMcpClient.ts";
import {
  type AgentResult,
  type AgentTurn,
  isProjectPath,
  launchOnContinue,
  linkedStepMarker,
  WorkflowAgentError,
  WorkflowAgentMutationUncertain,
  type WorkflowAgents,
} from "./WorkflowAgents.ts";
import * as WorkflowStore from "./WorkflowStore.ts";

// A run acts for whoever owns this environment, not for an agent with modes of its own, so the
// only ceiling on what it starts there is the access the link was paired with.
const LIMITS = { runtimeMode: "full-access", interactionMode: "default" } as const;

// T3 Connect's edge drops a request that is idle for about 100 s, so a wait goes out in pieces.
const WAIT_PIECE = Duration.seconds(50);

/** The link is gone or the thread is: asking again cannot help. */
const isFinal = (error: OrchestratorMcpFailure) =>
  error.code === "capability_denied" ||
  error.code === "thread_not_found" ||
  error.code === "run_not_found" ||
  error.code === "invalid_request";

// The other machine asleep or a dropped route is worth a few more tries before the run pauses.
const retried = { schedule: Schedule.exponential("2 seconds"), times: 5 };
const decodeSend = Schema.decodeUnknownEffect(OrchestratorMcpThreadSendInput);
const encodeSend = Schema.encodeEffect(OrchestratorMcpThreadSendInput);

type Service = WorkflowAgents["Service"];
type Route = { readonly environmentId: EnvironmentId; readonly generation: string };
const turnMarker = (key: string) => `workflow-agent/turn/v1/${key}`;
const turnGenerationPrefix = (key: string) => `workflow-agent/turn-generation/v1/${key}/`;

export const make = Effect.gen(function* () {
  const peers = yield* PeerMcpClient.PeerMcpClient;
  const links = yield* PeerLinks.PeerLinks;
  const digest = yield* Digest.Digest;
  const sql = yield* SqlClient.SqlClient;
  const store = yield* WorkflowStore.make;

  const read = <S extends Schema.Decoder<unknown>>(
    route: Route,
    tool: "t3_project_list" | "t3_thread_read" | "t3_thread_wait",
    success: S,
    args: Record<string, unknown>,
  ) =>
    peers
      .call({
        environmentId: route.environmentId,
        expectedGeneration: route.generation,
        tool,
        arguments: args,
        limits: LIMITS,
        success,
      })
      .pipe(
        Effect.retry({ ...retried, while: (error) => !isFinal(error) }),
        Effect.mapError(
          (error) =>
            new WorkflowAgentError({
              reason: `Environment ${route.environmentId} refused ${tool}: ${error.message}`,
            }),
        ),
      );

  const write = <S extends Schema.Decoder<unknown>>(
    route: Route,
    tool: "t3_project_create" | "t3_thread_launch" | "t3_thread_send" | "t3_thread_interrupt",
    success: S,
    args: Record<string, unknown>,
    origin?: LinkedMessageOrigin,
  ) =>
    peers.call({
      environmentId: route.environmentId,
      expectedGeneration: route.generation,
      tool,
      arguments: args,
      limits: LIMITS,
      success,
      ...(origin === undefined ? {} : { origin }),
    });

  const capture = Effect.fn("WorkflowAgentLinks.capture")(function* (environment: string) {
    if (!(yield* links.enabled)) {
      return yield* new WorkflowAgentError({
        reason:
          "Linked environments is off on this machine, so an agent step cannot run in another one. Turn it on in Settings → Labs.",
      });
    }
    const environmentId = EnvironmentId.make(environment);
    const current = yield* links
      .currentGeneration(environmentId)
      .pipe(
        Effect.mapError(
          () => new WorkflowAgentError({ reason: `Could not read the link to ${environment}.` }),
        ),
      );
    if (Option.isNone(current)) {
      return yield* new WorkflowAgentError({
        reason: `This environment is not linked to ${environment}.`,
      });
    }
    return { environmentId, generation: current.value };
  });

  const requireOriginalPairing = Effect.fn("WorkflowAgentLinks.requireOriginalPairing")(function* (
    route: Route,
  ) {
    const current = yield* capture(route.environmentId);
    if (current.generation !== route.generation) {
      return yield* new WorkflowAgentError({
        reason: `The link to ${route.environmentId} changed after the workflow turn was accepted.`,
      });
    }
  });

  const hash = Effect.fn("WorkflowAgentLinks.hash")(function* (value: unknown) {
    return Hex.encode(
      yield* digest
        .digest(Digest.sha256, new TextEncoder().encode(canonicalJson(value)))
        .pipe(Effect.orDie),
    );
  });

  const turnKey = Effect.fn("WorkflowAgentLinks.turnKey")(function* (
    environment: string,
    { threadId, turnId }: Pick<AgentTurn, "threadId" | "turnId">,
  ) {
    return yield* hash([environment, threadId, turnId]);
  });

  const turnGenerations = (runId: string, key: string) => sql<{ name: string }>`
    SELECT name FROM workflow_marks
    WHERE run_id = ${runId} AND name GLOB ${turnGenerationPrefix(key) + "*"}
    LIMIT 2
  `;

  const acceptedRoute = Effect.fn("WorkflowAgentLinks.acceptedRoute")(function* (
    runId: string | undefined,
    environment: string,
    key: string,
  ) {
    if (runId === undefined) return undefined;
    const [mark, ambiguous] = yield* turnGenerations(runId, key);
    if (mark === undefined || ambiguous !== undefined) return undefined;
    const generation = mark.name.slice(turnGenerationPrefix(key).length);
    if (generation === "") return undefined;
    return { environmentId: EnvironmentId.make(environment), generation };
  });

  const accepted = Effect.fn("WorkflowAgentLinks.accepted")(function* (
    runId: string,
    route: Route,
    turn: Pick<AgentTurn, "threadId" | "turnId">,
    uncertain: WorkflowAgentMutationUncertain,
  ) {
    const key = yield* turnKey(route.environmentId, turn);
    const binding = `${turnGenerationPrefix(key)}${route.generation}`;
    const proved = yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const arrival = yield* store.mark(runId, turnMarker(key));
          const prior = arrival === 1 ? [] : yield* turnGenerations(runId, key);
          // A legacy marker may name a different accepted write with the same turn ids.
          if (arrival > 1 && prior.length === 0) return false;
          // Keep conflicting evidence so neither pairing can later claim this turn.
          yield* store.mark(runId, binding);
          return prior.every((mark) => mark.name === binding);
        }),
      )
      .pipe(Effect.catchTags({ SqlError: () => Effect.fail(uncertain) }));
    if (!proved) return yield* uncertain;
    return { ...turn, environment: route.environmentId };
  });

  const recordIntent = Effect.fn("WorkflowAgentLinks.recordIntent")(function* <A extends Route>(
    runId: string,
    requestId: string,
    provenance: string,
    request: unknown,
    prepared: Result.Result<A, WorkflowAgentError>,
    uncertain: WorkflowAgentMutationUncertain,
  ) {
    const marker = `workflow-agent/write/v1/${requestId}`;
    const binding = Result.isSuccess(prepared)
      ? yield* hash([prepared.success.environmentId, prepared.success.generation, request])
      : undefined;
    // The per-run mark grants one dispatch. The companion mark lets a replay verify the
    // original generation and request before doing a read-only receipt lookup.
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          // Never infer that a legacy step or turn had no writes just because it lacks a journal.
          if ((yield* store.mark(runId, provenance)) === 1) return yield* uncertain;
          const arrival = yield* store.mark(runId, marker);
          if (Result.isFailure(prepared) || binding === undefined) {
            return yield* arrival === 1 && Result.isFailure(prepared)
              ? prepared.failure
              : uncertain;
          }
          const boundArrival = yield* store.mark(runId, `${marker}/${binding}`);
          // Roll back any mark for a mismatched request. It must never become a matching replay.
          if (boundArrival !== arrival) return yield* uncertain;
          return { ...prepared.success, first: arrival === 1 };
        }),
      )
      .pipe(Effect.catchTags({ SqlError: () => Effect.fail(uncertain) }));
  });

  const listed = Effect.fn("WorkflowAgentLinks.listed")(function* (route: Route, path: string) {
    const target = normalizeProjectPathForComparison(path);
    let cursor: number | null = 0;
    while (cursor !== null) {
      const page: Schema.Schema.Type<typeof ProjectToolkit.tools.t3_project_list.successSchema> =
        yield* read(route, "t3_project_list", ProjectToolkit.tools.t3_project_list.successSchema, {
          cursor,
          limit: 100,
        });
      const found = page.projects.find(
        (project) => normalizeProjectPathForComparison(project.workspaceRoot) === target,
      );
      if (found !== undefined) return found.id;
      cursor = page.nextCursor;
    }
    return undefined;
  });

  const registerProject = Effect.fn("WorkflowAgentLinks.registerProject")(function* (
    route: Route,
    path: string,
    uncertain: WorkflowAgentMutationUncertain,
  ) {
    const title = isWindowsDrivePath(path)
      ? fileBasename(path)
      : (path.split("/").findLast((part) => part.length > 0) ?? path);
    const created = yield* write(
      route,
      "t3_project_create",
      ProjectToolkit.tools.t3_project_create.successSchema,
      { title, workspaceRoot: path },
    ).pipe(Effect.result);
    if (Result.isSuccess(created)) return created.success.id;
    // Two steps can register the same folder. Only an explicit conflict proves this call
    // was refused. An interrupted transport does not grant permission to continue.
    if (
      created.failure.code === "invalid_request" &&
      created.failure.message === "The workspace is already registered to a project."
    ) {
      const existing = yield* listed(route, path).pipe(Effect.mapError(() => uncertain));
      if (existing !== undefined) return existing;
    }
    return yield* uncertain;
  });

  /** The whole text of the run's last assistant message. */
  const reply = Effect.fn("WorkflowAgentLinks.reply")(function* (turn: AgentTurn, route: Route) {
    let last: OrchestratorMcpThreadReadResult["items"][number] | undefined;
    let cursor: { beforePosition?: number; afterPosition?: number } = {};
    while (true) {
      const page = yield* read(route, "t3_thread_read", OrchestratorMcpThreadReadResult, {
        threadId: turn.threadId,
        view: "messages",
        limit: 100,
        ...cursor,
      });
      last =
        page.items.findLast(
          (item) => item.type === "assistant_message" && item.runId === turn.turnId,
        ) ?? last;
      const { earlierPosition, nextPosition, hasMore } = page;
      // Backward pages are chronological too, and hasMore points toward newer items.
      if (cursor.afterPosition === undefined && earlierPosition != null) {
        if (last !== undefined || earlierPosition >= (cursor.beforePosition ?? Infinity)) break;
        cursor = { beforePosition: earlierPosition };
        continue;
      }
      if (
        cursor.beforePosition !== undefined ||
        !hasMore ||
        nextPosition === null ||
        nextPosition <= (cursor.afterPosition ?? -1)
      )
        break;
      cursor = { afterPosition: nextPosition };
    }
    if (last === undefined) return "";
    let text = last.text ?? "";
    let textOffset = last.textTruncated ? (last.nextTextOffset ?? null) : null;
    while (textOffset !== null) {
      const rest = yield* read(route, "t3_thread_read", OrchestratorMcpThreadReadResult, {
        threadId: turn.threadId,
        itemId: last.itemId,
        textOffset,
        maxCharsPerItem: 50_000,
      });
      const part = rest.items[0];
      if (part === undefined) break;
      text += part.text ?? "";
      textOffset = part.textTruncated ? (part.nextTextOffset ?? null) : null;
    }
    return text;
  });

  const start = Effect.fn("WorkflowAgentLinks.start")(function* (
    input: Parameters<Service["start"]>[0],
    environment: string,
    requestId: string,
    here: EnvironmentId,
  ) {
    const route = capture(environment);
    const outputSchema =
      input.outputSchema === undefined ? {} : { outputSchema: input.outputSchema };
    const { runId, step } = input.origin;
    const provenance = linkedStepMarker(step);
    const origin = { schemaVersion: "t3.linked-message-origin/v1", environmentId: here } as const;
    if (input.thread !== undefined) {
      const threadId = input.thread;
      const uncertain = new WorkflowAgentMutationUncertain({
        environment,
        operation: "the send",
        requestId,
        threadId,
      });
      const request = {
        threadId,
        message: input.prompt,
        mode: "queue",
        ...outputSchema,
        clientRequestId: requestId,
      };
      const prepared = yield* Effect.gen(function* () {
        const captured = yield* route;
        if (input.launch !== undefined) return yield* launchOnContinue;
        const args = yield* decodeSend(request).pipe(
          Effect.flatMap(encodeSend),
          Effect.mapError(
            () => new WorkflowAgentError({ reason: "The linked send request is invalid." }),
          ),
        );
        return { ...captured, args };
      }).pipe(Effect.result);
      const intent = yield* recordIntent(
        runId,
        requestId,
        provenance,
        ["t3_thread_send", here, Result.isSuccess(prepared) ? prepared.success.args : null],
        prepared,
        uncertain,
      );
      const { args } = intent;
      const sending = intent.first
        ? yield* write(
            intent,
            "t3_thread_send",
            OrchestratorMcpThreadSendResult,
            args,
            origin,
          ).pipe(Effect.option)
        : Option.none<OrchestratorMcpThreadSendResult>();
      const sent = yield* Option.match(sending, {
        onSome: (sent) =>
          sent.threadId === threadId ? Effect.succeed(sent) : Effect.fail(uncertain),
        onNone: Effect.fn("WorkflowAgentLinks.recoverSendReceipt")(function* () {
          const requestDigest = yield* hash(args);
          const receipt = yield* PeerMcpClient.lookupSendReceipt(peers, {
            environmentId: intent.environmentId,
            expectedGeneration: intent.generation,
            origin,
            limits: LIMITS,
            request: {
              threadId: ThreadId.make(args.threadId),
              clientRequestId: requestId,
              requestDigest,
            },
            success: OrchestratorMcpThreadSendReceiptLookupResult,
          }).pipe(Effect.mapError(() => uncertain));
          if (
            receipt.environmentId !== intent.environmentId ||
            receipt.threadId !== threadId ||
            receipt.clientRequestId !== requestId ||
            receipt.requestDigest !== requestDigest ||
            receipt.outcome !== "accepted"
          )
            return yield* uncertain;
          return receipt.sent;
        }),
      });
      return yield* accepted(
        runId,
        intent,
        { threadId: sent.threadId, turnId: sent.runId },
        uncertain,
      );
    }
    const project = input.project;
    const byPath = project !== undefined && isProjectPath(project);
    const launchRequest = {
      title: input.title,
      ...(input.model === undefined ? {} : { modelSelection: input.model }),
      ...(input.launch?.runtimeMode === undefined ? {} : { runtimeMode: input.launch.runtimeMode }),
      ...(input.launch?.interactionMode === undefined
        ? {}
        : { interactionMode: input.launch.interactionMode }),
      ...(input.launch?.workspace === undefined
        ? {}
        : { workspaceStrategy: input.launch.workspace }),
      message: input.prompt,
      ...outputSchema,
      clientRequestId: requestId,
      // The thread there says which run it is a step of, and where that run lives.
      workflowRun: { ...input.origin, environmentId: here },
    };
    const uncertain = new WorkflowAgentMutationUncertain({
      environment,
      operation: byPath ? `the launch or project registration at ${project}` : "the launch",
      requestId,
    });
    const prepared = yield* Effect.gen(function* () {
      const captured = yield* route;
      if (project === undefined) {
        return yield* new WorkflowAgentError({
          reason: "An agent step needs a `project` to start in, or a `thread` to continue.",
        });
      }
      const projectId = byPath ? yield* listed(captured, project) : project;
      return { ...captured, project, projectId };
    }).pipe(Effect.result);
    const intent = yield* recordIntent(
      runId,
      requestId,
      provenance,
      ["t3_thread_launch", project, launchRequest],
      prepared,
      uncertain,
    );
    if (!intent.first) return yield* uncertain;
    const projectId =
      intent.projectId ?? (yield* registerProject(intent, intent.project, uncertain));
    const launched = yield* write(
      intent,
      "t3_thread_launch",
      ProjectToolkit.tools.t3_thread_launch.successSchema,
      { projectId, ...launchRequest },
      origin,
    ).pipe(Effect.mapError(() => uncertain));
    if (launched.runId === null) {
      return yield* new WorkflowAgentMutationUncertain({
        environment,
        operation: "the launch's first turn",
        requestId,
        threadId: launched.threadId,
      });
    }
    return yield* accepted(
      runId,
      intent,
      { threadId: launched.threadId, turnId: launched.runId },
      uncertain,
    );
  });

  const result = Effect.fn("WorkflowAgentLinks.result")(function* (
    turn: AgentTurn,
    environment: string,
    options?: Parameters<Service["result"]>[1],
  ): Effect.fn.Return<AgentResult, WorkflowAgentError> {
    const key = yield* turnKey(environment, turn);
    const unproved = new WorkflowAgentError({
      reason: `Cannot prove the original pairing for workflow turn ${turn.turnId} in ${environment}.`,
    });
    const route = yield* acceptedRoute(options?.workflowRunId, environment, key).pipe(
      Effect.mapError(() => unproved),
    );
    if (route === undefined) return yield* unproved;
    yield* requireOriginalPairing(route);
    const ended = new Set(["completed", "failed", "interrupted", "cancelled", "rolled_back"]);
    /** The result a later turn submits, or nothing once the thread has been quiet. */
    const owed = Effect.fn("WorkflowAgentLinks.owed")(function* (quiet: Duration.Duration) {
      let left = Duration.toMillis(quiet);
      // The newest turn already seen to end without the result, so quiet is not restarted by it.
      let counted = turn.turnId;
      while (true) {
        const piece = Math.min(left, Duration.toMillis(WAIT_PIECE));
        const waited = yield* read(route, "t3_thread_wait", OrchestratorMcpThreadWaitResult, {
          threadId: turn.threadId,
          runId: turn.turnId,
          untilOutput: true,
          timeoutMs: piece,
        });
        if (waited.output !== undefined) return Option.some(waited.output);
        // An environment that does not know `untilOutput` answers at once: nothing to wait on.
        if (!waited.timedOut) return Option.none<Schema.Json>();
        left -= piece;
        if (left > 0) continue;
        const page = yield* read(route, "t3_thread_read", OrchestratorMcpThreadReadResult, {
          threadId: turn.threadId,
          limit: 1,
          runLimit: 1,
        });
        const latest = page.recentRuns[0];
        if (latest === undefined || latest.runId === counted) return Option.none<Schema.Json>();
        // Quiet is counted from the thread's last turn: one under way, or one not yet counted.
        if (ended.has(latest.status)) counted = latest.runId;
        left = Duration.toMillis(quiet);
      }
    });
    while (true) {
      const waited = yield* read(route, "t3_thread_wait", OrchestratorMcpThreadWaitResult, {
        threadId: turn.threadId,
        runId: turn.turnId,
        timeoutMs: Duration.toMillis(WAIT_PIECE),
      });
      if (waited.timedOut) continue;
      const page = yield* read(route, "t3_thread_read", OrchestratorMcpThreadReadResult, {
        threadId: turn.threadId,
        limit: 1,
        runLimit: 50,
      });
      const status = page.recentRuns.find((run) => run.runId === turn.turnId)?.status;
      if (status === undefined) {
        return yield* new WorkflowAgentError({
          reason: `Thread ${turn.threadId} in ${environment} no longer lists turn ${turn.turnId}.`,
        });
      }
      if (status === "completed") {
        const output =
          waited.output !== undefined || options?.quiet === undefined
            ? Option.fromUndefinedOr(waited.output)
            : yield* owed(options.quiet);
        return {
          ending: "completed",
          detail: "",
          reply: yield* reply(turn, route),
          ...(Option.isNone(output) ? {} : { output: output.value }),
        };
      }
      if (
        status === "failed" ||
        status === "interrupted" ||
        status === "cancelled" ||
        status === "rolled_back"
      ) {
        return { ending: status === "failed" ? "failed" : "stopped", detail: status, reply: "" };
      }
      // The wait answered for a turn that has not ended. Asking again at once would spin.
      yield* Effect.sleep("5 seconds");
    }
  });

  const interrupt = Effect.fn("WorkflowAgentLinks.interrupt")(function* (
    target: Parameters<Service["interrupt"]>[0],
    environment: string,
  ) {
    const { threadId, turnId, workflowRunId } = target;
    if (turnId === undefined || workflowRunId === undefined) {
      return yield* new WorkflowAgentMutationUncertain({
        environment,
        operation: "which workflow turn to interrupt",
        threadId,
      });
    }
    // Timeout, failed-turn cleanup and cancellation are all stopping the same turn.
    const key = yield* turnKey(environment, { threadId, turnId });
    const requestId = `workflow:interrupt:${key}`;
    const uncertain = new WorkflowAgentMutationUncertain({
      environment,
      operation: `interruption of turn ${turnId}`,
      requestId,
      threadId,
    });
    const route = yield* acceptedRoute(workflowRunId, environment, key).pipe(
      Effect.mapError(() => uncertain),
    );
    if (route === undefined) return yield* uncertain;
    yield* requireOriginalPairing(route).pipe(Effect.mapError(() => uncertain));
    const args = {
      threadId,
      runId: turnId,
      clientRequestId: requestId,
      reason: "Its workflow run was cancelled",
    };
    const intent = yield* recordIntent(
      workflowRunId,
      requestId,
      turnMarker(key),
      ["t3_thread_interrupt", args],
      Result.succeed(route),
      uncertain,
    );
    if (!intent.first) return yield* uncertain;
    const stopped = yield* write(
      intent,
      "t3_thread_interrupt",
      OrchestratorMcpThreadInterruptResult,
      args,
    ).pipe(Effect.mapError(() => uncertain));
    if (stopped.threadId !== threadId || (stopped.runId !== null && stopped.runId !== turnId))
      return yield* uncertain;
    return stopped.status === "interrupt_requested" ? ("stopped" as const) : ("ended" as const);
  });

  return { start, result, interrupt };
});
