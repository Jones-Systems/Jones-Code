import { makeCommit } from "./Transactions.ts";
import {
  CommandId,
  MessageId,
  EnvironmentId,
  type ThreadId,
  type ThreadRequestObservation,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Forwarding from "../../peer/PeerForwarding.ts";
import type * as Invocation from "../../mcp/McpInvocationContext.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import * as PeerMonitor from "../remoteDelegation/PeerMonitor.ts";
import { makeDependencyStore, type Dependency } from "./DependencyStore.ts";
import * as Changes from "./DependencyChanges.ts";
import * as RequestWatch from "./RequestWatch.ts";
import { ThreadRequestObserveTool } from "./tools.ts";
import { DependencyWakeCommit } from "./DependencyCommit.ts";
import { notification } from "./Wake.ts";

// Local pacing for every follower, subscriber and retention retry: 1s, 2s, 4s ... capped at 60s.
// This never replaces PeerMonitor's shared per-peer delay; it only bounds how fast this module
// can loop when a failure carries no delay of its own.
const retryDelayMs = (attempt: number) => Math.min(60_000, 1_000 * 2 ** Math.min(attempt, 6));
const PRUNE_INTERVAL_MS = 60 * 60 * 1_000;

export class DependencyReactor extends Context.Service<
  DependencyReactor,
  {
    readonly start: () => Effect.Effect<
      void,
      Orchestrator.OrchestratorV2Error,
      import("effect/Scope").Scope
    >;
  }
>()("t3/jones/threadResultDependencies/DependencyReactor") {}
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* SecureRandom.SecureRandom;
  const store = makeDependencyStore(sql, makeJonesTablePresence(sql));
  const changes = yield* Changes.DependencyChanges;
  const watcher = yield* RequestWatch.RequestWatch;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const forwarding = yield* Forwarding.PeerForwarding;
  const peerMonitor = yield* PeerMonitor.make;
  const running = new Map<string, { generation: number; fiber?: Fiber.Fiber<void> }>();
  const mutate = makeCommit(sql, store, changes);
  const wake = Effect.fn("DependencyReactor.wake")(function* (dependency: Dependency) {
    const messageId = MessageId.make(yield* crypto.uuidV4());
    const value = notification(dependency);
    yield* orchestrator
      .dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(yield* crypto.uuidV4()),
        messageId,
        threadId: dependency.waiterThreadId,
        text: `${value.summary}\n\n${value.detail}`,
        notification: value,
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        creationSource: "server",
      })
      .pipe(
        Effect.provideService(DependencyWakeCommit, {
          dependencyId: dependency.dependencyId,
          generation: dependency.generation,
          waiterThreadId: dependency.waiterThreadId,
          messageId,
        }),
      );
  });
  const follow = Effect.fn("DependencyReactor.follow")(function* (initial: Dependency) {
    let token: string | undefined;
    let unreachable = 0;
    while (true) {
      const current = yield* store.get(initial.dependencyId);
      if (
        !current ||
        current.generation !== initial.generation ||
        current.state === "released" ||
        current.state === "delivered"
      )
        return;
      if (current.state === "resolved") {
        yield* wake(current);
        return;
      }
      let observation: ThreadRequestObservation;
      if (current.targetEnvironmentId === null) {
        // Local subscribers sleep on facts/events, never on a model or a timer.
        const requests = yield* store.requests(current.targetThreadId);
        const request = requests.find((request) => request.requestId === current.requestId);
        if (!request) return;
        observation = yield* watcher.observe({
          originEnvironmentId: request.originEnvironmentId,
          operationId: current.operationId,
          targetThreadId: current.targetThreadId,
          ...(token === undefined ? {} : { afterToken: token }),
          timeoutMs: Infinity,
        });
      } else {
        if (current.scope === null) return;
        const scope: Invocation.McpThreadInvocationScope = {
          ...current.scope,
          issuedAt: DateTime.toEpochMillis(yield* DateTime.now),
          capabilities: new Set(["orchestration"]),
          client: undefined,
        };
        // Revalidate the surviving caller/mode limits and resolve the current link on every chunk.
        const observed = yield* peerMonitor(
          EnvironmentId.make(current.targetEnvironmentId),
          forwarding.call(
            scope,
            ThreadRequestObserveTool,
            EnvironmentId.make(current.targetEnvironmentId),
            {
              operationId: current.operationId,
              ...(token === undefined ? {} : { afterToken: token }),
              fence: current.state === "sending",
              timeoutMs: 50_000,
            },
          ),
        ).pipe(Effect.result);
        if (observed._tag === "Failure") {
          if (
            observed.failure.code === "capability_denied" ||
            observed.failure.code === "thread_not_found" ||
            observed.failure.code === "invalid_request"
          ) {
            yield* mutate(
              store.resolve(
                current.dependencyId,
                current.generation,
                "unknown",
                "Observation authority ended. Remote work may continue; its outcome is unknown.",
              ),
            );
            continue;
          }
          yield* mutate(store.observe(current.dependencyId, current.generation, "unreachable"));
          // Every other failure code carries no delay of its own; never spin on it.
          yield* Effect.sleep(retryDelayMs(unreachable));
          unreachable += 1;
          continue;
        }
        observation = observed.success;
      }
      unreachable = 0;
      token = observation.token;
      yield* mutate(
        Effect.gen(function* () {
          yield* store.observe(
            current.dependencyId,
            current.generation,
            observation.observation,
            observation.request ?? undefined,
          );
          if (observation.outcome !== null)
            yield* store.resolve(
              current.dependencyId,
              current.generation,
              observation.outcome,
              observation.result ?? "The request ended without an explicit result.",
            );
        }),
      );
    }
  });
  // A failed follower retries itself; follow() re-reads the dependency and exits once it is
  // released, delivered or replaced, so a retry never outlives its work.
  const followWithRetry = Effect.fn("DependencyReactor.followWithRetry")(function* (
    dependency: Dependency,
  ) {
    for (let attempt = 0; ; attempt++) {
      const exit = yield* Effect.exit(follow(dependency));
      if (Exit.isSuccess(exit)) return;
      if (Cause.hasInterrupts(exit.cause)) return yield* Effect.interrupt;
      const delayMs = retryDelayMs(attempt);
      yield* Effect.logWarning("Result dependency follower failed; retrying", {
        dependencyId: dependency.dependencyId,
        delayMs,
        cause: exit.cause,
      });
      yield* Effect.sleep(delayMs);
    }
  });
  const start = Effect.fn("DependencyReactor.start")(function* () {
    const scope = yield* Effect.scope;
    // A completed acquisition fences publication even if stream execution starts later.
    // Failure propagates before any subscriber/follower is admitted; it is not readiness.
    const domainEvents = yield* orchestrator.acquireDomainEvents;
    const subscription = yield* changes.subscribe;
    const reconcile = Effect.fn("DependencyReactor.reconcile")(function* (
      threadIds?: ReadonlyArray<ThreadId>,
    ) {
      const dependencies =
        threadIds === undefined
          ? yield* store.dependencies()
          : (yield* Effect.forEach(threadIds, (id) => store.dependencies(id))).flat();
      for (const dependency of dependencies) {
        const previous = running.get(dependency.dependencyId);
        if (
          dependency.state === "released" ||
          dependency.state === "delivered" ||
          (previous?.generation !== undefined && previous.generation !== dependency.generation)
        ) {
          if (previous) {
            running.delete(dependency.dependencyId);
            if (previous.fiber) yield* Fiber.interrupt(previous.fiber);
          }
        }
        if (
          ["released", "delivered"].includes(dependency.state) ||
          running.has(dependency.dependencyId) ||
          changes.isSending(dependency.dependencyId)
        )
          continue;
        const entry: { generation: number; fiber?: Fiber.Fiber<void> } = {
          generation: dependency.generation,
        };
        running.set(dependency.dependencyId, entry);
        const fiber = yield* followWithRetry(dependency).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              if (running.get(dependency.dependencyId) === entry)
                running.delete(dependency.dependencyId);
            }),
          ),
          Effect.forkIn(scope),
        );
        entry.fiber = fiber;
      }
      for (const threadId of threadIds ?? []) {
        if ((yield* store.duties(threadId)).outgoing !== 0) continue;
        yield* orchestrator.finalizeDelegatedTask(threadId);
      }
    });
    // Run a subscriber for the life of the startup scope. If it ends or fails, back off and
    // resubscribe, then reconcile everything once so changes made while it was down are seen.
    // The first run uses the baseline acquired above, which fences startup.
    const supervise = Effect.fn("DependencyReactor.supervise")(function* (
      name: string,
      first: Effect.Effect<void, Orchestrator.OrchestratorV2Error>,
      resubscribe: Effect.Effect<
        Effect.Effect<void, Orchestrator.OrchestratorV2Error>,
        Orchestrator.OrchestratorV2Error,
        Scope.Scope
      >,
    ) {
      let run = first;
      for (let attempt = 0; ; attempt++) {
        const exit = yield* Effect.exit(run);
        if (Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)) return yield* Effect.interrupt;
        const delayMs = retryDelayMs(attempt);
        yield* Effect.logWarning(`Dependency ${name} subscriber stopped; resubscribing`, {
          delayMs,
          ...(Exit.isFailure(exit) ? { cause: exit.cause } : {}),
        });
        yield* Effect.sleep(delayMs);
        run = Effect.scoped(
          resubscribe.pipe(
            Effect.flatMap((next) =>
              reconcile().pipe(
                Effect.catchCause((cause) =>
                  Effect.logWarning("Dependency recovery failed", { cause }),
                ),
                Effect.andThen(next),
              ),
            ),
          ),
        );
      }
    });
    const consumeChanges = (source: typeof subscription) =>
      Stream.fromSubscription(source).pipe(
        Stream.runForEach((change) =>
          reconcile(change.threadIds).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("Dependency change reconciliation failed", { cause }),
            ),
          ),
        ),
      );
    const consumeEvents = (source: typeof domainEvents) =>
      source.pipe(
        Stream.filter(
          (event) =>
            event.type === "run.updated" ||
            event.type === "thread.unarchived" ||
            event.type === "thread.unsettled",
        ),
        Stream.runForEach((event) =>
          reconcile([event.threadId]).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("Dependency event reconciliation failed", { cause }),
            ),
          ),
        ),
      );
    yield* supervise(
      "changes",
      consumeChanges(subscription),
      changes.subscribe.pipe(Effect.map(consumeChanges)),
    ).pipe(Effect.forkScoped);
    yield* supervise(
      "event",
      consumeEvents(domainEvents),
      orchestrator.acquireDomainEvents.pipe(Effect.map(consumeEvents)),
    ).pipe(Effect.forkScoped);
    yield* sql
      .withTransaction(store.prune)
      .pipe(
        Effect.catchCause((cause) => Effect.logWarning("Dependency retention deferred", { cause })),
      );
    yield* reconcile().pipe(
      Effect.catchCause((cause) => Effect.logWarning("Dependency recovery failed", { cause })),
    );
    // Retention also runs while the server stays up; the startup pass alone never repeats.
    yield* Effect.sleep(PRUNE_INTERVAL_MS).pipe(
      Effect.andThen(
        sql
          .withTransaction(store.prune)
          .pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("Dependency retention deferred", { cause }),
            ),
          ),
      ),
      Effect.forever,
      Effect.forkScoped,
    );
  });
  return DependencyReactor.of({ start });
});
export const layer = Layer.effect(DependencyReactor, make);
