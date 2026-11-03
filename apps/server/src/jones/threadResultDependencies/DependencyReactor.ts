import { makeCommit } from "./Transactions.ts";
import {
  CommandId,
  MessageId,
  EnvironmentId,
  type ThreadId,
  type ThreadRequestObservation,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
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

export class DependencyReactor extends Context.Service<
  DependencyReactor,
  { readonly start: () => Effect.Effect<void, never, import("effect/Scope").Scope> }
>()("t3/jones/threadResultDependencies/DependencyReactor") {}
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const store = makeDependencyStore(sql, makeJonesTablePresence(sql));
  const changes = yield* Changes.DependencyChanges;
  const watcher = yield* RequestWatch.RequestWatch;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const forwarding = yield* Forwarding.PeerForwarding;
  const peerMonitor = yield* PeerMonitor.make;
  const running = new Map<string, { generation: number; fiber?: Fiber.Fiber<void> }>();
  const mutate = makeCommit(sql, store, changes);
  const wake = Effect.fn("DependencyReactor.wake")(function* (dependency: Dependency) {
    const messageId = MessageId.make(yield* crypto.randomUUIDv4());
    const value = notification(dependency);
    yield* orchestrator
      .dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(yield* crypto.randomUUIDv4()),
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
          continue;
        }
        observation = observed.success;
      }
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
  const start = Effect.fn("DependencyReactor.start")(function* () {
    const scope = yield* Effect.scope;
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
        const fiber = yield* follow(dependency).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Result dependency follower deferred until the next change", {
              dependencyId: dependency.dependencyId,
              cause,
            }),
          ),
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
    yield* Stream.fromSubscription(subscription).pipe(
      Stream.runForEach((change) =>
        reconcile(change.threadIds).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Dependency change reconciliation failed", { cause }),
          ),
        ),
      ),
      Effect.catchCause((cause) =>
        Effect.logWarning("Dependency changes subscriber failed", { cause }),
      ),
      Effect.forkScoped,
    );
    yield* orchestrator.streamDomainEvents.pipe(
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
      Effect.catchCause((cause) =>
        Effect.logWarning("Dependency event subscriber failed", { cause }),
      ),
      Effect.forkScoped,
    );
    yield* sql
      .withTransaction(store.prune)
      .pipe(
        Effect.catchCause((cause) => Effect.logWarning("Dependency retention deferred", { cause })),
      );
    yield* reconcile().pipe(
      Effect.catchCause((cause) => Effect.logWarning("Dependency recovery failed", { cause })),
    );
  });
  return DependencyReactor.of({ start });
});
export const layer = Layer.effect(DependencyReactor, make);
