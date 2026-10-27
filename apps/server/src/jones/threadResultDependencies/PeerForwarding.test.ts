import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import { assert, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  EnvironmentId,
  OrchestratorMcpFailure,
  type ThreadResultRequest,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Projection from "../../orchestration-v2/ProjectionStore.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Forwarding from "../../peer/PeerForwarding.ts";
import * as Peers from "../../peer/PeerMcpClient.ts";
import * as Links from "../../peer/PeerLinks.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import { OrchestratorToolkit } from "../../mcp/toolkits/orchestrator/tools.ts";
import type { McpThreadInvocationScope } from "../../mcp/McpInvocationContext.ts";
import * as Fixture from "./testkit.ts";

type Seen = { tool: string; arguments: Record<string, unknown> };
const threadPorts = Layer.unwrap(
  Effect.gen(function* () {
    const projections = yield* Projection.ProjectionStoreV2;
    return Layer.mock(Threads.ThreadManagementService)({
      getThreadShell: (id) =>
        projections
          .getThreadShell(id)
          .pipe(
            Effect.mapError(
              (cause) => new Orchestrator.OrchestratorProjectionError({ threadId: id, cause }),
            ),
          ),
      getThreadRecords: (id, fields, filter) =>
        projections
          .getThreadRecords(id, fields, filter)
          .pipe(
            Effect.mapError(
              (cause) => new Orchestrator.OrchestratorProjectionError({ threadId: id, cause }),
            ),
          ),
    });
  }),
);
const withPeer = (seen: Seen[], mode: "old" | "held" | "lost" | "answered") =>
  Forwarding.layer.pipe(
    Layer.provideMerge(threadPorts),
    Layer.provide(Layer.mock(ProjectService.ProjectService)({ getById: () => Effect.succeedNone })),
    Layer.provide(Layer.mock(Links.PeerLinks)({ get: () => Effect.succeedNone })),
    Layer.provide(
      Layer.mock(Peers.PeerMcpClient)({
        call: (input) =>
          Effect.suspend(() => {
            seen.push({ tool: input.tool, arguments: input.arguments });
            if (input.tool === "t3_thread_read")
              return Effect.succeed({
                thread: { title: "Linked receiver" },
                remoteMessagePolicy: {
                  schemaVersion: "t3.linked-message-policy/v1",
                  authenticatedOrigin: true,
                  incomingAcceptance: true,
                },
              } as never);
            if (input.tool === "orchestrator_capabilities")
              return Effect.succeed({
                features:
                  mode === "old"
                    ? {}
                    : { threadResultRequestVersion: "t3.thread-result-request/v1" },
              } as never);
            if (input.tool === "t3_thread_send") {
              if (mode === "held")
                return Effect.fail(
                  new OrchestratorMcpFailure({
                    code: "remote_admission_pending",
                    message: "Owner review required",
                  }),
                );
              if (mode === "lost")
                return Effect.fail(
                  new OrchestratorMcpFailure({
                    code: "orchestration_error",
                    message: "Lost response after possible dispatch",
                  }),
                );
              return Effect.succeed({
                threadId: "receiver",
                messageId: "remote-message",
                runId: "remote-run",
                status: "running",
                delivery: "started",
              } as never);
            }
            if (input.tool === "t3_thread_request_observe") {
              const request = {
                requestId: "remote-request",
                operationId: input.arguments.operationId,
                targetThreadId: "receiver",
                messageId: "remote-message",
                originThreadId: "waiter",
                originEnvironmentId: "local",
                originLabel: "Waiter",
                state: "fulfilled",
                result: "Early explicit answer",
                resolvedByRunId: "remote-run",
                requesterReleased: false,
              } as ThreadResultRequest;
              return Effect.succeed({
                token: "answered",
                state: "answered",
                request,
                admissionId: null,
                observation: "receiver_working",
                outcome: "fulfilled",
                result: request.result,
              } as never);
            }
            return Effect.die("Unexpected fake-peer operation");
          }),
      }),
    ),
    Layer.provide(NodeCrypto.layer),
    Layer.provideMerge(Fixture.layer),
  );
const scopeFor = (f: Effect.Success<typeof Fixture.seed>): McpThreadInvocationScope => ({
  environmentId: EnvironmentId.make("local"),
  issuedAt: 0,
  requestNamespace: "original-namespace",
  capabilities: new Set(["orchestration"]),
  client: undefined,
  thread: {
    threadId: f.waiter.id,
    providerSessionId: "original-session",
    providerInstanceId: f.waiter.providerInstanceId,
  },
});

it.effect("capability denial occurs before any send or durable intent", () => {
  const seen: Seen[] = [];
  return Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const forwarding = yield* Forwarding.PeerForwarding;
    const result = yield* forwarding
      .call(scopeFor(f), OrchestratorToolkit.tools.t3_thread_send, EnvironmentId.make("peer"), {
        threadId: f.receiver.id,
        message: "Need a result",
        awaitResult: true,
        clientRequestId: "old",
      })
      .pipe(Effect.result);
    assert.isTrue(Result.isFailure(result));
    if (Result.isFailure(result)) assert.equal(result.failure.code, "capability_denied");
    assert.isFalse(seen.some((call) => call.tool === "t3_thread_send"));
    assert.deepEqual(yield* f.store.dependencies(f.waiter.id), []);
  }).pipe(Effect.provide(withPeer(seen, "old")));
});

it.effect.each(["held", "lost"] as const)(
  "%s remote response preserves one exact forwarding intent and a visible duty",
  (mode) => {
    const seen: Seen[] = [];
    return Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const forwarding = yield* Forwarding.PeerForwarding;
      const result = yield* forwarding
        .call(scopeFor(f), OrchestratorToolkit.tools.t3_thread_send, EnvironmentId.make("peer"), {
          threadId: f.receiver.id,
          message: "Need a result",
          awaitResult: true,
          clientRequestId: mode,
        })
        .pipe(Effect.result);
      assert.isTrue(Result.isFailure(result));
      if (Result.isFailure(result)) {
        assert.isDefined(result.failure.dependency);
        assert.equal(
          result.failure.message,
          mode === "held" ? "Owner review required" : "Lost response after possible dispatch",
        );
      }
      const dependency = (yield* f.store.dependencies(f.waiter.id))[0]!;
      assert.equal(dependency.observation, mode === "held" ? "admission_held" : "delivery_unknown");
      assert.equal(dependency.state, mode === "held" ? "awaiting" : "sending");
      assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 1);
      const send = seen.find((call) => call.tool === "t3_thread_send")!;
      assert.isUndefined(send.arguments.awaitResult);
      assert.deepEqual(send.arguments.resultRequest, {
        schemaVersion: "t3.thread-result-request/v1",
        operationId: dependency.operationId,
      });
      assert.equal(dependency.forwardedRequestKey, send.arguments.clientRequestId);
      assert.equal(dependency.scope?.requestNamespace, "original-namespace");
    }).pipe(Effect.provide(withPeer(seen, mode)));
  },
);

it.effect(
  "an explicit answer before forwarding returns is bound and resolved without waiting for run completion",
  () => {
    const seen: Seen[] = [];
    return Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const forwarding = yield* Forwarding.PeerForwarding;
      const result = yield* forwarding.call(
        scopeFor(f),
        OrchestratorToolkit.tools.t3_thread_send,
        EnvironmentId.make("peer"),
        {
          threadId: f.receiver.id,
          message: "Need a result",
          awaitResult: true,
          clientRequestId: "early",
        },
      );
      const dependency = (yield* f.store.dependencies(f.waiter.id))[0]!;
      assert.equal(result.status, "running");
      assert.equal(dependency.state, "resolved");
      assert.equal(dependency.requestId, "remote-request");
      assert.equal(dependency.result, "Early explicit answer");
      assert.equal(result.dependency?.dependencyId, dependency.dependencyId);
    }).pipe(Effect.provide(withPeer(seen, "answered")));
  },
);
