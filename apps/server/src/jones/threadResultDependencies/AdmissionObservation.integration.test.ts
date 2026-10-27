import { assert, it } from "@effect/vitest";
import {
  AuthSessionId,
  EnvironmentId,
  OrchestratorMcpFailure,
  type AuthClientSession,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Auth from "../../auth/EnvironmentAuth.ts";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Providers from "../../provider/ProviderInstanceRegistry.ts";
import { IncomingLinkedMessage } from "../peerTransport/LinkedMessageOrigin.ts";
import * as Admissions from "../remoteAdmission/RemoteAdmission.ts";
import * as Intake from "../remoteAdmissionIntegration/RemoteAdmissionIntake.ts";
import { DependencyCommit } from "./DependencyCommit.ts";
import { threadSendIdentity } from "./RemoteRequestProtocol.ts";
import * as Watch from "./RequestWatch.ts";
import * as Fixture from "./testkit.ts";
import { runtime } from "./runtimeTestkit.ts";

const invocation: McpInvocationScope = {
  environmentId: EnvironmentId.make("receiving-environment"),
  capabilities: new Set(["orchestration"]),
  requestNamespace: "linked:original/namespace",
  issuedAt: 1,
  thread: undefined,
  client: {
    sessionId: "linked-session",
    label: "Synthetic origin",
    linked: true,
    access: "full-access",
    remoteOrigin: {
      schemaVersion: "t3.linked-message-origin/v1",
      environmentId: "origin",
      linkSessionId: "linked-session",
      thread: { threadId: "waiter" },
    },
  },
};
const admissions = Intake.layer.pipe(
  Layer.provide(
    Admissions.layerWithEvaluator(() =>
      Effect.succeed({ verdict: "pass", category: "none", reason: "Synthetic admission" }),
    ),
  ),
  Layer.provide(
    Layer.mock(Auth.EnvironmentAuth)({
      listSessions: () =>
        Effect.succeed([{ sessionId: AuthSessionId.make("linked-session") } as AuthClientSession]),
    }),
  ),
  // Policy is installed as synthetic fixture data; this regression never resolves a model or starts a provider.
  Layer.provide(Layer.mock(Providers.ProviderInstanceRegistry)({})),
);

it.effect.each([
  "refused",
  "uncertain",
  "rejected-receipt",
  "accepted-receipt",
  "bound-request",
] as const)(
  "CR-R2: real admission dispatch reconciles a lost %s response using its exact result and send receipt",
  (scenario) =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const intake = yield* Intake.RemoteAdmissionIntake;
      const watch = yield* Watch.RequestWatch;
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const key = "fwd:original/key";
      const operationId = `lost-response:${scenario}`;
      const identity = threadSendIdentity(invocation.requestNamespace, key);
      const params = {
        clientRequestId: key,
        threadId: f.receiver.id,
        message: "Review",
        resultRequest: { schemaVersion: "t3.thread-result-request/v1", operationId },
      };
      const policy = JSON.stringify({
        enabled: true,
        revision: "synthetic-policy",
        modelSelection: null,
      });
      yield* f.sql`INSERT INTO jones_remote_admission_policy (singleton, policy_json) VALUES (1, ${policy})`;
      if (scenario === "refused") {
        yield* f.sink.write({
          events: [
            f.event({
              type: "thread.metadata-updated",
              threadId: f.receiver.id,
              payload: { ...f.receiver, threadMessagesBlocked: true },
            }),
          ],
        });
      }
      // A refusal belonging to a different namespace must never settle this operation.
      const other = threadSendIdentity("unrelated-namespace", key);
      yield* f.sink.commitRejectedCommand({
        commandId: other.commandId,
        threadId: f.receiver.id,
        commandType: "message.dispatch",
        rejectedAt: Fixture.time,
        error: "Unrelated rejection",
      });
      let calls = 0;
      intake.register(
        "t3_thread_send",
        () =>
          Effect.gen(function* () {
            calls++;
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
            if (scenario === "uncertain")
              return yield* new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Uncertain send effect",
              });
            if (scenario === "rejected-receipt") {
              yield* f.sink.commitRejectedCommand({
                commandId: identity.commandId,
                threadId: f.receiver.id,
                commandType: "message.dispatch",
                rejectedAt: Fixture.time,
                error: "Exact send rejected",
              });
              return yield* new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Response lost after rejection",
              });
            }
            const { localDependency: _local, ...baseRequest } = f.requestInput(scenario);
            const request = {
              ...baseRequest,
              ...identity,
              requestId: `request:${identity.messageId}`,
              operationId,
              originKind: "linked" as const,
              originEnvironmentId: "origin",
            };
            const sent = yield* orchestrator
              .dispatch({
                type: "message.dispatch",
                ...identity,
                threadId: f.receiver.id,
                text: params.message,
                attachments: [],
                dispatchMode: { type: "queue_after_active" },
                createdBy: "agent",
                creationSource: "mcp",
              })
              .pipe(
                Effect.provideService(IncomingLinkedMessage, true),
                Effect.provideService(
                  DependencyCommit,
                  scenario === "bound-request" ? request : undefined,
                ),
                Effect.mapError(
                  (error) =>
                    new OrchestratorMcpFailure({
                      code:
                        error._tag === "OrchestratorThreadMessagesBlockedError"
                          ? "thread_not_sendable"
                          : "orchestration_error",
                      message: "Receiving send failed",
                    }),
                ),
              );
            if (scenario === "accepted-receipt")
              return yield* new OrchestratorMcpFailure({
                code: "thread_not_sendable",
                message: "A refusal code after an accepted effect is not no-effect proof",
              });
            return { sequence: sent.sequence };
          }).pipe(
            Effect.mapError((error) =>
              error instanceof OrchestratorMcpFailure
                ? error
                : new Intake.RemoteAdmissionIntakeError({ cause: error }),
            ),
          ),
        () => Effect.succeed({ threadId: f.receiver.id }),
      );
      const dispatch = yield* intake
        .invoke("t3_thread_send", params, invocation)
        .pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      const pending = yield* watch.observe({
        originEnvironmentId: "origin",
        operationId,
        timeoutMs: 0,
      });
      assert.equal(pending.observation, "admission_dispatching");
      assert.equal(pending.outcome, null);
      assert.isNotNull(pending.admissionId);
      const observing = yield* watch
        .observe({
          originEnvironmentId: "origin",
          operationId,
          afterToken: pending.token,
          timeoutMs: Infinity,
        })
        .pipe(Effect.forkScoped);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.await(dispatch); // Simulate loss of the transport response; recovery uses only durable facts.
      yield* Fiber.join(observing);
      const recovered = yield* watch.observe({
        originEnvironmentId: "origin",
        operationId,
        fence: true,
        timeoutMs: 0,
      });
      assert.notEqual(recovered.token, pending.token);
      const admission =
        yield* f.sql`SELECT status FROM jones_remote_admissions WHERE id = ${pending.admissionId}`;
      assert.deepEqual(admission, [{ status: "dispatched" }]);
      assert.lengthOf(
        yield* f.sql`SELECT result_json FROM jones_remote_admission_results WHERE admission_id = ${pending.admissionId}`,
        1,
      );
      if (scenario === "bound-request") {
        assert.equal(recovered.request?.messageId, identity.messageId);
        assert.equal(recovered.outcome, null);
        assert.equal(recovered.state, "pending");
      } else {
        assert.equal(recovered.request, null);
        assert.equal(recovered.state, "answered");
        assert.equal(
          recovered.outcome,
          scenario === "refused" || scenario === "rejected-receipt" ? "refused" : "unknown",
        );
        if (recovered.outcome === "unknown")
          assert.include(recovered.result, "Remote work may continue");
      }
      const receipt = yield* f.sql<{
        status: string;
      }>`SELECT status FROM orchestration_command_receipts WHERE command_id = ${identity.commandId}`;
      assert.equal(receipt.length, ["refused", "uncertain"].includes(scenario) ? 0 : 1);
      if (receipt[0])
        assert.equal(receipt[0].status, scenario === "rejected-receipt" ? "rejected" : "accepted");
      const stranger = yield* watch.observe({
        originEnvironmentId: "other-origin",
        operationId,
        timeoutMs: 0,
      });
      assert.equal(stranger.admissionId, null);
      assert.equal(stranger.request, null);
      yield* intake.invoke("t3_thread_send", params, invocation).pipe(Effect.ignore);
      assert.equal(calls, 1);
    }).pipe(Effect.provide(Layer.merge(admissions, Watch.layer).pipe(Layer.provideMerge(runtime)))),
);
