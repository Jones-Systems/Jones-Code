import { assert, it } from "@effect/vitest";
import { CommandId, ProviderSessionId, RunId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { fixture } from "../taskContinuation/testkit.ts";
import * as Caller from "./SelfSettlementCaller.ts";

it.effect("forwards authenticated credential and instance without inventing the distinct native session", () =>
  Effect.gen(function* () {
    const {scope} = fixture();
    const captured = Caller.capture(scope, scope.thread!.threadId)!;
    const commandId = CommandId.make("stable-self-settlement-command");
    const input = {threadId: scope.thread!.threadId, commandId, ...captured, projectRemaining: ["Remaining work"]};
    let received: Caller.Request | undefined;
    const nativeSessionId = ProviderSessionId.make("verified-native-session-not-the-credential");
    const owner: Caller.Ports = {requestSelfSettlement: (request) => Effect.sync(() => {
      received = request;
      return {commandId, mcpCredentialId: request.mcpCredentialId,
        providerInstanceId: request.providerInstanceId, providerSessionId: nativeSessionId,
        runId: RunId.make("captured-owning-run")};
    })};
    const intent = yield* Caller.request(owner, input);
    assert.deepEqual(received, input);
    assert.equal(received?.mcpCredentialId, scope.thread!.providerSessionId);
    assert.notEqual(intent.providerSessionId, received?.mcpCredentialId);
    assert.isFalse("providerSessionId" in received!);
    assert.deepEqual(received?.projectRemaining, ["Remaining work"]);
  }));

it.effect("another thread or outside client cannot supply an own-agent identity", () =>
  Effect.gen(function* () {
    const {scope} = fixture();
    assert.isUndefined(Caller.capture(scope, ThreadId.make("different-thread")));
    assert.isUndefined(Caller.capture({...scope, client: {sessionId: "client", label: "Client", access: "full-access"}}, scope.thread!.threadId));
    assert.isUndefined(Caller.capture({...scope, thread: undefined}, scope.thread!.threadId));
  }));

it.effect("missing durable core port fails instead of acknowledging or scheduling an ephemeral competitor", () =>
  Effect.gen(function* () {
    const {scope} = fixture();
    const result = yield* Caller.request({}, {threadId: scope.thread!.threadId,
      commandId: CommandId.make("stable-key"), ...Caller.capture(scope, scope.thread!.threadId)!}).pipe(Effect.result);
    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") assert.equal(result.failure._tag, "OrchestratorDispatchError");
  }));
