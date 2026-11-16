import { assert, it } from "@effect/vitest";
import { EnvironmentId, ThreadId, OrchestratorMcpThreadSendReceiptLookupInput,
  OrchestratorMcpThreadSendReceiptLookupResult } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { Toolkit } from "effect/ai";
import * as Access from "../../McpToolAccess.ts";
import * as Invocation from "../../McpInvocationContext.ts";
import * as Service from "../../OrchestratorMcpService.ts";
import * as Threads from "../../../orchestration-v2/ThreadManagementService.ts";
import { lookupSendReceiptHandler } from "../../../jones/remoteDelegation/SendIngressHandler.ts";
import { OrchestratorToolkit, ThreadSendReceiptLookupTool } from "./tools.ts";
import { handlers } from "./handlers.ts";

it("registers the read-only receipt declaration with shared strict boundary decoders", () => {
  const tool = OrchestratorToolkit.tools.t3_thread_send_receipt_lookup;
  assert.strictEqual(tool, ThreadSendReceiptLookupTool);
  assert.strictEqual(tool.parametersSchema, OrchestratorMcpThreadSendReceiptLookupInput);
  assert.strictEqual(tool.successSchema, OrchestratorMcpThreadSendReceiptLookupResult);
  assert.strictEqual(handlers.t3_thread_send_receipt_lookup, lookupSendReceiptHandler);
  assert.throws(() => Schema.decodeUnknownSync(tool.parametersSchema)({threadId: "child",
    clientRequestId: "key", requestDigest: "digest", originNamespace: "forged"}));
  assert.throws(() => Schema.decodeUnknownSync(tool.successSchema)({version: "jones.remote-followup-receipt/v1",
    environmentId: "peer", threadId: "child", clientRequestId: "key", requestDigest: "digest",
    outcome: "accepted", sent: null}));
});
it("SEND's actual toolkit decoder retains the target birth through JSON encoding", () => {
  const schema = Schema.fromJsonString(OrchestratorToolkit.tools.t3_thread_send.parametersSchema);
  const input = {threadId: ThreadId.make("child"), message: "follow up", clientRequestId: "key",
    expectedThreadCreatedAt: "2026-11-16T00:00:00.000Z"};
  assert.deepEqual(Schema.decodeUnknownSync(schema)(Schema.encodeSync(schema)(input)), input);
});
it.effect("the registered handler forwards the authenticated invocation and exact selectors only", () => {
  const scope: Invocation.McpInvocationScope = {environmentId: EnvironmentId.make("receiver"),
    thread: undefined, client: {sessionId: "session", label: "Peer", linked: true, access: "read-only"},
    requestNamespace: "authenticated-only", issuedAt: 1, capabilities: new Set(["orchestration"])};
  const input = {threadId: ThreadId.make("child"), clientRequestId: "original-key", requestDigest: "a".repeat(64)};
  const toolkit = Toolkit.make(ThreadSendReceiptLookupTool);
  const declarations = Access.toLayer(toolkit, {t3_thread_send_receipt_lookup: handlers.t3_thread_send_receipt_lookup});
  return Effect.gen(function* () {
    const built = yield* toolkit;
    const {result} = yield* built.handle("t3_thread_send_receipt_lookup", input)
      .pipe(Stream.unwrap, Stream.run(Sink.last()), Effect.flatMap(Effect.fromOption));
    assert.isTrue("outcome" in result);
    if ("outcome" in result) assert.equal(result.outcome, "unknown");
  }).pipe(Effect.provide(Access.HandlersLayer.layer(declarations)),
    Effect.provideService(Invocation.McpInvocationContext, scope),
    Effect.provide(Layer.mock(Threads.ThreadManagementService)({})),
    Effect.provide(Layer.mock(Service.OrchestratorMcpService)({lookupSendReceipt: (authenticated, request) => {
      assert.strictEqual(authenticated, scope); assert.deepEqual(request, input);
      return Effect.succeed({version: "jones.remote-followup-receipt/v1" as const, environmentId: scope.environmentId,
        ...request, outcome: "unknown" as const, sent: null});
    }})));
});
