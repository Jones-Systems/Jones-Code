import { assert, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { EnvironmentId, RunId, ThreadId, type OrchestratorMcpThreadSendResult } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import { threadSendIdentity } from "../threadResultDependencies/RemoteRequestProtocol.ts";
import { fixture as parentFixture } from "../taskContinuation/testkit.ts";
import * as Ingress from "./SendIngress.ts";

const fixture = Effect.gen(function* () {
  const base = parentFixture();
  const scope: McpInvocationScope = {...base.scope, thread: undefined, requestNamespace: "authenticated-peer-namespace",
    client: {sessionId: "authenticated-link-session", label: "Peer", access: "full-access", linked: true,
      remoteOrigin: {schemaVersion: "t3.linked-message-origin/v1", environmentId: "origin-peer",
        thread: {threadId: "parent"}, linkSessionId: "authenticated-link-session"}}};
  const input: Ingress.SendInput = {threadId: ThreadId.make("receiver-child"), message: "follow up",
    clientRequestId: "fwd:original-stable-key", mode: "queue", expectedThreadCreatedAt: "2026-11-16T00:00:00.000Z"};
  const key = input.clientRequestId!;
  const identity = {...threadSendIdentity(scope.requestNamespace, key), key, link: "original-link"};
  const sent: OrchestratorMcpThreadSendResult = {threadId: input.threadId, messageId: identity.messageId,
    runId: RunId.make("original-accepted-run"), status: "starting", delivery: "queued"};
  const captures: Ingress.SendCapture[] = [];
  const lookups: Array<Ingress.ReceiptLookupInput & {originNamespace: string}> = [];
  let known: OrchestratorMcpThreadSendResult | undefined;
  let operations = 0;
  const owner: Ingress.SendIngressPorts = {remoteSendReceipts: {
    guardPreparation: (_id, effect) => effect,
    withSendCommit: (capture, operation) => Effect.gen(function* () {
      captures.push({...capture});
      // The actual core port validates/freeze-captures and returns the original
      // receipt even if today's operation fails. Its SQL/lock proof stays core's.
      if (known !== undefined) return known;
      operations++;
      return yield* operation;
    }),
    lookup: (request) => Effect.sync(() => {
      lookups.push(request);
      return known === undefined ? {outcome: "unknown" as const, sent: null} : {outcome: "accepted" as const, sent: known};
    }),
  }};
  const service = yield* Ingress.make(owner);
  return {scope, input, identity, sent, captures, lookups, service,
    operation: Effect.succeed(sent), operations: () => operations,
    accept: () => { known = sent; }};
});

it.effect("captures the authenticated namespace, original key/message and canonical final argument digest", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    assert.deepEqual(yield* f.service.commit(f.scope, f.input, f.identity, f.operation), f.sent);
    const original = f.captures[0]!;
    assert.equal(original.originNamespace, f.scope.requestNamespace);
    assert.equal(original.clientRequestId, f.input.clientRequestId);
    assert.equal(original.messageId, f.identity.messageId);
    assert.equal(original.threadId, f.input.threadId);
    assert.equal(original.expectedThreadCreatedAt, f.input.expectedThreadCreatedAt);
    assert.match(original.requestDigest, /^[a-f0-9]{64}$/);
    const reordered = {expectedThreadCreatedAt: f.input.expectedThreadCreatedAt!, mode: f.input.mode!,
      message: f.input.message, threadId: f.input.threadId, clientRequestId: f.input.clientRequestId!};
    yield* f.service.commit(f.scope, {...reordered, environmentId: EnvironmentId.make("routing-only")}, f.identity, f.operation);
    assert.equal(f.captures[1]?.requestDigest, original.requestDigest);
    yield* f.service.commit(f.scope, {...f.input, message: "different"}, f.identity, f.operation);
    assert.notEqual(f.captures[2]?.requestDigest, original.requestDigest);
  }).pipe(Effect.provide(NodeCrypto.layer)));

it.effect("missing core receipt port or unauthenticated origin refuses before the operation", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const absent = yield* Ingress.make({});
    assert.equal((yield* absent.commit(f.scope, f.input, f.identity, f.operation).pipe(Effect.result))._tag, "Failure");
    const stale = {...f.scope, client: {...f.scope.client!, sessionId: "replaced-link-session"}};
    assert.equal((yield* f.service.commit(stale, f.input, f.identity, f.operation).pipe(Effect.result))._tag, "Failure");
    assert.equal(f.operations(), 0);
    assert.equal(f.captures.length, 0);
  }).pipe(Effect.provide(NodeCrypto.layer)));

it.effect("a caller namespace or changed key cannot replace the authenticated command identity", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const forged = {...f.identity, ...threadSendIdentity("caller-supplied-namespace", f.identity.key)};
    assert.equal((yield* f.service.commit(f.scope, f.input, forged, f.operation).pipe(Effect.result))._tag, "Failure");
    assert.equal((yield* f.service.commit(f.scope, {...f.input, clientRequestId: "changed-key"}, f.identity, f.operation)
      .pipe(Effect.result))._tag, "Failure");
    assert.equal(f.operations(), 0);
  }).pipe(Effect.provide(NodeCrypto.layer)));

it.effect("accepted replay returns original exact run/message even when today's target operation fails", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    f.accept();
    const replay = yield* f.service.commit(f.scope, f.input, f.identity, Effect.fail("target deleted or replaced"));
    assert.deepEqual(replay, f.sent);
    assert.equal(f.operations(), 0);
  }).pipe(Effect.provide(NodeCrypto.layer)));

it.effect("ordinary local sends preserve their operation without a linked receipt owner", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const local = yield* Ingress.make({});
    const scope = parentFixture().scope;
    assert.deepEqual(yield* local.commit(scope, f.input, f.identity, f.operation), f.sent);
  }).pipe(Effect.provide(NodeCrypto.layer)));

it.effect("read-only lookup uses only authenticated namespace and absence remains unknown", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const input = {threadId: f.input.threadId, clientRequestId: f.identity.key, requestDigest: "a".repeat(64)};
    const unknown = yield* f.service.lookup(f.scope, input);
    assert.equal(unknown.outcome, "unknown");
    assert.isNull(unknown.sent);
    assert.equal(f.lookups[0]?.originNamespace, f.scope.requestNamespace);
    f.accept();
    const accepted = yield* f.service.lookup(f.scope, input);
    assert.equal(accepted.outcome, "accepted");
    assert.equal(accepted.sent?.messageId, f.sent.messageId);
    assert.equal(accepted.sent?.runId, f.sent.runId);
    assert.equal(f.operations(), 0);
    const stale = {...f.scope, client: {...f.scope.client!, sessionId: "different-session"}};
    assert.equal((yield* f.service.lookup(stale, input).pipe(Effect.result))._tag, "Failure");
    assert.equal(f.lookups.length, 2);
  }).pipe(Effect.provide(NodeCrypto.layer)));
