import { assert, it } from "@effect/vitest";
import { CommandId, EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Locks from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as Sessions from "../../mcp/McpSessionRegistry.ts";
import type { McpThreadInvocationScope } from "../../mcp/McpInvocationContext.ts";
import * as Fixture from "../threadResultDependencies/testkit.ts";
import * as Owner from "./FollowUpCommandOwner.ts";

const layer = Fixture.layer.pipe(Layer.provideMerge(Locks.layer));
it.effect("requires the exact parent transaction and rolls local reservation plus dispatch back together", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const port = yield* Owner.make(() => f.send("followup-core").pipe(Effect.as({ storedEvents: [] })),
      () => f.receiver.id);
    const command = { type: "queue.resume" as const, commandId: CommandId.make("followup-core"), threadId: f.receiver.id };
    assert.isTrue(Exit.isFailure(yield* port.dispatch(command).pipe(Effect.exit)));
    assert.isTrue(Exit.isFailure(yield* port.inParentTransaction(f.waiter.id, port.dispatch(command)).pipe(Effect.exit)));
    assert.isUndefined(yield* f.store.get("dependency:followup-core"));
    const before = yield* f.sink.latestSequence();
    const failed = yield* port.inParentTransaction(f.receiver.id, port.dispatch(command).pipe(
      Effect.andThen(Effect.fail("synthetic reservation failure")),
    )).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(failed));
    assert.equal(yield* f.sink.latestSequence(), before);
    assert.isUndefined(yield* f.store.get("dependency:followup-core"));
    yield* port.inParentTransaction(f.receiver.id, port.dispatch(command));
    assert.equal((yield* f.store.get("dependency:followup-core"))?.state, "awaiting");
  }).pipe(Effect.provide(layer)),
);

it.effect("rechecks the original credential and exact current parent run inside the owning transaction", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const scope: McpThreadInvocationScope = { environmentId: EnvironmentId.make("local"), requestNamespace: "captured-credential",
      thread: { threadId: f.receiver.id, providerInstanceId: f.receiver.providerInstanceId,
        providerSessionId: "captured-credential", subagent: false }, client: undefined,
      capabilities: new Set(["orchestration"]), issuedAt: 0 };
    let current: McpThreadInvocationScope | undefined = scope;
    const port = yield* Owner.make(() => Effect.succeed({ storedEvents: [] }), () => f.receiver.id).pipe(
      Effect.provide(Layer.mock(Sessions.McpSessionRegistry)({ lookupByCredentialId: () => Effect.sync(() => current) })),
    );
    yield* port.inParentTransaction(f.receiver.id, port.assertParentCaller(scope, f.receiverRun.id));
    current = { ...scope, thread: { ...scope.thread, providerSessionId: "replacement-session" } };
    assert.isTrue(Exit.isFailure(yield* port.inParentTransaction(f.receiver.id,
      port.assertParentCaller(scope, f.receiverRun.id)).pipe(Effect.exit)));
    current = scope;
    const newer = f.run("newer-parent-run", f.receiver.id, "running", 2);
    yield* f.sink.write({ events: [f.event({ type: "run.created", threadId: f.receiver.id, runId: newer.id, payload: newer })] });
    assert.isTrue(Exit.isFailure(yield* port.inParentTransaction(f.receiver.id,
      port.assertParentCaller(scope, f.receiverRun.id)).pipe(Effect.exit)));
    current = undefined;
    assert.isTrue(Exit.isFailure(yield* port.inParentTransaction(f.receiver.id,
      port.assertParentCaller(scope, newer.id)).pipe(Effect.exit)));
  }).pipe(Effect.provide(layer)),
);

it.effect("rejects recursive transaction entry and missing caller authentication before reservation", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const port = yield* Owner.make(() => Effect.succeed({ storedEvents: [] }), () => f.receiver.id);
    const nested = yield* port.inParentTransaction(f.receiver.id,
      port.inParentTransaction(f.receiver.id, Effect.void),
    ).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(nested));
    const caller = { environmentId: EnvironmentId.make("local"), requestNamespace: "not-an-authenticated-session",
      thread: { threadId: f.receiver.id, providerInstanceId: f.receiver.providerInstanceId,
        providerSessionId: "original-session" } };
    const result = yield* port.inParentTransaction(f.receiver.id,
      port.assertParentCaller(caller, f.receiverRun.id),
    ).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(result));
    assert.equal((yield* f.store.requests(f.receiver.id)).length, 0);
  }).pipe(Effect.provide(layer)),
);
