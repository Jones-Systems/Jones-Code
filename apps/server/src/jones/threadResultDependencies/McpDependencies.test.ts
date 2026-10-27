import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as Changes from "./DependencyChanges.ts";
import * as McpDependencies from "./McpDependencies.ts";
import * as RequestWatch from "./RequestWatch.ts";
import { makeCommit } from "./Transactions.ts";
import * as Fixture from "./testkit.ts";

// These cases observe missing or already-answered requests, without provider runs.
const observationPorts = Layer.mock(Orchestrator.OrchestratorV2)({
  getThreadEventSequence: () => Effect.succeed(0),
  streamStoredEventsFrom: () => Stream.never,
});

it.effect("standalone MCP dependencies construct a real request watcher and durable fence", () =>
  Effect.gen(function* () {
    const watcher = yield* RequestWatch.RequestWatch;
    const input = {
      originEnvironmentId: "standalone-origin",
      operationId: "unaccepted-operation",
      timeoutMs: 0,
    };
    const fenced = yield* watcher.observe({ ...input, fence: true });
    assert.equal(fenced.state, "not_sent");
    assert.equal(fenced.outcome, "not_sent");
    const readback = yield* watcher.observe(input);
    assert.equal(readback.outcome, "not_sent");
    assert.equal(readback.token, fenced.token);
  }).pipe(
    Effect.provide(
      McpDependencies.layer.pipe(
        Layer.provide(observationPorts),
        Layer.provide(Sqlite.layerMemory),
      ),
    ),
  ),
);

it.effect("MCP and runtime construction share the event sink's dependency notification bus", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const memoMap = yield* Layer.makeMemoMap;
      const scope = yield* Effect.scope;
      const runtime = yield* Layer.buildWithMemoMap(Fixture.layer, memoMap, scope);
      const fixture = yield* Fixture.seed.pipe(Effect.provide(runtime));
      const mcp = yield* Layer.buildWithMemoMap(
        McpDependencies.layer.pipe(
          Layer.provide(observationPorts),
          Layer.provide(Layer.succeedContext(runtime)),
        ),
        memoMap,
        scope,
      );
      const runtimeChanges = Context.get(runtime, Changes.DependencyChanges);
      const mcpChanges = Context.get(mcp, Changes.DependencyChanges);
      assert.strictEqual(mcpChanges, runtimeChanges);
      const subscription = yield* runtimeChanges.subscribe;
      const request = yield* fixture.send("composition");
      const opened = yield* PubSub.take(subscription);
      assert.include(opened.threadIds, fixture.waiter.id);
      assert.include(opened.threadIds, fixture.receiver.id);
      yield* makeCommit(
        fixture.sql,
        fixture.store,
        mcpChanges,
      )(
        fixture.store.answer(
          fixture.receiver.id,
          request.requestId,
          "fulfilled",
          "Shared composition answer",
          fixture.receiverRun.id,
        ),
      );
      const answered = yield* PubSub.take(subscription);
      assert.include(answered.threadIds, fixture.waiter.id);
      assert.include(answered.threadIds, fixture.receiver.id);
      const observed = yield* Context.get(mcp, RequestWatch.RequestWatch).observe({
        originEnvironmentId: "local",
        operationId: request.operationId,
        timeoutMs: 0,
      });
      assert.equal(observed.outcome, "fulfilled");
      assert.equal(observed.result, "Shared composition answer");
    }),
  ),
);
