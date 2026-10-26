import { assert, it } from "@effect/vitest";
import {
  NodeId,
  OrchestratorMcpFailure,
  RunId,
  ThreadId,
  type OrchestrationV2Subagent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Forwarding from "../../peer/PeerForwarding.ts";
import * as Links from "../../peer/PeerLinks.ts";
import { LinkedRequestSentAtOverride } from "../peerTransport/LinkedRequestContext.ts";
import * as Lifecycle from "./Lifecycle.ts";
import * as Store from "./IntentStore.ts";
import { commandId, envelope, layer } from "./RemoteDelegation.testkit.ts";

const makeFixture = Effect.gen(function* () {
  const store = yield* Store.make;
  const bound = yield* Deferred.make<void>();
  const taskId = NodeId.make("reserved:task");
  let task: OrchestrationV2Subagent | undefined;
  let launches = 0;
  let remoteExecutions = 0;
  let held = false;
  let denied = false;
  let unreachable = false;
  let observationSupported = true;
  let cancelAfterDispatch = false;
  let withdrawals = 0;
  const withdrew = yield* Deferred.make<void>();
  const keys: Array<string | undefined> = [];
  const notices = new Map<string, string>();
  const namespaces: Array<string> = [];
  const launchSentAts: Array<string | undefined> = [];
  const otherSentAts: Array<string | undefined> = [];
  const result = {
    threadId: ThreadId.make("remote:child"),
    runId: RunId.make("remote:run"),
    modelSelection: envelope.launch.modelSelection,
    projectId: envelope.launch.projectId,
  } as Store.LaunchResult;
  const dependencies = Layer.mergeAll(
    Layer.mock(Threads.ThreadManagementService)({
      dispatch: (command) =>
        Effect.gen(function* () {
          if (command.type === "delegated_task.remote.request") {
            task ??= {
              id: taskId,
              threadId: envelope.request.parentThreadId,
              runId: envelope.request.parentRunId,
              origin: "app_owned",
              driver: "remote",
              result: null,
              status: "running",
              childThreadId: null,
            } as OrchestrationV2Subagent;
            return {
              sequence: 1,
              storedEvents: [{ event: { type: "subagent.updated", payload: task } }],
            } as never;
          }
          if (command.type === "delegated_task.remote.hold") {
            if (task !== undefined && task.result === null)
              task = { ...task, status: "waiting", progress: command.progress };
          } else if (command.type === "message.dispatch") {
            notices.set(command.commandId, command.text);
          } else if (command.type === "delegated_task.remote.bind") {
            if (task === undefined) return yield* Effect.die("No reservation");
            task = {
              ...task,
              remoteChild: {
                ...command.remoteChild,
                ...(task.result === null ? {} : { stoppedThere: false }),
              },
            };
          } else if (command.type === "delegated_task.remote.complete") {
            if (task !== undefined && task.result === null)
              task = { ...task, result: command.result, status: command.status };
          } else return yield* Effect.die("Unexpected command");
          return { sequence: 2, storedEvents: [] } as never;
        }),
      getThreadRecords: () =>
        Effect.succeed({ subagents: task === undefined ? [] : [task] } as never),
    }),
    Layer.mock(Links.PeerLinks)({
      get: () =>
        Effect.succeedSome({
          environmentId: envelope.environmentId,
          linkedAt: DateTime.makeUnsafe(envelope.linkedAt),
        } as never),
    }),
    Layer.mock(Forwarding.PeerForwarding)({
      call: (scope, tool, _environmentId, input) =>
        Effect.gen(function* () {
          const sentAt = yield* LinkedRequestSentAtOverride;
          if (tool.name === "t3_thread_launch") launchSentAts.push(sentAt);
          else otherSentAts.push(sentAt);
          if (tool.name === "orchestrator_capabilities")
            return {
              features: observationSupported
                ? { remoteTaskObservationVersion: "t3.remote-task-observation/v1" }
                : {},
            } as never;
          if (tool.name === "t3_remote_admission_cancel") {
            withdrawals++;
            yield* Deferred.succeed(withdrew, undefined);
            assert.deepEqual<unknown>(input, { id: "held-1", bindingDigest: "a".repeat(64) });
            if (unreachable)
              return yield* new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Synthetic unreachable",
              });
            return {
              id: "held-1",
              bindingDigest: "a".repeat(64),
              status: cancelAfterDispatch ? "dispatched" : "cancelled",
            } as never;
          }
          const saved = yield* store.get(commandId);
          assert.equal(saved?.taskId, taskId);
          assert.equal(saved?.launchStarted, true);
          assert.include(
            task?.result === null ? ["running", "waiting"] : ["cancelled"],
            task?.status,
          );
          launches++;
          keys.push((input as { clientRequestId?: string }).clientRequestId);
          namespaces.push(scope.requestNamespace);
          if (unreachable)
            return yield* new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "Synthetic unreachable",
            });
          if (held || denied)
            return yield* new OrchestratorMcpFailure({
              code: denied ? "remote_admission_denied" : "remote_admission_pending",
              message: "Synthetic admission",
              remoteAdmission: {
                id: "held-1",
                bindingDigest: "a".repeat(64),
                status: denied ? "denied" : "held",
              },
            });
          if (remoteExecutions === 0) remoteExecutions++;
          return result as never;
        }),
    }),
  );
  const create = () =>
    Effect.gen(function* () {
      const lifetime = yield* Scope.make();
      const lifecycle = yield* Lifecycle.make(() =>
        Deferred.succeed(bound, undefined).pipe(Effect.asVoid),
      ).pipe(Effect.provide(dependencies), Effect.provideService(Scope.Scope, lifetime));
      return { lifecycle, close: Scope.close(lifetime, Exit.succeed(undefined)) };
    });
  return {
    create,
    store,
    bound,
    withdrew,
    keys,
    namespaces,
    launchSentAts,
    otherSentAts,
    dispatchRacedCancel: () => {
      cancelAfterDispatch = true;
    },
    getTask: () => task!,
    getCounts: () => ({ launches, remoteExecutions, withdrawals, notices: notices.size }),
    getNotices: () => [...notices.entries()],
    observationSupported: (value: boolean) => {
      observationSupported = value;
    },
    held: (value: boolean) => {
      held = value;
    },
    denied: () => {
      denied = true;
    },
    unreachable: (value: boolean) => {
      unreachable = value;
    },
  };
});

it.effect("CSR-F1 refuses an unsupported peer before any new remote launch", () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    fixture.observationSupported(false);
    const active = yield* fixture.create();
    yield* active.lifecycle.submit(commandId, { task: "Inspect fixture" }, envelope);
    assert.equal(fixture.getCounts().launches, 0);
    assert.equal(fixture.getCounts().remoteExecutions, 0);
    assert.equal(fixture.getTask().status, "failed");
    assert.match(fixture.getTask().result ?? "", /observation.*unsupported/i);
    const intent = yield* fixture.store.get(commandId);
    assert.equal(intent?.launchStarted, false);
    assert.equal(intent?.status, "denied");
    yield* active.close;
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["lost receipt", "held admission"] as const)(
  "CSR-F1 preserves an unknown launch and its %s while observation support is unavailable",
  (state) =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      const sql = yield* SqlClient.SqlClient;
      if (state === "lost receipt")
        yield* sql`CREATE TRIGGER simulated_crash BEFORE UPDATE OF launched_json ON jones_remote_delegation_intents BEGIN SELECT RAISE(ABORT, 'synthetic crash after remote launch'); END`;
      else fixture.held(true);
      const first = yield* fixture.create();
      yield* first.lifecycle.submit(commandId, { task: "Inspect fixture" }, envelope);
      yield* first.close;
      if (state === "lost receipt") yield* sql`DROP TRIGGER simulated_crash`;
      const original = yield* fixture.store.get(commandId);
      assert.equal(original?.launchStarted, true);
      assert.equal(original?.launched, null);
      const launches = fixture.getCounts().launches;
      fixture.observationSupported(false);

      const recovering = yield* fixture.create();
      yield* recovering.lifecycle.submit(commandId, { task: "Inspect fixture" }, envelope);
      assert.equal(fixture.getCounts().launches, launches);
      assert.equal(fixture.getTask().result, null);
      const pending = yield* fixture.store.get(commandId);
      assert.equal(pending?.status, "pending");
      assert.equal(pending?.launchStarted, true);
      assert.equal(pending?.launched, null);
      assert.deepEqual(pending?.admission, original?.admission);
      if (state === "held admission")
        assert.equal(fixture.getTask().progress, "Awaiting owner approval in Destination.");
      const holds = fixture
        .getNotices()
        .filter(([, text]) => /observation/i.test(text) && /reconciliation/i.test(text));
      assert.equal(holds.length, 1);
      yield* recovering.lifecycle.submit(commandId, { task: "Inspect fixture" }, envelope);
      assert.equal(fixture.getCounts().launches, launches);
      assert.deepEqual(
        fixture
          .getNotices()
          .filter(([, text]) => /observation/i.test(text) && /reconciliation/i.test(text)),
        holds,
      );
      yield* recovering.close;

      fixture.observationSupported(true);
      fixture.held(false);
      const resumed = yield* fixture.create();
      yield* resumed.lifecycle.start;
      yield* Deferred.await(fixture.bound);
      assert.equal(fixture.getCounts().remoteExecutions, 1);
      assert.deepEqual(new Set(fixture.keys), new Set([envelope.launch.clientRequestId]));
      assert.deepEqual(new Set(fixture.namespaces), new Set([envelope.scope.requestNamespace]));
      assert.equal(fixture.getTask().id, original?.taskId);
      assert.equal(fixture.getTask().remoteChild?.runId, "remote:run");
      yield* resumed.close;
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "recovers a launch acknowledgement lost before local persistence after restart using the original key",
  () =>
    Effect.gen(function* () {
      const createdAt = "2026-10-26T12:00:00.000Z";
      yield* TestClock.setTime(Date.parse(createdAt));
      const fixture = yield* makeFixture;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TRIGGER simulated_crash BEFORE UPDATE OF launched_json ON jones_remote_delegation_intents BEGIN SELECT RAISE(ABORT, 'synthetic crash after remote launch'); END`;
      const first = yield* fixture.create();
      const submitted = yield* first.lifecycle.submit(
        commandId,
        { task: "Inspect fixture" },
        envelope,
      );
      assert.equal(submitted.taskId, "reserved:task");
      assert.equal((yield* fixture.store.get(commandId))?.launched, null);
      yield* first.close;
      yield* sql`DROP TRIGGER simulated_crash`;
      yield* TestClock.adjust("1 hour");
      const restarted = yield* fixture.create();
      yield* restarted.lifecycle.start;
      yield* Deferred.await(fixture.bound);
      assert.equal(fixture.getCounts().remoteExecutions, 1);
      assert.isAtLeast(fixture.getCounts().launches, 2);
      assert.deepEqual(new Set(fixture.keys), new Set([envelope.launch.clientRequestId]));
      assert.deepEqual(new Set(fixture.namespaces), new Set([envelope.scope.requestNamespace]));
      assert.deepEqual(new Set(fixture.launchSentAts), new Set([createdAt]));
      assert.equal((yield* fixture.store.get(commandId))?.createdAt, createdAt);
      assert.equal(fixture.getTask().remoteChild?.runId, "remote:run");
      yield* restarted.close;
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "held admission keeps the reserved task nonterminal and approval binds that same task after restart",
  () =>
    Effect.gen(function* () {
      const createdAt = "2026-10-26T12:00:00.000Z";
      yield* TestClock.setTime(Date.parse(createdAt));
      const fixture = yield* makeFixture;
      fixture.held(true);
      const first = yield* fixture.create();
      const submitted = yield* first.lifecycle.submit(
        commandId,
        { task: "Inspect fixture" },
        envelope,
      );
      assert.equal(fixture.getTask().id, submitted.taskId);
      assert.equal(fixture.getTask().result, null);
      assert.equal(fixture.getTask().status, "waiting");
      assert.equal(fixture.getTask().progress, "Awaiting owner approval in Destination.");
      assert.equal(fixture.getCounts().notices, 1);
      assert.equal(fixture.getTask().remoteChild, undefined);
      assert.equal(fixture.getCounts().remoteExecutions, 0);
      assert.deepEqual(new Set(fixture.launchSentAts), new Set([createdAt]));
      yield* TestClock.adjust("10 seconds");
      const launchesBeforeRetry = fixture.getCounts().launches;
      yield* first.lifecycle.submit(commandId, { task: "Inspect fixture" }, envelope);
      assert.isAbove(fixture.getCounts().launches, launchesBeforeRetry);
      assert.deepEqual(new Set(fixture.launchSentAts), new Set([createdAt]));
      yield* first.close;
      yield* TestClock.adjust("1 hour");
      fixture.held(false);
      const restarted = yield* fixture.create();
      yield* restarted.lifecycle.start;
      yield* Deferred.await(fixture.bound);
      assert.equal(fixture.getTask().id, submitted.taskId);
      assert.equal(fixture.getTask().result, null);
      assert.equal(fixture.getCounts().remoteExecutions, 1);
      assert.deepEqual(new Set(fixture.launchSentAts), new Set([createdAt]));
      assert.deepEqual(new Set(fixture.otherSentAts), new Set([undefined]));
      assert.equal((yield* fixture.store.get(commandId))?.createdAt, createdAt);
      yield* restarted.close;
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "cancellation while unreachable stays durable and binding after restart still owes a stop",
  () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      fixture.held(true);
      const first = yield* fixture.create();
      yield* first.lifecycle.submit(commandId, { task: "Inspect fixture" }, envelope);
      fixture.unreachable(true);
      yield* first.lifecycle.cancelPending(fixture.getTask(), "Owner cancelled");
      assert.equal(fixture.getTask().status, "cancelled");
      assert.equal((yield* fixture.store.pending).length, 1);
      yield* first.close;
      fixture.held(false);
      fixture.unreachable(false);
      fixture.dispatchRacedCancel();
      const restarted = yield* fixture.create();
      yield* restarted.lifecycle.start;
      yield* Deferred.await(fixture.bound);
      assert.equal(fixture.getTask().status, "cancelled");
      assert.equal(fixture.getTask().remoteChild?.stoppedThere, false);
      yield* restarted.close;
    }).pipe(Effect.provide(layer)),
);

it.effect("denied admission completes distinctly without remote execution", () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    fixture.denied();
    const first = yield* fixture.create();
    yield* first.lifecycle.submit(commandId, { task: "Inspect fixture" }, envelope);
    assert.equal(fixture.getTask().status, "failed");
    assert.equal(fixture.getTask().result, "Remote admission denied.");
    assert.equal(fixture.getCounts().remoteExecutions, 0);
    assert.equal((yield* fixture.store.pending).length, 0);
    yield* first.close;
  }).pipe(Effect.provide(layer)),
);

it.effect("withdraws held admission through the same connection before any provider executes", () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    fixture.held(true);
    const first = yield* fixture.create();
    yield* first.lifecycle.submit(commandId, { task: "Inspect fixture" }, envelope);
    yield* first.lifecycle.cancelPending(fixture.getTask(), "Withdraw");
    yield* first.close;
    const restarted = yield* fixture.create();
    yield* restarted.lifecycle.start;
    yield* Deferred.await(fixture.withdrew);
    assert.equal(fixture.getCounts().remoteExecutions, 0);
    assert.equal(fixture.getTask().status, "cancelled");
    yield* restarted.close;
  }).pipe(Effect.provide(layer)),
);
