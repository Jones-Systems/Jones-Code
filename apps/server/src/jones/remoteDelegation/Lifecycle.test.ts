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
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
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
  let withdrawalStatus: "cancelled" | "denied" | "dispatching" | "dispatched" = "cancelled";
  let linkState: "original" | "missing" | "replaced" = "original";
  let launchFailure: OrchestratorMcpFailure | undefined;
  let failuresRemaining = Infinity;
  let probeFailure: OrchestratorMcpFailure | undefined;
  let lostAcknowledgement = false;
  let probes = 0;
  let activeLaunches = 0;
  let maxActiveLaunches = 0;
  const launchEntered = yield* Deferred.make<void>();
  const launchRelease = yield* Deferred.make<void>();
  const parked = yield* Deferred.make<void>();
  let blockLaunch = false;
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
            if (command.commandId.endsWith(":reconciliation-notice"))
              yield* Deferred.succeed(parked, undefined);
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
        Effect.sync(() =>
          linkState === "missing"
            ? Option.none()
            : Option.some({
                environmentId: envelope.environmentId,
                linkedAt: DateTime.makeUnsafe(
                  linkState === "replaced" ? "2026-10-22T00:00:00.000Z" : envelope.linkedAt,
                ),
              } as never),
        ),
    }),
    Layer.mock(Forwarding.PeerForwarding)({
      call: (scope, tool, _environmentId, input) =>
        Effect.gen(function* () {
          const sentAt = yield* LinkedRequestSentAtOverride;
          if (tool.name === "t3_thread_launch") launchSentAts.push(sentAt);
          else otherSentAts.push(sentAt);
          if (tool.name === "orchestrator_capabilities") {
            probes++;
            if (probeFailure !== undefined) return yield* probeFailure;
            return {
              features: observationSupported
                ? { remoteTaskObservationVersion: "t3.remote-task-observation/v1" }
                : {},
            } as never;
          }
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
              // The withdrawal adapter now returns authenticated, binding-verified
              // existing states. It does not claim dispatched work was cancelled.
              status: withdrawalStatus,
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
          activeLaunches++;
          maxActiveLaunches = Math.max(maxActiveLaunches, activeLaunches);
          return yield* Effect.gen(function* () {
            yield* Deferred.succeed(launchEntered, undefined);
            if (blockLaunch) yield* Deferred.await(launchRelease);
            if (launchFailure !== undefined && failuresRemaining-- > 0) return yield* launchFailure;
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
            if (withdrawalStatus === "dispatching")
              return yield* new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Dispatch is claimed but its receipt is not available.",
                remoteAdmission: {
                  id: "held-1",
                  bindingDigest: "a".repeat(64),
                  status: "dispatching",
                },
              });
            if (remoteExecutions === 0) remoteExecutions++;
            if (lostAcknowledgement)
              return yield* new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Synthetic lost acknowledgement",
              });
            return result as never;
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                activeLaunches--;
              }),
            ),
          );
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
    parked,
    launchEntered,
    launchRelease,
    dispatchRacedCancel: (status: "dispatched" | "dispatching" | "denied" = "dispatched") => {
      withdrawalStatus = status;
      if (status === "dispatched") remoteExecutions = 1;
    },
    getTask: () => task!,
    getCounts: () => ({
      launches,
      probes,
      maxActiveLaunches,
      remoteExecutions,
      withdrawals,
      notices: notices.size,
    }),
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
    linkState: (value: typeof linkState) => {
      linkState = value;
    },
    launchFailure: (value: OrchestratorMcpFailure | undefined, count = Infinity) => {
      launchFailure = value;
      failuresRemaining = count;
    },
    probeFailure: (value: OrchestratorMcpFailure | undefined) => {
      probeFailure = value;
    },
    lostAcknowledgement: () => {
      lostAcknowledgement = true;
    },
    blockLaunch: () => {
      blockLaunch = true;
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
      assert.match(fixture.getTask().progress ?? "", /reconciliation.*on hold/i);
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
      const probes = fixture.getCounts().probes;
      yield* TestClock.adjust("1 day");
      assert.equal(fixture.getCounts().probes, probes);
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
      assert.equal(fixture.getCounts().launches, launchesBeforeRetry);
      yield* TestClock.adjust("10 minutes");
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

it.effect.each(["submit", "startup", "replay", "cancellation"] as const)(
  "%s activation stops after eight failed recovery attempts, excluding synchronous submit",
  (entry) =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      if (entry !== "submit") {
        fixture.held(true);
        const first = yield* fixture.create();
        yield* first.lifecycle.submit(commandId, { task: "Inspect fixture" }, envelope);
        yield* first.close;
      }
      const before = yield* fixture.store.get(commandId);
      fixture.unreachable(true);
      const active = yield* fixture.create();
      const activate = () => {
        switch (entry) {
          case "submit":
            return active.lifecycle
              .submit(commandId, { task: "Inspect fixture" }, envelope)
              .pipe(Effect.asVoid);
          case "startup":
            return active.lifecycle.start;
          case "replay":
            return active.lifecycle
              .replay(commandId, { task: "Inspect fixture" })
              .pipe(Effect.asVoid);
          case "cancellation":
            return active.lifecycle
              .cancelPending(fixture.getTask(), "Owner cancelled")
              .pipe(Effect.asVoid);
        }
      };
      const count = () =>
        entry === "cancellation" ? fixture.getCounts().withdrawals : fixture.getCounts().launches;
      const initialCalls = count();
      yield* activate();
      yield* TestClock.adjust("1 hour");
      yield* Deferred.await(fixture.parked);
      const attempts = entry === "submit" ? 9 : 8;
      assert.equal(count() - initialCalls, attempts);
      const pending = yield* fixture.store.get(commandId);
      assert.equal(pending?.status, "pending");
      assert.equal(pending?.launchStarted, true);
      assert.deepEqual(pending?.envelope, envelope);
      assert.equal(pending?.taskId, fixture.getTask().id);
      if (before !== undefined) {
        assert.equal(pending?.createdAt, before.createdAt);
        assert.deepEqual(pending?.admission, before.admission);
      }
      assert.equal(fixture.getTask().result === null, entry !== "cancellation");
      if (entry === "cancellation") assert.equal(fixture.getTask().status, "cancelled");
      else assert.match(fixture.getTask().progress ?? "", /reconciliation.*on hold/i);
      const stopped = fixture.getCounts();
      yield* TestClock.adjust("1 day");
      assert.deepEqual(fixture.getCounts(), stopped);

      // A deliberate activation gets a fresh bounded window, retaining identity
      // and the one durable reconciliation notice across both windows.
      yield* activate();
      yield* TestClock.adjust("1 hour");
      assert.equal(count() - initialCalls, attempts * 2);
      assert.equal(
        fixture.getNotices().filter(([id]) => id.endsWith(":reconciliation-notice")).length,
        1,
      );
      assert.equal(fixture.getCounts().remoteExecutions, 0);
      assert.deepEqual(new Set(fixture.keys), new Set([envelope.launch.clientRequestId]));
      assert.deepEqual(new Set(fixture.namespaces), new Set([envelope.scope.requestNamespace]));
      assert.deepEqual(yield* fixture.store.get(commandId), pending);
      yield* active.close;
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "invalid_request",
  "request_conflict",
  "capability_denied",
  "model_unavailable",
  "provider_unavailable",
  "runtime_mode_escalation_denied",
  "interaction_mode_escalation_denied",
] as const)("first typed %s refusal rejects without automatic retries", (code) =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    fixture.launchFailure(
      new OrchestratorMcpFailure({ code, message: "Synthetic permanent refusal" }),
    );
    const active = yield* fixture.create();
    yield* active.lifecycle.submit(commandId, { task: "Inspect fixture" }, envelope);
    assert.equal(fixture.getTask().status, "failed");
    assert.equal(fixture.getCounts().launches, 1);
    assert.equal(fixture.getCounts().remoteExecutions, 0);
    assert.equal((yield* fixture.store.get(commandId))?.status, "denied");
    const stopped = fixture.getCounts();
    yield* TestClock.adjust("1 day");
    assert.deepEqual(fixture.getCounts(), stopped);
    yield* active.close;
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["missing", "replaced", "probe refusal", "request conflict", "denial"] as const)(
  "%s after possible execution parks the original intent without spinning or fabricating failure",
  (refusal) =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      fixture.unreachable(true);
      const first = yield* fixture.create();
      yield* first.lifecycle.submit(commandId, { task: "Inspect fixture" }, envelope);
      yield* first.close;
      const original = yield* fixture.store.get(commandId);
      fixture.unreachable(false);
      if (refusal === "missing" || refusal === "replaced") fixture.linkState(refusal);
      else if (refusal === "probe refusal")
        fixture.probeFailure(
          new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "Synthetic probe refusal",
          }),
        );
      else
        fixture.launchFailure(
          new OrchestratorMcpFailure({
            code: refusal === "denial" ? "remote_admission_denied" : "request_conflict",
            message: "Synthetic later refusal",
            ...(refusal === "denial"
              ? {
                  remoteAdmission: {
                    id: "held-1",
                    bindingDigest: "a".repeat(64),
                    status: "denied" as const,
                  },
                }
              : {}),
          }),
        );
      const active = yield* fixture.create();
      yield* active.lifecycle.start;
      yield* Deferred.await(fixture.parked);
      assert.equal(fixture.getTask().result, null);
      assert.equal(fixture.getTask().status, "waiting");
      const pending = yield* fixture.store.get(commandId);
      assert.deepEqual({ ...pending, admission: original?.admission }, original);
      if (refusal === "denial") assert.equal(pending?.admission?.status, "denied");
      else assert.deepEqual(pending?.admission, original?.admission);
      assert.match(fixture.getTask().progress ?? "", /execution may already exist/i);
      const stopped = fixture.getCounts();
      yield* TestClock.adjust("1 day");
      assert.deepEqual(fixture.getCounts(), stopped);
      assert.equal(fixture.getCounts().notices, 1);
      yield* active.close;
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["lost acknowledgement", "lost persistence", "dispatching"] as const)(
  "%s remains nonterminal after recovery exhaustion",
  (unknown) =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      const sql = yield* SqlClient.SqlClient;
      if (unknown === "lost acknowledgement") fixture.lostAcknowledgement();
      else if (unknown === "lost persistence")
        yield* sql`CREATE TRIGGER simulated_crash BEFORE UPDATE OF launched_json ON jones_remote_delegation_intents BEGIN SELECT RAISE(ABORT, 'synthetic crash after remote launch'); END`;
      else
        fixture.launchFailure(
          new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "Dispatching without a receipt",
            remoteAdmission: {
              id: "held-1",
              bindingDigest: "a".repeat(64),
              status: "dispatching",
            },
          }),
        );
      const active = yield* fixture.create();
      yield* active.lifecycle.submit(commandId, { task: "Inspect fixture" }, envelope);
      yield* TestClock.adjust("1 hour");
      yield* Deferred.await(fixture.parked);
      assert.equal(fixture.getCounts().launches, 9);
      assert.equal(fixture.getCounts().remoteExecutions, unknown === "dispatching" ? 0 : 1);
      assert.equal(fixture.getTask().result, null);
      assert.equal(fixture.getTask().status, "waiting");
      const pending = yield* fixture.store.get(commandId);
      assert.equal(pending?.status, "pending");
      assert.equal(pending?.launchStarted, true);
      assert.equal(pending?.launched, null);
      if (unknown === "dispatching") assert.equal(pending?.admission?.status, "dispatching");
      const stopped = fixture.getCounts();
      yield* TestClock.adjust("1 day");
      assert.deepEqual(fixture.getCounts(), stopped);
      assert.deepEqual(new Set(fixture.keys), new Set([envelope.launch.clientRequestId]));
      yield* active.close;
    }).pipe(Effect.provide(layer)),
);

it.effect.each([0, 8])(
  "held approval remains actionable beyond the budget after %s initial errors and binds the original task",
  (errors) =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      fixture.held(true);
      fixture.launchFailure(
        new OrchestratorMcpFailure({ code: "orchestration_error", message: "Synthetic transient" }),
        errors,
      );
      const active = yield* fixture.create();
      const submitted = yield* active.lifecycle.submit(
        commandId,
        { task: "Inspect fixture" },
        envelope,
      );
      yield* TestClock.adjust("1 hour");
      assert.isAbove(fixture.getCounts().launches, errors + 8);
      assert.equal(fixture.getTask().progress, "Awaiting owner approval in Destination.");
      assert.equal(fixture.getTask().result, null);
      assert.equal(fixture.getCounts().notices, 1);
      const original = yield* fixture.store.get(commandId);
      assert.equal(original?.admission?.status, "held");
      fixture.held(false);
      yield* TestClock.adjust("10 minutes");
      yield* Deferred.await(fixture.bound);
      assert.equal(fixture.getTask().id, submitted.taskId);
      assert.equal(fixture.getCounts().remoteExecutions, 1);
      assert.equal(fixture.getCounts().notices, 1);
      assert.deepEqual(new Set(fixture.keys), new Set([envelope.launch.clientRequestId]));
      assert.deepEqual(new Set(fixture.namespaces), new Set([envelope.scope.requestNamespace]));
      assert.deepEqual(new Set(fixture.launchSentAts), new Set([original?.createdAt]));
      const recorded = yield* fixture.store.get(commandId);
      assert.equal(recorded?.status, "recorded");
      assert.deepEqual(recorded?.envelope, original?.envelope);
      assert.deepEqual(recorded?.admission, original?.admission);
      yield* active.close;
    }).pipe(Effect.provide(layer)),
);

it.effect("submit, startup, replay and cancellation share one in-flight activation", () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    fixture.blockLaunch();
    fixture.unreachable(true);
    const active = yield* fixture.create();
    const first = yield* active.lifecycle
      .submit(commandId, { task: "Inspect fixture" }, envelope)
      .pipe(Effect.forkChild);
    yield* Deferred.await(fixture.launchEntered);
    const duplicates = yield* Effect.all(
      [
        active.lifecycle.submit(commandId, { task: "Inspect fixture" }, envelope),
        active.lifecycle.replay(commandId, { task: "Inspect fixture" }),
      ],
      { concurrency: "unbounded" },
    ).pipe(Effect.forkChild);
    yield* active.lifecycle.start;
    yield* active.lifecycle.cancelPending(fixture.getTask(), "Owner cancelled");
    assert.equal(fixture.getCounts().launches, 1);
    yield* Deferred.succeed(fixture.launchRelease, undefined);
    yield* Fiber.join(first);
    yield* Fiber.join(duplicates);
    yield* TestClock.adjust("1 hour");
    yield* Deferred.await(fixture.parked);
    assert.equal(fixture.getCounts().maxActiveLaunches, 1);
    assert.equal(fixture.getCounts().launches, 9);
    assert.equal(fixture.getCounts().remoteExecutions, 0);
    assert.equal((yield* fixture.store.get(commandId))?.status, "pending");
    assert.equal(fixture.getTask().status, "cancelled");
    fixture.unreachable(false);
    yield* active.lifecycle.start;
    yield* Deferred.await(fixture.bound);
    assert.equal(fixture.getCounts().launches, 10);
    assert.equal(fixture.getCounts().remoteExecutions, 1);
    assert.equal(fixture.getTask().remoteChild?.stoppedThere, false);
    yield* active.close;
  }).pipe(Effect.provide(layer)),
);

it.effect("healthy approval holds do not reset failures already spent in the activation", () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const transient = new OrchestratorMcpFailure({
      code: "orchestration_error",
      message: "Synthetic transient",
    });
    fixture.held(true);
    // One synchronous failure and seven recovery failures precede healthy holds.
    fixture.launchFailure(transient, 8);
    const active = yield* fixture.create();
    yield* active.lifecycle.submit(commandId, { task: "Inspect fixture" }, envelope);
    yield* TestClock.adjust("1 hour");
    assert.equal(fixture.getTask().progress, "Awaiting owner approval in Destination.");
    const launches = fixture.getCounts().launches;
    fixture.launchFailure(transient, 1);
    yield* TestClock.adjust("10 minutes");
    yield* Deferred.await(fixture.parked);
    assert.equal(fixture.getCounts().launches, launches + 1);
    assert.equal(fixture.getTask().result, null);
    assert.equal((yield* fixture.store.get(commandId))?.admission?.status, "held");
    const stopped = fixture.getCounts();
    yield* TestClock.adjust("1 day");
    assert.deepEqual(fixture.getCounts(), stopped);
    yield* active.close;
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["dispatching", "denied"] as const)(
  "cancellation reconciles authenticated withdrawal status %s without losing stop debt",
  (status) =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture;
      fixture.held(true);
      const first = yield* fixture.create();
      yield* first.lifecycle.submit(commandId, { task: "Inspect fixture" }, envelope);
      yield* first.close;
      fixture.held(false);
      fixture.dispatchRacedCancel(status);
      const active = yield* fixture.create();
      yield* active.lifecycle.cancelPending(fixture.getTask(), "Withdraw");
      yield* TestClock.adjust("1 hour");
      assert.equal(fixture.getTask().status, "cancelled");
      assert.equal(fixture.getCounts().remoteExecutions, 0);
      const intent = yield* fixture.store.get(commandId);
      assert.equal(intent?.status, status === "denied" ? "stopped" : "pending");
      if (status === "dispatching") {
        yield* Deferred.await(fixture.parked);
        assert.equal(fixture.getCounts().withdrawals, 8);
        assert.equal(fixture.getTask().remoteChild, undefined);
        assert.equal(intent?.admission?.status, "dispatching");
        const stopped = fixture.getCounts();
        yield* TestClock.adjust("1 day");
        assert.deepEqual(fixture.getCounts(), stopped);
      } else assert.equal(fixture.getCounts().launches, 1);
      yield* active.close;
    }).pipe(Effect.provide(layer)),
);
