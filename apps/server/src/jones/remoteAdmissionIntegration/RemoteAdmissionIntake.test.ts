import { expect, it } from "@effect/vitest";
import {
  AuthSessionId,
  AuthAdministrativeScopes,
  EnvironmentId,
  ProviderInstanceId,
  type AuthClientSession,
  type EnvironmentSessionPrincipalShape,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import migrateCore from "../persistence/Migrations/115_JonesRemoteAdmission.ts";
import migrateIntegration from "../persistence/Migrations/117_JonesRemoteAdmissionIntegration.ts";
import * as Admission from "../remoteAdmission/RemoteAdmission.ts";
import * as Intake from "./RemoteAdmissionIntake.ts";
import { isRemoteAdmissionOwner } from "./owner.ts";
import { screensRemoteOperation } from "./operations.ts";

const invocation: McpInvocationScope = {
  environmentId: EnvironmentId.make("receiver"),
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
  requestNamespace: "client:remote",
  thread: undefined,
  client: { sessionId: "remote", label: "Synthetic peer", access: "full-access", linked: true },
};
const owner: EnvironmentSessionPrincipalShape = {
  sessionId: AuthSessionId.make("owner"),
  subject: "desktop-bootstrap",
  method: "browser-session-cookie",
  scopes: new Set(AuthAdministrativeScopes),
};
const pass = { verdict: "pass", category: "none", reason: "Ordinary work" };
const hold = { verdict: "hold", category: "risky", reason: "Needs the receiving owner" };
const memory = Layer.effectDiscard(migrateCore.pipe(Effect.andThen(migrateIntegration))).pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const sessions = Layer.succeed(EnvironmentAuth.EnvironmentAuth, {
  listSessions: () =>
    Effect.succeed([{ sessionId: AuthSessionId.make("remote") } as AuthClientSession]),
} as unknown as EnvironmentAuth.EnvironmentAuth["Service"]);
const fixture = (evaluate: Admission.RemoteAdmissionEvaluator) =>
  Intake.layer.pipe(
    Layer.provideMerge(Admission.layerWithEvaluator(evaluate)),
    Layer.provide(sessions),
    Layer.provideMerge(memory),
  );
const enable = (intake: Intake.RemoteAdmissionIntake["Service"]) =>
  intake.updatePolicy({
    expectedRevision: "initial",
    enabled: true,
    modelSelection: { instanceId: ProviderInstanceId.make("claude"), model: "synthetic" },
  });
const target = () => Effect.succeed({ project: "synthetic", model: "synthetic" });
const input = {
  clientRequestId: "launch-key",
  projectId: "project",
  prompt: "Implement a synthetic change",
  runtimeMode: "full-access",
};

it.effect(
  "held launch runs no handler, rejects agent approval, then owner approval dispatches original once",
  () =>
    Effect.gen(function* () {
      const intake = yield* Intake.RemoteAdmissionIntake;
      const sql = yield* SqlClient.SqlClient;
      yield* enable(intake);
      const received: unknown[] = [];
      intake.register(
        "t3_thread_launch",
        (params, scope) =>
          Effect.sync(() => {
            received.push({ params, scope });
            return { threadId: "child", launchKey: "ordinary-result" };
          }),
        target,
      );
      const pending = yield* intake
        .invoke("t3_thread_launch", input, invocation)
        .pipe(Effect.result);
      expect(pending._tag).toBe("Failure");
      if (pending._tag === "Failure") expect(pending.failure.code).toBe("remote_admission_pending");
      expect(received).toHaveLength(0);
      const record = (yield* intake.snapshot()).pending[0]!;
      expect(record.status).toBe("held");
      const decision = {
        id: record.id,
        bindingDigest: record.bindingDigest,
        decision: "approve" as const,
      };
      const forbidden = yield* intake
        .decide(decision, { ...owner, subject: "mcp-client", method: "bearer-access-token" })
        .pipe(Effect.result);
      expect(forbidden._tag).toBe("Failure");
      expect(received).toHaveLength(0);
      expect((yield* intake.decide(decision, owner)).status).toBe("dispatched");
      yield* intake.decide(decision, owner);
      expect(received).toHaveLength(1);
      expect(received[0]).toEqual({ params: input, scope: { ...invocation, issuedAt: 0 } });
      expect(
        yield* intake.invoke("t3_thread_launch", input, { ...invocation, issuedAt: 999 }),
      ).toEqual({ threadId: "child", launchKey: "ordinary-result" });
      expect(received).toHaveLength(1);
      expect(yield* sql`SELECT admission_id FROM jones_remote_admission_results`).toHaveLength(1);
    }).pipe(Effect.provide(fixture(() => Effect.succeed(hold)))),
);

it.effect(
  "default off goes directly to ordinary dispatch without evaluator; enabled pass preserves modes",
  () => {
    let evaluated = 0;
    return Effect.gen(function* () {
      const intake = yield* Intake.RemoteAdmissionIntake;
      let calls = 0;
      intake.register(
        "t3_thread_send",
        (params) =>
          Effect.sync(() => {
            calls++;
            return params;
          }),
        target,
      );
      expect(yield* intake.invoke("t3_thread_send", input, invocation)).toEqual(input);
      expect(evaluated).toBe(0);
      yield* enable(intake);
      const next = { ...input, clientRequestId: "second" };
      expect(yield* intake.invoke("t3_thread_send", next, invocation)).toEqual(next);
      expect(evaluated).toBe(1);
      expect(calls).toBe(2);
    }).pipe(
      Effect.provide(
        fixture(() =>
          Effect.sync(() => {
            evaluated++;
            return pass;
          }),
        ),
      ),
    );
  },
);

it.effect(
  "changed bytes or policy cannot reuse approval; denial and sender cancellation never dispatch",
  () =>
    Effect.gen(function* () {
      const intake = yield* Intake.RemoteAdmissionIntake;
      const config = yield* enable(intake);
      let calls = 0;
      intake.register(
        "t3_thread_launch",
        () =>
          Effect.sync(() => {
            calls++;
            return {};
          }),
        target,
      );
      yield* intake.invoke("t3_thread_launch", input, invocation).pipe(Effect.ignore);
      const record = (yield* intake.snapshot()).pending[0]!;
      expect(
        (yield* intake
          .invoke("t3_thread_launch", { ...input, prompt: "Changed" }, invocation)
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      yield* intake.updatePolicy({
        expectedRevision: config.policy.revision,
        enabled: true,
        modelSelection: config.policy.modelSelection,
      });
      expect(
        (yield* intake
          .decide(
            { id: record.id, bindingDigest: record.bindingDigest, decision: "approve" },
            owner,
          )
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      yield* intake.decide(
        { id: record.id, bindingDigest: record.bindingDigest, decision: "deny" },
        owner,
      );
      const denied = yield* intake
        .invoke("t3_thread_launch", input, invocation)
        .pipe(Effect.result);
      if (denied._tag === "Failure") expect(denied.failure.code).toBe("remote_admission_denied");
      else expect.fail("denial must not pass");
      yield* intake
        .invoke("t3_thread_launch", { ...input, clientRequestId: "cancel-me" }, invocation)
        .pipe(Effect.ignore);
      const cancel = (yield* intake.snapshot()).pending[0]!;
      expect((yield* intake.withdraw(cancel, "another-client").pipe(Effect.result))._tag).toBe(
        "Failure",
      );
      expect((yield* intake.withdraw(cancel, "remote")).status).toBe("cancelled");
      expect(calls).toBe(0);
    }).pipe(Effect.provide(fixture(() => Effect.succeed(hold)))),
);

it.effect("restart resumes approved work with stored scope exactly once", () =>
  Effect.gen(function* () {
    const admissions = yield* Admission.RemoteAdmission;
    const firstLayer = Intake.layer.pipe(Layer.provide(sessions));
    const id = yield* Effect.gen(function* () {
      const intake = yield* Intake.RemoteAdmissionIntake;
      yield* enable(intake);
      intake.register("t3_thread_launch", () => Effect.succeed({}), target);
      yield* intake.invoke("t3_thread_launch", input, invocation).pipe(Effect.ignore);
      const record = (yield* intake.snapshot()).pending[0]!;
      yield* admissions.resolve({
        id: record.id,
        bindingDigest: record.bindingDigest,
        decision: "approve",
        principal: { kind: "human-owner", subject: "owner" },
      });
      return record.id;
    }).pipe(Effect.provide(firstLayer));
    let calls = 0;
    yield* Effect.gen(function* () {
      const intake = yield* Intake.RemoteAdmissionIntake;
      intake.register(
        "t3_thread_launch",
        () =>
          Effect.sync(() => {
            calls++;
            return { threadId: "child" };
          }),
        target,
      );
      yield* intake.resumeApproved();
      yield* intake.resumeApproved();
      expect((yield* admissions.get(id))?.status).toBe("dispatched");
      expect(calls).toBe(1);
    }).pipe(Effect.provide(Intake.layer.pipe(Layer.provide(sessions))));
  }).pipe(
    Effect.provide(
      Admission.layerWithEvaluator(() => Effect.succeed(hold)).pipe(Layer.provideMerge(memory)),
    ),
  ),
);

it("owner classification accepts administrative browser pairing and rejects MCP, agent bearer, and missing admin scopes", () => {
  expect(isRemoteAdmissionOwner(owner)).toBe(true);
  expect(isRemoteAdmissionOwner({ ...owner, subject: "one-time-token" })).toBe(true);
  expect(isRemoteAdmissionOwner({ ...owner, subject: "reusable-dev-token-child" })).toBe(true);
  expect(isRemoteAdmissionOwner({ ...owner, subject: "mcp-client" })).toBe(false);
  expect(isRemoteAdmissionOwner({ ...owner, method: "bearer-access-token" })).toBe(false);
  expect(isRemoteAdmissionOwner({ ...owner, method: "dpop-access-token" })).toBe(false);
  expect(isRemoteAdmissionOwner({ ...owner, scopes: new Set() })).toBe(false);
});

it("all remote task and queue mutation surfaces are screened while cancellation passes", () => {
  for (const operation of [
    "t3_thread_launch",
    "t3_thread_send",
    "delegate_task",
    "create_threads",
    "t3_queue_edit",
    "t3_queue_reorder",
    "t3_queue_promote_to_steer",
    "t3_pending_request_respond",
    "run_scheduled_task_now",
  ])
    expect(screensRemoteOperation(operation, {})).toBe(true);
  for (const operation of [
    "task_cancel",
    "t3_queue_cancel",
    "t3_thread_interrupt",
    "t3_remote_admission_cancel",
  ])
    expect(screensRemoteOperation(operation, {})).toBe(false);
});

it.effect(
  "unkeyed identical sends are separate requests and disabled receivers write no admission rows",
  () =>
    Effect.gen(function* () {
      const intake = yield* Intake.RemoteAdmissionIntake;
      const sql = yield* SqlClient.SqlClient;
      let calls = 0;
      intake.register(
        "t3_thread_send",
        () =>
          Effect.sync(() => {
            calls++;
            return { sequence: calls };
          }),
        target,
      );
      const unkeyed = { threadId: "thread", message: "Continue" };
      yield* intake.invoke("t3_thread_send", unkeyed, invocation);
      yield* intake.invoke("t3_thread_send", unkeyed, invocation);
      expect(yield* sql`SELECT id FROM jones_remote_admissions`).toHaveLength(0);
      yield* enable(intake);
      yield* intake.invoke("t3_thread_send", unkeyed, invocation);
      yield* intake.invoke("t3_thread_send", unkeyed, invocation);
      expect(yield* sql`SELECT id FROM jones_remote_admissions`).toHaveLength(2);
      expect(calls).toBe(4);
    }).pipe(Effect.provide(fixture(() => Effect.succeed(pass)))),
);

it.effect("an owner approval cannot dispatch a changed project, model, or queue snapshot", () =>
  Effect.gen(function* () {
    const intake = yield* Intake.RemoteAdmissionIntake;
    yield* enable(intake);
    let configuration = { workspace: "/synthetic/a", model: "first", queue: "reviewed bytes" };
    let calls = 0;
    intake.register(
      "t3_queue_promote_to_steer",
      () =>
        Effect.sync(() => {
          calls++;
          return {};
        }),
      () => Effect.succeed(configuration),
    );
    yield* intake.invoke("t3_queue_promote_to_steer", input, invocation).pipe(Effect.ignore);
    const record = (yield* intake.snapshot()).pending[0]!;
    configuration = { workspace: "/synthetic/b", model: "second", queue: "changed bytes" };
    expect(
      (yield* intake
        .decide({ id: record.id, bindingDigest: record.bindingDigest, decision: "approve" }, owner)
        .pipe(Effect.result))._tag,
    ).toBe("Failure");
    expect(calls).toBe(0);
    yield* intake.resumeApproved();
    expect(calls).toBe(0);
    expect((yield* intake.snapshot()).pending[0]?.status).toBe("held");
  }).pipe(Effect.provide(fixture(() => Effect.succeed(hold)))),
);

it.effect("a handler interrupted after claiming is never blindly dispatched again", () =>
  Effect.gen(function* () {
    const intake = yield* Intake.RemoteAdmissionIntake;
    yield* enable(intake);
    let calls = 0;
    intake.register(
      "t3_thread_launch",
      () =>
        Effect.sync(() => {
          calls++;
        }).pipe(Effect.andThen(Effect.die("synthetic lost dispatch response"))),
      target,
    );
    yield* intake.invoke("t3_thread_launch", input, invocation).pipe(Effect.exit);
    const record = (yield* intake.snapshot()).pending[0]!;
    expect(record.status).toBe("dispatching");
    yield* intake.invoke("t3_thread_launch", input, invocation).pipe(Effect.ignore);
    yield* intake.resumeApproved();
    expect(calls).toBe(1);
  }).pipe(Effect.provide(fixture(() => Effect.succeed(pass)))),
);

it.effect(
  "keyed completed retries reuse their receipt even after queue or target state advances",
  () =>
    Effect.gen(function* () {
      const intake = yield* Intake.RemoteAdmissionIntake;
      yield* enable(intake);
      let configuration = "queued";
      let calls = 0;
      intake.register(
        "t3_queue_promote_to_steer",
        () =>
          Effect.sync(() => {
            calls++;
            configuration = "delivered";
            return { sequence: 7 };
          }),
        () => Effect.succeed({ configuration }),
      );
      expect(yield* intake.invoke("t3_queue_promote_to_steer", input, invocation)).toEqual({
        sequence: 7,
      });
      expect(yield* intake.invoke("t3_queue_promote_to_steer", input, invocation)).toEqual({
        sequence: 7,
      });
      expect(calls).toBe(1);
    }).pipe(Effect.provide(fixture(() => Effect.succeed(pass)))),
);
