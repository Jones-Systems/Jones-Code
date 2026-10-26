// @effect-diagnostics nodeBuiltinImport:off -- Reproduces the released idempotency key for synthetic legacy rows.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import {
  AuthSessionId,
  AuthAdministrativeScopes,
  EnvironmentId,
  ProviderInstanceId,
  RemoteAdmissionSnapshot,
  ThreadId,
  type AuthClientSession,
  type EnvironmentSessionPrincipalShape,
  type LinkedRequestContext,
  type OrchestrationV2ThreadShell,
  type Project,
  type RemoteAdmissionEnvelope,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import migrateCore from "../persistence/Migrations/116_JonesRemoteAdmission.ts";
import migrateIntegration from "../persistence/Migrations/118_JonesRemoteAdmissionIntegration.ts";
import * as Admission from "../remoteAdmission/RemoteAdmission.ts";
import * as Intake from "./RemoteAdmissionIntake.ts";
import { isRemoteAdmissionOwner } from "./owner.ts";
import { screensRemoteOperation } from "./operations.ts";
import * as ProviderInstanceRegistry from "../../provider/ProviderInstanceRegistry.ts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import { IncomingLinkedRequestContext } from "../peerTransport/LinkedRequestContext.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import { resolveTarget } from "./target.ts";

const providers = Layer.succeed(ProviderInstanceRegistry.ProviderInstanceRegistry, {
  getInstance: (id) =>
    Effect.succeed(
      id === "claude"
        ? ({
            enabled: true,
            snapshot: {
              getSnapshot: Effect.succeed({
                enabled: true,
                supportsRemoteRequestEvaluation: true,
                models: [
                  {
                    slug: "synthetic",
                    capabilities: {
                      optionDescriptors: [
                        {
                          id: "reasoningEffort",
                          type: "select",
                          label: "Effort",
                          options: [
                            { id: "medium", label: "Medium" },
                            { id: "high", label: "High" },
                          ],
                        },
                      ],
                    },
                  },
                ],
              }),
            },
          } as unknown as ProviderInstance)
        : undefined,
    ),
} as ProviderInstanceRegistry.ProviderInstanceRegistry["Service"]);

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
const fixture = <E>(evaluate: Admission.RemoteAdmissionEvaluator<E>) =>
  Intake.layer.pipe(
    Layer.provide(providers),
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
  "enabled policy rejects unsupported instance/model without changing the saved selection",
  () =>
    Effect.gen(function* () {
      const intake = yield* Intake.RemoteAdmissionIntake;
      const saved = yield* enable(intake);
      for (const modelSelection of [
        { instanceId: ProviderInstanceId.make("codex-cli"), model: "gpt-6.1-sol" },
        { instanceId: ProviderInstanceId.make("claude"), model: "missing-model" },
        {
          instanceId: ProviderInstanceId.make("claude"),
          model: "synthetic",
          options: [{ id: "reasoningEffort", value: "unsupported" }],
        },
      ]) {
        yield* intake
          .updatePolicy({ enabled: true, expectedRevision: saved.policy.revision, modelSelection })
          .pipe(Effect.flip);
        expect((yield* intake.snapshot()).policy).toEqual(saved.policy);
      }
      const selected = {
        ...saved.policy.modelSelection!,
        options: [{ id: "reasoningEffort", value: "high" }],
      };
      const updated = yield* intake.updatePolicy({
        enabled: true,
        expectedRevision: saved.policy.revision,
        modelSelection: selected,
      });
      expect(updated.policy.modelSelection).toEqual(selected);
      const disabled = yield* intake.updatePolicy({
        enabled: false,
        expectedRevision: updated.policy.revision,
        modelSelection: updated.policy.modelSelection,
      });
      expect(disabled.policy.modelSelection).toEqual(selected);
    }).pipe(Effect.provide(fixture(() => Effect.succeed(pass)))),
);

it.effect(
  "attention counts only actionable held requests across every request lifecycle status",
  () =>
    Effect.gen(function* () {
      const intake = yield* Intake.RemoteAdmissionIntake;
      const sql = yield* SqlClient.SqlClient;
      yield* enable(intake);
      intake.register("t3_thread_launch", () => Effect.die("held request cannot dispatch"), target);
      yield* intake.invoke("t3_thread_launch", input, invocation).pipe(Effect.ignore);
      const record = (yield* intake.snapshot()).pending[0]!;
      for (const status of [
        "held",
        "evaluating",
        "passed",
        "approved",
        "denied",
        "cancelled",
        "dispatching",
        "dispatched",
      ]) {
        const commandId = ["dispatching", "dispatched"].includes(status)
          ? "synthetic-command"
          : null;
        yield* sql`UPDATE jones_remote_admissions SET status = ${status}, command_id = ${commandId} WHERE id = ${record.id}`;
        expect((yield* intake.attention()).count).toBe(status === "held" ? 1 : 0);
      }
    }).pipe(Effect.provide(fixture(() => Effect.succeed(hold)))),
);

it.effect(
  "evaluator failure stays held and actionable without dispatch or risk misclassification",
  () =>
    Effect.gen(function* () {
      const intake = yield* Intake.RemoteAdmissionIntake;
      yield* enable(intake);
      intake.register(
        "t3_thread_launch",
        () => Effect.die("failed evaluation cannot dispatch"),
        target,
      );
      yield* intake.invoke("t3_thread_launch", input, invocation).pipe(Effect.ignore);
      expect((yield* intake.snapshot()).pending[0]).toMatchObject({
        status: "held",
        category: "evaluation_unavailable",
      });
      expect((yield* intake.attention()).count).toBe(1);
    }).pipe(Effect.provide(fixture(() => Effect.fail("Synthetic evaluation failure")))),
);

it.effect.each(["browser-session-cookie", "bearer-access-token"] as const)(
  "held launch rejects agent approval, then %s owner dispatches original once",
  (method) =>
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
      expect((yield* intake.decide(decision, { ...owner, method })).status).toBe("dispatched");
      yield* intake.decide(decision, { ...owner, method });
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
    const firstLayer = Intake.layer.pipe(Layer.provide(sessions), Layer.provide(providers));
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
    }).pipe(Effect.provide(Intake.layer.pipe(Layer.provide(sessions), Layer.provide(providers))));
  }).pipe(
    Effect.provide(
      Admission.layerWithEvaluator(() => Effect.succeed(hold)).pipe(Layer.provideMerge(memory)),
    ),
  ),
);

it("owner classification accepts administrative browser pairing and desktop bearer while rejecting non-owners", () => {
  expect(isRemoteAdmissionOwner(owner)).toBe(true);
  expect(isRemoteAdmissionOwner({ ...owner, subject: "one-time-token" })).toBe(true);
  expect(isRemoteAdmissionOwner({ ...owner, subject: "reusable-dev-token-child" })).toBe(true);
  expect(isRemoteAdmissionOwner({ ...owner, subject: "mcp-client" })).toBe(false);
  expect(isRemoteAdmissionOwner({ ...owner, method: "bearer-access-token" })).toBe(true);
  expect(
    isRemoteAdmissionOwner({ ...owner, subject: "agent", method: "bearer-access-token" }),
  ).toBe(false);
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

const firstContext: LinkedRequestContext = {
  schemaVersion: "t3.linked-request-context/v1",
  sentAt: "2026-10-26T12:00:00.000Z",
  thread: { threadId: "sender-thread", title: "Draft" },
  project: { projectId: "sender-project", title: "Sender project" },
};
const originInvocation = (title: string | null = "Draft"): McpInvocationScope => ({
  ...invocation,
  client: {
    ...invocation.client!,
    remoteOrigin: {
      schemaVersion: "t3.linked-message-origin/v1",
      environmentId: "sender",
      linkSessionId: "remote",
      thread: { threadId: "sender-thread", ...(title === null ? {} : { title }) },
    },
  },
});
const withContext = (
  intake: Intake.RemoteAdmissionIntake["Service"],
  {
    scope = originInvocation(),
    context = firstContext,
    params = input,
    linkSessionId = "remote",
  }: {
    scope?: McpInvocationScope;
    context?: LinkedRequestContext | null;
    params?: Record<string, unknown>;
    linkSessionId?: string;
  } = {},
) =>
  intake
    .invoke("t3_thread_launch", params, scope)
    .pipe(
      Effect.provideService(
        IncomingLinkedRequestContext,
        context === null ? undefined : { context, linkSessionId },
      ),
    );
const bindings = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{
    id: string;
    envelope_json: string;
    request_digest: string;
    binding_digest: string;
  }>`SELECT id, envelope_json, request_digest, binding_digest FROM jones_remote_admissions ORDER BY id`;
});
// Literal payload from the 907 intake shape: no advisory and no transient scope fields.
const legacyPayload = {
  invocation: {
    capabilities: ["orchestration"],
    client: {
      access: "full-access",
      label: "Synthetic peer",
      linked: true,
      remoteOrigin: {
        environmentId: "sender",
        linkSessionId: "remote",
        schemaVersion: "t3.linked-message-origin/v1",
        thread: { threadId: "sender-thread", title: "Draft" },
      },
      sessionId: "remote",
    },
    environmentId: "receiver",
    requestNamespace: "client:remote",
    thread: null,
  },
  params: {
    clientRequestId: "launch-key",
    projectId: "project",
    prompt: "Implement a synthetic change",
    runtimeMode: "full-access",
  },
  target: { model: "synthetic", project: "synthetic" },
};
const legacyKey = NodeCrypto.createHash("sha256")
  .update('{"key":"launch-key","namespace":"client:remote","operation":"t3_thread_launch"}')
  .digest("hex");
const legacyEnvelope = (
  policy: RemoteAdmissionEnvelope["receiverPolicy"],
  payload: RemoteAdmissionEnvelope["payload"] = legacyPayload,
): RemoteAdmissionEnvelope => ({
  identity: { clientId: "remote", linkId: "remote" },
  operation: "t3_thread_launch",
  payload,
  target: {
    projectId: "project",
    threadId: null,
    runtimeMode: "full-access",
    interactionMode: "default",
  },
  receiverPolicy: policy,
});
const decodeSnapshot = Schema.decodeUnknownSync(RemoteAdmissionSnapshot);

it.effect("B1-1: a 907 golden row keeps its bytes and both digests on a new-context retry", () =>
  Effect.gen(function* () {
    const intake = yield* Intake.RemoteAdmissionIntake;
    const admissions = yield* Admission.RemoteAdmission;
    const { policy } = yield* enable(intake);
    intake.register("t3_thread_launch", () => Effect.die("held"), target);
    const original = yield* admissions.request({
      idempotencyKey: legacyKey,
      envelope: legacyEnvelope(policy),
    });
    const before = yield* bindings;
    const retry = yield* withContext(intake, { scope: originInvocation("Final") }).pipe(
      Effect.asVoid,
      Effect.flip,
    );
    expect(retry.code).toBe("remote_admission_pending");
    expect(retry.remoteAdmission?.id).toBe(original.id);
    expect(yield* bindings).toEqual(before);
    const snapshot = decodeSnapshot(yield* intake.snapshot());
    expect(snapshot.pending[0]?.envelope.payload).toEqual(legacyPayload);
    expect(Object.hasOwn(snapshot.pending[0]!.envelope.payload as object, "advisory")).toBe(false);
  }).pipe(Effect.provide(fixture(() => Effect.succeed(hold)))),
);

it.effect.each([null, "", "Draft"])(
  "B1-2/3: first title (%s), advisory and sender time survive held and approved retries",
  (title) => {
    let evaluations = 0;
    return Effect.gen(function* () {
      const intake = yield* Intake.RemoteAdmissionIntake;
      yield* enable(intake);
      const scopes: McpInvocationScope[] = [];
      intake.register(
        "t3_thread_launch",
        (_params, scope) =>
          Effect.sync(() => {
            scopes.push(scope);
            return { threadId: "launched" };
          }),
        target,
      );
      const scope = originInvocation(title);
      const first = yield* withContext(intake, { scope }).pipe(Effect.asVoid, Effect.flip);
      expect(first.code).toBe("remote_admission_pending");
      const before = yield* bindings;
      const record = (yield* intake.snapshot()).pending[0]!;
      expect(record.envelope.payload).toMatchObject({
        advisory: {
          schemaVersion: "t3.remote-admission-advisory/v1",
          requestContext: firstContext,
        },
      });
      const retryOptions = {
        scope: originInvocation("Final"),
        context: {
          ...firstContext,
          sentAt: "2026-10-26T13:00:00.000Z",
          project: { projectId: "renamed" },
        },
      };
      const retry = yield* withContext(intake, retryOptions).pipe(Effect.asVoid, Effect.flip);
      expect(retry.code).toBe("remote_admission_pending");
      expect(retry.remoteAdmission?.id).toBe(record.id);
      const withoutContext = yield* withContext(intake, {
        scope: retryOptions.scope,
        context: null,
      }).pipe(Effect.asVoid, Effect.flip);
      expect(withoutContext.remoteAdmission?.id).toBe(record.id);
      expect(yield* bindings).toEqual(before);
      yield* intake.decide({ ...record, decision: "approve" }, owner);
      expect(scopes).toEqual([{ ...scope, issuedAt: 0 }]);
      expect(yield* withContext(intake, retryOptions)).toEqual({ threadId: "launched" });
      expect(yield* bindings).toEqual(before);
      expect(scopes).toHaveLength(1);
      expect(evaluations).toBe(1);
    }).pipe(
      Effect.provide(
        fixture(() =>
          Effect.sync(() => {
            evaluations++;
            return hold;
          }),
        ),
      ),
    );
  },
);

it.effect("B1-4: changed body conflicts while display-only changes keep the original binding", () =>
  Effect.gen(function* () {
    const intake = yield* Intake.RemoteAdmissionIntake;
    yield* enable(intake);
    intake.register("t3_thread_launch", () => Effect.die("held"), target);
    yield* withContext(intake).pipe(Effect.asVoid, Effect.flip);
    const before = yield* bindings;
    const changed = yield* withContext(intake, {
      params: { ...input, prompt: "Different requested work" },
    }).pipe(Effect.asVoid, Effect.flip);
    expect(changed.code).toBe("orchestration_error");
    const display = yield* withContext(intake, {
      context: { ...firstContext, sentAt: "2026-10-27T12:00:00.000Z" },
    }).pipe(Effect.asVoid, Effect.flip);
    expect(display.code).toBe("remote_admission_pending");
    expect(display.remoteAdmission?.id).toBe(before[0]!.id);
    expect(yield* bindings).toEqual(before);
  }).pipe(Effect.provide(fixture(() => Effect.succeed(hold)))),
);

const authorityChanges: ReadonlyArray<
  readonly [string, (scope: McpInvocationScope) => McpInvocationScope]
> = [
  [
    "origin environment",
    (scope) => ({
      ...scope,
      client: {
        ...scope.client!,
        remoteOrigin: { ...scope.client!.remoteOrigin!, environmentId: "other-sender" },
      },
    }),
  ],
  [
    "origin thread",
    (scope) => ({
      ...scope,
      client: {
        ...scope.client!,
        remoteOrigin: {
          ...scope.client!.remoteOrigin!,
          thread: { threadId: "other-thread", title: "Final" },
        },
      },
    }),
  ],
  [
    "origin link session",
    (scope) => ({
      ...scope,
      client: {
        ...scope.client!,
        remoteOrigin: { ...scope.client!.remoteOrigin!, linkSessionId: "other-link" },
      },
    }),
  ],
  ["capabilities", (scope) => ({ ...scope, capabilities: new Set(["orchestration", "worktree"]) })],
  ["access", (scope) => ({ ...scope, client: { ...scope.client!, access: "auto" } })],
  [
    "narrowedTo",
    (scope) => ({
      ...scope,
      client: {
        ...scope.client!,
        narrowedTo: { runtimeMode: "approval-required", interactionMode: "plan" },
      },
    }),
  ],
  ["label", (scope) => ({ ...scope, client: { ...scope.client!, label: "Another label" } })],
  ["linked", (scope) => ({ ...scope, client: { ...scope.client!, linked: false } })],
  [
    "receiver identity",
    (scope) => ({ ...scope, environmentId: EnvironmentId.make("other-receiver") }),
  ],
  [
    "local thread binding",
    (scope) => ({
      ...scope,
      thread: {
        threadId: ThreadId.make("local-thread"),
        providerSessionId: "provider-session",
        providerInstanceId: ProviderInstanceId.make("claude"),
      },
    }),
  ],
];
it.effect.each(authorityChanges)("B1-4b/c: changed %s still conflicts", ([, change]) =>
  Effect.gen(function* () {
    const intake = yield* Intake.RemoteAdmissionIntake;
    yield* enable(intake);
    intake.register("t3_thread_launch", () => Effect.die("held"), target);
    yield* withContext(intake).pipe(Effect.asVoid, Effect.flip);
    const before = yield* bindings;
    const error = yield* withContext(intake, { scope: change(originInvocation()) }).pipe(
      Effect.asVoid,
      Effect.flip,
    );
    expect(error.code).toBe("orchestration_error");
    expect(yield* bindings).toEqual(before);
  }).pipe(Effect.provide(fixture(() => Effect.succeed(hold)))),
);

it.effect.each([false, true])(
  "B1-5: simultaneous first writers pin display data once (body conflict: %s)",
  (bodyConflict) => {
    let evaluations = 0;
    return Effect.gen(function* () {
      const intake = yield* Intake.RemoteAdmissionIntake;
      yield* enable(intake);
      const release = yield* Deferred.make<void>();
      let arrivals = 0;
      intake.register(
        "t3_thread_launch",
        () => Effect.die("held"),
        () =>
          Effect.gen(function* () {
            arrivals++;
            if (arrivals === 2) yield* Deferred.succeed(release, undefined);
            yield* Deferred.await(release);
            return { project: "synthetic", model: "synthetic" };
          }),
      );
      const secondContext = { ...firstContext, sentAt: "2026-10-26T14:00:00.000Z" };
      const outcomes = yield* Effect.all(
        [
          withContext(intake).pipe(Effect.asVoid, Effect.flip),
          withContext(intake, {
            scope: originInvocation("Contender"),
            context: secondContext,
            params: bodyConflict ? { ...input, prompt: "Conflicting body" } : input,
          }).pipe(Effect.asVoid, Effect.flip),
        ],
        { concurrency: "unbounded" },
      );
      expect(arrivals).toBe(2); // Both completed the no-previous lookup before either insertion.
      expect(evaluations).toBe(1);
      expect(yield* bindings).toHaveLength(1);
      const record = (yield* intake.snapshot()).pending[0]!;
      if (bodyConflict) {
        expect(outcomes.map((outcome) => outcome.code).sort()).toEqual([
          "orchestration_error",
          "remote_admission_pending",
        ]);
      } else {
        expect(outcomes.map((outcome) => outcome.code)).toEqual([
          "remote_admission_pending",
          "remote_admission_pending",
        ]);
        expect(outcomes.map((outcome) => outcome.remoteAdmission?.id)).toEqual([
          record.id,
          record.id,
        ]);
      }
      const payload = record.envelope.payload as {
        params: typeof input;
        advisory: { requestContext: LinkedRequestContext };
        invocation: { client: { remoteOrigin: { thread: { title: string } } } };
      };
      expect([firstContext.sentAt, secondContext.sentAt]).toContain(
        payload.advisory.requestContext.sentAt,
      );
      expect(payload.invocation.client.remoteOrigin.thread.title).toBe(
        payload.advisory.requestContext.sentAt === firstContext.sentAt ? "Draft" : "Contender",
      );
      const before = yield* bindings;
      const retry = yield* withContext(intake, {
        params: payload.params,
      }).pipe(Effect.asVoid, Effect.flip);
      expect(retry.code).toBe("remote_admission_pending");
      expect(yield* bindings).toEqual(before);
      expect(evaluations).toBe(1);
    }).pipe(
      Effect.provide(
        fixture(() =>
          Effect.sync(() => {
            evaluations++;
            return hold;
          }),
        ),
      ),
    );
  },
);

it.effect(
  "B1-6: receiver project/thread renames leave the real target snapshot dispatchable",
  () => {
    let projectTitle = "Original receiver project";
    let threadTitle = "Original receiver thread";
    return Effect.gen(function* () {
      const intake = yield* Intake.RemoteAdmissionIntake;
      yield* enable(intake);
      let calls = 0;
      intake.register(
        "t3_thread_launch",
        () =>
          Effect.sync(() => {
            calls++;
            return { ok: true };
          }),
        (params, scope) =>
          resolveTarget(params, scope).pipe(
            Effect.mapError((cause) => new Intake.RemoteAdmissionIntakeError({ cause })),
          ),
      );
      yield* withContext(intake, { params: { ...input, threadId: "receiver-thread" } }).pipe(
        Effect.asVoid,
        Effect.flip,
      );
      const record = (yield* intake.snapshot()).pending[0]!;
      projectTitle = "Renamed receiver project";
      threadTitle = "Renamed receiver thread";
      expect((yield* intake.decide({ ...record, decision: "approve" }, owner)).status).toBe(
        "dispatched",
      );
      expect(calls).toBe(1);
    }).pipe(
      Effect.provide(
        fixture(() => Effect.succeed(hold)).pipe(
          Layer.provideMerge(
            Layer.merge(
              Layer.mock(ProjectService.ProjectService)({
                getById: () =>
                  Effect.succeedSome({
                    title: projectTitle,
                    workspaceRoot: "/synthetic/project",
                    defaultModelSelection: null,
                    defaultThreadEnvMode: "local",
                    scripts: [],
                    deletedAt: null,
                  } as unknown as Project),
              }),
              Layer.mock(ProjectionStore.ProjectionStoreV2)({
                getThreadShell: () =>
                  Effect.succeed({
                    id: "receiver-thread",
                    title: threadTitle,
                    projectId: "project",
                    modelSelection: { instanceId: "claude", model: "synthetic" },
                    runtimeMode: "full-access",
                    interactionMode: "default",
                    branch: "task",
                    worktreePath: "/synthetic/worktree",
                    deletedAt: null,
                  } as unknown as OrchestrationV2ThreadShell),
              }),
            ),
          ),
        ),
      ),
    );
  },
);

it.effect("B1-8: another session's context is ignored and dispatch scope stays identical", () =>
  Effect.gen(function* () {
    const intake = yield* Intake.RemoteAdmissionIntake;
    yield* enable(intake);
    const scopes: McpInvocationScope[] = [];
    intake.register(
      "t3_thread_launch",
      (_params, scope) =>
        Effect.sync(() => {
          scopes.push(scope);
          return { ok: true };
        }),
      target,
    );
    yield* withContext(intake, { linkSessionId: "other-session" }).pipe(Effect.asVoid, Effect.flip);
    const record = (yield* intake.snapshot()).pending[0]!;
    expect(Object.hasOwn(record.envelope.payload as object, "advisory")).toBe(false);
    const before = yield* bindings;
    const plain = yield* withContext(intake, { context: null }).pipe(Effect.asVoid, Effect.flip);
    expect(plain.remoteAdmission?.id).toBe(record.id);
    expect(yield* bindings).toEqual(before);
    yield* intake.decide({ ...record, decision: "approve" }, owner);
    expect(scopes).toEqual([{ ...originInvocation(), issuedAt: 0 }]);
  }).pipe(Effect.provide(fixture(() => Effect.succeed(hold)))),
);

it.effect("B1-8: policy-off direct dispatch ignores advisory storage and preserves the scope", () =>
  Effect.gen(function* () {
    const intake = yield* Intake.RemoteAdmissionIntake;
    const scopes: McpInvocationScope[] = [];
    intake.register(
      "t3_thread_launch",
      (params, scope) =>
        Effect.sync(() => {
          scopes.push(scope);
          return params;
        }),
      () => Effect.die("Policy-off direct path must not resolve a target"),
    );
    expect(yield* withContext(intake)).toEqual(input);
    expect(scopes).toEqual([originInvocation()]);
    expect(yield* bindings).toHaveLength(0);
  }).pipe(Effect.provide(fixture(() => Effect.die("Policy off must not evaluate")))),
);

it.effect.each([null, { schemaVersion: "future/v2", futureKeys: { nested: [1, true, null] } }])(
  "B1-9: raw existing advisory JSON survives new retries (%s)",
  (advisory) =>
    Effect.gen(function* () {
      const intake = yield* Intake.RemoteAdmissionIntake;
      const admissions = yield* Admission.RemoteAdmission;
      const { policy } = yield* enable(intake);
      intake.register("t3_thread_launch", () => Effect.die("held"), target);
      const record = yield* admissions.request({
        idempotencyKey: legacyKey,
        envelope: legacyEnvelope(policy, { ...legacyPayload, advisory }),
      });
      const before = yield* bindings;
      const retry = yield* withContext(intake).pipe(Effect.asVoid, Effect.flip);
      expect(retry.remoteAdmission?.id).toBe(record.id);
      expect(yield* bindings).toEqual(before);
      expect((yield* intake.snapshot()).pending[0]?.envelope.payload).toEqual({
        ...legacyPayload,
        advisory,
      });
    }).pipe(Effect.provide(fixture(() => Effect.succeed(hold)))),
);

it.effect("B1-10: operation, session and namespace still select independent admission keys", () => {
  let evaluations = 0;
  return Effect.gen(function* () {
    const intake = yield* Intake.RemoteAdmissionIntake;
    yield* enable(intake);
    intake.register("t3_thread_launch", () => Effect.die("held"), target);
    intake.register("t3_thread_send", () => Effect.die("held"), target);
    const scopes = [
      invocation,
      { ...invocation, requestNamespace: "another-namespace" },
      { ...invocation, client: { ...invocation.client!, sessionId: "another-session" } },
    ];
    const outcomes = [];
    for (const scope of scopes) {
      outcomes.push(
        yield* intake.invoke("t3_thread_launch", input, scope).pipe(Effect.asVoid, Effect.flip),
      );
    }
    outcomes.push(
      yield* intake.invoke("t3_thread_send", input, invocation).pipe(Effect.asVoid, Effect.flip),
    );
    expect(outcomes.map((outcome) => outcome.code)).toEqual(
      Array(4).fill("remote_admission_pending"),
    );
    expect(new Set(outcomes.map((outcome) => outcome.remoteAdmission?.id)).size).toBe(4);
    expect(yield* bindings).toHaveLength(4);
    expect(evaluations).toBe(4);
  }).pipe(
    Effect.provide(
      fixture(() =>
        Effect.sync(() => {
          evaluations++;
          return hold;
        }),
      ),
    ),
  );
});
