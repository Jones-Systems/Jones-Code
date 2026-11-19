import { expect, it } from "@effect/vitest";
import {
  AuthAdministrativeScopes,
  AuthRequestsManageScope,
  AuthSessionId,
  EnvironmentId,
  EnvironmentHttpApi,
  ProviderInstanceId,
  ThreadId,
  type EnvironmentSessionPrincipalShape,
  type RemoteAdmissionStatus,
} from "@t3tools/contracts";
import {
  OwnerRequestsResult,
  REQUESTS_CLI_SUBJECT,
} from "@t3tools/contracts/jones/requestManagement";
import * as Sqlite from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { McpSchema, McpServer } from "effect/ai";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import * as AuthHttp from "../../auth/http.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import * as McpHttp from "../../mcp/McpHttpServer.ts";
import { liveThreadShell } from "../../mcp/McpToolAccess.testkit.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import core from "../persistence/Migrations/116_JonesRemoteAdmission.ts";
import integration from "../persistence/Migrations/118_JonesRemoteAdmissionIntegration.ts";
import review from "../persistence/Migrations/157_JonesRemoteAdmissionReview.ts";
import management from "../persistence/Migrations/165_JonesRequestManagement.ts";
import * as Admission from "../remoteAdmission/RemoteAdmission.ts";
import * as Requests from "./RequestManagement.ts";
import { ReceiverReviewReaders } from "./ReviewService.ts";
import { isRemoteAdmissionOwner } from "./owner.ts";
import { syntheticReceiverAuth } from "./receiver.testkit.ts";
import { requestManagementHttpApiLayer } from "./requestManagementHttp.ts";
import * as RequestsMcp from "./requestManagementMcp.ts";

const receiverEnvironmentId = EnvironmentId.make("test-receiver");
const cli = {
  sessionId: AuthSessionId.make("synthetic-owner-cli"),
  subject: REQUESTS_CLI_SUBJECT,
  method: "bearer-access-token",
  scopes: new Set([AuthRequestsManageScope]),
} satisfies EnvironmentSessionPrincipalShape;

const memory = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* core;
    yield* integration;
    yield* review;
    yield* management;
    yield* syntheticReceiverAuth;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO auth_sessions VALUES
    ('peer-a','mcp-client','bearer-access-token','["orchestration:read"]',NULL,'2099-01-01T00:00:00.000Z'),
    ('peer-b','mcp-client','bearer-access-token','["orchestration:read"]',NULL,'2099-01-01T00:00:00.000Z'),
    (${cli.sessionId},${cli.subject},'bearer-access-token','["requests:manage"]',NULL,'2099-01-01T00:00:00.000Z')`;
  }),
).pipe(Layer.provideMerge(Sqlite.layer({ filename: ":memory:" })));
const layer = Requests.layer.pipe(
  Layer.provide(
    Layer.succeed(ReceiverReviewReaders, {
      receiverEnvironmentId,
      liveDestinations: () => Effect.succeed([]),
      validLeaseIds: () => Effect.succeed([]),
    }),
  ),
  Layer.provideMerge(Admission.layerWithEvaluator(() => Effect.die("Policy disabled"))),
  Layer.provideMerge(memory),
);

const seed = Effect.fnUntraced(function* (
  clientId: string,
  key: string,
  status: RemoteAdmissionStatus = "held",
  operation = "t3_thread_launch",
) {
  const admissions = yield* Admission.RemoteAdmission;
  const sql = yield* SqlClient.SqlClient;
  const record = yield* admissions.request({
    idempotencyKey: key,
    envelope: {
      identity: { clientId, linkId: clientId },
      operation,
      payload: { message: "SENTINEL_PRIVATE_TEXT", token: "SENTINEL_BEARER" },
      target: {
        projectId: "SENTINEL_PRIVATE_PROJECT",
        threadId: "SENTINEL_PRIVATE_THREAD",
        runtimeMode: "approval-required",
        interactionMode: "default",
      },
      receiverPolicy: { enabled: false, revision: "test-policy", modelSelection: null },
    },
  });
  const commandId = status === "dispatching" || status === "dispatched" ? `test-${key}` : null;
  yield* sql`UPDATE jones_remote_admissions SET status=${status},command_id=${commandId} WHERE id=${record.id}`;
  return record.id;
});

it.effect(
  "binds reads to the active link session and redacts private fields across paging and exact lookup",
  () =>
    Effect.gen(function* () {
      const requests = yield* Requests.RequestManagement;
      const sql = yield* SqlClient.SqlClient;
      const a = yield* seed("peer-a", "a");
      const b = yield* seed("peer-b", "b");
      yield* seed("peer-a", "retained", "denied");
      yield* seed("peer-a", "bookkeeping", "held", "t3_remote_admission_ack");
      const before = yield* sql`SELECT * FROM jones_remote_admissions`;
      expect((yield* requests.list({}, null)).requests.map((item) => item.id).sort()).toEqual(
        [a, b].sort(),
      );
      const reader = { sessionId: "peer-a" };
      expect((yield* requests.list({}, reader)).requests.map((item) => item.id)).toEqual([a]);
      const first = yield* requests.list({ view: "all", limit: 1 }, reader);
      expect(first.requests).toHaveLength(1);
      expect(first.next).not.toBeNull();
      const second = yield* requests.list({ view: "all", after: first.next! }, reader);
      expect(second.requests).toHaveLength(1);
      expect(second.next).toBeNull();
      expect(second.requests[0]?.id).not.toBe(first.requests[0]?.id);
      const owned = yield* requests.inspect(a, reader);
      expect(Object.keys(owned.request!).sort()).toEqual([
        "createdAt",
        "id",
        "operation",
        "recoveryOutcome",
        "status",
        "updatedAt",
      ]);
      expect(JSON.stringify([first, second, owned])).not.toContain("SENTINEL");
      expect(yield* requests.inspect(b, reader)).toEqual(yield* requests.inspect("absent", reader));
      expect(yield* requests.list({ after: b }, reader).pipe(Effect.flip)).toMatchObject({
        code: "request_unavailable",
      });
      expect(yield* sql`SELECT * FROM jones_remote_admissions`).toEqual(before);
      yield* sql`UPDATE auth_sessions SET revoked_at='2026-01-01' WHERE session_id='peer-a'`;
      expect(yield* requests.inspect(a, reader).pipe(Effect.flip)).toMatchObject({
        code: "requests_owner_required",
      });
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "requires the exact local CLI session and revision, then audits reversible tombstones",
  () =>
    Effect.gen(function* () {
      const requests = yield* Requests.RequestManagement;
      const admissions = yield* Admission.RemoteAdmission;
      const sql = yield* SqlClient.SqlClient;
      const id = yield* seed("peer-a", "denied", "denied");
      const read = { action: "inspect", receiverEnvironmentId, id } as const;
      const snapshot = yield* requests.ownerQuery(read, cli);
      if (snapshot.action !== "inspect") return yield* Effect.die("Expected an exact request");
      const write = { id, receiverEnvironmentId, expectedRevision: snapshot.request.revision };
      const inbox = () =>
        admissions.review.page({
          view: "done",
          authorizationScope: "synthetic-owner",
          now: "2026-11-19T16:00:00.000Z",
          liveDestinations: () => Effect.succeed([]),
          validLeaseIds: () => Effect.succeed([]),
          projectItem: () => Effect.die("IDs are not projected by this test"),
          limit: 1,
        });
      const original = yield* admissions.get(id);
      const mcpPrincipal = {
        ...cli,
        subject: "mcp-client",
        scopes: new Set(AuthAdministrativeScopes),
      };
      expect(
        yield* requests.edit({ ...write, action: "dismiss" }, mcpPrincipal).pipe(Effect.flip),
      ).toMatchObject({ code: "requests_owner_required" });
      expect(isRemoteAdmissionOwner(cli)).toBe(false);
      expect(
        yield* requests
          .edit(
            {
              ...write,
              receiverEnvironmentId: EnvironmentId.make("wrong"),
              action: "dismiss",
            },
            cli,
          )
          .pipe(Effect.flip),
      ).toMatchObject({ code: "receiver_changed" });
      const labelled = yield* requests.edit(
        { ...write, action: "label", label: "Local triage" },
        cli,
      );
      expect(labelled.request.label).toBe("Local triage");
      expect(JSON.stringify(yield* requests.inspect(id, { sessionId: "peer-a" }))).not.toContain(
        "Local triage",
      );
      expect(
        yield* requests.edit({ ...write, action: "dismiss" }, cli).pipe(Effect.flip),
      ).toMatchObject({ code: "revision_changed" });
      const dismissed = yield* requests.edit(
        {
          ...write,
          action: "dismiss",
          expectedRevision: labelled.request.revision,
        },
        cli,
      );
      expect(dismissed.request.dismissed).toBe(true);
      expect((yield* inbox()).retainedRecordCount).toBe(0);
      expect((yield* requests.list({ view: "all" }, null)).requests).toEqual([]);
      expect((yield* requests.inspect(id, null)).request?.id).toBe(id);
      expect(yield* admissions.get(id)).toEqual(original);
      const restored = yield* requests.edit(
        {
          ...write,
          action: "restore",
          expectedRevision: dismissed.request.revision,
        },
        cli,
      );
      expect(restored.request.dismissed).toBe(false);
      expect((yield* requests.list({ view: "all" }, null)).requests.map((item) => item.id)).toEqual(
        [id],
      );
      const audit =
        yield* sql`SELECT action,actor_session_id FROM jones_request_management_edits ORDER BY revision`;
      expect(audit).toEqual(
        ["label", "dismiss", "restore"].map((action) => ({
          action,
          actor_session_id: cli.sessionId,
        })),
      );
      expect(yield* admissions.get(id)).toEqual(original);
    }).pipe(Effect.provide(layer)),
);

it.effect("does not dismiss holds, unconfirmed launches, or other nonterminal states", () =>
  Effect.gen(function* () {
    const requests = yield* Requests.RequestManagement;
    const sql = yield* SqlClient.SqlClient;
    for (const status of [
      "held",
      "evaluating",
      "passed",
      "approved",
      "dispatching",
      "dispatched",
    ] as const) {
      const id = yield* seed("peer-a", status, status);
      const current = yield* requests.ownerQuery(
        { receiverEnvironmentId, id, action: "inspect" },
        cli,
      );
      if (current.action !== "inspect") return yield* Effect.die("Expected an exact request");
      expect(current.request.canDismiss).toBe(false);
      const rejected = yield* requests
        .edit(
          {
            receiverEnvironmentId,
            id,
            action: "dismiss",
            expectedRevision: current.request.revision,
          },
          cli,
        )
        .pipe(Effect.flip);
      expect(rejected).toMatchObject({ code: "request_not_dismissible" });
      if (status === "dispatching" || status === "dispatched")
        expect(current.request.recoveryOutcome).toBe("unknown");
    }
    expect(yield* sql`SELECT * FROM jones_request_management_edits`).toEqual([]);
  }).pipe(Effect.provide(layer)),
);

it.effect("authorizes the owner CLI and denies remote writes on the actual HTTP routes", () =>
  Effect.gen(function* () {
    const requests = yield* Requests.RequestManagement;
    const id = yield* seed("peer-a", "route-denied", "denied");
    const auth = Layer.mock(EnvironmentAuth.EnvironmentAuth)({
      authenticateHttpRequest: (request) =>
        Effect.succeed(
          request.headers.authorization === "Bearer synthetic-local-cli"
            ? { ...cli, scopes: [...cli.scopes] }
            : { ...cli, subject: "mcp-client", scopes: AuthAdministrativeScopes },
        ),
    });
    const routes = HttpApiBuilder.layer(
      HttpApi.make("environment").add(EnvironmentHttpApi.groups.requestManagement),
    ).pipe(
      Layer.provide(requestManagementHttpApiLayer),
      Layer.provide(Layer.succeed(Requests.RequestManagement, requests)),
      Layer.provide(AuthHttp.layerAuthenticatedAuth),
      Layer.provide(auth),
      Layer.provide(HttpServer.layerServices),
    );
    const server = yield* Effect.acquireRelease(
      Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
      (server) => Effect.promise(() => server.dispose()),
    );
    const send = (path: string, payload: object, token = "synthetic-local-cli") =>
      Effect.promise(() =>
        server.handler(
          new Request(`http://localhost/api/jones/requests/${path}`, {
            method: "POST",
            headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
            body: JSON.stringify(payload),
          }),
        ),
      );
    const query = yield* send("query", { receiverEnvironmentId, id, action: "inspect" });
    expect(query.status).toBe(200);
    expect(query.headers.get("cache-control")).toBe("no-store");
    const body = yield* Effect.promise(() => query.json()).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(OwnerRequestsResult)),
    );
    expect(body).toMatchObject({ receiverEnvironmentId, request: { id, status: "denied" } });
    if (body.action !== "inspect") throw new Error("Expected an exact request result.");
    const target = { receiverEnvironmentId, id, expectedRevision: body.request.revision };
    const denied = yield* send("edit", { ...target, action: "dismiss" }, "synthetic-linked");
    expect(denied.status).toBe(403);
    const approve = yield* send("edit", { ...target, action: "approve" });
    expect(approve.status).toBe(400);
    const edit = yield* send("edit", { ...target, action: "dismiss" });
    expect(edit.status).toBe(200);
    expect(yield* Effect.promise(() => edit.json())).toMatchObject({
      request: { id, dismissed: true },
    });
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "registers local and linked MCP reads and blocks a linked caller from forwarding again",
  () =>
    Effect.gen(function* () {
      const requests = yield* Requests.RequestManagement;
      const a = yield* seed("peer-a", "mcp-a");
      const b = yield* seed("peer-b", "mcp-b");
      const childId = ThreadId.make("linked-born");
      const threads = Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: (threadId) =>
          Effect.succeed({
            ...liveThreadShell(threadId),
            ...(threadId === childId
              ? {
                  linkOrigin: {
                    sessionId: "peer-a",
                    label: "Synthetic peer",
                    access: "full-access" as const,
                  },
                }
              : {}),
          }),
      });
      const registration = McpHttp.toolkitRegistration(
        RequestsMcp.toolkit,
        RequestsMcp.handlers,
      ).pipe(
        Layer.provideMerge(McpHttp.layerInProcessMcpServer),
        Layer.provide(Layer.succeed(Requests.RequestManagement, requests)),
        Layer.provide(threads),
      );
      const client = McpSchema.McpServerClient.of({
        clientId: 1,
        protocolVersion: "2025-06-18",
        clientCapabilities: {},
        clientInfo: { name: "synthetic", version: "1" },
        initializePayload: {
          protocolVersion: "2025-06-18",
          capabilities: {},
          clientInfo: { name: "synthetic", version: "1" },
        },
        getClient: Effect.die("Unused"),
      });
      const linked: Invocation.McpInvocationScope = {
        environmentId: receiverEnvironmentId,
        thread: undefined,
        client: { sessionId: "peer-a", label: "Synthetic peer", access: "read-only", linked: true },
        requestNamespace: "synthetic-peer-a",
        capabilities: new Set(["orchestration"]),
        issuedAt: 0,
        linkedEnvironments: true,
      };
      const thread = (threadId: ThreadId): Invocation.McpInvocationScope => ({
        ...linked,
        client: undefined,
        thread: {
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          providerSessionId: "provider-session",
        },
      });
      const call = (
        name: string,
        args: Record<string, string>,
        scope: Invocation.McpInvocationScope,
      ) =>
        McpServer.McpServer.pipe(
          Effect.flatMap((server) => server.callTool({ name, arguments: args })),
          Effect.provideService(Invocation.McpInvocationContext, scope),
          Effect.provideService(McpSchema.McpServerClient, client),
        );
      const tools = Effect.gen(function* () {
        const own = yield* call("t3_requests_list", {}, thread(ThreadId.make("local-agent")));
        const fromPeer = yield* call(
          "t3_requests_list",
          { environmentId: receiverEnvironmentId },
          linked,
        );
        expect(JSON.stringify(own)).toContain(b);
        expect(JSON.stringify(fromPeer)).toContain(a);
        expect(JSON.stringify(fromPeer)).not.toContain(b);
        const exact = yield* call("t3_requests_read", { id: b }, linked);
        expect(JSON.stringify(exact)).toContain('"request":null');
        const secondHop = yield* call("t3_requests_list", { environmentId: "other" }, linked);
        expect(JSON.stringify(secondHop)).toContain("capability_denied");
        const fromChild = yield* call("t3_requests_list", {}, thread(childId));
        expect(JSON.stringify(fromChild)).toContain(a);
        expect(JSON.stringify(fromChild)).not.toContain(b);
        const viaChild = yield* call(
          "t3_requests_list",
          { environmentId: "other" },
          thread(childId),
        );
        expect(JSON.stringify(viaChild)).toContain("capability_denied");
      });
      yield* tools.pipe(Effect.provide(registration));
    }).pipe(Effect.provide(layer)),
);
