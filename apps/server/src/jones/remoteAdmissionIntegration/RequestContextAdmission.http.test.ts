import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, type LinkedRequestContext } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import * as ProviderInstanceRegistry from "../../provider/ProviderInstanceRegistry.ts";
import {
  LINKED_MESSAGE_ORIGIN_HEADER,
  encodeLinkedMessageOrigin,
} from "../peerTransport/LinkedMessageOrigin.ts";
import {
  LINKED_REQUEST_CONTEXT_HEADER,
  encodeLinkedRequestContext,
} from "../peerTransport/LinkedRequestContext.ts";
import {
  LINKED_MCP_PROBE_TOOL,
  serveLinkedMcpProbe,
} from "../peerTransport/LinkedMcpProbe.testkit.ts";
import migrateCore from "../persistence/Migrations/116_JonesRemoteAdmission.ts";
import migrateIntegration from "../persistence/Migrations/118_JonesRemoteAdmissionIntegration.ts";
import * as Admission from "../remoteAdmission/RemoteAdmission.ts";
import * as Intake from "./RemoteAdmissionIntake.ts";
import { encodeJson } from "./json.ts";

const scope: McpInvocationScope = {
  environmentId: EnvironmentId.make("receiver"),
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
  requestNamespace: "client:remote",
  thread: undefined,
  client: { sessionId: "remote", label: "Synthetic peer", access: "auto", linked: true },
};
const firstContext: LinkedRequestContext = {
  schemaVersion: "t3.linked-request-context/v1",
  sentAt: "2026-10-26T12:00:00.000Z",
  thread: { threadId: "sender-thread", title: "Draft" },
  project: { projectId: "sender-project", title: "Sender project" },
};
const headers = (title: string, context: LinkedRequestContext) => ({
  [LINKED_MESSAGE_ORIGIN_HEADER]: encodeLinkedMessageOrigin({
    schemaVersion: "t3.linked-message-origin/v1",
    environmentId: "sender",
    thread: { threadId: "sender-thread", title },
  }),
  [LINKED_REQUEST_CONTEXT_HEADER]: encodeLinkedRequestContext(context),
});
const memory = Layer.effectDiscard(migrateCore.pipe(Effect.andThen(migrateIntegration))).pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);

it.effect(
  "B1-11: real linked MCP intake stores context and reuses bytes after title/time changes",
  () => {
    let evaluations = 0;
    const fixture = Intake.layer.pipe(
      Layer.provide(
        Admission.layerWithEvaluator(() =>
          Effect.sync(() => {
            evaluations++;
            return { verdict: "hold", category: "risky", reason: "Owner review" };
          }),
        ),
      ),
      Layer.provide(Layer.mock(EnvironmentAuth.EnvironmentAuth)({})),
      Layer.provide(Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({})),
      Layer.provideMerge(memory),
    );
    return Effect.scoped(
      Effect.gen(function* () {
        const intake = yield* Intake.RemoteAdmissionIntake;
        const sql = yield* SqlClient.SqlClient;
        const policy = {
          enabled: true,
          revision: "synthetic-policy",
          modelSelection: { instanceId: ProviderInstanceId.make("synthetic"), model: "synthetic" },
        };
        yield* sql`INSERT INTO jones_remote_admission_policy (singleton, policy_json) VALUES (1, ${encodeJson(policy)})`;
        const probe = yield* serveLinkedMcpProbe(
          Layer.succeed(Intake.RemoteAdmissionIntake, intake),
          {
            "synthetic-token": scope,
          },
        );
        const session = yield* probe.open("synthetic-token");
        const params = { clientRequestId: "http-request", message: "Clear original request body" };
        const first = yield* probe.call(session, {
          params,
          headers: headers("Draft", firstContext),
        });
        expect(first.status).toBe(200);
        expect(first.text).toContain("remote_admission_pending");
        const record = (yield* intake.snapshot()).pending[0]!;
        expect(record.envelope.operation).toBe(LINKED_MCP_PROBE_TOOL);
        expect(record.envelope.payload).toMatchObject({
          params,
          advisory: {
            schemaVersion: "t3.remote-admission-advisory/v1",
            requestContext: firstContext,
          },
          invocation: {
            client: {
              remoteOrigin: {
                linkSessionId: "remote",
                thread: { threadId: "sender-thread", title: "Draft" },
              },
            },
          },
        });
        const before =
          yield* sql`SELECT id, envelope_json, request_digest, binding_digest FROM jones_remote_admissions`;
        const retry = yield* probe.call(session, {
          params,
          headers: headers("Final", {
            ...firstContext,
            sentAt: "2026-10-26T13:00:00.000Z",
            thread: { threadId: "sender-thread", title: "Final" },
          }),
        });
        expect(retry.status).toBe(200);
        expect(retry.text).toContain("remote_admission_pending");
        expect(retry.text).toContain(record.id);
        const after =
          yield* sql`SELECT id, envelope_json, request_digest, binding_digest FROM jones_remote_admissions`;
        expect(after).toEqual(before);
        expect((yield* intake.snapshot()).pending.map((pending) => pending.id)).toEqual([
          record.id,
        ]);
        expect(evaluations).toBe(1);
      }).pipe(Effect.provide(fixture)),
    );
  },
);
