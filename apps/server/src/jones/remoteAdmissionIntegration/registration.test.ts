import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, type LinkedRequestContext } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import * as ServerConfig from "../../config.ts";
import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import * as ProviderInstanceRegistry from "../../provider/ProviderInstanceRegistry.ts";
import * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import { IncomingLinkedRequestContext } from "../peerTransport/LinkedRequestContext.ts";
import migrateCore from "../persistence/Migrations/116_JonesRemoteAdmission.ts";
import migrateIntegration from "../persistence/Migrations/118_JonesRemoteAdmissionIntegration.ts";
import * as Intake from "./RemoteAdmissionIntake.ts";
import { decodeJson, encodeJson } from "./json.ts";
import * as Registration from "./registration.ts";

const scope: McpInvocationScope = {
  environmentId: EnvironmentId.make("receiver"),
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
  requestNamespace: "client:remote",
  thread: undefined,
  client: { sessionId: "remote", label: "Synthetic peer", access: "auto", linked: true },
};
const context: LinkedRequestContext = {
  schemaVersion: "t3.linked-request-context/v1",
  sentAt: "2026-10-26T12:00:00.000Z",
  project: { projectId: "sender-project", title: "Sender project" },
};
const params = {
  clientRequestId: "evaluation-key",
  prompt: "Review the original request body",
  advisory: { nestedParamMustRemain: true },
};
const policy = {
  enabled: true,
  revision: "synthetic-policy",
  modelSelection: { instanceId: ProviderInstanceId.make("synthetic"), model: "synthetic" },
};
const memory = Layer.effectDiscard(migrateCore.pipe(Effect.andThen(migrateIntegration))).pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const fixture = (messages: string[]) =>
  Registration.servicesLayer.pipe(
    Layer.provide(
      Layer.mock(TextGeneration.TextGeneration)({
        evaluateRemoteRequest: (input) =>
          Effect.sync(() => {
            expect(input.cwd).toBe("/synthetic/receiver");
            expect(input.modelSelection).toEqual(policy.modelSelection);
            messages.push(input.message);
            return { verdict: "hold" as const, category: "risky" as const, reason: "Owner review" };
          }),
      }),
    ),
    Layer.provide(
      Layer.mock(ServerConfig.ServerConfig)({
        cwd: "/synthetic/receiver",
      } as ServerConfig.ServerConfig["Service"]),
    ),
    Layer.provide(Layer.mock(EnvironmentAuth.EnvironmentAuth)({})),
    Layer.provide(Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({})),
    Layer.provideMerge(memory),
    Layer.fresh,
  );

it.effect(
  "B1-7: registered evaluator input stays byte-identical and removes only payload.advisory",
  () => {
    const run = (withAdvisory: boolean) => {
      const messages: string[] = [];
      return Effect.gen(function* () {
        const intake = yield* Intake.RemoteAdmissionIntake;
        const sql = yield* SqlClient.SqlClient;
        // Synthetic receiver policy, isolated in this test's in-memory database.
        yield* sql`INSERT INTO jones_remote_admission_policy (singleton, policy_json) VALUES (1, ${encodeJson(policy)})`;
        intake.register(
          "t3_thread_launch",
          () => Effect.die("held"),
          () => Effect.succeed({ advisory: "Nested target data stays bound" }),
        );
        const pending = yield* intake
          .invoke("t3_thread_launch", params, scope)
          .pipe(
            Effect.provideService(
              IncomingLinkedRequestContext,
              withAdvisory ? { context, linkSessionId: "remote" } : undefined,
            ),
            Effect.asVoid,
            Effect.flip,
          );
        expect(pending.code).toBe("remote_admission_pending");
        expect(messages).toHaveLength(1);
        const record = (yield* intake.snapshot()).pending[0]!;
        const payload = record.envelope.payload as Record<string, unknown>;
        expect(payload.params).toEqual(params);
        expect(payload.target).toEqual({ advisory: "Nested target data stays bound" });
        expect(Object.hasOwn(payload, "advisory")).toBe(withAdvisory);
        if (withAdvisory) {
          expect(payload.advisory).toEqual({
            schemaVersion: "t3.remote-admission-advisory/v1",
            requestContext: context,
          });
        } else {
          expect(messages[0]).toBe(
            encodeJson({
              operation: record.envelope.operation,
              payload: record.envelope.payload,
              target: record.envelope.target,
            }),
          );
        }
        return messages[0]!;
      }).pipe(Effect.provide(fixture(messages)));
    };
    return Effect.gen(function* () {
      const legacyMessage = yield* run(false);
      const advisoryMessage = yield* run(true);
      expect(advisoryMessage).toBe(legacyMessage);
      const evaluated = decodeJson(advisoryMessage) as {
        payload: { params: unknown; target: { advisory: string } };
      };
      expect(evaluated.payload.params).toEqual(params);
      expect(evaluated.payload.target.advisory).toBe("Nested target data stays bound");
      expect(Object.hasOwn(evaluated.payload, "advisory")).toBe(false);
    });
  },
);
