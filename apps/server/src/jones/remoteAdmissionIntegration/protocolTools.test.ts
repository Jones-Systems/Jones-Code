import { expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import type * as DTO from "@t3tools/contracts/jones/remoteAdmissionReview";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { McpSchema, McpServer, Tool } from "effect/ai";
import * as McpHttpServer from "../../mcp/McpHttpServer.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import { advertisedJsonSchema } from "../../mcp/advertisedJsonSchema.ts";
import { ReceiverInspection } from "./Inspection.ts";
import { handlers, ReceiverPeerProtocol, toolkit } from "./protocolTools.ts";

const scope: Invocation.McpInvocationScope = {
  environmentId: EnvironmentId.make("synthetic-receiver"),
  requestNamespace: "synthetic-peer-session",
  thread: undefined,
  client: {
    sessionId: "synthetic-peer-session",
    label: "Synthetic peer",
    access: "read-only",
    linked: true,
  },
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
};
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  clientCapabilities: {},
  clientInfo: { name: "receiver-protocol-test", version: "1" },
  protocolVersion: "2025-06-18",
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "receiver-protocol-test", version: "1" },
  },
  getClient: Effect.die("unused"),
});
const toolNames = [
  "t3_remote_admission_ack",
  "t3_remote_admission_attempt_register",
  "t3_remote_admission_capabilities_register",
  "t3_remote_admission_notice_ack",
  "t3_remote_admission_notice_register",
  "t3_remote_admission_observe",
  "t3_remote_admission_peer_status",
  "t3_remote_admission_recovery_status",
];
const inspectionResult: DTO.RemoteAdmissionRecoveryStatus = {
  receiverEnvironmentId: scope.environmentId,
  state: "unknown",
  reasonCode: "synthetic_unknown",
  stages: {
    creation: "unknown",
    availability: "unknown",
    initialMessage: "none",
    preparation: "unknown",
    providerProgress: "unknown",
  },
  confirmedThreadId: null,
  requestsRoute: "/requests",
};
const fixture = () => {
  const observations: Array<{
    scope: Invocation.McpInvocationScope;
    input: DTO.RemoteAdmissionObservationInput;
  }> = [];
  const acknowledgments: Array<{
    scope: Invocation.McpInvocationScope;
    input: DTO.RemoteAdmissionAckInput;
  }> = [];
  const inspections: Array<DTO.RemoteAdmissionRecoveryStatusInput> = [];
  return {
    observations,
    acknowledgments,
    inspections,
    layer: McpHttpServer.toolkitRegistration(toolkit, handlers).pipe(
      Layer.provideMerge(McpHttpServer.layerInProcessMcpServer),
      Layer.provide(
        Layer.mock(ReceiverPeerProtocol)({
          observe: (caller, input) =>
            Effect.sync(() => {
              observations.push({ scope: caller, input });
              return { admission: null, result: null };
            }),
          ack: (caller, input) =>
            Effect.sync(() => {
              acknowledgments.push({ scope: caller, input });
              return { acknowledged: true as const };
            }),
        }),
      ),
      Layer.provide(
        Layer.succeed(ReceiverInspection, {
          status: (input) =>
            Effect.sync(() => {
              inspections.push(input);
              return inspectionResult;
            }),
        }),
      ),
    ),
  };
};
const call = (name: string, args: Record<string, unknown>) =>
  Effect.gen(function* () {
    const server = yield* McpServer.McpServer;
    return yield* server
      .callTool({ name, arguments: args })
      .pipe(
        Effect.provideService(Invocation.McpInvocationContext, scope),
        Effect.provideService(McpSchema.McpServerClient, client),
      );
  });

it("all eight protocol tools advertise object roots before and after JSON codec derivation", () => {
  expect(Object.keys(toolkit.tools).toSorted()).toEqual(toolNames);
  for (const tool of Object.values(toolkit.tools)) {
    expect(Tool.getJsonSchema(tool)).toMatchObject({ type: "object" });
    expect(advertisedJsonSchema(tool)).toMatchObject({ type: "object" });
    expect(Tool.getJsonSchemaFromSchema(Schema.toCodecJson(tool.parametersSchema))).toMatchObject({
      type: "object",
    });
  }
});

it.effect(
  "production registration preserves both exact selector variants and ack dispositions",
  () => {
    const source = fixture();
    return Effect.gen(function* () {
      const server = yield* McpServer.McpServer;
      expect(server.tools.map(({ tool }) => tool.name).toSorted()).toEqual(toolNames);
      for (const { tool } of server.tools)
        expect(tool.inputSchema).toMatchObject({ type: "object" });
      const selectors: ReadonlyArray<DTO.RemoteAdmissionObservationInput> = [
        { clientRequestId: "original-request" },
        { attemptId: "retry-attempt", observationKey: "registered-observation" },
      ];
      for (const input of selectors) {
        const result = yield* call("t3_remote_admission_observe", input);
        expect(result.isError).toBe(false);
        expect(result.structuredContent).toEqual({ admission: null, result: null });
        for (const disposition of ["delivered", "suppressed"] as const) {
          const ack = yield* call("t3_remote_admission_ack", { ...input, disposition });
          expect(ack.isError).toBe(false);
          expect(ack.structuredContent).toEqual({ acknowledged: true });
        }
      }
      const inspectionSelectors: ReadonlyArray<DTO.RemoteAdmissionRecoveryStatusInput> = [
        { locator: "retained-locator" },
        { clientRequestId: "original-request", originThreadId: "exact-origin" },
      ];
      for (const input of inspectionSelectors) {
        const result = yield* call("t3_remote_admission_recovery_status", input);
        expect(result.isError).toBe(false);
        expect(result.structuredContent).toEqual(inspectionResult);
      }
      expect(source.observations).toEqual(selectors.map((input) => ({ scope, input })));
      expect(source.acknowledgments).toEqual(
        selectors.flatMap((input) =>
          ["delivered", "suppressed"].map((disposition) => ({
            scope,
            input: { ...input, disposition },
          })),
        ),
      );
      expect(source.inspections).toEqual(inspectionSelectors);
    }).pipe(Effect.provide(source.layer));
  },
);

const invalidObservations: ReadonlyArray<Record<string, unknown>> = [
  {},
  { attemptId: "attempt" },
  { observationKey: "key" },
  { clientRequestId: "original", attemptId: "attempt" },
  { clientRequestId: "original", observationKey: "key" },
  { clientRequestId: "original", attemptId: "attempt", observationKey: "key" },
  { clientRequestId: "original", attemptId: null },
  { clientRequestId: null, attemptId: "attempt", observationKey: "key" },
  { clientRequestId: "" },
  { clientRequestId: "x".repeat(257) },
  { clientRequestId: 1 },
  { attemptId: "attempt", observationKey: "" },
  { attemptId: "attempt", observationKey: null },
];
const invalidInspections: ReadonlyArray<Record<string, unknown>> = [
  {},
  { clientRequestId: "original" },
  { originThreadId: "origin" },
  { locator: "locator", clientRequestId: "original" },
  { locator: "locator", originThreadId: "origin" },
  { locator: "locator", clientRequestId: "original", originThreadId: "origin" },
  { locator: "locator", originThreadId: null },
  { locator: null, clientRequestId: "original", originThreadId: "origin" },
  { locator: "" },
  { locator: "x".repeat(257) },
  { locator: 1 },
  { clientRequestId: "original", originThreadId: "" },
  { clientRequestId: "original", originThreadId: null },
];
it.effect(
  "malformed selectors fail as InvalidParams before any protocol or inspection handler",
  () => {
    const source = fixture();
    return Effect.gen(function* () {
      const invalidAcks = [
        ...invalidObservations.map((input) => ({ ...input, disposition: "delivered" })),
        { clientRequestId: "original" },
        { attemptId: "attempt", observationKey: "key" },
        { clientRequestId: "original", disposition: "unknown" },
        { clientRequestId: "original", disposition: null },
      ];
      for (const [name, inputs] of [
        ["t3_remote_admission_observe", invalidObservations],
        ["t3_remote_admission_ack", invalidAcks],
        ["t3_remote_admission_recovery_status", invalidInspections],
      ] as const) {
        for (const input of inputs) {
          const result = yield* call(name, input).pipe(Effect.result);
          expect(result).toMatchObject({ _tag: "Failure", failure: { _tag: "InvalidParams" } });
        }
      }
      expect(source.observations).toEqual([]);
      expect(source.acknowledgments).toEqual([]);
      expect(source.inspections).toEqual([]);
    }).pipe(Effect.provide(source.layer));
  },
);
