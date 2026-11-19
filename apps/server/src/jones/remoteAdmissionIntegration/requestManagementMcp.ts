import { OrchestratorMcpEnvironmentTarget, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as DTO from "@t3tools/contracts/jones/requestManagement";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import type { SqlError } from "effect/sql/SqlError";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import * as Access from "../../mcp/McpToolAccess.ts";
import { assertLiveCaller, readCaller } from "../../mcp/threadAccess.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as Requests from "./RequestManagement.ts";

const denied = () =>
  new OrchestratorMcpFailure({
    code: "capability_denied",
    message: "Requests are unavailable to this caller.",
  });
const caller = Effect.fn("RequestManagement.caller")(function* () {
  const access = yield* readCaller();
  yield* assertLiveCaller(access);
  return access;
});
const localForwarder = () =>
  caller().pipe(
    Effect.flatMap(({ scope, caller }) =>
      scope.thread && !scope.client?.linked && !caller?.linkOrigin
        ? Effect.void
        : Effect.fail(denied()),
    ),
  );
const reader = Effect.fn("RequestManagement.reader")(function* () {
  const { scope, caller: thread } = yield* caller();
  if (thread?.linkOrigin) {
    if (scope.linkedEnvironments === false) return yield* denied();
    return { sessionId: thread.linkOrigin.sessionId };
  }
  if (scope.thread && thread && !scope.client) return null;
  if (scope.client?.linked && !scope.thread && scope.linkedEnvironments !== false)
    return { sessionId: scope.client.sessionId };
  return yield* denied();
});
const mcpError = (cause: DTO.RequestManagementError | SqlError) =>
  "code" in cause && cause.code === "requests_owner_required"
    ? denied()
    : new OrchestratorMcpFailure({
        code: "orchestration_error",
        message: "code" in cause ? cause.message : "Requests are unavailable on this environment.",
      });
const dependencies = [
  Requests.RequestManagement,
  Invocation.McpInvocationContext,
  ThreadManagement.ThreadManagementService,
];
const List = Tool.make("t3_requests_list", {
  description:
    "List redacted pending or retained Requests here or on a linked receiver. A linked session sees only its own admissions.",
  parameters: Schema.Struct({
    ...DTO.RequestListFields,
    view: Schema.optionalKey(Schema.Literals(["pending", "all"])),
    environmentId: OrchestratorMcpEnvironmentTarget,
  }),
  success: DTO.RequestListResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
}).annotate(Tool.Readonly, true);
const Read = Tool.make("t3_requests_read", {
  description:
    "Inspect one retained Request by exact receiving environment and request ID. Only redacted metadata is returned.",
  parameters: Schema.Struct({ id: DTO.RequestId, environmentId: OrchestratorMcpEnvironmentTarget }),
  success: DTO.RequestInspectResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
}).annotate(Tool.Readonly, true);

export const toolkit = Toolkit.make(List, Read);
export const handlers = Access.toLayer(toolkit, {
  t3_requests_list: Access.reads(
    (input) =>
      Effect.gen(function* () {
        const requests = yield* Requests.RequestManagement;
        return yield* requests.list(input, yield* reader()).pipe(Effect.mapError(mcpError));
      }),
    localForwarder,
  ),
  t3_requests_read: Access.reads(
    ({ id }) =>
      Effect.gen(function* () {
        const requests = yield* Requests.RequestManagement;
        return yield* requests.inspect(id, yield* reader()).pipe(Effect.mapError(mcpError));
      }),
    localForwarder,
  ),
});
