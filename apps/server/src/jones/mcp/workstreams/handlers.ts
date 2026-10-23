import * as Layer from "effect/Layer";
import * as WorkstreamOperations from "../../workstreams/operations/service.ts";
import * as WorkstreamGateway from "../../../workstreams/WorkstreamGateway.ts";
import { OrchestratorMcpFailure, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as McpToolAccess from "../../../mcp/McpToolAccess.ts";
import { requireMcpCapability } from "../../../mcp/McpInvocationContext.ts";
import { WorkstreamToolkit, WorkstreamToolsBackend } from "./tools.ts";

export const handlers = McpToolAccess.toLayer(
  WorkstreamToolkit,
  Effect.gen(function* () {
    // Capture the application-owned adapter once; request scope stays per invocation.
    const backend = yield* WorkstreamToolsBackend;
    const available = Effect.gen(function* () {
      yield* requireMcpCapability("orchestration");
      return backend;
    });
    return {
      t3_workstreams_list: McpToolAccess.reads((input) =>
        Effect.flatMap(available, (service) => service.list(input)),
      ),
      t3_workstream_create: McpToolAccess.writes((input) =>
        Effect.flatMap(available, (service) => service.submit(input)),
      ),
      t3_thread_workstream_set: McpToolAccess.writesThreads(
        (input) => [ThreadId.make(input.request.thread.thread_id)],
        (input) => Effect.flatMap(available, (service) => service.submit(input)),
      ),
      t3_thread_workstream_read: McpToolAccess.reads((input) =>
        Effect.gen(function* () {
          const scope = yield* requireMcpCapability("orchestration");
          if (input.thread.environment_id !== scope.environmentId)
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "foreign_thread: use the server that owns this thread.",
            });
          return yield* (yield* available).read(input);
        }),
      ),
      t3_workstream_operation_status: McpToolAccess.reads((input) =>
        Effect.flatMap(available, (service) =>
          service.read(
            input.operation_id === undefined
              ? { open: true }
              : { operation_id: input.operation_id },
          ),
        ),
      ),
      t3_workstreams_status: McpToolAccess.reads(() =>
        Effect.flatMap(available, (service) => service.setupStatus()),
      ),
    };
  }),
);

/** Capture the application-owned service shared with HTTP; never construct a second cache. */
export const backendLayer = Layer.effect(
  WorkstreamToolsBackend,
  Effect.gen(function* () {
    const operations = yield* WorkstreamOperations.WorkstreamOperations;
    const gateway = yield* WorkstreamGateway.WorkstreamGateway;
    const unavailable = () =>
      new OrchestratorMcpFailure({
        code: "orchestration_error",
        message:
          "Workstreams are unavailable. Read t3_workstreams_status for setup and repair steps; retain the operation ID on retry.",
      });
    return WorkstreamToolsBackend.of({
      list: (input) => gateway.readMetadata(input).pipe(Effect.mapError(unavailable)),
      submit: (input) =>
        operations.submit(input).pipe(
          Effect.catchTags({
            OperationConflict: (error) =>
              Effect.fail({
                code: error.code,
                ...(error.open_operation_id === undefined
                  ? {}
                  : { open_operation_id: error.open_operation_id }),
              }),
            OperationInternalError: () => Effect.fail(unavailable()),
          }),
        ),
      read: (input) => operations.read(input),
      setupStatus: () => operations.setupStatus(),
    });
  }),
);
