import * as Layer from "effect/Layer";
import * as WorkstreamOperations from "../../workstreams/operations/service.ts";
import * as WorkstreamGateway from "../../../workstreams/WorkstreamGateway.ts";
import { OrchestratorMcpFailure, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as McpToolAccess from "../../../mcp/McpToolAccess.ts";
import { requireMcpCapability } from "../../../mcp/McpInvocationContext.ts";
import { WorkstreamToolkit, WorkstreamToolsBackend } from "./tools.ts";

export const handlers = McpToolAccess.toLayer(
  WorkstreamToolkit,
  Effect.gen(function* () {
    // Capture the application-owned adapter once; request scope stays per invocation.
    const backend = yield* Effect.serviceOption(WorkstreamToolsBackend);
    const available = Effect.gen(function* () {
      yield* requireMcpCapability("orchestration");
      if (Option.isNone(backend))
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message:
            "Workstream operations are unsupported by this server. Update the server before retrying; retain your operation ID.",
        });
      return backend.value;
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

/** Provided once by McpHttpServer; WorkstreamOperations itself is app-owned and
 * shared with HTTP. Root integrates the final J-A service before qualification. */
export const backendLayer = Layer.effect(
  WorkstreamToolsBackend,
  Effect.gen(function* () {
    const operations = yield* Effect.serviceOption(WorkstreamOperations.WorkstreamOperations);
    const unsupported = new OrchestratorMcpFailure({
      code: "capability_denied",
      message:
        "Workstream operations are unsupported by this server. Update the server before retrying; retain your operation ID.",
    });
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
        Option.isNone(operations)
          ? Effect.fail(unsupported)
          : operations.value.submit(input).pipe(
              Effect.mapError((error) => {
                if (
                  "code" in error &&
                  (error.code === "operation_id_conflict" ||
                    error.code === "thread_operation_in_progress")
                ) {
                  return {
                    code: error.code,
                    ...("open_operation_id" in error && typeof error.open_operation_id === "string"
                      ? { open_operation_id: error.open_operation_id }
                      : {}),
                  };
                }
                return unavailable();
              }),
            ),
      read: (input) =>
        Option.isNone(operations) ? Effect.fail(unsupported) : operations.value.read(input),
      setupStatus: () =>
        Option.isNone(operations) ? Effect.fail(unsupported) : operations.value.setupStatus(),
    });
  }),
);
