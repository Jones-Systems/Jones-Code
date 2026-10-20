import {
  CommandId,
  NodeId,
  RuntimeRequestId,
  TurnItemId,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import { OrchestrationV2UserInputQuestion } from "@t3tools/contracts";
import type {
  UserResponseRequestInput,
  UserResponseRequestResult,
} from "@t3tools/contracts/jones/userResponseRequest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import type { McpThreadInvocationScope } from "../../../mcp/McpInvocationContext.ts";
import { dispatchFailure, unavailable } from "../../../mcp/threadAccess.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";

export class UserResponseRequestMcpService extends Context.Service<
  UserResponseRequestMcpService,
  {
    readonly request: (
      scope: McpThreadInvocationScope,
      input: UserResponseRequestInput,
    ) => Effect.Effect<UserResponseRequestResult, OrchestratorMcpFailure>;
  }
>()("t3/jones/mcp/userResponseRequest/UserResponseRequestMcpService") {}

const encodeKeyPart = Schema.encodeSync(Schema.fromJsonString(Schema.String));
const encodeQuestions = Schema.encodeSync(
  Schema.fromJsonString(Schema.Array(OrchestrationV2UserInputQuestion)),
);
const refused = (message: string) =>
  new OrchestratorMcpFailure({ code: "invalid_request", message });
const make = Effect.gen(function* () {
  const threads = yield* ThreadManagementService;
  const serial = yield* Semaphore.make(1);
  return UserResponseRequestMcpService.of({
    request: (scope, input) =>
      serial.withPermits(1)(
        Effect.gen(function* () {
          const key = `${encodeURIComponent(encodeKeyPart(scope.requestNamespace))}:${encodeURIComponent(encodeKeyPart(input.clientRequestId))}`;
          const commandId = CommandId.make(`user-response:${key}`);
          const requestId = RuntimeRequestId.make(`user-response:${key}`);
          const requestNodeId = NodeId.make(`user-response:${key}`);
          const turnItemId = TurnItemId.make(`user-response:${key}`);
          const question: OrchestrationV2UserInputQuestion = {
            id: "response",
            header: "Action needed",
            question: input.instruction,
            options: (input.choices ?? []).map((label) => ({ label, description: label })),
            allowCustomAnswer: true,
            required: true,
          };
          const read = () =>
            threads
              .getThreadRecords(scope.thread.threadId, [
                "runs",
                "nodes",
                "turnItems",
                "runtimeRequests",
                "providerThreads",
              ])
              .pipe(Effect.mapError(unavailable));
          const before = yield* read();
          const run = before.runs.findLast((candidate) =>
            ["starting", "running", "waiting"].includes(candidate.status),
          );
          const providerThread = before.providerThreads.find(
            (candidate) => candidate.id === run?.providerThreadId,
          );
          if (
            before.thread.deletedAt !== null ||
            before.thread.archivedAt !== null ||
            run === undefined ||
            run.rootNodeId === null ||
            run.providerInstanceId !== scope.thread.providerInstanceId ||
            providerThread?.providerSessionId !== scope.thread.providerSessionId
          ) {
            return yield* refused("The calling provider no longer owns an active run.");
          }
          const existing = before.turnItems.find((item) => item.id === turnItemId);
          if (
            existing !== undefined &&
            (existing.type !== "user_input_request" ||
              existing.requestId !== requestId ||
              existing.responseMode !== "message" ||
              encodeQuestions(existing.questions) !== encodeQuestions([question]))
          ) {
            return yield* refused(
              "This clientRequestId already belongs to a different response request.",
            );
          }
          const hadReceipt = yield* threads.hasReceipt(commandId);
          yield* threads
            .dispatch({
              type: "user_input_request.record",
              commandId,
              threadId: scope.thread.threadId,
              runId: run.id,
              rootNodeId: run.rootNodeId,
              providerInstanceId: scope.thread.providerInstanceId,
              requestId,
              requestNodeId,
              turnItemId,
              question,
            })
            .pipe(Effect.mapError(dispatchFailure));
          const after = yield* read();
          const item = after.turnItems.find((candidate) => candidate.id === turnItemId);
          const request = after.runtimeRequests.find((candidate) => candidate.id === requestId);
          if (
            item?.type !== "user_input_request" ||
            item.requestId !== requestId ||
            item.responseMode !== "message" ||
            encodeQuestions(item.questions) !== encodeQuestions([question]) ||
            request?.kind !== "user_input" ||
            request.responseCapability.type !== "message" ||
            request.nodeId !== requestNodeId
          ) {
            return yield* refused(
              "The stored response request does not match this clientRequestId.",
            );
          }
          return { requestId, status: request.status, created: !hadReceipt };
        }),
      ),
  });
});
export const layer = Layer.effect(UserResponseRequestMcpService, make);
