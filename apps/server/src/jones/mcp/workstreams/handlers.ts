import * as AgentWorkstreams from "../../workstreams/personal/agentService.ts";
import * as Effect from "effect/Effect";
import * as P from "@t3tools/contracts/jones/workstreamsPersonal";
import * as PersonalWorkstreams from "../../workstreams/personal/service.ts";
import * as McpToolAccess from "../../../mcp/McpToolAccess.ts";
import { requireMcpCapability } from "../../../mcp/McpInvocationContext.ts";
import { WorkstreamToolkit } from "./tools.ts";

export const handlers = McpToolAccess.toLayer(
  WorkstreamToolkit,
  Effect.gen(function* () {
    const service = yield* PersonalWorkstreams.PersonalWorkstreams;
    const agent = yield* AgentWorkstreams.AgentWorkstreams;
    const thread = Effect.fn("PersonalWorkstreamsMcp.thread")(function* (
      id?: import("@t3tools/contracts").ThreadId,
    ) {
      const scope = yield* requireMcpCapability("orchestration");
      const threadId = id ?? scope.thread?.threadId;
      if (threadId === undefined)
        return yield* new P.WorkstreamsPersonalError({ code: "invalid_request" });
      return threadId;
    });
    const result = (
      id: import("@t3tools/contracts").ThreadId,
      a: P.PersonalWorkstreamAssignment,
      w: P.PersonalWorkstream | null,
    ) => ({
      threadId: id,
      workstreamId: a.workstreamId,
      workstreamName: w?.name ?? null,
      revision: a.revision,
    });
    return {
      t3_workstreams_list: McpToolAccess.reads(() =>
        Effect.gen(function* () {
          yield* requireMcpCapability("orchestration");
          const s = yield* service.snapshot();
          return {
            environmentId: s.environmentId,
            sequence: s.sequence,
            workstreams: s.workstreams
              .filter((w) => w.deletedAt === null)
              .map((w) => ({
                id: w.id,
                name: w.name,
                lifecycle: w.lifecycle,
                color: w.color,
                orderKey: w.orderKey,
                threadCount: s.assignments.filter((a) => a.workstreamId === w.id).length,
              })),
          };
        }),
      ),
      t3_workstream_create: McpToolAccess.writes((input) => agent.create(input)),
      t3_workstream_update: McpToolAccess.writes((input) => agent.update(input)),
      t3_thread_workstream_read: McpToolAccess.reads((input) =>
        Effect.gen(function* () {
          const id = yield* thread(input.threadId);
          const r = yield* service.readThread(id);
          return result(id, r.assignment, r.workstream);
        }),
      ),
      t3_thread_workstream_set: McpToolAccess.writesThreads(
        (input) => (input.threadId === undefined ? [] : [input.threadId]),
        (input) =>
          Effect.gen(function* () {
            const id = yield* thread(input.threadId);
            const r = yield* agent.set({ ...input, threadId: id });
            return { ...result(id, r.assignment, r.workstream), outcome: r.outcome };
          }),
      ),
    };
  }),
);
