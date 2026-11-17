import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import type * as P from "@t3tools/contracts/jones/workstreamsPersonal";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpEffect from "effect/http/HttpEffect";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as SqlClient from "effect/sql/SqlClient";
import { failEnvironmentInternal, requireEnvironmentScope } from "../../../auth/http.ts";
import * as AgentWorkstreams from "./agentService.ts";

const noStore = HttpEffect.appendPreResponseHandler((_request, response) =>
  Effect.succeed(HttpServerResponse.setHeader(response, "cache-control", "no-store")),
);
const internal = (error: P.WorkstreamsPersonalError) =>
  failEnvironmentInternal("internal_error", error);

// Bearer clients such as the external work queue; browser clients keep the RPC subscription.
export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "workstreamsPersonal",
  Effect.fnUntraced(function* (handlers) {
    const agent = yield* AgentWorkstreams.AgentWorkstreams;
    const sql = yield* SqlClient.SqlClient;
    return handlers
      .handle(
        "list",
        Effect.fn("workstreamsPersonal.http.list")(function* () {
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          yield* noStore;
          const list = yield* agent
            .list()
            .pipe(Effect.catchTags({ WorkstreamsPersonalError: internal }));
          return {
            schema: "jones.personal-workstreams-list/v1" as const,
            environmentId: list.environmentId,
            sequence: list.sequence,
            observedAt: list.observedAt,
            workstreams: list.workstreams.map((w) => ({
              id: w.id,
              name: w.name,
              lifecycle: w.lifecycle,
              color: w.color,
              orderKey: w.orderKey,
              rev: w.rev,
              updatedAt: w.updatedAt,
            })),
          };
        }),
      )
      .handle(
        "assign",
        Effect.fn("workstreamsPersonal.http.assign")(function* ({ payload }) {
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          yield* noStore;
          yield* Effect.annotateCurrentSpan({
            "workstreams.operation_id": payload.operationId,
            "workstreams.origin.kind": payload.origin?.kind ?? "unspecified",
            ...(payload.origin?.requestId === undefined
              ? {}
              : { "workstreams.origin.request_id": payload.origin.requestId }),
          });
          const result = yield* agent
            .setExpecting({
              threadId: payload.threadId,
              workstreamId: payload.workstreamId,
              expected: payload.expected,
            })
            .pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
              Effect.catchTags({ WorkstreamsPersonalError: internal }),
            );
          return {
            schema: "jones.personal-workstream-assign-result/v1" as const,
            operationId: payload.operationId,
            environmentId: result.environmentId,
            outcome: result.outcome,
            assignment: result.assignment,
            workstream:
              result.workstream === null
                ? null
                : {
                    id: result.workstream.id,
                    name: result.workstream.name,
                    rev: result.workstream.rev,
                  },
            sequence: result.sequence,
            observedAt: result.observedAt,
          };
        }),
      );
  }),
);
