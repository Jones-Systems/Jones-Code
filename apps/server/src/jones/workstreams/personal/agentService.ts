import * as P from "@t3tools/contracts/jones/workstreamsPersonal";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type { ThreadId } from "@t3tools/contracts";
import * as PersonalWorkstreams from "./service.ts";

export class AgentWorkstreams extends Context.Service<
  AgentWorkstreams,
  {
    readonly create: (input: {
      id: string;
      name: string;
      lifecycle?: P.PersonalWorkstreamLifecycle;
      color?: string | null;
    }) => Effect.Effect<P.PutResult, P.WorkstreamsPersonalError>;
    readonly update: (
      input: P.PersonalWorkstreamPatch & { id: string; delete?: boolean },
    ) => Effect.Effect<P.PutResult, P.WorkstreamsPersonalError>;
    readonly set: (input: {
      threadId: ThreadId;
      workstreamId: string | null;
      expectedRevision: number;
      workstream?: P.PersonalWorkstream;
    }) => Effect.Effect<P.AssignResult, P.WorkstreamsPersonalError>;
  }
>()("t3/jones/workstreams/personal/agentService/AgentWorkstreams") {}
const make = Effect.gen(function* () {
  const service = yield* PersonalWorkstreams.PersonalWorkstreams;
  const find = Effect.fn("AgentWorkstreams.find")(function* (id: string) {
    const w = (yield* service.readDefinitions([id]))[0];
    if (!w) return yield* new P.WorkstreamsPersonalError({ code: "workstream_not_found" });
    if (w.deletedAt !== null)
      return yield* new P.WorkstreamsPersonalError({ code: "workstream_deleted" });
    return w;
  });
  return AgentWorkstreams.of({
    create: service.create,
    update: Effect.fn("AgentWorkstreams.update")(function* (input) {
      const old = yield* find(input.id);
      const s = yield* service.snapshot();
      const now = yield* DateTime.now.pipe(Effect.map(DateTime.formatIso));
      const candidate = yield* Effect.try({
        try: () =>
          input.delete === true
            ? P.deletePersonalWorkstream(old, now, `env:${s.environmentId}`)
            : P.editPersonalWorkstream(old, input, now, `env:${s.environmentId}`),
        catch: () => new P.WorkstreamsPersonalError({ code: "invalid_request" }),
      });
      return yield* service.put(candidate);
    }),
    set: Effect.fn("AgentWorkstreams.set")(function* (input) {
      const w =
        input.workstreamId === null
          ? null
          : (input.workstream ?? (yield* find(input.workstreamId)));
      if (w !== null && w.id !== input.workstreamId)
        return yield* new P.WorkstreamsPersonalError({ code: "invalid_request" });
      return yield* service.assign({
        threadId: input.threadId,
        workstream: w,
        expectedRevision: input.expectedRevision,
      });
    }),
  });
});
export const layer = Layer.effect(AgentWorkstreams, make);
