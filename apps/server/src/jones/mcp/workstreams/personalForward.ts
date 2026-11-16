import * as Effect from "effect/Effect";
import * as PersonalWorkstreams from "../../workstreams/personal/service.ts";

/** Routing remains the transport's responsibility; this only carries source metadata. */
export const enrichAssignment = Effect.fn("PersonalWorkstreamsMcp.enrichAssignment")(function* (
  params: Readonly<Record<string, unknown>>,
) {
  if (params.workstreamId === null || typeof params.workstreamId !== "string") return params;
  const service = yield* PersonalWorkstreams.PersonalWorkstreams;
  const local = (yield* service.readDefinitions([params.workstreamId]))[0];
  if (local !== undefined) return { ...params, workstream: local };
  // A carried definition is validated by the receiver. If absent, the receiver may know it.
  return params;
});
