import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import type { WsRpcProtocolClient } from "../../../rpc/protocol.ts";
import type { PersonalConnection, PersonalWorkstreamsTransport } from "./controller.ts";

/** Resolve only the captured generation. Returning a replacement session here would
 * attribute a new server response to an old generation. The connection supervisor
 * owns reconnects; this adapter makes one attempt and never replays mutations. */
export function createPersonalWorkstreamsRpcTransport(
  resolveClient: (connection: PersonalConnection) => WsRpcProtocolClient | undefined,
): PersonalWorkstreamsTransport {
  const client = (connection: PersonalConnection) => {
    const resolved = resolveClient(connection);
    if (!resolved) throw new Error("Personal Workstreams session unavailable");
    return resolved;
  };
  const run = async <A, E>(effect: Effect.Effect<A, E>): Promise<A> => {
    // Preserve domain failures as values until the Promise boundary, rather than
    // hiding precondition_failed inside a FiberFailure wrapper.
    const result = await Effect.runPromise(
      Effect.match(effect, {
        onSuccess: (value) => ({ ok: true as const, value }),
        onFailure: (error) => ({ ok: false as const, error }),
      }),
    );
    if (!result.ok) throw result.error;
    return result.value;
  };
  return {
    subscribe: (connection, onSnapshot, onError) => {
      const abort = new AbortController();
      const stream = client(connection)["workstreamsPersonal.subscribe"]({});
      void Effect.runPromise(
        Stream.runForEach(stream, (snapshot) => Effect.sync(() => onSnapshot(snapshot))),
        { signal: abort.signal },
      ).catch((error) => {
        if (!abort.signal.aborted) onError(error);
      });
      return () => abort.abort();
    },
    put: (connection, workstream) =>
      run(client(connection)["workstreamsPersonal.put"]({ workstream })),
    assign: (connection, input) => run(client(connection)["workstreamsPersonal.assign"](input)),
  };
}
