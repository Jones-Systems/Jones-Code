import type { ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";

export interface Change {
  readonly threadIds: ReadonlyArray<ThreadId>;
  readonly sequence: number;
}
export class DependencyChanges extends Context.Service<
  DependencyChanges,
  {
    readonly generation: (threadId: ThreadId) => number;
    readonly beginSend: (id: string) => void;
    readonly endSend: (id: string) => void;
    readonly isSending: (id: string) => boolean;
    readonly subscribe: ReturnType<typeof PubSub.subscribe<Change>>;
    readonly publish: (change: Change) => Effect.Effect<void>;
  }
>()("t3/jones/threadResultDependencies/DependencyChanges") {}
export const layer = Layer.effect(
  DependencyChanges,
  Effect.gen(function* () {
    const sending = new Map<string, number>();
    const generations = new Map<ThreadId, number>();
    const changes = yield* PubSub.unbounded<Change>();
    return DependencyChanges.of({
      generation: (id) => generations.get(id) ?? 0,
      beginSend: (id) => {
        sending.set(id, (sending.get(id) ?? 0) + 1);
      },
      endSend: (id) => {
        const remaining = (sending.get(id) ?? 1) - 1;
        if (remaining === 0) sending.delete(id);
        else sending.set(id, remaining);
      },
      isSending: (id) => sending.has(id),
      subscribe: PubSub.subscribe(changes),
      publish: (change) =>
        Effect.sync(() => {
          for (const id of change.threadIds) generations.set(id, (generations.get(id) ?? 0) + 1);
        }).pipe(Effect.andThen(PubSub.publish(changes, change)), Effect.asVoid),
    });
  }),
);
