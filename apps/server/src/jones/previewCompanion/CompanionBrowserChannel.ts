import type { PreviewAutomationRuntimeIdentity } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { makeTabCdpEndpoint, type DesktopTabKey } from "../../preview/DesktopBrowserChannel.ts";
import * as CompanionHostRegistry from "./CompanionHostRegistry.ts";

export class CompanionBrowserChannel extends Context.Service<
  CompanionBrowserChannel,
  {
    readonly availableFor: (hostId: string) => Effect.Effect<boolean>;
    readonly mount: (
      hostId: string,
      key: DesktopTabKey,
    ) => Effect.Effect<void, CompanionHostRegistry.CompanionHostUnavailable>;
    readonly unmount: (hostId: string, key: DesktopTabKey) => Effect.Effect<void>;
    readonly awaitAttached: (key: DesktopTabKey, timeout: Duration.Input) => Effect.Effect<boolean>;
    readonly isAttached: (key: DesktopTabKey) => Effect.Effect<boolean>;
    readonly detached: Stream.Stream<DesktopTabKey>;
    readonly runtimeEvidence: (key: DesktopTabKey) => Effect.Effect<{
      readonly runtimeIdentity: PreviewAutomationRuntimeIdentity | null;
      readonly attachmentGeneration: number;
    } | null>;
    readonly endpoint: (key: DesktopTabKey) => Effect.Effect<string, never, Scope.Scope>;
    readonly pointer: (
      key: DesktopTabKey,
      pointer: { readonly phase: "move" | "click"; readonly x: number; readonly y: number },
    ) => Effect.Effect<void>;
  }
>()("t3/jones/previewCompanion/CompanionBrowserChannel") {}

const keyOf = (key: DesktopTabKey) => `${key.threadId}\u0000${key.tabId}`;
const make = Effect.gen(function* () {
  const registry = yield* CompanionHostRegistry.CompanionHostRegistry;
  const changes = yield* PubSub.unbounded<{ key: DesktopTabKey; attached: boolean }>();
  const attached = new Map<
    string,
    {
      key: DesktopTabKey;
      hostId: string;
      generation: number;
      attachmentGeneration: number;
      runtimeIdentity: PreviewAutomationRuntimeIdentity | null;
    }
  >();
  const inbound = new Map<string, Queue.Queue<string>>();
  let attachmentGeneration = 0;
  const detach = (key: DesktopTabKey) =>
    Effect.gen(function* () {
      attached.delete(keyOf(key));
      const queue = inbound.get(keyOf(key));
      if (queue) {
        inbound.delete(keyOf(key));
        yield* Queue.shutdown(queue);
      }
      yield* PubSub.publish(changes, { key, attached: false });
    });
  yield* registry.events.pipe(
    Stream.runForEach((message) =>
      Effect.gen(function* () {
        if (message.type === "offline") {
          for (const entry of attached.values())
            if (entry.hostId === message.hostId && entry.generation === message.generation)
              yield* detach(entry.key);
          return;
        }
        const status = yield* registry.status(message.hostId);
        if (!status?.online || status.connectionGeneration !== message.generation) return;
        const event = message.event;
        const key = { threadId: event.threadId, tabId: event.tabId };
        if (event.type === "attached") {
          if (attached.has(keyOf(key))) yield* detach(key);
          attached.set(keyOf(key), {
            key,
            hostId: message.hostId,
            generation: message.generation,
            attachmentGeneration: ++attachmentGeneration,
            runtimeIdentity: status.runtimeIdentity,
          });
          yield* PubSub.publish(changes, { key, attached: true });
        } else if (event.type === "detached") {
          yield* detach(key);
        } else {
          const current = attached.get(keyOf(key));
          const queue = inbound.get(keyOf(key));
          if (
            queue &&
            current?.hostId === message.hostId &&
            current.generation === message.generation
          )
            yield* Queue.offer(queue, event.message);
        }
      }),
    ),
    Effect.forkScoped,
  );
  return CompanionBrowserChannel.of({
    availableFor: (hostId) =>
      registry.status(hostId).pipe(Effect.map((status) => status?.online ?? false)),
    mount: registry.assign,
    unmount: (hostId, key) => detach(key).pipe(Effect.andThen(registry.unassign(hostId, key))),
    isAttached: (key) => Effect.sync(() => attached.has(keyOf(key))),
    awaitAttached: (key, timeout) =>
      Effect.scoped(
        Effect.gen(function* () {
          const subscription = yield* PubSub.subscribe(changes);
          if (attached.has(keyOf(key))) return true;
          return yield* Stream.fromSubscription(subscription).pipe(
            Stream.filter((change) => change.attached && keyOf(change.key) === keyOf(key)),
            Stream.runHead,
            Effect.map(Option.isSome),
            Effect.timeoutOption(timeout),
            Effect.map((result) => Option.getOrElse(result, () => false)),
          );
        }),
      ),
    detached: Stream.fromPubSub(changes).pipe(
      Stream.filter((change) => !change.attached),
      Stream.map((change) => change.key),
    ),
    runtimeEvidence: (key) =>
      Effect.sync(() => {
        const entry = attached.get(keyOf(key));
        return entry
          ? {
              runtimeIdentity: entry.runtimeIdentity,
              attachmentGeneration: entry.attachmentGeneration,
            }
          : null;
      }),
    endpoint: (key) =>
      Effect.suspend(() => {
        const entry = attached.get(keyOf(key));
        return makeTabCdpEndpoint({
          key,
          inbound,
          isAttached: () => entry !== undefined && attached.get(keyOf(key)) === entry,
          command: (command) =>
            Effect.suspend(() =>
              entry && attached.get(keyOf(key)) === entry
                ? registry
                    .send(entry.hostId, { type: "browser", command }, entry.generation)
                    .pipe(Effect.ignore)
                : Effect.void,
            ),
        });
      }),
    pointer: (key, pointer) =>
      Effect.suspend(() => {
        const entry = attached.get(keyOf(key));
        return entry
          ? registry
              .send(
                entry.hostId,
                { type: "browser", command: { type: "pointer", ...key, ...pointer } },
                entry.generation,
              )
              .pipe(Effect.ignore)
          : Effect.void;
      }),
  });
});
export const layer = Layer.effect(CompanionBrowserChannel, make);
