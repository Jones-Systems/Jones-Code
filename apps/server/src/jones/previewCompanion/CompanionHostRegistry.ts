import {
  CompanionUp,
  ThreadId,
  PREVIEW_COMPANION_PROTOCOL,
  type CompanionDown,
  type DesktopBrowserEvent,
  type PreviewCompanionHostStatus,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import type { DesktopTabKey } from "../../preview/DesktopBrowserChannel.ts";
import {
  CompanionFrameDecoder,
  CompanionFrameError,
  encodeFrames,
  MAX_MESSAGE_BYTES,
} from "./framing.ts";

export const HEARTBEAT_MS = 15_000;
export const HEARTBEAT_TIMEOUT_MS = 45_000;
export class CompanionTransportError extends Schema.TaggedError<CompanionTransportError>()(
  "CompanionTransportError",
  { cause: Schema.Defect() },
) {}

export interface CompanionTransport {
  readonly read: Effect.Effect<ReadonlyArray<string | Uint8Array>, CompanionTransportError>;
  readonly write: (message: string) => Effect.Effect<void, CompanionTransportError>;
  readonly close: (code: number, reason: string) => Effect.Effect<void>;
  readonly writableLength: () => number;
}
export type RegistryEvent =
  | {
      readonly type: "browser";
      readonly hostId: string;
      readonly generation: number;
      readonly event: DesktopBrowserEvent;
    }
  | { readonly type: "offline"; readonly hostId: string; readonly generation: number };

export class CompanionHostUnavailable extends Schema.TaggedError<CompanionHostUnavailable>()(
  "CompanionHostUnavailable",
  {
    hostId: Schema.String,
    label: Schema.String,
    state: Schema.Literals(["unknown", "offline", "timeout"]),
  },
) {
  readonly outcome = "not_started" as const;
  override get message() {
    return `Preview browser host ${this.label} is ${this.state}.`;
  }
}

export class CompanionHostRegistry extends Context.Service<
  CompanionHostRegistry,
  {
    readonly connect: (transport: CompanionTransport) => Effect.Effect<void, never, Scope.Scope>;
    readonly hosts: Effect.Effect<ReadonlyArray<PreviewCompanionHostStatus>>;
    readonly status: (hostId: string) => Effect.Effect<PreviewCompanionHostStatus | undefined>;
    readonly events: Stream.Stream<RegistryEvent>;
    readonly send: (
      hostId: string,
      message: CompanionDown,
      generation?: number,
    ) => Effect.Effect<void, CompanionHostUnavailable>;
    readonly assign: (
      hostId: string,
      key: DesktopTabKey,
    ) => Effect.Effect<void, CompanionHostUnavailable>;
    readonly unassign: (hostId: string, key: DesktopTabKey) => Effect.Effect<void>;
  }
>()("t3/jones/previewCompanion/CompanionHostRegistry") {}

const wireKey = (key: DesktopTabKey) => ({
  threadId: ThreadId.make(key.threadId),
  tabId: key.tabId,
});
const keyOf = (key: DesktopTabKey) => `${key.threadId}\u0000${key.tabId}`;
const decode = Schema.decodeUnknownOption(CompanionUp);
const make = Effect.gen(function* () {
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const environmentId = yield* environment.getEnvironmentId;
  const events = yield* PubSub.unbounded<RegistryEvent>();
  const handshakeLock = yield* Semaphore.make(1);
  const statuses = new Map<string, PreviewCompanionHostStatus>();
  const assignments = new Map<string, Map<string, DesktopTabKey>>();
  const connections = new Map<
    string,
    {
      generation: number;
      transport: CompanionTransport;
      send: (message: CompanionDown) => Effect.Effect<void>;
    }
  >();
  let generation = 0;
  const unavailable = (hostId: string) =>
    new CompanionHostUnavailable({
      hostId,
      label: statuses.get(hostId)?.label ?? hostId,
      state: statuses.has(hostId) ? "offline" : "unknown",
    });
  const disconnect = (hostId: string, connectionGeneration: number) =>
    Effect.gen(function* () {
      if (connections.get(hostId)?.generation !== connectionGeneration) return;
      connections.delete(hostId);
      const previous = statuses.get(hostId);
      if (previous)
        statuses.set(hostId, {
          ...previous,
          online: false,
          connectionGeneration: null,
          runtimeIdentity: null,
        });
      yield* PubSub.publish(events, { type: "offline", hostId, generation: connectionGeneration });
    });
  const send = (hostId: string, message: CompanionDown, expected?: number) =>
    Effect.suspend(() => {
      const connection = connections.get(hostId);
      if (!connection || (expected !== undefined && expected !== connection.generation))
        return Effect.fail(unavailable(hostId));
      return connection.send(message);
    });
  return CompanionHostRegistry.of({
    hosts: Effect.sync(() => [...statuses.values()]),
    status: (hostId) => Effect.sync(() => statuses.get(hostId)),
    events: Stream.fromPubSub(events),
    send,
    assign: (hostId, key) =>
      Effect.gen(function* () {
        const tabs = assignments.get(hostId) ?? new Map<string, DesktopTabKey>();
        tabs.set(keyOf(key), key);
        assignments.set(hostId, tabs);
        yield* send(hostId, { type: "mount", ...wireKey(key) });
      }),
    unassign: (hostId, key) =>
      Effect.gen(function* () {
        assignments.get(hostId)?.delete(keyOf(key));
        yield* send(hostId, { type: "unmount", ...wireKey(key) }).pipe(Effect.ignore);
      }),
    connect: (transport) =>
      Effect.gen(function* () {
        const lock = yield* Semaphore.make(1);
        const decoder = new CompanionFrameDecoder();
        const textDecoder = new TextDecoder();
        let identity: { hostId: string; generation: number } | undefined;
        let lastSeen = yield* Clock.currentTimeMillis;
        let sequence = 0;
        let ended = false;
        let loggedUnknown = false;
        const announced = new Set<string>();
        const close = (code: number, reason: string) =>
          Effect.gen(function* () {
            if (ended) return;
            ended = true;
            if (identity) yield* disconnect(identity.hostId, identity.generation);
            yield* transport.close(code, reason);
          });
        const write = (message: CompanionDown) =>
          lock.withPermits(1)(
            Effect.gen(function* () {
              if (ended) return;
              const frames = yield* Effect.try({
                try: () => encodeFrames(message, String(++sequence)),
                catch: () => new CompanionFrameError(4413),
              });
              for (const frame of frames) {
                if (
                  transport.writableLength() + new TextEncoder().encode(frame).byteLength + 14 >
                  MAX_MESSAGE_BYTES
                ) {
                  yield* close(4413, "send buffer full");
                  return;
                }
                yield* transport.write(frame);
              }
            }).pipe(Effect.catch(() => close(4413, "send failed"))),
          );
        yield* Effect.addFinalizer(() =>
          identity ? disconnect(identity.hostId, identity.generation) : Effect.void,
        );
        const receive = (frame: string | Uint8Array) =>
          Effect.gen(function* () {
            if (
              ended ||
              (identity && connections.get(identity.hostId)?.generation !== identity.generation)
            )
              return;
            const value = yield* Effect.try({
              try: () =>
                decoder.accept(typeof frame === "string" ? frame : textDecoder.decode(frame)),
              catch: (error) =>
                error instanceof CompanionFrameError ? error : new CompanionFrameError(4426),
            });
            if (value === undefined) return;
            const decoded = decode(value);
            if (Option.isNone(decoded)) {
              if (
                typeof value === "object" &&
                value !== null &&
                "type" in value &&
                value.type === "hello"
              )
                return yield* close(4426, "unsupported protocol");
              if (!loggedUnknown) {
                loggedUnknown = true;
                yield* Effect.logWarning("Ignoring unknown preview companion message.");
              }
              return;
            }
            const message = decoded.value;
            lastSeen = yield* Clock.currentTimeMillis;
            if (!identity) {
              if (message.type !== "hello") return yield* close(4426, "hello required");
              yield* handshakeLock.withPermits(1)(
                Effect.gen(function* () {
                  const previous = connections.get(message.hostId);
                  if (previous) {
                    yield* disconnect(message.hostId, previous.generation);
                    yield* previous.transport.close(4409, "superseded");
                  }
                  identity = { hostId: message.hostId, generation: ++generation };
                  connections.set(message.hostId, {
                    generation: identity.generation,
                    transport,
                    send: write,
                  });
                  statuses.set(message.hostId, {
                    hostId: message.hostId,
                    label: message.label,
                    platform: message.platform,
                    capabilities: message.capabilities,
                    runtimeIdentity: message.runtimeIdentity,
                    online: true,
                    lastSeenAt: lastSeen,
                    connectionGeneration: identity.generation,
                  });
                  yield* write({
                    type: "welcome",
                    protocol: PREVIEW_COMPANION_PROTOCOL,
                    environmentId,
                    connectionGeneration: identity.generation,
                    heartbeatMs: HEARTBEAT_MS,
                  });
                  yield* write({
                    type: "assignments",
                    tabs: [...(assignments.get(message.hostId)?.values() ?? [])].map(wireKey),
                  });
                  const accepted = identity;
                  yield* Effect.sleep("20 seconds").pipe(
                    Effect.andThen(
                      Effect.gen(function* () {
                        if (connections.get(accepted.hostId)?.generation !== accepted.generation)
                          return;
                        for (const key of assignments.get(accepted.hostId)?.values() ?? [])
                          if (!announced.has(keyOf(key)))
                            yield* write({ type: "mount", ...wireKey(key) });
                      }),
                    ),
                    Effect.forkScoped,
                  );
                }),
              );
              return;
            }
            if (message.type === "hello") return yield* close(4426, "duplicate hello");
            const status = statuses.get(identity.hostId);
            if (status) statuses.set(identity.hostId, { ...status, lastSeenAt: lastSeen });
            if (message.type === "browser") {
              if (!assignments.get(identity.hostId)?.has(keyOf(message.event))) {
                yield* write({
                  type: "unmount",
                  threadId: ThreadId.make(message.event.threadId),
                  tabId: message.event.tabId,
                });
                return;
              }
              if (message.event.type === "attached") announced.add(keyOf(message.event));
              if (message.event.type === "detached") announced.delete(keyOf(message.event));
              yield* PubSub.publish(events, { type: "browser", ...identity, event: message.event });
            }
          }).pipe(Effect.catch((error) => close(error.code, "invalid framing")));
        const heartbeat = Effect.gen(function* () {
          yield* Effect.sleep(HEARTBEAT_MS);
          if (ended) return yield* Effect.interrupt;
          const now = yield* Clock.currentTimeMillis;
          if (now - lastSeen >= HEARTBEAT_TIMEOUT_MS)
            return yield* close(4408, "heartbeat timeout");
          yield* write({ type: "heartbeat", sentAt: now });
        }).pipe(Effect.forever);
        const read = transport.read.pipe(
          Effect.flatMap((frames) => Effect.forEach(frames, receive, { discard: true })),
          Effect.forever,
        );
        yield* Effect.raceFirst(read, heartbeat).pipe(Effect.ignore);
      }),
  });
});
export const layer = Layer.effect(CompanionHostRegistry, make);
