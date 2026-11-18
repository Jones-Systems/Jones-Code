import { useAtomValue } from "@effect/atom-react";
import {
  createPersonalWorkstreamsController,
  createPersonalWorkstreamsRpcTransport,
  type PersonalConnection,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { PersonalWorkstreamsSnapshot } from "@t3tools/contracts/jones/workstreamsPersonal";
import * as Schema from "effect/Schema";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import { Atom } from "effect/reactivity";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { PersonalWorkstreamAssignment } from "@t3tools/contracts/jones/workstreamsPersonal";
import type { WsRpcProtocolClient } from "@t3tools/client-runtime/rpc";
import { appAtomRegistry } from "../rpc/atomRegistry";

const clients = new Map<string, { generation: string | number; client: WsRpcProtocolClient }>();
const randomUUID = () =>
  Effect.runSync(
    Effect.provide(
      SecureRandom.SecureRandom.pipe(Effect.flatMap((random) => random.uuidV4())),
      SecureRandom.layerWebCrypto,
    ),
  );
function writerOrigin() {
  const key = "jones.personal-workstreams.origin.v1";
  try {
    const saved = localStorage.getItem(key);
    if (saved && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(saved))
      return saved;
    const value = randomUUID();
    localStorage.setItem(key, value);
    return value;
  } catch {
    return randomUUID();
  }
}
const decodeCacheSnapshot = Schema.decodeUnknownSync(PersonalWorkstreamsSnapshot);
const cacheKey = (id: string) => `jones.personal-workstreams.snapshot.v1:${id}`;
type LiveAssignments = ReadonlyMap<ThreadId, PersonalWorkstreamAssignment>;
const liveAssignments = Atom.make<ReadonlyMap<EnvironmentId, LiveAssignments>>(new Map()).pipe(
  Atom.keepAlive,
);
function publishLiveAssignments(environmentId: EnvironmentId, assignments: LiveAssignments | null) {
  const next = new Map(appAtomRegistry.get(liveAssignments));
  if (assignments) next.set(environmentId, assignments);
  else next.delete(environmentId);
  appAtomRegistry.set(liveAssignments, next);
}
const rpcTransport = createPersonalWorkstreamsRpcTransport((connection) => {
  const entry = clients.get(connection.environmentId);
  // oxlint-disable-next-line t3code/no-rpc-permission-bypass -- Resolver boundary for C1 transport; C1 gates every write by the captured ordinary operate permission.
  return entry?.generation === connection.generation ? entry.client : undefined;
});
export const personalWorkstreamsController = createPersonalWorkstreamsController({
  registry: appAtomRegistry,
  origin: writerOrigin(),
  transport: {
    ...rpcTransport,
    subscribe: (connection, onSnapshot, onError) => {
      const { environmentId, generation } = connection;
      let active = true;
      let sequence = -1;
      const stop = rpcTransport.subscribe(
        connection,
        (snapshot) => {
          if (
            !active ||
            clients.get(environmentId)?.generation !== generation ||
            snapshot.environmentId !== environmentId ||
            snapshot.sequence < sequence
          )
            return;
          sequence = snapshot.sequence;
          Atom.batch(() => {
            publishLiveAssignments(
              environmentId,
              new Map(snapshot.assignments.map((assignment) => [assignment.threadId, assignment])),
            );
            onSnapshot(snapshot);
          });
          // Cached copies are display-only; the controller alone decides precedence.
          if (clients.get(connection.environmentId)?.generation !== connection.generation) return;
          try {
            localStorage.setItem(cacheKey(connection.environmentId), JSON.stringify(snapshot));
          } catch {
            /* Storage can be unavailable. */
          }
        },
        (error) => {
          if (!active) return;
          Atom.batch(() => {
            publishLiveAssignments(environmentId, null);
            onError(error);
          });
        },
      );
      return () => {
        active = false;
        stop();
        publishLiveAssignments(environmentId, null);
      };
    },
  },
});
export function bindPersonalWorkstreams(
  connections: readonly PersonalConnection[],
  resolveClient: (connection: PersonalConnection) => WsRpcProtocolClient | undefined,
) {
  clients.clear();
  for (const connection of connections) {
    const client = resolveClient(connection);
    if (client)
      clients.set(connection.environmentId, { generation: connection.generation, client });
  }
  personalWorkstreamsController.setEnvironments(connections);
  for (const connection of connections) {
    try {
      const raw = localStorage.getItem(cacheKey(connection.environmentId));
      if (raw) {
        const snapshot = decodeCacheSnapshot(JSON.parse(raw));
        if (snapshot.environmentId === connection.environmentId)
          personalWorkstreamsController.loadCache(snapshot);
      }
    } catch {
      /* Ignore malformed display caches. */
    }
  }
}
export function usePersonalWorkstreams() {
  return {
    controller: personalWorkstreamsController,
    state: useAtomValue(personalWorkstreamsController.state),
  };
}

/** Exact live membership, before the controller's sidebar projection masks unavailable groups. */
const noLiveAssignments = Atom.make<LiveAssignments | null>(null);
const liveAssignmentsFor = Atom.family((environmentId: EnvironmentId) =>
  Atom.map(liveAssignments, (byEnvironment) => byEnvironment.get(environmentId) ?? null),
);
export function useLivePersonalAssignments(environmentId: EnvironmentId | null) {
  return useAtomValue(environmentId ? liveAssignmentsFor(environmentId) : noLiveAssignments);
}

export function readLivePersonalAssignments(environmentId: EnvironmentId) {
  return appAtomRegistry.get(liveAssignments).get(environmentId) ?? null;
}
