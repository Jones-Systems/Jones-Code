import { useAtomValue } from "@effect/atom-react";
import {
  createPersonalWorkstreamsController,
  createPersonalWorkstreamsRpcTransport,
  createPersonalWorkstreamsCommands,
  type PersonalConnection,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { PersonalWorkstreamsSnapshot } from "@t3tools/contracts/jones/workstreamsPersonal";
import * as Schema from "effect/Schema";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import { layer as browserCryptoLayer } from "../cloud/dpop";
import type { WsRpcProtocolClient } from "@t3tools/client-runtime/rpc";
import { appAtomRegistry } from "../rpc/atomRegistry";
import { connectionAtomRuntime } from "../connection/runtime";

const clients = new Map<string, { generation: string | number; client: WsRpcProtocolClient }>();
const randomUUID = () =>
  Effect.runSync(
    Effect.provide(
      Crypto.Crypto.pipe(Effect.flatMap((crypto) => crypto.randomUUIDv4())),
      browserCryptoLayer,
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
    subscribe: (connection, onSnapshot, onError) =>
      rpcTransport.subscribe(
        connection,
        (snapshot) => {
          onSnapshot(snapshot);
          // Cached copies are display-only; the controller alone decides precedence.
          if (clients.get(connection.environmentId)?.generation !== connection.generation) return;
          try {
            localStorage.setItem(cacheKey(connection.environmentId), JSON.stringify(snapshot));
          } catch {
            /* Storage can be unavailable. */
          }
        },
        onError,
      ),
  },
});
export const personalWorkstreamsCommands = createPersonalWorkstreamsCommands(
  connectionAtomRuntime,
  personalWorkstreamsController,
);
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
