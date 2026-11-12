/* oxlint-disable t3code/no-rpc-permission-bypass -- Frozen C1 transport requires captured session handles here. This adapter binds capability and ordinary session scope; all RPC execution remains in C1. */
import { EnvironmentSupervisor } from "@t3tools/client-runtime/connection";
import { followStreamInEnvironment } from "@t3tools/client-runtime/state/runtime";
import {
  createPersonalWorkstreamsController,
  createPersonalWorkstreamsCommands,
  createPersonalWorkstreamsRpcTransport,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import type { RpcSession } from "@t3tools/client-runtime/rpc";
import { sessionGrantsScope, type EnvironmentId } from "@t3tools/contracts";
import { PersonalWorkstreamsSnapshot } from "@t3tools/contracts/jones/workstreamsPersonal";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Schema from "effect/Schema";
import { AsyncResult, Atom } from "effect/reactivity";
import * as Crypto from "expo-crypto";
import { personalMobileStorage } from "./personalStorage";
import { connectionAtomRuntime } from "../../connection/runtime";
import { environmentCatalog } from "../../connection/catalog";
import { environmentSession } from "../../state/session";
import { appAtomRegistry } from "../../state/atom-registry";

const ORIGIN = "jones.workstreams.personal.origin.v1";
const CACHE = "jones.workstreams.personal.cache.v1:";
const decodeSnapshot = Schema.decodeUnknownSync(PersonalWorkstreamsSnapshot);
const sessionIds = new WeakMap<object, number>();
let nextSessionId = 0;
const sessionId = (session: object | undefined) => {
  if (!session) return 0;
  let id = sessionIds.get(session);
  if (id === undefined) {
    id = ++nextSessionId;
    sessionIds.set(session, id);
  }
  return id;
};
const sessions = Atom.family((id: EnvironmentId) =>
  connectionAtomRuntime.atom(
    followStreamInEnvironment(
      id,
      Stream.unwrap(
        EnvironmentSupervisor.EnvironmentSupervisor.pipe(
          Effect.map((supervisor) => SubscriptionRef.changes(supervisor.session)),
        ),
      ),
    ),
    { initialValue: Option.none<RpcSession>() },
  ),
);

export function personalMobileAccess(input: {
  authenticated: boolean;
  connected: boolean;
  capable: boolean;
  writable: boolean;
}) {
  return {
    connected: input.authenticated && input.connected,
    capable: input.authenticated && input.capable,
    writable: input.authenticated && input.writable,
  };
}

async function makeBinding() {
  const stored = await personalMobileStorage.getItem(ORIGIN);
  const origin = stored && /^[0-9a-f-]{36}$/i.test(stored) ? stored : Crypto.randomUUID();
  if (origin !== stored) await personalMobileStorage.setItem(ORIGIN, origin);
  const clients = new Map<
    EnvironmentId,
    { generation: string; client: import("@t3tools/client-runtime/rpc").WsRpcProtocolClient }
  >();
  const rpc = createPersonalWorkstreamsRpcTransport((connection) => {
    const entry = clients.get(connection.environmentId);
    return entry?.generation === connection.generation ? entry.client : undefined;
  });
  const controller = createPersonalWorkstreamsController({
    registry: appAtomRegistry,
    origin,
    randomUUID: Crypto.randomUUID,
    transport: {
      ...rpc,
      subscribe: (connection, onSnapshot, onError) =>
        rpc.subscribe(
          connection,
          (snapshot) => {
            onSnapshot(snapshot);
            if (clients.get(connection.environmentId)?.generation !== connection.generation) return;
            void personalMobileStorage
              .setItem(CACHE + snapshot.environmentId, JSON.stringify(snapshot))
              .catch(() => {});
          },
          onError,
        ),
    },
  });
  const commands = createPersonalWorkstreamsCommands(connectionAtomRuntime, controller);
  return { controller, commands, clients };
}
let binding: ReturnType<typeof makeBinding> | undefined;
export const getPersonalMobileBinding = () =>
  (binding ??= makeBinding().catch((error) => {
    binding = undefined;
    throw error;
  }));

export function mobilePersonalConnectionsAtom(ids: readonly EnvironmentId[]) {
  return Atom.make((get) =>
    ids.map((environmentId) => {
      const config = get(environmentSession.initialConfigValueAtom(environmentId));
      const state = get(environmentCatalog.stateAtom(environmentId));
      const auth = get(environmentSession.sessionStateAtom(environmentId));
      const session = get(sessions(environmentId));
      const authenticated = AsyncResult.isSuccess(auth) && auth.value.authenticated;
      return {
        environmentId,
        generation: `${AsyncResult.isSuccess(state) ? state.value.generation : 0}:${sessionId(AsyncResult.isSuccess(session) ? Option.getOrUndefined(session.value) : undefined)}`,
        ...personalMobileAccess({
          authenticated,
          connected:
            AsyncResult.isSuccess(state) &&
            state.value.phase === "connected" &&
            AsyncResult.isSuccess(session) &&
            Option.isSome(session.value),
          capable: config?.environment.capabilities?.workstreamsPersonal?.version === 1,
          writable:
            AsyncResult.isSuccess(auth) && sessionGrantsScope(auth.value, "orchestration:operate"),
        }),
        client: AsyncResult.isSuccess(session)
          ? Option.getOrUndefined(session.value)?.client
          : undefined,
      };
    }),
  );
}

// Home and Sidebar can both mount; only one binding subscribes to the shared inputs.
let consumers = 0;
let release: (() => void) | undefined;
let attachedKey = "";
export async function mountPersonalMobile(ids: readonly EnvironmentId[]) {
  const instance = await getPersonalMobileBinding();
  const key = JSON.stringify(ids);
  consumers += 1;
  if (attachedKey !== key || !release) {
    release?.();
    attachedKey = key;
    const atom = mobilePersonalConnectionsAtom(ids);
    const update = () => {
      const connections = appAtomRegistry.get(atom);
      instance.clients.clear();
      for (const connection of connections) {
        if (connection.connected && connection.client)
          instance.clients.set(connection.environmentId, {
            generation: connection.generation,
            client: connection.client,
          });
      }
      instance.controller.setEnvironments(connections);
    };
    release = appAtomRegistry.subscribe(atom, update, { immediate: true });
    for (const id of ids) {
      void personalMobileStorage
        .getItem(CACHE + id)
        .then((raw) => {
          if (raw) {
            const snapshot = decodeSnapshot(JSON.parse(raw));
            if (snapshot.environmentId === id) instance.controller.loadCache(snapshot);
          }
        })
        .catch(() => {});
    }
  }
  return {
    ...instance,
    release: () => {
      consumers -= 1;
      if (consumers === 0) {
        release?.();
        release = undefined;
        instance.clients.clear();
        instance.controller.dispose();
        binding = undefined;
      }
    },
  };
}
