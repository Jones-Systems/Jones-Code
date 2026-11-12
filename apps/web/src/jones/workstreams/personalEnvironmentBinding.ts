import { useAtomValue } from "@effect/atom-react";
import { EnvironmentSupervisor } from "@t3tools/client-runtime/connection";
import type { PersonalConnection } from "@t3tools/client-runtime/jones/workstreams/personal";
import type { RpcSession } from "@t3tools/client-runtime/rpc";
import { followStreamInEnvironment } from "@t3tools/client-runtime/state/runtime";
import type { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom } from "effect/reactivity";
import { createElement, useEffect } from "react";

import { connectionAtomRuntime } from "../../connection/runtime";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { environmentAtomList, environmentIdsKey } from "../../state/environmentAtomList";
import { useEnvironments } from "../../state/environments";
import { useEnvironmentsWithScope } from "../../state/session";
import { bindPersonalWorkstreams } from "../../state/workstreamsPersonal";
import { PersonalWorkstreamsNotifications } from "./personalNotifications";

const sessionGenerations = new WeakMap<RpcSession, number>();
let nextGeneration = 0;

const sessionAtom = Atom.family((environmentId: EnvironmentId) =>
  connectionAtomRuntime.atom(
    followStreamInEnvironment(
      environmentId,
      Stream.unwrap(
        EnvironmentSupervisor.EnvironmentSupervisor.pipe(
          Effect.map((supervisor) => SubscriptionRef.changes(supervisor.session)),
        ),
      ),
    ),
    { initialValue: Option.none<RpcSession>() },
  ),
);

const capturedSessionAtom = Atom.family((environmentId: EnvironmentId) =>
  Atom.make((get) => {
    const session = Option.getOrNull(
      Option.getOrElse(AsyncResult.value(get(sessionAtom(environmentId))), () =>
        Option.none<RpcSession>(),
      ),
    );
    if (session === null) return null;
    let generation = sessionGenerations.get(session);
    if (generation === undefined) {
      generation = ++nextGeneration;
      sessionGenerations.set(session, generation);
    }
    // oxlint-disable-next-line t3code/no-rpc-permission-bypass -- Capture a session handle only; C1 owns RPC execution and this binding applies ordinary scope and capability state.
    return { generation, client: session.client };
  }),
);

const capturedSessionsAtom = environmentAtomList(
  "jones-personal-workstreams-sessions",
  capturedSessionAtom,
);

function usePersonalWorkstreamsEnvironmentBinding() {
  const { environments } = useEnvironments();
  const writableEnvironments = useEnvironmentsWithScope(environments, "orchestration:operate");
  const capturedSessions = useAtomValue(
    capturedSessionsAtom(environmentIdsKey(environments.map((entry) => entry.environmentId))),
  );

  // oxlint-disable-next-line t3code/react-effect-budget -- Synchronize the external controller after committed catalog, session and permission changes.
  useEffect(() => {
    const connections: PersonalConnection[] = environments.map((entry, index) => {
      const capture = capturedSessions[index];
      return {
        environmentId: entry.environmentId,
        generation: capture?.generation ?? "disconnected",
        connected: entry.connection.phase === "connected" && capture != null,
        capable: entry.serverConfig?.environment.capabilities.workstreamsPersonal?.version === 1,
        writable: writableEnvironments.has(entry.environmentId),
      };
    });
    bindPersonalWorkstreams(connections, (connection) => {
      const current = appAtomRegistry.get(capturedSessionAtom(connection.environmentId));
      // oxlint-disable-next-line t3code/no-rpc-permission-bypass -- Resolve only the captured session generation; execution remains in the C1 transport.
      return current?.generation === connection.generation ? current.client : undefined;
    });
  }, [environments, capturedSessions, writableEnvironments]);

  // oxlint-disable-next-line t3code/react-effect-budget -- Release the external controller subscriptions when the app coordinator unmounts.
  useEffect(() => () => bindPersonalWorkstreams([], () => undefined), []);
}

export function PersonalWorkstreamsCoordinator() {
  usePersonalWorkstreamsEnvironmentBinding();
  return createElement(PersonalWorkstreamsNotifications);
}
