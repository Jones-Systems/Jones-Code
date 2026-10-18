import { CommandId, MessageId, ThreadId } from "@t3tools/contracts";
import type { DesignRequestBinding } from "@t3tools/contracts/jones/designRequests";
import {
  createDesignRequestGateway,
  resolveDesignRequestRoute,
  type DesignRequestGateway,
  type DesignRequestResolvedRoute,
  type DesignRequestThreadCandidate,
} from "@t3tools/client-runtime/jones/design-requests";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import * as Effect from "effect/Effect";
import { useEffect, useEffectEvent, useLayoutEffect, useRef, useState } from "react";

import { PrimaryEnvironmentHttpClient } from "../../environments/primary/httpClient";
import { runPrimaryHttp } from "../../lib/runtime";
import { useThreadShells } from "../../state/entities";
import { usePrimaryEnvironmentId } from "../../state/environments";
import { threadEnvironment } from "../../state/threads";
import { useOrchestrationCommand } from "../../state/use-orchestration-command";
import { loadDesignRequestRegistry, type DesignRequestRegistrySnapshot } from "./registry";

const LOADING: DesignRequestRegistrySnapshot = { status: "loading", data: null, placements: null };

const designRequestThreadCandidates = (
  threads: readonly EnvironmentThreadShell[],
): DesignRequestThreadCandidate[] =>
  threads.map((thread) => ({
    environmentId: thread.environmentId,
    id: thread.id,
    title: thread.title,
    archivedAt: thread.archivedAt,
    runtimeMode: thread.runtimeMode,
    interactionMode: thread.interactionMode,
  }));

export interface DesignRequestRouter {
  readonly registry: DesignRequestRegistrySnapshot;
  readonly route: DesignRequestResolvedRoute;
  readonly refresh: () => void;
  readonly gateway: DesignRequestGateway;
}

/**
 * Routes through the existing workstream registry and thread shells, and queues through the
 * shipped `startThreadTurn` command. Submit always re-reads the registry before dispatching.
 */
export function useDesignRequestRouter(binding: DesignRequestBinding | null): DesignRequestRouter {
  const shells = useThreadShells();
  const primaryEnvironmentId = usePrimaryEnvironmentId();
  const startTurn = useOrchestrationCommand(threadEnvironment.startTurn, { reportFailure: false });
  const [revision, setRevision] = useState(0);
  const [now, setNow] = useState(Date.now);
  const threadIdentityKey = JSON.stringify(
    shells.map((thread) => [thread.environmentId, thread.id]).toSorted(),
  );
  const boundWorkstreamId = binding?.workstreamId ?? null;
  const registryKey = JSON.stringify([revision, threadIdentityKey, boundWorkstreamId]);
  const [observation, setObservation] = useState({ key: registryKey, snapshot: LOADING });
  const registry = observation.key === registryKey ? observation.snapshot : LOADING;
  const latest = useRef({ shells, primaryEnvironmentId, binding, startTurn, registryKey });
  useLayoutEffect(() => {
    latest.current = { shells, primaryEnvironmentId, binding, startTurn, registryKey };
  });
  const loadRegistry = useEffectEvent((signal: AbortSignal) =>
    loadDesignRequestRegistry(shells, signal, boundWorkstreamId),
  );

  useEffect(() => {
    const controller = new AbortController();
    loadRegistry(controller.signal).then(
      (snapshot) => {
        if (!controller.signal.aborted) {
          setNow(Date.now());
          setObservation({ key: registryKey, snapshot });
        }
      },
      () => {
        if (!controller.signal.aborted)
          setObservation({
            key: registryKey,
            snapshot: { status: "error", data: null, placements: null },
          });
      },
    );
    return () => controller.abort();
  }, [registryKey]);

  // Re-evaluate when the earliest placement expires so an expired primary is held, not shown.
  useEffect(() => {
    const expiries = (registry.placements?.items ?? [])
      .map((item) => Date.parse(item.expires_at))
      .filter((value) => value > now);
    if (expiries.length === 0) return;
    const timer = window.setTimeout(
      () => setNow(Date.now()),
      Math.min(2_147_483_647, Math.min(...expiries) - Date.now()),
    );
    return () => window.clearTimeout(timer);
  }, [registry.placements, now]);

  const route = resolveDesignRequestRoute({
    binding,
    registry: registry.status,
    data: registry.data,
    placements: registry.placements,
    primaryEnvironmentId,
    threads: designRequestThreadCandidates(shells),
    now,
  });

  const gatewayRef = useRef<DesignRequestGateway | null>(null);
  useLayoutEffect(() => {
    // Keep the in-flight submission map when React replays effect setup.
    if (gatewayRef.current !== null) return;
    gatewayRef.current = createDesignRequestGateway({
      jonesOrigin: window.location.origin,
      resolveRoute: async () => {
        const current = latest.current;
        const snapshot = await loadDesignRequestRegistry(
          current.shells,
          undefined,
          current.binding?.workstreamId ?? null,
        );
        setNow(Date.now());
        setObservation({ key: current.registryKey, snapshot });
        return resolveDesignRequestRoute({
          binding: latest.current.binding,
          registry: snapshot.status,
          data: snapshot.data,
          placements: snapshot.placements,
          primaryEnvironmentId: latest.current.primaryEnvironmentId,
          threads: designRequestThreadCandidates(latest.current.shells),
          now: Date.now(),
        });
      },
      observe: (input) =>
        runPrimaryHttp(
          PrimaryEnvironmentHttpClient.pipe(
            Effect.flatMap((client) =>
              client.orchestration.commandObservation({
                headers: {},
                params: {
                  threadId: ThreadId.make(input.threadId),
                  commandId: CommandId.make(input.commandId),
                },
                query: { messageId: MessageId.make(input.messageId) },
              }),
            ),
          ),
        ),
      dispatch: async (input) => {
        const { primaryEnvironmentId: environmentId, startTurn: run } = latest.current;
        if (environmentId === null) return { kind: "failed", message: "No primary environment." };
        const result = await run({ environmentId, input });
        if (result._tag !== "Failure") return { kind: "dispatched" };
        const error = squashAtomCommandFailure(result);
        return {
          kind: "failed",
          message: error instanceof Error ? error.message : String(error),
        };
      },
      isReadableThread: (threadId) =>
        latest.current.shells.some(
          (thread) =>
            thread.environmentId === latest.current.primaryEnvironmentId && thread.id === threadId,
        ),
    });
  }, []);
  const [gateway] = useState<DesignRequestGateway>(() => ({
    submit: async (input) => {
      const current = gatewayRef.current;
      if (current === null) throw new Error("Design request router is not mounted.");
      return current.submit(input);
    },
    reconcile: async (input) => {
      const current = gatewayRef.current;
      if (current === null) throw new Error("Design request router is not mounted.");
      return current.reconcile(input);
    },
  }));

  const refresh = () => setRevision((value) => value + 1);
  return { registry, route, refresh, gateway };
}
