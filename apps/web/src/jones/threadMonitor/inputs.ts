import { Atom } from "effect/reactivity";
import * as Option from "effect/Option";
import {
  AuthOrchestrationOperateScope,
  sessionGrantsScope,
  type EnvironmentId,
} from "@t3tools/contracts";
import type { MonitorEnvironmentInput } from "@t3tools/client-runtime/jones/thread-monitor";
import { enabledEnvironmentIds } from "@t3tools/client-runtime/state/connections";
import { environmentCatalog } from "../../connection/catalog";
import { environmentShell } from "../../state/shell";
import { environmentSession } from "../../state/session";
import { environmentPresentations } from "../../state/presentation";
import { environmentThreadShells } from "../../state/threads";
import { monitorStore } from "./store";

export const monitorInputsAtom = Atom.make((get): ReadonlyArray<MonitorEnvironmentInput> => {
  const catalog = get(environmentCatalog.catalogValueAtom);
  monitorStore.observeCatalog(new Set(catalog.entries.keys()));
  const suppressed = get(environmentThreadShells.delegatedUnderParentAtom);
  const inputs: MonitorEnvironmentInput[] = [];
  for (const environmentId of enabledEnvironmentIds(catalog)) {
    const entry = catalog.entries.get(environmentId);
    if (!entry) continue;
    const lifetime = monitorStore.register(environmentId);
    if (!lifetime) continue;
    const shell = get(environmentShell.stateValueAtom(environmentId));
    const snapshot = Option.getOrNull(shell.snapshot);
    const presentation = get(environmentPresentations.presentationAtom(environmentId));
    const session = get(environmentSession.sessionStateValueAtom(environmentId));
    const connected = presentation?.connection.phase === "connected";
    const attention = get(environmentThreadShells.pendingSubagentRequestsAtom(environmentId));
    inputs.push({
      environment: {
        ...lifetime,
        label: entry.target.label.slice(0, 256),
        transport: connected
          ? "connected"
          : presentation?.connection.phase === "connecting" ||
              presentation?.connection.phase === "reconnecting"
            ? "connecting"
            : "disconnected",
        freshness:
          shell.status === "live" && Option.isNone(shell.error)
            ? "live"
            : shell.status === "cached" || snapshot !== null
              ? "cached"
              : "loading",
        reason: Option.isSome(shell.error)
          ? "Shell data could not synchronize"
          : connected
            ? null
            : "Environment is not connected",
      },
      threads: snapshot?.threads ?? [],
      projectLabels: new Map(
        snapshot?.projects.map((project) => [project.id, project.title]) ?? [],
      ),
      canOperate: session !== null && sessionGrantsScope(session, AuthOrchestrationOperateScope),
      canQueue:
        presentation?.serverConfig?.environment.capabilities.serverResolvedCommandContext ===
          true || presentation?.serverConfig?.environment.capabilities.queuedRunStart === true,
      ...(presentation?.serverConfig?.environment.capabilities.threadMonitorGuardedSettle ===
      undefined
        ? {}
        : {
            guardedSettleCapability:
              presentation.serverConfig.environment.capabilities.threadMonitorGuardedSettle,
          }),
      delegatedUnderParent: suppressed,
      delegatedAttention: [...attention].map(([parentThreadId, request]) => ({
        parentThreadId,
        request,
      })),
      pendingActionThreadIds: monitorStore.pendingThreads(environmentId),
    });
  }
  return inputs;
}).pipe(Atom.withLabel("jones-thread-monitor-inputs"));

export function monitorInputFor(
  inputs: ReadonlyArray<MonitorEnvironmentInput>,
  environmentId: EnvironmentId,
) {
  return inputs.find((input) => input.environment.environmentId === environmentId);
}
