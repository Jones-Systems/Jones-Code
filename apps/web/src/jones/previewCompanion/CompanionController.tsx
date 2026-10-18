import { useEffect } from "react";
import { useAtomValue } from "@effect/atom-react";
import { createEnvironmentCommand, runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import { EnvironmentSupervisor } from "@t3tools/client-runtime/connection";
import type {
  DesktopCompanionTicketRequest,
  DesktopCompanionTicketResponse,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { Atom } from "effect/reactivity";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { environmentCatalog } from "../../connection/catalog";
import { connectionAtomRuntime } from "../../connection/runtime";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { environmentSession, readPreparedConnection } from "../../state/session";
import { previewEnvironment } from "../../state/preview";
import { toastManager } from "../../components/ui/toast";
import { companionStateAtom, refreshCompanionBindings } from "./state.ts";
import { companionTicketEligibility, resolveCompanionTicket } from "./ticket.ts";
import { subscribeCompanionController } from "./controller.ts";

const ticketCommand = createEnvironmentCommand(connectionAtomRuntime, {
  label: "companion:ticket",
  execute: (input: { readonly environmentId: DesktopCompanionTicketRequest["environmentId"] }) =>
    Effect.gen(function* () {
      const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
      const prepared = yield* SubscriptionRef.get(supervisor.prepared);
      if (Option.isNone(prepared)) return { _tag: "unavailable" } as const;
      const entry = appAtomRegistry
        .get(environmentCatalog.catalogValueAtom)
        .entries.get(input.environmentId);
      const rejected = companionTicketEligibility({
        registered: !!entry,
        enabled: entry?.enabled === true,
        prepared: prepared.value,
        session: appAtomRegistry.get(environmentSession.sessionStateValueAtom(input.environmentId)),
      });
      return rejected ?? (yield* resolveCompanionTicket(prepared.value));
    }),
});

async function ticket(
  request: DesktopCompanionTicketRequest,
): Promise<DesktopCompanionTicketResponse["result"]> {
  const entry = appAtomRegistry
    .get(environmentCatalog.catalogValueAtom)
    .entries.get(request.environmentId);
  const prepared = readPreparedConnection(request.environmentId);
  const session = appAtomRegistry.get(
    environmentSession.sessionStateValueAtom(request.environmentId),
  );
  const rejected = companionTicketEligibility({
    registered: entry !== undefined,
    enabled: entry?.enabled === true,
    prepared,
    session,
  });
  if (rejected) return rejected;
  const result = await runAtomCommand(
    appAtomRegistry,
    ticketCommand,
    { environmentId: request.environmentId, input: { environmentId: request.environmentId } },
    { reportFailure: false, reportDefect: false },
  );
  if (readPreparedConnection(request.environmentId) !== prepared) return { _tag: "unavailable" };
  return result._tag === "Success" ? result.value : { _tag: "unavailable" };
}

const noSession = Atom.make(null);
const noPrepared = Atom.make(Option.none());

export function CompanionController() {
  const state = useAtomValue(companionStateAtom);
  const environmentId = state?.config.environmentId ?? null;
  const catalog = useAtomValue(environmentCatalog.catalogValueAtom);
  // Keep the grant and prepared-connection atoms observed while hosting, so
  // reconnects cannot reuse an unobserved permission snapshot.
  const sessionAtom = environmentId
    ? environmentSession.sessionStateValueAtom(environmentId)
    : noSession;
  const session = useAtomValue(sessionAtom);
  const prepared = useAtomValue(
    environmentId ? environmentSession.preparedConnectionValueAtom(environmentId) : noPrepared,
  );
  useEffect(() => {
    const bridge = window.desktopBridge?.previewCompanion;
    if (!bridge) return;
    return subscribeCompanionController({
      bridge,
      ticket,
      state: (next) => {
        appAtomRegistry.set(companionStateAtom, next);
        if (!next?.config.environmentId) return;
        for (const threadId of new Set(next.assignments.map((key) => key.threadId))) {
          const threadRef = { environmentId: next.config.environmentId, threadId };
          refreshCompanionBindings(threadRef);
          appAtomRegistry.refresh(
            previewEnvironment.list({
              environmentId: threadRef.environmentId,
              input: { threadId },
            }),
          );
        }
      },
      notice: (notice) =>
        toastManager.add({
          type: "info",
          title: "Pop-up blocked",
          description: `Preview doesn't support pop-up windows or pop-up sign-in yet. Use the site's same-tab sign-in if it offers one.${notice.origin ? ` (${notice.origin})` : ""}`,
        }),
    });
  }, []);
  useEffect(() => {
    if (!state?.config.enabled || !environmentId) return;
    const entry = catalog.entries.get(environmentId);
    const rejected = companionTicketEligibility({
      registered: !!entry,
      enabled: entry?.enabled === true,
      prepared: Option.getOrNull(prepared),
      session,
    });
    const bridge = window.desktopBridge?.previewCompanion;
    if (rejected && state.status === "online") {
      void bridge?.setTicketProviderReady(false).catch(() => undefined);
    } else if (!rejected) {
      void bridge?.setTicketProviderReady(true).catch(() => undefined);
      if (
        state.status === "auth_required" ||
        state.status === "unavailable" ||
        state.status === "awaiting_ticket"
      )
        void bridge?.retry().catch(() => undefined);
    }
  }, [catalog, environmentId, prepared, session, state?.config.enabled]);
  return null;
}
