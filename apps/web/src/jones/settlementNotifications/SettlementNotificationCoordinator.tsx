import { useAtomValue } from "@effect/atom-react";
import { useNavigate } from "@tanstack/react-router";
import type { EnvironmentId } from "@t3tools/contracts";
import { useEffect, useRef } from "react";
import type { SettlementOriginResolver } from "@t3tools/client-runtime/jones/settlement-notifications";
import { resolveSettlementOrigins } from "./resolveOrigins";
import { toastManager } from "../../components/ui/toast";
import { useEnvironmentIds } from "../../state/environments";
import { environmentShell } from "../../state/shell";
import { getSettlementNotificationController } from "./controller";
import { SettlementNotificationToastContent } from "./SettlementNotificationToastContent";

type Controller = ReturnType<typeof getSettlementNotificationController>;

export function SettlementNotificationCoordinator() {
  const controller = getSettlementNotificationController();
  const environmentIds = useEnvironmentIds();
  const navigate = useNavigate();
  const navigateRef = useRef(navigate);
  useEffect(() => {
    navigateRef.current = navigate;
  }, [navigate]);
  const toastId = useRef<string | null>(null);
  const generation = useRef(0);
  const collapsedIds = useRef(new Set<string>());

  useEffect(() => {
    const currentGeneration = ++generation.current;
    let active = true;
    const reconcile = () => {
      if (!active) return;
      const { visible } = controller.getSnapshot();
      const visibleIds = new Set(visible.map((entry) => entry.id));
      for (const id of collapsedIds.current) {
        if (!visibleIds.has(id)) collapsedIds.current.delete(id);
      }
      if (visible.length === 0) {
        const id = toastId.current;
        toastId.current = null;
        if (id !== null) toastManager.close(id);
        return;
      }
      const shownIds = visible.map((entry) => entry.id);
      const content = (
        <SettlementNotificationToastContent
          entries={visible}
          collapsedIds={collapsedIds.current}
          onCollapsedChange={(ids) => {
            // Retained toast content seeds from this same Set when its panel remounts.
            collapsedIds.current.clear();
            for (const id of ids) collapsedIds.current.add(id);
          }}
          onOpen={(entry) => {
            void navigateRef.current({
              to: "/$environmentId/$threadId",
              params: { environmentId: entry.environmentId, threadId: entry.threadId },
            });
          }}
        />
      );
      const options = {
        title: visible.length === 1 ? "Thread settled" : `${visible.length} threads settled`,
        description: visible.length === 1 ? content : "Settled threads",
        timeout: 0,
        data: {
          explicitDismissOnly: true,
          hideCopyButton: true,
          onClose: () => controller.dismiss(shownIds),
          expandableContent: visible.length > 1 ? content : undefined,
          expandableDescriptionTrigger: true,
          expandableFitWidth: visible.length > 1,
          expandableLabels: { expand: "Show settled threads", collapse: "Hide settled threads" },
        },
      };
      if (toastId.current !== null) {
        const id = toastId.current;
        toastManager.update(id, {
          ...options,
          onRemove: () => {
            if (toastId.current !== id) return;
            toastId.current = null;
            queueMicrotask(reconcile);
          },
        });
      } else {
        const id = toastManager.add({
          ...options,
          onRemove: () => {
            if (toastId.current !== id) return;
            toastId.current = null;
            queueMicrotask(reconcile);
          },
        });
        toastId.current = id;
      }
    };
    const unsubscribe = controller.subscribe(reconcile);
    // Let the parent ToastProvider subscribe before publishing on a remount.
    void controller.hydrate().then(reconcile);
    const flush = () => {
      void controller.flush();
    };
    window.addEventListener("pagehide", flush);
    return () => {
      active = false;
      unsubscribe();
      window.removeEventListener("pagehide", flush);
      flush();
      // StrictMode's immediate re-subscription retains the same toast and expansion.
      queueMicrotask(() => {
        if (generation.current !== currentGeneration) return;
        const id = toastId.current;
        toastId.current = null;
        if (id !== null) toastManager.close(id);
      });
    };
  }, [controller]);

  useEffect(() => controller.noteCatalog(environmentIds), [controller, environmentIds]);
  return environmentIds.map((environmentId) => (
    <EnvironmentObserver
      key={environmentId}
      environmentId={environmentId}
      controller={controller}
    />
  ));
}

function EnvironmentObserver({
  environmentId,
  controller,
}: {
  environmentId: EnvironmentId;
  controller: Controller;
}) {
  const state = useAtomValue(environmentShell.stateValueAtom(environmentId));
  useEffect(() => {
    const resolveOrigins: SettlementOriginResolver = (arrivals) =>
      resolveSettlementOrigins(environmentId, arrivals);
    controller.observe(environmentId, state, { resolveOrigins });
  }, [controller, environmentId, state]);
  return null;
}
