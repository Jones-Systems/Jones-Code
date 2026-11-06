import { useEffect, useState } from "react";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import {
  planCreationSlotKey,
  type PlanCreationOperation,
  type PlanCreationSlot,
} from "@t3tools/client-runtime/jones/planCreationOperation";
import { planCreationStorage, planCreationStorageKey } from "./planCreationStorage";

export function PlanCreationStatus(props: {
  readonly environmentId: EnvironmentId;
  readonly sourceThreadId: ThreadId;
  readonly disabled: boolean;
  readonly onResume: (slot: PlanCreationSlot) => Promise<void>;
}) {
  const [snapshot, setSnapshot] = useState<{
    environmentId: EnvironmentId;
    sourceThreadId: ThreadId;
    operations: readonly PlanCreationOperation[];
    held: boolean;
  } | null>(null);
  const [inFlight, setInFlight] = useState<string | null>(null);
  // oxlint-disable-next-line t3code/react-effect-budget -- Hydrate and subscribe to the acknowledged platform journal so reload recovery does not depend on the current picker or active plan.
  useEffect(() => {
    let current = true;
    const load = () => {
      void planCreationStorage.readSource(props.environmentId, props.sourceThreadId).then(
        (operations) => {
          if (current)
            setSnapshot({
              environmentId: props.environmentId,
              sourceThreadId: props.sourceThreadId,
              operations,
              held: false,
            });
        },
        () => {
          if (current)
            setSnapshot({
              environmentId: props.environmentId,
              sourceThreadId: props.sourceThreadId,
              operations: [],
              held: true,
            });
        },
      );
    };
    const unsubscribe = planCreationStorage.subscribe(props.environmentId, load);
    const onStorage = (event: StorageEvent) => {
      if (
        event.storageArea === window.localStorage &&
        event.key === planCreationStorageKey(props.environmentId)
      )
        load();
    };
    window.addEventListener("storage", onStorage);
    load();
    return () => {
      current = false;
      unsubscribe();
      window.removeEventListener("storage", onStorage);
    };
  }, [props.environmentId, props.sourceThreadId]);
  if (
    snapshot?.environmentId !== props.environmentId ||
    snapshot.sourceThreadId !== props.sourceThreadId
  )
    return null;
  if (snapshot.held)
    return (
      <p role="status" className="text-xs text-muted-foreground">
        Saved plan actions need reconciliation. No replacement will be created.
      </p>
    );
  return (
    <div className="flex flex-col gap-1 text-xs text-muted-foreground">
      {snapshot.operations
        .filter((operation) => operation.phase !== "create-rejected")
        .map((operation) => {
          const key = planCreationSlotKey(operation);
          const open = operation.phase === "accepted" || operation.phase === "message-rejected";
          return (
            <div key={key} className="flex items-center gap-2">
              <span role="status">
                {open ? "Implementation thread saved" : "Plan delivery needs confirmation"}
              </span>
              <button
                type="button"
                className="underline"
                disabled={props.disabled || inFlight !== null}
                onClick={() => {
                  setInFlight(key);
                  const finish = () => setInFlight((current) => (current === key ? null : current));
                  void props.onResume(operation).then(finish, finish);
                }}
              >
                {open ? "Open" : "Retry"}: {operation.create.title}
              </button>
            </div>
          );
        })}
    </div>
  );
}
