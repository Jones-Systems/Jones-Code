import { useEffect, useState } from "react";
import { isClosedPersonalWorkstream } from "@t3tools/contracts/jones/workstreamsPersonal";
import {
  personalThreadKey,
  type PersonalThreadRef,
  type PersonalWorkstreamHistory,
  type PersonalWorkstreamsController,
  type PersonalWorkstreamView,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { Button } from "../../components/ui/button";
import {
  Dialog,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogPanel,
  DialogPopup,
  DialogTitle,
} from "../../components/ui/dialog";
import { showPersonalFailure } from "./personalNotifications";

function WorkstreamHistory({
  workstream,
  controller,
  onClose,
  onOpenThread,
  canReopen,
}: {
  workstream: PersonalWorkstreamView;
  controller: PersonalWorkstreamsController;
  onClose: () => void;
  onOpenThread: ((thread: PersonalThreadRef) => void) | undefined;
  canReopen: boolean;
}) {
  const [history, setHistory] = useState<PersonalWorkstreamHistory | null>(null);
  const [busy, setBusy] = useState(false);
  const id = workstream.id;
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Read history for the mounted dialog and discard late results on close.
  useEffect(() => {
    let live = true;
    const unavailable = { members: [], complete: false };
    void (controller.readHistory?.(id) ?? Promise.resolve(unavailable))
      .catch(() => unavailable)
      .then((result) => {
        if (live) setHistory(result);
      });
    return () => {
      live = false;
    };
  }, [controller, id]);

  return (
    <Dialog open onOpenChange={(open) => !open && !busy && onClose()}>
      <DialogPopup>
        <DialogHeader>
          <DialogTitle>{workstream.name}</DialogTitle>
          <DialogDescription>
            {workstream.lifecycle === "completed" ? "Completed" : "Abandoned"}
          </DialogDescription>
        </DialogHeader>
        <DialogPanel>
          <h3 className="text-sm font-medium">Assigned threads</h3>
          {history === null ? <p role="status">Loading threads…</p> : null}
          {history && !history.complete ? (
            <p role="status">Some environments are unavailable. This list may be incomplete.</p>
          ) : null}
          {history?.complete && history.members.length === 0 ? <p>No assigned threads.</p> : null}
          {history?.members.length ? (
            <ul className="space-y-1">
              {history.members.map((member) => (
                <li
                  key={personalThreadKey(member)}
                  className="flex items-center justify-between gap-2"
                >
                  {onOpenThread ? (
                    <Button
                      variant="ghost"
                      size="sm"
                      className="min-w-0 justify-start"
                      onClick={() => {
                        onOpenThread(member);
                        onClose();
                      }}
                    >
                      <span className="truncate">{member.title}</span>
                    </Button>
                  ) : (
                    <span className="truncate">{member.title}</span>
                  )}
                  <span className="shrink-0 text-xs text-muted-foreground">
                    {member.settled ? "Settled" : "Unsettled"}
                  </span>
                </li>
              ))}
            </ul>
          ) : null}
        </DialogPanel>
        <DialogFooter>
          <Button
            disabled={busy || !canReopen}
            onClick={() => {
              setBusy(true);
              void controller
                .setLifecycle(id, "active")
                .then((result) => {
                  if (result.ok) onClose();
                  else showPersonalFailure(result.failure);
                })
                .finally(() => setBusy(false));
            }}
          >
            Reopen Workstream
          </Button>
        </DialogFooter>
      </DialogPopup>
    </Dialog>
  );
}

export function ClosedWorkstreams({
  controller,
  state,
  onOpenThread,
}: {
  controller: PersonalWorkstreamsController;
  state: PersonalWorkstreamsState;
  onOpenThread: ((thread: PersonalThreadRef) => void) | undefined;
}) {
  const [selection, setSelection] = useState<string | null>(null);
  const closed = state.workstreams.filter((workstream) =>
    isClosedPersonalWorkstream(workstream.lifecycle),
  );
  const workstream = closed.find(({ id }) => id === selection);
  if (!closed.length) return null;

  return (
    <>
      <details className="py-1 text-sm text-sidebar-muted-foreground">
        <summary className="cursor-pointer rounded-md px-2 py-1 hover:bg-sidebar-row-hover">
          Closed Workstreams
        </summary>
        <ul className="pl-3">
          {closed.map((entry) => (
            <li key={entry.id}>
              <Button
                variant="ghost"
                className="w-full justify-between"
                onClick={() => setSelection(entry.id)}
              >
                <span className="truncate">{entry.name}</span>
                <span className="text-xs">
                  {entry.lifecycle === "completed" ? "Completed" : "Abandoned"}
                </span>
              </Button>
            </li>
          ))}
        </ul>
      </details>
      {workstream ? (
        <WorkstreamHistory
          key={workstream.id}
          controller={controller}
          workstream={workstream}
          onOpenThread={onOpenThread}
          onClose={() => setSelection(null)}
          canReopen={state.canCreate}
        />
      ) : null}
    </>
  );
}
