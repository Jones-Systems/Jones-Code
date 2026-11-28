import { useRef, useState } from "react";
import {
  openPersonalWorkstreams,
  personalThreadKey,
  personalWorkstreamClosedLifecycle,
  type PersonalThreadRef,
  type PersonalWorkstreamsController,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { useAtomValue } from "@effect/atom-react";
import {
  Dialog,
  DialogDescription,
  DialogPopup,
  DialogHeader,
  DialogTitle,
} from "../../components/ui/dialog";
import { Button } from "../../components/ui/button";
import { Input } from "../../components/ui/input";
import { showPersonalFailure } from "./personalNotifications";
export function WorkstreamPicker({
  controller,
  thread,
  onClose,
}: {
  controller: PersonalWorkstreamsController;
  thread: PersonalThreadRef;
  onClose: () => void;
}) {
  const state = useAtomValue(controller.state);
  const key = personalThreadKey(thread);
  const savedWorkstreamId = state.assignmentsByThreadKey.get(key)?.workstreamId;
  const historicalWorkstream = state.workstreams.find(
    (item) =>
      item.id === savedWorkstreamId && personalWorkstreamClosedLifecycle(state, item) !== null,
  );
  const automaticWorkstream = state.autoAssignedThreadKeys?.has(key)
    ? state.workstreams.find(
        (item) => item.id === state.assignmentsByThreadKey.get(key)?.workstreamId,
      )
    : undefined;
  const [query, setQuery] = useState("");
  const [busy, setBusy] = useState(false);
  const pending = useRef(false);
  const choose = async (id: string | null, create = false) => {
    if (pending.current) return;
    pending.current = true;
    setBusy(true);
    try {
      const result = create
        ? await controller.createAndAssign(thread, query.trim())
        : await controller.assign(thread, id);
      if (result.ok) onClose();
      else showPersonalFailure(result.failure);
    } catch (error) {
      pending.current = false;
      setBusy(false);
      throw error;
    }
    pending.current = false;
    setBusy(false);
  };
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open && !busy) onClose();
      }}
    >
      <DialogPopup>
        <DialogHeader>
          <DialogTitle>Move to Workstream</DialogTitle>
          {historicalWorkstream ? (
            <DialogDescription>
              This thread appears in Unassigned. Its saved Workstream is {historicalWorkstream.name}
              .
            </DialogDescription>
          ) : automaticWorkstream ? (
            <DialogDescription>
              Assigned automatically to {automaticWorkstream.name} from a queued prompt. Choose
              another Workstream or remove it.
            </DialogDescription>
          ) : null}
        </DialogHeader>
        <Input
          aria-label="Search Workstreams"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          maxLength={160}
        />
        <div className="grid gap-1 py-3">
          {openPersonalWorkstreams(state)
            .filter((item) => item.name.toLowerCase().includes(query.toLowerCase()))
            .map((item) => (
              <Button
                key={item.id}
                variant="ghost"
                disabled={busy || !controller.canAssign(thread)}
                onClick={() => void choose(item.id)}
              >
                {item.name}
              </Button>
            ))}
          <Button
            variant="ghost"
            disabled={busy || !controller.canAssign(thread)}
            onClick={() => void choose(null)}
          >
            {historicalWorkstream ? "Remove saved Workstream" : "Unassigned"}
          </Button>
          {query.trim() ? (
            <Button
              disabled={busy || !controller.canAssign(thread) || !state.canCreate}
              onClick={() => void choose(null, true)}
            >
              Create “{query.trim()}” and move
            </Button>
          ) : null}
        </div>
      </DialogPopup>
    </Dialog>
  );
}
