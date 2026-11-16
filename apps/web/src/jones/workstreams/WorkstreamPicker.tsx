import { useRef, useState } from "react";
import type {
  PersonalThreadRef,
  PersonalWorkstreamsController,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { useAtomValue } from "@effect/atom-react";
import { Dialog, DialogPopup, DialogHeader, DialogTitle } from "../../components/ui/dialog";
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
        </DialogHeader>
        <Input
          aria-label="Search Workstreams"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          maxLength={160}
        />
        <div className="grid gap-1 py-3">
          {state.workstreams
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
            Unassigned
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
