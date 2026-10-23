import { useEffect, useRef, useState } from "react";
import {
  operationOpen,
  operationSucceeded,
  operationMessage,
  WorkstreamOperationError,
  WorkstreamOperationJoined,
} from "@t3tools/client-runtime/jones/workstreams/operations";
import type { WorkstreamListView } from "../../state/workstreams";
import { Button } from "../../components/ui/button";
import { Input } from "../../components/ui/input";
import {
  Dialog,
  DialogHeader,
  DialogPanel,
  DialogPopup,
  DialogTitle,
  DialogDescription,
} from "../../components/ui/dialog";

type Choice = { kind: "existing"; id: string | null } | { kind: "create"; name: string };
export function WorkstreamPicker({
  controller,
  thread,
  onClose,
  commandId,
}: {
  controller: WorkstreamListView;
  thread: { environmentId: string; id: string; title?: string };
  onClose: () => void;
  commandId: () => Promise<string>;
}) {
  const [query, setQuery] = useState("");
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [choice, setChoice] = useState<Choice | null>(null);
  const [waitingId, setWaitingId] = useState<string | null>(null);
  const [joinedId, setJoinedId] = useState<string | null>(null);
  const request = useRef<AbortController | null>(null);
  const running = useRef(false);
  const retryRef = useRef<() => void>(() => {});
  useEffect(
    () => () => {
      request.current?.abort();
    },
    [],
  );
  const name = query.trim();
  const items = (controller.data?.items ?? [])
    .filter((item) => item.name.toLocaleLowerCase().includes(name.toLocaleLowerCase()))
    .sort((a, b) => Number(b.name === name) - Number(a.name === name));
  const exact = items.find((item) => item.name === name);
  const waiting = controller.operationEntries?.find(
    (entry) => entry.operationId === (joinedId ?? waitingId),
  )?.result;
  useEffect(() => {
    if (!waiting || operationOpen(waiting)) return;
    if (joinedId) {
      setJoinedId(null);
      setMessage(`${operationMessage(waiting)} Retry your choice to apply it.`);
      return;
    }
    if (operationSucceeded(waiting)) {
      setWaitingId(null);
      if (waiting.kind === "create_workstream") retryRef.current();
      else onClose();
    } else setMessage(operationMessage(waiting));
  }, [waiting, joinedId, onClose]);
  const run = async (selected: Choice, retry = false) => {
    if (running.current) return;
    running.current = true;
    setBusy(true);
    setChoice(selected);
    setMessage(null);
    setWaitingId(null);
    setJoinedId(null);
    const abort = new AbortController();
    request.current = abort;
    try {
      let destination =
        selected.kind === "existing"
          ? selected.id
          : controller.data?.items.find((item) => item.name === selected.name)?.workstreamId;
      if (selected.kind === "create" && !destination) {
        if (!controller.createWorkstream)
          throw new Error("Create a Workstream first on this server, then choose it here.");
        destination = await controller.createWorkstream(selected.name, {
          commandId,
          signal: abort.signal,
        });
      }
      abort.signal.throwIfAborted();
      if (destination === undefined || !controller.assignNativeThread)
        throw new Error("Assignment is unavailable.");
      await controller.assignNativeThread(thread, destination, {
        commandId,
        signal: abort.signal,
        retry,
      });
      if (!abort.signal.aborted) onClose();
    } catch (cause) {
      if (abort.signal.aborted) return;
      if (cause instanceof WorkstreamOperationError) setWaitingId(cause.result.operation_id);
      if (cause instanceof WorkstreamOperationJoined) setJoinedId(cause.operationId);
      setMessage(
        cause instanceof Error
          ? cause.message
          : "Outcome unknown. Retry checks the retained request.",
      );
    } finally {
      running.current = false;
      if (!abort.signal.aborted) setBusy(false);
    }
  };
  retryRef.current = () => {
    if (choice) void run(choice, true);
  };
  const pending = busy || (waiting != null && operationOpen(waiting));
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
    >
      <DialogPopup>
        <DialogHeader>
          <DialogTitle>Assign Workstream</DialogTitle>
          <DialogDescription>
            {thread.title ?? "Choose a Workstream for this thread."}
          </DialogDescription>
        </DialogHeader>
        <DialogPanel>
          <div className="space-y-3">
            <Input
              nativeInput
              autoFocus
              aria-label="Search Workstreams"
              placeholder="Search or create Workstream"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              disabled={pending}
            />
            {pending ? (
              <p role="status" className="text-sm text-muted-foreground">
                {waiting ? operationMessage(waiting) : "Saving Workstream change…"}
              </p>
            ) : null}
            {message ? (
              <p role="alert" className="text-sm text-muted-foreground">
                {message}
              </p>
            ) : null}
            {choice && message ? (
              <Button size="sm" disabled={busy} onClick={() => void run(choice, true)}>
                Retry
              </Button>
            ) : null}
            <div
              className="flex max-h-72 flex-col items-start gap-1 overflow-y-auto"
              aria-label="Workstream choices"
            >
              {items.map((item) => (
                <Button
                  key={item.workstreamId}
                  variant="ghost"
                  size="sm"
                  disabled={pending}
                  onClick={() => void run({ kind: "existing", id: item.workstreamId })}
                >
                  {item.name}
                  {exact === item ? " (existing)" : ""}
                </Button>
              ))}
              {name && !exact && controller.createWorkstream ? (
                <Button
                  variant="ghost"
                  size="sm"
                  disabled={pending}
                  onClick={() => void run({ kind: "create", name })}
                >
                  Create '{name}'
                </Button>
              ) : null}
              <Button
                variant="ghost"
                size="sm"
                disabled={pending}
                onClick={() => void run({ kind: "existing", id: null })}
              >
                Unassigned
              </Button>
            </div>
          </div>
        </DialogPanel>
      </DialogPopup>
    </Dialog>
  );
}
