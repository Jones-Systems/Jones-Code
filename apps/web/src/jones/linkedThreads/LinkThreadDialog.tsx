import { endpointKey, sameEndpoint } from "@t3tools/client-runtime/jones/linked-threads";
import type { ThreadEndpointRef } from "@t3tools/contracts/jones/linkedThreads";
import { useId, useState } from "react";
import { Button } from "../../components/ui/button";
import { Input } from "../../components/ui/input";
import {
  Dialog,
  DialogPopup,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogPanel,
  DialogFooter,
} from "../../components/ui/dialog";
import type { useLinkedThreads } from "./useLinkedThreads";

export function LinkThreadDialog(props: {
  readonly subject: ThreadEndpointRef;
  readonly view: ReturnType<typeof useLinkedThreads>;
  readonly onClose: () => void;
}) {
  const [relationship, setRelationship] = useState("continued-in");
  const [search, setSearch] = useState("");
  const [targetKey, setTargetKey] = useState("");
  const [busy, setBusy] = useState(false);
  const id = useId();
  const targets = props.view.shells.filter(
    (shell) =>
      !sameEndpoint({ environmentId: shell.environmentId, threadId: shell.id }, props.subject) &&
      props.view.environments.some(
        (env) => env.environmentId === shell.environmentId && env.connection.phase === "connected",
      ),
  );
  const label = (environmentId: ThreadEndpointRef["environmentId"]) =>
    props.view.environments.find((env) => env.environmentId === environmentId)?.label ??
    environmentId;
  const matching = targets.filter((shell) =>
    `${shell.title} ${shell.id} ${label(shell.environmentId)}`
      .toLowerCase()
      .includes(search.toLowerCase()),
  );
  const selected = targets.find(
    (shell) =>
      endpointKey({ environmentId: shell.environmentId, threadId: shell.id }) === targetKey,
  );
  const submit = async () => {
    if (!selected || busy) return;
    const target = { environmentId: selected.environmentId, threadId: selected.id };
    setBusy(true);
    await (async () => {
      const succeeded = await props.view.link({
        kind: relationship === "peer" ? "peer" : "continuation",
        from: relationship === "continues-from" ? target : props.subject,
        to: relationship === "continues-from" ? props.subject : target,
      });
      if (succeeded) props.onClose();
    })().finally(() => {
      setBusy(false);
    });
  };
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open && !busy) props.onClose();
      }}
    >
      <DialogPopup>
        <DialogHeader>
          <DialogTitle>Link thread</DialogTitle>
          <DialogDescription>
            A link records navigation. It does not delegate a task, grant access, or transfer
            ownership.
          </DialogDescription>
        </DialogHeader>
        <DialogPanel>
          <label className="grid gap-1 text-sm" htmlFor={`${id}-relationship`}>
            Relationship
            <select
              id={`${id}-relationship`}
              className="h-9 rounded-md border bg-background px-2"
              value={relationship}
              disabled={busy}
              onChange={(event) => setRelationship(event.target.value)}
            >
              <option value="continued-in">Continued in</option>
              <option value="continues-from">Continues from</option>
              <option value="peer">Peer</option>
            </select>
          </label>
          <label className="grid gap-1 text-sm" htmlFor={`${id}-search`}>
            Find a thread
            <Input
              id={`${id}-search`}
              type="search"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
            />
          </label>
          <div className="max-h-64 overflow-y-auto" role="radiogroup" aria-label="Thread to link">
            {matching.map((shell) => {
              const key = endpointKey({ environmentId: shell.environmentId, threadId: shell.id });
              return (
                <label
                  key={key}
                  className="flex min-h-10 cursor-pointer items-center gap-2 rounded-md px-2 hover:bg-muted"
                >
                  <input
                    type="radio"
                    name={`${id}-target`}
                    checked={targetKey === key}
                    disabled={busy}
                    onChange={() => setTargetKey(key)}
                  />
                  <span className="min-w-0">
                    <span className="block truncate text-sm">{shell.title || shell.id}</span>
                    <span className="block text-2xs text-muted-foreground">
                      {label(shell.environmentId)} · {shell.id}
                    </span>
                  </span>
                </label>
              );
            })}
            {matching.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                No matching threads on connected environments.
              </p>
            ) : null}
          </div>
          <p className="text-xs text-muted-foreground">
            When a target environment cannot record links, the link is saved here and the mirror is
            reported unavailable.
          </p>
        </DialogPanel>
        <DialogFooter>
          <Button variant="outline" disabled={busy} onClick={props.onClose}>
            Cancel
          </Button>
          <Button disabled={!selected || busy || !props.view.canLink} onClick={() => void submit()}>
            {busy ? "Linking…" : "Link thread"}
          </Button>
        </DialogFooter>
      </DialogPopup>
    </Dialog>
  );
}
