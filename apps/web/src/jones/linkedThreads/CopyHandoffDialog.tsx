import type {
  LinkedThreadHandoff,
  ThreadEndpointRef,
} from "@t3tools/contracts/jones/linkedThreads";
import { renderThreadHandoff } from "@t3tools/shared/jones/linkedThreads/handoffText";
import { useId, useState } from "react";
import { Button } from "../../components/ui/button";
import { Textarea } from "../../components/ui/textarea";
import {
  Dialog,
  DialogPopup,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogPanel,
  DialogFooter,
} from "../../components/ui/dialog";
import { writeTextToClipboard } from "../../hooks/useCopyToClipboard";
import { toastManager } from "../../components/ui/toast";

function parseHandoffSources(text: string): LinkedThreadHandoff["sourceRefs"] {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (lines.length > 16) throw new Error("Use at most 16 sources");
  return lines.map((line) => {
    const match = /^(thread|pull_request|commit|file|url|artifact|other):\s*(.*)$/.exec(line);
    const kind = match ? (match[1] as LinkedThreadHandoff["sourceRefs"][number]["kind"]) : "other";
    const ref = match ? match[2]! : line;
    if (!ref || ref.length > 512)
      throw new Error("Each source needs a reference of 1–512 characters");
    return { kind, ref };
  });
}

export function CopyHandoffDialog(props: {
  readonly subject: ThreadEndpointRef;
  readonly title: string;
  readonly environmentLabel: string;
  readonly onClose: () => void;
}) {
  const [goal, setGoal] = useState("");
  const [scope, setScope] = useState("");
  const [status, setStatus] = useState("");
  const [sources, setSources] = useState("");
  const [busy, setBusy] = useState(false);
  const [offeredAt] = useState(() => new Date().toISOString());
  const id = useId();
  let text = "";
  let error = "";
  try {
    if (goal.trim() && scope.trim() && status.trim()) {
      text = renderThreadHandoff({
        linkId: null,
        handoff: {
          schemaVersion: "t3.thread-handoff/v1",
          origin: {
            ...props.subject,
            title: props.title.slice(0, 512) || props.subject.threadId,
            environmentLabel: props.environmentLabel.slice(0, 256) || "unknown environment",
          },
          destination: null,
          returnRoute: { kind: "linked_environment", ...props.subject },
          goal: goal.trim(),
          scope: scope.trim(),
          status: status.trim(),
          sourceRefs: parseHandoffSources(sources),
          offeredAt,
        },
      });
    } else parseHandoffSources(sources);
  } catch (failure) {
    error = failure instanceof Error ? failure.message : String(failure);
  }
  const copy = async () => {
    if (!text || busy || error) return;
    setBusy(true);
    try {
      if (await writeTextToClipboard(text, "handoff text")) {
        toastManager.add({ type: "success", title: "Handoff text copied" });
        props.onClose();
      } else toastManager.add({ type: "error", title: "Could not copy handoff text" });
    } catch (failure) {
      toastManager.add({
        type: "error",
        title: "Could not copy handoff text",
        description: failure instanceof Error ? failure.message : String(failure),
      });
    } finally {
      setBusy(false);
    }
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
          <DialogTitle>Copy handoff text</DialogTitle>
          <DialogDescription>
            Paste this into a receiving thread. Ownership stays offered until that thread accepts.
          </DialogDescription>
        </DialogHeader>
        <DialogPanel>
          <label className="grid gap-1 text-sm" htmlFor={`${id}-goal`}>
            Goal
            <Textarea
              id={`${id}-goal`}
              value={goal}
              maxLength={4000}
              onChange={(event) => setGoal(event.target.value)}
            />
          </label>
          <label className="grid gap-1 text-sm" htmlFor={`${id}-scope`}>
            Scope
            <Textarea
              id={`${id}-scope`}
              value={scope}
              maxLength={4000}
              onChange={(event) => setScope(event.target.value)}
            />
          </label>
          <label className="grid gap-1 text-sm" htmlFor={`${id}-status`}>
            Status
            <Textarea
              id={`${id}-status`}
              value={status}
              maxLength={2000}
              onChange={(event) => setStatus(event.target.value)}
            />
          </label>
          <label className="grid gap-1 text-sm" htmlFor={`${id}-sources`}>
            Sources (one per line, for example file: src/app.ts)
            <Textarea
              id={`${id}-sources`}
              value={sources}
              onChange={(event) => setSources(event.target.value)}
            />
          </label>
          {error ? (
            <p role="alert" className="text-sm text-destructive">
              {error}
            </p>
          ) : null}
          {text ? (
            <label className="grid gap-1 text-sm" htmlFor={`${id}-preview`}>
              Handoff preview
              <Textarea id={`${id}-preview`} readOnly value={text} />
            </label>
          ) : null}
        </DialogPanel>
        <DialogFooter>
          <Button variant="outline" disabled={busy} onClick={props.onClose}>
            Cancel
          </Button>
          <Button disabled={!text || !!error || busy} onClick={() => void copy()}>
            {busy ? "Copying…" : "Copy handoff text"}
          </Button>
        </DialogFooter>
      </DialogPopup>
    </Dialog>
  );
}
