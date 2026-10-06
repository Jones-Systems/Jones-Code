import type { WorkstreamAppearance, WorkstreamAppearanceWrite } from "@t3tools/contracts";
import {
  WORKSTREAM_COLOR_PRESETS,
  normalizeWorkstreamColor,
  workstreamTint,
  workstreamAppearanceBorder,
} from "@t3tools/client-runtime/state/workstreams";
import { useRef, useState } from "react";
import { Button } from "../../components/ui/button";
import { Input } from "../../components/ui/input";
import {
  Dialog,
  DialogPopup,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "../../components/ui/dialog";

export function WorkstreamColorDialog(props: {
  readonly name: string;
  readonly saved: WorkstreamAppearance;
  readonly generation: number;
  readonly createCommandId: () => Promise<string>;
  readonly save: (request: WorkstreamAppearanceWrite) => Promise<WorkstreamAppearance>;
  readonly onClose: () => void;
}) {
  const [saved] = useState(() => props.saved);
  const [draft, setDraft] = useState(saved.border_color ?? "#0284C7");
  const [automatic, setAutomatic] = useState(saved.border_color === null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [retry, setRetry] = useState<WorkstreamAppearanceWrite | null>(null);
  const busyRef = useRef(false);
  const color = normalizeWorkstreamColor(draft);
  const save = async () => {
    if (busyRef.current || (!automatic && !color)) return;
    busyRef.current = true;
    setBusy(true);
    setError(null);
    try {
      const request = retry ?? {
        command_id: await props.createCommandId(),
        workstream_id: saved.workstream_id,
        expected_server_generation: props.generation,
        expected_version: saved.version,
        border_color: automatic ? null : color,
      };
      setRetry(request);
      await props.save(request);
      props.onClose();
    } catch {
      setError(
        "Could not confirm the saved color. Retry the same save or refresh Workstreams to check the result.",
      );
    } finally {
      busyRef.current = false;
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
      <DialogPopup showCloseButton={!busy}>
        <DialogHeader>
          <DialogTitle>Color for {props.name}</DialogTitle>
          <DialogDescription>
            Choose a border color. Save to apply it on your devices.
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-4 py-4">
          <div
            aria-label="Border preview"
            className={`rounded-md border-l-2 p-3 ${workstreamTint(saved.workstream_id)}`}
            style={workstreamAppearanceBorder(automatic ? null : color)}
          >
            {props.name}
          </div>
          <div role="group" aria-label="Preset colors" className="grid grid-cols-6 gap-2">
            {WORKSTREAM_COLOR_PRESETS.map(([name, value]) => (
              <button
                key={value}
                type="button"
                disabled={busy || retry !== null}
                aria-label={`${name} ${value}`}
                aria-pressed={!automatic && color === value}
                onClick={() => {
                  setDraft(value);
                  setAutomatic(false);
                }}
                className="h-8 rounded border-2 border-transparent outline-none focus-visible:ring-2 focus-visible:ring-ring"
                style={{
                  backgroundColor: value,
                  borderColor: !automatic && color === value ? "var(--foreground)" : "transparent",
                }}
              >
                {!automatic && color === value ? (
                  <span className="rounded bg-background px-1 text-foreground">✓</span>
                ) : null}
              </button>
            ))}
          </div>
          <label className="flex items-center gap-3">
            Custom color
            <input
              aria-label="Custom RGB color"
              type="color"
              disabled={busy || retry !== null}
              value={color ?? "#0284C7"}
              onChange={(event) => {
                setDraft(event.target.value);
                setAutomatic(false);
              }}
            />
          </label>
          <label>
            Hex color
            <Input
              aria-label="Hex color"
              value={draft}
              disabled={busy || retry !== null}
              onChange={(event) => {
                setDraft(event.target.value);
                setAutomatic(false);
              }}
            />
          </label>
          <Button
            variant="outline"
            disabled={busy || retry !== null}
            onClick={() => setAutomatic(true)}
            aria-pressed={automatic}
          >
            Reset to automatic
          </Button>
          {error ? <p role="alert">{error}</p> : null}
        </div>
        <DialogFooter>
          <Button variant="outline" disabled={busy} onClick={props.onClose}>
            Cancel
          </Button>
          <Button disabled={busy || (!automatic && !color)} onClick={() => void save()}>
            {busy ? "Saving…" : retry ? "Retry save" : "Save"}
          </Button>
        </DialogFooter>
      </DialogPopup>
    </Dialog>
  );
}
