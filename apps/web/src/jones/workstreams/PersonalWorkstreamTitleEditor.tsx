import { useEffect, useId, useRef, useState } from "react";

export interface PersonalWorkstreamTitleEditorProps {
  name: string;
  pending: boolean;
  error?: string | undefined;
  onSave: (name: string) => void | Promise<void>;
  onCancel: () => void;
}

// Mount for each rename session; server updates must not replace an unsaved draft.
export function PersonalWorkstreamTitleEditor({
  name,
  pending,
  error,
  onSave,
  onCancel,
}: PersonalWorkstreamTitleEditorProps) {
  const [draft, setDraft] = useState(name);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string>();
  const inputRef = useRef<HTMLInputElement>(null);
  const settledRef = useRef(false);
  const errorId = useId();
  const locked = pending || saving;
  const message = error || saveError;

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Restore DOM input focus after a failed save has unlocked editing.
  useEffect(() => {
    // A blur save can fail after focus moved elsewhere; return to the retained draft.
    if (message && !locked) {
      settledRef.current = false;
      inputRef.current?.focus();
    }
  }, [message, locked]);

  const save = () => {
    if (locked || settledRef.current) return;
    const trimmed = draft.trim();
    if (!trimmed || trimmed.length > 160) {
      setSaveError(!trimmed ? "Workstream name cannot be empty." : "Use 160 characters or fewer.");
      inputRef.current?.focus();
      return;
    }
    settledRef.current = true;
    setSaving(true);
    setSaveError(undefined);
    void Promise.resolve()
      .then(() => onSave(trimmed))
      .catch(() => {
        settledRef.current = false;
        setSaveError("Couldn't rename the Workstream. Try again.");
      })
      .finally(() => {
        settledRef.current = false;
        setSaving(false);
      });
  };

  return (
    <div className="min-w-0">
      <input
        ref={inputRef}
        autoFocus
        value={draft}
        dir="auto"
        maxLength={160}
        readOnly={locked}
        aria-label="Workstream name"
        aria-busy={locked}
        aria-invalid={Boolean(message)}
        aria-describedby={message ? errorId : undefined}
        onChange={(event) => {
          settledRef.current = false;
          setDraft(event.target.value);
          setSaveError(undefined);
        }}
        onFocus={(event) => event.currentTarget.select()}
        onKeyDown={(event) => {
          event.stopPropagation();
          if (event.nativeEvent.isComposing || event.keyCode === 229) return;
          if (event.key === "Enter") {
            event.preventDefault();
            save();
          } else if (event.key === "Escape") {
            event.preventDefault();
            if (locked) return;
            settledRef.current = true;
            onCancel();
          }
        }}
        onBlur={save}
        onPointerDown={(event) => event.stopPropagation()}
        onClick={(event) => event.stopPropagation()}
        onDoubleClick={(event) => event.stopPropagation()}
        onDragStart={(event) => {
          event.preventDefault();
          event.stopPropagation();
        }}
        className="w-full min-w-0 rounded-sm border border-input bg-card px-1 text-sm font-semibold leading-5 text-card-foreground outline-none focus:border-foreground"
      />
      {message ? (
        <span id={errorId} role="alert" className="block text-xs text-thread-failed">
          {message}
        </span>
      ) : null}
    </div>
  );
}
