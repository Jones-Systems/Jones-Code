import { PauseIcon, PlayIcon } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Textarea } from "../ui/textarea";
import { Badge } from "../ui/badge";
import { Alert, AlertDescription, AlertTitle } from "../ui/alert";
import { type WorkQueueItem, type WorkQueueSource } from "./mockWorkQueue";

function submittedTime(value: string | null) {
  if (!value) return "Submission time unavailable";
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? "Submission time unavailable"
    : new Intl.DateTimeFormat("en-US", {
        timeZone: "America/New_York",
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
        timeZoneName: "short",
      }).format(date);
}

function queueStatus(item: WorkQueueItem) {
  if (item.editability === "readonly")
    return item.statusLabel === "Held" ? "Blocked" : item.statusLabel;
  if (item.pause === "manual") return "Paused";
  if (item.pause === "editing") return "Paused while editing";
  if (item.pause === "grace") return "Paused · 2-minute grace";
  return "Ready";
}

type Editor = { base: WorkQueueItem; draft: string; current?: WorkQueueItem };

export function WorkQueuePanel({
  source,
  onDirtyChange,
}: {
  source: WorkQueueSource;
  onDirtyChange?: (dirty: boolean) => void;
}) {
  const [items, setItems] = useState<readonly WorkQueueItem[]>([]);
  const [editor, setEditor] = useState<Editor | null>(null);
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState("all");
  const [switchTo, setSwitchTo] = useState<WorkQueueItem | null>(null);
  const [message, setMessage] = useState("");
  const [saving, setSaving] = useState(false);
  const [loading, setLoading] = useState(false);
  const [unknown, setUnknown] = useState(false);
  const generation = useRef(0);
  const saveInFlight = useRef(false);
  const readRequest = useRef(0);
  const mutation = useRef(0);
  const editorRef = useRef(editor);
  editorRef.current = editor;
  const dirty = !!editor && editor.draft !== (editor.base.text ?? "");
  useEffect(() => {
    onDirtyChange?.(dirty);
  }, [dirty, onDirtyChange]);

  const refresh = useCallback(async () => {
    const scope = generation.current;
    const request = ++readRequest.current;
    const version = mutation.current;
    setLoading(true);
    try {
      const next = await source.load();
      if (
        scope !== generation.current ||
        request !== readRequest.current ||
        version !== mutation.current
      )
        return;
      setItems(next);
      setUnknown(false);
      const existing = editorRef.current;
      const current = next.find((item) => item.id === existing?.base.id);
      if (!existing) {
        if (next[0]) setEditor({ base: next[0], draft: next[0].text ?? "" });
      } else if (!current) {
        setMessage("This submission is unavailable. Your draft is preserved.");
        setUnknown(true);
      } else if (existing.draft !== (existing.base.text ?? "")) {
        if (current.snapshotToken !== existing.base.snapshotToken) {
          setEditor({ ...existing, current });
          setMessage("The submission changed. Compare the current text with your preserved draft.");
        } else {
          setMessage("Readback complete. Your draft is preserved.");
        }
      } else {
        setEditor({ base: current, draft: current.text ?? "" });
      }
    } catch {
      if (
        scope === generation.current &&
        request === readRequest.current &&
        version === mutation.current
      )
        setMessage("Could not load sample submissions. Your draft is preserved.");
    } finally {
      if (scope === generation.current && request === readRequest.current) setLoading(false);
    }
  }, [source]);

  useEffect(() => {
    generation.current++;
    mutation.current++;
    editorRef.current = null;
    setEditor(null);
    setItems([]);
    setSwitchTo(null);
    setMessage("");
    setUnknown(false);
    setSaving(false);
    saveInFlight.current = false;
    void refresh();
    return () => {
      generation.current++;
    };
  }, [refresh]);

  function updateItem(item: WorkQueueItem) {
    setItems((previous) => previous.map((row) => (row.id === item.id ? item : row)));
    setEditor((previous) =>
      previous?.base.id === item.id
        ? {
            ...previous,
            base: { ...previous.base, pause: item.pause, graceUntil: item.graceUntil },
          }
        : previous,
    );
  }
  const protectedId =
    editor && (dirty || saving || unknown || editor.current) ? editor.base.id : null;
  useEffect(() => {
    if (saving) return;
    const deadlines = items
      .filter(
        (item) =>
          item.id !== protectedId && item.pause === "grace" && item.graceUntil !== undefined,
      )
      .map((item) => item.graceUntil!);
    if (!deadlines.length) return;
    const timer = setTimeout(
      () => {
        if (saveInFlight.current) return;
        mutation.current++;
        const next = source.tick(protectedId ? [protectedId] : []);
        setItems(next);
        setEditor((previous) => {
          const current = next.find((item) => item.id === previous?.base.id);
          return previous && current
            ? {
                ...previous,
                base: { ...previous.base, pause: current.pause, graceUntil: current.graceUntil },
              }
            : previous;
        });
      },
      Math.max(1, Math.min(...deadlines) - Date.now()),
    );
    return () => clearTimeout(timer);
  }, [items, protectedId, source, saving]);
  function discard(item: WorkQueueItem) {
    mutation.current++;
    const updated = source.finishEdit(item.id);
    const next = updated ? { ...item, pause: updated.pause, graceUntil: updated.graceUntil } : item;
    updateItem(next);
    open(next);
  }
  function togglePause(item: WorkQueueItem, paused = item.pause !== "manual") {
    if (
      saving ||
      (!paused && (unknown || (item.id === editor?.base.id && (dirty || editor.current))))
    )
      return;
    mutation.current++;
    const updated = source.setManualPause(item.id, paused);
    if (updated) updateItem(updated);
  }
  function sendNow() {
    if (
      !editor ||
      dirty ||
      saving ||
      unknown ||
      editor.current ||
      editor.base.editability !== "editable"
    )
      return;
    mutation.current++;
    const result = source.sendNow({ id: editor.base.id, baseToken: editor.base.snapshotToken });
    if (result.kind === "saved") {
      if (result.item.id !== editor.base.id || result.item.text !== editor.base.text) {
        setUnknown(true);
        setMessage("Submission acknowledgment did not match. Read back before further action.");
        return;
      }
      updateItem(result.item);
      open(result.item);
      setMessage("Submitted once in this simulation. No real work was sent.");
    } else if (result.kind === "conflict" || result.kind === "held") {
      setEditor({ ...editor, current: result.current });
      setMessage(
        result.kind === "held"
          ? result.reason
          : "The submission changed. Reload current text before sending.",
      );
    } else {
      setUnknown(result.effect === "unknown");
      setMessage(result.message);
    }
  }
  function select(item: WorkQueueItem) {
    if (saving || unknown || item.id === editor?.base.id) return;
    if (dirty) {
      setSwitchTo(item);
      return;
    }
    open(item);
  }
  function open(item: WorkQueueItem) {
    setEditor({ base: item, draft: item.text ?? "" });
    setSwitchTo(null);
    setMessage("");
    setUnknown(false);
  }
  async function save() {
    if (
      !editor ||
      !dirty ||
      !editor.draft.trim() ||
      saving ||
      saveInFlight.current ||
      unknown ||
      editor.current ||
      editor.base.editability !== "editable"
    )
      return;
    const scope = generation.current;
    const version = ++mutation.current;
    const submitted = editor;
    saveInFlight.current = true;
    setSaving(true);
    setMessage("");
    try {
      const result = await source.save({
        id: submitted.base.id,
        baseToken: submitted.base.snapshotToken,
        text: submitted.draft,
      });
      if (scope !== generation.current || version !== mutation.current) return;
      if (result.kind === "saved") {
        if (result.item.id !== submitted.base.id || result.item.text !== submitted.draft) {
          setUnknown(true);
          setMessage(
            "Save acknowledgment did not match your draft. Read back before saving again.",
          );
          return;
        }
        setItems((previous) =>
          previous.map((item) => (item.id === result.item.id ? result.item : item)),
        );
        setEditor({ base: result.item, draft: result.item.text ?? "" });
        setMessage("Mock edit saved in this preview.");
      } else if (result.kind === "conflict" || result.kind === "held") {
        setItems((previous) =>
          previous.map((item) => (item.id === result.current.id ? result.current : item)),
        );
        setEditor({ ...submitted, current: result.current });
        setMessage(
          result.kind === "held"
            ? result.reason
            : "The submission changed. Compare the current text with your preserved draft.",
        );
      } else {
        setUnknown(result.effect === "unknown");
        setMessage(result.message);
      }
    } catch {
      if (scope === generation.current && version === mutation.current) {
        setUnknown(true);
        setMessage(
          "Save effect is unknown. Your draft is preserved. Read back before saving again.",
        );
      }
    } finally {
      if (scope === generation.current && version === mutation.current) {
        saveInFlight.current = false;
        setSaving(false);
      }
    }
  }
  const recent = items.filter(
    (item) => item.statusLabel === "Delivered" || item.statusLabel === "Submitted (mock)",
  );
  const queued = items.filter(
    (item) => item.statusLabel !== "Delivered" && item.statusLabel !== "Submitted (mock)",
  );
  const visible = queued.filter(
    (item) =>
      (filter === "all" || item.editability === filter) &&
      `${item.text ?? ""} ${item.targetLabel ?? ""} ${queueStatus(item)}`
        .toLowerCase()
        .includes(query.toLowerCase()),
  );
  const editableCount = items.filter((item) => item.editability === "editable").length;
  return (
    <div className="flex flex-col gap-6">
      <Alert variant="info">
        <AlertTitle>Sample data</AlertTitle>
        <AlertDescription>
          Sample data. Edits apply only to this preview. Pause, resume, and send are local
          simulations.
        </AlertDescription>
      </Alert>
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Submitted work</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            Review submitted text, its target, and whether it can still be edited.
          </p>
        </div>
        <Button variant="outline" disabled={saving || loading} onClick={() => void refresh()}>
          {loading ? "Loading…" : unknown ? "Read back sample data" : "Refresh sample data"}
        </Button>
      </div>
      <div className="flex flex-wrap gap-3 text-sm text-muted-foreground">
        <span>{items.length} submissions</span>
        <span>·</span>
        <span>{editableCount} editable</span>
        <span>·</span>
        <span>{items.length - editableCount} read-only</span>
      </div>
      <div className="grid min-w-0 gap-6 lg:grid-cols-[minmax(260px,0.8fr)_minmax(0,1.4fr)]">
        <section aria-label="Queued work" className="flex min-w-0 flex-col gap-3">
          <h2 className="text-lg font-semibold">Queued work</h2>
          <Input
            aria-label="Search submissions"
            placeholder="Search text or target…"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
          <div className="flex flex-wrap gap-2">
            {[
              ["all", "All"],
              ["editable", "Editable"],
              ["readonly", "Read-only"],
            ].map(([value, label]) => (
              <Button
                key={value}
                size="sm"
                variant={filter === value ? "secondary" : "ghost"}
                aria-pressed={filter === value}
                onClick={() => setFilter(value!)}
              >
                {label}
              </Button>
            ))}
          </div>
          <div className="overflow-hidden rounded-xl border border-border">
            {visible.map((item) => (
              <div
                key={item.id}
                className={`flex gap-3 border-b border-border p-4 last:border-0 ${item.id === editor?.base.id ? "bg-muted/60" : "bg-card"}`}
              >
                <button
                  type="button"
                  disabled={saving || unknown}
                  aria-pressed={item.id === editor?.base.id}
                  onClick={() => select(item)}
                  className="min-w-0 flex-1 text-left hover:text-primary disabled:opacity-60"
                >
                  <Badge variant="outline">{queueStatus(item)}</Badge>
                  <p className="mt-2 line-clamp-2 break-words text-sm font-medium">
                    {item.text ?? "Text unavailable"}
                  </p>
                  <p className="mt-2 break-words text-xs text-muted-foreground">
                    {item.targetLabel ?? "Target unavailable"}
                  </p>
                  {item.reason && item.editability === "readonly" && (
                    <p className="mt-2 text-xs text-muted-foreground">{item.reason}</p>
                  )}
                </button>
                <span className="max-w-24 shrink-0 text-right text-xs text-muted-foreground">
                  {submittedTime(item.submittedAt)}
                </span>
                {item.editability === "editable" && (
                  <div className="flex shrink-0 flex-col items-center justify-center gap-2 self-stretch">
                    <Button
                      size="icon"
                      variant="outline"
                      aria-label={`${item.pause ? "Resume" : "Pause"} ${item.id}`}
                      title={item.pause ? "Resume" : "Pause"}
                      disabled={
                        saving ||
                        (!!item.pause &&
                          (unknown || (item.id === editor?.base.id && (dirty || !!editor.current))))
                      }
                      onClick={() => togglePause(item, !item.pause)}
                    >
                      {item.pause ? (
                        <PlayIcon aria-hidden="true" />
                      ) : (
                        <PauseIcon aria-hidden="true" />
                      )}
                    </Button>
                    {(item.pause === "editing" || item.pause === "grace") && (
                      <Button
                        size="micro"
                        variant="ghost"
                        aria-label={`Pause ${item.id}`}
                        title="Keep paused until you resume"
                        disabled={saving}
                        onClick={() => togglePause(item, true)}
                      >
                        Pause indefinitely
                      </Button>
                    )}
                  </div>
                )}
              </div>
            ))}
            {!visible.length && (
              <p className="p-5 text-sm text-muted-foreground">
                {loading ? "Loading sample submissions…" : "No matching submissions."}
              </p>
            )}
          </div>
        </section>
        <section
          aria-label="Submission details"
          className="min-w-0 rounded-xl border border-border bg-card p-5"
        >
          {switchTo && (
            <div className="mb-5 flex flex-col gap-3">
              <Alert variant="warning">
                <AlertTitle>Unsaved changes</AlertTitle>
                <AlertDescription>Switching submissions will discard your draft.</AlertDescription>
              </Alert>
              <div className="flex flex-wrap gap-2">
                <Button variant="outline" onClick={() => setSwitchTo(null)}>
                  Keep editing
                </Button>
                <Button
                  onClick={() => {
                    if (editor) discard(editor.current ?? editor.base);
                    open(switchTo);
                  }}
                >
                  Discard changes and switch
                </Button>
              </div>
            </div>
          )}
          {editor ? (
            <div className="flex flex-col gap-5">
              <div className="flex items-center justify-between gap-2">
                <h2 className="text-lg font-semibold">Submission details</h2>
                <Badge variant="outline">{queueStatus(editor.base)}</Badge>
              </div>
              <dl className="grid gap-3 text-sm">
                <div>
                  <dt className="text-muted-foreground">Target</dt>
                  <dd className="mt-1 break-words">
                    {editor.base.targetLabel ?? "Target unavailable"}
                  </dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Submitted</dt>
                  <dd className="mt-1">{submittedTime(editor.base.submittedAt)}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Submission ID</dt>
                  <dd className="mt-1 font-mono text-xs">{editor.base.id}</dd>
                </div>
              </dl>
              {editor.base.editability === "readonly" && (
                <Alert>
                  <AlertTitle>Read-only</AlertTitle>
                  <AlertDescription>
                    {editor.base.reason ?? "This submission cannot be edited."}
                  </AlertDescription>
                </Alert>
              )}
              {message && (
                <Alert variant={unknown || editor.current ? "warning" : "info"}>
                  <AlertDescription>{message}</AlertDescription>
                </Alert>
              )}
              {unknown && (
                <p className="text-sm text-muted-foreground">
                  The save effect is unknown. Use “Read back sample data” to reconcile it. Your
                  draft stays here.
                </p>
              )}
              {editor.current && (
                <div className="flex flex-col gap-3">
                  <h3 className="text-sm font-medium">Current submitted text</h3>
                  <pre className="whitespace-pre-wrap break-words rounded-lg bg-muted/40 p-3 text-sm font-sans">
                    {editor.current.text ?? "Text unavailable"}
                  </pre>
                  <Button
                    variant="outline"
                    disabled={saving}
                    onClick={() => discard(editor.current!)}
                  >
                    Reload current text and discard draft
                  </Button>
                </div>
              )}
              {editor.base.editability === "editable" ? (
                <>
                  <label htmlFor="work-queue-draft" className="text-sm font-medium">
                    {editor.current ? "Your preserved draft" : "Submitted text"}
                  </label>
                  <Textarea
                    id="work-queue-draft"
                    value={editor.draft}
                    disabled={saving}
                    onChange={(event) => {
                      const paused =
                        event.target.value === (editor.base.text ?? "")
                          ? source.finishEdit(editor.base.id)
                          : source.beginEdit(editor.base.id);
                      mutation.current++;
                      if (paused) updateItem(paused);
                      setEditor({
                        ...editor,
                        base: paused
                          ? { ...editor.base, pause: paused.pause, graceUntil: paused.graceUntil }
                          : editor.base,
                        draft: event.target.value,
                      });
                    }}
                    onKeyDown={(event) => {
                      if (
                        event.key !== "Enter" ||
                        event.shiftKey ||
                        event.nativeEvent.isComposing ||
                        event.repeat
                      )
                        return;
                      event.preventDefault();
                      void save();
                    }}
                    rows={8}
                  />
                  <div className="flex flex-wrap items-center gap-2">
                    <Button
                      disabled={
                        !dirty || !editor.draft.trim() || saving || unknown || !!editor.current
                      }
                      onClick={() => void save()}
                    >
                      {saving ? "Saving…" : "Save mock edit"}
                    </Button>
                    <Button
                      variant="outline"
                      disabled={!dirty || saving || unknown}
                      onClick={() => discard(editor.current ?? editor.base)}
                    >
                      Discard changes
                    </Button>
                    <Button
                      variant="outline"
                      disabled={
                        dirty ||
                        saving ||
                        unknown ||
                        !!editor.current ||
                        editor.base.pause === "editing"
                      }
                      onClick={sendNow}
                    >
                      Send now (mock)
                    </Button>
                    <span className="text-xs text-muted-foreground">
                      {dirty ? "Unsaved changes" : "No unsaved changes"}
                    </span>
                  </div>
                  <p className="text-xs text-muted-foreground">
                    Enter updates text. Shift+Enter adds a line. Automatic edit pauses last two
                    minutes after an update or discard; expiry only makes it ready. Manual pauses
                    last until you resume or send.
                  </p>
                </>
              ) : (
                <>
                  <h3 className="text-sm font-medium">Submitted text</h3>
                  <pre className="whitespace-pre-wrap break-words text-sm font-sans">
                    {editor.base.text ?? "Text unavailable"}
                  </pre>
                </>
              )}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">
              Select a submission to review its full text.
            </p>
          )}
        </section>
      </div>
      <section aria-label="Recently submitted" className="flex flex-col gap-3">
        <h2 className="text-lg font-semibold">Recently submitted</h2>
        <p className="text-sm text-muted-foreground">
          Sample deliveries and explicit mock submissions appear here after leaving the queue. This
          history is read-only; nothing is dispatched automatically.
        </p>
        {recent.map((item) => (
          <article key={item.id} className="rounded-xl border border-border bg-card p-4">
            <div className="flex flex-wrap justify-between gap-2">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="outline">{item.statusLabel}</Badge>
                <span className="text-sm font-medium">
                  {item.targetLabel ?? "Target unavailable"}
                </span>
              </div>
              <span className="text-xs text-muted-foreground">
                {submittedTime(item.submittedAt)}
              </span>
            </div>
            <pre className="mt-3 whitespace-pre-wrap break-words font-sans text-sm">
              {item.text ?? "Text unavailable"}
            </pre>
          </article>
        ))}
        {!recent.length && <p className="text-sm text-muted-foreground">No submissions yet.</p>}
      </section>
    </div>
  );
}
