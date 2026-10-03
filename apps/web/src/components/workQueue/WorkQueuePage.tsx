import { useCallback, useEffect, useRef, useState } from "react";
import { useBlocker } from "@tanstack/react-router";
import { isElectron } from "../../env";
import { WorkspacePageHeader } from "../WorkspacePageHeader";
import { WorkspacePageContainer } from "../WorkspacePageContainer";
import { SidebarInset } from "../ui/sidebar";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Textarea } from "../ui/textarea";
import { Badge } from "../ui/badge";
import { Alert, AlertAction, AlertDescription, AlertTitle } from "../ui/alert";
import {
  createMockWorkQueueSource,
  type WorkQueueItem,
  type WorkQueueSource,
} from "./mockWorkQueue";

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
    void refresh();
    return () => {
      generation.current++;
    };
  }, [refresh]);

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
      saving ||
      unknown ||
      editor.current ||
      editor.base.editability !== "editable"
    )
      return;
    const scope = generation.current;
    const version = ++mutation.current;
    const submitted = editor;
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
      if (scope === generation.current && version === mutation.current) setSaving(false);
    }
  }
  const visible = items.filter(
    (item) =>
      (filter === "all" || item.editability === filter) &&
      `${item.text ?? ""} ${item.targetLabel ?? ""} ${item.statusLabel}`
        .toLowerCase()
        .includes(query.toLowerCase()),
  );
  const editableCount = items.filter((item) => item.editability === "editable").length;
  return (
    <div className="flex flex-col gap-6">
      <Alert variant="info">
        <AlertTitle>Sample data</AlertTitle>
        <AlertDescription>Sample data. Edits apply only to this preview.</AlertDescription>
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
        <section aria-label="Submissions" className="flex min-w-0 flex-col gap-3">
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
              <button
                key={item.id}
                type="button"
                disabled={saving || unknown}
                aria-pressed={item.id === editor?.base.id}
                onClick={() => select(item)}
                className={`block w-full border-b border-border p-4 text-left last:border-0 hover:bg-muted/40 disabled:opacity-60 ${item.id === editor?.base.id ? "bg-muted/60" : "bg-card"}`}
              >
                <div className="mb-2 flex items-center justify-between gap-2">
                  <Badge variant="outline">{item.statusLabel}</Badge>
                  <span className="text-xs text-muted-foreground">
                    {submittedTime(item.submittedAt)}
                  </span>
                </div>
                <p className="line-clamp-2 break-words text-sm font-medium">
                  {item.text ?? "Text unavailable"}
                </p>
                <p className="mt-2 text-xs text-muted-foreground">
                  {item.targetLabel ?? "Target unavailable"}
                </p>
              </button>
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
                <Button onClick={() => open(switchTo)}>Discard changes and switch</Button>
              </div>
            </div>
          )}
          {editor ? (
            <div className="flex flex-col gap-5">
              <div className="flex items-center justify-between gap-2">
                <h2 className="text-lg font-semibold">Submission details</h2>
                <Badge variant="outline">{editor.base.statusLabel}</Badge>
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
                  <Button variant="outline" disabled={saving} onClick={() => open(editor.current!)}>
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
                    onChange={(event) => setEditor({ ...editor, draft: event.target.value })}
                    rows={8}
                  />
                  <div className="flex flex-wrap items-center gap-2">
                    <Button
                      disabled={!dirty || saving || unknown || !!editor.current}
                      onClick={() => void save()}
                    >
                      {saving ? "Saving…" : "Save mock edit"}
                    </Button>
                    <Button
                      variant="outline"
                      disabled={!dirty || saving || unknown}
                      onClick={() => open(editor.current ?? editor.base)}
                    >
                      Discard changes
                    </Button>
                    <span className="text-xs text-muted-foreground">
                      {dirty ? "Unsaved changes" : "No unsaved changes"}
                    </span>
                  </div>
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
    </div>
  );
}

export function WorkQueuePage() {
  const [source] = useState(createMockWorkQueueSource);
  const [dirty, setDirty] = useState(false);
  const blocker = useBlocker({
    shouldBlockFn: () => dirty,
    enableBeforeUnload: () => dirty,
    withResolver: true,
  });
  return (
    <SidebarInset className="h-dvh overflow-hidden">
      <WorkspacePageHeader electron={isElectron}>
        <span className="text-sm font-medium">Submitted work</span>
      </WorkspacePageHeader>
      <div className="min-h-0 flex-1 overflow-y-auto">
        <WorkspacePageContainer width="expanded">
          {blocker.status === "blocked" && (
            <Alert variant="warning">
              <AlertTitle>Leave this preview?</AlertTitle>
              <AlertDescription>Your unsaved draft will be discarded.</AlertDescription>
              <AlertAction>
                <Button variant="outline" onClick={() => blocker.reset()}>
                  Keep editing
                </Button>
                <Button onClick={() => blocker.proceed()}>Discard changes and leave</Button>
              </AlertAction>
            </Alert>
          )}
          <WorkQueuePanel source={source} onDirtyChange={setDirty} />
        </WorkspacePageContainer>
      </div>
    </SidebarInset>
  );
}
