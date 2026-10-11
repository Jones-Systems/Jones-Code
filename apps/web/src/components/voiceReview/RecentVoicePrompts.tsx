import type {
  ThreadRegistryAssociation,
  ThreadRegistryComposedSnapshot,
  ThreadRegistryWorkstreams,
  VoiceReviewRecentEntry,
} from "@t3tools/contracts";
import { useState } from "react";
import { XIcon } from "lucide-react";
import { randomUUID } from "../../lib/utils";
import { Button } from "../ui/button";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { RegistryCorrectionActions, type RegistryCorrectionTransport } from "./voiceReviewActions";

const promptTimeFormatter = new Intl.DateTimeFormat("en-US", {
  month: "short",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
  timeZone: "America/New_York",
  timeZoneName: "short",
});

type Thread = ThreadRegistryComposedSnapshot["threads"][number];

function textField(record: Readonly<Record<string, unknown>> | null, key: string) {
  const value = record?.[key];
  return typeof value === "string" && value.length > 0 ? value : null;
}

export function recentThread(entry: VoiceReviewRecentEntry, threads: readonly Thread[]) {
  if (!entry.thread_key) return null;
  return threads.find((thread) => thread.thread_key === entry.thread_key) ?? null;
}

export function RecentVoicePrompts({
  entries,
  registry,
  workstreams,
  transport,
  onRefresh,
  unavailable,
  title = "Recent prompts",
  emptyMessage = "No recent prompts observed.",
}: {
  entries: readonly VoiceReviewRecentEntry[];
  registry: ThreadRegistryComposedSnapshot | null;
  workstreams: ThreadRegistryWorkstreams | null;
  transport: RegistryCorrectionTransport;
  onRefresh: () => Promise<void>;
  unavailable: boolean;
  title?: string;
  emptyMessage?: string;
}) {
  return (
    <aside
      className="flex min-w-0 flex-col gap-3 rounded-xl border bg-muted/20 p-4"
      aria-label="Recent voice prompts"
    >
      <h2 className="font-medium">{title}</h2>
      <p className="text-sm text-muted-foreground">
        Prompt history is read-only. Workstream corrections never resend prompts.
      </p>
      {entries.length === 0 ? (
        <p className="text-sm text-muted-foreground">{emptyMessage}</p>
      ) : null}
      {entries.map((entry) => (
        <RecentPromptRow
          key={entry.draft.id}
          entry={entry}
          thread={recentThread(entry, registry?.threads ?? [])}
          registry={registry}
          workstreams={workstreams}
          transport={transport}
          onRefresh={onRefresh}
          unavailable={unavailable}
        />
      ))}
    </aside>
  );
}

function RecentPromptRow({
  entry,
  thread,
  registry,
  workstreams,
  transport,
  onRefresh,
  unavailable,
}: {
  entry: VoiceReviewRecentEntry;
  thread: Thread | null;
  registry: ThreadRegistryComposedSnapshot | null;
  workstreams: ThreadRegistryWorkstreams | null;
  transport: RegistryCorrectionTransport;
  onRefresh: () => Promise<void>;
  unavailable: boolean;
}) {
  const [selected, setSelected] = useState("");
  const [actions] = useState(() => new RegistryCorrectionActions(transport));
  const [, render] = useState(0);
  const [acknowledged, setAcknowledged] = useState<readonly ThreadRegistryAssociation[]>([]);
  const [refreshError, setRefreshError] = useState(false);
  const subject = entry.command_id ? `prompt:${entry.command_id}` : null;
  const associations = new Map<string, ThreadRegistryAssociation>();
  for (const record of entry.associations ?? [])
    associations.set(`${record.subject}|${record.workstream_ref}`, record);
  for (const record of acknowledged) {
    const key = `${record.subject}|${record.workstream_ref}`;
    if ((associations.get(key)?.revision ?? 0) < record.revision) associations.set(key, record);
  }
  const native = thread?.native_memberships ?? [];
  const refs = new Set([...entry.workstream_refs, ...native.map((item) => item.workstream_ref)]);
  for (const record of associations.values()) {
    if (record.state === "active") refs.add(record.workstream_ref);
    else refs.delete(record.workstream_ref);
  }
  const disabled = unavailable || actions.busy || actions.uncertain;
  const correct = async (
    ref: string,
    state: "active" | "suppressed",
    record?: ThreadRegistryAssociation,
  ) => {
    if (disabled) return;
    const target = record?.subject ?? subject;
    if (!target || !ref.startsWith("inferred:") || entry.associations === undefined) return;
    const pending = actions.correct({
      schema: "voice.association-mutation/v1",
      subject: target,
      workstream_ref: ref,
      state,
      expected_revision: record?.revision ?? 0,
      request_id: randomUUID(),
      command_id: entry.command_id,
    });
    render((value) => value + 1);
    const receipt = await pending;
    if (receipt && "subject" in receipt.record) {
      const updated = receipt.record;
      setAcknowledged((records) => [
        ...records.filter(
          (item) =>
            !(item.subject === updated.subject && item.workstream_ref === updated.workstream_ref),
        ),
        updated,
      ]);
      setSelected("");
    }
    render((value) => value + 1);
  };
  const refresh = async () => {
    try {
      await onRefresh();
      actions.reconciled();
      setAcknowledged([]);
      setRefreshError(false);
    } catch {
      setRefreshError(true);
    }
    render((value) => value + 1);
  };
  const title =
    textField(thread?.registration ?? null, "purpose") ??
    entry.draft.routing_target ??
    "Thread not yet observed";
  const summary = textField(thread?.summary ?? null, "text");
  const options = new Map<string, string>();
  for (const record of registry?.threads ?? []) {
    for (const item of record.native_memberships)
      options.set(item.workstream_ref, item.placement.workstream_id);
  }
  for (const label of workstreams?.workstreams ?? []) {
    if (label.state === "active") options.set(label.label_id, label.name);
  }
  return (
    <article className="flex flex-col gap-3 rounded-lg border bg-background p-4">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <h3 className="break-words text-sm font-medium">{title}</h3>
        <span className="text-xs text-muted-foreground">{entry.draft.state}</span>
      </div>
      {entry.text_state === "available" && entry.text !== null ? (
        <p className="whitespace-pre-wrap break-words text-sm">{entry.text}</p>
      ) : (
        <p className="text-sm text-muted-foreground">
          {entry.text_state === "deleted"
            ? "Prompt text was deleted."
            : entry.text_state === "expired"
              ? "Prompt text has expired."
              : "Prompt text is unavailable."}
        </p>
      )}
      <time dateTime={entry.draft.created_at} className="text-xs text-muted-foreground">
        {promptTimeFormatter.format(new Date(entry.draft.created_at))}
      </time>
      {thread?.freshness.stale === true ? (
        <p className="text-xs text-muted-foreground">Thread context is stale.</p>
      ) : null}
      <div className="flex flex-wrap gap-2" aria-label="Workstreams">
        {[...refs].map((ref) => {
          const record = [...associations.values()].find(
            (item) => item.workstream_ref === ref && item.state === "active",
          );
          const readonly = ref.startsWith("native:");
          return (
            <span
              key={ref}
              className="group flex items-center gap-1 rounded-full border px-2 py-1 text-xs"
            >
              {options.get(ref) ?? ref}
              {readonly ? (
                <span className="text-muted-foreground"> · native</span>
              ) : (
                <Tooltip>
                  <TooltipTrigger
                    render={
                      <button
                        type="button"
                        disabled={disabled || !record || entry.associations === undefined}
                      />
                    }
                    className="rounded-sm opacity-0 group-hover:opacity-100 group-focus-within:opacity-100 focus-visible:opacity-100 focus-visible:outline-2 disabled:opacity-40"
                    aria-label={`Remove ${options.get(ref) ?? ref} workstream`}
                    disabled={disabled || !record || entry.associations === undefined}
                    onClick={() => void correct(ref, "suppressed", record)}
                  >
                    <XIcon className="size-3" />
                  </TooltipTrigger>
                  <TooltipPopup>
                    {record ? "Remove workstream association" : "Association revision unavailable"}
                  </TooltipPopup>
                </Tooltip>
              )}
            </span>
          );
        })}
      </div>
      <div className="flex flex-wrap items-center gap-2">
        <label className="min-w-0 flex-1 text-xs">
          Add existing workstream
          <select
            aria-label={`Add existing workstream to ${entry.draft.id}`}
            value={selected}
            disabled={disabled || !subject || !workstreams || entry.associations === undefined}
            onChange={(event) => setSelected(event.target.value)}
            className="mt-1 w-full rounded-md border bg-background p-2 text-sm"
          >
            <option value="">Choose a workstream…</option>
            {[...options].map(([ref, name]) => (
              <option key={ref} value={ref} disabled={ref.startsWith("native:") || refs.has(ref)}>
                {name}
                {ref.startsWith("native:") ? " (native · read-only)" : ""}
              </option>
            ))}
          </select>
        </label>
        <Button
          variant="outline"
          size="compact"
          disabled={disabled || !selected || !subject || entry.associations === undefined}
          onClick={() =>
            void correct(
              selected,
              "active",
              [...associations.values()].find(
                (item) => item.subject === subject && item.workstream_ref === selected,
              ),
            )
          }
        >
          Add
        </Button>
      </div>
      {!subject || entry.associations === undefined ? (
        <p className="text-xs text-muted-foreground">
          Prompt corrections are unavailable until command and association revisions are observed.
        </p>
      ) : null}
      {actions.error || refreshError ? (
        <p role="alert" className="text-sm">
          {refreshError
            ? "Workstream refresh unavailable. Correction remains unconfirmed."
            : actions.error}
        </p>
      ) : null}
      {actions.uncertain ? (
        <div>
          <Button variant="outline" size="compact" onClick={() => void refresh()}>
            Check current workstreams
          </Button>
        </div>
      ) : null}
      <details className="border-t pt-3 text-xs text-muted-foreground">
        <summary className="cursor-pointer">Prompt details</summary>
        <div className="mt-2 flex flex-col gap-2">
          <p>{summary ?? "Generated thread summary unavailable."}</p>
          <p>
            Text source:{" "}
            {entry.text_origin === "retained_command"
              ? "Retained command text"
              : entry.text_origin === "draft"
                ? "Draft text"
                : "Not available"}
          </p>
          {entry.original_source_text !== null && entry.original_source_text !== entry.text ? (
            <details>
              <summary className="cursor-pointer">Original transcript</summary>
              <p className="whitespace-pre-wrap break-words">{entry.original_source_text}</p>
            </details>
          ) : null}
          <p>
            Source: {entry.draft.source_id} · revision {entry.draft.revision}
          </p>
          <p>Thread key: {entry.thread_key ?? "not observed"}</p>
          <p>
            Routing: {entry.draft.routing_state ?? "not observed"} ·{" "}
            {entry.draft.routing_target ?? "target not observed"}
          </p>
          <p>
            Command: {entry.command_id ?? "not observed"} · status:{" "}
            {entry.draft.command_status ?? "not observed"}
          </p>
          <p>
            Release is a handoff for processing. Queue receipt and native execution evidence are
            unavailable in this view.
          </p>
        </div>
      </details>
    </article>
  );
}
