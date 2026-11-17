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

type Thread = ThreadRegistryComposedSnapshot["threads"][number];

function textField(record: Readonly<Record<string, unknown>> | null, key: string) {
  const value = record?.[key];
  return typeof value === "string" && value.length > 0 ? value : null;
}

export function recentThread(entry: VoiceReviewRecentEntry, threads: readonly Thread[]) {
  if (!entry.thread_key) return null;
  return threads.find((thread) => thread.thread_key === entry.thread_key) ?? null;
}

/** The registry purpose of the prompt's thread, when the verified thread key joins one. */
export function recentThreadPurpose(thread: Thread | null) {
  return textField(thread?.registration ?? null, "purpose");
}

function correctionState(actions: RegistryCorrectionActions) {
  return { busy: actions.busy, uncertain: actions.uncertain, error: actions.error };
}

/**
 * Workstream associations for one released voice prompt. Native memberships are read-only;
 * only `inferred:` suggestions can be removed or added, and only once the command and
 * association revisions are observed. An uncertain correction blocks further changes until a
 * successful refresh, and is never retried.
 */
export function VoiceWorkstreamCorrections({
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
  const [actionState, setActionState] = useState(() => correctionState(actions));
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
  const native = [...(thread?.native_memberships ?? []), ...(thread?.personal_memberships ?? [])];
  const refs = new Set([...entry.workstream_refs, ...native.map((item) => item.workstream_ref)]);
  for (const record of associations.values()) {
    if (record.state === "active") refs.add(record.workstream_ref);
    else refs.delete(record.workstream_ref);
  }
  const disabled = unavailable || actionState.busy || actionState.uncertain;
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
    setActionState(correctionState(actions));
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
    setActionState(correctionState(actions));
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
    setActionState(correctionState(actions));
  };
  const summary = textField(thread?.summary ?? null, "text");
  const options = new Map<string, string>();
  for (const record of registry?.threads ?? []) {
    for (const item of record.personal_memberships ?? [])
      options.set(item.workstream_ref, item.name);
  }
  for (const label of workstreams?.workstreams ?? []) {
    if (label.state === "active") options.set(label.label_id, label.name);
  }
  return (
    <div className="flex min-w-0 flex-col gap-2 text-xs">
      {thread?.freshness.stale === true ? (
        <p className="text-muted-foreground">Thread context is stale.</p>
      ) : null}
      {summary ? <p className="text-muted-foreground wrap-anywhere">{summary}</p> : null}
      <div className="flex flex-wrap gap-2" aria-label="Workstreams">
        {[...refs].map((ref) => {
          const record = [...associations.values()].find(
            (item) => item.workstream_ref === ref && item.state === "active",
          );
          const readonly = ref.startsWith("native:");
          return (
            <span
              key={ref}
              className="group flex items-center gap-1 rounded-full border px-2 py-1 wrap-anywhere"
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
      <div className="flex flex-wrap items-end gap-2">
        <label className="min-w-0 flex-1">
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
      <p className="text-muted-foreground">Workstream corrections never resend prompts.</p>
      {!subject || entry.associations === undefined ? (
        <p className="text-muted-foreground">
          Prompt corrections are unavailable until command and association revisions are observed.
        </p>
      ) : null}
      {actionState.error || refreshError ? (
        <p role="alert" className="text-sm">
          {refreshError
            ? "Workstream refresh unavailable. Correction remains unconfirmed."
            : actionState.error}
        </p>
      ) : null}
      {actionState.uncertain ? (
        <div>
          <Button variant="outline" size="compact" onClick={() => void refresh()}>
            Check current workstreams
          </Button>
        </div>
      ) : null}
    </div>
  );
}
