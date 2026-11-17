import type {
  ThreadRegistryAssociation,
  ThreadRegistryComposedSnapshot,
  ThreadRegistryWorkstreams,
  VoiceReviewRecentEntry,
} from "@t3tools/contracts";
import { useState, useSyncExternalStore } from "react";
import { ChevronsUpDownIcon, XIcon } from "lucide-react";
import { randomUUID } from "../../lib/utils";
import { Badge } from "../ui/badge";
import {
  Combobox,
  ComboboxEmpty,
  ComboboxItem,
  ComboboxList,
  ComboboxPopup,
  ComboboxSearchInput,
  ComboboxTrigger,
} from "../ui/combobox";
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

export class VoiceWorkstreamCorrectionSession {
  readonly actions: RegistryCorrectionActions;
  private readonly listeners = new Set<() => void>();
  private readonly acknowledged = new Map<string, ThreadRegistryAssociation>();
  private snapshot = {
    busy: false,
    uncertain: false,
    error: null as string | null,
    records: [] as readonly ThreadRegistryAssociation[],
  };

  constructor(transport: RegistryCorrectionTransport) {
    this.actions = new RegistryCorrectionActions(transport);
  }
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };
  getSnapshot = () => this.snapshot;
  publish(record?: ThreadRegistryAssociation) {
    if (record) this.acknowledged.set(`${record.subject}|${record.workstream_ref}`, record);
    this.snapshot = {
      busy: this.actions.busy,
      uncertain: this.actions.uncertain,
      error: this.actions.error,
      records: [...this.acknowledged.values()],
    };
    for (const listener of this.listeners) listener();
  }
  reconcile() {
    this.actions.reconciled();
    this.acknowledged.clear();
    this.publish();
  }
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
  session,
  onRefresh,
  unavailable,
}: {
  entry: VoiceReviewRecentEntry;
  thread: Thread | null;
  registry: ThreadRegistryComposedSnapshot | null;
  workstreams: ThreadRegistryWorkstreams | null;
  transport: RegistryCorrectionTransport;
  session?: VoiceWorkstreamCorrectionSession | undefined;
  onRefresh: () => Promise<void>;
  unavailable: boolean;
}) {
  const [selected, setSelected] = useState<string | null>(null);
  const [localSession] = useState(() => new VoiceWorkstreamCorrectionSession(transport));
  const active = session ?? localSession;
  const actionState = useSyncExternalStore(
    active.subscribe,
    active.getSnapshot,
    active.getSnapshot,
  );
  const [refreshError, setRefreshError] = useState(false);
  const subject = entry.command_id ? `prompt:${entry.command_id}` : null;
  const associations = new Map<string, ThreadRegistryAssociation>();
  for (const record of entry.associations ?? [])
    associations.set(`${record.subject}|${record.workstream_ref}`, record);
  const relevantSubjects = new Set([
    subject,
    ...[...associations.values()].map((record) => record.subject),
  ]);
  for (const record of actionState.records) {
    if (!relevantSubjects.has(record.subject)) continue;
    const key = `${record.subject}|${record.workstream_ref}`;
    if ((associations.get(key)?.revision ?? 0) < record.revision) associations.set(key, record);
  }
  const refs = new Set(entry.workstream_refs.filter((ref) => ref.startsWith("inferred:")));
  for (const record of associations.values()) {
    if (record.state === "active") refs.add(record.workstream_ref);
    else refs.delete(record.workstream_ref);
  }
  const disabled =
    unavailable || actionState.busy || actionState.uncertain || !registry || !workstreams;
  const readOnlyReason = actionState.uncertain
    ? "Check current workstreams before another change."
    : disabled
      ? actionState.busy
        ? "A Workstream change is in progress."
        : "Workstream reads or corrections are currently unavailable."
      : !subject || entry.associations === undefined
        ? "Prompt corrections require an observed command and association revisions."
        : null;
  const correct = async (
    ref: string,
    state: "active" | "suppressed",
    record?: ThreadRegistryAssociation,
  ) => {
    if (readOnlyReason || active.actions.busy || active.actions.uncertain) return;
    const target = record?.subject ?? subject;
    if (!target || !ref.startsWith("inferred:") || entry.associations === undefined) return;
    const pending = active.actions.correct({
      schema: "voice.association-mutation/v1",
      subject: target,
      workstream_ref: ref,
      state,
      expected_revision: record?.revision ?? 0,
      request_id: randomUUID(),
      command_id: entry.command_id,
    });
    active.publish();
    const receipt = await pending;
    if (receipt && "subject" in receipt.record) {
      active.publish(receipt.record);
      setSelected(null);
    } else {
      active.publish();
    }
  };
  const refresh = async () => {
    if (active.actions.busy) return;
    try {
      await onRefresh();
      active.reconcile();
      setRefreshError(false);
    } catch {
      setRefreshError(true);
    }
  };
  const summary = textField(thread?.summary ?? null, "text");
  const options = new Map<string, string>();
  for (const label of workstreams?.workstreams ?? []) {
    if (label.state === "active") options.set(label.label_id, label.name);
  }
  const available = [...options.keys()].filter((ref) => !refs.has(ref));
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
          const name = options.get(ref);
          const unavailable =
            readOnlyReason ?? (record ? null : "Association revision unavailable");
          return (
            <span key={ref} className="flex min-w-0 flex-wrap items-center gap-1">
              <Badge
                variant="outline"
                size="sm"
                className="h-auto max-w-full whitespace-normal wrap-anywhere"
              >
                <bdi className={name ? undefined : "font-mono"}>{name ?? ref}</bdi>
                <span className="text-muted-foreground">suggested</span>
              </Badge>
              <Tooltip>
                <TooltipTrigger
                  render={<Button type="button" size="icon-xs" variant="ghost" />}
                  aria-label={`Remove ${name ?? ref} workstream`}
                  aria-disabled={unavailable !== null}
                  onClick={() => {
                    if (!unavailable) void correct(ref, "suppressed", record);
                  }}
                >
                  <XIcon aria-hidden className="size-3" />
                </TooltipTrigger>
                <TooltipPopup>{unavailable ?? "Remove workstream association"}</TooltipPopup>
              </Tooltip>
            </span>
          );
        })}
      </div>
      <div className="flex min-w-0 flex-wrap items-center gap-2">
        <Combobox
          items={available}
          value={selected}
          onValueChange={setSelected}
          itemToStringLabel={(ref) => options.get(ref) ?? ref}
          disabled={readOnlyReason !== null}
        >
          <ComboboxTrigger
            render={<Button type="button" variant="outline" size="compact" />}
            aria-label={`Choose a workstream for ${entry.draft.id}`}
            className="max-w-full"
          >
            <span className="truncate">
              {selected ? (options.get(selected) ?? selected) : "Choose a workstream…"}
            </span>
            <ChevronsUpDownIcon aria-hidden />
          </ComboboxTrigger>
          <ComboboxPopup>
            <ComboboxSearchInput
              aria-label="Search existing workstreams"
              placeholder="Search workstreams"
            />
            <ComboboxEmpty>No available workstreams.</ComboboxEmpty>
            <ComboboxList>
              {(ref: string) => (
                <ComboboxItem key={ref} value={ref}>
                  {options.get(ref)}
                </ComboboxItem>
              )}
            </ComboboxList>
          </ComboboxPopup>
        </Combobox>
        <Button
          variant="outline"
          size="compact"
          disabled={readOnlyReason !== null || !selected || !available.includes(selected)}
          onClick={() => {
            if (!selected || !available.includes(selected)) return;
            void correct(
              selected,
              "active",
              [...associations.values()].find(
                (item) => item.subject === subject && item.workstream_ref === selected,
              ),
            );
          }}
        >
          Add
        </Button>
      </div>
      <p className="text-muted-foreground">Workstream corrections never resend prompts.</p>
      {readOnlyReason ? <p className="text-muted-foreground">{readOnlyReason}</p> : null}
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
