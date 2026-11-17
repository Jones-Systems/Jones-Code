import { useId, type ReactNode } from "react";
import { Badge } from "../../components/ui/badge";
import { Button } from "../../components/ui/button";
import type { PromptDestination, PromptItem } from "./promptItems";
import {
  assignmentReasonLabel,
  assignmentStateLabel,
  destinationAssignment,
  requestKindLabel,
  turnStateLabel,
  type PromptStatus,
} from "./promptPresentation";
import { toneVariant } from "./PromptRow";

/**
 * The detail pane: heading, prompt text or editor, progress, the sentence that says what the
 * state means, actions with a reserved status line, then Routing and Evidence disclosures.
 */
export function PromptDetail({
  title,
  status,
  threadStatus,
  openThread,
  meta,
  children,
  progress,
  workstreams,
  actions,
  actionStatus,
  routing,
  evidence,
}: {
  readonly title: ReactNode;
  readonly status: PromptStatus;
  readonly threadStatus?: ReactNode;
  readonly openThread?: ReactNode;
  readonly meta: ReactNode;
  readonly children: ReactNode;
  readonly progress: ReactNode;
  readonly workstreams?: ReactNode;
  readonly actions?: ReactNode;
  readonly actionStatus?: ReactNode;
  readonly routing?: ReactNode;
  readonly evidence: ReactNode;
}) {
  const headingId = useId();
  return (
    <section
      aria-labelledby={headingId}
      className="flex min-h-72 min-w-0 flex-col gap-4 rounded-md border bg-background p-4"
    >
      <header className="min-w-0 space-y-1.5">
        <div className="flex min-w-0 flex-wrap items-start gap-2">
          <h3
            id={headingId}
            tabIndex={-1}
            data-prompt-detail-heading
            className="min-w-0 flex-1 text-sm font-medium wrap-anywhere focus:outline-none"
          >
            <bdi>{title}</bdi>
          </h3>
          {threadStatus}
          {openThread}
        </div>
        <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 text-xs text-muted-foreground">
          <Badge size="sm" variant={toneVariant[status.tone]}>
            {status.label}
          </Badge>
          {meta}
        </div>
      </header>
      <div className="min-w-0">{children}</div>
      {progress}
      {workstreams}
      <div className="mt-auto min-w-0 space-y-2 border-t pt-3">
        {status.explanation ? (
          <p className="text-xs text-muted-foreground">{status.explanation}</p>
        ) : null}
        {actions}
        <div role="status" className="min-h-8 text-xs wrap-anywhere">
          {actionStatus}
        </div>
      </div>
      {routing}
      {evidence}
    </section>
  );
}

/** Destructive action at the start, the rest at the end with the primary action last. */
export function PromptActions({
  destructive,
  children,
}: {
  readonly destructive?: ReactNode;
  readonly children?: ReactNode;
}) {
  return (
    <div className="flex min-w-0 flex-wrap items-center gap-2">
      {destructive ? <div className="me-auto flex flex-wrap gap-2">{destructive}</div> : null}
      <div className="ms-auto flex flex-wrap justify-end gap-2">{children}</div>
    </div>
  );
}

/**
 * An action that stays focusable when unavailable, with the reason announced through
 * `aria-describedby` and shown in the detail's status line.
 */
export function PromptAction({
  children,
  label,
  reason,
  reasonId,
  variant = "outline",
  onClick,
}: {
  readonly children: ReactNode;
  readonly label: string;
  readonly reason: string | null;
  readonly reasonId: string;
  readonly variant?: "default" | "outline" | "destructive-outline";
  readonly onClick: () => void;
}) {
  return (
    <Button
      size="sm"
      variant={variant}
      aria-label={label}
      aria-disabled={reason ? true : undefined}
      aria-describedby={reason ? reasonId : undefined}
      onClick={() => {
        if (!reason) onClick();
      }}
    >
      {children}
    </Button>
  );
}

export function PromptDisclosure({
  summary,
  children,
  onToggle,
}: {
  readonly summary: string;
  readonly children: ReactNode;
  readonly onToggle?: (open: boolean) => void;
}) {
  return (
    <details
      className="min-w-0 border-t pt-3 text-xs"
      onToggle={(event) => onToggle?.(event.currentTarget.open)}
    >
      <summary className="cursor-pointer font-medium text-muted-foreground">{summary}</summary>
      <div className="mt-2 min-w-0 space-y-2 wrap-anywhere text-muted-foreground">{children}</div>
    </details>
  );
}

export type PromptFact = readonly [label: string, value: ReactNode];
export function PromptFacts({ facts }: { readonly facts: readonly PromptFact[] }) {
  return (
    <dl className="grid min-w-0 grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1">
      {facts.map(([label, value]) => (
        <div key={label} className="contents">
          <dt>{label}</dt>
          <dd className="min-w-0 wrap-anywhere">{value}</dd>
        </div>
      ))}
    </dl>
  );
}

const bindingLabels: Readonly<Record<string, string>> = {
  owner_id: "Owner ID",
  server_generation: "Server generation",
  registry_version: "Registry version",
  membership_id: "Membership ID",
  native_reference_id: "Native reference ID",
  source_instance_id: "Source instance ID",
  native_thread_id: "Native thread ID",
  authority_namespace: "Authority namespace",
  store_generation: "Store generation",
  expires_at: "Binding expires at",
};

function destinationFacts(
  destination: PromptDestination,
  formatTime: (ms: number) => string,
  workstreamName: (workstreamId: string) => string,
): PromptFact[] {
  const facts: PromptFact[] = [];
  if (destination.deliveryId) {
    facts.push(["Delivery", destination.deliveryId]);
    facts.push([
      "Backend",
      `${destination.backend ?? "Not observed"} · ${destination.instance ?? "Not observed"}`,
    ]);
  }
  facts.push(["Thread", destination.threadId ?? "Not observed"]);
  if (destination.queueRequestId) facts.push(["Queue request", destination.queueRequestId]);
  const item = destination.queueItem;
  if (item) {
    facts.push(["Queue workstream", item.workstream_id]);
    if (item.target)
      facts.push(["Target", `${item.target.host_id} · ${item.target.environment_ref}`]);
    facts.push([
      "Submitted",
      item.submitted_at_ms === null ? "Not available" : formatTime(item.submitted_at_ms),
    ]);
    facts.push([
      "Lane",
      `${item.lane} · ${item.entry_kind} · ${requestKindLabel(item.request_kind)}`,
    ]);
    facts.push(["Dispatch", item.dispatch_status ?? "Not observed"]);
    facts.push(["Native command", item.native_command_status ?? "Not observed"]);
    if ("payload_sha256" in item)
      facts.push(["Payload digest", item.payload_sha256 ?? "Not reported"]);
    if (item.canonical_binding) {
      facts.push(["Identity", "Canonical binding verified at sample"]);
      for (const [key, value] of Object.entries(item.canonical_binding))
        facts.push([bindingLabels[key] ?? key, String(value)]);
    } else {
      facts.push(["Identity", "Legacy / unverified identity · no canonical binding"]);
    }
    const assignment = destinationAssignment(destination);
    if (assignment === null) facts.push(["Assignment", "Assignment information unavailable"]);
    else if (assignment) {
      facts.push(["Assignment", assignmentStateLabel(assignment.state)]);
      facts.push(["Assignment reason", assignmentReasonLabel(assignment.reason)]);
      facts.push(["Desired workstream", workstreamName(assignment.workstream_id)]);
      facts.push([
        "Observed workstream",
        assignment.observed_workstream_id === null
          ? "Not observed"
          : workstreamName(assignment.observed_workstream_id),
      ]);
      facts.push(["Assignment operation", assignment.operation_id]);
      facts.push([
        "Result updated",
        assignment.updated_at === null ? "Not available" : formatTime(assignment.updated_at),
      ]);
    }
  }
  const handoff = destination.handoff;
  if (handoff) {
    facts.push(["Receipt", handoff.confirmation.receiptId]);
    facts.push(["Receipt source", handoff.confirmation.source]);
    facts.push(["Handoff observed", formatTime(handoff.sentAt)]);
    facts.push([
      "Provider turn",
      `${turnStateLabel(handoff.providerState)} · finish line not tracked`,
    ]);
  }
  facts.push(["Completion", "Not tracked"]);
  return facts;
}

/** Every identifier the page has ever shown for an item, out of the primary reading path. */
export function PromptEvidence({
  item,
  formatTime,
  workstreamName,
}: {
  readonly item: PromptItem;
  readonly formatTime: (ms: number) => string;
  readonly workstreamName: (workstreamId: string) => string;
}) {
  const draft = item.draft;
  const entry = item.entry;
  const links = entry?.queue_delivery_links;
  return (
    <PromptDisclosure summary="Evidence">
      {draft ? (
        <PromptFacts
          facts={[
            ["Prompt", draft.id],
            ["Source", `${draft.source_id} · revision ${draft.revision}`],
            ["Transcript", draft.transcript_provider],
            [
              "Command",
              `${draft.command_id ?? entry?.command_id ?? "Not observed"} · status ${draft.command_status ?? "not observed"}`,
            ],
            ["Routing reference", draft.routing_ref ?? "Not observed"],
            ...(entry
              ? ([
                  ["Thread key", entry.thread_key ?? "Not observed"],
                  [
                    "Text source",
                    entry.text_origin === "retained_command"
                      ? "Retained command text"
                      : entry.text_origin === "draft"
                        ? "Draft text"
                        : "Not available",
                  ],
                ] as const)
              : []),
          ]}
        />
      ) : null}
      {entry && entry.original_source_text !== null && entry.original_source_text !== entry.text ? (
        <details>
          <summary className="cursor-pointer">Original transcript</summary>
          <p className="mt-1 whitespace-pre-wrap wrap-anywhere">{entry.original_source_text}</p>
        </details>
      ) : null}
      {item.origin === "voice" ? (
        <p>Release and queue intake do not prove native handoff or completed work.</p>
      ) : null}
      {entry ? (
        links?.status === "ready" ? (
          links.unlinked_delivery_count === undefined ? (
            <p>
              Additional delivery links may be unavailable; native handoff remains unconfirmed here.
            </p>
          ) : links.unlinked_delivery_count > 0 ? (
            <p>
              Deliveries without queue links: {links.unlinked_delivery_count}. Native handoff
              remains unconfirmed for these deliveries.
            </p>
          ) : null
        ) : (
          <p>Queue delivery links unavailable; native handoff remains unconfirmed here.</p>
        )
      ) : null}
      {item.destinations.map((destination, index) => (
        <div key={destination.key} className="space-y-1">
          {item.destinations.length > 1 ? (
            <p className="font-medium">
              Destination {index + 1} of {item.destinations.length}
            </p>
          ) : null}
          <PromptFacts facts={destinationFacts(destination, formatTime, workstreamName)} />
        </div>
      ))}
    </PromptDisclosure>
  );
}
