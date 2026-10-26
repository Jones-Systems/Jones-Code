import { useId, useState } from "react";
import { GitBranchIcon, MessageSquareIcon } from "lucide-react";
import type { RemoteAdmissionDecision, RemoteAdmissionRecord } from "@t3tools/contracts";
import type { TimestampFormat } from "@t3tools/contracts/settings";
import { Button } from "../../components/ui/button";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { MachineIcon } from "../remoteAgentLineage/MachineIcon";
import { admissionPresentation, admissionStatus, requestDisplayText } from "./presentation";
import { formatRequestTime } from "./requestTime";
import type { RequestTarget } from "./requestTarget";
import { RequestMarkdown } from "./RequestMarkdown";
import { SenderProjectIcon } from "./SenderProjectIcon";

export function RequestCard({
  record,
  view,
  target,
  timestampFormat,
  policyRevision,
  blocked,
  onDecide,
  nowMs,
}: {
  readonly record: RemoteAdmissionRecord;
  readonly view: ReturnType<typeof admissionPresentation>;
  readonly target: RequestTarget;
  readonly timestampFormat: TimestampFormat;
  readonly policyRevision: string;
  readonly blocked: string | null;
  readonly onDecide: (input: RemoteAdmissionDecision) => void;
  readonly nowMs?: number;
}) {
  const id = useId();
  const [expanded, setExpanded] = useState(false);
  const stalePolicy = record.envelope.receiverPolicy.revision !== policyRevision;
  const approvalBlocked =
    blocked ??
    (stalePolicy
      ? "Evaluator settings changed. Deny this request and ask the sender to submit a new request."
      : null);
  const iso = view.sentAt ?? record.createdAt;
  const time = (value: string, full = false) =>
    formatRequestTime(value, timestampFormat, { ...(nowMs === undefined ? {} : { nowMs }), full });
  const hasContext = Boolean(view.project || view.parent || view.thread);
  const segments = [
    view.project
      ? {
          key: "project",
          label: view.project.label,
          glyph: <SenderProjectIcon icon={view.project.icon} title={view.project.title} />,
        }
      : null,
    view.parent
      ? {
          key: "parent",
          label: view.parent.label,
          glyph: <GitBranchIcon aria-hidden="true" className="size-3.5 shrink-0" />,
        }
      : null,
    view.thread
      ? {
          key: "thread",
          label: view.thread.label,
          glyph: <MessageSquareIcon aria-hidden="true" className="size-3.5 shrink-0" />,
        }
      : null,
  ].filter((segment) => segment !== null);
  return (
    <details
      className="min-w-0 max-w-full rounded-md border"
      data-request-id={record.id}
      open={expanded}
    >
      <summary
        className="block min-w-0 cursor-pointer space-y-1 rounded-md px-3 py-2 focus-visible:outline-2 focus-visible:outline-ring"
        onClick={(event) => {
          event.preventDefault();
          setExpanded((value) => !value);
        }}
      >
        <span
          data-request-slot="source-time"
          className="grid min-w-0 grid-cols-[minmax(0,1fr)_auto] items-start gap-2 text-xs text-muted-foreground [container-type:inline-size]"
        >
          <span className="flex min-w-0 items-center gap-1">
            <MachineIcon label={view.source} aria-hidden="true" className="size-3.5 shrink-0" />
            <span className="truncate">
              From <bdi>{view.source}</bdi>
            </span>
          </span>
          <span className="min-w-0 max-w-[min(10rem,45cqw)] text-right wrap-anywhere">
            {view.sentAt ? "Sent" : "Received"} <time dateTime={iso}>{time(iso)}</time>
          </span>
        </span>
        {hasContext ? (
          <span
            data-request-slot="breadcrumb"
            className="flex min-w-0 flex-wrap items-center gap-x-1 gap-y-1 text-xs text-muted-foreground"
          >
            <span className="sr-only">Reported by sender:</span>
            {segments.map((segment, index) => (
              <span key={segment.key} className="inline-flex min-w-0 max-w-full items-center gap-1">
                {index > 0 ? (
                  <span aria-hidden="true" className="text-icon-muted">
                    /
                  </span>
                ) : null}
                {segment.glyph}
                <bdi className="max-w-40 truncate">{segment.label}</bdi>
              </span>
            ))}
          </span>
        ) : null}
        <span data-request-slot="operation" className="block text-xs wrap-anywhere">
          {view.operation} ·{" "}
          <span className="text-muted-foreground">{admissionStatus(record)}</span>
        </span>
        {view.title ? (
          <span data-request-slot="title" className="block text-sm font-medium wrap-anywhere">
            <bdi>{view.title}</bdi>
          </span>
        ) : null}
        <span
          data-request-slot="target"
          className="block text-xs text-muted-foreground wrap-anywhere"
        >
          To{" "}
          {target.projectName ? (
            <>
              <bdi>{target.projectName}</bdi>
              {target.projectState ? ` (${target.projectState})` : ""}
            </>
          ) : null}
          {target.projectName && target.threadName ? " / " : null}
          {target.threadName ? (
            <>
              <bdi>{target.threadName}</bdi>
              {target.threadState ? ` (${target.threadState})` : ""}
            </>
          ) : null}
          {!target.projectName && !target.threadName ? "This environment" : null}
        </span>
        <span
          data-request-slot="preview"
          className="line-clamp-2 text-xs text-muted-foreground wrap-anywhere"
        >
          {view.message.slice(0, 280)}
        </span>
      </summary>
      {expanded ? (
        <div className="min-w-0 space-y-3 border-t px-3 py-3 text-sm wrap-anywhere">
          <dl className="grid min-w-0 grid-cols-1 gap-y-1 text-xs [&_dd]:min-w-0 [&_dd]:mb-2">
            <dt className="text-muted-foreground">Sent (sender's clock)</dt>
            <dd>
              {view.sentAt ? (
                <time dateTime={view.sentAt}>{time(view.sentAt, true)}</time>
              ) : view.invalidSentAt ? (
                `Invalid value ${view.invalidSentAt}`
              ) : (
                "Not reported"
              )}
            </dd>
            <dt className="text-muted-foreground">Received (this environment)</dt>
            <dd>
              <time dateTime={record.createdAt}>{time(record.createdAt, true)}</time>
            </dd>
            <dt className="text-muted-foreground">From</dt>
            <dd>
              <bdi>{view.source}</bdi> ·{" "}
              <bdi>{requestDisplayText(record.envelope.identity.clientId)}</bdi>
            </dd>
            <dt className="text-muted-foreground">Reported by sender</dt>
            <dd>
              {!hasContext ? "The sender did not report thread context." : null}
              {view.project ? (
                <p>
                  Project: <bdi>{view.project.label}</bdi> ·{" "}
                  <bdi>{requestDisplayText(view.project.projectId)}</bdi>
                </p>
              ) : null}
              {view.parent ? (
                <p>
                  Parent: <bdi>{view.parent.label}</bdi> ·{" "}
                  <bdi>{requestDisplayText(view.parent.threadId)}</bdi>
                  {view.parent.environmentId ? (
                    <>
                      {" "}
                      · Environment <bdi>{requestDisplayText(view.parent.environmentId)}</bdi>
                    </>
                  ) : null}
                </p>
              ) : null}
              {view.thread ? (
                <p>
                  Thread: <bdi>{view.thread.label}</bdi> ·{" "}
                  <bdi>{requestDisplayText(view.thread.threadId)}</bdi>
                </p>
              ) : null}
            </dd>
            <dt className="text-muted-foreground">Operation</dt>
            <dd>{requestDisplayText(record.envelope.operation)}</dd>
            <dt className="text-muted-foreground">Target</dt>
            <dd>
              {target.projectName ? (
                <p>
                  Project: <bdi>{target.projectName}</bdi> ·{" "}
                  <bdi>{requestDisplayText(target.projectId ?? "")}</bdi>
                  {target.projectState ? ` (${target.projectState})` : ""}
                </p>
              ) : null}
              {target.threadName ? (
                <p>
                  Thread: <bdi>{target.threadName}</bdi> ·{" "}
                  <bdi>{requestDisplayText(target.threadId ?? "")}</bdi>
                  {target.threadState ? ` (${target.threadState})` : ""}
                </p>
              ) : null}
              {!target.projectName && !target.threadName ? "This environment" : null}
            </dd>
            {target.location ? (
              <>
                <dt className="text-muted-foreground">Location</dt>
                <dd>
                  <bdi>{target.location}</bdi>
                </dd>
              </>
            ) : null}
            <dt className="text-muted-foreground">Modes</dt>
            <dd>
              {requestDisplayText(record.envelope.target.runtimeMode)} ·{" "}
              {requestDisplayText(record.envelope.target.interactionMode)}
            </dd>
            <dt className="text-muted-foreground">Category</dt>
            <dd>{record.category ?? "Not evaluated yet"}</dd>
          </dl>
          <RequestMarkdown>{view.message}</RequestMarkdown>
          {record.reason ? <p className="text-muted-foreground">{record.reason}</p> : null}
          <details>
            <summary className="cursor-pointer text-xs underline underline-offset-2">
              Full bound request and evaluator configuration
            </summary>
            <pre className="mt-2 max-h-72 min-w-0 overflow-y-auto overscroll-contain whitespace-pre-wrap text-xs wrap-anywhere">
              {JSON.stringify(
                {
                  id: record.id,
                  bindingDigest: record.bindingDigest,
                  requestDigest: record.requestDigest,
                  envelope: record.envelope,
                },
                null,
                2,
              )}
            </pre>
          </details>
          {record.status === "held" ? (
            <>
              <p className="text-xs text-muted-foreground">
                Approve authorizes dispatch on the receiving environment. It does not confirm
                execution.
              </p>
              <div className="flex flex-wrap gap-2">
                {(["approve", "deny"] as const).map((decision) => {
                  const reason = decision === "approve" ? approvalBlocked : blocked;
                  return (
                    <Tooltip key={decision}>
                      <TooltipTrigger
                        render={
                          <Button
                            size="sm"
                            variant={decision === "approve" ? "default" : "outline"}
                            aria-disabled={Boolean(reason)}
                            aria-describedby={reason ? `${id}-${decision}` : undefined}
                            onClick={() => {
                              if (!reason)
                                onDecide({
                                  id: record.id,
                                  bindingDigest: record.bindingDigest,
                                  decision,
                                });
                            }}
                          >
                            {decision === "approve" ? "Approve" : "Deny"}
                          </Button>
                        }
                      />
                      {reason ? <TooltipPopup>{reason}</TooltipPopup> : null}
                    </Tooltip>
                  );
                })}
              </div>
              {approvalBlocked ? (
                <p id={`${id}-approve`} className="text-xs text-muted-foreground">
                  {approvalBlocked}
                </p>
              ) : null}
              {blocked ? (
                <p id={`${id}-deny`} className="text-xs text-muted-foreground">
                  {blocked}
                </p>
              ) : null}
            </>
          ) : (
            <p className="text-xs text-muted-foreground">
              {admissionStatus(record)}. No resend or replay is available here.
            </p>
          )}
        </div>
      ) : null}
    </details>
  );
}
