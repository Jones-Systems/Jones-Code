import { useId, useState } from "react";
import type { RemoteAdmissionDecision, RemoteAdmissionRecord } from "@t3tools/contracts";
import { Button } from "../../components/ui/button";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { admissionPresentation, admissionStatus } from "./presentation";

export function RequestReviewRow({
  record,
  policyRevision,
  blocked,
  onDecide,
}: {
  readonly record: RemoteAdmissionRecord;
  readonly policyRevision: string;
  readonly blocked: string | null;
  readonly onDecide: (input: RemoteAdmissionDecision) => void;
}) {
  const id = useId();
  const [expanded, setExpanded] = useState(false);
  const view = admissionPresentation(record);
  const stalePolicy = record.envelope.receiverPolicy.revision !== policyRevision;
  const approvalBlocked =
    blocked ??
    (stalePolicy
      ? "Evaluator settings changed. Deny this request and ask the sender to submit a new request."
      : null);
  return (
    <details className="min-w-0 rounded-md border" data-request-id={record.id} open={expanded}>
      <summary
        className="cursor-pointer space-y-1 rounded-md px-3 py-2 focus-visible:outline-2 focus-visible:outline-ring"
        onClick={(event) => {
          event.preventDefault();
          setExpanded((value) => !value);
        }}
      >
        <span className="text-sm font-medium wrap-anywhere">{view.title}</span>
        <span className="ml-2 text-xs text-muted-foreground">{admissionStatus(record)}</span>
        <span className="block truncate text-xs text-muted-foreground">{view.target}</span>
        <span className="line-clamp-2 text-xs text-muted-foreground wrap-anywhere">
          {view.message.slice(0, 280)}
        </span>
      </summary>
      {expanded ? (
        <div className="space-y-3 border-t px-3 py-3 text-sm wrap-anywhere">
          <dl className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1 text-xs">
            <dt className="text-muted-foreground">From</dt>
            <dd>
              {view.source} · {record.envelope.identity.clientId}
            </dd>
            <dt className="text-muted-foreground">Operation</dt>
            <dd>{record.envelope.operation}</dd>
            <dt className="text-muted-foreground">Target</dt>
            <dd>{view.target}</dd>
            <dt className="text-muted-foreground">Modes</dt>
            <dd>
              {record.envelope.target.runtimeMode} · {record.envelope.target.interactionMode}
            </dd>
            <dt className="text-muted-foreground">Category</dt>
            <dd>{record.category ?? "Not evaluated yet"}</dd>
          </dl>
          <div className="max-h-60 overflow-y-auto overscroll-contain whitespace-pre-wrap">
            {view.message}
          </div>
          {record.reason ? <p className="text-muted-foreground">{record.reason}</p> : null}
          <details>
            <summary className="cursor-pointer text-xs underline underline-offset-2">
              Full bound request and evaluator configuration
            </summary>
            <pre className="mt-2 max-h-72 overflow-y-auto overscroll-contain whitespace-pre-wrap text-xs wrap-anywhere">
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
