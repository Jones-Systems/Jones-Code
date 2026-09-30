import { AlertCircleIcon, CheckCircle2Icon, Clock3Icon } from "lucide-react";

import { Badge } from "../ui/badge";
import type { WorkstreamReceipts } from "./types";
import { getReceiptSummary } from "./workstreamPresentation";

const labels = {
  pending: "Pending",
  succeeded: "Succeeded",
  failed: "Failed",
} as const;

export function WorkstreamReceiptStatus({ receipts }: { readonly receipts: WorkstreamReceipts }) {
  const summary = getReceiptSummary(receipts);
  const entries = [
    { label: "Coordination disposition", receipt: summary.coordination },
    { label: "Native T3 settlement", receipt: summary.nativeSettlement },
  ] as const;

  return (
    <section aria-labelledby="workstream-receipts-title">
      <div className="flex items-center gap-2">
        <h3 className="font-medium text-sm" id="workstream-receipts-title">
          Operation receipts
        </h3>
        {summary.isPartial && <Badge variant="warning">Partial result</Badge>}
      </div>
      <dl className="mt-2 grid gap-2">
        {entries.map(({ label, receipt }) => {
          const Icon =
            receipt.state === "succeeded"
              ? CheckCircle2Icon
              : receipt.state === "failed"
                ? AlertCircleIcon
                : Clock3Icon;
          return (
            <div className="rounded-lg border border-border/70 p-2" key={label}>
              <dt className="flex items-center gap-2 text-sm">
                <Icon aria-hidden className="size-4" />
                <span>{label}</span>
                <Badge
                  className="ms-auto"
                  variant={receipt.state === "failed" ? "error" : "outline"}
                >
                  {labels[receipt.state]}
                </Badge>
              </dt>
              <dd className="mt-1 text-muted-foreground text-xs">
                {receipt.operation}
                {receipt.message ? ` · ${receipt.message}` : ""}
              </dd>
            </div>
          );
        })}
      </dl>
    </section>
  );
}
