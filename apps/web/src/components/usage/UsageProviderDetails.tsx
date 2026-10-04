import { formatCount, formatTokens, formatUsd } from "@t3tools/shared/usageFormat";
import type { ModelTotals, ProviderTotals } from "@t3tools/shared/usageMerge";
import { XIcon } from "lucide-react";

import { Button } from "../ui/button";
import { ProviderInstanceIcon } from "../chat/ProviderInstanceIcon";
import { PROVIDER_PRESENTATION } from "./usageProviders";

export const USAGE_PROVIDER_DETAILS_ID = "usage-provider-details";

export function UsageTokenDetails({
  detail,
}: {
  readonly detail: Pick<
    ModelTotals,
    | "totals"
    | "totalTokens"
    | "costUsd"
    | "records"
    | "providerReportedRecords"
    | "modelPricedRecords"
    | "unpricedRecords"
  >;
}) {
  const costUnknown = detail.records > 0 && detail.unpricedRecords >= detail.records;
  return (
    <div className="flex flex-col gap-4">
      <dl className="grid grid-cols-2 gap-x-6 gap-y-4 py-1 md:grid-cols-4">
        <DetailMetric label="Processed tokens" value={formatTokens(detail.totalTokens)} />
        <DetailMetric
          label="Uncached input"
          value={formatTokens(detail.totals.uncachedInputTokens)}
        />
        <DetailMetric label="Cached input" value={formatTokens(detail.totals.cachedInputTokens)} />
        <DetailMetric
          label="Cache creation"
          value={formatTokens(detail.totals.cacheCreationTokens)}
        />
        <DetailMetric label="Output" value={formatTokens(detail.totals.outputTokens)} />
        <DetailMetric
          label="Reasoning · included in output"
          value={formatTokens(detail.totals.reasoningTokens)}
        />
        <DetailMetric label="Recorded responses" value={formatCount(detail.records)} />
        <DetailMetric
          label="API estimate"
          value={costUnknown ? "Unpriced" : formatUsd(detail.costUsd)}
        />
      </dl>
      <div className="flex flex-col gap-2">
        <h3 className="text-xs font-medium text-muted-foreground">
          Pricing coverage · recorded responses
        </h3>
        <dl className="grid grid-cols-2 gap-x-6 gap-y-3 md:grid-cols-3">
          <DetailMetric
            label="Provider reported"
            value={formatCount(detail.providerReportedRecords)}
          />
          <DetailMetric label="Model priced" value={formatCount(detail.modelPricedRecords)} />
          <DetailMetric label="Unpriced" value={formatCount(detail.unpricedRecords)} />
        </dl>
      </div>
      <p className="text-xs text-muted-foreground">
        Recorded counters may omit token breakdowns. Zero does not establish complete coverage. API
        estimates exclude unpriced responses and do not represent subscription billing.
      </p>
    </div>
  );
}

export function UsageProviderDetails({
  provider,
  onClose,
}: {
  readonly provider: ProviderTotals;
  readonly onClose: () => void;
}) {
  const presentation = PROVIDER_PRESENTATION[provider.provider];
  return (
    <section
      id={USAGE_PROVIDER_DETAILS_ID}
      aria-labelledby={`${USAGE_PROVIDER_DETAILS_ID}-title`}
      className="flex flex-col gap-4 rounded-lg border border-border/60 p-4"
    >
      <div className="flex items-center justify-between gap-3">
        <h2
          id={`${USAGE_PROVIDER_DETAILS_ID}-title`}
          className="flex items-center gap-2 text-sm font-medium text-foreground"
        >
          <ProviderInstanceIcon
            driverKind={presentation.driverKind}
            displayName={presentation.label}
            iconClassName="size-4 shrink-0"
          />
          {presentation.label} details
        </h2>
        <Button
          variant="ghost"
          size="icon-sm"
          aria-label={`Close ${presentation.label} details`}
          onClick={onClose}
        >
          <XIcon aria-hidden />
        </Button>
      </div>
      <UsageTokenDetails detail={provider} />
    </section>
  );
}

function DetailMetric({ label, value }: { readonly label: string; readonly value: string }) {
  return (
    <div className="flex min-w-0 flex-col gap-0.5">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="text-base font-medium text-foreground tabular-nums">{value}</dd>
    </div>
  );
}
