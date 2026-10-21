import { formatPercent, formatTokens, formatUsd } from "@t3tools/shared/usageFormat";
import { isModelCostUnknown, type ModelTotals } from "@t3tools/shared/usageMerge";
import { ChevronDownIcon } from "lucide-react";

import { ProviderInstanceIcon } from "../../components/chat/ProviderInstanceIcon";
import { modelShare } from "../../components/usage/usageBreakdown";
import { PROVIDER_PRESENTATION } from "../../components/usage/usageProviders";
import { cn } from "../../lib/utils";
import { UsageTokenDetails } from "./UsageProviderDetails";

export function UsageModelRow({
  model,
  index,
  metric,
  breakdownPeak,
  loading,
  expanded,
  onSelect,
  onToggle,
}: {
  readonly model: ModelTotals;
  readonly index: number;
  readonly metric: "tokens" | "cost";
  readonly breakdownPeak: number;
  readonly loading: boolean;
  readonly expanded: boolean;
  readonly onSelect: () => void;
  readonly onToggle: () => void;
}) {
  const key = `${model.provider}:${model.model}`;
  const detailId = `usage-model-${encodeURIComponent(key)}`;
  const value = metric === "tokens" ? model.totalTokens : model.costUsd;
  const costShare = modelShare(model, "cost");
  const costPercent = costShare === null ? "—" : formatPercent(costShare);
  const tokenPercent = formatPercent(model.tokenShare);
  const rowFigures = cn("transition-opacity", loading && "opacity-40 delay-150");
  const presentation = PROVIDER_PRESENTATION[model.provider];
  return (
    <>
      <tr className="relative border-b border-border/50 text-right whitespace-nowrap text-muted-foreground tabular-nums transition-colors hover:bg-muted/50 has-focus-visible:bg-muted/50">
        <td className="py-2.5 pr-3 text-left text-xs">{index + 1}</td>
        <td className="max-w-0 py-2.5 text-left whitespace-normal">
          <div className="flex min-w-0 items-center gap-1.5">
            {/* The overlay opens the model except at the token-details toggle.
            Focus shows as the row's hover fill, not a ring. */}
            <button
              type="button"
              onClick={onSelect}
              className="flex min-w-0 items-center gap-2 text-left text-foreground outline-none after:absolute after:inset-0"
            >
              <ProviderInstanceIcon
                driverKind={presentation.driverKind}
                displayName={presentation.label}
                iconClassName="size-3.5 shrink-0"
              />
              <span className="truncate">{model.model}</span>
            </button>
            <button
              type="button"
              aria-label={`${model.model} token details`}
              aria-expanded={expanded}
              aria-controls={expanded ? detailId : undefined}
              onClick={onToggle}
              className="relative z-10 shrink-0 rounded-sm text-muted-foreground outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
            >
              <ChevronDownIcon className={cn("size-3", expanded && "rotate-180")} aria-hidden />
            </button>
          </div>
          <div aria-hidden className={cn("mt-1.5 h-0.5 max-w-48", rowFigures)}>
            <div
              className="h-full rounded-full"
              style={{
                // A short minimum keeps tiny shares a dash, not a dot.
                width:
                  value > 0 && breakdownPeak > 0
                    ? `max(0.5rem, ${(value / breakdownPeak) * 100}%)`
                    : 0,
                backgroundColor: PROVIDER_PRESENTATION[model.provider].color,
              }}
            />
          </div>
        </td>
        <td className={cn("py-2.5 pl-3 text-foreground @2xl:pl-6", rowFigures)}>
          {isModelCostUnknown(model) ? (
            <span className="text-muted-foreground">Unpriced</span>
          ) : (
            formatUsd(model.costUsd)
          )}
          <span
            aria-label="Cost percentage"
            className="block text-xs text-muted-foreground @2xl:hidden"
          >
            {costPercent}
          </span>
        </td>
        <td className={cn("hidden py-2.5 pl-6 @2xl:table-cell", rowFigures)}>{costPercent}</td>
        <td className={cn("py-2.5 pl-3 @2xl:pl-6", rowFigures)}>
          {formatTokens(model.totalTokens)}
          <span
            aria-label="Token percentage"
            className="block text-xs text-muted-foreground @2xl:hidden"
          >
            {tokenPercent}
          </span>
        </td>
        <td className={cn("hidden py-2.5 pl-6 @2xl:table-cell", rowFigures)}>{tokenPercent}</td>
      </tr>
      {expanded ? (
        <tr>
          <td colSpan={6} className="border-b border-border/50 py-4">
            <div id={detailId} role="region" aria-label={`${model.model} token details`}>
              <UsageTokenDetails detail={model} />
            </div>
          </td>
        </tr>
      ) : null}
    </>
  );
}
