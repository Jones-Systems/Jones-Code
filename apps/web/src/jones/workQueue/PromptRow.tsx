import type { EnvironmentId } from "@t3tools/contracts";
import { ArrowUpRightIcon, LinkIcon } from "lucide-react";
import { Button } from "../../components/ui/button";
import { Badge } from "../../components/ui/badge";
import { useClientSettings } from "../../hooks/useSettings";
import { cn } from "../../lib/utils";
import { requestDisplayText } from "../remoteAdmission/presentation";
import { formatRequestTime } from "../remoteAdmission/requestTime";
import { PromptWorkstreamBadge } from "./PromptWorkstreamDetails";
import type { PromptWorkstreamLine } from "./promptWorkstreams";
import type { PromptTone } from "./promptPresentation";
import { PromptThreadStatus, PromptThreadTitle, usePromptThreadShell } from "./PromptThreadStatus";

export const toneVariant = {
  neutral: "outline",
  info: "info",
  warning: "warning",
  success: "success",
} as const satisfies Record<PromptTone, "outline" | "info" | "warning" | "success">;

/** Formats sample times with the person's timestamp setting, in the Requests convention. */
export function usePromptTime() {
  const timestampFormat = useClientSettings((settings) => settings.timestampFormat);
  return (ms: number) =>
    Number.isFinite(ms)
      ? formatRequestTime(new Date(ms).toISOString(), timestampFormat)
      : "Not available";
}

/** Thread route for a destination, when the thread is known in this environment. */
export function threadHref(environmentId: EnvironmentId, threadId: string) {
  return `/${encodeURIComponent(environmentId)}/${encodeURIComponent(threadId)}`;
}

/** Externally supplied single-line text, stripped of control characters. */
export function displayText(value: string | null | undefined, fallback: string) {
  const text = value ? requestDisplayText(value) : "";
  return text || fallback;
}

export interface PromptRowView {
  readonly key: string;
  readonly environmentId: EnvironmentId;
  /** Thread whose live title and status may replace the fallback title. */
  readonly threadId: string | null;
  readonly fallbackTitle: string;
  readonly destinationCount: number;
  readonly status: { readonly label: string; readonly tone: PromptTone };
  readonly preview: string;
  readonly previewMuted: boolean;
  readonly source: string;
  readonly workstreams: readonly PromptWorkstreamLine[];
  readonly promptHref?: string | undefined;
  readonly time: number | null;
  /** This session has unsaved text, a running action or an uncertain outcome here. */
  readonly dirty: boolean;
  /** The row left this stage while the list was held still. */
  readonly gone: boolean;
}

export function PromptRow({
  row,
  selected,
  onSelect,
  onFilterWorkstream,
}: {
  readonly row: PromptRowView;
  readonly selected: boolean;
  readonly onSelect: () => void;
  readonly onFilterWorkstream?: ((line: PromptWorkstreamLine) => void) | undefined;
}) {
  const formatTime = usePromptTime();
  const shell = usePromptThreadShell(row.environmentId, row.threadId);
  const title =
    row.destinationCount > 1 ? `${row.destinationCount} destinations` : row.fallbackTitle;
  return (
    <div
      className={cn(
        "grid min-w-0 grid-cols-[minmax(0,1fr)_auto] items-start gap-1 rounded-md border p-2.5",
        selected ? "border-ring bg-accent" : "bg-background",
        row.gone && "opacity-60",
      )}
    >
      <button
        type="button"
        className="min-w-0 flex-1 space-y-1 rounded-sm text-left focus-visible:outline-2 focus-visible:outline-ring"
        aria-current={selected ? "true" : undefined}
        aria-disabled={row.gone ? "true" : undefined}
        onClick={() => {
          if (!row.gone) onSelect();
        }}
      >
        <span className="flex min-w-0 flex-wrap items-start gap-x-2 gap-y-1">
          <span className="min-w-0 grow basis-40 text-sm font-medium wrap-anywhere">
            {row.dirty ? (
              <span
                aria-label="Unsaved or unconfirmed work"
                role="img"
                className="me-1.5 inline-block size-1.5 rounded-full bg-warning align-middle"
              />
            ) : null}
            <bdi>
              {row.destinationCount > 1 ? (
                title
              ) : (
                <PromptThreadTitle
                  environmentId={row.environmentId}
                  threadId={row.threadId}
                  fallback={title}
                />
              )}
            </bdi>
          </span>
          <Badge
            size="sm"
            variant={toneVariant[row.status.tone]}
            className="h-auto min-h-5 max-w-full text-start wrap-anywhere whitespace-normal sm:h-auto sm:min-h-4"
          >
            {row.gone ? "Moved · refresh for current state" : row.status.label}
          </Badge>
        </span>
        <span
          className={cn(
            "line-clamp-2 block text-xs wrap-anywhere",
            row.previewMuted ? "text-muted-foreground italic" : "text-foreground",
          )}
        >
          {row.preview}
        </span>
        <span className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-0.5 text-xs text-muted-foreground">
          <span>{row.source}</span>
          {row.time !== null ? (
            <time dateTime={new Date(row.time).toISOString()}>{formatTime(row.time)}</time>
          ) : null}
        </span>
      </button>
      <span className="flex shrink-0 flex-col items-end gap-1">
        <PromptThreadStatus environmentId={row.environmentId} threadId={row.threadId} />
        {!row.gone && shell && row.threadId ? (
          <a
            href={threadHref(row.environmentId, row.threadId)}
            aria-label={`Open thread for ${row.fallbackTitle}`}
            className="rounded-sm p-1 text-muted-foreground hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring"
          >
            <ArrowUpRightIcon aria-hidden className="size-3.5" />
          </a>
        ) : null}
        {!row.gone && row.promptHref ? (
          <a
            href={row.promptHref}
            aria-label={`Open link to prompt for ${row.fallbackTitle}`}
            className="rounded-sm p-1 text-muted-foreground hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring"
          >
            <LinkIcon aria-hidden className="size-3.5" />
          </a>
        ) : null}
      </span>
      {row.workstreams.length > 0 ? (
        <div className="col-span-full flex min-w-0 flex-wrap items-center gap-1">
          {row.workstreams.map((line) =>
            onFilterWorkstream && !row.gone ? (
              <Button
                key={line.key}
                size="micro"
                variant="ghost"
                className="h-auto max-w-full"
                aria-label={`Filter prompts by ${line.name ?? line.id}`}
                onClick={() => onFilterWorkstream(line)}
              >
                <PromptWorkstreamBadge line={line} />
              </Button>
            ) : (
              <PromptWorkstreamBadge key={line.key} line={line} />
            ),
          )}
        </div>
      ) : null}
    </div>
  );
}
