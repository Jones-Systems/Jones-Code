import type { EnvironmentId } from "@t3tools/contracts";
import type { CSSProperties, ReactNode } from "react";
import { Badge } from "../../components/ui/badge";
import { Button } from "../../components/ui/button";
import { requestDisplayText } from "../remoteAdmission/presentation";
import { usePromptThreadShell } from "./PromptThreadStatus";
import type { PromptWorkstreamLine } from "./promptWorkstreams";

const provenanceLabels = {
  from_thread: "from thread",
  suggested: "suggested",
  requested_by_queue: "requested by queue",
  queue_id: "queue ID",
} as const;

export function PromptWorkstreamBadge({ line }: { readonly line: PromptWorkstreamLine }) {
  const name = line.name ? requestDisplayText(line.name) : null;
  const color = line.color && /^#[\da-f]{6}$/i.test(line.color) ? line.color : null;
  return (
    <Badge
      variant={name && color ? "label" : "outline"}
      style={color ? ({ "--label": color } as CSSProperties) : undefined}
      size="sm"
      className="h-auto max-w-full flex-wrap whitespace-normal wrap-anywhere"
    >
      {color ? (
        <span
          aria-hidden
          className="size-1.5 shrink-0 rounded-full"
          style={{ background: color }}
        />
      ) : null}
      <bdi className={name ? undefined : "font-mono"}>{name || line.id}</bdi>
      <span className="text-muted-foreground">
        {provenanceLabels[line.provenance]}
        {line.assignedAutomatically ? " · assigned automatically" : ""}
      </span>
    </Badge>
  );
}

function OpenWorkstreamThread({
  environmentId,
  threadId,
}: {
  readonly environmentId: EnvironmentId;
  readonly threadId: string;
}) {
  const shell = usePromptThreadShell(environmentId, threadId);
  if (!shell) return null;
  return (
    <Button
      size="xs"
      variant="outline"
      render={<a href={`/${encodeURIComponent(environmentId)}/${encodeURIComponent(threadId)}`} />}
      aria-label={`Open thread ${shell.title}`}
    >
      Open thread
    </Button>
  );
}

export function PromptWorkstreams({
  environmentId,
  lines,
  namesUnavailable,
  corrections,
}: {
  readonly environmentId: EnvironmentId;
  readonly lines: readonly PromptWorkstreamLine[];
  readonly namesUnavailable?: boolean;
  readonly corrections?: ReactNode;
}) {
  const shown = corrections ? lines.filter((line) => line.provenance !== "suggested") : lines;
  return (
    <div className="min-w-0 space-y-2 border-t pt-3 text-xs">
      <h4 className="font-medium">Workstream</h4>
      {namesUnavailable ? (
        <p className="text-muted-foreground">Workstream names unavailable.</p>
      ) : null}
      {shown.length > 0 ? (
        <ul className="space-y-1">
          {shown.map((line) => (
            <li key={line.key} className="flex min-w-0 flex-wrap items-center gap-2">
              <PromptWorkstreamBadge line={line} />
              {line.summary ? (
                <span className="text-muted-foreground wrap-anywhere">{line.summary}</span>
              ) : null}
              {line.provenance === "from_thread" && line.threadId ? (
                <OpenWorkstreamThread environmentId={environmentId} threadId={line.threadId} />
              ) : null}
            </li>
          ))}
        </ul>
      ) : null}
      {corrections}
      {lines.length === 0 && !corrections && !namesUnavailable ? (
        <p className="text-muted-foreground">No Workstream observed for this prompt.</p>
      ) : null}
    </div>
  );
}
