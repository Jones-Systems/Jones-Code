import type { ThreadContextRecord } from "@t3tools/contracts";
import { Link } from "@tanstack/react-router";
import { MessagesSquareIcon } from "lucide-react";

import { useThreadShell } from "~/state/entities";
import { ContextChip, ContextChipLabel } from "./ContextChip";
import { Tooltip, TooltipPopup, TooltipTrigger } from "./ui/tooltip";

export function ThreadContextChip(props: {
  record: Pick<ThreadContextRecord, "environmentId" | "threadId" | "title" | "excerpt">;
  copyMarkdown?: string;
  unreadable?: boolean;
}) {
  const { environmentId, threadId } = props.record;
  const shell = useThreadShell({ environmentId, threadId });
  const title = shell?.title?.trim() || props.record.title;
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <ContextChip
            kind="thread"
            {...(props.unreadable ? { state: "invalid" as const } : {})}
            render={<Link to="/$environmentId/$threadId" params={{ environmentId, threadId }} />}
            aria-label={`Thread, ${title}`}
            data-markdown-copy={props.copyMarkdown}
            className="no-underline"
          >
            <MessagesSquareIcon />
            <ContextChipLabel>{title}</ContextChipLabel>
          </ContextChip>
        }
      />
      <TooltipPopup side="top">
        {shell ? "Open thread" : "Thread no longer available"}
        {props.record.excerpt?.status === "captured"
          ? " — bounded point-in-time context attached"
          : props.record.excerpt?.status === "unavailable"
            ? ` — context unavailable: ${props.record.excerpt.reason.replaceAll("_", " ")}; thread identity retained`
            : ""}
      </TooltipPopup>
    </Tooltip>
  );
}
