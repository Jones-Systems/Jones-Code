import type { ThreadEndpointRef } from "@t3tools/contracts/jones/linkedThreads";
import { CopyIcon, LinkIcon, MoreHorizontalIcon, UnplugIcon } from "lucide-react";
import { useState } from "react";
import { ThreadDetailsControl } from "../../components/chat/ThreadDetailsControl";
import { Menu, MenuItem, MenuPopup, MenuTrigger } from "../../components/ui/menu";
import { CopyHandoffDialog } from "./CopyHandoffDialog";
import { LinkThreadDialog } from "./LinkThreadDialog";
import type { useLinkedThreads } from "./useLinkedThreads";

export function LinkedThreadsActions(props: {
  readonly subject: ThreadEndpointRef;
  readonly view: ReturnType<typeof useLinkedThreads>;
  readonly title: string;
  readonly environmentLabel: string;
  readonly canDetach: boolean;
  readonly busy: boolean;
  readonly onDetach: () => void;
}) {
  const [dialog, setDialog] = useState<"link" | "handoff" | null>(null);
  if (!props.canDetach && !props.view.canLink && !props.view.canCopy) return null;
  return (
    <>
      <Menu>
        <MenuTrigger
          render={
            <ThreadDetailsControl
              size="icon-xs"
              variant="ghost"
              part="icon"
              aria-label="More thread actions"
              disabled={props.busy}
            />
          }
        >
          <MoreHorizontalIcon className="size-3.5" />
        </MenuTrigger>
        <MenuPopup align="end" className="min-w-60 max-w-(--available-width)">
          {props.view.canLink ? (
            <MenuItem onClick={() => setDialog("link")}>
              <LinkIcon className="size-3.5" />
              Link thread…
            </MenuItem>
          ) : null}
          {props.view.canCopy ? (
            <MenuItem onClick={() => setDialog("handoff")}>
              <CopyIcon className="size-3.5" />
              Copy handoff text…
            </MenuItem>
          ) : null}
          {props.canDetach ? (
            <MenuItem onClick={props.onDetach}>
              <UnplugIcon className="size-3.5" />
              Disconnect agent session
            </MenuItem>
          ) : null}
        </MenuPopup>
      </Menu>
      {dialog === "link" ? (
        <LinkThreadDialog
          subject={props.subject}
          view={props.view}
          onClose={() => setDialog(null)}
        />
      ) : null}
      {dialog === "handoff" ? (
        <CopyHandoffDialog
          subject={props.subject}
          title={props.title}
          environmentLabel={props.environmentLabel}
          onClose={() => setDialog(null)}
        />
      ) : null}
    </>
  );
}
