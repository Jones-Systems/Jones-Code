import { MoreHorizontalIcon } from "lucide-react";

import { Button } from "../ui/button";
import { Menu, MenuItem, MenuPopup, MenuTrigger } from "../ui/menu";
import type { WorkstreamMemberPresentation, WorkstreamMembershipCommand } from "./types";
import { getWorkstreamMemberActions, type WorkstreamMemberAction } from "./workstreamPresentation";

export interface WorkstreamMemberActionsProps {
  readonly workstreamId: string;
  readonly member: WorkstreamMemberPresentation;
  readonly onCommand: (command: WorkstreamMembershipCommand) => void;
  readonly onChooseTarget: (action: Extract<WorkstreamMemberAction, "move" | "link">) => void;
}

export function WorkstreamMemberActions(props: WorkstreamMemberActionsProps) {
  const runAction = (action: WorkstreamMemberAction) => {
    if (action === "move" || action === "link") {
      props.onChooseTarget(action);
      return;
    }
    props.onCommand(
      action === "remove"
        ? {
            type: "remove",
            memberRef: props.member.ref,
            workstreamId: props.workstreamId,
          }
        : {
            type: "reattach",
            memberRef: props.member.ref,
            workstreamId: props.workstreamId,
            association: props.member.association,
          },
    );
  };
  return (
    <div className="flex shrink-0 items-center" data-workstream-member-actions>
      <Menu>
        <MenuTrigger
          aria-label={`Actions for ${props.member.title}`}
          render={<Button size="icon-micro" variant="ghost-muted" />}
        >
          <MoreHorizontalIcon />
        </MenuTrigger>
        <MenuPopup align="end">
          {getWorkstreamMemberActions({
            association: props.member.association,
            isRemoved: props.member.removedAt !== undefined,
          }).map((action) => (
            <MenuItem
              key={action.id}
              onClick={() => runAction(action.id)}
              variant={action.destructive ? "destructive" : "default"}
            >
              {action.label}
            </MenuItem>
          ))}
        </MenuPopup>
      </Menu>
    </div>
  );
}
