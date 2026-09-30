import { ChevronDownIcon, GripVerticalIcon, PencilIcon } from "lucide-react";
import { useEffect, useState } from "react";

import { cn } from "~/lib/utils";
import { Badge } from "../ui/badge";
import { Button } from "../ui/button";
import { Collapsible, CollapsiblePanel, CollapsibleTrigger } from "../ui/collapsible";
import { Input } from "../ui/input";
import type {
  WorkstreamMemberPresentation,
  WorkstreamMembershipCommand,
  WorkstreamPresentation,
} from "./types";
import { WorkstreamMemberActions } from "./WorkstreamMemberActions";

export interface WorkstreamSidebarGroupProps {
  readonly workstream: WorkstreamPresentation;
  readonly open: boolean;
  readonly selectedMemberRef?: string;
  readonly onOpenChange: (open: boolean) => void;
  readonly onRename: (name: string) => void;
  readonly onSelectMember: (member: WorkstreamMemberPresentation) => void;
  readonly onMemberCommand: (command: WorkstreamMembershipCommand) => void;
  readonly onChooseMemberTarget: (
    member: WorkstreamMemberPresentation,
    action: "move" | "link",
  ) => void;
  readonly onDragMemberStart: (member: WorkstreamMemberPresentation) => void;
  readonly onDropMember: (position: number) => void;
  readonly onLoadMore: () => void;
}

export function WorkstreamSidebarGroup(props: WorkstreamSidebarGroupProps) {
  const [isEditingName, setIsEditingName] = useState(false);
  const [draftName, setDraftName] = useState(props.workstream.name);
  useEffect(() => {
    if (!isEditingName) setDraftName(props.workstream.name);
  }, [isEditingName, props.workstream.name]);
  const saveName = () => {
    const name = draftName.trim();
    if (name && name !== props.workstream.name) props.onRename(name);
    if (!name) setDraftName(props.workstream.name);
    setIsEditingName(false);
  };

  return (
    <Collapsible onOpenChange={props.onOpenChange} open={props.open}>
      <div className="flex min-h-8 items-center gap-1 px-2">
        {isEditingName ? (
          <Input
            aria-label="Workstream name"
            autoFocus
            nativeInput
            onBlur={saveName}
            onChange={(event) => setDraftName(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter") saveName();
              if (event.key === "Escape") {
                setDraftName(props.workstream.name);
                setIsEditingName(false);
              }
            }}
            size="compact"
            value={draftName}
          />
        ) : (
          <CollapsibleTrigger
            aria-label={`${props.open ? "Collapse" : "Expand"} ${props.workstream.name}`}
            className="flex min-w-0 flex-1 items-center gap-1 rounded-md py-1 text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            <ChevronDownIcon
              className={cn("size-3.5 shrink-0 transition-transform", !props.open && "-rotate-90")}
            />
            <span className="truncate font-medium text-xs">{props.workstream.name}</span>
            <Badge className="ms-auto" size="sm" variant="secondary">
              {props.workstream.memberCount}
            </Badge>
          </CollapsibleTrigger>
        )}
        <Button
          aria-label={`Rename ${props.workstream.name}`}
          onClick={() => {
            setDraftName(props.workstream.name);
            setIsEditingName(true);
          }}
          size="icon-micro"
          variant="ghost-muted"
        >
          <PencilIcon />
        </Button>
      </div>
      <CollapsiblePanel>
        <ul aria-label={`${props.workstream.name} members`} className="space-y-0.5 px-2 pb-2">
          {props.workstream.members.map((member, position) => (
            <li
              className={cn(
                "group flex min-h-8 items-center rounded-md",
                member.ref === props.selectedMemberRef && "bg-accent",
                member.removedAt && "opacity-64",
              )}
              draggable={!member.removedAt && member.association === "primary"}
              key={member.ref}
              onDragOver={(event) => event.preventDefault()}
              onDragStart={() => props.onDragMemberStart(member)}
              onDrop={(event) => {
                event.preventDefault();
                props.onDropMember(position);
              }}
            >
              <GripVerticalIcon
                aria-hidden
                className="ms-1 size-3.5 shrink-0 text-muted-foreground"
              />
              <button
                className="min-w-0 flex-1 px-1 py-1 text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                onClick={() => props.onSelectMember(member)}
                type="button"
              >
                <span className="block truncate text-sm">{member.title}</span>
                <span className="block truncate text-muted-foreground text-xs">
                  {member.removedAt ? "Removed" : member.association}
                  {member.subtitle ? ` · ${member.subtitle}` : ""}
                </span>
              </button>
              <WorkstreamMemberActions
                member={member}
                onChooseTarget={(action) => props.onChooseMemberTarget(member, action)}
                onCommand={props.onMemberCommand}
                workstreamId={props.workstream.id}
              />
            </li>
          ))}
        </ul>
        <div
          aria-hidden
          className="h-1"
          onDragOver={(event) => event.preventDefault()}
          onDrop={(event) => {
            event.preventDefault();
            props.onDropMember(props.workstream.members.length);
          }}
        />
        {props.workstream.hasMoreMembers && (
          <Button className="mx-2 mb-2" onClick={props.onLoadMore} size="xs" variant="ghost">
            Load more members
          </Button>
        )}
      </CollapsiblePanel>
    </Collapsible>
  );
}
