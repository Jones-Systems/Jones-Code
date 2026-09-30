import { useEffect, useId, useState } from "react";

import { Button } from "../ui/button";
import {
  Dialog,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogPanel,
  DialogPopup,
  DialogTitle,
} from "../ui/dialog";
import type {
  WorkstreamAssociation,
  WorkstreamMemberPresentation,
  WorkstreamMembershipCommand,
} from "./types";

export interface WorkstreamTargetOption {
  readonly id: string;
  readonly name: string;
  readonly memberCount: number;
}

export interface WorkstreamMembershipDialogProps {
  readonly open: boolean;
  readonly action: "move" | "link" | "reattach";
  readonly sourceWorkstreamId: string;
  readonly member: WorkstreamMemberPresentation;
  readonly targets: readonly WorkstreamTargetOption[];
  readonly onOpenChange: (open: boolean) => void;
  readonly onCommand: (command: WorkstreamMembershipCommand) => void;
}

export function WorkstreamMembershipDialog(props: WorkstreamMembershipDialogProps) {
  const [targetId, setTargetId] = useState(props.targets.at(0)?.id ?? "");
  const [association, setAssociation] = useState<WorkstreamAssociation>(props.member.association);
  const associationName = useId();
  useEffect(() => {
    if (!props.open) return;
    setTargetId((current) =>
      props.targets.some((candidate) => candidate.id === current)
        ? current
        : (props.targets.at(0)?.id ?? ""),
    );
    setAssociation(props.member.association);
  }, [props.member.association, props.open, props.targets]);
  const target = props.targets.find((candidate) => candidate.id === targetId);
  const submit = () => {
    if (!target) return;
    const command: WorkstreamMembershipCommand =
      props.action === "move"
        ? {
            type: "move",
            memberRef: props.member.ref,
            fromWorkstreamId: props.sourceWorkstreamId,
            toWorkstreamId: target.id,
          }
        : props.action === "link"
          ? {
              type: "link",
              memberRef: props.member.ref,
              workstreamId: target.id,
              association: "secondary",
            }
          : {
              type: "reattach",
              memberRef: props.member.ref,
              workstreamId: target.id,
              association,
            };
    props.onCommand(command);
    props.onOpenChange(false);
  };

  return (
    <Dialog onOpenChange={props.onOpenChange} open={props.open}>
      <DialogPopup>
        <DialogHeader>
          <DialogTitle>
            {props.action.charAt(0).toUpperCase() + props.action.slice(1)} membership
          </DialogTitle>
          <DialogDescription>
            {props.action === "move"
              ? "Change the primary Workstream without changing native T3 settlement."
              : props.action === "link"
                ? "Add a secondary association without changing the primary Workstream."
                : "Restore a historical membership without changing native T3 settlement."}
          </DialogDescription>
        </DialogHeader>
        <DialogPanel>
          <div className="space-y-4">
            <label className="grid gap-1.5 text-sm">
              <span className="font-medium">Workstream</span>
              <select
                className="h-8 rounded-lg border border-input bg-background px-2"
                onChange={(event) => setTargetId(event.target.value)}
                value={targetId}
              >
                {props.targets.map((option) => (
                  <option key={option.id} value={option.id}>
                    {option.name}
                  </option>
                ))}
              </select>
            </label>
            {props.action === "reattach" && (
              <fieldset className="grid gap-1.5 text-sm">
                <legend className="font-medium">Association</legend>
                {(["primary", "secondary"] as const).map((value) => (
                  <label className="flex items-center gap-2" key={value}>
                    <input
                      checked={association === value}
                      name={associationName}
                      onChange={() => setAssociation(value)}
                      type="radio"
                    />
                    <span className="capitalize">{value}</span>
                  </label>
                ))}
              </fieldset>
            )}
            {props.targets.length === 0 && (
              <p className="text-muted-foreground text-sm">No eligible Workstreams.</p>
            )}
          </div>
        </DialogPanel>
        <DialogFooter>
          <Button onClick={() => props.onOpenChange(false)} variant="outline">
            Cancel
          </Button>
          <Button disabled={!target} onClick={submit}>
            Confirm {props.action}
          </Button>
        </DialogFooter>
      </DialogPopup>
    </Dialog>
  );
}
