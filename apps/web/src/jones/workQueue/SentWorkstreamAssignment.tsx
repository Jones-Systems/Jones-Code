import { personalThreadKey } from "@t3tools/client-runtime/jones/workstreams/personal";
import type { PersonalWorkstreamsState } from "@t3tools/client-runtime/jones/workstreams/personal";
import { ChevronDownIcon } from "lucide-react";
import { useRef, useState } from "react";
import { Button } from "../../components/ui/button";
import { Menu, MenuItem, MenuPopup, MenuTrigger } from "../../components/ui/menu";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { toastManager } from "../../components/ui/toast";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import {
  personalWorkstreamsController,
  readLivePersonalAssignments,
} from "../../state/workstreamsPersonal";
import { showPersonalAssignment } from "../workstreams/personalNotifications";
import type { PromptDestination, PromptItem } from "./promptItems";
import { displayText } from "./PromptRow";
import {
  sentAssignmentOptions,
  sentThreadWorkstream,
  type SentReceiptSource,
  type SentThreadWorkstream,
} from "./sentWorkstreams";

type UnassignedThread = Extract<SentThreadWorkstream, { kind: "unassigned" }>;
const controller = personalWorkstreamsController;

function startAssignment(
  destination: PromptDestination,
  source: SentReceiptSource,
  { thread, revision }: UnassignedThread,
  id: string,
) {
  const state = appAtomRegistry.get(controller.state);
  const live = readLivePersonalAssignments(thread.environmentId);
  const current = sentThreadWorkstream(destination, source, state, live, Date.now());
  const key = personalThreadKey(thread);
  const option = sentAssignmentOptions(state, thread.environmentId).find((item) => item.id === id);
  if (
    current.kind !== "unassigned" ||
    current.revision !== revision ||
    (state.assignmentsByThreadKey.get(key)?.revision ?? 0) !== revision ||
    state.pendingThreadKeys.has(key) ||
    !controller.canAssign(thread) ||
    !option
  )
    return null;

  // The controller captures the revision and session synchronously. Keep these checks in
  // the same turn so a pending local move cannot become this action's predecessor.
  return { result: controller.assign(thread, id), option, key };
}

export function SentAssignmentDetails({
  item,
  memberships,
  source,
  state,
  filteredToUnassigned,
}: {
  readonly item: PromptItem;
  readonly memberships: readonly SentThreadWorkstream[];
  readonly source: SentReceiptSource | null;
  readonly state: PersonalWorkstreamsState;
  readonly filteredToUnassigned: boolean;
}) {
  const options = source ? sentAssignmentOptions(state, source.environmentId) : [];
  return (
    <>
      {filteredToUnassigned && memberships.every((membership) => membership.kind === "assigned") ? (
        <p role="status" className="text-muted-foreground">
          This prompt is assigned and has left Unassigned.
        </p>
      ) : null}
      {memberships.map((membership, index) => {
        if (membership.kind === "assigned") return null;
        const destination = item.destinations[index]!;
        return (
          <div key={destination.key} className="flex flex-wrap items-center gap-2">
            {item.destinations.length > 1 ? <span>Destination {index + 1}</span> : null}
            {membership.kind === "unassigned" && source ? (
              <>
                <span className="text-muted-foreground">Unassigned</span>
                <SentWorkstreamAssignment
                  assignment={membership}
                  destination={destination}
                  source={source}
                  options={options}
                  label={
                    item.destinations.length > 1
                      ? `Assign Workstream for destination ${index + 1}`
                      : "Assign Workstream"
                  }
                  pending={state.pendingThreadKeys.has(personalThreadKey(membership.thread))}
                />
              </>
            ) : (
              <span className="text-muted-foreground">
                Current Workstream assignment unavailable.
              </span>
            )}
          </div>
        );
      })}
    </>
  );
}

function SentWorkstreamAssignment({
  assignment,
  destination,
  source,
  options,
  label,
  pending,
}: {
  readonly assignment: UnassignedThread;
  readonly destination: PromptDestination;
  readonly source: SentReceiptSource;
  readonly options: ReturnType<typeof sentAssignmentOptions>;
  readonly label: string;
  readonly pending: boolean;
}) {
  const inFlight = useRef(false);
  const [busy, setBusy] = useState(false);
  const choose = async (id: string) => {
    if (inFlight.current) return;
    inFlight.current = true;
    setBusy(true);
    try {
      const action = startAssignment(destination, source, assignment, id);
      if (!action) {
        toastManager.add({
          type: "error",
          title: "Assignment changed. Check the current Workstream.",
        });
        return;
      }
      const result = await action.result;
      const next = appAtomRegistry.get(controller.state);
      if (
        result.ok &&
        readLivePersonalAssignments(assignment.thread.environmentId) !== null &&
        !next.pendingThreadKeys.has(action.key) &&
        next.assignmentsByThreadKey.get(action.key)?.workstreamId === id
      ) {
        showPersonalAssignment(displayText(action.option.name, "Workstream"), false);
      } else {
        toastManager.add({
          type: "error",
          title:
            !result.ok && result.failure.reason === "changed_elsewhere"
              ? "The thread’s Workstream changed elsewhere."
              : "Couldn’t confirm the assignment. Check the destination thread.",
        });
      }
    } catch {
      toastManager.add({
        type: "error",
        title: "Couldn’t confirm the assignment. Check the destination thread.",
      });
    } finally {
      inFlight.current = false;
      setBusy(false);
    }
  };

  const disabledReason =
    busy || pending
      ? "Waiting for the current Workstream change."
      : !controller.canAssign(assignment.thread)
        ? "This connection has read-only Workstream access."
        : options.length === 0
          ? "No assignable Workstreams on this environment."
          : null;
  return disabledReason ? (
    <Tooltip>
      <TooltipTrigger
        render={<Button variant="outline" size="compact" aria-disabled="true" aria-label={label} />}
      >
        Assign Workstream
      </TooltipTrigger>
      <TooltipPopup>{disabledReason}</TooltipPopup>
    </Tooltip>
  ) : (
    <Menu>
      <MenuTrigger render={<Button variant="outline" size="compact" />} aria-label={label}>
        Assign Workstream
        <ChevronDownIcon aria-hidden />
      </MenuTrigger>
      <MenuPopup align="start">
        {options.map((option) => (
          <MenuItem key={option.id} onClick={() => void choose(option.id)}>
            <bdi className="min-w-0 wrap-anywhere">{displayText(option.name, "Workstream")}</bdi>
          </MenuItem>
        ))}
      </MenuPopup>
    </Menu>
  );
}
