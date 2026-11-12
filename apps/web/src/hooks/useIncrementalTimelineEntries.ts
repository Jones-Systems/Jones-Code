import { useState } from "react";
import { shallow } from "zustand/vanilla/shallow";
import {
  deriveTimelineEntriesFromVisibleTurnItemsWithState,
  type TimelineEntriesInput,
  type TimelineEntriesProjection,
} from "../session-logic";

function sameInput(left: TimelineEntriesInput, right: TimelineEntriesInput): boolean {
  // Local feedback rows are reconstructed from immutable submissions. Their scalar fields,
  // rather than the allocation of a row or array, determine whether the projection changed.
  const sameMessages = (
    a: TimelineEntriesInput["anchoredMessages"],
    b: TimelineEntriesInput["anchoredMessages"],
  ) =>
    a === b ||
    (a !== undefined &&
      b !== undefined &&
      a.length === b.length &&
      a.every((row, index) => shallow(row, b[index])));
  return (
    shallow(left.visibleTurnItems, right.visibleTurnItems) &&
    sameMessages(left.optimisticMessages, right.optimisticMessages) &&
    sameMessages(left.anchoredMessages, right.anchoredMessages) &&
    shallow(left.attachmentUrlById, right.attachmentUrlById) &&
    shallow(left.attempts, right.attempts) &&
    shallow(left.nodes, right.nodes) &&
    shallow(left.plans, right.plans)
  );
}

export function useIncrementalTimelineEntries(
  threadKey: string | null,
  input: TimelineEntriesInput,
): TimelineEntriesProjection["entries"] {
  const [state, setState] = useState(() => ({
    threadKey,
    projection: deriveTimelineEntriesFromVisibleTurnItemsWithState(input),
  }));
  if (state.threadKey === threadKey && sameInput(state.projection.input, input)) {
    return state.projection.entries;
  }
  const projection = deriveTimelineEntriesFromVisibleTurnItemsWithState(
    input,
    state.threadKey === threadKey ? state.projection : null,
  );
  setState({ threadKey, projection });
  return projection.entries;
}
