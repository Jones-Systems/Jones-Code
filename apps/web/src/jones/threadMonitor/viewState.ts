import type {
  ThreadMonitorViewState,
  ThreadMonitorLifetime,
} from "@t3tools/contracts/jones/desktopThreadMonitor";
export const emptyMonitorViewState = (publisherGeneration: string): ThreadMonitorViewState => ({
  publisherGeneration,
  revision: 0,
  collapsed: { needsYou: false, working: false, waiting: false, done: false },
  list: { scrollTop: 0, anchors: [] },
  detail: { scrollTop: 0, anchors: [] },
  editor: null,
});
export function clearedMonitorViewState(
  state: ThreadMonitorViewState,
  retired?: ThreadMonitorLifetime,
): ThreadMonitorViewState {
  const removedEditor =
    state.editor !== null &&
    (retired === undefined ||
      (state.editor.ref.environmentId === retired.environmentId &&
        state.editor.ref.environmentEpoch === retired.environmentEpoch &&
        state.editor.ref.publisherGeneration === retired.publisherGeneration));
  return {
    ...state,
    revision: state.revision + 1,
    list: { ...state.list, anchors: [] },
    detail: { scrollTop: removedEditor ? 0 : state.detail.scrollTop, anchors: [] },
    editor: removedEditor ? null : state.editor,
  };
}
