import type { ThreadMonitorNativeState } from "@t3tools/contracts/jones/desktopThreadMonitor";
let nativeState: ThreadMonitorNativeState | null = null;
const listeners = new Set<() => void>();
export const readMonitorNativeState = () => nativeState;
export const subscribeMonitorNativeState = (listener: () => void) => {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
};
export function setMonitorNativeState(state: ThreadMonitorNativeState | null) {
  nativeState = state;
  for (const listener of listeners) listener();
}
export function monitorMainActivity() {
  return nativeState?.supported
    ? { visible: nativeState.mainVisible, focused: nativeState.mainFocused }
    : null;
}
