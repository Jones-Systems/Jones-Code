export const MONITOR_ENTRY = "/thread-monitor.html";
export const MONITOR_CHANNELS = {
  state: "jones:thread-monitor:state",
  getState: "jones:thread-monitor:get-state",
  setEnabled: "jones:thread-monitor:set-enabled",
  publish: "jones:thread-monitor:publish",
  invalidate: "jones:thread-monitor:invalidate",
  intent: "jones:thread-monitor:intent",
  dispatch: "jones:thread-monitor:dispatch",
  frame: "jones:thread-monitor:frame",
} as const;
