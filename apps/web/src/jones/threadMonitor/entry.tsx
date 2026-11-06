/* eslint-disable shadcn/no-unknown-classes -- This isolated native peer uses the dedicated monitor.css stylesheet instead of Tailwind utilities. */
import { flushSync } from "react-dom";
import { createRoot } from "react-dom/client";
import { MonitorUI } from "./MonitorUI";
import { MonitorPeerStore } from "./peerStore";
import { applyMonitorTheme } from "./theme";
import "./monitor.css";
const bridge = window.threadMonitor;
const root = createRoot(document.getElementById("root")!);
if (!bridge)
  root.render(
    <p className="monitor-unavailable">The native thread monitor bridge is unavailable.</p>,
  );
else {
  const store = new MonitorPeerStore((intent) => bridge.dispatch(intent));
  const stop = bridge.onFrame((frame) => {
    if (frame.type === "theme" && frame.publisherGeneration === store.generation)
      applyMonitorTheme(frame.theme);
    else if (frame.type === "invalidateEnvironment") {
      flushSync(() => store.receive(frame));
      if (frame.publisherGeneration === store.generation)
        void bridge.dispatch({ ...frame, type: "environmentInvalidated" }).catch(() => undefined);
    } else store.receive(frame);
  });
  root.render(<MonitorUI store={store} />);
  void bridge.ready().catch(() => {
    store.error =
      "Jones Code is reconnecting. Actions are unavailable until its publisher is ready.";
    store.changed();
  });
  window.addEventListener(
    "pagehide",
    () => {
      stop();
      root.unmount();
    },
    { once: true },
  );
}
