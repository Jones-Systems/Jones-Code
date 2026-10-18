import { isElectron } from "./env";
import { isHostedStaticApp } from "./hostedPairing";
import { showBootError } from "./lib/bootError";

// Bundled dev can move UI code into shared chunks. Load it only after this
// entry runs the React refresh preamble, and catch failures before React mounts.
const standaloneQueue =
  !isElectron &&
  typeof window !== "undefined" &&
  window.location?.pathname === "/work-queue" &&
  isHostedStaticApp();

void (
  standaloneQueue ? import("./workQueuePreview") : import("./main").then(({ startup }) => startup)
).catch(showBootError);
