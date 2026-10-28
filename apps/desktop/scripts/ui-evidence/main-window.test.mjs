import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import { waitForMainPage } from "./main-window.mjs";
const page = (url, role) => ({
  isClosed: () => false,
  url: () => url,
  evaluate: async () => role,
});
NodeTest.test("monitor-first creation order cannot redirect main renderer evidence", async () => {
  const monitor = page("t3://app/thread-monitor.html", false);
  const main = page("t3://app/index.html", true);
  NodeAssert.equal(
    await waitForMainPage({
      windows: () => [monitor, main],
    }),
    main,
  );
});
NodeTest.test("waits for the actual bridge through renderer startup", async () => {
  let elapsed = 0;
  const main = page("t3://app/index.html", true);
  NodeAssert.equal(
    await waitForMainPage(
      {
        windows: () => (elapsed ? [main] : []),
      },
      {
        now: () => elapsed,
        pause: async (ms) => {
          elapsed += ms;
        },
      },
    ),
    main,
  );
});
NodeTest.test("missing bridge fails instead of selecting an arbitrary window", async () => {
  let elapsed = 0;
  await NodeAssert.rejects(
    waitForMainPage(
      {
        windows: () => [page("t3://app/thread-monitor.html", true), page("about:blank", false)],
      },
      {
        timeoutMs: 200,
        now: () => elapsed,
        pause: async (ms) => {
          elapsed += ms;
        },
      },
    ),
    /Main desktop renderer bridge/,
  );
});

NodeTest.test("an unresponsive renderer cannot stall the startup deadline", async () => {
  const unresponsive = {
    ...page("t3://app/index.html", false),
    evaluate: () => new Promise(() => {}),
  };
  await NodeAssert.rejects(
    waitForMainPage({ windows: () => [unresponsive] }, { timeoutMs: 30, probeTimeoutMs: 5 }),
    { name: "TimeoutError", message: "Main desktop renderer bridge did not become ready" },
  );
});
