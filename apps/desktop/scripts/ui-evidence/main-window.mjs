import * as NodeTimersPromises from "node:timers/promises";

// The monitor may be the first BrowserWindow. Bind evidence to the main
// renderer's privileged bridge, never to creation order or a guessed title.
export async function waitForMainPage(
  app,
  {
    timeoutMs = 60000,
    probeTimeoutMs = 1000,
    now = Date.now,
    pause = NodeTimersPromises.setTimeout,
  } = {},
) {
  const deadline = now() + timeoutMs;
  do {
    for (const page of app.windows()) {
      if (page.isClosed() || /(?:^|\/)thread-monitor\.html(?:[?#]|$)/.test(page.url())) continue;
      let timer;
      try {
        const ready = await Promise.race([
          Promise.resolve().then(() =>
            page.evaluate(() => Boolean(window.desktopBridge) && !window.threadMonitor),
          ),
          new Promise((resolve) => {
            timer = setTimeout(
              () => resolve(false),
              Math.max(1, Math.min(probeTimeoutMs, deadline - now())),
            );
          }),
        ]);
        if (ready) return page;
      } catch {
        // A loading/replaced renderer has no established role yet.
      } finally {
        clearTimeout(timer);
      }
    }
    if (now() >= deadline) break;
    await pause(Math.min(100, deadline - now()));
  } while (now() <= deadline);
  const error = new Error("Main desktop renderer bridge did not become ready");
  error.name = "TimeoutError";
  throw error;
}
