import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
const support = new URL(
  new URL(import.meta.url).pathname.endsWith("/scenario.mjs")
    ? "./thread-self-configure-support.mjs"
    : "../thread-self-configure-support.mjs",
  import.meta.url,
);
const { installFixture, selectionA, readTrace, waitForFixture } = await import(support.href);
async function peerPage(app) {
  return waitForFixture(
    async () => {
      for (const page of app.windows()) {
        if (!page.url().includes("/thread-monitor.html")) continue;
        if (
          await page
            .evaluate(() => !!window.threadMonitor && !window.desktopBridge)
            .catch(() => false)
        )
          return page;
      }
      return null;
    },
    "dedicated monitor renderer",
    30000,
  );
}
const native = (handle) =>
  handle.evaluate((win) => ({
    visible: win.isVisible(),
    minimized: win.isMinimized(),
    focused: win.isFocused(),
    alwaysOnTop: win.isAlwaysOnTop(),
    bounds: win.getBounds(),
  }));
export default async function threadMonitor(ctx) {
  const projectId = NodeCrypto.randomUUID(),
    threadId = "native-self-config-" + NodeCrypto.randomUUID();
  const title = "Monitor synthetic reply target",
    renamed = "Monitor live minimized update";
  const evidence = {
    syntheticProvider: true,
    liveInference: "not claimed",
    projectId,
    threadId,
    captures: [],
    observations: {},
  };
  Object.assign(ctx.fixture, evidence);
  const persist = () =>
    NodeFSP.writeFile(
      ctx.artifacts + "/thread-monitor-evidence.json",
      JSON.stringify(evidence, null, 2),
      {
        mode: 0o600,
      },
    );
  let peer, mainWindow, monitorWindow;
  let motionSession = null;
  const motionFrames = [];
  let motionBytes = 0;
  const startMotion = async () => {
    motionSession = await ctx.electronApp.context().newCDPSession(peer);
    const session = motionSession;
    session.on("Page.screencastFrame", ({ data, metadata, sessionId }) => {
      const bytes = Buffer.from(data, "base64");
      if (
        motionFrames.length < 128 &&
        motionBytes + bytes.length <= 16 * 1024 * 1024 &&
        Number.isFinite(metadata.timestamp)
      ) {
        motionFrames.push({ bytes, timestampSeconds: metadata.timestamp });
        motionBytes += bytes.length;
      }
      void session.send("Page.screencastFrameAck", { sessionId }).catch(() => undefined);
    });
    await session.send("Page.startScreencast", {
      format: "jpeg",
      quality: 70,
      maxWidth: 384,
      maxHeight: 704,
      everyNthFrame: 2,
    });
  };
  const stopMotion = async () => {
    if (!motionSession) return;
    const session = motionSession;
    motionSession = null;
    try {
      await session.send("Page.stopScreencast");
    } finally {
      await session.detach();
    }
    const directory = ctx.artifacts + "/monitor-motion";
    await NodeFSP.mkdir(directory, { mode: 0o700 });
    const frames = [];
    for (const [index, frame] of motionFrames.entries()) {
      const file = String(index).padStart(4, "0") + ".jpg";
      await NodeFSP.writeFile(directory + "/" + file, frame.bytes, { flag: "wx", mode: 0o600 });
      frames.push({
        file,
        timestampSeconds: frame.timestampSeconds,
        bytes: frame.bytes.length,
        sha256: NodeCrypto.createHash("sha256").update(frame.bytes).digest("hex"),
      });
    }
    evidence.motion = {
      method:
        "Actual native peer renderer CDP screencast; original timestamp seconds retained for video encoding",
      directory: "monitor-motion",
      frames,
      totalBytes: motionBytes,
      frameCap: 128,
      byteCap: 16 * 1024 * 1024,
    };
    await NodeFSP.writeFile(directory + "/frames.json", JSON.stringify(evidence.motion, null, 2), {
      flag: "wx",
      mode: 0o600,
    });
  };
  const state = () => ctx.page.evaluate(() => window.desktopBridge.threadMonitor.getState());
  const snapshot = async () => (await ctx.readFixtureThread(threadId)).projection;
  const capture = async (name) => {
    const bytes = await peer.screenshot({
      type: "png",
      animations: "disabled",
    });
    await NodeFSP.writeFile(ctx.artifacts + "/" + name + ".png", bytes, {
      flag: "wx",
      mode: 0o600,
    });
    evidence.captures.push({
      file: name + ".png",
      sha256: NodeCrypto.createHash("sha256").update(bytes).digest("hex"),
    });
  };
  try {
    await ctx.step("Install private synthetic engine and metadata fixtures", async () => {
      evidence.git = await installFixture(ctx.workspace);
      await ctx.completeOnboarding();
      await ctx.registerSyntheticProvider(selectionA.instanceId);
      await ctx.dispatch({
        type: "project.create",
        commandId: NodeCrypto.randomUUID(),
        projectId,
        title: "Monitor native verification",
        workspaceRoot: ctx.workspace,
      });
      for (const [target, name] of [
        [threadId, title],
        ["native-self-config-" + NodeCrypto.randomUUID(), "Monitor unchanged control"],
      ]) {
        await ctx.dispatch({
          type: "thread.create",
          commandId: NodeCrypto.randomUUID(),
          createdBy: "user",
          creationSource: "web",
          projectId,
          threadId: target,
          title: name,
          modelSelection: selectionA,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: evidence.git.branch,
          worktreePath: evidence.git.worktreePath,
        });
      }
      await ctx.page
        .getByRole("button", {
          name: new RegExp("^" + title + "(?:,|$)"),
        })
        .first()
        .click();
      await ctx.page.getByTestId("composer-editor").waitFor();
      peer = await peerPage(ctx.electronApp);
      peer.setDefaultTimeout(15000);
      mainWindow = await ctx.electronApp.browserWindow(ctx.page);
      monitorWindow = await ctx.electronApp.browserWindow(peer);
    });
    await ctx.step("Default-visible independent peer and aggregate counts", async () => {
      const value = await waitForFixture(async () => {
        const s = await state();
        return s.enabled && s.visible ? s : null;
      }, "default-visible monitor");
      ctx.assert(value.supported, "Native platform supports monitor");
      const isolated = await peer.evaluate(
        () =>
          !window.desktopBridge &&
          Object.keys(window.threadMonitor).sort().join() === "dispatch,onFrame,ready",
      );
      ctx.assert(isolated, "Dedicated peer has no main renderer bridge");
      ctx.assert(
        (await native(monitorWindow)).alwaysOnTop,
        "Monitor native window is always on top",
      );
      await startMotion();
      await peer
        .getByRole("button", {
          name: /^Open thread monitor:/,
        })
        .click();
      await peer.locator(".count-strip").waitFor();
      await peer
        .locator("[data-monitor-thread-id]")
        .filter({
          hasText: title,
        })
        .waitFor();
      const counts = await peer.locator(".count-strip").innerText();
      ctx.assert(
        (await peer.locator(".count-strip > span").count()) === 4,
        "All four aggregate counts are at top",
      );
      for (const name of ["Needs You", "Working", "Waiting", "Done"]) {
        const toggle = peer.getByRole("button", {
          name: new RegExp("^" + name + "(?:\\s|$)"),
        });
        await toggle.click();
        ctx.assert((await toggle.getAttribute("aria-expanded")) === "false", name + " collapses");
        ctx.assert(
          (await peer.locator(".count-strip").innerText()) === counts,
          "Aggregate counts survive " + name + " collapse",
        );
        await toggle.click();
        ctx.assert((await toggle.getAttribute("aria-expanded")) === "true", name + " expands");
      }
      await capture("monitor-expanded-counts");
      await stopMotion();
      ctx.assert(motionFrames.length > 1, "Actual monitor motion frames were captured");
    });
    await ctx.step("Native peer inherits themes and fits 320px with enlarged text", async () => {
      const bounds = (await native(monitorWindow)).bounds;
      const originalTheme = ctx.theme;
      evidence.observations.reflow = [];
      for (const theme of ["light", "dark"]) {
        await ctx.setTheme(theme);
        await waitForFixture(
          async () =>
            peer.evaluate(
              (value) => document.documentElement.dataset.monitorTheme === value,
              theme,
            ),
          "inherited peer theme " + theme,
        );
        for (const percent of [100, 200]) {
          await monitorWindow.evaluate((win, b) => win.setBounds({ ...b, width: 320 }), bounds);
          await peer.evaluate((value) => {
            document.documentElement.style.fontSize = value + "%";
          }, percent);
          await peer.evaluate(
            () =>
              new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))),
          );
          const geometry = await peer.evaluate(() => {
            const width = document.documentElement.clientWidth;
            const counts = [...document.querySelectorAll(".count-strip > span")].map((element) => {
              const rect = element.getBoundingClientRect();
              return { left: rect.left, right: rect.right, top: rect.top, bottom: rect.bottom };
            });
            const root = document.querySelector("[data-thread-monitor]");
            return {
              width,
              scrollWidth: Math.max(
                document.documentElement.scrollWidth,
                document.body.scrollWidth,
              ),
              counts,
              fontSize: getComputedStyle(root).fontSize,
              theme: document.documentElement.dataset.monitorTheme,
            };
          });
          ctx.assert(geometry.width === 320, "Native peer is actually 320px wide");
          ctx.assert(geometry.scrollWidth <= geometry.width + 1, "Peer has no horizontal overflow");
          ctx.assert(
            geometry.counts.length === 4 &&
              geometry.counts.every((r) => r.left >= 0 && r.right <= geometry.width + 1),
            "All four top counts fit",
          );
          evidence.observations.reflow.push({
            theme,
            textPercent: percent,
            method: "root CSS text enlargement; no OS text-setting claim",
            ...geometry,
          });
          await capture("monitor-" + theme + "-320-text-" + percent);
        }
      }
      await peer.evaluate(() => {
        document.documentElement.style.removeProperty("font-size");
      });
      await monitorWindow.evaluate((win, b) => win.setBounds(b), bounds);
      await ctx.setTheme(originalTheme);
      await peer.emulateMedia({ reducedMotion: "reduce" });
      const motion = await peer.evaluate(() => ({
        requested: matchMedia("(prefers-reduced-motion: reduce)").matches,
        transition: getComputedStyle(document.querySelector("[data-thread-monitor]"))
          .transitionDuration,
      }));
      ctx.assert(
        motion.requested && motion.transition.split(",").every((value) => parseFloat(value) === 0),
        "Reduced-motion media disables panel transitions",
      );
      evidence.observations.reducedMotion = {
        ...motion,
        method: "renderer media emulation; no OS preference claim",
      };
      await peer.emulateMedia({ reducedMotion: "no-preference" });
      await peer.keyboard.press("Tab");
      ctx.assert(
        await peer.evaluate(
          () =>
            document.activeElement instanceof HTMLElement &&
            document.activeElement !== document.body,
        ),
        "Peer controls are keyboard focusable",
      );
    });
    await ctx.step("Draft survives hide/show with domain state unchanged", async () => {
      await peer
        .locator("[data-monitor-thread-id]")
        .filter({
          hasText: title,
        })
        .click();
      const editor = peer.getByRole("textbox", {
        name: "Reply to " + title,
        exact: true,
      });
      await editor.fill("Retained private monitor draft");
      const before = await ctx.readSnapshot();
      await peer
        .getByRole("button", {
          name: "Hide thread monitor",
          exact: true,
        })
        .click();
      await waitForFixture(async () => !(await state()).visible, "native monitor hidden");
      const toggle = ctx.page.getByRole("button", {
        name: "Show thread monitor",
        exact: true,
      });
      await toggle.click();
      await waitForFixture(async () => (await state()).visible, "toolbar shows native monitor");
      ctx.assert(
        (await editor.inputValue()) === "Retained private monitor draft",
        "Draft survives native hide/show",
      );
      ctx.assert(
        JSON.stringify(await ctx.readSnapshot()) === JSON.stringify(before),
        "Visibility has no domain effect",
      );
    });
    await ctx.step("Actual main minimization and live server rename", async () => {
      await mainWindow.evaluate((win) => win.minimize());
      await waitForFixture(
        async () => (await native(mainWindow)).minimized,
        "WM minimizes main BrowserWindow",
      );
      await waitForFixture(async () => {
        const s = await state();
        return !s.mainVisible && !s.mainFocused;
      }, "main native state reaches publisher");
      const before = await native(monitorWindow);
      ctx.assert(
        before.visible && before.alwaysOnTop,
        "Monitor remains visible while main is minimized",
      );
      await ctx.dispatch({
        type: "thread.metadata.update",
        commandId: NodeCrypto.randomUUID(),
        threadId,
        title: renamed,
      });
      await peer
        .getByRole("heading", {
          name: renamed,
          exact: true,
        })
        .waitFor();
      ctx.assert(
        (await peer
          .getByRole("textbox", {
            name: "Reply to " + renamed,
            exact: true,
          })
          .inputValue()) === "Retained private monitor draft",
        "Draft retains identity across live metadata update",
      );
      ctx.assert((await native(mainWindow)).minimized, "Live update never restores main window");
      evidence.observations.minimizedLive = {
        main: await native(mainWindow),
        monitor: await native(monitorWindow),
        bridge: await state(),
      };
      await capture("monitor-minimized-live");
    });
    await ctx.step("Genuine monitor reply reaches isolated provider once", async () => {
      const editor = peer.getByRole("textbox", {
        name: "Reply to " + renamed,
        exact: true,
      });
      await editor.fill("NATIVE_FRESH_B");
      await editor.press("Enter");
      const projection = await waitForFixture(
        async () => {
          const p = await snapshot();
          return p.runs.length === 1 &&
            p.runs[0].status === "completed" &&
            p.checkpoints.some((c) => c.runId === p.runs[0].id && c.status === "ready")
            ? p
            : null;
        },
        "durable provider run and checkpoint from monitor",
        45000,
      );
      const trace = await readTrace();
      ctx.assert(
        trace.filter((e) => e.method === "turn/start" && e.input === "NATIVE_FRESH_B").length === 1,
        "Exactly one provider delivery",
      );
      ctx.assert(
        !trace.some((e) => e.method === "turn/steer" || e.method === "turn/interrupt"),
        "Monitor reply never steers or interrupts",
      );
      ctx.assert(
        projection.messages.filter((m) => m.role === "user" && m.text === "NATIVE_FRESH_B")
          .length === 1,
        "Exactly one durable user message",
      );
      await waitForFixture(
        async () => (await editor.inputValue()) === "",
        "accepted submitted draft cleared",
      );
      ctx.assert((await native(mainWindow)).minimized, "Reply does not restore the main window");
      evidence.observations.delivery = {
        runs: projection.runs,
        checkpoints: projection.checkpoints,
        trace,
      };
      await capture("monitor-real-reply-complete");
    });
    await ctx.step("Real Open in Jones intent restores correct main route", async () => {
      await peer
        .getByRole("button", {
          name: /^Open in Jones Code/,
        })
        .click();
      await waitForFixture(
        async () => !(await native(mainWindow)).minimized && (await native(mainWindow)).visible,
        "main restored by explicit Open",
      );
      await ctx.page.waitForFunction(
        (id) => location.pathname.includes(id) || location.hash.includes(id),
        threadId,
      );
      ctx.assert((await state()).mainVisible, "Main visibility is truthful after explicit Open");
    });
    await ctx.step("Safe Settle and real placement-only Restore", async () => {
      const settle = peer.getByRole("button", {
        name: "Settle",
        exact: true,
      });
      await waitForFixture(
        async () => (await settle.getAttribute("aria-disabled")) !== "true",
        "fresh safe settle availability",
      );
      await settle.click();
      await waitForFixture(
        async () =>
          (await ctx.readSnapshot()).threads.find((t) => t.id === threadId)?.settledAt != null,
        "real server settlement",
      );
      const restore = peer.getByRole("button", {
        name: "Restore thread",
        exact: true,
      });
      await restore.waitFor();
      ctx.assert(
        (await restore.getAttribute("aria-disabled")) !== "true",
        "Actual settle receipt enables supported placement restoration",
      );
      await restore.click();
      await waitForFixture(
        async () =>
          (await ctx.readSnapshot()).threads.find((t) => t.id === threadId)?.settledAt === null,
        "real placement restoration",
      );
      ctx.assert((await snapshot()).runs.length === 1, "Restore creates no extra provider run");
      await capture("monitor-restored-placement");
    });
    evidence.status = "passed";
  } finally {
    try {
      await stopMotion();
    } finally {
      await persist();
    }
  }
}
