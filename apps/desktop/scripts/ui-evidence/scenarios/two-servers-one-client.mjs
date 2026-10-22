import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeTimersPromises from "node:timers/promises";

// The runner stages the selected scenario at /harness/scenario.mjs.
const support = new URL(import.meta.url).pathname.endsWith("/harness/scenario.mjs")
  ? "./support/"
  : "../support/";
const { SecondaryServer, SECONDARY } = await import(`${support}secondary-server.mjs`);
const { EndpointTrace, assertPinRouting } = await import(`${support}endpoint-trace.mjs`);
const A = "http://127.0.0.1:43773";
const B = SECONDARY.origin;
const hash = (value) => NodeCrypto.createHash("sha256").update(value).digest("hex");
const requireThat = (condition, code) => {
  if (!condition) throw new Error(code);
};

export function canonical(value) {
  const ordered = (item) =>
    Array.isArray(item)
      ? item.map(ordered)
      : item !== null && typeof item === "object"
        ? Object.fromEntries(
            Object.keys(item)
              .sort()
              .map((key) => [key, ordered(item[key])]),
          )
        : item;
  return JSON.stringify(ordered(value));
}

export function rowSelector(environmentId, threadId, pinned) {
  if (
    ![environmentId, threadId].every((id) => typeof id === "string" && /^[a-zA-Z0-9-]+$/.test(id))
  )
    throw new Error("row-identity-not-environment-scoped");
  const row = `li[data-thread-item="${environmentId}:${threadId}"]`;
  if (pinned === undefined) return row;
  return pinned
    ? `[data-testid="sidebar-pinned-header"] ~ ${row}:has(~ [data-testid="sidebar-pinned-divider"])`
    : `[data-testid="sidebar-pinned-divider"] ~ ${row}`;
}

function one(rows, id) {
  const matches = rows.filter((row) => row.id === id);
  requireThat(matches.length === 1, "fixture-entity-missing-or-duplicated");
  return matches[0];
}

export function selectedSnapshot(snapshot, fixture) {
  return canonical({
    project: one(snapshot.projects, fixture.projectId),
    target: one(snapshot.threads, fixture.threadId),
    control: one(snapshot.threads, fixture.controlThreadId),
    threadIds: snapshot.threads.map((row) => row.id).sort(),
    archivedThreadIds: snapshot.archivedThreads.map((row) => row.id).sort(),
  });
}

export function assertPinSnapshot(snapshot, fixture, pinned) {
  const target = one(snapshot.threads, fixture.threadId);
  const control = one(snapshot.threads, fixture.controlThreadId);
  requireThat(
    target.projectId === fixture.projectId && control.projectId === fixture.projectId,
    "fixture-project-mismatch",
  );
  requireThat(
    pinned
      ? typeof target.pinnedAt === "string" &&
          Number.isFinite(Date.parse(target.pinnedAt)) &&
          typeof target.pinOrderKey === "string" &&
          target.pinOrderKey.length > 0
      : target.pinnedAt === null && target.pinOrderKey === null,
    "backend-pin-mismatch",
  );
  requireThat(control.pinnedAt === null && control.pinOrderKey === null, "control-pin-changed");
}

export function inspectEncryptedCatalog(bytes) {
  requireThat(bytes.length <= 1024 * 1024, "catalog-unbounded");
  let document;
  try {
    document = JSON.parse(bytes);
  } catch {
    throw new Error("catalog-not-encrypted");
  }
  requireThat(
    document?.version === 1 &&
      typeof document.encryptedCatalog === "string" &&
      Object.keys(document).sort().join(",") === "encryptedCatalog,version",
    "catalog-not-encrypted",
  );
  const cipher = Buffer.from(document.encryptedCatalog, "base64");
  requireThat(
    cipher.length > 0 && cipher.toString("base64") === document.encryptedCatalog,
    "catalog-not-encrypted",
  );
  requireThat(
    ![bytes, cipher].some(
      (value) => value.includes("127.0.0.1:43774") || value.includes("bearer:"),
    ),
    "catalog-plaintext-present",
  );
  return { encrypted: true, plaintextAbsent: true, bytes: bytes.length, sha256: hash(bytes) };
}

export default async function twoServersOneClient(ctx) {
  const fixture = {
    projectId: NodeCrypto.randomUUID(),
    threadId: NodeCrypto.randomUUID(),
    controlThreadId: NodeCrypto.randomUUID(),
    liveThreadId: NodeCrypto.randomUUID(),
    title: "JE03 colliding thread",
    controlTitle: "JE03 control",
    renamedControl: "JE03 B renamed while off",
    liveTitle: "JE03 A live probe",
    renamedLive: "JE03 A live probe updated",
  };
  Object.assign(ctx.fixture, fixture);
  const evidence = {
    schema: "je03-two-servers/v1",
    status: "failed",
    excludedFields: [],
    scenarioCapMs: 300000,
    operatorCapMs: 390000,
    steps: [],
    bindings: {
      rowKey: "environmentId:threadId",
      disabledStatus: "Off",
      pinCommand: "thread.pin",
      runtimeFields: ["version", "pid", "port", "origin"],
      observability: "page request/websocket events, attach then ordinary reload",
      restartDisabledRows: "U8 observational, non-gating",
    },
  };
  const controller = new AbortController();
  const trace = new EndpointTrace(fixture.threadId);
  const b = new SecondaryServer({ signal: controller.signal });
  let identity, before, selectedBefore, bFingerprint, failed;
  let stepSignal = controller.signal;
  const cancel = () => controller.abort(new Error("je03-cancelled"));
  process.once("SIGINT", cancel);
  process.once("SIGTERM", cancel);
  const timer = setTimeout(() => controller.abort(new Error("je03-scenario-cap")), 300000);

  const take = async (work) => {
    stepSignal.throwIfAborted();
    const result = await work();
    stepSignal.throwIfAborted();
    return result;
  };
  const step = async (name, timeoutMs, work) => {
    controller.signal.throwIfAborted();
    const local = new AbortController();
    stepSignal = AbortSignal.any([controller.signal, local.signal]);
    const started = performance.now();
    const entry = { name, timeoutMs, status: "failed" };
    evidence.steps.push(entry);
    const deadline = setTimeout(() => local.abort(new Error(`${name}-timeout`)), timeoutMs);
    let onAbort;
    try {
      await ctx.step(name, () =>
        Promise.race([
          work(),
          new Promise((_, reject) => {
            onAbort = () => reject(stepSignal.reason);
            stepSignal.addEventListener("abort", onAbort, { once: true });
            if (stepSignal.aborted) onAbort();
          }),
        ]),
      );
      entry.status = "passed";
    } catch (error) {
      controller.abort(new Error("je03-step-failed"));
      // Browser errors can quote fill arguments, RPC URLs and DOM values.
      const code =
        typeof error?.message === "string" && /^[a-z][a-z0-9-]{1,90}$/.test(error.message)
          ? error.message
          : `${name}-failed`;
      // oxlint-disable-next-line preserve-caught-error -- Browser error causes can contain pairing credentials; retain only the sanitized code.
      throw new Error(code);
    } finally {
      clearTimeout(deadline);
      stepSignal.removeEventListener("abort", onAbort);
      entry.durationMs = Math.round(performance.now() - started);
    }
  };
  const until = async (predicate, timeoutMs, code) => {
    const end = performance.now() + timeoutMs;
    while (performance.now() < end) {
      if (await take(predicate)) return;
      await take(() => NodeTimersPromises.setTimeout(50, undefined, { signal: stepSignal }));
    }
    throw new Error(code);
  };
  const row = (env, id, pinned) => ctx.page.locator(rowSelector(env, id, pinned));
  const showThreads = async () => {
    const back = ctx.page.getByRole("button", {
      name: "Back to conversation from Settings",
      exact: true,
    });
    if (await take(() => back.isVisible())) await take(() => back.click({ timeout: 5000 }));
  };
  const settings = async () => {
    const button = ctx.page.getByRole("button", { name: "Settings", exact: true });
    if (await take(() => button.isVisible())) await take(() => button.click({ timeout: 5000 }));
    await take(() =>
      ctx.page.getByRole("button", { name: "Connections", exact: true }).click({ timeout: 5000 }),
    );
    await take(() =>
      ctx.page
        .getByRole("button", { name: "Add environment", exact: true })
        .waitFor({ timeout: 5000 }),
    );
  };
  const bEntry = async (enabled) => {
    const toggle = ctx.page.getByRole("switch", {
      name: `${enabled ? "Switch off" : "Switch on"} ${identity.label}`,
      exact: true,
    });
    const entry = ctx.page
      .locator('div[class*="grid-cols-[auto_minmax(0,1fr)_auto]"]')
      .filter({ has: toggle })
      .filter({ hasText: B });
    await until(async () => (await entry.count()) === 1, 5000, "b-entry-ambiguous");
    requireThat((await take(() => entry.getByRole("switch").count())) === 1, "b-entry-ambiguous");
    return entry;
  };
  const status = async (enabled) => {
    const entry = await bEntry(enabled);
    const text = `${B}/ · ${enabled ? "Connected" : "Off"}`;
    // Onboarding normalizes the profile to a trailing slash; tolerate display normalization only.
    await until(
      async () => {
        const content = await entry.innerText();
        return content.includes(text) || content.includes(text.replace(`${B}/`, B));
      },
      10000,
      enabled ? "b-not-connected" : "disabled-status-unbound",
    );
    return entry;
  };
  const uiPin = async (env, id, pinned, writable = true) => {
    const selected = row(env, id, pinned);
    await take(() => selected.waitFor({ state: "visible", timeout: 10000 }));
    requireThat(
      (await take(() => selected.count())) === 1 &&
        (await take(() => row(env, id, !pinned).count())) === 0,
      "row-identity-not-environment-scoped",
    );
    requireThat(
      (await take(() =>
        selected.getByRole("button", { name: "Unpin thread", exact: true }).count(),
      )) === (pinned && writable ? 1 : 0),
      "ui-pin-mismatch",
    );
    requireThat(
      (await take(() => selected.getByRole("img", { name: "Pinned", exact: true }).count())) ===
        (pinned && !writable ? 1 : 0),
      "ui-pin-indicator-mismatch",
    );
  };
  const title = async (env, id, value, timeout = 10000) => {
    await take(() =>
      row(env, id)
        .getByText(value, { exact: true })
        .and(row(env, id).locator("span[aria-hidden]"))
        .waitFor({ state: "visible", timeout }),
    );
  };
  const seed = async (dispatch, workspace, includeLive) => {
    await take(() =>
      dispatch({
        type: "project.create",
        commandId: NodeCrypto.randomUUID(),
        projectId: fixture.projectId,
        title: "JE03 colliding project",
        workspaceRoot: workspace,
      }),
    );
    const threads = [
      [fixture.threadId, fixture.title],
      [fixture.controlThreadId, fixture.controlTitle],
    ];
    if (includeLive) threads.push([fixture.liveThreadId, fixture.liveTitle]);
    for (const [threadId, threadTitle] of threads)
      await take(() =>
        dispatch({
          type: "thread.create",
          commandId: NodeCrypto.randomUUID(),
          createdBy: "user",
          creationSource: "web",
          projectId: fixture.projectId,
          threadId,
          title: threadTitle,
          modelSelection: { instanceId: "codex", model: "gpt-5" },
          runtimeMode: "approval-required",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
        }),
      );
  };
  const selectedInvariant = async () => {
    const snapshot = await take(() => ctx.readSnapshot());
    const selected = selectedSnapshot(snapshot, fixture);
    requireThat(selected === selectedBefore, "a-selected-invariance-failed");
    return hash(selected);
  };
  const checkpoint = async () => {
    evidence.crossover = await take(() => trace.checkpoint());
  };

  try {
    await step("onboarding-and-observability", 40000, async () => {
      await take(() => ctx.completeOnboarding());
      const topology = await take(() => ctx.topology());
      requireThat(new URL(topology.httpBaseUrl).origin === A, "primary-origin-mismatch");
      trace.attach(ctx.page);
      await take(() => ctx.reload());
      await until(
        () => trace.entries.some((entry) => entry.kind === "websocket" && entry.origin === A),
        30000,
        "renderer-network-unobservable",
      );
    });
    await step("seed-a", 25000, () =>
      seed((command) => ctx.dispatch(command), ctx.workspace, true),
    );
    await step("start-and-seed-b", 60000, async () => {
      identity = await take(() => b.start());
      evidence.environments = { a: identity.primaryEnvironmentId, b: identity.environmentId };
      await seed((command) => b.seed(command), SECONDARY.workspace, false);
    });
    await step("pair-b-ui", 30000, async () => {
      await settings();
      await take(() =>
        ctx.page
          .getByRole("button", { name: "Add environment", exact: true })
          .click({ timeout: 5000 }),
      );
      const dialog = ctx.page.getByRole("dialog", { name: "Add Environment", exact: true });
      await take(() =>
        dialog.getByRole("button", { name: /Remote link/ }).click({ timeout: 5000 }),
      );
      await take(() => dialog.getByLabel("Host", { exact: true }).fill(B, { timeout: 5000 }));
      await take(() =>
        dialog.getByLabel("Pairing code", { exact: true }).fill(b.pairingCode, { timeout: 5000 }),
      );
      let pairingStage = "submit-click";
      try {
        await take(() =>
          dialog
            .getByRole("button", { name: "Add environment", exact: true })
            .click({ timeout: 5000 }),
        );
        pairingStage = "toast-visible";
        await take(() =>
          ctx.page.getByText("Backend added", { exact: true }).waitFor({ timeout: 15000 }),
        );
        pairingStage = "dialog-hidden";
        await take(() => dialog.waitFor({ state: "hidden", timeout: 5000 }));
      } catch (error) {
        const message = typeof error?.message === "string" ? error.message.slice(0, 8192) : "";
        const category = message.includes("strict mode violation")
          ? "strict-locator"
          : error?.name === "TimeoutError" ||
              /Timeout \d+ms exceeded|^pair-b-ui-timeout$/.test(message)
            ? "timeout"
            : error?.name === "TargetClosedError" ||
                /Target (?:page, context or browser has been closed|closed)/.test(message)
              ? "target-closed"
              : "other";
        const diagnostic = {
          stage: pairingStage,
          category,
          countLimit: 20,
          probeLimitMs: 750,
          probeStatus: "unavailable",
        };
        evidence.pairingUi = diagnostic;
        let diagnosticTimer;
        try {
          const counts = await Promise.race([
            Promise.all([
              ctx.page.getByText("Backend added", { exact: true }).count(),
              ctx.page
                .locator('[data-slot="toast-title"]')
                .filter({ hasText: /^Backend added$/ })
                .count(),
              dialog.count(),
              ctx.page.getByText("Could not add backend", { exact: true }).count(),
            ]),
            new Promise((resolve) => {
              diagnosticTimer = setTimeout(() => resolve(null), 750);
            }),
          ]).catch(() => null);
          if (counts) {
            const [successText, successTitle, dialogs, failureText] = counts.map((count) =>
              Math.min(count, diagnostic.countLimit),
            );
            diagnostic.counts = { successText, successTitle, dialogs, failureText };
            diagnostic.probeStatus = "observed";
          }
        } finally {
          clearTimeout(diagnosticTimer);
        }
        // oxlint-disable-next-line preserve-caught-error -- Browser errors and causes can contain pairing credentials; retain only stage, category and bounded counts.
        throw new Error(`remote-link-${pairingStage}-${category}`);
      }
      const catalog = await take(() => trace.catalog(ctx.page, identity.environmentId));
      requireThat(!catalog.disabled, "paired-b-disabled");
      bFingerprint = catalog.b;
      await status(true);
      await showThreads();
      await checkpoint();
      for (const origin of [A, B])
        requireThat(
          trace.entries.some(
            (entry) => entry.kind === "request" && entry.origin === origin && entry.authFp !== null,
          ),
          "renderer-network-unobservable",
        );
    });
    await step("pre-action-non-vacuity", 15000, async () => {
      assertPinSnapshot(await take(() => b.readSnapshot()), fixture, false);
      await uiPin(identity.environmentId, fixture.threadId, false);
      await uiPin(identity.primaryEnvironmentId, fixture.threadId, false);
      await take(() =>
        row(identity.environmentId, fixture.threadId, false)
          .getByRole("button", { name: fixture.title })
          .click({ timeout: 5000 }),
      );
      await take(() =>
        ctx.page.waitForFunction(
          ({ environmentId, threadId }) => location.pathname === `/${environmentId}/${threadId}`,
          { environmentId: identity.environmentId, threadId: fixture.threadId },
          { timeout: 5000 },
        ),
      );
      await take(() => ctx.page.getByTestId("composer-editor").waitFor({ timeout: 5000 }));
      await take(() => ctx.page.getByTestId("composer-editor").focus({ timeout: 5000 }));
      const snapshot = await take(() => ctx.readSnapshot());
      before = canonical(snapshot);
      selectedBefore = selectedSnapshot(snapshot, fixture);
      evidence.aBeforeSha256 = hash(before);
      await checkpoint();
    });
    await step("pin-b", 15000, async () => {
      const from = trace.mark();
      await take(() => ctx.page.keyboard.press("Control+Shift+P"));
      await uiPin(identity.environmentId, fixture.threadId, true);
      await uiPin(identity.primaryEnvironmentId, fixture.threadId, false);
      assertPinSnapshot(await take(() => b.readSnapshot()), fixture, true);
      const after = canonical(await take(() => ctx.readSnapshot()));
      requireThat(after === before, "a-strict-invariance-failed");
      evidence.aStrictInvariance = true;
      evidence.aAfterSha256 = hash(after);
      await checkpoint();
      evidence.pinRouting = assertPinRouting(trace.entries, from, trace.mark(), trace.generation);
    });
    await step("credential-fence", 15000, async () => {
      evidence.nc2 = await take(() => b.credentialFence());
      await checkpoint();
    });
    await step("switch-off-b", 20000, async () => {
      await settings();
      const entry = await bEntry(true);
      const from = trace.mark();
      await take(() => entry.getByRole("switch").click({ timeout: 5000 }));
      await until(
        () =>
          trace.entries.some(
            (item) => item.sequence >= from && item.kind === "close" && item.origin === B,
          ),
        10000,
        "b-socket-not-closed",
      );
      await status(false);
      const catalog = await take(() => trace.catalog(ctx.page, identity.environmentId));
      requireThat(catalog.disabled, "b-not-disabled");
      await showThreads();
      await uiPin(identity.environmentId, fixture.threadId, true, false);
      await title(identity.environmentId, fixture.controlThreadId, fixture.controlTitle);
      evidence.bRetainedRows = { ok: true };
      await checkpoint();
    });
    await step("b-not-live", 15000, async () => {
      const from = trace.mark();
      await take(() =>
        b.seed({
          type: "thread.metadata.update",
          commandId: NodeCrypto.randomUUID(),
          threadId: fixture.controlThreadId,
          title: fixture.renamedControl,
        }),
      );
      requireThat(
        one((await take(() => b.readSnapshot())).threads, fixture.controlThreadId).title ===
          fixture.renamedControl,
        "b-offline-rename-not-committed",
      );
      await take(() => NodeTimersPromises.setTimeout(5000, undefined, { signal: stepSignal }));
      await title(identity.environmentId, fixture.controlThreadId, fixture.controlTitle, 1000);
      requireThat(
        (await take(() =>
          row(identity.environmentId, fixture.controlThreadId)
            .getByText(fixture.renamedControl, { exact: true })
            .count(),
        )) === 0,
        "b-live-while-disabled",
      );
      requireThat(
        !trace.entries.some(
          (entry) =>
            entry.sequence >= from &&
            entry.origin === B &&
            ["frame", "websocket"].includes(entry.kind),
        ),
        "b-live-while-disabled",
      );
      evidence.nc3 = { ok: true, quietWindowMs: 5000 };
      await checkpoint();
    });
    await step("a-stays-live", 10000, async () => {
      await take(() =>
        ctx.dispatch({
          type: "thread.metadata.update",
          commandId: NodeCrypto.randomUUID(),
          threadId: fixture.liveThreadId,
          title: fixture.renamedLive,
        }),
      );
      await title(identity.primaryEnvironmentId, fixture.liveThreadId, fixture.renamedLive);
      evidence.aLive = { ok: true, selectedSha256: await selectedInvariant() };
      await checkpoint();
    });
    await step("restart-disabled-b", 90000, async () => {
      const restart = await take(() => ctx.restart());
      requireThat(restart.oldProcessesClosed === true, "restart-processes-not-closed");
      trace.attach(ctx.page);
      await take(() => ctx.reload());
      await until(
        () =>
          trace.entries.some(
            (entry) =>
              entry.generation === trace.generation &&
              entry.kind === "websocket" &&
              entry.origin === A,
          ),
        30000,
        "renderer-network-unobservable",
      );
      const catalog = await take(() => trace.catalog(ctx.page, identity.environmentId));
      requireThat(catalog.disabled && catalog.b === bFingerprint, "catalog-restart-mismatch");
      for (const directory of ["/scratch/t3home", "/scratch/t3home/userdata"])
        requireThat(
          (await take(() => NodeFSP.lstat(directory))).isDirectory(),
          "catalog-path-unbound",
        );
      const path = "/scratch/t3home/userdata/connection-catalog.json";
      const info = await take(() => NodeFSP.lstat(path));
      requireThat(info.isFile() && info.size <= 1024 * 1024, "catalog-path-unbound");
      evidence.catalog = {
        ...inspectEncryptedCatalog(await take(() => NodeFSP.readFile(path))),
        restored: catalog,
      };
      const storage = await take(() =>
        ctx.electronApp.evaluate(({ safeStorage }) => ({
          available: safeStorage.isEncryptionAvailable(),
          backend: safeStorage.getSelectedStorageBackend(),
        })),
      );
      requireThat(
        storage.available && storage.backend === "gnome_libsecret",
        "catalog-encryption-unavailable",
      );
      evidence.catalog.storage = storage;
      await settings();
      await status(false);
      await showThreads();
      evidence.restartDisabledRows = {
        gating: false,
        targetCount: await take(() => row(identity.environmentId, fixture.threadId).count()),
        controlCount: await take(() =>
          row(identity.environmentId, fixture.controlThreadId).count(),
        ),
      };
      evidence.aRestartSelectedSha256 = await selectedInvariant();
      requireThat(
        !trace.entries.some(
          (entry) =>
            entry.generation === trace.generation &&
            entry.origin === B &&
            ["websocket", "frame"].includes(entry.kind),
        ),
        "disabled-b-reconnected-after-restart",
      );
      await checkpoint();
    });
    await step("reenable-b", 20000, async () => {
      await settings();
      const entry = await bEntry(false);
      const from = trace.mark();
      await take(() => entry.getByRole("switch").click({ timeout: 5000 }));
      await status(true);
      await showThreads();
      await uiPin(identity.environmentId, fixture.threadId, true);
      await title(identity.environmentId, fixture.controlThreadId, fixture.renamedControl);
      const catalog = await take(() => trace.catalog(ctx.page, identity.environmentId));
      requireThat(!catalog.disabled && catalog.b === bFingerprint, "reenable-credential-mismatch");
      await checkpoint();
      requireThat(
        trace.entries.some(
          (item) => item.sequence >= from && item.origin === B && item.kind === "websocket",
        ),
        "renderer-network-unobservable",
      );
      requireThat(
        !trace.entries.some(
          (item) =>
            item.sequence >= from &&
            item.origin === B &&
            item.kind === "request" &&
            item.pathname === "/oauth/token",
        ),
        "reenable-paired-again",
      );
      evidence.reenable = { ok: true, sameCredential: true, pairedAgain: false };
      evidence.aFinalSelectedSha256 = await selectedInvariant();
      await checkpoint();
    });
    evidence.status = "passed";
  } catch (error) {
    failed = error;
    evidence.failure = /^[a-z][a-z0-9-]{1,90}$/.test(error.message) ? error.message : "je03-failed";
  } finally {
    clearTimeout(timer);
    process.removeListener("SIGINT", cancel);
    process.removeListener("SIGTERM", cancel);
    trace.detach();
    try {
      await trace.flush();
    } catch {
      failed ??= new Error("renderer-network-unobservable");
    }
    const path = `${ctx.artifacts}/je03-evidence.json`;
    let safeToWrite = false;
    try {
      evidence.trace = trace.summary();
      evidence.b = b.evidence;
      b.assertSafeEvidence(evidence);
      await NodeFSP.writeFile(path, JSON.stringify(evidence, null, 2), { flag: "wx", mode: 0o600 });
      safeToWrite = true;
    } catch {
      failed ??= new Error("secret-in-evidence-or-write-failed");
    }
    try {
      await b.stop();
    } catch {
      failed = new Error("secondary-stop-unknown");
    }
    evidence.b = b.evidence;
    if (failed) {
      evidence.status = "failed";
      evidence.failure = failed.message;
    }
    try {
      // Only the fixed-shape stop result has changed since the secret scan.
      if (safeToWrite)
        await NodeFSP.writeFile(path, JSON.stringify(evidence, null, 2), { mode: 0o600 });
    } catch {
      failed ??= new Error("je03-evidence-write-failed");
    }
    trace.dispose();
  }
  if (failed) throw failed;
}
