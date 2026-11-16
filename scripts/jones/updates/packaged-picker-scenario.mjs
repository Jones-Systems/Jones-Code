import * as NodeAssert from "node:assert/strict";

const repo = "Jones-Systems/Jones-Code",
  root = `repos/${repo}`;
export const labels = {
  catalog: "synthetic GitHub metadata through real JonesActionsClient qualification",
  worker: "synthetic preparation with controlled failure/pending result; no payload download",
  state: "fresh CI-owned empty database and profile",
  renderer: "owned page and scripted caller; production web UI handoff not repeated",
  loader: "modified disposable COPY layout; unchanged packaged narrow-entry bytes",
};
export const bridgeMembers = [
  "getUpdateState",
  "setUpdateChannel",
  "checkForUpdate",
  "listJonesUpdateBuilds",
  "selectJonesUpdateBuild",
  "downloadUpdate",
  "discardUpdate",
  "setJonesBackgroundPreparation",
  "installPlan",
  "onInstallPlan",
  "fleetUpdates",
  "installUpdate",
  "onUpdateState",
].sort();

// The fixture supplies API records, not a replacement for qualification or admission.
export function createPickerFixture(installedSource, onPrepare = () => {}) {
  const calls = [],
    jobs = [],
    downloads = [];
  const builds = [0, 1].map((index) => {
    const id = 201 - index,
      source = (index ? "b" : "a").repeat(40),
      tree = (index ? "d" : "c").repeat(40);
    return {
      id,
      source,
      tree,
      attempt: 1,
      artifactId: 301 - index,
      digest: (index ? "f" : "e").repeat(64),
    };
  });
  // No source collision with a real artifact's installed revision.
  if (builds.some((b) => b.source === installedSource))
    throw new Error("Synthetic source collision.");
  const workflow = ".github/workflows/artifact-desktop-mac.yml";
  const version = (b) => `0.0.44-preview.20261116.${b.id}.${b.attempt}`;
  const run = (b, ci = false) => ({
    id: ci ? b.id + 1000 : b.id,
    path: ci ? ".github/workflows/ci.yml" : workflow,
    workflow_id: ci ? 12 : 10,
    run_attempt: b.attempt,
    head_sha: b.source,
    head_branch: "main",
    event: "push",
    status: "completed",
    conclusion: "success",
    run_started_at: "2026-11-16T12:00:00Z",
    repository: { id: 77, full_name: repo },
    head_repository: { id: 77, full_name: repo },
  });
  const artifact = (b) => ({
    id: b.artifactId,
    name: `desktop-mac-arm64-${b.id}-${b.attempt}--${version(b)}`,
    size_in_bytes: 100,
    expired: false,
    expires_at: "2099-01-01T00:00:00Z",
    digest: `sha256:${b.digest}`,
    workflow_run: {
      id: b.id,
      repository_id: 77,
      head_repository_id: 77,
      head_branch: "main",
      head_sha: b.source,
    },
  });
  const transport = {
    api: async (endpoint) => {
      calls.push(endpoint);
      if (endpoint === `${root}/commits/main`) return { sha: builds[0].source };
      for (const b of builds) {
        if (endpoint === `${root}/git/commits/${b.source}`)
          return { sha: b.source, tree: { sha: b.tree } };
        if (endpoint === `${root}/actions/runs/${b.id}`) return run(b);
        if (endpoint === `${root}/actions/runs/${b.id + 1000}`) return run(b, true);
        if (endpoint.includes(`/actions/runs/${b.id}/artifacts?`))
          return { artifacts: [artifact(b)] };
        if (endpoint === `${root}/actions/artifacts/${b.artifactId}`) return artifact(b);
      }
      if (endpoint.startsWith(`${root}/compare/`)) {
        const [base, head] = endpoint.split("/compare/")[1].split("?")[0].split("...");
        if (
          ![installedSource, ...builds.map((b) => b.source)].includes(base) ||
          !builds.some((b) => b.source === head)
        )
          throw new Error("Unexpected ancestry.");
        return { status: base === head ? "identical" : "ahead", merge_base_commit: { sha: base } };
      }
      if (endpoint.includes("/actions/workflows/artifact-desktop-mac.yml/runs?")) {
        const query = new URL(`https://fixture.invalid/${endpoint}`).searchParams;
        const sha = query.get("head_sha");
        return {
          workflow_runs:
            query.get("page") === "1"
              ? builds.filter((b) => !sha || b.source === sha).map((b) => run(b))
              : [],
        };
      }
      if (endpoint.includes("/actions/workflows/ci.yml/runs?")) {
        const sha = new URL(`https://fixture.invalid/${endpoint}`).searchParams.get("head_sha");
        return { workflow_runs: builds.filter((b) => b.source === sha).map((b) => run(b, true)) };
      }
      throw new Error(`Unexpected synthetic endpoint: ${endpoint}`);
    },
    download: async (...args) => {
      downloads.push(args);
      throw new Error("Transport download forbidden.");
    },
  };
  let mode = "fail",
    release;
  const worker = {
    run: async (job, onVerifying) => {
      jobs.push(structuredClone(job));
      onPrepare(job);
      if (job.kind !== "prepare") throw new Error(`Forbidden worker job: ${job.kind}`);
      if (mode === "pending") {
        await onVerifying?.();
        return new Promise((resolve) => {
          release = resolve;
        });
      }
      throw new Error("Controlled synthetic staging failure.");
    },
    cancel: async () => "not-running",
    reaped: () => true,
    heldWorker: () => undefined,
    close: async () => {},
  };
  return {
    transport,
    worker,
    calls,
    jobs,
    downloads,
    builds,
    setMode: (value) => {
      mode = value;
    },
    release: () => release?.(),
    rebuild: () => {
      builds[1].attempt++;
      builds[1].artifactId++;
      builds[1].digest = "9".repeat(64);
    },
  };
}

export async function runPickerScenario(phase, host, fixture) {
  const checks = [];
  const check = (id, actual, expected) => {
    NodeAssert.deepEqual(actual, expected, id);
    checks.push({ id, status: "pass" });
  };
  const call = (member, ...args) => host.bridgeCall(member, args);
  const inspect = await host.inspect();
  check("IPC-A1-capability-members", inspect.members, bridgeMembers);
  check("IPC-A2-renderer-require", inspect.require, "undefined");
  check("IPC-A3-renderer-process", inspect.process, "undefined");
  check("IPC-A4-inert-list", await call("listJonesUpdateBuilds"), {
    builds: [],
    unavailable: true,
  });
  check("IPC-A5-inert-transport", fixture.calls.length, 0);
  await host.preflightStableAppPath();
  await host.start();
  let plan = await call("installPlan", { type: "read" });
  const start = (build) =>
    call("installPlan", {
      type: "start",
      origin: "update-everything",
      target: {
        repository: repo,
        sourceSha: build.sourceSha,
        sourceTree: build.sourceTree,
        version: build.version,
        laptopArtifactId: build.artifactId,
      },
      select: [],
      laptop: { action: "download" },
    });
  if (phase === "catalog") {
    const catalog = await call("listJonesUpdateBuilds");
    check("CAT-A1-two-builds", catalog.builds.length, 2);
    check("CAT-A2-catalog-unavailable", catalog.unavailable, false);
    check("CAT-A3-no-dispatch", fixture.jobs.length, 0);
    check("CAT-A4-no-plan", plan.state.current, null);
    const [, old] = catalog.builds;
    check("SEL-A1-unselected-start", (await start(old)).refused?.code, "not-selectable");
    fixture.rebuild();
    check("SEL-A2-stale-selection", await call("selectJonesUpdateBuild", old), false);
    const alternate = (await call("listJonesUpdateBuilds")).builds[1];
    const before = await call("getUpdateState");
    await NodeAssert.rejects(() => call("selectJonesUpdateBuild", { artifactId: "malformed" }));
    check("IPC-B1-malformed-no-state", await call("getUpdateState"), before);
    check("SEL-A3-alternate-selected", await call("selectJonesUpdateBuild", alternate), true);
    const selected = await call("getUpdateState");
    check("IPC-B2-selected-provenance", selected.jones.provenance, alternate);
    check("SEL-A4-no-dispatch-before-confirm", fixture.jobs.length, 0);
    const mismatch = await call("downloadUpdate", {
      artifactId: alternate.artifactId + 99,
      sourceSha: alternate.sourceSha,
    });
    check("SEL-A5-mismatch-refused", mismatch.accepted, false);
    plan = await start(alternate);
    check("PLAN-A1-start-admitted", plan.refused, undefined);
    await host.waitForJobs(1);
    await host.waitForPlan(
      (result) => result.state.current?.laptop.operations.at(-1)?.result?.status === "failed",
    );
    check("PREP-A1-exact-artifact", fixture.jobs[0].candidate.artifactId, alternate.artifactId);
    const snapshots = await call("installPlan", { type: "read" });
    const operation = snapshots.state.current.laptop.operations[0];
    check("PREP-A2-operation-token", fixture.jobs[0].token, operation.operationId);
    const duplicate = await start(alternate);
    check("PLAN-A2-duplicate-plan", duplicate.state.current.planId, snapshots.state.current.planId);
    check("PLAN-A3-duplicate-zero-dispatch", fixture.jobs.length, 1);
    const events = (await host.inspect()).events;
    check(
      "IPC-B3-provenance-listener",
      events.some(
        (e) =>
          e.kind === "state" &&
          JSON.stringify(e.value.jones?.provenance) === JSON.stringify(alternate),
      ),
      true,
    );
    check(
      "IPC-B4-plan-listener",
      events.some(
        (e) => e.kind === "plan" && e.value.current?.planId === snapshots.state.current.planId,
      ),
      true,
    );
  } else if (phase === "persisted") {
    check(
      "RESTART-A1-retained-failure",
      plan.state.current.laptop.operations.at(-1).result.status,
      "failed",
    );
    check("RESTART-A2-zero-auto-replay", fixture.jobs.length, 0);
    const retry = await call("installPlan", {
      type: "laptop",
      planId: plan.state.current.planId,
      action: "download",
    });
    check("RESTART-A3-requires-reselection", retry.refused?.code, "not-selectable");
    // The catalog fixture persists the rebuilt attempt across process launches.
    fixture.rebuild();
    const alternate = (await call("listJonesUpdateBuilds")).builds[1];
    check("RESTART-A4-explicit-reselection", await call("selectJonesUpdateBuild", alternate), true);
    await call("installPlan", {
      type: "laptop",
      planId: plan.state.current.planId,
      action: "download",
    });
    await host.waitForJobs(1);
    await host.waitForPlan(
      (result) => result.state.current?.laptop.operations.at(-1)?.result?.status === "failed",
    );
    check("RESTART-A5-single-explicit-retry", fixture.jobs.length, 1);
  } else if (phase === "inflight") {
    fixture.rebuild();
    const alternate = (await call("listJonesUpdateBuilds")).builds[1];
    check("RESTART-B1-reselection", await call("selectJonesUpdateBuild", alternate), true);
    fixture.setMode("pending");
    const requests = await Promise.all(
      [0, 1].map(() =>
        call("installPlan", {
          type: "laptop",
          planId: plan.state.current.planId,
          action: "download",
        }),
      ),
    );
    await host.waitForJobs(1);
    const state = await host.waitForPlan(
      (result) => result.state.current?.laptop.operations.at(-1)?.result?.code === "copy-admitted",
    );
    check("CONC-A1-one-dispatch", fixture.jobs.length, 1);
    check(
      "CONC-A2-one-plan",
      requests.every((r) => r.state.current.planId === state.state.current.planId),
      true,
    );
    check(
      "CONC-A3-operation-token",
      fixture.jobs[0].token,
      state.state.current.laptop.operations.at(-1).operationId,
    );
    // Caller exits the actual Electron process without closing this in-flight operation.
  } else if (phase === "held") {
    check(
      "RESTART-B2-held-status",
      plan.state.current.laptop.operations.at(-1).result.status,
      "held",
    );
    check(
      "RESTART-B3-held-code",
      plan.state.current.laptop.operations.at(-1).result.code,
      "copy-unconfirmed",
    );
    check("RESTART-B4-no-replay", fixture.jobs.length, 0);
    const refused = await call("installPlan", {
      type: "laptop",
      planId: plan.state.current.planId,
      action: "download",
    });
    check("RESTART-B5-held-refused", Boolean(refused.refused), true);
    check("RESTART-B6-no-extra-dispatch", fixture.jobs.length, 0);
  } else throw new Error("Unknown picker phase.");
  const state = await call("installPlan", { type: "read" });
  check("SAFE-A1-zero-native", host.nativeCalls, []);
  check("SAFE-A2-zero-install", host.installCalls(), 0);
  check("SAFE-A3-zero-transport-download", fixture.downloads.length, 0);
  check(
    "SAFE-A4-no-install-operation",
    state.state.current?.laptop.operations.some((o) => o.step === "install") ?? false,
    false,
  );
  await host.assertNoTransactions();
  return { phase, checks, jobs: fixture.jobs, calls: fixture.calls, labels };
}

export function createPickerObservations() {
  const listeners = new Set();
  return {
    notify: () => {
      for (const wake of listeners) wake();
    },
    wait: (predicate, read) =>
      new Promise((resolve, reject) => {
        let checking = false,
          again = false;
        const finish = (error, value) => {
          clearTimeout(deadline);
          listeners.delete(wake);
          if (error) reject(error);
          else resolve(value);
        };
        const wake = async () => {
          if (checking) {
            again = true;
            return;
          }
          checking = true;
          try {
            do {
              again = false;
              const value = await read();
              if (predicate(value)) {
                finish(null, value);
                return;
              }
            } while (again);
          } catch (error) {
            finish(error);
          } finally {
            checking = false;
          }
        };
        const deadline = setTimeout(
          () => finish(new Error("Picker observation deadline exceeded.")),
          30_000,
        );
        listeners.add(wake);
        void wake();
      }),
  };
}
