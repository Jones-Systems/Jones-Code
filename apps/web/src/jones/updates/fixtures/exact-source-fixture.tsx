import type { JonesAction } from "@t3tools/client-runtime/jones/updates";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesUpdateProvenance,
  JonesUpdateState,
} from "@t3tools/contracts/jones/jonesUpdates";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/reactivity";
import { useState, useSyncExternalStore, type ReactNode } from "react";
import { APP_SOURCE_SHA, APP_VERSION } from "../../../branding";
import { ComposerBannerStack } from "../../../components/chat/ComposerBannerStack";
import { requestConfirmDialog } from "../../../confirmDialog";
import { useClientUpdateBanners } from "../clientUpdateBanner";
import {
  ExactSourceUpdateAction,
  type ExactSourceHost,
  type ExactSourceTarget,
} from "../ExactSourceUpdateAction";

/**
 * A synthetic host behind the production exact-source controller. It answers requests from a
 * scripted state machine; nothing here reaches a network, a host or an installer.
 */
const EXACT_SCENARIOS = [
  "exact",
  "legacy-match",
  "legacy-mismatch",
  "staged-other",
  "stale",
  "unsupported",
] as const;
type ExactScenario = (typeof EXACT_SCENARIOS)[number];
const NEXT_DOWNLOAD = ["accept", "lost", "refuse", "wrong-source"] as const;
const NEXT_INSTALL = ["accept", "lost-started", "lost-not-started", "refuse"] as const;
type NextDownload = (typeof NEXT_DOWNLOAD)[number];
type NextInstall = (typeof NEXT_INSTALL)[number];

// Distinct from the running app's source, which this fixture reads from the build.
const distinct = (...taken: ReadonlyArray<string | undefined>) =>
  ["b", "c", "d", "e", "f", "1"].find((digit) => !taken.some((sha) => sha?.startsWith(digit)))!;
const INSTALLED_SOURCE = distinct(APP_SOURCE_SHA).repeat(39) + "9";
const OFFERED_SOURCE = distinct(APP_SOURCE_SHA, INSTALLED_SOURCE).repeat(39) + "8";
const HOST_VERSION = "0.1.0-preview.20261019.39341000929.1";

const build = (sourceSha: string, artifactId: number, version: string): JonesUpdateProvenance => ({
  repository: "Jones-Systems/Jones-Code",
  sourceSha,
  sourceTree: sourceSha,
  workflow: ".github/workflows/artifact-desktop.yml",
  runId: artifactId,
  runAttempt: 1,
  artifactId,
  artifactDigest: "1".repeat(64),
  platform: "linux",
  architecture: "x64",
  version,
});
const offeredBuild = () => build(OFFERED_SOURCE, 501, "0.1.0-preview.20261019.39341000950.1");
// Another platform's artifact of the app's own source carries its own run number and version.
const targetBuild = () => build(APP_SOURCE_SHA ?? "", 502, "0.1.0-preview.20261019.39341000901.1");

const installedSource = (sha: string) => ({
  repository: "Jones-Systems/Jones-Code" as const,
  sha,
  tree: sha,
});
function initialState(scenario: ExactScenario): JonesUpdateState | null {
  if (scenario === "unsupported") return null;
  const legacy = scenario === "legacy-match" || scenario === "legacy-mismatch";
  return {
    source: "jones-actions",
    channel: "jones-main",
    phase: scenario === "staged-other" ? "staged" : "available",
    revision: 1,
    capability: legacy
      ? { check: true, download: true, install: true }
      : { check: true, download: true, install: true, exactSource: true },
    currentVersion: HOST_VERSION,
    installedSource: installedSource(INSTALLED_SOURCE),
    provenance: scenario === "legacy-match" ? targetBuild() : offeredBuild(),
    ...(scenario === "staged-other" ? { stagedHandle: "staged-other-handle" } : {}),
    message: "A newer main build is available.",
  };
}

interface ExactStoreSnapshot {
  readonly scenario: ExactScenario;
  readonly state: JonesUpdateState | null;
  readonly freshness: "fresh" | "stale" | "unsupported";
  readonly canOperate: boolean;
  readonly nextDownload: NextDownload;
  readonly nextInstall: NextInstall;
  readonly calls: ReadonlyArray<{ sequence: number; action: string; input: unknown }>;
}

function createSyntheticExactStore(initialScenario: ExactScenario = "exact") {
  const listeners = new Set<() => void>();
  const freshnessOf = (scenario: ExactScenario) =>
    scenario === "stale" ? "stale" : scenario === "unsupported" ? "unsupported" : "fresh";
  let snapshot: ExactStoreSnapshot = {
    scenario: initialScenario,
    state: initialState(initialScenario),
    freshness: freshnessOf(initialScenario),
    canOperate: true,
    nextDownload: "accept",
    nextInstall: "accept",
    calls: [],
  };
  const commit = (next: Partial<ExactStoreSnapshot>) => {
    snapshot = { ...snapshot, ...next };
    for (const listener of listeners) listener();
  };
  const record = (action: string, input: unknown) =>
    commit({ calls: [...snapshot.calls, { sequence: snapshot.calls.length + 1, action, input }] });
  /** Every state the host reports carries a new revision, like the real state endpoint. */
  const publish = (change: (state: JonesUpdateState) => JonesUpdateState) => {
    const state = snapshot.state;
    if (state === null) return;
    commit({ state: { ...change(state), revision: (state.revision ?? 0) + 1 } });
  };
  const withoutStaged = ({ stagedHandle: _handle, ...state }: JonesUpdateState) => state;
  const succeed = () => AsyncResult.success(snapshot.state);
  const lost = (): AtomCommandResult<JonesUpdateState | null, unknown> =>
    AsyncResult.failure(Cause.fail(new Error("Synthetic response lost.")));

  const run = async (
    request: JonesAction,
  ): Promise<AtomCommandResult<JonesUpdateState | null, unknown>> => {
    record(request.action, request.action === "check" ? null : request.input);
    const state = snapshot.state;
    if (state === null) return AsyncResult.success(null);
    if (request.action === "downloadExact" || request.action === "download") {
      if (request.action === "downloadExact" && state.capability.exactSource !== true)
        return AsyncResult.failure(Cause.fail(new Error("Synthetic 404: no exact selection.")));
      const outcome = snapshot.nextDownload;
      commit({ nextDownload: "accept" });
      if (outcome === "lost") return lost();
      if (outcome === "refuse") {
        publish((current) => ({
          ...current,
          phase: "blocked",
          message: "Synthetic refusal: this app's build is not newer than the host's.",
        }));
        return succeed();
      }
      if (outcome === "wrong-source") {
        publish((current) => ({
          ...current,
          phase: "staged",
          stagedHandle: "staged-wrong-source-handle",
          provenance: offeredBuild(),
        }));
        return succeed();
      }
      publish((current) => ({
        ...current,
        phase: "downloading",
        provenance: targetBuild(),
        message: "Synthetic download is in progress.",
      }));
      return succeed();
    }
    if (request.action === "install") {
      if (state.stagedHandle !== request.input.stagedHandle)
        return AsyncResult.failure(
          Cause.fail(new Error("Install did not bind the staged selection.")),
        );
      const outcome = snapshot.nextInstall;
      commit({ nextInstall: "accept" });
      if (outcome === "lost-not-started") return lost();
      if (outcome === "refuse") {
        publish((current) => ({
          ...current,
          phase: "blocked",
          message: "Synthetic install refusal.",
        }));
        return succeed();
      }
      publish((current) => ({
        ...current,
        phase: "installing",
        message: "Synthetic install accepted; waiting for the launcher outcome.",
      }));
      return outcome === "lost-started" ? lost() : succeed();
    }
    return succeed();
  };

  /**
   * The host view for one rendered snapshot. `snapshot` is bound to the view passed in, so a
   * caller that renders from a subscribed snapshot depends on it structurally; `readLatest` and
   * `run` stay live because a request or confirmation outlives the render that started it.
   */
  const hostFor = (view: ExactStoreSnapshot): ExactSourceHost => {
    const read = (from: ExactStoreSnapshot) => ({
      observation: {
        state: from.state,
        freshness: from.freshness,
        ...(from.freshness === "stale"
          ? { message: "Update status is temporarily unavailable; retrying." }
          : {}),
      },
      canOperate: from.canOperate,
    });
    return {
      snapshot: read(view),
      readLatest: () => read(snapshot),
      run,
      // The fixture renders the production confirmation dialog host; no host means no consent.
      confirm: async (message) => (await requestConfirmDialog(message)) ?? false,
    };
  };

  return {
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    read: () => snapshot,
    hostFor,
    reset: (scenario: ExactScenario) =>
      commit({
        scenario,
        state: initialState(scenario),
        freshness: freshnessOf(scenario),
        nextDownload: "accept",
        nextInstall: "accept",
        calls: [],
      }),
    setNext: (kind: "download" | "install", outcome: NextDownload | NextInstall) =>
      commit(
        kind === "download"
          ? { nextDownload: outcome as NextDownload }
          : { nextInstall: outcome as NextInstall },
      ),
    togglePermission: () => commit({ canOperate: !snapshot.canOperate }),
    reportState: () => publish((state) => state),
    completeDownload: () => {
      if (snapshot.state?.phase !== "downloading")
        throw new Error("No synthetic download is pending.");
      publish((state) => ({
        ...state,
        phase: "staged",
        stagedHandle: "staged-target-handle",
        provenance: targetBuild(),
        message: "Synthetic download complete.",
      }));
    },
    settleInstall: (outcome: "committed" | "rolled-back") => {
      if (snapshot.state?.phase !== "installing")
        throw new Error("No synthetic install is pending.");
      publish((state) => ({
        ...withoutStaged(state),
        phase: outcome,
        ...(outcome === "committed"
          ? {
              installedSource: installedSource(APP_SOURCE_SHA ?? ""),
              currentVersion: targetBuild().version!,
            }
          : {}),
        outcome: {
          status: outcome,
          fromVersion: HOST_VERSION,
          targetVersion: targetBuild().version!,
          ...(outcome === "rolled-back" ? { reason: "Synthetic rollback." } : {}),
        },
        message: outcome === "committed" ? "Synthetic install committed." : "Synthetic rollback.",
      }));
    },
  };
}

type SyntheticExactSourceStore = ReturnType<typeof createSyntheticExactStore>;

/**
 * Subscribes to the store itself and builds the host from the snapshot it just read. The notice
 * hook that places this element does not re-render on a store change, so the element must carry
 * its own subscription; keeping one component type in place also keeps the production
 * controller's instance, and its lost-response state, across every host transition.
 */
function SyntheticExactSourceAction({
  store,
  ...target
}: ExactSourceTarget & { readonly store: SyntheticExactSourceStore }) {
  const snapshot = useSyncExternalStore(store.subscribe, store.read, store.read);
  return <ExactSourceUpdateAction {...target} host={store.hostFor(snapshot)} />;
}

/** Subscribes the caller to the store and returns the renderer the notice hook accepts. */
export function useSyntheticExactSource(initialScenario: ExactScenario = "exact") {
  const [store] = useState(() => createSyntheticExactStore(initialScenario));
  const snapshot = useSyncExternalStore(store.subscribe, store.read, store.read);
  return {
    store,
    snapshot,
    renderExactSourceAction: (target: ExactSourceTarget): ReactNode => (
      <SyntheticExactSourceAction {...target} store={store} />
    ),
  };
}

const short = (sha: string | undefined) => sha?.slice(0, 7) ?? "none";

/** Exact-source notice fixture: scripted host scenarios behind the production notice and controller. */
export function ExactSourcePreview({ runId }: { readonly runId: string }) {
  const { store, snapshot, renderExactSourceAction } = useSyntheticExactSource();
  const [failure, setFailure] = useState<string | null>(null);
  const control = (action: () => void) => {
    try {
      action();
      setFailure(null);
    } catch (error) {
      setFailure(error instanceof Error ? error.message : String(error));
    }
  };
  // The notice describes the host's installed source, so it closes when an install commits.
  const hostSource = snapshot.state?.installedSource?.sha ?? INSTALLED_SOURCE;
  const environmentId = EnvironmentId.make(`exact-source-${runId}-Mini`);
  const environment = {
    environmentId,
    label: "Mac Mini",
    serverConfig: {
      environment: {
        environmentId,
        label: "Mac Mini",
        platform: { os: "darwin", arch: "arm64" } as const,
        capabilities: { repositoryIdentity: true },
        serverVersion: APP_VERSION,
        jonesSource: {
          repository: "Jones-Systems/Jones-Code" as const,
          sha: hostSource,
          tree: hostSource,
        },
      },
    },
  };
  const banners = useClientUpdateBanners({
    activeEnvironment: environment,
    automaticEnvironment: false,
    eligibleEnvironments: [environment],
    renderExactSourceAction,
  });
  const state = snapshot.state;
  const buttonClass = "rounded border px-2 py-1 text-xs";
  return (
    <section aria-label="Exact-source action fixture" className="space-y-3 border-t pt-4">
      <h2 className="font-medium">Exact-source action fixture</h2>
      <p className="text-sm text-muted-foreground">
        Production notice and action controller against a scripted synthetic host. Requests are
        answered locally; nothing is downloaded or installed.
      </p>
      <p className="break-all font-mono text-xs" data-testid="exact-app-source">
        App source {APP_SOURCE_SHA ?? "unknown"}; host installed {hostSource}
      </p>
      <output className="block font-mono text-xs" data-testid="exact-host-state">
        Host: {snapshot.scenario}; {snapshot.freshness}; phase {state?.phase ?? "none"}; revision{" "}
        {state?.revision ?? "none"}; installed {short(state?.installedSource?.sha)}; offered/staged{" "}
        {short(state?.provenance?.sourceSha)}; handle {state?.stagedHandle ?? "none"}; operate{" "}
        {snapshot.canOperate ? "yes" : "no"}
      </output>
      <output className="block font-mono text-xs" data-testid="exact-next">
        Next download: {snapshot.nextDownload}; next install: {snapshot.nextInstall}
      </output>
      <div className="flex flex-wrap gap-2">
        {EXACT_SCENARIOS.map((scenario) => (
          <button
            type="button"
            className={buttonClass}
            key={scenario}
            data-testid={`exact-scenario-${scenario}`}
            onClick={() => store.reset(scenario)}
          >
            Host: {scenario}
          </button>
        ))}
        {NEXT_DOWNLOAD.map((outcome) => (
          <button
            type="button"
            className={buttonClass}
            key={`download-${outcome}`}
            data-testid={`exact-next-download-${outcome}`}
            onClick={() => store.setNext("download", outcome)}
          >
            Next download: {outcome}
          </button>
        ))}
        {NEXT_INSTALL.map((outcome) => (
          <button
            type="button"
            className={buttonClass}
            key={`install-${outcome}`}
            data-testid={`exact-next-install-${outcome}`}
            onClick={() => store.setNext("install", outcome)}
          >
            Next install: {outcome}
          </button>
        ))}
        <button
          type="button"
          className={buttonClass}
          data-testid="exact-complete-download"
          onClick={() => control(store.completeDownload)}
        >
          Complete synthetic download
        </button>
        <button
          type="button"
          className={buttonClass}
          data-testid="exact-commit-install"
          onClick={() => control(() => store.settleInstall("committed"))}
        >
          Commit synthetic install
        </button>
        <button
          type="button"
          className={buttonClass}
          data-testid="exact-rollback-install"
          onClick={() => control(() => store.settleInstall("rolled-back"))}
        >
          Roll back synthetic install
        </button>
        <button
          type="button"
          className={buttonClass}
          data-testid="exact-report-state"
          onClick={() => store.reportState()}
        >
          Host reports state (new revision)
        </button>
        <button
          type="button"
          className={buttonClass}
          data-testid="exact-toggle-permission"
          onClick={() => store.togglePermission()}
        >
          Toggle operate permission
        </button>
      </div>
      {failure ? <p role="alert">Fixture control failed: {failure}</p> : null}
      <output className="block" data-testid="exact-notice-count">
        Exact-source notices: {banners.length}
      </output>
      <div data-testid="exact-banners" className="pt-6">
        <ComposerBannerStack items={banners} />
        <div className="rounded-b-lg border p-4 text-muted-foreground">Synthetic composer</div>
      </div>
      <details>
        <summary>Exact-source request receipts</summary>
        <pre className="max-h-72 overflow-auto text-xs" data-testid="exact-receipts">
          {JSON.stringify(snapshot.calls, null, 2)}
        </pre>
      </details>
    </section>
  );
}
