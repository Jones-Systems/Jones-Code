import {
  DesktopUpdateActionResultSchema,
  DesktopUpdateStateSchema,
  type DesktopBridge,
  type DesktopUpdateActionResult,
  type DesktopUpdateState,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

const SCENARIOS = ["available", "staged", "bootstrap-required", "initial-race"] as const;
const OUTCOMES = ["accept", "reject", "refuse"] as const;
export type FixtureOutcome = (typeof OUTCOMES)[number];
const SOURCE = "a".repeat(40);
const INSTALLED_SOURCE = "b".repeat(40);
const STAGED_HANDLE = "c".repeat(64);
const UPDATE_ID = "d".repeat(64);
const ARTIFACT_ID = 9001;
const CURRENT_VERSION = "0.0.45-preview.20261011.1.1";
const TARGET_VERSION = "0.0.45-preview.20261011.2.1";
const decodeState = Schema.decodeUnknownSync(DesktopUpdateStateSchema);
const decodeResult = Schema.decodeUnknownSync(DesktopUpdateActionResultSchema);
const StoredFixture = Schema.Struct({
  schema: Schema.Literal(1),
  runId: Schema.String,
  sourceSha: Schema.String,
  scenario: Schema.Literals(SCENARIOS),
  boots: Schema.Number,
  state: DesktopUpdateStateSchema,
  nextDownload: Schema.Literals(OUTCOMES),
  nextInstall: Schema.Literals(OUTCOMES),
  calls: Schema.Array(
    Schema.Struct({ sequence: Schema.Number, action: Schema.String, input: Schema.Unknown }),
  ),
});
type StoredFixture = typeof StoredFixture.Type;

function initialState(staged: boolean, blocked = false): DesktopUpdateState {
  return decodeState({
    enabled: true,
    status: staged ? "downloaded" : "available",
    channel: "latest",
    currentVersion: CURRENT_VERSION,
    hostArch: "arm64",
    appArch: "arm64",
    runningUnderArm64Translation: false,
    availableVersion: TARGET_VERSION,
    downloadedVersion: staged ? TARGET_VERSION : null,
    releaseNotes: [],
    omittedReleaseCount: 0,
    downloadPercent: staged ? 100 : null,
    checkedAt: "2026-10-11T12:00:00.000Z",
    message: blocked ? "Synthetic host needs qualified launcher setup." : null,
    errorContext: null,
    canRetry: true,
    jones: {
      source: "jones-actions",
      channel: "jones-main",
      phase: blocked ? "blocked" : staged ? "staged" : "available",
      revision: 1,
      capability: {
        check: true,
        download: true,
        install: !blocked,
        ...(blocked ? { reason: "bootstrap-required" } : {}),
      },
      installedSource: {
        repository: "Jones-Systems/Jones-Code",
        sha: INSTALLED_SOURCE,
        tree: "e".repeat(40),
      },
      provenance: {
        repository: "Jones-Systems/Jones-Code",
        sourceSha: SOURCE,
        sourceTree: "f".repeat(40),
        workflow: ".github/workflows/artifact-desktop.yml",
        runId: 2,
        runAttempt: 1,
        artifactId: ARTIFACT_ID,
        artifactDigest: "1".repeat(64),
        platform: "darwin",
        architecture: "arm64",
        version: TARGET_VERSION,
      },
      ...(staged ? { stagedHandle: STAGED_HANDLE } : {}),
      currentVersion: CURRENT_VERSION,
      ...(blocked ? { message: "Synthetic host needs qualified launcher setup." } : {}),
    },
  });
}

export function installPreviewBridge(runId: string, scenario: string, sourceSha: string) {
  if (!/^[a-zA-Z0-9_-]{1,80}$/.test(runId))
    throw new Error("A bounded fixture run ID is required.");
  const selectedScenario = Schema.decodeUnknownSync(Schema.Literals(SCENARIOS))(scenario);
  const key = `jones:install-preview:v1:${runId}:${selectedScenario}`;
  const saved = sessionStorage.getItem(key);
  let stored: StoredFixture = saved
    ? Schema.decodeUnknownSync(StoredFixture)(JSON.parse(saved))
    : {
        schema: 1,
        runId,
        sourceSha,
        scenario: selectedScenario,
        boots: 0,
        state: initialState(
          selectedScenario === "staged" || selectedScenario === "bootstrap-required",
          selectedScenario === "bootstrap-required",
        ),
        nextDownload: "accept",
        nextInstall: "accept",
        calls: [],
      };
  if (
    stored.runId !== runId ||
    stored.sourceSha !== sourceSha ||
    stored.scenario !== selectedScenario
  )
    throw new Error("Saved fixture state belongs to a different run, scenario or source build.");
  stored = { ...stored, boots: stored.boots + 1 };
  const subscribers = new Set<() => void>();
  const updateListeners = new Set<(state: DesktopUpdateState) => void>();
  let releaseInitial: (() => void) | undefined;
  let initialReadPending = false;
  let mounted = false;
  let observed: DesktopUpdateState | null = null;
  const persist = () => {
    sessionStorage.setItem(key, JSON.stringify(stored));
    for (const listener of subscribers) listener();
  };
  const record = (action: string, input: unknown = null) => {
    if (stored.calls.length >= 1000) throw new Error("Fixture action receipt bound exceeded.");
    stored = {
      ...stored,
      calls: [...stored.calls, { sequence: stored.calls.length + 1, action, input }],
    };
    persist();
  };
  const publish = (state: DesktopUpdateState) => {
    stored = {
      ...stored,
      state: decodeState({
        ...state,
        jones: { ...state.jones, revision: (stored.state.jones?.revision ?? 0) + 1 },
      }),
    };
    record("state-event", {
      phase: stored.state.jones?.phase,
      revision: stored.state.jones?.revision,
    });
    for (const listener of updateListeners) listener(structuredClone(stored.state));
  };
  const result = (
    action: string,
    accepted: boolean,
    completed: boolean,
  ): DesktopUpdateActionResult => {
    const value = decodeResult({ accepted, completed, state: stored.state });
    record(`${action}-result`, { accepted, completed, phase: value.state.jones?.phase });
    return structuredClone(value);
  };
  const rejectOrRefuse = (action: "download" | "install", outcome: FixtureOutcome) => {
    const message = `Synthetic ${action} ${outcome === "reject" ? "transport failure" : "refusal"}.`;
    const state = stored.state;
    publish({
      ...state,
      status: "error",
      errorContext: action,
      message,
      canRetry: true,
      jones: { ...state.jones!, phase: "error", message },
    });
    if (outcome === "reject") {
      record(`${action}-rejected`, { message });
      throw new Error(message);
    }
    return result(action, false, false);
  };
  const bridge = {
    getAppBranding: () => ({
      baseName: "Jones Code",
      stageLabel: "Dev" as const,
      displayName: "Jones Code Fixture",
    }),
    getClientPlatform: () => "darwin",
    getLocalEnvironmentEnabled: () => false,
    getLocalEnvironmentBootstraps: () => [],
    getLocalEnvironmentBearerToken: async (): Promise<string> => {
      throw new Error("The renderer fixture has no primary credential.");
    },
    getClientSettings: async () => null,
    setClientSettings: async () => {
      throw new Error("Settings writes are outside the renderer fixture.");
    },
    getConnectionCatalog: async () => null,
    setConnectionCatalog: async () => {
      throw new Error("Connection writes are outside the renderer fixture.");
    },
    setTheme: async (theme) => {
      record("setTheme", { theme });
    },
    openExternal: async (url: string) => {
      record("external-link-refused", { url });
      return false;
    },
    getUpdateState: async () => {
      const initial = structuredClone(stored.state);
      record("getUpdateState");
      if (selectedScenario === "initial-race" && stored.boots === 1) {
        initialReadPending = true;
        record("initial-read-held");
        await new Promise<void>((resolve) => {
          releaseInitial = resolve;
        });
      }
      return initial;
    },
    onUpdateState: (listener: (state: DesktopUpdateState) => void) => {
      updateListeners.add(listener);
      return () => {
        updateListeners.delete(listener);
      };
    },
    setUpdateChannel: async () => {
      throw new Error("Jones fixture update track is fixed.");
    },
    checkForUpdate: async () => {
      record("checkForUpdate");
      publish(initialState(false));
      return { checked: true, state: structuredClone(stored.state) };
    },
    downloadUpdate: async (selection) => {
      record("downloadUpdate", selection);
      if (selection?.artifactId !== ARTIFACT_ID || selection.sourceSha !== SOURCE)
        throw new Error("Download did not bind the displayed artifact and source.");
      const outcome = stored.nextDownload;
      stored = { ...stored, nextDownload: "accept" };
      if (outcome !== "accept") return rejectOrRefuse("download", outcome);
      publish({
        ...initialState(false),
        status: "downloading",
        downloadPercent: 25,
        jones: {
          ...initialState(false).jones!,
          phase: "downloading",
          message: "Synthetic download is in progress.",
        },
      });
      return result("download", true, false);
    },
    installUpdate: async (stagedHandle, campaignId) => {
      record("installUpdate", { stagedHandle, campaignId: campaignId ?? null });
      if (
        stagedHandle !== STAGED_HANDLE ||
        stored.state.jones?.stagedHandle !== STAGED_HANDLE ||
        campaignId !== undefined
      )
        throw new Error(
          "Install did not bind the displayed staged selection without a fleet campaign.",
        );
      const outcome = stored.nextInstall;
      stored = { ...stored, nextInstall: "accept" };
      if (outcome !== "accept") return rejectOrRefuse("install", outcome);
      publish({
        ...stored.state,
        status: "downloaded",
        errorContext: null,
        message: "Synthetic installation accepted; waiting for restart outcome.",
        jones: {
          ...stored.state.jones!,
          phase: "preparing",
          updateId: UPDATE_ID,
          message: "Synthetic installation accepted; waiting for restart outcome.",
        },
      });
      return result("install", true, false);
    },
    discardUpdate: async (stagedHandle) => {
      record("discardUpdate", { stagedHandle });
      if (stagedHandle !== STAGED_HANDLE || stored.state.jones?.stagedHandle !== STAGED_HANDLE)
        throw new Error("Discard did not bind the displayed staged selection.");
      publish(initialState(false));
      return result("discard", true, true);
    },
    fleetUpdates: async (input) => {
      record("fleetUpdates", input);
      if (input.action !== "read") throw new Error("The fixture has no enrolled fleet members.");
      return { schema: 1 as const, enrollments: [], campaigns: [] };
    },
  } satisfies Pick<
    DesktopBridge,
    | "getAppBranding"
    | "getClientPlatform"
    | "getLocalEnvironmentEnabled"
    | "getLocalEnvironmentBootstraps"
    | "getLocalEnvironmentBearerToken"
    | "getClientSettings"
    | "setClientSettings"
    | "getConnectionCatalog"
    | "setConnectionCatalog"
    | "setTheme"
    | "openExternal"
    | "getUpdateState"
    | "onUpdateState"
    | "setUpdateChannel"
    | "checkForUpdate"
    | "downloadUpdate"
    | "installUpdate"
    | "discardUpdate"
    | "fleetUpdates"
  >;
  Object.defineProperty(window, "desktopBridge", {
    value: Object.freeze(bridge),
    configurable: false,
  });
  const snapshot = () =>
    structuredClone({
      ...stored,
      mounted,
      observed,
      initialReadPending,
      listenerCount: updateListeners.size,
    });
  Object.defineProperty(window, "__jonesInstallPreview", {
    value: Object.freeze({ snapshot }),
    configurable: false,
  });
  persist();
  return {
    subscribe: (listener: () => void) => {
      subscribers.add(listener);
      return () => {
        subscribers.delete(listener);
      };
    },
    read: () => stored,
    observe: (state: DesktopUpdateState | null) => {
      observed = state === null ? null : decodeState(state);
    },
    mounted: (isElectron: boolean) => {
      if (!isElectron)
        throw new Error("Production renderer evaluated before the synthetic bridge.");
      mounted = true;
      record("renderer-mounted", { isElectron });
    },
    setOutcome: (action: "download" | "install", outcome: FixtureOutcome) => {
      stored = {
        ...stored,
        ...(action === "download" ? { nextDownload: outcome } : { nextInstall: outcome }),
      };
      record("configure-next-result", { action, outcome });
    },
    completeDownload: () => {
      if (stored.state.jones?.phase !== "downloading")
        throw new Error("No synthetic download is pending.");
      publish(initialState(true));
    },
    publishRaceEvent: () => {
      if (!initialReadPending) throw new Error("No initial state read is held.");
      publish(initialState(true));
    },
    releaseInitialRead: () => {
      if (!releaseInitial) throw new Error("No initial state read is held.");
      const release = releaseInitial;
      releaseInitial = undefined;
      initialReadPending = false;
      record("initial-read-released");
      release();
    },
    settleInstall: (outcome: "committed" | "rolled-back") => {
      if (stored.state.jones?.phase !== "preparing")
        throw new Error("No synthetic installation is pending.");
      const state = initialState(false);
      publish({
        ...state,
        status: "up-to-date",
        availableVersion: null,
        currentVersion: outcome === "committed" ? TARGET_VERSION : CURRENT_VERSION,
        jones: {
          ...state.jones!,
          phase: outcome,
          updateId: UPDATE_ID,
          currentVersion: outcome === "committed" ? TARGET_VERSION : CURRENT_VERSION,
          installedSource: {
            repository: "Jones-Systems/Jones-Code",
            sha: outcome === "committed" ? SOURCE : INSTALLED_SOURCE,
            tree: (outcome === "committed" ? "f" : "e").repeat(40),
          },
          outcome: { status: outcome, fromVersion: CURRENT_VERSION, targetVersion: TARGET_VERSION },
          message:
            outcome === "committed"
              ? "Synthetic update committed."
              : "Synthetic update rolled back.",
        },
      });
    },
  };
}

export type InstallPreviewFixture = ReturnType<typeof installPreviewBridge>;
