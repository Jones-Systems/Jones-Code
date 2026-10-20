import { StrictMode, useEffect, useState, useSyncExternalStore } from "react";
import { createRoot } from "react-dom/client";
import { EnvironmentId } from "@t3tools/contracts";
import { APP_SOURCE_SHA, APP_VERSION } from "../../../branding";
import { ComposerBannerStack } from "../../../components/chat/ComposerBannerStack";
import { useClientUpdateBanners, type ClientUpdateBannerInput } from "../clientUpdateBanner";
import {
  createMemoryHistory,
  createRootRoute,
  createRouter,
  RouterProvider,
} from "@tanstack/react-router";
import "../../../index.css";
import { isElectron } from "../../../env";
import { AppAtomRegistryProvider } from "../../../rpc/atomRegistry";
import { useDesktopUpdateState } from "../../../state/desktopUpdate";
import { AboutVersionSection } from "../../../components/settings/SettingsPanels";
import { SettingsGroup } from "../../../components/settings/SettingsGroup";
import { SidebarUpdatePill } from "../../../components/sidebar/SidebarUpdatePill";
import { ConfirmDialogHost } from "../../../components/ConfirmDialogHost";
import { ToastProvider, AnchoredToastProvider } from "../../../components/ui/toast";
import { TooltipProvider } from "../../../components/ui/tooltip";
import { ExactSourcePreview, useSyntheticExactSource } from "./exact-source-fixture";
import type { InstallPreviewFixture } from "./install-preview-bridge";

function ObservedUpdate({ fixture }: { readonly fixture: InstallPreviewFixture }) {
  const state = useDesktopUpdateState();
  useEffect(() => fixture.observe(state), [fixture, state]);
  return (
    <output data-testid="observed-update" className="block font-mono text-xs">
      Production atom: {state?.jones?.phase ?? "loading"}; revision{" "}
      {state?.jones?.revision ?? "none"}
    </output>
  );
}

type NoticePair = "different" | "changed" | "equal" | "missing" | "stable";

function NoticeRenderer(input: ClientUpdateBannerInput) {
  const banners = useClientUpdateBanners(input);
  return (
    <>
      <output data-testid="notice-count" className="block">
        Client notices: {banners.length}
      </output>
      <div data-testid="notice-banners" className="pt-6">
        <ComposerBannerStack items={banners} />
        <div className="rounded-b-lg border p-4 text-muted-foreground">Synthetic composer</div>
      </div>
    </>
  );
}

function ClientNoticePreview({ runId }: { readonly runId: string }) {
  const [selected, setSelected] = useState("Laptop");
  const [automatic, setAutomatic] = useState(false);
  const [pair, setPair] = useState<NoticePair>("different");
  const [mount, setMount] = useState(1);
  // Notices here cover selection and dismissal; their actions run against a synthetic host.
  const { renderExactSourceAction } = useSyntheticExactSource();
  // Both synthetic sources differ from the client and share their displayed abbreviation.
  const differentSource = (APP_SOURCE_SHA?.startsWith("a") ? "b" : "a").repeat(39) + "b";
  const source =
    pair === "equal"
      ? APP_SOURCE_SHA
      : pair === "missing"
        ? undefined
        : pair === "changed"
          ? differentSource.slice(0, -1) + "c"
          : differentSource;
  const serverVersion = pair === "stable" ? "0.1.0" : APP_VERSION;
  const environments = ["Laptop", "Remote", "Excluded"].map((label) => {
    const environmentId = EnvironmentId.make(`client-notice-${runId}-${label}`);
    return {
      environmentId,
      label,
      serverConfig: {
        environment: {
          environmentId,
          label,
          platform: { os: "linux", arch: "x64" } as const,
          capabilities: { repositoryIdentity: true },
          serverVersion,
          ...(source
            ? {
                jonesSource: {
                  repository: "Jones-Systems/Jones-Code" as const,
                  sha: source,
                  tree: source,
                },
              }
            : {}),
        },
      },
    };
  });
  return (
    <section aria-label="Client notice renderer fixture" className="space-y-3 border-t pt-4">
      <h2 className="font-medium">Client notice renderer fixture</h2>
      <p className="text-sm text-muted-foreground">
        Synthetic older client version and environment descriptors; exact build source. These inputs
        do not claim an official installed artifact version.
      </p>
      <p className="break-all font-mono text-xs" data-testid="notice-branding">
        Synthetic client {APP_VERSION}; exact source {APP_SOURCE_SHA ?? "unknown"}
      </p>
      <p className="break-all font-mono text-xs" data-testid="notice-server">
        Synthetic server {serverVersion}; source {source ?? "missing"}
      </p>
      <output className="block" data-testid="notice-inputs">
        Selection: {automatic ? "automatic (Laptop, Remote eligible)" : selected}; pair: {pair};
        mount: {mount}
      </output>
      <div className="flex flex-wrap gap-2">
        {["Laptop", "Remote", "Excluded"].map((label) => (
          <button
            type="button"
            className="rounded border px-2 py-1 text-xs"
            key={label}
            data-testid={`notice-select-${label.toLowerCase()}`}
            onClick={() => {
              setSelected(label);
              setAutomatic(false);
            }}
          >
            Select {label}
          </button>
        ))}
        <button
          type="button"
          className="rounded border px-2 py-1 text-xs"
          data-testid="notice-automatic"
          onClick={() => setAutomatic(true)}
        >
          Use automatic eligible environments
        </button>
        {(["different", "changed", "equal", "missing", "stable"] as const).map((value) => (
          <button
            type="button"
            className="rounded border px-2 py-1 text-xs"
            key={value}
            data-testid={`notice-pair-${value}`}
            onClick={() => setPair(value)}
          >
            Pair: {value}
          </button>
        ))}
        <button
          type="button"
          className="rounded border px-2 py-1 text-xs"
          data-testid="notice-remount"
          onClick={() => setMount((previous) => previous + 1)}
        >
          Remount production notice hook and stack
        </button>
      </div>
      <NoticeRenderer
        key={mount}
        activeEnvironment={environments.find((environment) => environment.label === selected)}
        automaticEnvironment={automatic}
        eligibleEnvironments={environments.slice(0, 2)}
        renderExactSourceAction={renderExactSourceAction}
      />
    </section>
  );
}

function FixtureScreen({ fixture }: { readonly fixture: InstallPreviewFixture }) {
  const stored = useSyncExternalStore(fixture.subscribe, fixture.read, fixture.read);
  const [failure, setFailure] = useState<string | null>(null);
  const [showClientNotices, setShowClientNotices] = useState(false);
  const [showExactSource, setShowExactSource] = useState(false);
  const control = (action: () => void) => {
    try {
      action();
      setFailure(null);
    } catch (error) {
      setFailure(error instanceof Error ? error.message : String(error));
    }
  };
  useEffect(() => fixture.mounted(isElectron), [fixture]);
  return (
    <ToastProvider>
      <AnchoredToastProvider>
        <TooltipProvider>
          <ConfirmDialogHost />
          <main className="mx-auto max-w-4xl space-y-6 p-6 text-foreground">
            <header className="space-y-2">
              <h1 className="text-xl font-semibold">Jones install renderer fixture</h1>
              <p className="text-sm text-muted-foreground">
                Real production controls and atoms. Synthetic desktop bridge; no Electron IPC,
                native install, enrollment, or deployment.
              </p>
              <p className="font-mono text-xs">
                Run {stored.runId} · {stored.scenario} · boot {stored.boots}
              </p>
              <ObservedUpdate fixture={fixture} />
            </header>
            <section
              aria-label="Production Settings update controls"
              data-testid="settings-updates"
            >
              <h2 className="mb-2 font-medium">Settings — About</h2>
              <SettingsGroup>
                <AboutVersionSection />
              </SettingsGroup>
            </section>
            <section aria-label="Production sidebar update controls" data-testid="sidebar-updates">
              <h2 className="mb-2 font-medium">Sidebar</h2>
              <ul className="flex w-64 rounded-lg border bg-sidebar p-3">
                <SidebarUpdatePill />
              </ul>
            </section>
            <section aria-label="Synthetic fixture controls" className="space-y-3 border-t pt-4">
              <h2 className="font-medium">Synthetic fixture controls</h2>
              <p className="text-xs text-muted-foreground">
                These controls configure test inputs. Use the production controls above to request
                updates.
              </p>
              <div className="flex flex-wrap gap-2">
                {(["download", "install"] as const).flatMap((action) =>
                  (["reject", "refuse", "accept"] as const).map((outcome) => (
                    <button
                      type="button"
                      className="rounded border px-2 py-1 text-xs"
                      key={`${action}-${outcome}`}
                      data-testid={`next-${action}-${outcome}`}
                      onClick={() => control(() => fixture.setOutcome(action, outcome))}
                    >
                      Next {action}: {outcome}
                    </button>
                  )),
                )}
                <button
                  type="button"
                  className="rounded border px-2 py-1 text-xs"
                  data-testid="complete-download"
                  disabled={stored.state.jones?.phase !== "downloading"}
                  onClick={() => control(fixture.completeDownload)}
                >
                  Complete synthetic download
                </button>
                <button
                  type="button"
                  className="rounded border px-2 py-1 text-xs"
                  data-testid="commit-install"
                  disabled={stored.state.jones?.phase !== "preparing"}
                  onClick={() => control(() => fixture.settleInstall("committed"))}
                >
                  Commit synthetic install
                </button>
                <button
                  type="button"
                  className="rounded border px-2 py-1 text-xs"
                  data-testid="rollback-install"
                  disabled={stored.state.jones?.phase !== "preparing"}
                  onClick={() => control(() => fixture.settleInstall("rolled-back"))}
                >
                  Roll back synthetic install
                </button>
                {stored.scenario === "initial-race" ? (
                  <>
                    <button
                      type="button"
                      className="rounded border px-2 py-1 text-xs"
                      data-testid="publish-race-event"
                      onClick={() => control(fixture.publishRaceEvent)}
                    >
                      Publish newer staged event
                    </button>
                    <button
                      type="button"
                      className="rounded border px-2 py-1 text-xs"
                      data-testid="release-initial-read"
                      onClick={() => control(fixture.releaseInitialRead)}
                    >
                      Release older initial read
                    </button>
                  </>
                ) : null}
                <button
                  type="button"
                  className="rounded border px-2 py-1 text-xs"
                  data-testid="reload-fixture"
                  onClick={() => window.location.reload()}
                >
                  Reload fixture
                </button>
              </div>
              {failure ? <p role="alert">Fixture control failed: {failure}</p> : null}
              <p className="font-mono text-xs" data-testid="fixture-result-plan">
                Next results: download {stored.nextDownload}; install {stored.nextInstall}
              </p>
              <details>
                <summary>Fixture action receipts</summary>
                <pre className="max-h-72 overflow-auto text-xs" data-testid="fixture-receipts">
                  {JSON.stringify(stored.calls, null, 2)}
                </pre>
              </details>
            </section>
            {showClientNotices ? (
              <ClientNoticePreview runId={stored.runId} />
            ) : (
              <button
                type="button"
                className="rounded border px-2 py-1 text-xs"
                data-testid="show-client-notices"
                onClick={() => setShowClientNotices(true)}
              >
                Open client notice fixture
              </button>
            )}
            {showExactSource ? (
              <ExactSourcePreview runId={stored.runId} />
            ) : (
              <button
                type="button"
                className="rounded border px-2 py-1 text-xs"
                data-testid="show-exact-source"
                onClick={() => setShowExactSource(true)}
              >
                Open exact-source action fixture
              </button>
            )}
          </main>
        </TooltipProvider>
      </AnchoredToastProvider>
    </ToastProvider>
  );
}

export async function mountInstallPreview(fixture: InstallPreviewFixture) {
  const routeTree = createRootRoute({ component: () => <FixtureScreen fixture={fixture} /> });
  const router = createRouter({
    routeTree,
    history: createMemoryHistory({ initialEntries: ["/"] }),
  });
  await router.load();
  createRoot(document.getElementById("root")!).render(
    <StrictMode>
      <AppAtomRegistryProvider>
        <RouterProvider router={router} />
      </AppAtomRegistryProvider>
    </StrictMode>,
  );
}
