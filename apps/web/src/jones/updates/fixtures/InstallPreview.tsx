import { StrictMode, useEffect, useState, useSyncExternalStore } from "react";
import { createRoot } from "react-dom/client";
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

function FixtureScreen({ fixture }: { readonly fixture: InstallPreviewFixture }) {
  const stored = useSyncExternalStore(fixture.subscribe, fixture.read, fixture.read);
  const [failure, setFailure] = useState<string | null>(null);
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
