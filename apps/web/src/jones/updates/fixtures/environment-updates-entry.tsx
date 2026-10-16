import { StrictMode, useState } from "react";
import { createRoot } from "react-dom/client";
import { createRootRoute, createRouter, RouterProvider } from "@tanstack/react-router";
import { EnvironmentId, type EnvironmentMachineKind } from "@t3tools/contracts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import "../../../index.css";
import { EnvironmentRow } from "../../../components/settings/EnvironmentRow";
import { SettingsGroup } from "../../../components/settings/SettingsGroup";
import { Switch } from "../../../components/ui/switch";
import { ToastProvider, AnchoredToastProvider } from "../../../components/ui/toast";
import { TooltipProvider } from "../../../components/ui/tooltip";
import {
  FleetEnrollmentStatus,
  type FleetHostReadiness,
} from "../../fleetUpdates/FleetEnrollmentStatus";
import { JonesBuildStatus, type JonesBuildRequest } from "../JonesBuildStatus";

// Synthetic presentation-only states: no connection, bridge, host request, or installation.
const repository = "Jones-Systems/Jones-Code";
const installedSha = "d994dd4c7a0e8b1f2c3d4e5f60718293a4b5c6d7";
const offeredSha = "0368cbb07390dfed4e66c0f8067a36e5b0ff3ad1";
const base = {
  source: "jones-actions",
  channel: "jones-main",
  currentVersion: "0.0.45-preview.20261013.38343490676.1",
  installedSource: { repository, sha: installedSha, tree: "e".repeat(40) },
  capability: { check: true, download: true, install: true },
} as const;
const offered = {
  repository,
  sourceSha: offeredSha,
  artifactId: 4242,
  version: "0.0.45-preview.20261016.38974945620.1",
};
const lastInstall = {
  status: "committed",
  fromVersion: "0.0.45-preview.20261011.38178738344.1",
  targetVersion: "0.0.45-preview.20261013.38343490676.1",
} as const;
const state = (environmentId: EnvironmentId, fields: object) =>
  ({ ...base, environmentId, ...fields }) as unknown as JonesUpdateState;

interface Scenario {
  readonly key: string;
  readonly label: string;
  readonly kind: EnvironmentMachineKind;
  readonly subtitle: string;
  readonly subtitleTone?: "error";
  readonly build?: {
    readonly state: JonesUpdateState | null;
    readonly freshness: "loading" | "fresh" | "stale" | "unsupported";
    readonly freshnessMessage?: string;
    readonly comparison?: "matches" | "different" | "unavailable";
    readonly actionError?: string;
  };
  readonly fleet: {
    readonly enrolled: boolean;
    readonly readiness: FleetHostReadiness;
    readonly campaign?: { laptopPhase: string; hostPhase: string; reason?: string };
    readonly notice?: string;
  };
}

function scenarios(): readonly Scenario[] {
  const id = (key: string) => EnvironmentId.make(`fixture-${key}`);
  return [
    {
      key: "reconnecting",
      label: "MJones’s Mac mini",
      kind: "server",
      subtitle:
        "T3 Connect · Reconnecting: Relay could not reach the environment endpoint (endpoint_request_failed)",
      subtitleTone: "error",
      build: {
        state: null,
        freshness: "stale",
        freshnessMessage: "Update status is waiting for a connection.",
      },
      fleet: { enrolled: false, readiness: "unavailable" },
    },
    {
      key: "downloading",
      label: "VPS",
      kind: "cloud",
      subtitle: "via T3 Connect · Connected",
      build: {
        state: state(id("downloading"), {
          phase: "downloading",
          message: "Downloading the selected main build…",
          provenance: offered,
          outcome: lastInstall,
          updateId: "7bd079aa5c1e4f0a9b8c7d6e5f4a3b2c1d0e9f8a",
          migrationPlan: { pendingUpstream: [], pendingJones: [] },
          recovery: { method: "copy", bytes: 54.6 * 1024 ** 3 },
        }),
        freshness: "fresh",
        comparison: "different",
      },
      fleet: { enrolled: false, readiness: "setup-required" },
    },
    {
      key: "available",
      label: "Build server",
      kind: "linux",
      subtitle: "SSH builder@build.example · Connected",
      build: {
        state: state(id("available"), {
          phase: "available",
          message: "A newer main build is available.",
          provenance: offered,
        }),
        freshness: "fresh",
        comparison: "matches",
      },
      fleet: { enrolled: false, readiness: "qualified" },
    },
    {
      key: "staged",
      label: "Studio",
      kind: "server",
      subtitle: "via T3 Connect · Connected",
      build: {
        state: state(id("staged"), {
          phase: "staged",
          message: "Downloaded and verified. Ready to install.",
          provenance: offered,
          stagedHandle: "staged-fixture",
          capability: { check: true, download: true, install: false, reason: "bootstrap-required" },
        }),
        freshness: "fresh",
        comparison: "unavailable",
      },
      fleet: {
        enrolled: true,
        readiness: "qualified",
        campaign: { laptopPhase: "committed", hostPhase: "waiting", reason: "host offline" },
      },
    },
    {
      key: "rolled-back",
      label: "Lab host",
      kind: "server",
      subtitle: "Remote link · Connected",
      build: {
        state: state(id("rolled-back"), {
          phase: "rolled-back",
          message: "The update was rolled back.",
          outcome: {
            status: "rolled-back",
            fromVersion: base.currentVersion,
            targetVersion: offered.version,
            reason: "health check failed",
          },
        }),
        freshness: "stale",
        freshnessMessage: "Update status is temporarily unavailable; retrying.",
        actionError: "The update request failed. Check the connection and try again.",
      },
      fleet: {
        enrolled: true,
        readiness: "unavailable",
        notice:
          "The host didn’t confirm. Automatic dispatch from this laptop is stopped if its local save succeeded; refresh before retrying.",
      },
    },
    {
      key: "unsupported",
      label: "Older host",
      kind: "server",
      subtitle: "Remote link · Connected",
      build: { state: null, freshness: "unsupported" },
      fleet: { enrolled: false, readiness: "setup-required" },
    },
  ];
}

function ScenarioRow({
  scenario,
  onRequest,
}: {
  readonly scenario: Scenario;
  readonly onRequest: (label: string, request: unknown) => void;
}) {
  const [enrolled, setEnrolled] = useState(scenario.fleet.enrolled);
  const [continueThreads, setContinueThreads] = useState(false);
  const environmentId = EnvironmentId.make(`fixture-${scenario.key}`);
  return (
    <EnvironmentRow
      kind={scenario.kind}
      label={scenario.label}
      subtitle={
        <span className={scenario.subtitleTone === "error" ? "text-destructive" : undefined}>
          {scenario.subtitle}
        </span>
      }
      below={
        <>
          {scenario.build ? (
            <JonesBuildStatus
              environmentId={environmentId}
              state={scenario.build.state}
              freshness={scenario.build.freshness}
              freshnessMessage={scenario.build.freshnessMessage}
              comparison={scenario.build.comparison ?? null}
              actionError={scenario.build.actionError ?? null}
              onRequest={(request: JonesBuildRequest) => onRequest(scenario.label, request)}
            />
          ) : null}
          <FleetEnrollmentStatus
            label={scenario.label}
            enrolled={enrolled}
            readiness={scenario.fleet.readiness}
            campaign={scenario.fleet.campaign}
            notice={scenario.fleet.notice}
            continueThreads={continueThreads}
            busy={false}
            onContinueThreadsChange={setContinueThreads}
            onToggle={() => {
              onRequest(scenario.label, {
                fleet: enrolled ? "turn-off" : "turn-on",
                continueThreads,
              });
              setEnrolled(!enrolled);
            }}
            onRefresh={() => onRequest(scenario.label, { fleet: "refresh" })}
          />
        </>
      }
    >
      <Switch size="sm" checked aria-label={`Switch off ${scenario.label}`} />
    </EnvironmentRow>
  );
}

function FixtureScreen() {
  const [receipts, setReceipts] = useState<readonly { id: number; text: string }[]>([]);
  const record = (label: string, request: unknown) =>
    setReceipts((previous) =>
      [
        { id: (previous[0]?.id ?? 0) + 1, text: `${label}: ${JSON.stringify(request)}` },
        ...previous,
      ].slice(0, 20),
    );
  const local = EnvironmentId.make("fixture-local");
  return (
    <ToastProvider>
      <AnchoredToastProvider>
        <TooltipProvider>
          <main className="mx-auto max-w-4xl space-y-6 p-6 text-foreground">
            <header className="space-y-1">
              <h1 className="text-xl font-semibold">Jones environment updates fixture</h1>
              <p className="text-sm text-muted-foreground">
                Production update views with synthetic states. No connection, desktop bridge, host
                request, enrollment, or installation.
              </p>
            </header>
            <section aria-label="This machine">
              <h2 className="mb-2 text-sm text-muted-foreground">This machine</h2>
              <SettingsGroup>
                <JonesBuildStatus
                  environmentId={local}
                  layout="row"
                  state={state(local, {
                    phase: "no-new",
                    message: "This is the newest main build.",
                    outcome: lastInstall,
                  })}
                  freshness="fresh"
                  onRequest={(request) => record("This machine", request)}
                />
              </SettingsGroup>
            </section>
            <section aria-label="Environments">
              <h2 className="mb-2 text-sm text-muted-foreground">Environments</h2>
              <SettingsGroup>
                {scenarios().map((scenario) => (
                  <ScenarioRow key={scenario.key} scenario={scenario} onRequest={record} />
                ))}
              </SettingsGroup>
            </section>
            <section aria-label="Fixture receipts" className="border-t pt-4">
              <h2 className="text-sm font-medium">Requested actions</h2>
              <ol data-testid="fixture-receipts" className="mt-2 space-y-1 font-mono text-xs">
                {receipts.length === 0 ? <li>None</li> : null}
                {receipts.map((receipt) => (
                  <li key={receipt.id}>{receipt.text}</li>
                ))}
              </ol>
            </section>
          </main>
        </TooltipProvider>
      </AnchoredToastProvider>
    </ToastProvider>
  );
}

if (
  !import.meta.env.DEV ||
  !["127.0.0.1", "localhost", "[::1]"].includes(window.location.hostname) ||
  window.desktopBridge !== undefined
) {
  document.getElementById("root")!.textContent =
    "This synthetic fixture runs only from a loopback development server in a browser.";
} else {
  const router = createRouter({ routeTree: createRootRoute({ component: FixtureScreen }) });
  createRoot(document.getElementById("root")!).render(
    <StrictMode>
      <RouterProvider router={router} />
    </StrictMode>,
  );
}
