import { EllipsisIcon, ChevronRightIcon, GripVerticalIcon, PlusIcon } from "lucide-react";
import { StrictMode, useState } from "react";
import { createRoot } from "react-dom/client";
import { createRootRoute, createRouter, RouterProvider } from "@tanstack/react-router";
import { EnvironmentId, type EnvironmentMachineKind } from "@t3tools/contracts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import { AUTH_SCOPE_OPTIONS } from "@t3tools/shared/authScopeOptions";
import "../../../index.css";
import { EnvironmentRow } from "../../../components/settings/EnvironmentRow";
import { SettingsGroup } from "../../../components/settings/SettingsGroup";
import { Button } from "../../../components/ui/button";
import { cn } from "../../../lib/utils";
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

interface SyntheticRoute {
  readonly label: string;
  readonly address: string;
  readonly inUse?: boolean;
}

interface Scenario {
  readonly key: string;
  readonly label: string;
  readonly kind: EnvironmentMachineKind;
  readonly subtitle: string;
  readonly subtitleTone?: "error";
  /** Synthetic stand-ins for the saved routes and session grants the real row reads. */
  readonly routes: readonly SyntheticRoute[];
  readonly deniedScopes?: readonly string[];
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
      routes: [{ label: "T3 Connect", address: "relay · mac-mini" }],
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
      routes: [
        { label: "T3 Connect", address: "relay · vps", inUse: true },
        { label: "SSH", address: "deploy@vps.example:22" },
      ],
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
      routes: [
        { label: "SSH", address: "builder@build.example", inUse: true },
        { label: "Remote link", address: "http://192.168.1.40:3773 · found automatically" },
      ],
      deniedScopes: [AUTH_SCOPE_OPTIONS[2]!.scope],
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
      routes: [{ label: "T3 Connect", address: "relay · studio", inUse: true }],
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
      routes: [{ label: "Remote link", address: "http://lab.example:3773", inUse: true }],
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
      routes: [{ label: "Remote link", address: "http://older.example:3773", inUse: true }],
      build: { state: null, freshness: "unsupported" },
      fleet: { enrolled: false, readiness: "setup-required" },
    },
  ];
}

const SUBTITLE_TOGGLE_CLASSNAME =
  "inline-flex shrink-0 items-center gap-0.5 rounded-sm text-muted-foreground outline-none hover:text-foreground focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-ring";

/** The row's Routes and Permissions toggles, copied from the saved environment row's markup. */
function SubtitleToggle({
  open,
  onToggle,
  children,
}: {
  readonly open: boolean;
  readonly onToggle: () => void;
  readonly children: string;
}) {
  return (
    <>
      <span aria-hidden className="shrink-0">
        ·
      </span>
      <button
        type="button"
        aria-expanded={open}
        onClick={onToggle}
        className={SUBTITLE_TOGGLE_CLASSNAME}
      >
        {children}
        <ChevronRightIcon
          aria-hidden
          className={cn(
            "size-3 shrink-0 transition-transform duration-150 motion-reduce:transition-none",
            open && "rotate-90",
          )}
        />
      </button>
    </>
  );
}

/**
 * Static copies of the expanded Routes list and session Permissions. The real panels read the
 * connection catalog and session; these keep their layout with inert controls.
 */
function SyntheticRoutes({
  label,
  routes,
}: {
  readonly label: string;
  readonly routes: readonly SyntheticRoute[];
}) {
  return (
    <div data-synthetic="routes" className="mt-2 border-t border-border/50 py-2">
      <ol aria-label={`Synthetic routes to ${label}, preferred first`}>
        {routes.map((route, index) => (
          <li key={route.label} className="flex items-center gap-2 py-2">
            <div className="min-w-0 flex-1">
              <p className="flex items-center gap-1.5 text-xs font-medium text-foreground">
                {route.label}
                {route.inUse ? (
                  <span className="rounded-sm bg-success/12 px-1 text-2xs font-normal text-success-foreground">
                    In use
                  </span>
                ) : null}
              </p>
              <p className="truncate text-2xs text-muted-foreground">{route.address}</p>
            </div>
            <button
              type="button"
              aria-label={`Reorder ${route.label}, position ${index + 1}`}
              className="flex size-6 shrink-0 cursor-grab items-center justify-center rounded text-muted-foreground/70"
            >
              <GripVerticalIcon className="size-3.5" />
            </button>
          </li>
        ))}
      </ol>
      <div className="-ml-1.5 pt-1">
        <Button size="xs" variant="ghost-muted">
          <PlusIcon className="size-3" />
          Add route
        </Button>
      </div>
    </div>
  );
}

function SyntheticPermissions({
  connected,
  deniedScopes,
}: {
  readonly connected: boolean;
  readonly deniedScopes?: readonly string[] | undefined;
}) {
  return (
    <div data-synthetic="permissions" className="mt-2 border-t border-border/50">
      {connected ? (
        <div className="space-y-3 py-3 text-xs">
          <p className="text-muted-foreground">
            These permissions apply to your active route. Other routes may have different
            permissions.
          </p>
          <ul className="grid gap-x-6 gap-y-2 sm:grid-cols-2">
            {AUTH_SCOPE_OPTIONS.map(({ scope, title }) => (
              <li key={scope} className="flex items-start justify-between gap-3">
                <span>{title}</span>
                <span className="shrink-0 text-muted-foreground">
                  {deniedScopes?.includes(scope) ? "Not granted" : "Allowed"}
                </span>
              </li>
            ))}
          </ul>
        </div>
      ) : (
        <p className="py-3 text-xs text-muted-foreground">
          Permissions not checked. Connect to this environment to view this session’s permissions.
        </p>
      )}
    </div>
  );
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
  const [routesOpen, setRoutesOpen] = useState(false);
  const [permissionsOpen, setPermissionsOpen] = useState(false);
  const environmentId = EnvironmentId.make(`fixture-${scenario.key}`);
  const routeCount = scenario.routes.length;
  return (
    <EnvironmentRow
      kind={scenario.kind}
      label={scenario.label}
      subtitle={
        <span className="flex min-w-0 items-center gap-1">
          <span
            className={cn(
              "min-w-0 truncate",
              scenario.subtitleTone === "error" && "text-destructive",
            )}
          >
            {scenario.subtitle}
          </span>
          <SubtitleToggle open={routesOpen} onToggle={() => setRoutesOpen((open) => !open)}>
            {routeCount === 1 ? "Routes" : `${routeCount} routes`}
          </SubtitleToggle>
          <SubtitleToggle
            open={permissionsOpen}
            onToggle={() => setPermissionsOpen((open) => !open)}
          >
            Permissions
          </SubtitleToggle>
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
      detail={
        <>
          {routesOpen ? <SyntheticRoutes label={scenario.label} routes={scenario.routes} /> : null}
          {permissionsOpen ? (
            <SyntheticPermissions
              connected={scenario.subtitleTone !== "error"}
              deniedScopes={scenario.deniedScopes}
            />
          ) : null}
        </>
      }
    >
      <Switch size="sm" checked aria-label={`Switch off ${scenario.label}`} />
      <Button
        size="icon-xs"
        variant="ghost-muted"
        aria-label={`More actions for ${scenario.label}`}
      >
        <EllipsisIcon className="size-3.5" />
      </Button>
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
                request, enrollment, or installation. Routes and Permissions open static copies of
                those panels with synthetic routes and grants; their controls do nothing.
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
                  <div key={scenario.key} data-fixture-row={scenario.key}>
                    <ScenarioRow scenario={scenario} onRequest={record} />
                  </div>
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
