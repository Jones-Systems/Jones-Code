import {
  compareJonesSource,
  jonesCandidatePresentation,
  jonesUpdatePresentation,
  shortJonesSource,
  shortJonesUpdateId,
  type JonesSourceComparison,
} from "@t3tools/client-runtime/jones/updates";
import type { EnvironmentId } from "@t3tools/contracts";
import { useAtomValue } from "@effect/atom-react";
import { Atom } from "effect/reactivity";
import { type ReactNode, useState } from "react";
import { jonesUpdates } from "./jonesUpdates";
import { isElectron } from "../../env";
import { readPrimaryEnvironmentTarget } from "../../environments/primary/target";
import { primaryEnvironmentIdAtom } from "../../state/primaryEnvironment";
import { environmentSession } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import { useCopyToClipboard } from "../../hooks/useCopyToClipboard";
import { Badge } from "../../components/ui/badge";
import { Button } from "../../components/ui/button";
import { toastManager } from "../../components/ui/toast";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { ITEM_ROW_CLASSNAME, ITEM_ROW_INNER_CLASSNAME } from "../../components/settings/itemRows";

const noLaptopSourceAtom = Atom.make(() => undefined);

const laptopInstalledSourceAtom = Atom.make((get) => {
  if (!isElectron) return undefined;
  const environmentId = get(primaryEnvironmentIdAtom);
  if (environmentId === null) return undefined;
  try {
    const localTarget = readPrimaryEnvironmentTarget();
    if (localTarget?.source !== "desktop-managed") return undefined;
    const prepared = get(environmentSession.preparedConnectionValueAtom(environmentId));
    if (prepared._tag === "None" || prepared.value.target._tag !== "PrimaryConnectionTarget")
      return undefined;
    const target = prepared.value.target;
    if (
      new URL(target.httpBaseUrl).toString() !==
        new URL(localTarget.target.httpBaseUrl).toString() ||
      new URL(target.wsBaseUrl).toString() !== new URL(localTarget.target.wsBaseUrl).toString()
    )
      return undefined;
  } catch {
    return undefined;
  }
  return get(jonesUpdates.value(environmentId))?.installedSource;
});

const LAYOUTS = {
  // A direct row in a grouped settings section, aligned with its SettingsRow siblings.
  row: {
    root: ITEM_ROW_CLASSNAME,
    header: ITEM_ROW_INNER_CLASSNAME,
    summary: "min-w-0 flex-1 space-y-1",
    title: "text-sm font-medium text-foreground",
    message: "max-w-xl text-xs leading-normal text-muted-foreground/80",
    actions: "flex w-full shrink-0 items-center gap-2 sm:w-auto sm:justify-end",
    button: "sm",
  },
  // Nested under another machine's row, or wherever an update action stands in for it.
  card: {
    root: "mt-2 max-w-lg rounded-md border border-border/70 px-2.5 py-2",
    header: "flex flex-wrap items-center justify-between gap-x-3 gap-y-1.5",
    summary: "min-w-0 grow basis-48 space-y-0.5",
    title: "text-xs font-medium text-foreground",
    message: "text-xs text-muted-foreground",
    actions: "flex shrink-0 items-center gap-1.5",
    button: "xs",
  },
} as const;

const COMPARISON_BADGES = {
  matches: {
    label: "Matches laptop",
    variant: "success",
    detail: "Built from the source commit installed on this laptop.",
  },
  different: {
    label: "Different source",
    variant: "warning",
    detail: "Built from a different source commit than the one installed on this laptop.",
  },
  unavailable: {
    label: "Comparison unavailable",
    variant: "outline",
    detail:
      "Needs a verified local desktop connection and the full source commit of both installed and offered builds. At least one is unavailable.",
  },
} as const;

/** Jones main-build status for one host. Download and Install stay separate host actions. */
export function JonesUpdateControls({
  environmentId,
  layout = "card",
  compareWithLaptop = false,
}: {
  readonly environmentId: EnvironmentId;
  readonly layout?: keyof typeof LAYOUTS;
  /** Badges another machine's offered or staged build against this laptop's installed source. */
  readonly compareWithLaptop?: boolean;
}) {
  const state = useAtomValue(jonesUpdates.value(environmentId));
  const laptopInstalledSource = useAtomValue(
    compareWithLaptop ? laptopInstalledSourceAtom : noLaptopSourceAtom,
  );
  const action = useAtomCommand(jonesUpdates.action, { reportFailure: false });
  const [pending, setPending] = useState(false);
  if (state === null) return null;
  const run = async (input: Parameters<typeof action>[0]["input"]) => {
    setPending(true);
    try {
      await action({ environmentId, input });
    } finally {
      setPending(false);
    }
  };
  const presentation = jonesUpdatePresentation(state);
  const candidate = jonesCandidatePresentation(state);
  const busy = pending || presentation.busy;
  const provenance = state.provenance;
  const styles = LAYOUTS[layout];
  const comparison = compareWithLaptop
    ? compareJonesSource(laptopInstalledSource, provenance)
    : null;
  const details = [
    state.migrationPlan === undefined
      ? null
      : `Pending migrations: ${state.migrationPlan.pendingUpstream.length} upstream, ${state.migrationPlan.pendingJones.length} Jones`,
    state.recovery === undefined
      ? null
      : `Recovery ${state.recovery.method} · ${(state.recovery.bytes / 1024 ** 3).toFixed(2)} GiB`,
  ].filter((detail): detail is string => detail !== null);
  return (
    <div className={styles.root}>
      <div className={styles.header}>
        <div className={styles.summary}>
          {layout === "row" ? (
            <h3 className={styles.title}>Jones main builds</h3>
          ) : (
            <p className={styles.title}>Jones main builds</p>
          )}
          <p className={styles.message}>{presentation.message}</p>
        </div>
        <div className={styles.actions}>
          <Button
            size={styles.button}
            variant="outline"
            disabled={busy || !state.capability.check}
            onClick={() => void run({ action: "check" })}
          >
            Check for builds
          </Button>
          {state.stagedHandle !== undefined ? (
            <Button
              size={styles.button}
              disabled={busy || !state.capability.install || state.currentVersion === undefined}
              onClick={() => {
                if (state.stagedHandle === undefined || state.currentVersion === undefined) return;
                void run({
                  action: "install",
                  input: {
                    stagedHandle: state.stagedHandle,
                    environmentId,
                    currentVersion: state.currentVersion,
                  },
                });
              }}
            >
              Install
            </Button>
          ) : provenance !== undefined ? (
            <Button
              size={styles.button}
              disabled={busy || !state.capability.download || state.phase !== "available"}
              onClick={() =>
                void run({
                  action: "download",
                  input: { artifactId: provenance.artifactId, sourceSha: provenance.sourceSha },
                })
              }
            >
              Download
            </Button>
          ) : null}
        </div>
      </div>
      <div className="mt-2 space-y-1 text-xs">
        {state.currentVersion !== undefined || candidate !== undefined ? (
          <dl className="grid grid-cols-[auto_minmax(0,1fr)] items-baseline gap-x-3 gap-y-1">
            {state.currentVersion !== undefined ? (
              <BuildLine label="Installed" version={state.currentVersion}>
                {state.installedSource === undefined ? (
                  <span className="text-muted-foreground/70">source unavailable</span>
                ) : (
                  <CopyableId
                    label="installed source commit"
                    value={state.installedSource.sha}
                    display={shortJonesSource(state.installedSource.sha)}
                  />
                )}
              </BuildLine>
            ) : null}
            {candidate !== undefined ? (
              <BuildLine label={candidate.label} version={candidate.version}>
                <CopyableId
                  label="source commit"
                  value={candidate.sourceSha}
                  display={shortJonesSource(candidate.sourceSha)}
                />
                {comparison !== null ? <ComparisonBadge comparison={comparison} /> : null}
              </BuildLine>
            ) : null}
          </dl>
        ) : null}
        {presentation.outcomeMessage ? <p role="status">{presentation.outcomeMessage}</p> : null}
        {state.updateId !== undefined || details.length > 0 ? (
          <p className="flex flex-wrap items-center gap-x-3 gap-y-1 text-muted-foreground">
            {state.updateId !== undefined ? (
              <span>
                Update{" "}
                <CopyableId
                  label="update ID"
                  value={state.updateId}
                  display={shortJonesUpdateId(state.updateId)}
                />
              </span>
            ) : null}
            {details.map((detail) => (
              <span key={detail}>{detail}</span>
            ))}
          </p>
        ) : null}
        {state.capability.reason === "bootstrap-required" ? (
          <p className="text-muted-foreground">
            This host needs a qualified launcher before installation.
          </p>
        ) : null}
      </div>
    </div>
  );
}

/** One build in the shared Installed/candidate grid, so versions line up across machines. */
function BuildLine({
  label,
  version,
  children,
}: {
  readonly label: string;
  readonly version: string | undefined;
  readonly children: ReactNode;
}) {
  return (
    <>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="flex min-w-0 flex-wrap items-center gap-x-1.5 gap-y-1">
        {version !== undefined ? (
          <span className="wrap-anywhere text-foreground tabular-nums">{version}</span>
        ) : (
          <span className="text-muted-foreground/70">version not reported</span>
        )}
        <span aria-hidden className="text-muted-foreground/70">
          ·
        </span>
        {children}
      </dd>
    </>
  );
}

/** Shows a short form; the full value is in the tooltip and is what a click copies. */
function CopyableId({
  label,
  value,
  display,
}: {
  readonly label: string;
  readonly value: string;
  readonly display: string;
}) {
  const { copyToClipboard } = useCopyToClipboard<{ value: string }>({
    target: label,
    onCopy: (copied) => {
      toastManager.add({ type: "success", title: `Copied ${label}`, description: copied.value });
    },
    onError: (error) => {
      toastManager.add({
        type: "error",
        title: `Could not copy ${label}`,
        description: error.message,
      });
    },
  });
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <button
            type="button"
            aria-label={`Copy ${label} ${value}`}
            className="cursor-pointer rounded-sm font-mono text-foreground outline-none hover:underline focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-ring"
            onClick={() => copyToClipboard(value, { value })}
          />
        }
      >
        {display}
      </TooltipTrigger>
      <TooltipPopup side="top" variant="code">
        {value}
      </TooltipPopup>
    </Tooltip>
  );
}

function ComparisonBadge({ comparison }: { readonly comparison: JonesSourceComparison }) {
  const badge = COMPARISON_BADGES[comparison];
  return (
    <Tooltip>
      <TooltipTrigger render={<Badge variant={badge.variant} size="sm" />}>
        {badge.label}
      </TooltipTrigger>
      <TooltipPopup side="top">{badge.detail}</TooltipPopup>
    </Tooltip>
  );
}
