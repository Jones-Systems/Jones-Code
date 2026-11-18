import {
  AuthOrchestrationReadScope,
  AuthSourceControlWriteScope,
  type ScopedProjectRef,
} from "@t3tools/contracts";
import type { RepoFreshnessStatus } from "@t3tools/contracts/jones/repoFreshness";
import {
  createRepoFreshnessAtoms,
  manualRepoFreshnessVerification,
  repoFreshnessTarget,
  supportsRepoFreshness,
} from "@t3tools/client-runtime/jones/repo-freshness";
import { isAtomCommandInterrupted } from "@t3tools/client-runtime/state/runtime";
import { ChevronDownIcon, DownloadIcon } from "lucide-react";
import { useRef, useState, useSyncExternalStore } from "react";

import { ThreadDetailsControl } from "../../components/chat/ThreadDetailsControl";
import {
  THREAD_DETAILS_PANEL_ICON_CLASS,
  THREAD_DETAILS_PANEL_LABEL_CLASS,
} from "../../components/chat/threadDetailsPanelStyles";
import { Menu, MenuItem, MenuItemLabel, MenuPopup, MenuTrigger } from "../../components/ui/menu";
import { toastManager } from "../../components/ui/toast";
import { connectionAtomRuntime } from "../../connection/runtime";
import { useDeadlinePassed } from "../../hooks/useDeadline";
import { useLiveRefresh } from "../../hooks/useLiveRefresh";
import { useRelativeTimeState } from "../../hooks/useRelativeTimeState";
import { getDocumentActivity, subscribeDocumentActivity } from "../../lib/documentActivity";
import { useEnvironment } from "../../state/environments";
import { useEnvironmentQuery, useEnvironmentQueryRefresh } from "../../state/query";
import { useEnvironmentScope } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";

const repoFreshness = createRepoFreshnessAtoms(connectionAtomRuntime);

const checkoutSkipMessages = new Map([
  ["auto-pull-disabled", "Automatically pull is off for this checkout."],
  ["wrong-branch", "This checkout is on a different branch."],
  ["upstream-mismatch", "The checkout tracks a different upstream."],
  ["dirty-checkout", "The checkout has uncommitted changes."],
  ["local-commits", "The checkout contains commits outside the remote default branch."],
  ["update-unverified", "The checkout update could not be verified."],
]);

function useFetchLatest(project: ScopedProjectRef, canFetch: boolean) {
  const refresh = useAtomCommand(repoFreshness.refresh, { reportFailure: false });
  const [pending, setPending] = useState(false);
  const fetching = useRef(false);
  const fetchLatest = async () => {
    if (fetching.current || !canFetch || !getDocumentActivity().visible) return;
    fetching.current = true;
    setPending(true);
    try {
      const result = await refresh(repoFreshnessTarget(project));
      if (result._tag === "Failure") {
        if (!isAtomCommandInterrupted(result)) {
          toastManager.add({ type: "error", title: "Fetch result unavailable" });
        }
        return;
      }
      const verification = manualRepoFreshnessVerification(result.value);
      if (verification === null) {
        toastManager.add({
          type: "error",
          title:
            result.value.status.target.status === "unavailable"
              ? "Repository remote unavailable"
              : "Latest fetch failed",
        });
        return;
      }
      const checkout = result.value.checkout;
      const checkoutLabel =
        checkout.status === "updated"
          ? "Checkout updated."
          : checkout.status === "already-current"
            ? "Checkout already current."
            : (checkoutSkipMessages.get(checkout.reason ?? "") ?? "Checkout update skipped.");
      toastManager.add({
        type: "success",
        title: "Fetched latest remote default",
        description: `${verification.remoteName}/${verification.branchName} at ${verification.commitSha.slice(0, 12)}. ${checkoutLabel}`,
      });
    } finally {
      fetching.current = false;
      setPending(false);
    }
  };
  return { fetchLatest, pending };
}

function FreshnessEvidence({
  status,
  receivedAtMs,
  cadence,
}: {
  status: RepoFreshnessStatus;
  receivedAtMs: number;
  cadence: RepoFreshnessStatus["cadenceSeconds"];
}) {
  const verified = status.latestVerified;
  // Age the server's observation from receipt on this client, even when their clocks differ.
  const verifiedAtMs =
    verified === null
      ? null
      : receivedAtMs - Math.max(0, status.sampledAtMs - verified.checkedAtMs);
  const age = useRelativeTimeState(
    verifiedAtMs === null ? null : new Date(verifiedAtMs).toISOString(),
  );
  const deadlinePassed = useDeadlinePassed(
    verifiedAtMs === null || cadence === 0 ? null : verifiedAtMs + 2.5 * cadence * 1_000,
  );
  const stale =
    deadlinePassed || (status.cadenceSeconds === cadence && status.freshness === "stale");
  return (
    <>
      {status.target.status === "unavailable" ? <p>Repository remote unavailable</p> : null}
      {status.latestAttempt?.status === "failed" ? (
        <p className="text-warning">Latest fetch failed</p>
      ) : null}
      {age.status === "relative" ? (
        <p>
          Last verified {age.value}
          {age.suffix === null ? "" : ` ${age.suffix}`}
          {stale ? <span className="text-warning"> · Stale</span> : null}
        </p>
      ) : (
        <p>No verified fetch yet</p>
      )}
    </>
  );
}

function RepoFreshnessMenuContents({
  project,
  canRead,
  canFetch,
  fetchLatest,
  pending,
  cadence,
}: {
  project: ScopedProjectRef;
  canRead: boolean;
  canFetch: boolean;
  fetchLatest: () => Promise<void>;
  pending: boolean;
  cadence: RepoFreshnessStatus["cadenceSeconds"];
}) {
  const target = repoFreshnessTarget(project);
  const { visible } = useSyncExternalStore(
    subscribeDocumentActivity,
    getDocumentActivity,
    getDocumentActivity,
  );
  const statusAtom = canRead && visible ? repoFreshness.status(target) : null;
  const query = useEnvironmentQuery(statusAtom);
  const statusRefresh = useEnvironmentQueryRefresh(statusAtom);
  useLiveRefresh(
    () => {
      if (!statusRefresh.isPending()) statusRefresh.refresh();
    },
    {
      enabled: statusAtom !== null && cadence !== 0,
      key: JSON.stringify(["repo-freshness", project.environmentId, project.projectId]),
      minimumIntervalMs: 60_000,
      intervalMs: cadence === 120 ? 60_000 : 120_000,
    },
  );

  return (
    <>
      <MenuItem
        closeOnClick={false}
        disabled={!canFetch || pending}
        onClick={() => void fetchLatest()}
      >
        <DownloadIcon />
        <MenuItemLabel>{pending ? "Fetching latest..." : "Fetch latest now"}</MenuItemLabel>
      </MenuItem>
      <div role="status" className="px-2 py-1.5 text-xs text-muted-foreground">
        {query.data !== null ? (
          <FreshnessEvidence
            status={query.data}
            receivedAtMs={query.dataUpdatedAt}
            cadence={cadence}
          />
        ) : query.isPending ? (
          <p>Checking freshness status...</p>
        ) : null}
        {!canRead || query.error !== null ? <p>Freshness status unavailable</p> : null}
      </div>
    </>
  );
}

export function RepoFreshnessControl({ project }: { project: ScopedProjectRef }) {
  const environment = useEnvironment(project.environmentId);
  const config = environment?.serverConfig;
  const supported = supportsRepoFreshness(config?.environment);
  const canRead = useEnvironmentScope(
    supported ? project.environmentId : null,
    AuthOrchestrationReadScope,
  );
  const canFetch = useEnvironmentScope(
    supported ? project.environmentId : null,
    AuthSourceControlWriteScope,
  );
  const [open, setOpen] = useState(false);
  const connected = environment?.connection.phase === "connected";
  const { fetchLatest, pending } = useFetchLatest(project, connected && canFetch);
  if (!supported || (!canRead && !canFetch)) return null;

  return (
    <Menu open={open} onOpenChange={setOpen}>
      <MenuTrigger render={<ThreadDetailsControl size="xs" variant="ghost" panel />}>
        <DownloadIcon aria-hidden className={THREAD_DETAILS_PANEL_ICON_CLASS} />
        <span className={THREAD_DETAILS_PANEL_LABEL_CLASS}>Repository freshness</span>
        <ChevronDownIcon aria-hidden className="ms-auto text-muted-foreground" />
      </MenuTrigger>
      <MenuPopup align="start">
        {open ? (
          <RepoFreshnessMenuContents
            project={project}
            canRead={connected && canRead}
            canFetch={connected && canFetch}
            fetchLatest={fetchLatest}
            pending={pending}
            cadence={
              config?.settings.projectSettingsOverrides[project.projectId]
                ?.repoFreshnessCadenceSeconds ?? 0
            }
          />
        ) : null}
      </MenuPopup>
    </Menu>
  );
}
