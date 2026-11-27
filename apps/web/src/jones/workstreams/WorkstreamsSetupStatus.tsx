import { useAtomValue } from "@effect/atom-react";
import type {
  PersonalWorkstreamsController,
  PersonalWorkstreamsState,
  PersonalResult,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { AuthAccessWriteScope, type EnvironmentId } from "@t3tools/contracts";
import { useState } from "react";
import { SettingsRow, SettingsSection } from "../../components/settings/settingsLayout";
import { Button } from "../../components/ui/button";
import { useEnvironments } from "../../state/environments";
import { peerLinkEnvironment } from "../../state/peerLinks";
import { localEnvironmentIdAtom } from "../../state/primaryEnvironment";
import { useEnvironmentQuery } from "../../state/query";
import { useEnvironmentsWithScope } from "../../state/session";
import { usePersonalWorkstreams } from "../../state/workstreamsPersonal";
import { showPersonalFailure } from "./personalNotifications";
const personalStatusLabels = {
  ready: "Ready",
  offline: "Offline",
  update_required: "Update required",
  read_only: "Read only",
  sync_failed: "Couldn't sync",
} as const;
const deliveryReasons: Readonly<Record<string, string>> = {
  approval_required: "Approve this laptop in Workstreams settings on the receiving environment",
  different_authority: "This environment follows a different authority",
  different_definition: "This environment has a different Workstream definition",
  newer_definition: "This environment has a newer Workstream definition",
  definition_changed: "The Workstream changed on this environment",
  peer_access_required: "A writable, approved link is required",
  peer_only: "This Workstream exists only on that environment",
  peer_identity_changed: "The environment identity changed",
  peer_link_changed: "The link changed. Refresh enrollment to use the new link",
  peer_link_expired: "The link expired. Reconnect it, then refresh enrollment",
  peer_link_required: "Create a link from the laptop to this environment",
  peer_unavailable: "Waiting for the environment to respond",
  peer_update_required: "Update this environment to sync",
  roster_changed: "The environment list changed. Cancel this request and submit it again",
  unsettled_threads: "Settle or move active threads before closing",
};
function FleetControls({
  controller,
  state,
}: {
  controller: PersonalWorkstreamsController;
  state: PersonalWorkstreamsState;
}) {
  const { environments } = useEnvironments();
  const localId = useAtomValue(localEnvironmentIdAtom);
  const access = useEnvironmentsWithScope(environments, AuthAccessWriteScope);
  const [busy, setBusy] = useState(false);
  const fleet = state.fleet;
  const authorityId = fleet?.authorityEnvironmentId ?? localId;
  const authority = environments.find((entry) => entry.environmentId === authorityId);
  const authorityReady =
    authorityId !== null &&
    access.has(authorityId) &&
    authority?.connection.phase === "connected" &&
    authority.serverConfig?.environment.capabilities.workstreamsPersonalFleet?.version === 1;
  const linked = useEnvironmentQuery(
    authorityReady ? peerLinkEnvironment.list({ environmentId: authorityId, input: {} }) : null,
  );
  const links = linked.data?.links ?? [];
  const label = (id: EnvironmentId) =>
    environments.find((entry) => entry.environmentId === id)?.label ??
    links.find((entry) => entry.environmentId === id)?.label ??
    id;
  const proposals = state.environments.flatMap((entry) =>
    (entry.fleetProposals ?? []).map((proposal) => ({ entry, proposal })),
  );
  const run = async (action: () => Promise<PersonalResult<unknown>>) => {
    setBusy(true);
    try {
      const result = await action();
      if (!result.ok) showPersonalFailure(result.failure);
    } finally {
      setBusy(false);
    }
  };
  const manage = controller.manageFleet;
  const manageReady = authorityReady && !!manage && !busy;
  const actionReady = state.fleetAuthorityStatus === "ready" && !busy;
  const roster = fleet?.role === "authority" ? fleet : undefined;
  const candidates = [
    ...new Set([...links.map((link) => link.environmentId), ...(roster?.members ?? [])]),
  ];
  return (
    <>
      {fleet ? (
        <SettingsRow
          title="Workstream authority"
          description="Definition changes are saved here and synced to enrolled environments."
          control={<span className="text-sm">{label(fleet.authorityEnvironmentId)}</span>}
        />
      ) : authorityReady && manage && authorityId !== null ? (
        <SettingsRow
          title="Use this environment as the Workstream authority"
          description={
            linked.isSuccess
              ? links.length
                ? `Enrolls ${links.map((link) => link.label).join(", ")}. Approve the link in Workstreams settings on each environment.`
                : "This environment will be the only participant. Add others in Connections."
              : "Checking which linked environments to enroll."
          }
          control={
            <Button
              variant="outline"
              size="sm"
              disabled={!manageReady || !linked.isSuccess}
              onClick={() =>
                void run(() =>
                  manage(authorityId, {
                    kind: "designate",
                    peerEnvironmentIds: links.map((link) => link.environmentId),
                  }),
                )
              }
            >
              Use {label(authorityId)} as authority
            </Button>
          }
        />
      ) : null}
      {proposals.map(({ entry, proposal }) => (
        <SettingsRow
          key={`${entry.environmentId}:${proposal.sessionId}`}
          title={`Approve Workstream sync on ${label(entry.environmentId)}`}
          description={`${proposal.label} (${proposal.authorityEnvironmentId}) is asking to manage Workstream definitions on this environment. Link session ${proposal.sessionId.slice(-8)}.`}
          control={
            manage && access.has(entry.environmentId) ? (
              <Button
                variant="outline"
                size="sm"
                disabled={busy}
                onClick={() =>
                  void run(() =>
                    manage(entry.environmentId, {
                      kind: "follow",
                      authorityEnvironmentId: proposal.authorityEnvironmentId,
                      sessionId: proposal.sessionId,
                    }),
                  )
                }
              >
                Approve authority
              </Button>
            ) : null
          }
        />
      ))}
      {state.environments.map((entry) => {
        const following = entry.followsAuthority;
        const release = controller.releaseFollower;
        if (
          !following ||
          !release ||
          !access.has(entry.environmentId) ||
          roster?.members.includes(entry.environmentId) ||
          roster?.operations.some(
            (operation) =>
              operation.kind === "recovery" &&
              operation.peers.some(
                (peer) => peer.environmentId === entry.environmentId && peer.state !== "delivered",
              ),
          )
        )
          return null;
        return (
          <SettingsRow
            key={`follower:${entry.environmentId}`}
            title={`${label(entry.environmentId)} follows ${label(following)}`}
            description="Disconnecting sync here keeps the existing copies. Reload the app to work with this environment independently."
            control={
              <Button
                variant="outline"
                size="sm"
                disabled={busy}
                onClick={() => {
                  if (
                    !window.confirm(
                      `Stop following ${label(following)} on ${label(entry.environmentId)}?`,
                    )
                  )
                    return;
                  void run(() => release(entry.environmentId, following));
                }}
              >
                Stop following
              </Button>
            }
          />
        );
      })}
      {roster &&
        authorityId !== null &&
        candidates.map((id) => {
          const enrolled = roster.members.includes(id);
          const refresh =
            enrolled &&
            roster.operations.some((operation) =>
              operation.peers.some(
                (peer) => peer.environmentId === id && peer.reason === "peer_link_changed",
              ),
            );
          return (
            <SettingsRow
              key={id}
              title={label(id)}
              description={enrolled ? "Enrolled for Workstream sync" : "Available to enroll"}
              control={
                manage ? (
                  <div className="flex gap-2">
                    {enrolled && (
                      <Button
                        variant="outline"
                        size="sm"
                        disabled={!manageReady}
                        onClick={() =>
                          void run(() =>
                            manage(authorityId, {
                              kind: "remove",
                              environmentId: id,
                              expectedRosterRevision: roster.rosterRevision,
                            }),
                          )
                        }
                      >
                        Remove from sync
                      </Button>
                    )}
                    {(!enrolled || refresh) && (
                      <Button
                        variant="outline"
                        size="sm"
                        disabled={!manageReady}
                        onClick={() =>
                          void run(() =>
                            manage(authorityId, {
                              kind: "enroll",
                              environmentId: id,
                              expectedRosterRevision: roster.rosterRevision,
                            }),
                          )
                        }
                      >
                        {refresh ? "Refresh enrollment" : "Enroll"}
                      </Button>
                    )}
                  </div>
                ) : null
              }
            />
          );
        })}
      {roster?.operations.map((operation) => {
        const discovered = operation.status === "discovered";
        const name =
          state.workstreams.find((workstream) => workstream.id === operation.workstreamId)?.name ??
          operation.peers.find((peer) => peer.observed)?.observed?.name ??
          operation.workstreamId;
        const cancel = controller.cancelOperation;
        const resolve = controller.resolveOperation;
        return (
          <SettingsRow
            key={operation.requestId}
            title={`${name} · ${discovered ? "Found on an enrolled environment" : operation.kind === "close" ? "Close requested" : operation.kind === "recovery" ? "Restoring a cancelled close" : "Sync pending"}`}
            description={
              <div className="space-y-2">
                {operation.reason && <p>{deliveryReasons[operation.reason] ?? operation.reason}</p>}
                {operation.peers
                  .filter((peer) => peer.state !== "delivered")
                  .map((peer) => (
                    <div key={peer.environmentId}>
                      <p>
                        {label(peer.environmentId)} ·{" "}
                        {peer.reason
                          ? (deliveryReasons[peer.reason] ?? "Waiting for confirmation")
                          : "Waiting for confirmation"}
                      </p>
                      {peer.state === "conflict" &&
                        peer.observed &&
                        (operation.kind === "put" || operation.kind === "delete") &&
                        resolve && (
                          <div className="mt-1 flex flex-wrap gap-2">
                            {!discovered &&
                              (peer.observed?.deleted !== true ||
                                !state.workstreams.some(
                                  (entry) => entry.id === operation.workstreamId,
                                )) && (
                                <Button
                                  variant="outline"
                                  size="sm"
                                  disabled={!actionReady}
                                  onClick={() =>
                                    void run(() =>
                                      resolve({
                                        requestId: operation.requestId,
                                        environmentId: peer.environmentId,
                                        expectedObservationKey: peer.observed!.key,
                                        choice: "use_authority",
                                      }),
                                    )
                                  }
                                >
                                  Use authority’s version
                                </Button>
                              )}
                            {(peer.observed?.deleted === true ||
                              discovered ||
                              state.workstreams.some(
                                (entry) => entry.id === operation.workstreamId,
                              )) && (
                              <Button
                                variant="outline"
                                size="sm"
                                disabled={!actionReady}
                                onClick={() => {
                                  if (
                                    peer.observed?.deleted &&
                                    !window.confirm(
                                      `Delete ${name} and clear its saved assignments?`,
                                    )
                                  )
                                    return;
                                  void run(() =>
                                    resolve({
                                      requestId: operation.requestId,
                                      environmentId: peer.environmentId,
                                      expectedObservationKey: peer.observed!.key,
                                      choice: "use_peer",
                                    }),
                                  );
                                }}
                              >
                                {peer.observed?.deleted
                                  ? "Apply peer deletion"
                                  : discovered
                                    ? peer.observed?.lifecycle === "completed" ||
                                      peer.observed?.lifecycle === "abandoned"
                                      ? "Import as Active"
                                      : "Import to authority"
                                    : `Use ${label(peer.environmentId)}’s version`}
                              </Button>
                            )}
                          </div>
                        )}
                    </div>
                  ))}
              </div>
            }
            control={
              operation.kind === "close" && cancel ? (
                <Button
                  variant="outline"
                  size="sm"
                  disabled={!actionReady}
                  onClick={() => void run(() => cancel(operation.requestId))}
                >
                  Cancel close request
                </Button>
              ) : null
            }
          />
        );
      })}
    </>
  );
}
export function WorkstreamsSetupStatus() {
  const { controller, state } = usePersonalWorkstreams();
  const { environments } = useEnvironments();
  return (
    <SettingsSection title="Workstreams">
      {controller?.manageFleet ? <FleetControls controller={controller} state={state} /> : null}
      {state.environments.map((entry) => (
        <SettingsRow
          key={entry.environmentId}
          title={
            environments.find((item) => item.environmentId === entry.environmentId)?.label ??
            "Environment"
          }
          control={
            <span className="text-sm text-muted-foreground">
              {personalStatusLabels[entry.status]}
            </span>
          }
        />
      ))}
    </SettingsSection>
  );
}
