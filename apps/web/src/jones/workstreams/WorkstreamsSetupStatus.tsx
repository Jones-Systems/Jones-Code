import { useAtomValue } from "@effect/atom-react";
import type {
  PersonalWorkstreamsController,
  PersonalWorkstreamsState,
  PersonalResult,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { AuthAccessWriteScope, type EnvironmentId } from "@t3tools/contracts";
import { msg, T, useGT, useMessages, Var } from "gt-react";
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
  approval_required: msg(
    "Approve this laptop in Workstreams settings on the receiving environment",
  ),
  different_authority: msg("This environment follows a different authority"),
  different_definition: msg("This environment has a different Workstream definition"),
  newer_definition: msg("This environment has a newer Workstream definition"),
  definition_changed: msg("The Workstream changed on this environment"),
  peer_access_required: msg("A writable, approved link is required"),
  peer_only: msg("This Workstream exists only on that environment"),
  peer_identity_changed: msg("The environment identity changed"),
  peer_link_changed: msg("The link changed. Refresh enrollment to use the new link"),
  peer_link_expired: msg("The link expired. Reconnect it, then refresh enrollment"),
  peer_link_required: msg("Create a link from the laptop to this environment"),
  peer_unavailable: msg("Waiting for the environment to respond"),
  peer_update_required: msg("Update this environment to sync"),
  roster_changed: msg("The environment list changed. Cancel this request and submit it again"),
  unsettled_threads: msg("Settle or move active threads before closing"),
};
function FleetControls({
  controller,
  state,
}: {
  controller: PersonalWorkstreamsController;
  state: PersonalWorkstreamsState;
}) {
  const gt = useGT();
  const m = useMessages();
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
  const run = (action: () => Promise<PersonalResult<unknown>>) => {
    setBusy(true);
    return Promise.resolve()
      .then(action)
      .then((result) => {
        if (!result.ok) showPersonalFailure(result.failure);
      })
      .finally(() => setBusy(false));
  };
  const manage = controller.manageFleet;
  const manageReady = authorityReady && !!manage && !busy;
  const actionReady = state.fleetAuthorityStatus === "ready" && !busy;
  const roster = fleet?.role === "authority" ? fleet : undefined;
  const operations = roster?.operations.filter(
    ({ status }) => status === "pending" || status === "stale" || status === "discovered",
  );
  const candidates = [
    ...new Set([...links.map((link) => link.environmentId), ...(roster?.members ?? [])]),
  ];
  return (
    <>
      {fleet ? (
        <SettingsRow
          title={gt("Workstream authority")}
          description={gt("Definition changes are saved here and synced to enrolled environments.")}
          control={<span className="text-sm">{label(fleet.authorityEnvironmentId)}</span>}
        />
      ) : authorityReady && manage && authorityId !== null ? (
        <SettingsRow
          title={gt("Use this environment as the Workstream authority")}
          description={
            linked.isSuccess
              ? links.length
                ? gt(
                    "Enrolls {environments}. Approve the link in Workstreams settings on each environment.",
                    { environments: links.map((link) => link.label).join(", ") },
                  )
                : gt("This environment will be the only participant. Add others in Connections.")
              : gt("Checking which linked environments to enroll.")
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
              <T>
                Use <Var>{label(authorityId)}</Var> as authority
              </T>
            </Button>
          }
        />
      ) : null}
      {proposals.map(({ entry, proposal }) => (
        <SettingsRow
          key={`${entry.environmentId}:${proposal.sessionId}`}
          title={gt("Approve Workstream sync on {environment}", {
            environment: label(entry.environmentId),
          })}
          description={gt(
            "{name} ({environmentId}) is asking to manage Workstream definitions on this environment. Link session {session}.",
            {
              name: proposal.label,
              environmentId: proposal.authorityEnvironmentId,
              session: proposal.sessionId.slice(-8),
            },
          )}
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
                <T>Approve authority</T>
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
            title={gt("{environment} follows {authority}", {
              environment: label(entry.environmentId),
              authority: label(following),
            })}
            description={gt(
              "Disconnecting sync here keeps the existing copies. Reload the app to work with this environment independently.",
            )}
            control={
              <Button
                variant="outline"
                size="sm"
                disabled={busy}
                onClick={() => {
                  if (
                    !window.confirm(
                      gt("Stop following {authority} on {environment}?", {
                        authority: label(following),
                        environment: label(entry.environmentId),
                      }),
                    )
                  )
                    return;
                  void run(() => release(entry.environmentId, following));
                }}
              >
                <T>Stop following</T>
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
              description={
                enrolled ? gt("Enrolled for Workstream sync") : gt("Available to enroll")
              }
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
                        <T>Remove from sync</T>
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
                        {refresh
                          ? gt("Refresh enrollment")
                          : gt("Enroll", {
                              $context: "verb, enroll an environment in Workstream sync",
                            })}
                      </Button>
                    )}
                  </div>
                ) : null
              }
            />
          );
        })}
      {operations?.map((operation) => {
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
            title={
              discovered
                ? gt("{name} · Found on an enrolled environment", { name })
                : operation.kind === "close"
                  ? gt("{name} · Close requested", { name })
                  : operation.kind === "recovery"
                    ? gt("{name} · Restoring a cancelled close", { name })
                    : gt("{name} · Sync pending", { name })
            }
            description={
              <span className="block space-y-2">
                {operation.reason && (
                  <span className="block">
                    {m(deliveryReasons[operation.reason] ?? operation.reason)}
                  </span>
                )}
                {operation.peers
                  .filter((peer) => peer.state !== "delivered")
                  .map((peer) => (
                    <span key={peer.environmentId} className="block">
                      <span className="block">
                        {label(peer.environmentId)} ·{" "}
                        {m(
                          (peer.reason && deliveryReasons[peer.reason]) ||
                            msg("Waiting for confirmation"),
                        )}
                      </span>
                      {peer.state === "conflict" &&
                        peer.observed &&
                        (operation.kind === "put" || operation.kind === "delete") &&
                        resolve && (
                          <span className="mt-1 flex flex-wrap gap-2">
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
                                  <T>Use authority’s version</T>
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
                                      gt("Delete {name} and clear its saved assignments?", {
                                        name,
                                      }),
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
                                  ? gt("Apply peer deletion")
                                  : discovered
                                    ? peer.observed?.lifecycle === "completed" ||
                                      peer.observed?.lifecycle === "abandoned"
                                      ? gt("Import as Active")
                                      : gt("Import to authority")
                                    : gt("Use {environment}’s version", {
                                        environment: label(peer.environmentId),
                                      })}
                              </Button>
                            )}
                          </span>
                        )}
                    </span>
                  ))}
              </span>
            }
            control={
              operation.kind === "close" && cancel ? (
                <Button
                  variant="outline"
                  size="sm"
                  disabled={!actionReady}
                  onClick={() => void run(() => cancel(operation.requestId))}
                >
                  <T>Cancel close request</T>
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
