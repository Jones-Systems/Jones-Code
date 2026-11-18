import { AuthSettingsWriteScope } from "@t3tools/contracts";
import type { RepoFreshnessStatus } from "@t3tools/contracts/jones/repoFreshness";
import { supportsRepoFreshness } from "@t3tools/client-runtime/jones/repo-freshness";
import { isAtomCommandInterrupted } from "@t3tools/client-runtime/state/runtime";
import { useRef, useState } from "react";

import { SettingsRow, SettingsSection } from "../../components/settings/settingsLayout";
import {
  Select,
  SelectItem,
  SelectPopup,
  SelectTrigger,
  SelectValue,
} from "../../components/ui/select";
import { toastManager } from "../../components/ui/toast";
import type { SidebarProjectGroupMember } from "../../sidebarProjectGrouping";
import { readEnvironment, useEnvironments } from "../../state/environments";
import { serverEnvironment } from "../../state/server";
import { useEnvironmentScope } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";

type Cadence = RepoFreshnessStatus["cadenceSeconds"];
const CADENCES = [
  { value: 0, label: "Off" },
  { value: 120, label: "2 minutes" },
  { value: 300, label: "5 minutes" },
] as const;

function RepoFreshnessCadenceRow({
  member,
  cadence,
  connected,
  showCheckout,
}: {
  member: SidebarProjectGroupMember;
  cadence: Cadence;
  connected: boolean;
  showCheckout: boolean;
}) {
  const { environmentId, id: projectId } = member;
  const canWrite = useEnvironmentScope(environmentId, AuthSettingsWriteScope);
  const update = useAtomCommand(serverEnvironment.updateSettings, { reportFailure: false });
  const [saving, setSaving] = useState(false);
  const savingRef = useRef(false);

  const save = async (next: Cadence | null) => {
    if (next === null || next === cadence || savingRef.current || !canWrite) return;
    const current = readEnvironment(environmentId);
    const config = current?.serverConfig;
    if (
      current?.connection.phase !== "connected" ||
      !config ||
      !supportsRepoFreshness(config.environment)
    )
      return;
    savingRef.current = true;
    setSaving(true);
    await update({
      environmentId,
      input: {
        patch: {
          projectSettingsOverrides: {
            [projectId]: {
              ...config.settings.projectSettingsOverrides[projectId],
              repoFreshnessCadenceSeconds: next,
            },
          },
        },
      },
    })
      .then((result) => {
        if (
          !isAtomCommandInterrupted(result) &&
          (result._tag === "Failure" ||
            result.value.projectSettingsOverrides[projectId]?.repoFreshnessCadenceSeconds !== next)
        ) {
          toastManager.add({ type: "error", title: "Could not save repository freshness" });
        }
      })
      .catch(() => {
        toastManager.add({ type: "error", title: "Could not save repository freshness" });
      })
      .finally(() => {
        savingRef.current = false;
        setSaving(false);
      });
  };

  const checkout = member.environmentLabel
    ? `${member.environmentLabel} · ${member.workspaceRoot}`
    : member.workspaceRoot;
  return (
    <SettingsRow
      title="Automatic fetch"
      description={showCheckout ? checkout : "Fetch the remote default branch on this environment."}
      control={
        <Select items={CADENCES} value={cadence} onValueChange={(next) => void save(next)}>
          <SelectTrigger
            size="sm"
            aria-label={showCheckout ? `Automatic fetch for ${checkout}` : "Automatic fetch"}
            disabled={!connected || !canWrite || saving}
          >
            <SelectValue />
          </SelectTrigger>
          <SelectPopup align="end" alignItemWithTrigger={false}>
            {CADENCES.map(({ value, label }) => (
              <SelectItem key={value} value={value}>
                {label}
              </SelectItem>
            ))}
          </SelectPopup>
        </Select>
      }
    />
  );
}

export function RepoFreshnessSettings({
  members,
}: {
  members: readonly SidebarProjectGroupMember[];
}) {
  const { environments } = useEnvironments();
  const environmentsById = new Map(
    environments.map((environment) => [environment.environmentId, environment]),
  );
  const rows = members.flatMap((member) => {
    const environment = environmentsById.get(member.environmentId);
    const config = environment?.serverConfig;
    if (!config || !supportsRepoFreshness(config.environment)) return [];
    return [
      <RepoFreshnessCadenceRow
        key={JSON.stringify([member.environmentId, member.id])}
        member={member}
        cadence={
          config.settings.projectSettingsOverrides[member.id]?.repoFreshnessCadenceSeconds ?? 0
        }
        connected={environment.connection.phase === "connected"}
        showCheckout={members.length > 1}
      />,
    ];
  });
  return rows.length > 0 ? <SettingsSection title="Git">{rows}</SettingsSection> : null;
}
