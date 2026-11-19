import { AuthSettingsWriteScope, type EnvironmentId } from "@t3tools/contracts";
import type { RepoFreshnessStatus } from "@t3tools/contracts/jones/repoFreshness";
import { supportsRepoFreshness } from "@t3tools/client-runtime/jones/repo-freshness";
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
import { readEnvironmentScope, useEnvironmentsWithScope } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";

type Cadence = RepoFreshnessStatus["cadenceSeconds"];
const CADENCES = [
  { value: 0, label: "Off" },
  { value: 120, label: "2 minutes" },
  { value: 300, label: "5 minutes" },
] as const;

export function RepoFreshnessSettings({
  members,
}: {
  members: readonly SidebarProjectGroupMember[];
}) {
  const { environments } = useEnvironments();
  const writable = useEnvironmentsWithScope(members, AuthSettingsWriteScope);
  const update = useAtomCommand(serverEnvironment.updateSettings, { reportFailure: false });
  const [choices, setChoices] = useState<Partial<Record<EnvironmentId, string>>>({});
  const [saving, setSaving] = useState(false);
  const savingRef = useRef(false);

  const byEnvironment = new Map<EnvironmentId, SidebarProjectGroupMember[]>();
  for (const member of members) {
    const checkouts = byEnvironment.get(member.environmentId);
    if (checkouts) checkouts.push(member);
    else byEnvironment.set(member.environmentId, [member]);
  }
  const groups = [...byEnvironment].map(([environmentId, checkouts]) => {
    const environment = environments.find((entry) => entry.environmentId === environmentId);
    const checkout =
      checkouts.length === 1
        ? checkouts[0]
        : checkouts.find((member) => member.physicalProjectKey === choices[environmentId]);
    return {
      environmentId,
      label: checkouts[0]?.environmentLabel ?? environment?.label ?? environmentId,
      checkouts,
      checkout,
      config: environment?.serverConfig,
      connected: environment?.connection.phase === "connected",
    };
  });

  if (!groups.some(({ config }) => supportsRepoFreshness(config?.environment))) return null;

  const unavailable = groups.find(
    ({ environmentId, config, connected }) =>
      !connected || !supportsRepoFreshness(config?.environment) || !writable.has(environmentId),
  );
  const needsCheckout = groups.some(({ checkout }) => !checkout);
  const cadences = groups.flatMap(({ checkout, config }) =>
    checkout && config
      ? [config.settings.projectSettingsOverrides[checkout.id]?.repoFreshnessCadenceSeconds ?? 0]
      : [],
  );
  const firstCadence = cadences[0];
  const cadence = cadences.every((value) => value === firstCadence) ? firstCadence : undefined;

  const save = (next: Cadence | null) => {
    if (next === null || next === cadence || needsCheckout || unavailable || savingRef.current) {
      return;
    }
    const targets = groups.map(({ environmentId, label, checkout }) => {
      const environment = readEnvironment(environmentId);
      return { environmentId, label, checkout, environment };
    });
    const blocked = targets.find(
      ({ environmentId, checkout, environment }) =>
        !checkout ||
        environment?.connection.phase !== "connected" ||
        !supportsRepoFreshness(environment.serverConfig?.environment) ||
        !readEnvironmentScope(environmentId, AuthSettingsWriteScope),
    );
    if (blocked) {
      toastManager.add({
        type: "error",
        title: "Automatic fetch was not saved",
        description: `Check the connection and settings access for ${blocked.label}.`,
      });
      return;
    }
    savingRef.current = true;
    setSaving(true);
    void Promise.all(
      targets.map(async ({ environmentId, label, checkout, environment }) => {
        if (!checkout || !environment?.serverConfig) return label;
        const projectId = checkout.id;
        const current = environment.serverConfig.settings.projectSettingsOverrides[projectId];
        if ((current?.repoFreshnessCadenceSeconds ?? 0) === next) return null;
        const result = await update({
          environmentId,
          input: {
            patch: {
              projectSettingsOverrides: { [projectId]: { repoFreshnessCadenceSeconds: next } },
            },
          },
        }).catch(() => null);
        return result?._tag === "Success" &&
          result.value.projectSettingsOverrides[projectId]?.repoFreshnessCadenceSeconds === next
          ? null
          : label;
      }),
    )
      .then((outcomes) => {
        const unconfirmed = outcomes.filter((label) => label !== null);
        if (unconfirmed.length === 0) return;
        toastManager.add({
          type: "error",
          title: "Automatic fetch could not be confirmed everywhere",
          description: `Check the setting on ${unconfirmed.join(", ")}.`,
        });
      })
      .finally(() => {
        savingRef.current = false;
        setSaving(false);
      });
  };

  return (
    <SettingsSection title="Git">
      <SettingsRow
        title="Automatic fetch"
        description={
          unavailable
            ? `Automatic fetch is unavailable on ${unavailable.label}. Check its connection, version, and settings access.`
            : needsCheckout
              ? "Choose a checkout on each environment that has more than one."
              : "Check the remote default branch for the selected environments."
        }
        control={
          <Select
            items={CADENCES}
            value={needsCheckout ? null : (cadence ?? null)}
            onValueChange={save}
          >
            <SelectTrigger
              size="sm"
              aria-label="Automatic fetch"
              disabled={Boolean(unavailable) || needsCheckout || saving}
            >
              <SelectValue placeholder={needsCheckout ? "Choose checkouts" : "Mixed"} />
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
      {groups
        .filter(({ checkouts }) => checkouts.length > 1)
        .map(({ environmentId, label, checkouts, checkout }) => (
          <SettingsRow
            key={environmentId}
            title={`${label} checkout`}
            control={
              <Select
                items={checkouts.map(({ physicalProjectKey, workspaceRoot }) => ({
                  value: physicalProjectKey,
                  label: workspaceRoot,
                }))}
                value={checkout?.physicalProjectKey ?? null}
                onValueChange={(value) =>
                  setChoices((current) => ({ ...current, [environmentId]: value ?? undefined }))
                }
              >
                <SelectTrigger size="sm" aria-label={`${label} checkout`} disabled={saving}>
                  <SelectValue placeholder="Choose checkout" />
                </SelectTrigger>
                <SelectPopup align="end" alignItemWithTrigger={false}>
                  {checkouts.map(({ physicalProjectKey, workspaceRoot }) => (
                    <SelectItem key={physicalProjectKey} value={physicalProjectKey}>
                      {workspaceRoot}
                    </SelectItem>
                  ))}
                </SelectPopup>
              </Select>
            }
          />
        ))}
    </SettingsSection>
  );
}
