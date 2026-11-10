import { useState } from "react";
import { Link } from "@tanstack/react-router";
import { ChevronDownIcon } from "lucide-react";
import type { EnvironmentId, ModelSelection } from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { Badge } from "../../components/ui/badge";
import { Button, InlineButton } from "../../components/ui/button";
import { Collapsible, CollapsiblePanel, CollapsibleTrigger } from "../../components/ui/collapsible";
import { RefreshIcon } from "../../components/ui/refresh-icon";
import { Switch } from "../../components/ui/switch";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import {
  SETTINGS_PICKER_TRIGGER_CLASSNAME,
  SettingsRow,
  SettingsSection,
} from "../../components/settings/settingsLayout";
import { ProviderModelPicker } from "../../components/chat/ProviderModelPicker";
import { TraitsPicker } from "../../components/chat/TraitsPicker";
import { useEnvironment, useEnvironments } from "../../state/environments";
import { EMPTY_SERVER_PROVIDERS } from "../../state/server";
import { useEnvironmentSettings } from "../../hooks/useSettings";
import { getCustomModelOptionsByInstance } from "../../modelSelection";
import {
  applyProviderInstanceSettings,
  deriveProviderInstanceEntries,
  resolveDefaultProviderModelSelection,
  sortProviderInstanceEntries,
} from "../../providerInstances";
import { useAdmission } from "./useAdmission";
import { UnavailableControl } from "./UnavailableControl";
import { ownerLoadMessage } from "./availability";
import { useEvaluatorProviders } from "./useEvaluatorProviders";

const SECTION_TITLE = "Background remote evaluation";

export function RemoteAdmissionConfigurations() {
  const { environments } = useEnvironments();
  return environments.map((environment) => (
    <RemoteAdmissionSettings
      key={environment.environmentId}
      environmentId={environment.environmentId}
    />
  ));
}

export function RemoteAdmissionSettings({
  environmentId,
}: {
  readonly environmentId: EnvironmentId;
}) {
  return <EvaluatorConfiguration key={environmentId} environmentId={environmentId} />;
}

function sameSelection(a: ModelSelection | null | undefined, b: ModelSelection | null | undefined) {
  if (!a || !b) return a === b;
  return (
    a.instanceId === b.instanceId &&
    a.model === b.model &&
    JSON.stringify(a.options ?? []) === JSON.stringify(b.options ?? [])
  );
}

function RefreshButton({ reason, onRefresh }: { reason: string | null; onRefresh: () => void }) {
  return (
    <UnavailableControl label="Refresh" reason={reason}>
      <Button
        size="icon-xs"
        variant="ghost-muted"
        aria-label="Refresh"
        disabled={reason !== null}
        onClick={onRefresh}
      >
        <RefreshIcon size="xs" refreshing={reason !== null} />
      </Button>
    </UnavailableControl>
  );
}

function EvaluatorConfiguration({ environmentId }: { readonly environmentId: EnvironmentId }) {
  const { snapshot, loadFailed, loadFailure, loading, error, busy, store } =
    useAdmission(environmentId);
  const [draft, setDraft] = useState<ModelSelection | null>(null);
  const environment = useEnvironment(environmentId);
  const settings = useEnvironmentSettings(environmentId);
  const receiver = environment?.label ?? environmentId;
  const providerData = useEvaluatorProviders(environmentId);
  const providersKnown = providerData !== null && environment?.connection.phase === "connected";
  const policyKnown = Boolean(snapshot && !loadFailed && !loading);
  const providers = (providerData ?? EMPTY_SERVER_PROVIDERS).filter(
    (provider) =>
      "supportsRemoteRequestEvaluation" in provider &&
      provider.supportsRemoteRequestEvaluation === true,
  );
  const entries = sortProviderInstanceEntries(
    applyProviderInstanceSettings(deriveProviderInstanceEntries(providers), settings, null),
  );
  const savedSelection = snapshot?.policy.modelSelection ?? null;
  const selected = draft ?? savedSelection ?? resolveDefaultProviderModelSelection(providers, null);
  const modelOptions = getCustomModelOptionsByInstance(
    settings,
    providers,
    selected?.instanceId,
    selected?.model,
  );
  const activeEntry = entries.find((entry) => entry.instanceId === selected?.instanceId);
  const selectedModel = selected
    ? modelOptions.get(selected.instanceId)?.find((model) => model.slug === selected.model)
    : undefined;
  const available = Boolean(
    providersKnown &&
    activeEntry?.enabled &&
    activeEntry.isAvailable &&
    activeEntry.status === "ready" &&
    selectedModel &&
    !selectedModel.isUnavailable,
  );
  // Save appears only when the shown selection differs from the saved policy.
  const dirty = Boolean(
    selected &&
    ((draft !== null && !sameSelection(draft, savedSelection)) ||
      (snapshot && savedSelection === null)),
  );
  const configuredProviders = providerData ?? EMPTY_SERVER_PROVIDERS;
  const configuredModelOptions = getCustomModelOptionsByInstance(settings, configuredProviders);
  const rejectedInstances = sortProviderInstanceEntries(
    applyProviderInstanceSettings(
      deriveProviderInstanceEntries(configuredProviders),
      settings,
      null,
    ),
  ).flatMap((entry) => {
    const provider = entry.snapshot;
    let reason: string;
    if (provider.supportsRemoteRequestEvaluation !== true) {
      reason = "Not supported.";
    } else if (provider.availability === "unavailable") {
      reason = `Unavailable.${provider.unavailableReason ? ` ${provider.unavailableReason}` : ""}`;
    } else if (!entry.enabled) {
      reason = "Not enabled.";
    } else if (provider.auth.status === "unauthenticated") {
      reason = `Not signed in.${provider.message ? ` ${provider.message}` : ""}`;
    } else if (entry.status !== "ready") {
      reason = `Status: ${entry.status}.${provider.message ? ` ${provider.message}` : ""}`;
    } else if (
      !configuredModelOptions.get(entry.instanceId)?.some((model) => !model.isUnavailable)
    ) {
      reason = "No available models reported.";
    } else {
      return [];
    }
    return [{ entry, reason }];
  });
  // Full explanation for disabled-control tooltips.
  const blocked = busy
    ? "Saving evaluator settings…"
    : loadFailed
      ? ownerLoadMessage(loadFailure ?? "other", receiver)
      : !snapshot || loading
        ? "Loading evaluator settings…"
        : !providersKnown
          ? "Provider availability is unknown. Waiting for current provider data from the receiving environment."
          : !available
            ? "Choose an available evaluator model first."
            : null;
  // Compact inline status; empty when the controls already say everything.
  const statusText = busy
    ? "Saving…"
    : loadFailed
      ? `Owner review is unavailable on ${receiver}. Retrying automatically.`
      : !snapshot
        ? "Loading…"
        : loading
          ? `Checking… Last known: ${snapshot.policy.enabled ? "enabled" : "disabled"}.`
          : !providersKnown
            ? "Provider availability is unknown."
            : selected && !available
              ? `Saved model ${selected.instanceId} / ${selected.model} is unavailable. No fallback will be saved.`
              : null;
  const statusId = `evaluator-status-${environmentId}`;
  const refresh = () => {
    void store.refresh();
  };
  const save = async (enabled: boolean) => {
    if (!snapshot || !policyKnown || busy || (enabled && !available)) return;
    await store.savePolicy({
      enabled,
      expectedRevision: snapshot.policy.revision,
      modelSelection: selected,
    });
  };

  if (loadFailure === "unauthorized" || loadFailure === "unsupported") {
    return (
      <SettingsSection
        title={SECTION_TITLE}
        aria-label={`Remote evaluator on ${receiver}`}
        headerAction={
          <RefreshButton reason={loading ? "Checking owner access…" : null} onRefresh={refresh} />
        }
      >
        <SettingsRow
          title="Evaluate remote requests"
          description={<span role="status">{ownerLoadMessage(loadFailure, receiver)}</span>}
          control={
            <Badge variant="outline" size="sm">
              Unknown
            </Badge>
          }
        />
      </SettingsSection>
    );
  }

  const controlsDisabled = busy || !policyKnown || !providersKnown;
  const toggleDisabled = busy || !policyKnown || (!snapshot?.policy.enabled && !available);
  return (
    <SettingsSection
      title={SECTION_TITLE}
      aria-label={`Remote evaluator on ${receiver}`}
      headerAction={
        <RefreshButton
          reason={
            busy ? "Saving evaluator settings…" : loading ? "Loading evaluator settings…" : null
          }
          onRefresh={refresh}
        />
      }
    >
      <SettingsRow
        title="Evaluate remote requests"
        description={
          <>
            Screens incoming tasks on <bdi>{receiver}</bdi> before they start. Held tasks wait in{" "}
            <Link to="/requests" className="underline underline-offset-2">
              Requests
            </Link>
            .
          </>
        }
        status={
          statusText || error ? (
            <>
              {statusText ? (
                <p id={statusId} role={loadFailed ? "alert" : "status"} className="wrap-anywhere">
                  {statusText}
                </p>
              ) : null}
              {error ? (
                <p role="alert" className="text-destructive">
                  {error}
                </p>
              ) : null}
            </>
          ) : null
        }
        control={
          <UnavailableControl
            label="Evaluate remote requests"
            reason={toggleDisabled ? blocked : null}
          >
            <Switch
              aria-label="Evaluate remote requests"
              {...(statusText ? { "aria-describedby": statusId } : {})}
              checked={snapshot?.policy.enabled ?? false}
              mixed={!policyKnown}
              disabled={toggleDisabled}
              onCheckedChange={(enabled) => {
                void save(enabled);
              }}
            />
          </UnavailableControl>
        }
      />
      <SettingsRow
        title="Evaluator"
        description={
          selected
            ? `Uses ${activeEntry?.displayName ?? (providersKnown ? "an unavailable account" : "an unknown account")}.`
            : "Model that screens incoming tasks."
        }
        status={
          providersKnown && rejectedInstances.length > 0 ? (
            <Collapsible>
              <CollapsibleTrigger render={<InlineButton tone="muted" />}>
                {rejectedInstances.length === 1
                  ? "1 provider unavailable for remote evaluation"
                  : `${rejectedInstances.length} providers unavailable for remote evaluation`}
                <ChevronDownIcon className="size-3 shrink-0" />
              </CollapsibleTrigger>
              <CollapsiblePanel>
                <ul
                  aria-label="Provider instances unavailable for remote evaluation"
                  className="mt-1 space-y-0.5 wrap-anywhere"
                >
                  {rejectedInstances.map(({ entry, reason }) => (
                    <li key={entry.instanceId}>
                      <bdi>{entry.displayName}</bdi> · <bdi>{entry.instanceId}</bdi>: {reason}
                    </li>
                  ))}
                </ul>
              </CollapsiblePanel>
            </Collapsible>
          ) : null
        }
        control={
          <div className="flex min-w-0 flex-wrap items-center justify-end gap-1.5">
            {selected ? (
              <UnavailableControl
                label="Evaluator model"
                reason={controlsDisabled ? blocked : null}
              >
                <ProviderModelPicker
                  environmentId={environmentId}
                  activeInstanceId={selected.instanceId}
                  model={selected.model}
                  lockedProvider={null}
                  instanceEntries={entries}
                  modelOptionsByInstance={modelOptions}
                  disabled={controlsDisabled}
                  triggerAriaLabel="Evaluator model"
                  triggerClassName={SETTINGS_PICKER_TRIGGER_CLASSNAME}
                  {...(!providersKnown
                    ? { triggerLabel: `${selected.model} (Availability unknown)` }
                    : !available
                      ? { triggerLabel: `${selected.model} (Unavailable)` }
                      : {})}
                  onInstanceModelChange={(instanceId, model) =>
                    setDraft(
                      selected.instanceId === instanceId && selected.model === model
                        ? selected
                        : { instanceId, model },
                    )
                  }
                />
              </UnavailableControl>
            ) : !policyKnown || !providersKnown ? (
              <UnavailableControl label="Evaluator model" reason={blocked}>
                <Button variant="outline" size="sm" aria-label="Evaluator model" disabled>
                  Availability unknown
                </Button>
              </UnavailableControl>
            ) : (
              <span className="text-sm text-muted-foreground">No supported providers</span>
            )}
            {selected && activeEntry ? (
              <UnavailableControl
                label="Evaluator options"
                reason={controlsDisabled ? blocked : null}
              >
                <TraitsPicker
                  provider={activeEntry.driverKind}
                  models={activeEntry.models}
                  model={selected.model}
                  modelOptions={selected.options ?? []}
                  allowPromptInjectedEffort={false}
                  planModeEnabled={false}
                  disabled={controlsDisabled}
                  triggerClassName={SETTINGS_PICKER_TRIGGER_CLASSNAME}
                  onModelOptionsChange={(options) =>
                    setDraft(createModelSelection(selected.instanceId, selected.model, options))
                  }
                />
              </UnavailableControl>
            ) : null}
            {dirty ? (
              <Tooltip>
                <TooltipTrigger
                  render={
                    <Button
                      size="sm"
                      aria-disabled={Boolean(blocked)}
                      {...(statusText ? { "aria-describedby": statusId } : {})}
                      onClick={() => {
                        if (!blocked && snapshot) void save(snapshot.policy.enabled);
                      }}
                    >
                      Save model
                    </Button>
                  }
                />
                {blocked ? <TooltipPopup>{blocked}</TooltipPopup> : null}
              </Tooltip>
            ) : null}
          </div>
        }
      />
    </SettingsSection>
  );
}
