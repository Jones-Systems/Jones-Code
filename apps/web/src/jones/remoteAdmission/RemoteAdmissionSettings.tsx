import { useState } from "react";
import { Link } from "@tanstack/react-router";
import type { EnvironmentId, ModelSelection } from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { Button } from "../../components/ui/button";
import { Switch } from "../../components/ui/switch";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
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
  const selected =
    draft ??
    snapshot?.policy.modelSelection ??
    resolveDefaultProviderModelSelection(providers, null);
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
  const blocked = busy
    ? "Saving evaluator settings…"
    : loadFailed
      ? ownerLoadMessage(loadFailure ?? "other", receiver)
      : !snapshot || loading
        ? "Loading evaluator settings…"
        : !providersKnown
          ? "Evaluator provider availability is unknown. Wait for current provider data from the receiving environment."
          : !available
            ? "Choose an available evaluator provider and model before saving or enabling."
            : null;
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
      <section
        className="space-y-3 rounded-lg border p-4"
        aria-label={`Remote evaluator on ${receiver}`}
      >
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h3 className="text-sm font-medium">Background remote evaluation · {receiver}</h3>
          <UnavailableControl label="Refresh" reason={loading ? "Checking owner access…" : null}>
            <Button
              variant="outline"
              size="sm"
              disabled={loading}
              onClick={() => {
                void store.refresh();
              }}
            >
              Refresh
            </Button>
          </UnavailableControl>
        </div>
        <p role="status" className="text-xs text-muted-foreground wrap-anywhere">
          {ownerLoadMessage(loadFailure, receiver)}
        </p>
        <p className="text-xs text-muted-foreground">
          Evaluator policy is unknown. Evaluator provider availability is unknown for this session.
        </p>
      </section>
    );
  }
  return (
    <section
      className="space-y-3 rounded-lg border p-4"
      aria-label={`Remote evaluator on ${environment?.label ?? environmentId}`}
    >
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="min-w-0">
          <h3 className="text-sm font-medium">Background remote evaluation</h3>
          <p className="text-xs text-muted-foreground wrap-anywhere">
            Receiving environment: {environment?.label ?? environmentId}. Evaluates incoming tasks
            before they start.
          </p>
        </div>
        <UnavailableControl
          label="Evaluate remote requests"
          reason={
            busy || !policyKnown || (!snapshot?.policy.enabled && !available) ? blocked : null
          }
        >
          <Switch
            aria-label="Evaluate remote requests"
            aria-describedby={`evaluator-status-${environmentId}`}
            checked={snapshot?.policy.enabled ?? false}
            mixed={!policyKnown}
            disabled={busy || !policyKnown || (!snapshot?.policy.enabled && !available)}
            onCheckedChange={(enabled) => {
              void save(enabled);
            }}
          />
        </UnavailableControl>
      </div>
      <div className="flex flex-wrap items-center gap-2">
        {selected ? (
          <UnavailableControl
            label="Evaluator model"
            reason={busy || !policyKnown || !providersKnown ? blocked : null}
          >
            <ProviderModelPicker
              environmentId={environmentId}
              activeInstanceId={selected.instanceId}
              model={selected.model}
              lockedProvider={null}
              instanceEntries={entries}
              modelOptionsByInstance={modelOptions}
              disabled={busy || !policyKnown || !providersKnown}
              triggerAriaLabel="Evaluator model"
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
        ) : (
          <>
            {!policyKnown || !providersKnown ? (
              <UnavailableControl label="Evaluator model" reason={blocked}>
                <Button variant="outline" size="sm" aria-label="Evaluator model" disabled>
                  Evaluator model
                </Button>
              </UnavailableControl>
            ) : null}
            <p className="text-sm text-muted-foreground">
              {policyKnown && providersKnown
                ? "No providers advertise remote evaluation support."
                : "Evaluator provider availability is unknown."}
            </p>
          </>
        )}
        {selected && activeEntry ? (
          <UnavailableControl
            label="Evaluator options"
            reason={busy || !policyKnown || !providersKnown ? blocked : null}
          >
            <TraitsPicker
              provider={activeEntry.driverKind}
              models={activeEntry.models}
              model={selected.model}
              modelOptions={selected.options ?? []}
              allowPromptInjectedEffort={false}
              planModeEnabled={false}
              disabled={busy || !policyKnown || !providersKnown}
              onModelOptionsChange={(options) =>
                setDraft(createModelSelection(selected.instanceId, selected.model, options))
              }
            />
          </UnavailableControl>
        ) : null}
        <Tooltip>
          <TooltipTrigger
            render={
              <Button
                variant="outline"
                size="sm"
                aria-disabled={Boolean(blocked)}
                aria-describedby={`evaluator-status-${environmentId}`}
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
        <UnavailableControl
          label="Refresh"
          reason={busy ? "Saving evaluator settings…" : loading ? "Loading evaluator settings…" : null}
        >
          <Button
            variant="outline"
            size="sm"
            disabled={loading || busy}
            onClick={() => {
              void store.refresh();
            }}
          >
            Refresh
          </Button>
        </UnavailableControl>
      </div>
      <p
        id={`evaluator-status-${environmentId}`}
        role={loadFailed ? "alert" : "status"}
        className="text-xs text-muted-foreground wrap-anywhere"
      >
        {!policyKnown
          ? snapshot
            ? `Current evaluator policy is unknown. Last known: ${snapshot.policy.enabled ? "enabled" : "disabled"}. `
            : "Evaluator policy is unknown. "
          : `Evaluator policy is ${snapshot?.policy.enabled ? "enabled" : "disabled"}. `}
        {blocked ??
          "Runs as a background service using this exact provider account, model and options."}
        {selected && !available && providersKnown
          ? ` Saved selection: ${selected.instanceId} / ${selected.model}. No fallback will be saved.`
          : ""}
      </p>
      {selected ? (
        <p className="text-xs text-muted-foreground wrap-anywhere">
          Account: {activeEntry?.displayName ?? (providersKnown ? "Unavailable" : "Unknown")} ·{" "}
          {selected.instanceId}
        </p>
      ) : null}
      {error ? (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      ) : null}
      <p className="text-xs text-muted-foreground">
        {providerData?.some((provider) => provider.driver === "codex") ? (
          <>
            This Codex CLI integration cannot disable all task tools. Managed Codex (ChatGPT
            authorization) and qualified Claude integrations support remote evaluation.{" "}
          </>
        ) : null}
        Held requests stay in the{" "}
        <Link to="/requests" className="underline underline-offset-2">
          Requests inbox
        </Link>
        . Disabling evaluation does not resolve them.
      </p>
    </section>
  );
}
