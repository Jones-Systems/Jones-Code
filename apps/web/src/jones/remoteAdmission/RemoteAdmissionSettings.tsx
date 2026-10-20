import { useCallback, useEffect, useState } from "react";
import {
  type EnvironmentId,
  type RemoteAdmissionSnapshot,
  type ModelSelection,
} from "@t3tools/contracts";
import { Button } from "../../components/ui/button";
import { ProviderModelPicker } from "../../components/chat/ProviderModelPicker";
import { useEnvironmentProviders } from "../../state/environments";
import { EMPTY_SERVER_PROVIDERS } from "../../state/server";
import { useEnvironmentSettings } from "../../hooks/useSettings";
import { getCustomModelOptionsByInstance } from "../../modelSelection";
import {
  applyProviderInstanceSettings,
  deriveProviderInstanceEntries,
  resolveDefaultProviderModelSelection,
  sortProviderInstanceEntries,
} from "../../providerInstances";
import { admissionPresentation } from "./presentation";
import { Switch } from "../../components/ui/switch";
import { remoteAdmissionRequest } from "./runtime";

export function RemoteAdmissionSettings({
  environmentId,
}: {
  readonly environmentId: EnvironmentId;
}) {
  const [snapshot, setSnapshot] = useState<RemoteAdmissionSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [selection, setSelection] = useState<ModelSelection | null>(null);
  const settings = useEnvironmentSettings(environmentId);
  const providers = (useEnvironmentProviders().get(environmentId) ?? EMPTY_SERVER_PROVIDERS).filter(
    (provider) => provider.driver === "claudeAgent",
  );
  const entries = sortProviderInstanceEntries(
    applyProviderInstanceSettings(deriveProviderInstanceEntries(providers), settings, null),
  );
  const selected =
    selection ??
    resolveDefaultProviderModelSelection(providers, snapshot?.policy.modelSelection ?? null);
  const modelOptions = getCustomModelOptionsByInstance(
    settings,
    providers,
    selected?.instanceId,
    selected?.model,
  );
  const refresh = useCallback(async () => {
    const result = await remoteAdmissionRequest(environmentId, { action: "snapshot" });
    if ("pending" in result) setSnapshot(result);
  }, [environmentId]);
  useEffect(() => {
    let active = true;
    let initialized = false;
    let inFlight = false;
    const read = async () => {
      if (inFlight || document.visibilityState === "hidden") return;
      inFlight = true;
      try {
        const result = await remoteAdmissionRequest(environmentId, { action: "snapshot" });
        if (active && "pending" in result) {
          setSnapshot(result);
          setError(null);
          if (!initialized && result.policy.modelSelection) {
            setSelection(result.policy.modelSelection);
            initialized = true;
          }
        }
      } catch {
        if (active) setSnapshot(null);
      } finally {
        inFlight = false;
      }
    };
    void read();
    const interval = window.setInterval(() => {
      void read();
    }, 10_000);
    return () => {
      active = false;
      window.clearInterval(interval);
    };
  }, [environmentId]);
  const act = async (work: () => Promise<unknown>) => {
    setBusy(true);
    setError(null);
    try {
      await work();
      await refresh();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Request could not be completed.");
    } finally {
      setBusy(false);
    }
  };
  if (!snapshot) return null;
  return (
    <section className="space-y-3 rounded-lg border p-4" aria-label="Remote request review">
      <div className="flex items-center justify-between gap-4">
        <div>
          <h3 className="text-sm font-medium">Review remote requests on this laptop</h3>
          <p className="text-xs text-muted-foreground">
            Evaluate incoming tasks before they start. Requests that need your input wait here.
          </p>
        </div>
        <Switch
          aria-label="Evaluate remote requests"
          checked={snapshot.policy.enabled}
          disabled={busy || selected === null}
          onCheckedChange={(enabled) => {
            void act(() =>
              remoteAdmissionRequest(environmentId, {
                action: "policy",
                input: {
                  enabled,
                  expectedRevision: snapshot.policy.revision,
                  modelSelection: selected,
                },
              }),
            );
          }}
        />
      </div>
      <div className="flex items-center gap-2">
        {selected ? (
          <ProviderModelPicker
            environmentId={environmentId}
            activeInstanceId={selected.instanceId}
            model={selected.model}
            lockedProvider={null}
            instanceEntries={entries}
            modelOptionsByInstance={modelOptions}
            disabled={busy}
            triggerAriaLabel="Evaluator model"
            onInstanceModelChange={(instanceId, model) => setSelection({ instanceId, model })}
          />
        ) : (
          <p className="text-sm text-muted-foreground">
            Set up a Claude Code provider in Providers to choose an evaluator model.
          </p>
        )}
        <Button
          variant="outline"
          disabled={busy || selected === null}
          onClick={() => {
            void act(() =>
              remoteAdmissionRequest(environmentId, {
                action: "policy",
                input: {
                  enabled: snapshot.policy.enabled,
                  expectedRevision: snapshot.policy.revision,
                  modelSelection: selected,
                },
              }),
            );
          }}
        >
          Save model
        </Button>
      </div>
      {error ? (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      ) : null}
      {snapshot.pending.map((request) => {
        const view = admissionPresentation(request);
        return (
          <article className="space-y-2 rounded-md border p-3" key={request.id}>
            <div className="text-sm font-medium">
              {request.status === "held"
                ? "Needs input"
                : request.status === "dispatching"
                  ? "Dispatch awaiting confirmation"
                  : "Request pending"}{" "}
              · {view.title}
            </div>
            <p className="text-xs text-muted-foreground">
              From {view.source} · {view.target}
            </p>
            <p className="whitespace-pre-wrap text-sm">{view.message}</p>
            {request.reason ? (
              <p className="text-sm text-muted-foreground">{request.reason}</p>
            ) : null}

            {request.status === "held" ? (
              <div className="flex gap-2">
                <Button
                  disabled={
                    busy || request.envelope.receiverPolicy.revision !== snapshot.policy.revision
                  }
                  onClick={() => {
                    void act(() =>
                      remoteAdmissionRequest(environmentId, {
                        action: "decide",
                        input: {
                          id: request.id,
                          bindingDigest: request.bindingDigest,
                          decision: "approve",
                        },
                      }),
                    );
                  }}
                >
                  Approve
                </Button>
                <Button
                  variant="outline"
                  disabled={busy}
                  onClick={() => {
                    void act(() =>
                      remoteAdmissionRequest(environmentId, {
                        action: "decide",
                        input: {
                          id: request.id,
                          bindingDigest: request.bindingDigest,
                          decision: "deny",
                        },
                      }),
                    );
                  }}
                >
                  Deny
                </Button>
              </div>
            ) : null}
            {request.envelope.receiverPolicy.revision !== snapshot.policy.revision ? (
              <p className="text-xs text-muted-foreground">
                Settings changed. Deny this request and ask the sender to submit a new request.
              </p>
            ) : null}
          </article>
        );
      })}
    </section>
  );
}
