import { useEffect, useRef, useState, useSyncExternalStore, type ComponentProps } from "react";
import type { EnvironmentId } from "@t3tools/contracts";
import { attentionKey, bindingKey, type Item } from "@t3tools/client-runtime/jones/remoteAdmission/controller";
import { toastManager } from "../../components/ui/toast";
import { Button } from "../../components/ui/button";
import { useClientSettings } from "../../hooks/useSettings";
import { useEnvironments } from "../../state/environments";
import { admissionBindingKey, actionableAdmissions } from "./store";
import { RequestReviewRow } from "./RequestReviewRow";
import { useAdmission } from "./useAdmission";
import { useReviewController } from "./reviewRuntime";
import { useFrozenRows, useReview } from "./reviewHooks";
import { requestHref, threadHref } from "./navigation";
import { admissionPresentation } from "./presentation";
import { ReviewControls } from "./ReviewControls";
import { RequestThreadTitle, RequestThreadStatus } from "./RequestThreadStatus";

import { attentionRevision, subscribeAttention, dismissedIncoming, dismissedResults, receiverAttention } from "./attentionPersistence";
export function RemoteAdmissionNotifications() {
  const { environments } = useEnvironments();
  return environments.map((environment) => <EnvironmentNotifications key={environment.environmentId} receiver={environment.environmentId} label={environment.displayLabel} />);
}
function EnvironmentNotifications({ receiver, label }: { receiver: EnvironmentId; label: string }) {
  const controller = useReviewController(receiver);
  return controller ? <ReceiverNotifications receiver={receiver} label={label} /> : <LegacyNotifications receiver={receiver} label={label} />;
}
function ReceiverNotifications({ receiver, label }: { receiver: EnvironmentId; label: string }) {
  const state = useReview(receiver);
  const attention = useSyncExternalStore(subscribeAttention, attentionRevision);
  const inApp = useClientSettings((settings) => settings.inAppNotificationsEnabled);
  const incoming = state.capabilities?.canDecide && !state.loadFailed ? state.incoming : [];
  const results = (state.capabilities?.canDecide && !state.loadFailed ? state.results : []).filter((item) => !dismissedResults.has(attentionKey(receiver, item)));
  const incomingId = useRef<string | null>(null);
  const resultId = useRef<string | null>(null);
  useEffect(() => {
    state.controller.setAttentionPersistence(receiverAttention(receiver));
    const reconnect = () => { if (document.visibilityState !== "hidden") void state.controller.watchResults(); };
    reconnect();
    window.addEventListener("online", reconnect);
    document.addEventListener("visibilitychange", reconnect);
    return () => {
      window.removeEventListener("online", reconnect);
      document.removeEventListener("visibilitychange", reconnect);
      state.controller.stopResults();
    };
  }, [state.controller, receiver]);
  useEffect(() => {
    if (inApp && incoming.some((item) => !dismissedIncoming.has(attentionKey(receiver, item))) && !incomingId.current) {
      incomingId.current = toastManager.add({ timeout: 0, data: { explicitDismissOnly: true, hideCopyButton: true, onClose: () => { for (const item of state.controller.getSnapshot().incoming) dismissedIncoming.add(attentionKey(receiver, item)); incomingId.current = null; }, bodyContent: <IncomingStack receiver={receiver} label={label} dismiss={() => { const current = state.controller.getSnapshot(); for (const item of current.incoming) dismissedIncoming.add(attentionKey(receiver, item)); if (incomingId.current) toastManager.close(incomingId.current); incomingId.current = null; }} /> } });
    }
    if (inApp && results.length && !resultId.current) {
      resultId.current = toastManager.add({ timeout: 0, data: { explicitDismissOnly: true, hideCopyButton: true, onClose: () => { for (const item of state.controller.getSnapshot().results) dismissedResults.add(attentionKey(receiver, item)); resultId.current = null; }, bodyContent: <CreatedStack receiver={receiver} label={label} dismiss={() => { for (const item of state.controller.getSnapshot().results) dismissedResults.add(attentionKey(receiver, item)); if (resultId.current) toastManager.close(resultId.current); resultId.current = null; }} /> } });
    }
    if (!inApp) { if (incomingId.current) toastManager.close(incomingId.current); if (resultId.current) toastManager.close(resultId.current); incomingId.current = null; resultId.current = null; }
  }, [inApp, incoming, results, state.controller, receiver, label, attention]);
  useEffect(() => {
    const a = incomingId; const b = resultId;
    return () => { if (a.current) toastManager.close(a.current); if (b.current) toastManager.close(b.current); };
  }, []);
  return null;
}
function StackHeader({ title, receiver, collapsed, toggle, dismiss }: { title: string; receiver: string; collapsed: boolean; toggle: () => void; dismiss: () => void }) {
  return <div className="flex items-center gap-2"><Button className="min-w-0 flex-1 justify-start whitespace-normal text-left" size="sm" variant="ghost" aria-expanded={!collapsed} onClick={toggle}>{collapsed ? "▸" : "▾"} {title}</Button><a className="mr-3 p-2 text-[10px]" href={requestHref(receiver)} aria-label="Open Requests">↗</a><Button size="sm" variant="ghost" aria-label="Dismiss stack" onClick={dismiss}>×</Button></div>;
}
function IncomingStack({ receiver, label, dismiss }: { receiver: EnvironmentId; label: string; dismiss: () => void }) {
  const state = useReview(receiver);
  useSyncExternalStore(subscribeAttention, attentionRevision);
  const [collapsed, setCollapsed] = useState(false);
  const records = state.incoming.filter((item) => item.record.status === "held" && !dismissedIncoming.has(attentionKey(receiver, item)));
  const sorted = [...records].sort((a, b) => b.record.createdAt.localeCompare(a.record.createdAt) || b.record.id.localeCompare(a.record.id));
  const frozen = useFrozenRows(sorted.slice(0, 3), (item) => bindingKey(item.record));
  const count = Math.max(records.length, state.currentHeldCount - state.incoming.filter((item) => dismissedIncoming.has(attentionKey(receiver, item))).length);
  useEffect(() => { if (!frozen.frozen && frozen.rows.length === 0) dismiss(); }, [frozen.frozen, frozen.rows.length, dismiss]);
  return <section aria-label={`Incoming approval requests on ${label}`} className="min-w-0 space-y-2">
    <StackHeader title={`${count} incoming approval request${count === 1 ? "" : "s"}`} receiver={receiver} collapsed={collapsed} toggle={() => setCollapsed(!collapsed)} dismiss={dismiss} />
    {!collapsed ? <div {...frozen.handlers} className="space-y-3"><p className="text-xs text-muted-foreground">Incoming to {label}</p>{frozen.rows.map((item) => {
      const current = records.find((entry) => bindingKey(entry.record) === bindingKey(item.record));
      const key = bindingKey(item.record);
      const blocked = !current ? "Resolved · controls disabled" : state.loadFailed ? "Review unavailable; refresh Requests." : state.policyBarrier ? "Evaluator settings are changing." : state.pending.has(key) ? "Recording this decision…" : state.uncertain.has(key) ? "Outcome unknown · do not resend" : item.record.envelope.receiverPolicy.revision !== state.page?.policyRevision ? "Evaluator settings changed. Review in Requests." : null;
      return <IncomingRow key={key} item={item} receiver={receiver} disabled={blocked} grants={state.capabilities?.features.has("grants") ?? false} decide={(input) => { void state.controller.mutate(input); }} />;
    })}{count > 3 ? <a href={requestHref(receiver)} className="text-xs underline">{count - 3} more in Requests</a> : null}</div> : null}
  </section>;
}
function IncomingRow({ item, receiver, disabled, grants, decide }: { item: Item; receiver: EnvironmentId; disabled: string | null; grants: boolean; decide: ComponentProps<typeof ReviewControls>["mutate"] }) {
  const [collapsed, setCollapsed] = useState(false);
  const view = admissionPresentation(item.record);
  const failure = item.record.category === "evaluation_unavailable" || item.record.category === "evaluation_interrupted";
  return <article className="min-w-0 space-y-2 border-t pt-2"><div className="flex items-start gap-1"><button type="button" className="min-w-0 flex-1 text-left text-sm font-medium wrap-anywhere" aria-expanded={!collapsed} onClick={() => setCollapsed(!collapsed)}>{collapsed ? "▸" : "▾"} <RequestThreadTitle environmentId={receiver} threadId={item.threadId} fallback={view.title ?? "Incoming request"} /></button><span className="rounded bg-muted px-1 text-xs text-muted-foreground">{view.operation}</span><a className="p-2 text-[10px]" aria-label="Open exact request" href={requestHref(receiver, item.record.id, item.record.bindingDigest)}>↗</a></div>{!collapsed ? <><p className="text-xs text-muted-foreground">{view.source}{view.project ? ` · ${view.project.label}` : ""}</p><p className="text-xs wrap-anywhere">{view.message.slice(0, 280)}</p>{failure ? <p className="text-xs text-muted-foreground">Evaluation did not complete. This is not a finding of unsafe content. Open the exact request for supported recovery actions; dismissing only clears attention.</p> : null}{failure ? <Button size="compact" variant="ghost" aria-label="Dismiss request attention" onClick={() => dismissedIncoming.add(attentionKey(receiver, item))}>Dismiss</Button> : null}<ReviewControls item={item} disabled={disabled} grantSupported={grants} mutate={decide} /></> : null}</article>;
}
export function creationSummary(items: readonly Item[]) {
  const created = items.filter((item) => item.outcome === "created").length;
  const failed = items.filter((item) => item.outcome === "failed" || item.outcome === "not_created").length;
  const unknown = items.filter((item) => item.outcome === "unknown").length;
  return [created ? `${created} thread${created === 1 ? "" : "s"} created` : null, failed ? `${failed} failed` : null, unknown ? `${unknown} unknown` : null].filter(Boolean).join(" · ");
}
function CreatedStack({ receiver, label, dismiss }: { receiver: EnvironmentId; label: string; dismiss: () => void }) {
  const state = useReview(receiver);
  useSyncExternalStore(subscribeAttention, attentionRevision);
  const [collapsed, setCollapsed] = useState(false);
  const rows = state.results.filter((item) => !dismissedResults.has(attentionKey(receiver, item)));
  const frozen = useFrozenRows(rows, (item) => attentionKey(receiver, item));
  return <section aria-label={`Thread creation results on ${label}`}><StackHeader title={creationSummary(rows) || "Thread creation results"} receiver={receiver} collapsed={collapsed} toggle={() => setCollapsed(!collapsed)} dismiss={dismiss} />{!collapsed ? <div {...frozen.handlers}>{frozen.rows.map((item) => <CreatedRow key={attentionKey(receiver, item)} receiver={receiver} item={item} label={label} />)}</div> : null}</section>;
}
function CreatedRow({ receiver, item, label }: { receiver: EnvironmentId; item: Item; label: string }) {
  const [collapsed, setCollapsed] = useState(false);
  const view = admissionPresentation(item.record);
  const available = item.outcome === "created" && item.availability === "available" && item.threadId;
  const status = item.outcome === "created" ? item.availability === "available" ? "Created" : item.availability === "deleted" ? "Created then deleted" : "Created · availability unknown" : item.outcome === "unknown" ? "Outcome unknown" : item.outcome === "not_created" ? "Not created" : "Creation failed";
  return <article className="space-y-1 border-t py-2"><div className="flex items-center gap-2"><button type="button" className="min-w-0 flex-1 text-left text-xs wrap-anywhere" aria-expanded={!collapsed} onClick={() => setCollapsed(!collapsed)}>{collapsed ? "▸" : "▾"} <RequestThreadTitle environmentId={receiver} threadId={available ? item.threadId : null} fallback={view.title ?? "Thread creation"} /></button><span className="ml-auto text-right text-xs">{status}</span><a className="p-2 text-[10px]" aria-label={available ? "Open exact thread" : "Open exact request"} href={available ? threadHref(receiver, item.threadId!) : requestHref(receiver, item.record.id, item.record.bindingDigest)}>↗</a></div>{!collapsed ? <><div className="flex flex-wrap gap-2 text-xs text-muted-foreground"><span>{label}{view.project ? ` · ${view.project.label}` : ""}</span><span className="ml-auto text-right">{item.messageStage}</span></div>{item.reason ? <p className="text-xs text-muted-foreground">{item.reason}</p> : null}{!available ? <p className="text-xs text-muted-foreground">Open the exact request for retained evidence and supported recovery actions. This link does not authorize or launch a retry.</p> : null}<Button size="compact" variant="ghost" aria-label="Dismiss result attention" onClick={() => dismissedResults.add(attentionKey(receiver, item))}>Dismiss</Button>{available ? <RequestThreadStatus environmentId={receiver} threadId={item.threadId} /> : null}</> : null}</article>;
}
function LegacyNotifications({ receiver, label }: { receiver: EnvironmentId; label: string }) {
  const state = useAdmission(receiver);
  const enabled = useClientSettings((settings) => settings.inAppNotificationsEnabled);
  const toast = useRef<string | null>(null);
  const records = actionableAdmissions(state);
  useEffect(() => {
    const unseen = records.some((record) => !dismissedIncoming.has(JSON.stringify([receiver, admissionBindingKey(record)])));
    if (enabled && !state.loadFailed && unseen && !toast.current) toast.current = toastManager.add({ timeout: 0, title: `${records.length} incoming approval request${records.length === 1 ? "" : "s"} · ${label}`, data: { explicitDismissOnly: true, hideCopyButton: true, onClose: () => { for (const record of records) dismissedIncoming.add(JSON.stringify([receiver, admissionBindingKey(record)])); toast.current = null; }, bodyContent: <LegacyStack receiver={receiver} /> } });
    if ((!enabled || state.loadFailed) && toast.current) { toastManager.close(toast.current); toast.current = null; }
  }, [enabled, state.loadFailed, records, receiver, label]);
  useEffect(() => { const owned = toast; return () => { if (owned.current) toastManager.close(owned.current); }; }, []);
  return null;
}
function LegacyStack({ receiver }: { receiver: EnvironmentId }) {
  const state = useAdmission(receiver);
  const rows = actionableAdmissions(state);
  const frozen = useFrozenRows(rows.slice(0, 3), admissionBindingKey);
  return <div className="space-y-2" {...frozen.handlers}><a href={requestHref(receiver)} aria-label="Open Requests" className="text-[10px]">↗</a>{frozen.rows.map((record) => <RequestReviewRow key={admissionBindingKey(record)} environmentId={receiver} record={record} policyRevision={state.snapshot?.policy.revision ?? ""} blocked={state.loadFailed || frozen.isGone(record) ? "Review unavailable or resolved" : state.deciding.has(admissionBindingKey(record)) ? "Recording this decision…" : state.unconfirmed.has(admissionBindingKey(record)) ? "Outcome unknown" : state.policyBusy ? "Evaluator settings are changing" : null} onDecide={(input) => { void state.store.decide(input); }} />)}<p className="text-xs text-muted-foreground">Legacy review · at most 200 records. Access and creation updates unavailable.</p></div>;
}
