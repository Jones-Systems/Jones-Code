import { useId, useState } from "react";
import type { EnvironmentId } from "@t3tools/contracts";
import type { JonesInstallPlanSelection } from "@t3tools/contracts/jones/installPlan";
import { ChevronDownIcon, ChevronRightIcon, Clock3Icon, InfoIcon, LaptopIcon, TriangleAlertIcon } from "lucide-react";
import { Button } from "../../components/ui/button";
import { Checkbox } from "../../components/ui/checkbox";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { cn } from "../../lib/utils";
import { useInstallPlan } from "./useInstallPlan";
import { installStatusStore, type InstallStatusStore } from "./panelUi";
import { panelPresentation } from "./presentation";
import { selectionFor } from "./entrypoints";
import { installPlanServices, requestInstallPlan } from "./services";
import { performPanelAction, type PanelAction } from "./actions";
import { INSTALL_PLAN_GO, INSTALL_PLAN_INFO, INSTALL_PLAN_WAIT, lastSeenLabel, lastUpdateLabel } from "./copy";

interface EditDraft { readonly planId: string; readonly revision: number; readonly selected: ReadonlySet<EnvironmentId> }

export function InstallStatusPanel({ store = installStatusStore }: { readonly store?: InstallStatusStore }) {
  const snapshot = useInstallPlan(store);
  const [draft, setDraft] = useState<EditDraft | null>(null);
  const [detail, setDetail] = useState<EnvironmentId | "laptop" | null>(null);
  const [pending, setPending] = useState<ReadonlySet<string>>(new Set());
  const descriptionId = useId();
  const view = panelPresentation(snapshot, snapshot.updatedAt);
  if (!snapshot.open || snapshot.native === null) return null;
  if (view?.plan == null) return snapshot.native.readOnly === undefined && snapshot.error === null ? null :
    <section aria-label="Install status" className="shrink-0 rounded-lg border border-sidebar-border p-2 text-xs text-muted-foreground"><h2>Install status</h2><p role="status">{snapshot.native.readOnly?.message ?? snapshot.error}</p></section>;
  const plan = view.plan;
  const editing = draft !== null && draft.planId === plan.planId && !view.readOnly;
  const selected = editing ? draft!.selected : new Set(plan.devices.filter((d) => d.membership === "selected").map((d) => d.environmentId));
  const complete = view.headerDoneVisible && !editing;
  const laptopId = plan.laptop.environmentId;
  const devices = new Map(snapshot.devices.filter((d) => d.environmentId !== laptopId).map((d) => [d.environmentId, d]));
  const ids = [...new Set([...plan.devices.map((d) => d.environmentId), ...devices.keys()])];
  const choices = ids.map((environmentId) => {
    const device = devices.get(environmentId);
    const member = plan.devices.find((d) => d.environmentId === environmentId);
    const selection = device === undefined ? undefined : selectionFor(device);
    const locked = (member?.operations.length ?? 0) > 0;
    const reason = locked ? "Already started. Open this device to stop its download." : selection === undefined
      ? device?.observation.message ?? (device?.canOperate === false ? "This session cannot change updates." : "Fresh operations and enrollment capability are required.") : undefined;
    const row = view.rows.find((r) => r.environmentId === environmentId);
    return { environmentId, label: member?.label ?? device?.label ?? "Device", selection, locked, reason, row, device };
  });
  const perform = async (key: string, work: () => Promise<void>) => {
    if (pending.has(key)) return;
    setPending((old) => new Set([...old, key]));
    store.update({ error: null });
    try { await work(); }
    catch (error) { store.update({ error: error instanceof Error ? error.message : "The request is not confirmed. Read status before continuing." }); }
    finally { setPending((old) => new Set([...old].filter((item) => item !== key))); }
  };
  const action = (environmentId: EnvironmentId | "laptop", value: PanelAction) =>
    void perform(`${environmentId}:${value}`, () => performPanelAction(environmentId, value));
  const editDone = () => void perform("edit", async () => {
    if (!editing) return;
    const select: JonesInstallPlanSelection[] = choices.flatMap((c) => selected.has(c.environmentId) && c.selection !== undefined ? [c.selection] : []);
    if (view.preview) store.update({ previewSelection: select });
    else {
      const binding = installPlanServices();
      const result = await requestInstallPlan({ type: "edit", planId: plan.planId, expectedRevision: draft!.revision, select,
        deselect: plan.devices.filter((d) => d.membership === "selected" && !selected.has(d.environmentId)).map((d) => d.environmentId) }, binding);
      if (result.refused !== undefined) throw new Error(result.refused.message);
      const locked = result.applied?.filter((d) => d.result === "locked-started") ?? [];
      if (locked.length > 0) store.update({ error: locked.map((d) => `${choices.find((c) => c.environmentId === d.environmentId)?.label ?? "Device"} started before the edit applied, so it stays in. Open it to stop the download.`).join(" ") });
    }
    setDraft(null);
  });
  const collapse = () => { setDraft(null); setDetail(null); store.collapse(!snapshot.collapsed); };
  const rowClass = "flex min-h-8 w-full min-w-0 items-center gap-2 rounded-md px-2 text-left text-xs hover:bg-sidebar-row-hover focus-visible:outline-2 focus-visible:outline-ring";
  const availableLaptop = view.laptop?.actions;
  return <section aria-label="Install status" data-install-status data-plan-id={plan.planId} data-plan-preview={view.preview || undefined}
    className="flex max-h-[min(26rem,45dvh)] min-h-0 shrink-0 flex-col rounded-xl border border-sidebar-border bg-sidebar text-sidebar-foreground"
    onKeyDown={(event) => { if (event.key === "Escape") { setDraft(null); setDetail(null); } }}>
    <header className="grid min-h-9 shrink-0 grid-cols-[auto_minmax(0,1fr)_2.5rem_1.5rem] items-center gap-1 px-2" data-install-header>
      <h2 className="whitespace-nowrap text-xs font-medium" data-install-title>Install status</h2>
      <Tooltip>
        <TooltipTrigger render={<span tabIndex={0} className="truncate text-2xs text-muted-foreground" />}>
          {view.readOnly ? lastUpdateLabel(plan.dismissedAt ?? plan.createdAt) : view.preview ? "Laptop first" : view.summary}
        </TooltipTrigger>
        <TooltipPopup>{view.readOnly ? lastUpdateLabel(plan.dismissedAt ?? plan.createdAt) : view.preview ? "Laptop first" : view.summary}</TooltipPopup>
      </Tooltip>
      <span data-install-done-slot aria-hidden={!complete} inert={!complete} style={{ visibility: complete ? "visible" : "hidden" }} className="flex w-10 justify-end">
        <Button variant="ghost" size="micro" aria-label="Done with update plan" disabled={pending.has("dismiss")} onClick={() => void perform("dismiss", async () => {
          const result = await requestInstallPlan({ type: "dismiss", planId: plan.planId, expectedRevision: plan.revision });
          if (result.refused !== undefined) throw new Error(result.refused.message);
          store.update({ open: false, showLast: true, previewSelection: [] });
        })}>Done</Button>
      </span>
      <span className="flex size-6 items-center justify-center">
        <Button variant="ghost-muted" size="icon-micro" aria-label={snapshot.collapsed ? "Expand Install status" : "Collapse Install status"} aria-expanded={!snapshot.collapsed} data-install-chevron onClick={collapse}>
          <ChevronDownIcon className={cn("size-3.5", snapshot.collapsed && "-rotate-90")} />
        </Button>
      </span>
    </header>
    {!snapshot.collapsed ? <div className="min-h-0 overflow-y-auto overscroll-contain px-1 pb-1" data-install-body>
      <button type="button" className={rowClass} onClick={() => setDetail(detail === "laptop" ? null : "laptop")} aria-expanded={detail === "laptop"} aria-label="Laptop update details">
        <LaptopIcon className="size-3.5 shrink-0 text-muted-foreground" /><span>Laptop</span><span className="ml-auto min-w-0 truncate text-right text-muted-foreground">{view.laptop?.text}</span><ChevronRightIcon className="size-3 shrink-0 text-muted-foreground" />
      </button>
      {!view.readOnly && !editing ? <div className="flex min-h-7 flex-wrap gap-1 px-2 pb-2">
        {availableLaptop?.download ? <Button size="micro" variant="outline" disabled={pending.has("laptop:download")} onClick={() => action("laptop", "download")}>Download on Laptop</Button> : null}
        {availableLaptop?.install ? <Button size="micro" variant="outline" disabled={pending.has("laptop:install")} onClick={() => action("laptop", "install")}>Install on Laptop</Button> : null}
        {availableLaptop?.stop ? <Button size="micro" variant="outline" disabled={pending.has("laptop:stop")} onClick={() => action("laptop", "stop")}>Stop download</Button> : null}
      </div> : null}
      {detail === "laptop" ? <div className="space-y-1 px-2 pb-2 text-2xs text-muted-foreground">
        <p>{view.laptop?.text}</p>
        {view.laptop?.operationId ? <OperationId value={view.laptop.operationId} /> : null}
        {plan.laptop.operations.at(-1)?.result?.code ? <p>Code: {plan.laptop.operations.at(-1)!.result!.code}</p> : null}
        {plan.laptop.operations.at(-1)?.result?.message ? <p>{plan.laptop.operations.at(-1)!.result!.message}</p> : null}
        {view.laptop?.phase === "held" ? <p>The download process has not proved exit. Existing app and data are preserved; no new work starts. Check again for an exact outcome.</p> : null}
        {!view.readOnly ? <Button variant="ghost-muted" size="micro" onClick={() => action("laptop", "check")}>Check again</Button> : null}
      </div> : null}
      <div data-install-gate className="flex min-h-8 items-center gap-1.5 border-t border-dashed border-sidebar-border px-2 text-2xs text-muted-foreground">
        <Clock3Icon className="size-3 shrink-0" /><span className="min-w-0 flex-1 truncate">{plan.laptop.gate !== undefined && plan.laptop.gateClosed === undefined ? INSTALL_PLAN_GO : INSTALL_PLAN_WAIT}</span>
        {!view.readOnly ? <span className="flex shrink-0 items-center text-primary">{editing ? <>
          <Button variant="link" size="micro" onClick={() => setDraft({ ...draft!, selected: new Set(choices.filter((c) => c.locked ? selected.has(c.environmentId) : c.selection !== undefined).map((c) => c.environmentId)) })}>All</Button>
          <Button variant="link" size="micro" onClick={() => setDraft({ ...draft!, selected: new Set(choices.filter((c) => c.locked && selected.has(c.environmentId)).map((c) => c.environmentId)) })}>None</Button>
          <Button variant="link" size="micro" disabled={pending.has("edit")} onClick={editDone}>Done</Button>
        </> : <Button variant="link" size="micro" onClick={() => setDraft({ planId: plan.planId, revision: plan.revision, selected: new Set(selected) })}>Edit</Button>}</span> : null}
      </div>
      <ul className="m-0 list-none p-0">
        {choices.map((choice) => {
          const { environmentId, label, row, device } = choice;
          const included = selected.has(environmentId);
          const text = editing ? included ? "Waits for Laptop" : "Not included" : row?.text ?? (choice.reason === undefined ? "Not included" : `Can't join · ${choice.reason}`);
          const open = detail === environmentId && !editing;
          const held = row?.phase === "held" || row?.phase === "unknown";
          const receipt = plan.devices.find((d) => d.environmentId === environmentId)?.operations.at(-1)?.receipt;
          return <li key={environmentId}>
            <div className="flex min-w-0 items-center gap-1">
              {editing ? <Checkbox aria-label={`Include ${label}`} checked={included} disabled={choice.locked || choice.selection === undefined}
                aria-describedby={choice.reason === undefined ? undefined : `${descriptionId}-${environmentId}`} className="ml-2" onCheckedChange={(checked) => { const next = new Set(selected); if (checked) next.add(environmentId); else next.delete(environmentId); setDraft({ ...draft!, selected: next }); }} /> : null}
              <Tooltip>
                <TooltipTrigger render={<button type="button" className={rowClass} />} aria-label={`${label} update details`} aria-expanded={open} onClick={() => { if (!editing) setDetail(open ? null : environmentId); }}>
                  {held ? <TriangleAlertIcon className="size-3 shrink-0 text-warning" /> : <span aria-hidden className={cn("size-1.5 shrink-0 rounded-full border border-muted-foreground", !included && "bg-muted-foreground opacity-50")} />}
                  <span className={cn("shrink-0", !included && "text-muted-foreground")}>{label}</span>
                  <span className="ml-auto min-w-0 truncate text-right text-2xs text-muted-foreground">{text}</span><ChevronRightIcon className="size-3 shrink-0 text-muted-foreground" />
                </TooltipTrigger>
                <TooltipPopup>{editing && choice.reason !== undefined ? choice.reason : text}</TooltipPopup>
              </Tooltip>
            </div>
            {editing && choice.reason !== undefined ? <p id={`${descriptionId}-${environmentId}`} className="px-2 pb-1 text-2xs text-muted-foreground">{choice.reason}</p> : null}
            {open ? <div className="space-y-1 px-3 pb-2 text-2xs text-muted-foreground" aria-label={`${label} update status`}>
              <p>{text}</p>
              {device?.observation.freshness !== "fresh" ? <p>{device?.observation.message ?? "Fresh status unavailable."} {lastSeenLabel(device?.lastObservedAt)}</p> : null}
              {row?.status?.check ? <p>{row.status.check.text}</p> : null}
              {row?.operationId ? <OperationId value={row.operationId} /> : null}
              {receipt?.code ? <p>Code: {receipt.code}</p> : null}{receipt?.message ? <p>{receipt.message}</p> : null}
              {receipt?.stop?.result === "too-late" ? <p>The download completed before Stop. Review installation separately.</p> : receipt?.stop?.result === "not-stoppable-phase" ? <p>This phase cannot be stopped. Read the operation outcome.</p> : null}
              {held ? <p>Existing app and data are preserved. No retry is sent. Check again for the exact operation receipt; use Connections for host reconciliation.</p> : null}
              {!view.readOnly ? <div className="flex flex-wrap gap-1">
                {row?.actions.remove ? <Button variant="outline" size="micro" onClick={() => action(environmentId, "remove")}>Remove from queue</Button> : null}
                {row?.actions.download ? <Button variant="outline" size="micro" onClick={() => action(environmentId, "download")}>Download on {label}</Button> : null}
                {row?.actions.install ? <Button variant="outline" size="micro" disabled={pending.has(`${environmentId}:install`)} onClick={() => action(environmentId, "install")}>Install on {label}</Button> : null}
                {row?.actions.stop ? <Button variant="outline" size="micro" disabled={pending.has(`${environmentId}:stop`)} onClick={() => action(environmentId, "stop")}>Stop download on {label}</Button> : null}
                {row?.actions.end ? <Button variant="ghost-muted" size="micro" onClick={() => action(environmentId, "end")}>Stop update</Button> : null}
                <Button variant="ghost-muted" size="micro" onClick={() => action(environmentId, "check")}>Check again</Button>
              </div> : null}
            </div> : null}
          </li>;
        })}
      </ul>
      {editing ? <p className="flex gap-1.5 px-2 pt-1 text-2xs leading-relaxed text-muted-foreground"><InfoIcon className="mt-0.5 size-3 shrink-0" />{INSTALL_PLAN_INFO}</p> : null}
      {snapshot.error ? <p role="status" className="px-2 py-1 text-2xs text-muted-foreground">{snapshot.error}</p> : null}
    </div> : null}
  </section>;
}
function OperationId({ value }: { readonly value: string }) {
  return <label className="block">Operation <input aria-label="Operation ID" readOnly value={value} className="w-full select-all rounded border border-sidebar-border bg-transparent px-1 font-mono text-3xs" onFocus={(event) => event.currentTarget.select()} /></label>;
}
