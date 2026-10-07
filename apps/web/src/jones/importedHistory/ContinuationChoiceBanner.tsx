import { useEffect, useMemo, useRef, useState } from "react";
import { CommandId, type EnvironmentId, type ThreadId, type ImportedHistoryDelivery } from "@t3tools/contracts";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { createImportedHistoryCommands } from "@t3tools/client-runtime/jones/imported-history/commands";
import { createImportedHistoryChoiceController, type ImportedHistoryChoiceState } from "@t3tools/client-runtime/jones/imported-history/continuation";
import { connectionAtomRuntime } from "../../connection/runtime";
import { importedHistoryCorrelationStorage } from "../../composerDraftStore";
import { useAtomCommand } from "../../state/use-atom-command";
import { ComposerBanner } from "../../components/chat/ComposerBanner";
import { Button } from "../../components/ui/button";

export const importedHistoryCommands = createImportedHistoryCommands(connectionAtomRuntime);
export type PreparedImportedHistoryChoice = {
  readonly delivery: ImportedHistoryDelivery;
  readonly reviewedBasis: string;
  readonly draftIdentity: string;
  readonly unchanged: () => boolean;
};
export function ContinuationChoiceBanner(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly prepared: PreparedImportedHistoryChoice | null;
  readonly reason?: string | null;
  readonly onDismiss?: () => void;
}) {
  const start = useAtomCommand(importedHistoryCommands.start, { reportFailure: false });
  const observe = useAtomCommand(importedHistoryCommands.observe, { reportFailure: false });
  const identity = useAtomCommand(importedHistoryCommands.identity, { reportFailure: false });
  const storage = useMemo(() => importedHistoryCorrelationStorage(scopeThreadRef(props.environmentId, props.threadId)), [props.environmentId, props.threadId]);
  const controller = useMemo(() => createImportedHistoryChoiceController(storage, {
    start: async (input) => { const result = await start({ environmentId: props.environmentId, input }); return result._tag === "Success" ? result.value : null; },
    observe: async (input) => { const result = await observe({ environmentId: props.environmentId, input }); return result._tag === "Success" ? result.value : null; },
  }), [storage, start, observe, props.environmentId]);
  const [state, setState] = useState<ImportedHistoryChoiceState | null>(null);
  const [saved, setSaved] = useState(false);
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const scope = `${props.environmentId}:${props.threadId}`;
  const scopeRef = useRef(scope);
  scopeRef.current = scope;
  useEffect(() => {
    setState(null);
    try { setSaved(storage.read() !== null); }
    catch { setSaved(true); setState({ status: "unknown", intentAccepted: false, reason: "Saved choice is unavailable. Resolve storage before another start." }); }
  }, [storage, props.prepared, props.reason]);
  const act = async () => {
    if (busyRef.current) return;
    busyRef.current = true;
    setBusy(true);
    try {
      let next: ImportedHistoryChoiceState;
      if (saved) next = await controller.observe();
      else {
        const prepared = props.prepared;
        if (prepared === null || !prepared.unchanged()) return;
        const commandId = prepared.delivery.type === "message" ? prepared.delivery.command.commandId : CommandId.make(globalThis.crypto.randomUUID());
        const command = { type: "thread.imported-history.start" as const, commandId, threadId: props.threadId, reviewedBasis: prepared.reviewedBasis, delivery: prepared.delivery };
        const digests = await identity({ environmentId: props.environmentId, input: command });
        if (digests._tag !== "Success") {
          next = { status: "unknown", intentAccepted: false, reason: "Could not bind the reviewed command. No start was submitted." };
        } else next = await controller.start({ environmentId: props.environmentId, command, draftIdentity: prepared.draftIdentity, ...digests.value }, () => scopeRef.current === scope && prepared.unchanged());
      }
      if (scopeRef.current === scope) {
        setState(next);
        setSaved(storage.read() !== null);
        if (next.status === "rejected") props.onDismiss?.();
      }
    } catch {
      if (scopeRef.current === scope) setState({ status: "unknown", intentAccepted: false, reason: "Observe the saved choice before continuing." });
    } finally { busyRef.current = false; setBusy(false); }
  };
  if (!saved && props.prepared === null && !props.reason && state === null) return null;
  return <ComposerBanner.Root variant="warning">
    <ComposerBanner.Row>
      <ComposerBanner.Content><span role="status">{state?.reason ?? props.reason ?? (saved ? "A saved choice awaits observation; execution is unconfirmed." : "Start a new conversation with the reviewed imported history?")}</span></ComposerBanner.Content>
      <ComposerBanner.Actions>
        <Button size="xs" variant="ghost" disabled={busy || (!saved && (props.prepared === null || !props.prepared.unchanged()))} onClick={() => { void act(); }}>{saved ? "Observe choice" : "Use imported history"}</Button>
        {!saved && props.onDismiss ? <Button size="xs" variant="ghost" disabled={busy} onClick={props.onDismiss}>Cancel</Button> : null}
      </ComposerBanner.Actions>
    </ComposerBanner.Row>
  </ComposerBanner.Root>;
}
